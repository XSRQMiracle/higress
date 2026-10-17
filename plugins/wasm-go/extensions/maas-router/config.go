package main

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
)

// Pool 是一个模型池：对应 Envoy 中的一条路由 / 一个 upstream cluster。
type Pool struct {
	Name  string `json:"name"`
	Model string `json:"model"`
}

type Rule struct {
	pattern *regexp.Regexp
	Raw     string `json:"pattern"`
	Pool    string `json:"pool"`
}

type Config struct {
	autoModel        string // 请求的 model 为该值时启用语义分发
	poolHeader       string // 写入选中模型池名称的请求头，Envoy 路由按它匹配
	defaultPool      string
	longPromptPool   string
	longPromptTokens int
	pools            map[string]Pool
	modelToPool      map[string]string
	embed            *wrapper.ClusterClient[wrapper.FQDNCluster]
	embedPath        string
	embedModel       string
	embedTimeout     uint32
	minSimilarity    float64
	classes          []*Class
	rules            []Rule
	debugHeaders     bool
	state            *routerState
	metrics          *routerMetrics
}

// routerState 是本 worker 内的状态：类别质心（首次需要时向 embedder 批量请求样例向量）与按类别统计的输出长度。
type routerState struct {
	centroidsReady bool
	initializing   bool
	outputEWMA     map[string]float64
}

func intOr(v gjson.Result, def int64) int64 {
	if v.Exists() {
		return v.Int()
	}
	return def
}

func parseConfig(json gjson.Result, c *Config) error {
	c.autoModel = json.Get("autoModel").String()
	if c.autoModel == "" {
		c.autoModel = "higress/auto"
	}
	c.poolHeader = json.Get("poolHeader").String()
	if c.poolHeader == "" {
		c.poolHeader = "x-maas-pool"
	}
	c.pools = map[string]Pool{}
	c.modelToPool = map[string]string{}
	for _, p := range json.Get("pools").Array() {
		pool := Pool{Name: p.Get("name").String(), Model: p.Get("model").String()}
		if pool.Name == "" || pool.Model == "" {
			return errors.New("every pool needs a name and a model")
		}
		c.pools[pool.Name] = pool
		c.modelToPool[pool.Model] = pool.Name
	}
	if len(c.pools) == 0 {
		return errors.New("at least one pool is required")
	}
	c.defaultPool = json.Get("defaultPool").String()
	if _, ok := c.pools[c.defaultPool]; !ok {
		return fmt.Errorf("defaultPool %q is not a configured pool", c.defaultPool)
	}
	c.longPromptPool = json.Get("longPromptPool").String()
	if c.longPromptPool == "" {
		c.longPromptPool = c.defaultPool
	}
	if _, ok := c.pools[c.longPromptPool]; !ok {
		return fmt.Errorf("longPromptPool %q is not a configured pool", c.longPromptPool)
	}
	c.longPromptTokens = int(intOr(json.Get("longPromptTokens"), 2000))

	for _, r := range json.Get("rules").Array() {
		re, err := regexp.Compile(r.Get("pattern").String())
		if err != nil {
			return fmt.Errorf("invalid rule pattern %q: %v", r.Get("pattern").String(), err)
		}
		rule := Rule{pattern: re, Raw: r.Get("pattern").String(), Pool: r.Get("pool").String()}
		if _, ok := c.pools[rule.Pool]; !ok {
			return fmt.Errorf("rule pool %q is not a configured pool", rule.Pool)
		}
		c.rules = append(c.rules, rule)
	}
	for _, cl := range json.Get("classes").Array() {
		class := &Class{Name: cl.Get("name").String(), Pool: cl.Get("pool").String()}
		for _, e := range cl.Get("examples").Array() {
			class.Examples = append(class.Examples, e.String())
		}
		if class.Name == "" || len(class.Examples) == 0 {
			return errors.New("every class needs a name and at least one example")
		}
		if _, ok := c.pools[class.Pool]; !ok {
			return fmt.Errorf("class %q routes to unknown pool %q", class.Name, class.Pool)
		}
		c.classes = append(c.classes, class)
	}
	if len(c.classes) > 0 {
		embed := json.Get("embedding")
		if embed.Get("serviceName").String() == "" {
			return errors.New("embedding.serviceName is required when classes are configured")
		}
		c.embed = wrapper.NewClusterClient(wrapper.FQDNCluster{
			FQDN: embed.Get("serviceName").String(), Host: embed.Get("serviceHost").String(), Port: intOr(embed.Get("servicePort"), 80),
		})
		c.embedPath = embed.Get("path").String()
		if c.embedPath == "" {
			c.embedPath = "/v1/embeddings"
		}
		c.embedModel = embed.Get("model").String()
		c.embedTimeout = uint32(intOr(embed.Get("timeoutMs"), 100))
	}
	c.minSimilarity = 0.55
	if v := json.Get("minSimilarity"); v.Exists() {
		c.minSimilarity = v.Float()
	}
	c.debugHeaders = !json.Get("debugHeaders").Exists() || json.Get("debugHeaders").Bool()
	c.state = &routerState{outputEWMA: map[string]float64{}}
	c.metrics = newRouterMetrics(c.pools)
	return nil
}

type routerMetrics struct {
	auto        proxywasm.MetricCounter
	explicit    proxywasm.MetricCounter
	semantic    proxywasm.MetricCounter
	rule        proxywasm.MetricCounter
	longPrompt  proxywasm.MetricCounter
	fallback    proxywasm.MetricCounter
	reused      proxywasm.MetricCounter
	embedErrors proxywasm.MetricCounter
	pools       map[string]proxywasm.MetricCounter
}

func newRouterMetrics(pools map[string]Pool) *routerMetrics {
	m := &routerMetrics{
		auto:        proxywasm.DefineCounterMetric("maas_router_auto_total"),
		explicit:    proxywasm.DefineCounterMetric("maas_router_explicit_total"),
		semantic:    proxywasm.DefineCounterMetric("maas_router_semantic_total"),
		rule:        proxywasm.DefineCounterMetric("maas_router_rule_total"),
		longPrompt:  proxywasm.DefineCounterMetric("maas_router_long_prompt_total"),
		fallback:    proxywasm.DefineCounterMetric("maas_router_default_total"),
		reused:      proxywasm.DefineCounterMetric("maas_router_embedding_reused_total"),
		embedErrors: proxywasm.DefineCounterMetric("maas_router_embed_errors_total"),
		pools:       map[string]proxywasm.MetricCounter{},
	}
	for name := range pools {
		m.pools[name] = proxywasm.DefineCounterMetric("maas_router_pool_" + name + "_total")
	}
	return m
}
