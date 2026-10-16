package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/maas-cache/l1"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
)

type Config struct {
	paths               map[string]string // 请求路径 → 接口类型（chat / completion）
	namespace           string            // 参与缓存键计算；更换模型版本时修改它即可让旧缓存整体失效
	keyPrefix           string
	ignoredFields       map[string]struct{}
	varyHeaders         []string // 参与缓存键计算的请求头，例如按租户隔离缓存
	bypassHeaders       []string // 值为 on/true/1 时既不读缓存也不写缓存
	maxRequestBodyBytes int64
	maxResponseBytes    int
	maxTemperature      float64 // 小于 0 表示不限制
	finishReasons       map[string]struct{}
	debugHeaders        bool
	l1                  *l1.Cache[*Entry]
	l1MaxEntryBytes     int
	l2                  *redisStore
	metrics             *cacheMetrics
}

type redisStore struct {
	client wrapper.RedisClient
	ttl    int // 秒，0 表示不过期
}

func stringList(json gjson.Result, defaults []string) []string {
	if !json.Exists() {
		return defaults
	}
	var out []string
	json.ForEach(func(_, v gjson.Result) bool {
		if s := strings.TrimSpace(v.String()); s != "" {
			out = append(out, s)
		}
		return true
	})
	return out
}

func toSet(items []string) map[string]struct{} {
	set := make(map[string]struct{}, len(items))
	for _, item := range items {
		set[item] = struct{}{}
	}
	return set
}

func intOr(json gjson.Result, def int64) int64 {
	if json.Exists() {
		return json.Int()
	}
	return def
}

func parseConfig(json gjson.Result, c *Config) error {
	c.paths = map[string]string{}
	for _, p := range stringList(json.Get("chatPaths"), []string{"/v1/chat/completions"}) {
		c.paths[p] = kindChat
	}
	for _, p := range stringList(json.Get("completionPaths"), []string{"/v1/completions"}) {
		c.paths[p] = kindCompletion
	}
	c.namespace = json.Get("namespace").String()
	c.keyPrefix = "maas:cache:v1:"
	if prefix := json.Get("keyPrefix"); prefix.Exists() {
		c.keyPrefix = prefix.String()
	}
	c.ignoredFields = toSet(stringList(json.Get("ignoredFields"), defaultIgnoredFields))
	for _, h := range stringList(json.Get("varyHeaders"), nil) {
		c.varyHeaders = append(c.varyHeaders, strings.ToLower(h))
	}
	for _, h := range stringList(json.Get("bypassHeaders"), []string{"x-maas-cache-bypass", "x-higress-skip-ai-cache"}) {
		c.bypassHeaders = append(c.bypassHeaders, strings.ToLower(h))
	}
	c.maxRequestBodyBytes = intOr(json.Get("maxRequestBodyBytes"), 4<<20)
	if c.maxRequestBodyBytes < 1 || c.maxRequestBodyBytes > 100<<20 {
		return fmt.Errorf("maxRequestBodyBytes must be within [1, %d]", 100<<20)
	}
	c.maxResponseBytes = int(intOr(json.Get("maxResponseBytes"), 4<<20))
	if c.maxResponseBytes < 1 {
		return errors.New("maxResponseBytes must be positive")
	}
	c.maxTemperature = -1
	if t := json.Get("maxTemperature"); t.Exists() {
		c.maxTemperature = t.Float()
	}
	c.finishReasons = toSet(stringList(json.Get("finishReasons"), []string{"stop", "length"}))
	c.debugHeaders = !json.Get("debugHeaders").Exists() || json.Get("debugHeaders").Bool()

	if l1Conf := json.Get("l1"); !l1Conf.Get("enabled").Exists() || l1Conf.Get("enabled").Bool() {
		policy := l1.Policy(l1Conf.Get("policy").String())
		if policy == "" {
			policy = l1.PolicyTinyLFU
		}
		cache, err := l1.New[*Entry](policy, int(intOr(l1Conf.Get("capacity"), 4096)))
		if err != nil {
			return err
		}
		c.l1 = cache
		c.l1MaxEntryBytes = int(intOr(l1Conf.Get("maxEntryBytes"), 256<<10))
	}

	if l2Conf := json.Get("l2"); l2Conf.Exists() && (!l2Conf.Get("enabled").Exists() || l2Conf.Get("enabled").Bool()) {
		serviceName := l2Conf.Get("serviceName").String()
		if serviceName == "" {
			return errors.New("l2.serviceName is required, e.g. redis.dns")
		}
		client := wrapper.NewRedisClusterClient(wrapper.FQDNCluster{
			FQDN: serviceName,
			Host: l2Conf.Get("serviceHost").String(),
			Port: intOr(l2Conf.Get("servicePort"), 6379),
		})
		// Init 失败（例如 Redis 尚未就绪）时 SDK 会在下次调用前重试，这里不让整个插件配置失败。
		err := client.Init(l2Conf.Get("username").String(), l2Conf.Get("password").String(),
			intOr(l2Conf.Get("timeoutMs"), 200), wrapper.WithDataBase(int(l2Conf.Get("database").Int())))
		if err != nil {
			proxywasm.LogWarnf("[maas-cache] redis init failed, will retry on demand: %v", err)
		}
		c.l2 = &redisStore{client: client, ttl: int(intOr(l2Conf.Get("ttlSeconds"), 3600))}
		if c.l2.ttl < 0 {
			return errors.New("l2.ttlSeconds must be >= 0")
		}
	}
	if c.l1 == nil && c.l2 == nil {
		return errors.New("at least one of l1 and l2 must be enabled")
	}
	c.metrics = newCacheMetrics()
	return nil
}
