// maas-scheduler：MaaS 网关的推理副本调度插件（阶段 0：能力验证版）。
//
// 阶段 0 只验证后续调度算法依赖的两项 Higress 扩展能力：
//  1. proxywasm.GetUpstreamHosts()：读取当前路由 cluster 中每个 endpoint 的健康状态，以及
//     Envoy 主动健康检查拉到的 Prometheus 指标（cluster 的 health_check 需开启 store_metrics: true）；
//  2. proxywasm.SetUpstreamOverrideHost()：绕过 Envoy 自带的负载均衡，把请求钉到指定 endpoint。
//
// 选择策略（policy）：
//   - header：只有请求带 targetHeader 时才强制指定 endpoint，其余请求交给 Envoy 默认负载均衡；
//   - round_robin：插件自己在健康 endpoint 间轮询，用来和 Envoy 默认负载均衡对照；
//   - least_waiting：按 vLLM 指标选择，依次比较 waiting 队列长度、本网关在途请求数、KV cache 占用。
//
// 任意策略下，带 targetHeader 的请求都会被钉到该 endpoint（验证 override 用）。
// GET debugPath 返回插件看到的 endpoint 列表与解析出的指标（验证 store_metrics 用）。
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
)

const (
	pluginName          = "maas-scheduler"
	ctxPickedKey        = "maas_picked"
	ctxReasonKey        = "maas_reason"
	maxRequestBodyBytes = 16 << 20

	reasonPolicy         = "policy"
	reasonTarget         = "target_header"
	reasonTargetNotFound = "target_not_found"
	reasonNoHealthyHost  = "no_healthy_host"
	reasonHostsError     = "hosts_unavailable"
	reasonOverrideFailed = "override_failed"
)

type Policy string

const (
	PolicyHeader       Policy = "header"
	PolicyRoundRobin   Policy = "round_robin"
	PolicyLeastWaiting Policy = "least_waiting"
)

type Config struct {
	policy       Policy
	targetHeader string
	debugHeaders bool
	debugPath    string
	metrics      *pluginMetrics
}

// 每个 Envoy worker 线程有独立的 WASM VM，下面的状态只在本 worker 内可见。
var (
	rrCounter uint64
	inflight  = map[string]int{}
	random    = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func main() {}

func init() {
	wrapper.SetCtx(
		pluginName,
		wrapper.ParseConfig(parseConfig),
		wrapper.ProcessRequestHeaders(onHttpRequestHeaders),
		wrapper.ProcessRequestBody(onHttpRequestBody),
		wrapper.ProcessResponseHeaders(onHttpResponseHeaders),
		wrapper.ProcessStreamDone(onHttpStreamDone),
	)
}

func parseConfig(json gjson.Result, config *Config) error {
	config.policy = Policy(json.Get("policy").String())
	switch config.policy {
	case "":
		config.policy = PolicyLeastWaiting
	case PolicyHeader, PolicyRoundRobin, PolicyLeastWaiting:
	default:
		return fmt.Errorf("unknown policy %q, expected header|round_robin|least_waiting", config.policy)
	}
	config.targetHeader = json.Get("targetHeader").String()
	if config.targetHeader == "" {
		config.targetHeader = "x-maas-target"
	}
	config.debugHeaders = !json.Get("debugHeaders").Exists() || json.Get("debugHeaders").Bool()
	config.debugPath = "/maas/debug/hosts"
	if path := json.Get("debugPath"); path.Exists() {
		config.debugPath = path.String()
	}
	config.metrics = newPluginMetrics()
	return nil
}

func onHttpRequestHeaders(ctx wrapper.HttpContext, config Config) types.Action {
	if config.debugPath != "" && wrapper.GetRequestPathWithoutQuery() == config.debugPath {
		ctx.DontReadRequestBody()
		serveDebug(config)
		return types.ActionContinue
	}
	if target, _ := proxywasm.GetHttpRequestHeader(config.targetHeader); target != "" {
		ctx.DontReadRequestBody()
		_ = proxywasm.RemoveHttpRequestHeader(config.targetHeader)
		pickTarget(ctx, config, target)
		return types.ActionContinue
	}
	if config.policy == PolicyHeader {
		ctx.DontReadRequestBody()
		return types.ActionContinue
	}
	if !hasPositiveContentLength() {
		ctx.DontReadRequestBody()
		pickByPolicy(ctx, config)
		return types.ActionContinue
	}
	// 有 body 的请求在 body 阶段再做决策（后续阶段要按 prompt 前缀选副本）。
	// 这里必须挂起请求头：若直接 Continue，router 会在拿到 body 之前就选定上游主机，override 不再生效。
	ctx.SetRequestBodyBufferLimit(maxRequestBodyBytes)
	return types.HeaderStopIteration
}

// hasPositiveContentLength 判断是否值得等待请求体。只认正数 Content-Length（与 ai-endpoint-picker 一致）：
// wrapper.HasRequestBody() 在只有 Content-Type 时也返回 true，而没有 body 的请求一旦挂起请求头，
// Envoy 不会再回调 body 阶段，请求会一直卡住。
func hasPositiveContentLength() bool {
	value, err := proxywasm.GetHttpRequestHeader("content-length")
	if err != nil {
		return false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return err == nil && n > 0
}

func onHttpRequestBody(ctx wrapper.HttpContext, config Config, _ []byte) types.Action {
	pickByPolicy(ctx, config)
	return types.ActionContinue
}

func onHttpResponseHeaders(ctx wrapper.HttpContext, config Config) types.Action {
	if config.debugHeaders {
		if picked, ok := ctx.GetContext(ctxPickedKey).(string); ok {
			_ = proxywasm.AddHttpResponseHeader("x-maas-picked", picked)
		}
		if reason, ok := ctx.GetContext(ctxReasonKey).(string); ok {
			_ = proxywasm.AddHttpResponseHeader("x-maas-reason", reason)
		}
	}
	ctx.DontReadResponseBody()
	return types.ActionContinue
}

func onHttpStreamDone(ctx wrapper.HttpContext, _ Config) {
	if picked, ok := ctx.GetContext(ctxPickedKey).(string); ok {
		if inflight[picked] <= 1 {
			delete(inflight, picked)
		} else {
			inflight[picked]--
		}
	}
}

func loadHosts() ([]HostSnapshot, error) {
	raw, err := proxywasm.GetUpstreamHosts()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, errors.New("upstream cluster has no hosts")
	}
	return ParseHosts(raw), nil
}

func pickTarget(ctx wrapper.HttpContext, config Config, target string) {
	hosts, err := loadHosts()
	if err != nil {
		fallback(ctx, config, reasonHostsError)
		return
	}
	for _, host := range hosts {
		if host.Address == target {
			override(ctx, config, target, reasonTarget)
			return
		}
	}
	fallback(ctx, config, reasonTargetNotFound)
}

func pickByPolicy(ctx wrapper.HttpContext, config Config) {
	hosts, err := loadHosts()
	if err != nil {
		fallback(ctx, config, reasonHostsError)
		return
	}
	var picked string
	var ok bool
	switch config.policy {
	case PolicyRoundRobin:
		picked, ok = PickRoundRobin(hosts, &rrCounter)
	default:
		picked, ok = PickLeastWaiting(hosts, inflight, random)
	}
	if !ok {
		fallback(ctx, config, reasonNoHealthyHost)
		return
	}
	override(ctx, config, picked, string(config.policy))
}

func override(ctx wrapper.HttpContext, config Config, address, reason string) {
	if err := proxywasm.SetUpstreamOverrideHost([]byte(address)); err != nil {
		fallback(ctx, config, reasonOverrideFailed)
		return
	}
	inflight[address]++
	ctx.SetContext(ctxPickedKey, address)
	ctx.SetContext(ctxReasonKey, reason)
	config.metrics.decisions.Increment(1)
}

// fallback 不做 override，由 Envoy cluster 自身的负载均衡兜底，保证调度插件失效时请求仍可用。
func fallback(ctx wrapper.HttpContext, config Config, reason string) {
	ctx.SetContext(ctxReasonKey, reason)
	config.metrics.fallbacks.Increment(1)
	proxywasm.LogDebugf("[%s] fallback to envoy load balancing: %s", pluginName, reason)
}

type debugResponse struct {
	Policy   Policy         `json:"policy"`
	Hosts    []HostSnapshot `json:"hosts"`
	Inflight map[string]int `json:"gateway_inflight"`
	Error    string         `json:"error,omitempty"`
}

func serveDebug(config Config) {
	resp := debugResponse{Policy: config.policy, Inflight: inflight}
	hosts, err := loadHosts()
	if err != nil {
		resp.Error = err.Error()
	}
	resp.Hosts = hosts
	body, _ := json.Marshal(resp)
	_ = proxywasm.SendHttpResponseWithDetail(200, pluginName+".debug", [][2]string{{"content-type", "application/json"}}, body, -1)
}

type pluginMetrics struct {
	decisions proxywasm.MetricCounter
	fallbacks proxywasm.MetricCounter
}

func newPluginMetrics() *pluginMetrics {
	return &pluginMetrics{
		decisions: proxywasm.DefineCounterMetric("maas_scheduler_decisions_total"),
		fallbacks: proxywasm.DefineCounterMetric("maas_scheduler_fallback_total"),
	}
}
