// maas-cache：MaaS 网关的多级响应缓存插件（阶段 1 精确匹配 + 阶段 3 语义缓存）。
//
// 请求路径：
//  1. 只处理配置的 OpenAI 兼容接口（默认 /v1/chat/completions、/v1/completions）的 POST 请求；
//  2. 读取请求体，过滤不可缓存的请求（n>1、logprobs、temperature 超过上限等），
//     对"规范化后的完整请求"计算缓存键（见 key.go）；
//  3. 依次查询 L1（本 worker 内的 W-TinyLFU，见 l1 包）、L2（Redis）与 L3（语义匹配，见 semantic.go）。
//     命中则按 vLLM 的响应格式直接回放（见 entry.go），请求不再经过分发、调度插件和推理后端；
//  4. 未命中则放行。响应在原样透传给客户端的同时被收集，完整且成功结束时写入 L1 与 L2，
//     并把问题向量登记到向量库。
//
// Redis、embedder、向量库不可用，请求或响应不符合缓存条件时一律放行，缓存故障不会影响请求本身。
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
	"github.com/tidwall/resp"
)

const (
	pluginName  = "maas-cache"
	ctxStateKey = "maas_cache_state"
	debugPath   = "/maas/debug/cache"

	statusPending = "pending"
	statusMiss    = "miss"
	statusBypass  = "bypass"
	statusL1      = "L1"
	statusL2      = "L2"
	statusL3      = "L3" // 语义命中
)

// requestState 记录单个请求在缓存插件中的处理状态。
type requestState struct {
	kind         string
	key          string
	stream       bool
	includeUsage bool
	status       string // pending | miss | bypass | skip:<原因> | L1 | L2 | L3
	collector    *collector
	startedAt    time.Time
	// L3 语义缓存
	question       string
	ctxKey         string
	embedding      string // 问题向量（JSON 数组），未命中时在写缓存后登记到向量库
	semanticScore  float64
	semanticReject string // 相似度达标、但没通过要素校验的原因（number / negation / term）
	skipSemantic   bool
}

func (s *requestState) hit() bool {
	return s.status == statusL1 || s.status == statusL2 || s.status == statusL3
}

var idSource = rand.New(rand.NewSource(time.Now().UnixNano()))

func main() {}

func init() {
	wrapper.SetCtx(
		pluginName,
		wrapper.ParseConfig(parseConfig),
		wrapper.ProcessRequestHeaders(onHttpRequestHeaders),
		wrapper.ProcessRequestBody(onHttpRequestBody),
		wrapper.ProcessResponseHeaders(onHttpResponseHeaders),
		wrapper.ProcessStreamingResponseBody(onHttpStreamingResponseBody),
	)
}

func onHttpRequestHeaders(ctx wrapper.HttpContext, config Config) types.Action {
	path := wrapper.GetRequestPathWithoutQuery()
	if path == debugPath {
		ctx.DontReadRequestBody()
		serveDebug(config)
		return types.ActionContinue
	}
	kind, ok := config.paths[path]
	if !ok || wrapper.GetRequestMethod() != "POST" {
		ctx.DontReadRequestBody()
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	state := &requestState{kind: kind, status: statusPending, startedAt: time.Now()}
	ctx.SetContext(ctxStateKey, state)
	if bypassRequested(config.bypassHeaders) {
		state.status = statusBypass
		config.metrics.bypass.Increment(1)
		ctx.DontReadRequestBody()
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	// 只跳过 L3（仍查写 L1/L2），用于消融实验
	state.skipSemantic = config.l3 != nil && bypassRequested(config.l3.bypassHeaders)
	if length := contentLength(); length <= 0 || length > config.maxRequestBodyBytes {
		ctx.DontReadRequestBody()
		skip(ctx, config, state, "body_size")
		return types.ActionContinue
	}
	// 上游响应必须是未压缩的，才能被收集并写入缓存。
	_ = proxywasm.RemoveHttpRequestHeader("accept-encoding")
	ctx.SetRequestBodyBufferLimit(uint32(config.maxRequestBodyBytes))
	// 命中缓存时由本插件直接回复，因此在查完缓存之前不能把请求交给后续插件和上游。
	return types.HeaderStopIteration
}

func onHttpRequestBody(ctx wrapper.HttpContext, config Config, body []byte) types.Action {
	state, _ := ctx.GetContext(ctxStateKey).(*requestState)
	if state == nil || state.status != statusPending {
		return types.ActionContinue
	}
	if !gjson.ValidBytes(body) {
		skip(ctx, config, state, "invalid_json")
		return types.ActionContinue
	}
	req := gjson.ParseBytes(body)
	if reason := uncacheable(req, state.kind, config); reason != "" {
		skip(ctx, config, state, reason)
		return types.ActionContinue
	}
	key, err := BuildKey(config.namespace, state.kind, varyValues(config), req, config.ignoredFields)
	if err != nil {
		skip(ctx, config, state, "key_error")
		return types.ActionContinue
	}
	state.key = key
	state.stream = req.Get("stream").Bool()
	state.includeUsage = req.Get("stream_options.include_usage").Bool()
	config.metrics.lookups.Increment(1)
	question, ctxKey, semantic := semanticCandidate(config, state.kind, body, req)
	semantic = semantic && !state.skipSemantic
	if semantic {
		state.question, state.ctxKey = question, ctxKey
	}

	if config.l1 != nil {
		if entry, ok := config.l1.Get(key); ok {
			replay(config, state, entry, statusL1)
			return types.ActionPause
		}
	}
	if config.l2 == nil {
		miss(config, state)
		return types.ActionContinue
	}
	err = config.l2.client.Get(config.keyPrefix+key, func(value resp.Value) {
		if entry := decodeEntry(config, value); entry != nil {
			if config.l1 != nil && entry.sizeBytes() <= config.l1MaxEntryBytes {
				config.l1.Put(key, entry)
			}
			replay(config, state, entry, statusL2)
			return
		}
		// 精确匹配未命中：尝试 L3 语义匹配（异步链结束时自行回放或放行）
		if semantic && lookupSemantic(config, state) {
			return
		}
		miss(config, state)
		_ = proxywasm.ResumeHttpRequest()
	})
	if err != nil { // Redis 尚未就绪或调用失败：直接放行
		config.metrics.l2Errors.Increment(1)
		miss(config, state)
		return types.ActionContinue
	}
	return types.ActionPause
}

func onHttpResponseHeaders(ctx wrapper.HttpContext, config Config) types.Action {
	state, _ := ctx.GetContext(ctxStateKey).(*requestState)
	if state == nil || state.hit() { // 命中缓存时的本地回复也会经过这里
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	if config.debugHeaders {
		_ = proxywasm.AddHttpResponseHeader("x-maas-cache", state.status)
		if state.semanticReject != "" {
			_ = proxywasm.AddHttpResponseHeader("x-maas-semantic-reject", state.semanticReject)
			_ = proxywasm.AddHttpResponseHeader("x-maas-semantic-score", strconv.FormatFloat(state.semanticScore, 'f', 4, 64))
		}
	}
	if state.status != statusMiss {
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	if status, _ := proxywasm.GetHttpResponseHeader(":status"); status != "200" {
		ctx.DontReadResponseBody()
		config.metrics.storeSkipped.Increment(1)
		return types.ActionContinue
	}
	contentType, _ := proxywasm.GetHttpResponseHeader("content-type")
	switch {
	case strings.Contains(contentType, "text/event-stream"):
		state.collector = newCollector(state.kind, true, config.maxResponseBytes)
	case strings.Contains(contentType, "application/json"):
		state.collector = newCollector(state.kind, false, config.maxResponseBytes)
	default:
		ctx.DontReadResponseBody()
		config.metrics.storeSkipped.Increment(1)
	}
	return types.ActionContinue
}

// onHttpStreamingResponseBody 不修改、不缓冲响应：分块原样返回给客户端，只在旁路收集内容。
func onHttpStreamingResponseBody(ctx wrapper.HttpContext, config Config, chunk []byte, isLastChunk bool) []byte {
	state, _ := ctx.GetContext(ctxStateKey).(*requestState)
	if state == nil || state.collector == nil {
		return chunk
	}
	state.collector.Feed(chunk)
	if isLastChunk {
		entry, reason := state.collector.Finish(config.finishReasons)
		state.collector = nil
		if entry == nil {
			config.metrics.storeSkipped.Increment(1)
			proxywasm.LogDebugf("[%s] response not cached: %s", pluginName, reason)
		} else {
			store(config, state.key, entry)
			if state.embedding != "" {
				upsertSemantic(config, state)
			}
		}
	}
	return chunk
}

// uncacheable 返回请求不可缓存的原因，空字符串表示可以缓存。
func uncacheable(req gjson.Result, kind string, config Config) string {
	if !req.IsObject() {
		return "invalid_json"
	}
	if n := req.Get("n"); n.Exists() && n.Type != gjson.Null && n.Int() != 1 {
		return "n"
	}
	if lp := req.Get("logprobs"); lp.Type == gjson.True || (kind == kindCompletion && lp.Type == gjson.Number) {
		return "logprobs"
	}
	if req.Get("top_logprobs").Exists() && req.Get("top_logprobs").Type != gjson.Null {
		return "logprobs"
	}
	if bestOf := req.Get("best_of"); bestOf.Exists() && bestOf.Int() > 1 {
		return "best_of"
	}
	if req.Get("echo").Bool() {
		return "echo"
	}
	if config.maxTemperature >= 0 {
		temperature := 1.0 // 未指定时按 OpenAI 默认值 1.0 处理
		if t := req.Get("temperature"); t.Type == gjson.Number {
			temperature = t.Float()
		}
		if temperature > config.maxTemperature {
			return "temperature"
		}
	}
	if kind == kindChat && !req.Get("messages").IsArray() {
		return "no_messages"
	}
	if kind == kindCompletion && !req.Get("prompt").Exists() {
		return "no_prompt"
	}
	return ""
}

func replay(config Config, state *requestState, entry *Entry, level string) {
	state.status = level
	body, contentType := Render(entry, renderOptions{
		id:           newResponseID(entry.Kind),
		created:      time.Now().Unix(),
		stream:       state.stream,
		includeUsage: state.includeUsage,
	})
	headers := [][2]string{{"content-type", contentType}}
	if config.debugHeaders {
		headers = append(headers, [2]string{"x-maas-cache", level})
		if level == statusL3 {
			headers = append(headers, [2]string{"x-maas-semantic-score", strconv.FormatFloat(state.semanticScore, 'f', 4, 64)})
		}
	}
	switch level {
	case statusL1:
		config.metrics.hitL1.Increment(1)
	case statusL2:
		config.metrics.hitL2.Increment(1)
	case statusL3:
		config.metrics.hitL3.Increment(1)
	}
	config.metrics.hitLatencyMs.Record(uint64(time.Since(state.startedAt).Milliseconds()))
	_ = proxywasm.SendHttpResponseWithDetail(200, pluginName+".hit", headers, body, -1)
}

func store(config Config, key string, entry *Entry) {
	config.metrics.stores.Increment(1)
	if config.l1 != nil && entry.sizeBytes() <= config.l1MaxEntryBytes {
		config.l1.Put(key, entry)
	}
	if config.l2 == nil {
		return
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	onWritten := func(value resp.Value) {
		if value.Error() != nil {
			config.metrics.l2Errors.Increment(1)
		}
	}
	if config.l2.ttl > 0 {
		err = config.l2.client.SetEx(config.keyPrefix+key, string(data), config.l2.ttl, onWritten)
	} else {
		err = config.l2.client.Set(config.keyPrefix+key, string(data), onWritten)
	}
	if err != nil {
		config.metrics.l2Errors.Increment(1)
	}
}

// decodeEntry 解析 Redis 返回值；不存在、出错或版本不兼容都视为未命中。
func decodeEntry(config Config, value resp.Value) *Entry {
	if value.Error() != nil {
		config.metrics.l2Errors.Increment(1)
		return nil
	}
	if value.IsNull() {
		return nil
	}
	var entry Entry
	if err := json.Unmarshal(value.Bytes(), &entry); err != nil || entry.Version != entryVersion {
		return nil
	}
	return &entry
}

func miss(config Config, state *requestState) {
	state.status = statusMiss
	config.metrics.misses.Increment(1)
}

func skip(ctx wrapper.HttpContext, config Config, state *requestState, reason string) {
	state.status = "skip:" + reason
	config.metrics.skipped.Increment(1)
	ctx.DontReadResponseBody()
}

func bypassRequested(headers []string) bool {
	for _, name := range headers {
		value, _ := proxywasm.GetHttpRequestHeader(name)
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "on", "true", "1":
			return true
		}
	}
	return false
}

func varyValues(config Config) []string {
	values := make([]string, 0, len(config.varyHeaders))
	for _, name := range config.varyHeaders {
		value, _ := proxywasm.GetHttpRequestHeader(name)
		values = append(values, name+"="+value)
	}
	return values
}

func contentLength() int64 {
	value, err := proxywasm.GetHttpRequestHeader("content-length")
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func newResponseID(kind string) string {
	prefix := "chatcmpl-"
	if kind == kindCompletion {
		prefix = "cmpl-"
	}
	return fmt.Sprintf("%s%016x%016x", prefix, idSource.Uint64(), idSource.Uint64())
}

type debugResponse struct {
	L1Enabled bool   `json:"l1_enabled"`
	Policy    string `json:"policy,omitempty"`
	Entries   int    `json:"entries"`
	Stats     any    `json:"stats,omitempty"`
	L2Enabled bool   `json:"l2_enabled"`
	L3Enabled bool   `json:"l3_enabled"`
	Note      string `json:"note"`
}

// serveDebug 返回处理本请求的 worker 的 L1 状态（每个 Envoy worker 各有一份 L1）。
func serveDebug(config Config) {
	out := debugResponse{L2Enabled: config.l2 != nil, L3Enabled: config.l3 != nil, Note: "L1 statistics belong to the Envoy worker that served this request"}
	if config.l1 != nil {
		out.L1Enabled = true
		out.Policy = string(config.l1.Policy())
		out.Entries = config.l1.Len()
		out.Stats = config.l1.Stats()
	}
	body, _ := json.Marshal(out)
	_ = proxywasm.SendHttpResponseWithDetail(200, pluginName+".debug", [][2]string{{"content-type", "application/json"}}, body, -1)
}

type cacheMetrics struct {
	lookups      proxywasm.MetricCounter
	hitL1        proxywasm.MetricCounter
	hitL2        proxywasm.MetricCounter
	hitL3        proxywasm.MetricCounter
	misses       proxywasm.MetricCounter
	bypass       proxywasm.MetricCounter
	skipped      proxywasm.MetricCounter
	stores       proxywasm.MetricCounter
	storeSkipped proxywasm.MetricCounter
	l2Errors     proxywasm.MetricCounter
	l3Lookups    proxywasm.MetricCounter
	l3Stores     proxywasm.MetricCounter
	l3Errors     proxywasm.MetricCounter
	l3Rejects    proxywasm.MetricCounter   // 相似度达标但未通过要素校验的候选数
	l3Stale      proxywasm.MetricCounter   // 向量库里有、L2 中已过期的候选数（随即删除）
	l3LookupMs   proxywasm.MetricHistogram // 向量化 + 检索耗时：语义缓存给未命中请求增加的首 token 延迟
	hitLatencyMs proxywasm.MetricHistogram // 命中请求从收到请求头到回放完成的耗时（即命中时的首 token 延迟）
}

func newCacheMetrics() *cacheMetrics {
	return &cacheMetrics{
		lookups:      proxywasm.DefineCounterMetric("maas_cache_lookups_total"),
		hitL1:        proxywasm.DefineCounterMetric("maas_cache_hits_l1_total"),
		hitL2:        proxywasm.DefineCounterMetric("maas_cache_hits_l2_total"),
		hitL3:        proxywasm.DefineCounterMetric("maas_cache_hits_l3_total"),
		misses:       proxywasm.DefineCounterMetric("maas_cache_misses_total"),
		bypass:       proxywasm.DefineCounterMetric("maas_cache_bypass_total"),
		skipped:      proxywasm.DefineCounterMetric("maas_cache_skipped_total"),
		stores:       proxywasm.DefineCounterMetric("maas_cache_stores_total"),
		storeSkipped: proxywasm.DefineCounterMetric("maas_cache_store_skipped_total"),
		l2Errors:     proxywasm.DefineCounterMetric("maas_cache_l2_errors_total"),
		l3Lookups:    proxywasm.DefineCounterMetric("maas_cache_l3_lookups_total"),
		l3Stores:     proxywasm.DefineCounterMetric("maas_cache_l3_stores_total"),
		l3Errors:     proxywasm.DefineCounterMetric("maas_cache_l3_errors_total"),
		l3Rejects:    proxywasm.DefineCounterMetric("maas_cache_l3_rejects_total"),
		l3Stale:      proxywasm.DefineCounterMetric("maas_cache_l3_stale_total"),
		l3LookupMs:   proxywasm.DefineHistogramMetric("maas_cache_l3_lookup_ms"),
		hitLatencyMs: proxywasm.DefineHistogramMetric("maas_cache_hit_latency_ms"),
	}
}
