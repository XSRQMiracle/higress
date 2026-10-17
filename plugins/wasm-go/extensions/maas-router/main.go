// maas-router：MaaS 网关的语义分发插件（阶段 3）。
//
// 请求的 model 为 autoModel（默认 higress/auto）时，按问题语义为它选择模型池：
//  1. 长上下文（估算 prompt token ≥ longPromptTokens）→ longPromptPool（默认就是 defaultPool）；
//  2. 正则规则命中（如包含"代码""推导"）→ 规则指定的池；
//  3. 语义分类：问题向量与各类别样例质心的余弦相似度，取最相近且 ≥ minSimilarity 的类别所属的池。
//     问题向量优先复用 maas-cache 在 L3 查找时已经算好的结果（Envoy filter state），没有才调用 embedder；
//  4. 都不适用 → defaultPool。
//
// 选定后把请求体中的 model 改写为该池的模型，并写入 poolHeader；Higress 在请求头变化后重新匹配路由，
// 请求被送往对应的 upstream cluster，后面的 maas-scheduler 再在该池的副本之间调度。
// 显式指定已知模型的请求只写入 poolHeader，不做改写（多模型直接接入）。
// 另外按语义类别统计输出 token 数（EWMA），通过 x-maas-predicted-output-tokens 交给 maas-scheduler，
// 用于显存水位准入时估计请求的 KV 峰值占用。
package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	pluginName             = "maas-router"
	ctxStateKey            = "maas_router_state"
	debugPath              = "/maas/debug/router"
	predictedOutputHeader  = "x-maas-predicted-output-tokens"
	queryEmbeddingProperty = "maas_query_embedding" // maas-cache 写入的问题向量
	maxRequestBodyBytes    = 16 << 20
	ewmaAlpha              = 0.1
	tailKeepBytes          = 1024
	maxEmbedChars          = 512
)

type requestState struct {
	pool       string
	class      string
	reason     string
	similarity float64
	tail       []byte
}

func main() {}

func init() {
	wrapper.SetCtx(
		pluginName,
		wrapper.ParseConfig(parseConfig),
		wrapper.ProcessRequestHeaders(onHttpRequestHeaders),
		wrapper.ProcessRequestBody(onHttpRequestBody),
		wrapper.ProcessResponseHeaders(onHttpResponseHeaders),
		wrapper.ProcessStreamingResponseBody(onHttpStreamingResponseBody),
		wrapper.ProcessStreamDone(onHttpStreamDone),
	)
}

func onHttpRequestHeaders(ctx wrapper.HttpContext, config Config) types.Action {
	path := wrapper.GetRequestPathWithoutQuery()
	if path == debugPath {
		ctx.DontReadRequestBody()
		serveDebug(config)
		return types.ActionContinue
	}
	if wrapper.GetRequestMethod() != "POST" || !strings.HasSuffix(path, "/completions") || !hasPositiveContentLength() {
		ctx.DontReadRequestBody()
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	ctx.SetRequestBodyBufferLimit(maxRequestBodyBytes)
	// 要根据请求体决定路由，必须在拿到请求体之前挂起请求头
	return types.HeaderStopIteration
}

func onHttpRequestBody(ctx wrapper.HttpContext, config Config, body []byte) types.Action {
	req := gjson.ParseBytes(body)
	model := req.Get("model").String()
	st := &requestState{}
	ctx.SetContext(ctxStateKey, st)
	if name, ok := config.modelToPool[model]; ok {
		config.metrics.explicit.Increment(1)
		route(config, st, body, config.pools[name], "model", false)
		return types.ActionContinue
	}
	if model != config.autoModel {
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	config.metrics.auto.Increment(1)
	question, prompt := requestText(req)
	if estimateTokens(prompt) >= config.longPromptTokens {
		route(config, st, body, config.pools[config.longPromptPool], "long_prompt", true)
		return types.ActionContinue
	}
	for _, rule := range config.rules {
		if rule.pattern.MatchString(question) {
			route(config, st, body, config.pools[rule.Pool], "rule", true)
			return types.ActionContinue
		}
	}
	if len(config.classes) == 0 || question == "" {
		route(config, st, body, config.pools[config.defaultPool], "default", true)
		return types.ActionContinue
	}
	if !config.state.centroidsReady {
		if config.state.initializing {
			route(config, st, body, config.pools[config.defaultPool], "warming", true)
			return types.ActionContinue
		}
		// 首个需要语义分类的请求：批量向量化所有样例、算出质心，再继续分类本请求
		config.state.initializing = true
		err := initCentroids(config, func(ok bool) {
			config.state.initializing = false
			if !ok {
				route(config, st, body, config.pools[config.defaultPool], "warming", true)
				_ = proxywasm.ResumeHttpRequest()
				return
			}
			semanticRoute(config, st, body, question, true)
		})
		if err != nil {
			config.state.initializing = false
			route(config, st, body, config.pools[config.defaultPool], "warming", true)
			return types.ActionContinue
		}
		return types.ActionPause
	}
	if semanticRoute(config, st, body, question, false) {
		return types.ActionPause
	}
	return types.ActionContinue
}

// requestText 返回用于分类的问题（最后一条 user 消息）与用于长度判断的全部 prompt 文本。
func requestText(req gjson.Result) (question, prompt string) {
	if p := req.Get("prompt"); p.Exists() {
		text := p.String()
		if p.IsArray() {
			text = p.Get("0").String()
		}
		return text, text
	}
	var all strings.Builder
	for _, m := range req.Get("messages").Array() {
		text := messageText(m)
		all.WriteString(text)
		if m.Get("role").String() == "user" {
			question = text
		}
	}
	return question, all.String()
}

func messageText(message gjson.Result) string {
	content := message.Get("content")
	if !content.IsArray() {
		return content.String()
	}
	var b strings.Builder
	content.ForEach(func(_, part gjson.Result) bool {
		if part.Get("type").String() == "text" {
			b.WriteString(part.Get("text").String())
		}
		return true
	})
	return b.String()
}

// semanticRoute 对问题做语义分类并路由。返回 true 表示发起了异步向量化（请求保持挂起，回调中恢复）；
// fromCallback 为 true 时调用方已挂起请求，同步完成后需要在这里恢复。
func semanticRoute(config Config, st *requestState, body []byte, question string, fromCallback bool) bool {
	if data, err := proxywasm.GetProperty([]string{queryEmbeddingProperty}); err == nil && len(data) > 0 {
		if vec := parseVector(gjson.ParseBytes(data)); len(vec) > 0 {
			config.metrics.reused.Increment(1)
			applyClass(config, st, body, vec)
			if fromCallback {
				_ = proxywasm.ResumeHttpRequest()
			}
			return false
		}
	}
	if len([]rune(question)) > maxEmbedChars {
		question = string([]rune(question)[:maxEmbedChars])
	}
	payload := `{"model":` + strconv.Quote(config.embedModel) + `,"input":` + jsonString(question) + `}`
	err := config.embed.Post(config.embedPath, [][2]string{{"content-type", "application/json"}}, []byte(payload),
		func(status int, _ http.Header, resp []byte) {
			vec := parseVector(gjson.GetBytes(resp, "data.0.embedding"))
			if status != 200 || len(vec) == 0 {
				config.metrics.embedErrors.Increment(1)
				route(config, st, body, config.pools[config.defaultPool], "embed_error", true)
			} else {
				applyClass(config, st, body, vec)
			}
			_ = proxywasm.ResumeHttpRequest()
		}, config.embedTimeout)
	if err != nil {
		config.metrics.embedErrors.Increment(1)
		route(config, st, body, config.pools[config.defaultPool], "embed_error", true)
		if fromCallback {
			_ = proxywasm.ResumeHttpRequest()
		}
		return false
	}
	return true
}

func applyClass(config Config, st *requestState, body []byte, vec []float64) {
	class, sim := classify(vec, config.classes, config.minSimilarity)
	st.similarity = sim
	if class == nil {
		route(config, st, body, config.pools[config.defaultPool], "semantic_none", true)
		return
	}
	st.class = class.Name
	route(config, st, body, config.pools[class.Pool], "semantic", true)
}

// route 把请求送往模型池：rewrite 时改写请求体中的 model 并同步 Content-Length，
// 然后写入 poolHeader（请求头变化后 Higress 会重新匹配路由）。
func route(config Config, st *requestState, body []byte, pool Pool, reason string, rewrite bool) {
	st.pool, st.reason = pool.Name, reason
	if rewrite {
		if newBody, err := sjson.SetBytes(body, "model", pool.Model); err == nil {
			_ = proxywasm.ReplaceHttpRequestBody(newBody)
			// 后面的 maas-scheduler 依赖正数 Content-Length 判断是否读取请求体，这里更新而不是删除
			_ = proxywasm.ReplaceHttpRequestHeader("content-length", strconv.Itoa(len(newBody)))
		}
	}
	_ = proxywasm.ReplaceHttpRequestHeader(config.poolHeader, pool.Name)
	if st.class != "" {
		if v, ok := config.state.outputEWMA[st.class]; ok {
			_ = proxywasm.ReplaceHttpRequestHeader(predictedOutputHeader, strconv.Itoa(int(v+0.5)))
		}
	}
	if counter, ok := config.metrics.pools[pool.Name]; ok {
		counter.Increment(1)
	}
	switch reason {
	case "semantic":
		config.metrics.semantic.Increment(1)
	case "rule":
		config.metrics.rule.Increment(1)
	case "long_prompt":
		config.metrics.longPrompt.Increment(1)
	case "model":
	default:
		config.metrics.fallback.Increment(1)
	}
}

// initCentroids 批量向量化所有类别的样例，计算每个类别的质心。
func initCentroids(config Config, done func(ok bool)) error {
	var inputs []string
	var owners []*Class
	for _, c := range config.classes {
		for _, e := range c.Examples {
			inputs = append(inputs, e)
			owners = append(owners, c)
		}
	}
	payload, _ := json.Marshal(map[string]any{"model": config.embedModel, "input": inputs})
	return config.embed.Post(config.embedPath, [][2]string{{"content-type", "application/json"}}, payload,
		func(status int, _ http.Header, resp []byte) {
			data := gjson.GetBytes(resp, "data").Array()
			if status != 200 || len(data) != len(inputs) {
				config.metrics.embedErrors.Increment(1)
				done(false)
				return
			}
			vectors := map[*Class][][]float64{}
			for _, item := range data {
				i := int(item.Get("index").Int())
				if i >= 0 && i < len(owners) {
					vectors[owners[i]] = append(vectors[owners[i]], parseVector(item.Get("embedding")))
				}
			}
			for _, c := range config.classes {
				c.centroid = centroid(vectors[c])
			}
			config.state.centroidsReady = true
			done(true)
		}, config.embedTimeout*4)
}

func onHttpResponseHeaders(ctx wrapper.HttpContext, config Config) types.Action {
	st, _ := ctx.GetContext(ctxStateKey).(*requestState)
	if st == nil || st.pool == "" {
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	if config.debugHeaders {
		_ = proxywasm.AddHttpResponseHeader("x-maas-route-pool", st.pool)
		_ = proxywasm.AddHttpResponseHeader("x-maas-route-reason", st.reason)
		if st.class != "" {
			_ = proxywasm.AddHttpResponseHeader("x-maas-route-class", st.class)
		}
		if st.similarity != 0 {
			_ = proxywasm.AddHttpResponseHeader("x-maas-route-similarity", strconv.FormatFloat(st.similarity, 'f', 4, 64))
		}
	}
	contentType, _ := proxywasm.GetHttpResponseHeader("content-type")
	if st.class == "" || !strings.Contains(contentType, "text/event-stream") {
		ctx.DontReadResponseBody()
	}
	return types.ActionContinue
}

func onHttpStreamingResponseBody(ctx wrapper.HttpContext, _ Config, chunk []byte, _ bool) []byte {
	if st, _ := ctx.GetContext(ctxStateKey).(*requestState); st != nil && st.class != "" {
		st.tail = append(st.tail, chunk...)
		if len(st.tail) > tailKeepBytes {
			st.tail = st.tail[len(st.tail)-tailKeepBytes:]
		}
	}
	return chunk
}

// onHttpStreamDone 用响应 usage 中的 completion_tokens 更新该类别的输出长度估计。
func onHttpStreamDone(ctx wrapper.HttpContext, config Config) {
	st, _ := ctx.GetContext(ctxStateKey).(*requestState)
	if st == nil || st.class == "" {
		return
	}
	i := bytes.LastIndex(st.tail, []byte(`"completion_tokens":`))
	if i < 0 {
		return
	}
	rest := st.tail[i+len(`"completion_tokens":`):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	tokens, err := strconv.Atoi(string(rest[:end]))
	if err != nil || tokens <= 0 {
		return
	}
	if old, ok := config.state.outputEWMA[st.class]; ok {
		config.state.outputEWMA[st.class] = (1-ewmaAlpha)*old + ewmaAlpha*float64(tokens)
	} else {
		config.state.outputEWMA[st.class] = float64(tokens)
	}
}

func hasPositiveContentLength() bool {
	value, err := proxywasm.GetHttpRequestHeader("content-length")
	if err != nil {
		return false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return err == nil && n > 0
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

type classDebug struct {
	Name          string  `json:"name"`
	Pool          string  `json:"pool"`
	Examples      int     `json:"examples"`
	Ready         bool    `json:"ready"`
	OutputTokensE float64 `json:"output_tokens_ewma,omitempty"`
}

// serveDebug 返回模型池、类别与处理该请求的 worker 的分类器状态。
func serveDebug(config Config) {
	out := map[string]any{
		"auto_model":      config.autoModel,
		"default_pool":    config.defaultPool,
		"pools":           config.pools,
		"centroids_ready": config.state.centroidsReady,
	}
	var classes []classDebug
	for _, c := range config.classes {
		classes = append(classes, classDebug{Name: c.Name, Pool: c.Pool, Examples: len(c.Examples), Ready: c.centroid != nil, OutputTokensE: config.state.outputEWMA[c.Name]})
	}
	out["classes"] = classes
	body, _ := json.Marshal(out)
	_ = proxywasm.SendHttpResponseWithDetail(200, pluginName+".debug", [][2]string{{"content-type", "application/json"}}, body, -1)
}
