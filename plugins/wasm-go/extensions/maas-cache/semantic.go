package main

// L3 语义缓存。
//
// 精确匹配（L1/L2）未命中时，把最后一个问题向量化，在向量库中查找"上下文完全相同、只是问题措辞不同"
// 的已缓存请求。相似度达到阈值的候选还要通过关键要素校验（数字、否定、关键词一致，见 guard.go），
// 才回放那条缓存，并以当前请求的精确键写回 L1/L2（同样的措辞下次直接精确命中）。
//
// 只比较问题本身是不够的：同一句"这款产品风险大吗"在不同的 system prompt、对话历史、采样参数下答案不同。
// 所以每个向量都带一个上下文键 ctx = 去掉最后一个问题后的规范化请求的哈希，检索时按 ctx 过滤。
// 向量库只保存"向量 → L2 精确键"的映射，L2 条目过期或被淘汰后，对应的语义命中自然失效（检索到时删除该点）。

import (
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
	"github.com/tidwall/resp"
	"github.com/tidwall/sjson"
)

// queryEmbeddingProperty 把本请求问题的向量（JSON 数组）放进 Envoy filter state，
// 同一请求上的 maas-router 可以直接复用，不必再向量化一次。
const queryEmbeddingProperty = "maas_query_embedding"

type semanticConfig struct {
	embed            *wrapper.ClusterClient[wrapper.FQDNCluster]
	embedPath        string
	embedModel       string
	embedTimeout     uint32
	vector           *wrapper.ClusterClient[wrapper.FQDNCluster]
	collection       string
	vectorTimeout    uint32
	threshold        float64
	candidates       int // 每次检索取回的候选数，按相似度从高到低逐个做要素校验
	maxQuestionChars int
	dims             int      // 向量维度，创建集合时使用（需与 embedding 模型一致）
	guard            *guard   // 关键要素校验，nil 表示只看相似度
	bypassHeaders    []string // 值为 on/true/1 时只跳过 L3
	collectionReady  bool     // 本 VM 已确认集合存在
	ensuring         bool
}

var jsonHeaders = [][2]string{{"content-type", "application/json"}}

func lastMessageText(message gjson.Result) string {
	content := message.Get("content")
	if content.IsArray() {
		var b strings.Builder
		content.ForEach(func(_, part gjson.Result) bool {
			if part.Get("type").String() == "text" {
				b.WriteString(part.Get("text").String())
			}
			return true
		})
		return b.String()
	}
	return content.String()
}

// semanticCandidate 判断请求能否做语义匹配，返回问题文本与上下文键。
func semanticCandidate(config Config, kind string, body []byte, req gjson.Result) (question, ctxKey string, ok bool) {
	if config.l3 == nil || kind != kindChat {
		return "", "", false
	}
	messages := req.Get("messages").Array()
	if len(messages) == 0 {
		return "", "", false
	}
	last := len(messages) - 1
	if messages[last].Get("role").String() != "user" {
		return "", "", false
	}
	question = strings.TrimSpace(lastMessageText(messages[last]))
	if question == "" || utf8.RuneCountInString(question) > config.l3.maxQuestionChars {
		return "", "", false
	}
	ctxBody, err := sjson.DeleteBytes(body, "messages."+strconv.Itoa(last)+".content")
	if err != nil {
		return "", "", false
	}
	ctxKey, err = BuildKey(config.namespace, kind+":ctx", varyValues(config), gjson.ParseBytes(ctxBody), config.ignoredFields)
	if err != nil {
		return "", "", false
	}
	return question, ctxKey, true
}

// lookupSemantic 发起 向量化 → 向量检索 →（命中时）读 L2 的异步调用链。调用方已挂起请求；
// 链上任何一步失败都按未命中放行，不影响请求本身。返回 false 表示连第一步都没能发出。
func lookupSemantic(config Config, state *requestState) bool {
	l3 := config.l3
	config.metrics.l3Lookups.Increment(1)
	start := time.Now()
	recorded := false
	recordLatency := func() { // 记录向量化 + 检索的耗时，即语义缓存给未命中请求增加的首 token 延迟
		if !recorded {
			recorded = true
			config.metrics.l3LookupMs.Record(uint64(time.Since(start).Milliseconds()))
		}
	}
	finishMiss := func() {
		recordLatency()
		miss(config, state)
		_ = proxywasm.ResumeHttpRequest()
	}
	payload := `{"model":` + quoteJSON(l3.embedModel) + `,"input":` + quoteJSON(state.question) + `}`
	err := l3.embed.Post(l3.embedPath, jsonHeaders, []byte(payload), func(status int, _ http.Header, body []byte) {
		vec := gjson.GetBytes(body, "data.0.embedding")
		if status != 200 || !vec.IsArray() {
			config.metrics.l3Errors.Increment(1)
			finishMiss()
			return
		}
		state.embedding = vec.Raw
		_ = proxywasm.SetProperty([]string{queryEmbeddingProperty}, []byte(vec.Raw))
		search := `{"vector":` + vec.Raw + `,"limit":` + strconv.Itoa(l3.candidates) + `,"with_payload":["key","q"],"score_threshold":` +
			strconv.FormatFloat(l3.threshold, 'f', -1, 64) +
			`,"filter":{"must":[{"key":"ctx","match":{"value":` + quoteJSON(state.ctxKey) + `}}]}}`
		err := l3.vector.Post("/collections/"+l3.collection+"/points/search", jsonHeaders, []byte(search), func(status int, _ http.Header, body []byte) {
			recordLatency()
			if status == 404 { // 集合不存在（还没建好或被删除）：交给定时任务重新创建
				l3.collectionReady = false
				finishMiss()
				return
			}
			if status != 200 {
				config.metrics.l3Errors.Increment(1)
				finishMiss()
				return
			}
			tryCandidates(config, state, gjson.GetBytes(body, "result").Array(), finishMiss)
		}, l3.vectorTimeout)
		if err != nil {
			config.metrics.l3Errors.Increment(1)
			finishMiss()
		}
	}, l3.embedTimeout)
	if err != nil {
		config.metrics.l3Errors.Increment(1)
		return false
	}
	return true
}

// tryCandidates 按相似度从高到低逐个检查候选：先做要素校验，通过后读出它对应的 L1/L2 缓存条目回放，
// 同时以当前请求的精确键写回 L1/L2。向量库里有记录、但 L2 条目已过期或被淘汰的候选会被删除，
// 再继续检查下一个，免得失效的点一直挡在有效候选前面。
func tryCandidates(config Config, state *requestState, candidates []gjson.Result, finishMiss func()) {
	l3 := config.l3
	for i, candidate := range candidates {
		score := candidate.Get("score").Float()
		if reason := l3.guard.conflict(state.question, candidate.Get("payload.q").String()); reason != "" {
			config.metrics.l3Rejects.Increment(1)
			if state.semanticReject == "" {
				state.semanticReject, state.semanticScore = reason, score
			}
			continue
		}
		key, rest := candidate.Get("payload.key").String(), candidates[i+1:]
		hit := func(entry *Entry) {
			state.semanticScore = score
			store(config, state.key, entry)
			replay(config, state, entry, statusL3)
		}
		if config.l1 != nil {
			if entry, ok := config.l1.Get(key); ok {
				hit(entry)
				return
			}
		}
		if config.l2 == nil {
			break
		}
		err := config.l2.client.Get(config.keyPrefix+key, func(value resp.Value) {
			if value.Error() != nil {
				config.metrics.l2Errors.Increment(1)
				finishMiss()
				return
			}
			if entry := decodeEntry(config, value); entry != nil {
				hit(entry)
				return
			}
			config.metrics.l3Stale.Increment(1)
			deletePoint(l3, candidate.Get("id").String())
			tryCandidates(config, state, rest, finishMiss)
		})
		if err != nil {
			config.metrics.l2Errors.Increment(1)
			finishMiss()
		}
		return
	}
	finishMiss()
}

// deletePoint 删除向量库中的一个点（不等待结果）。
func deletePoint(l3 *semanticConfig, id string) {
	body := `{"points":[` + quoteJSON(id) + `]}`
	_ = l3.vector.Post("/collections/"+l3.collection+"/points/delete?wait=false", jsonHeaders, []byte(body), func(int, http.Header, []byte) {}, l3.vectorTimeout)
}

// upsertSemantic 在响应写入缓存后，把本请求的问题向量登记到向量库。
//
// 这次外呼发生在响应的最后一个分块，请求随后结束，Envoy 会取消该请求上下文中尚未返回的外呼回调，
// 所以这里不依赖回调：发出即计数，集合由根上下文的定时任务预先创建（见 ensureCollection）。
func upsertSemantic(config Config, state *requestState) {
	l3 := config.l3
	if !l3.collectionReady {
		return
	}
	// q 保存问题原文（不超过 maxQuestionChars），命中前的要素校验要用它
	point := `{"points":[{"id":"` + pointID(state.key) + `","vector":` + state.embedding +
		`,"payload":{"ctx":` + quoteJSON(state.ctxKey) + `,"key":` + quoteJSON(state.key) + `,"q":` + quoteJSON(state.question) + `}}]}`
	err := l3.vector.Put("/collections/"+l3.collection+"/points?wait=false", jsonHeaders, []byte(point), func(int, http.Header, []byte) {}, l3.vectorTimeout)
	if err != nil {
		config.metrics.l3Errors.Increment(1)
		return
	}
	config.metrics.l3Stores.Increment(1)
}

// ensureCollection 由根上下文的定时任务调用，直到集合可用：不存在就创建（余弦距离），
// 并为 ctx 建关键字索引（按上下文过滤时不必扫描全部向量）。多个 worker 同时创建时"已存在"的错误直接忽略。
func ensureCollection(l3 *semanticConfig) {
	if l3.collectionReady || l3.ensuring {
		return
	}
	l3.ensuring = true
	path := "/collections/" + l3.collection
	err := l3.vector.Get(path, nil, func(status int, _ http.Header, _ []byte) {
		if status == 200 {
			l3.collectionReady, l3.ensuring = true, false
			return
		}
		body := `{"vectors":{"size":` + strconv.Itoa(l3.dims) + `,"distance":"Cosine"}}`
		err := l3.vector.Put(path, jsonHeaders, []byte(body), func(status int, _ http.Header, _ []byte) {
			index := `{"field_name":"ctx","field_schema":"keyword"}`
			err := l3.vector.Put(path+"/index?wait=true", jsonHeaders, []byte(index), func(status int, _ http.Header, _ []byte) {
				l3.collectionReady, l3.ensuring = status == 200, false
			}, l3.vectorTimeout*10)
			if err != nil {
				l3.ensuring = false
			}
		}, l3.vectorTimeout*10)
		if err != nil {
			l3.ensuring = false
		}
	}, l3.vectorTimeout*10)
	if err != nil {
		l3.ensuring = false
	}
}

// pointID 把精确键（sha256 十六进制）的前 128 位格式化成 Qdrant 接受的 UUID。
func pointID(key string) string {
	if len(key) < 32 {
		key += strings.Repeat("0", 32-len(key))
	}
	return key[0:8] + "-" + key[8:12] + "-" + key[12:16] + "-" + key[16:20] + "-" + key[20:32]
}
