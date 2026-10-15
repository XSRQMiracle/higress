package main

import (
	"bytes"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

const (
	kindChat       = "chat"
	kindCompletion = "completion"
	entryVersion   = 1
)

// Entry 是一条缓存的模型响应。只保存回放所需的最少信息，不保存原始 SSE 报文：
// 一个 256 token 的回答原样存约 50KB，按分片存约 1–2KB，L1 与 Redis 能多放几十倍条目。
//
// Content / Reasoning 保留上游流式响应的原始分块（vLLM 每个 chunk 约 1 个 token），
// 回放时按同样的粒度输出，压测工具统计的 token 数和 TPOT 与直连后端一致。
type Entry struct {
	Version      int      `json:"v"`
	Kind         string   `json:"k"`
	Model        string   `json:"m"`
	Reasoning    []string `json:"r,omitempty"` // 思考模型的 reasoning_content，回放时先于正文输出
	Content      []string `json:"c"`
	FinishReason string   `json:"f"`
	Usage        Usage    `json:"u"`
}

type Usage struct {
	PromptTokens     int  `json:"p"`
	CompletionTokens int  `json:"c"`
	Known            bool `json:"k"` // false 表示上游未返回 usage，completion_tokens 按分片数估算
}

func (e *Entry) sizeBytes() int {
	n := len(e.Model) + len(e.FinishReason) + 64
	for _, s := range e.Content {
		n += len(s) + 16
	}
	for _, s := range e.Reasoning {
		n += len(s) + 16
	}
	return n
}

// collector 在响应透传给客户端的同时收集内容，判断这次响应能否写入缓存。
type collector struct {
	kind     string
	stream   bool
	maxBytes int
	size     int
	pending  []byte // 流式：尚未凑成完整一行的数据；非流式：已收到的 body
	sawDone  bool
	usageSet bool
	entry    Entry
	failed   string
}

func newCollector(kind string, stream bool, maxBytes int) *collector {
	return &collector{kind: kind, stream: stream, maxBytes: maxBytes, entry: Entry{Version: entryVersion, Kind: kind}}
}

func (c *collector) fail(reason string) {
	if c.failed == "" {
		c.failed = reason
	}
	c.pending = nil
}

// Feed 处理一个响应分块。SSE 事件可能被拆在两个分块之间，不完整的行留到下一次处理。
func (c *collector) Feed(chunk []byte) {
	if c.failed != "" {
		return
	}
	c.size += len(chunk)
	if c.size > c.maxBytes {
		c.fail("response_too_large")
		return
	}
	c.pending = append(c.pending, chunk...)
	if !c.stream {
		return
	}
	for c.failed == "" {
		i := bytes.IndexByte(c.pending, '\n')
		if i < 0 {
			return
		}
		line := c.pending[:i]
		c.pending = c.pending[i+1:]
		c.handleLine(line)
	}
}

func (c *collector) handleLine(line []byte) {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, []byte("data:")) {
		return
	}
	data := bytes.TrimSpace(line[len("data:"):])
	if string(data) == "[DONE]" {
		c.sawDone = true
		return
	}
	c.handleStreamEvent(data)
}

func (c *collector) handleStreamEvent(data []byte) {
	if !gjson.ValidBytes(data) {
		c.fail("invalid_sse_json")
		return
	}
	event := gjson.ParseBytes(data)
	if event.Get("error").Exists() || event.Get("object").String() == "error" {
		c.fail("error_event")
		return
	}
	if model := event.Get("model"); model.Type == gjson.String && c.entry.Model == "" {
		c.entry.Model = model.Str
	}
	if usage := event.Get("usage"); usage.IsObject() {
		c.setUsage(usage)
	}
	event.Get("choices").ForEach(func(_, choice gjson.Result) bool {
		if choice.Get("index").Int() != 0 {
			c.fail("multiple_choices")
			return false
		}
		if c.kind == kindChat {
			delta := choice.Get("delta")
			if calls := delta.Get("tool_calls"); calls.IsArray() && len(calls.Array()) > 0 {
				c.fail("tool_calls")
				return false
			}
			if text := reasoningText(delta); text != "" {
				c.entry.Reasoning = append(c.entry.Reasoning, text)
			}
			if text := delta.Get("content"); text.Type == gjson.String && text.Str != "" {
				c.entry.Content = append(c.entry.Content, text.Str)
			}
		} else if text := choice.Get("text"); text.Type == gjson.String && text.Str != "" {
			c.entry.Content = append(c.entry.Content, text.Str)
		}
		if reason := choice.Get("finish_reason"); reason.Type == gjson.String {
			c.entry.FinishReason = reason.Str
		}
		return true
	})
}

// reasoningText 兼容 vLLM 推理解析器的两种字段名。
func reasoningText(delta gjson.Result) string {
	if text := delta.Get("reasoning_content"); text.Type == gjson.String {
		return text.Str
	}
	if text := delta.Get("reasoning"); text.Type == gjson.String {
		return text.Str
	}
	return ""
}

func (c *collector) setUsage(usage gjson.Result) {
	c.entry.Usage = Usage{
		PromptTokens:     int(usage.Get("prompt_tokens").Int()),
		CompletionTokens: int(usage.Get("completion_tokens").Int()),
		Known:            true,
	}
	c.usageSet = true
}

// Finish 在响应结束时调用：返回可缓存的条目，或者不可缓存的原因。
func (c *collector) Finish(finishReasons map[string]struct{}) (*Entry, string) {
	if c.failed == "" {
		if c.stream {
			if len(c.pending) > 0 { // 最后一行可能没有换行符
				c.handleLine(c.pending)
				c.pending = nil
			}
			if c.failed == "" && !c.sawDone {
				c.fail("incomplete_stream")
			}
		} else {
			c.parseJSONBody()
		}
	}
	if c.failed != "" {
		return nil, c.failed
	}
	if _, ok := finishReasons[c.entry.FinishReason]; !ok {
		return nil, "finish_reason_" + c.entry.FinishReason
	}
	if len(c.entry.Content) == 0 && len(c.entry.Reasoning) == 0 {
		return nil, "empty_response"
	}
	if !c.usageSet {
		c.entry.Usage = Usage{CompletionTokens: len(c.entry.Content) + len(c.entry.Reasoning)}
	}
	entry := c.entry
	return &entry, ""
}

func (c *collector) parseJSONBody() {
	body := c.pending
	c.pending = nil
	if !gjson.ValidBytes(body) {
		c.fail("invalid_json")
		return
	}
	resp := gjson.ParseBytes(body)
	choices := resp.Get("choices").Array()
	if len(choices) != 1 {
		c.fail("multiple_choices")
		return
	}
	c.entry.Model = resp.Get("model").String()
	if usage := resp.Get("usage"); usage.IsObject() {
		c.setUsage(usage)
	}
	choice := choices[0]
	c.entry.FinishReason = choice.Get("finish_reason").String()
	if c.kind == kindCompletion {
		c.entry.Content = Rechunk(choice.Get("text").String())
		return
	}
	message := choice.Get("message")
	if calls := message.Get("tool_calls"); calls.IsArray() && len(calls.Array()) > 0 {
		c.fail("tool_calls")
		return
	}
	c.entry.Reasoning = Rechunk(reasoningText(message))
	c.entry.Content = Rechunk(message.Get("content").String())
}

// Rechunk 把一段完整文本切成近似 token 粒度的分片：汉字及全角标点各成一片，
// 字母数字最多 4 个字符一片，空白并入后一个分片（与 BPE 词表"空格+词"的习惯一致）。
// 用于把非流式响应存成的条目回放成流式响应。
func Rechunk(text string) []string {
	if text == "" {
		return nil
	}
	var pieces []string
	start, alnum := 0, 0
	flush := func(end int) {
		if end > start {
			pieces = append(pieces, text[start:end])
		}
		start, alnum = end, 0
	}
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRuneInString(text[i:])
		switch {
		case unicode.IsSpace(r):
			if alnum > 0 {
				flush(i)
			}
		case r < utf8.RuneSelf && (unicode.IsLetter(r) || unicode.IsDigit(r)):
			if alnum == 4 {
				flush(i)
			}
			alnum++
		default: // 汉字、标点、其他符号：各自成为一个分片（前导空白随之输出）
			if alnum > 0 {
				flush(i)
			}
			flush(i + size)
		}
		i += size
	}
	flush(len(text))
	return pieces
}

// renderOptions 描述命中时的回放方式（取自当前请求，而不是写入缓存时的请求）。
type renderOptions struct {
	id           string
	created      int64
	stream       bool
	includeUsage bool
}

func usageJSON(e *Entry) string {
	p, c := e.Usage.PromptTokens, e.Usage.CompletionTokens
	// 命中缓存时没有进行任何计算，所有 prompt token 都视为"已缓存"。
	return `{"prompt_tokens":` + itoa(p) + `,"total_tokens":` + itoa(p+c) + `,"completion_tokens":` + itoa(c) +
		`,"prompt_tokens_details":{"cached_tokens":` + itoa(p) + `}}`
}

// Render 按 vLLM OpenAI 兼容接口的格式生成响应体，返回 body 与 Content-Type。
func Render(e *Entry, opt renderOptions) ([]byte, string) {
	if opt.stream {
		return renderStream(e, opt), "text/event-stream; charset=utf-8"
	}
	return renderJSON(e, opt), "application/json"
}

func renderStream(e *Entry, opt renderOptions) []byte {
	var buf bytes.Buffer
	object := "chat.completion.chunk"
	if e.Kind == kindCompletion {
		object = "text_completion"
	}
	head := `{"id":` + quoteJSON(opt.id) + `,"object":"` + object + `","created":` + itoa(int(opt.created)) + `,"model":` + quoteJSON(e.Model) + `,"choices":`
	event := func(choices string) {
		buf.WriteString("data: ")
		buf.WriteString(head)
		buf.WriteString(choices)
		buf.WriteString("}\n\n")
	}
	if e.Kind == kindChat {
		event(`[{"index":0,"delta":{"role":"assistant","content":""},"logprobs":null,"finish_reason":null}]`)
		for _, piece := range e.Reasoning {
			event(`[{"index":0,"delta":{"reasoning_content":` + quoteJSON(piece) + `},"logprobs":null,"finish_reason":null}]`)
		}
		for _, piece := range e.Content {
			event(`[{"index":0,"delta":{"content":` + quoteJSON(piece) + `},"logprobs":null,"finish_reason":null}]`)
		}
		event(`[{"index":0,"delta":{},"logprobs":null,"finish_reason":` + quoteJSON(e.FinishReason) + `,"stop_reason":null}]`)
	} else {
		for _, piece := range e.Content {
			event(`[{"index":0,"text":` + quoteJSON(piece) + `,"logprobs":null,"finish_reason":null,"stop_reason":null}]`)
		}
		event(`[{"index":0,"text":"","logprobs":null,"finish_reason":` + quoteJSON(e.FinishReason) + `,"stop_reason":null}]`)
	}
	if opt.includeUsage {
		buf.WriteString("data: ")
		buf.WriteString(head)
		buf.WriteString(`[],"usage":`)
		buf.WriteString(usageJSON(e))
		buf.WriteString("}\n\n")
	}
	buf.WriteString("data: [DONE]\n\n")
	return buf.Bytes()
}

func renderJSON(e *Entry, opt renderOptions) []byte {
	var buf bytes.Buffer
	content := strings.Join(e.Content, "")
	if e.Kind == kindChat {
		buf.WriteString(`{"id":` + quoteJSON(opt.id) + `,"object":"chat.completion","created":` + itoa(int(opt.created)) + `,"model":` + quoteJSON(e.Model))
		buf.WriteString(`,"choices":[{"index":0,"message":{"role":"assistant","content":` + quoteJSON(content))
		if len(e.Reasoning) > 0 {
			buf.WriteString(`,"reasoning_content":` + quoteJSON(strings.Join(e.Reasoning, "")))
		}
		buf.WriteString(`,"tool_calls":[]},"logprobs":null,"finish_reason":` + quoteJSON(e.FinishReason) + `,"stop_reason":null}]`)
	} else {
		buf.WriteString(`{"id":` + quoteJSON(opt.id) + `,"object":"text_completion","created":` + itoa(int(opt.created)) + `,"model":` + quoteJSON(e.Model))
		buf.WriteString(`,"choices":[{"index":0,"text":` + quoteJSON(content) + `,"logprobs":null,"finish_reason":` + quoteJSON(e.FinishReason) + `,"stop_reason":null}]`)
	}
	buf.WriteString(`,"usage":` + usageJSON(e) + `}`)
	return buf.Bytes()
}
