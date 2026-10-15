package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

var allowedFinish = toSet([]string{"stop", "length"})

// vllmStream 生成与 vLLM 一致的流式响应：role 帧、逐 token 内容帧、结束帧、usage 帧、[DONE]。
func vllmStream(pieces []string, finish string, withUsage bool) string {
	var b strings.Builder
	head := `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"mock-qwen","choices":`
	b.WriteString(head + `[{"index":0,"delta":{"role":"assistant","content":""},"logprobs":null,"finish_reason":null}]}` + "\n\n")
	for _, p := range pieces {
		b.WriteString(head + `[{"index":0,"delta":{"content":` + quoteJSON(p) + `},"logprobs":null,"finish_reason":null}]}` + "\n\n")
	}
	b.WriteString(head + `[{"index":0,"delta":{},"logprobs":null,"finish_reason":"` + finish + `","stop_reason":null}]}` + "\n\n")
	if withUsage {
		b.WriteString(head + `[],"usage":{"prompt_tokens":42,"total_tokens":45,"completion_tokens":3}}` + "\n\n")
	}
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

// feedInPieces 模拟网络把 SSE 报文切成任意大小的分块。
func feedInPieces(c *collector, data string, size int) {
	for start := 0; start < len(data); start += size {
		c.Feed([]byte(data[start:min(start+size, len(data))]))
	}
}

func TestCollectStreamAcrossArbitraryChunkBoundaries(t *testing.T) {
	pieces := []string{"你", "好", "，世界"}
	for _, size := range []int{1, 7, 64, 4096} {
		c := newCollector(kindChat, true, 1<<20)
		feedInPieces(c, vllmStream(pieces, "stop", true), size)
		entry, reason := c.Finish(allowedFinish)
		if entry == nil {
			t.Fatalf("chunk size %d: not cacheable: %s", size, reason)
		}
		if !reflect.DeepEqual(entry.Content, pieces) || entry.FinishReason != "stop" || entry.Model != "mock-qwen" {
			t.Fatalf("chunk size %d: unexpected entry %+v", size, entry)
		}
		if entry.Usage != (Usage{PromptTokens: 42, CompletionTokens: 3, Known: true}) {
			t.Fatalf("chunk size %d: usage %+v", size, entry.Usage)
		}
	}
}

func TestCollectRejectsUncacheableResponses(t *testing.T) {
	toolCall := strings.Replace(vllmStream([]string{"x"}, "tool_calls", true), `"delta":{"content":"x"}`,
		`"delta":{"tool_calls":[{"index":0,"function":{"name":"f","arguments":"{}"}}]}`, 1)
	cases := map[string]string{
		"tool_calls":          toolCall,
		"error_event":         `data: {"object":"error","message":"boom"}` + "\n\n",
		"incomplete_stream":   strings.TrimSuffix(vllmStream([]string{"x"}, "stop", true), "data: [DONE]\n\n"),
		"finish_reason_abort": vllmStream([]string{"x"}, "abort", true),
		"empty_response":      vllmStream(nil, "stop", true),
		"response_too_large":  vllmStream([]string{strings.Repeat("长", 400)}, "stop", true),
	}
	for want, stream := range cases {
		c := newCollector(kindChat, true, 1024)
		feedInPieces(c, stream, 13)
		if entry, reason := c.Finish(allowedFinish); entry != nil || reason != want {
			t.Errorf("%s: got entry=%v reason=%q", want, entry != nil, reason)
		}
	}
}

func TestCollectWithoutUsageEstimatesCompletionTokens(t *testing.T) {
	c := newCollector(kindChat, true, 1<<20)
	feedInPieces(c, vllmStream([]string{"a", "b"}, "length", false), 5)
	entry, _ := c.Finish(allowedFinish)
	if entry == nil || entry.Usage.Known || entry.Usage.CompletionTokens != 2 {
		t.Fatalf("usage should be estimated from pieces: %+v", entry)
	}
}

func TestCollectNonStreamJSONIsRechunked(t *testing.T) {
	body := `{"id":"x","object":"chat.completion","model":"mock-qwen","choices":[{"index":0,"message":{"role":"assistant","content":"你好 world","reasoning_content":"想一想"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":4,"total_tokens":9}}`
	c := newCollector(kindChat, false, 1<<20)
	feedInPieces(c, body, 10)
	entry, reason := c.Finish(allowedFinish)
	if entry == nil {
		t.Fatalf("not cacheable: %s", reason)
	}
	if want := []string{"你", "好", " worl", "d"}; !reflect.DeepEqual(entry.Content, want) {
		t.Fatalf("content pieces %q, want %q", entry.Content, want)
	}
	if strings.Join(entry.Reasoning, "") != "想一想" || entry.Usage.PromptTokens != 5 {
		t.Fatalf("unexpected entry %+v", entry)
	}
}

// 回放的 SSE 再被收集一遍，必须得到同样的条目：保证回放与原始响应结构一致。
func TestRenderStreamRoundTrip(t *testing.T) {
	original := &Entry{
		Version: entryVersion, Kind: kindChat, Model: "mock-qwen", Reasoning: []string{"先", "想"},
		Content: []string{"答", "案\n", "\"引号\""}, FinishReason: "length",
		Usage: Usage{PromptTokens: 10, CompletionTokens: 5, Known: true},
	}
	body, contentType := Render(original, renderOptions{id: "chatcmpl-x", created: 7, stream: true, includeUsage: true})
	if contentType != "text/event-stream; charset=utf-8" {
		t.Fatalf("content type %q", contentType)
	}
	c := newCollector(kindChat, true, 1<<20)
	feedInPieces(c, string(body), 17)
	replayed, reason := c.Finish(allowedFinish)
	if replayed == nil {
		t.Fatalf("replayed stream not parseable: %s", reason)
	}
	if !reflect.DeepEqual(replayed.Content, original.Content) || !reflect.DeepEqual(replayed.Reasoning, original.Reasoning) ||
		replayed.FinishReason != original.FinishReason || replayed.Usage != original.Usage || replayed.Model != original.Model {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", replayed, original)
	}
	events := strings.Count(string(body), "data: ")
	if want := 1 + 2 + 3 + 1 + 1 + 1; events != want { // role + reasoning + content + finish + usage + [DONE]
		t.Fatalf("got %d SSE events, want %d", events, want)
	}
	first := gjson.Get(strings.TrimPrefix(strings.SplitN(string(body), "\n\n", 2)[0], "data: "), "choices.0.delta.role").String()
	if first != "assistant" {
		t.Fatalf("first event must carry the assistant role, got %q", first)
	}
}

func TestRenderStreamWithoutUsageOmitsUsageEvent(t *testing.T) {
	entry := &Entry{Version: entryVersion, Kind: kindChat, Model: "m", Content: []string{"x"}, FinishReason: "stop"}
	body, _ := Render(entry, renderOptions{id: "i", stream: true})
	if strings.Contains(string(body), `"usage"`) {
		t.Fatal("usage must only be sent when the client asked for it")
	}
}

func TestRenderJSONChatAndCompletion(t *testing.T) {
	entry := &Entry{Version: entryVersion, Kind: kindChat, Model: "m", Reasoning: []string{"r"}, Content: []string{"你", "好"},
		FinishReason: "stop", Usage: Usage{PromptTokens: 3, CompletionTokens: 2, Known: true}}
	body, contentType := Render(entry, renderOptions{id: "chatcmpl-1", created: 9})
	parsed := gjson.ParseBytes(body)
	if contentType != "application/json" || !gjson.ValidBytes(body) {
		t.Fatalf("invalid json response: %s", body)
	}
	if parsed.Get("object").String() != "chat.completion" || parsed.Get("choices.0.message.content").String() != "你好" ||
		parsed.Get("choices.0.message.reasoning_content").String() != "r" || parsed.Get("usage.total_tokens").Int() != 5 ||
		parsed.Get("usage.prompt_tokens_details.cached_tokens").Int() != 3 {
		t.Fatalf("unexpected chat body: %s", body)
	}

	entry.Kind, entry.Reasoning = kindCompletion, nil
	body, _ = Render(entry, renderOptions{id: "cmpl-1", created: 9})
	parsed = gjson.ParseBytes(body)
	if parsed.Get("object").String() != "text_completion" || parsed.Get("choices.0.text").String() != "你好" {
		t.Fatalf("unexpected completion body: %s", body)
	}
	stream, _ := Render(entry, renderOptions{id: "cmpl-1", stream: true})
	c := newCollector(kindCompletion, true, 1<<20)
	c.Feed(stream)
	if replayed, reason := c.Finish(allowedFinish); replayed == nil || strings.Join(replayed.Content, "") != "你好" {
		t.Fatalf("completion stream round trip failed: %s", reason)
	}
}

func TestRechunk(t *testing.T) {
	cases := map[string][]string{
		"":            nil,
		"Hello world": {"Hell", "o", " worl", "d"},
		"你好，世界":       {"你", "好", "，", "世", "界"},
		"a 你\n\n":     {"a", " 你", "\n\n"},
		"x1234567":    {"x123", "4567"},
	}
	for input, want := range cases {
		if got := Rechunk(input); !reflect.DeepEqual(got, want) {
			t.Errorf("Rechunk(%q) = %q, want %q", input, got, want)
		}
	}
	text := "Mixed 中英 text,  with   spaces\tand 标点！123"
	if strings.Join(Rechunk(text), "") != text {
		t.Fatal("rechunked pieces must concatenate back to the original text")
	}
}
