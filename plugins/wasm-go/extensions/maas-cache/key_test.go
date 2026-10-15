package main

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func keyOf(t *testing.T, body string) string {
	t.Helper()
	key, err := BuildKey("ns", kindChat, nil, gjson.Parse(body), toSet(defaultIgnoredFields))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

const baseRequest = `{"model":"qwen","messages":[{"role":"system","content":"你是客服"},{"role":"user","content":"信用卡逾期怎么办？"}],"temperature":0.7,"max_tokens":256}`

func TestKeyIgnoresFieldOrderWhitespaceAndEscapes(t *testing.T) {
	reordered := `{ "max_tokens": 256, "temperature": 0.7,
		"messages": [ {"content":"你是客服","role":"system"}, {"role":"user","content":"信用卡逾期怎么办？"} ],
		"model": "qwen" }`
	if keyOf(t, baseRequest) != keyOf(t, reordered) {
		t.Fatal("equivalent requests must share a key")
	}
}

func TestKeyIgnoresTransportFields(t *testing.T) {
	withTransport := strings.Replace(baseRequest, `"model":"qwen"`,
		`"model":"qwen","stream":true,"stream_options":{"include_usage":true},"user":"u-1","n":1`, 1)
	if keyOf(t, baseRequest) != keyOf(t, withTransport) {
		t.Fatal("stream / stream_options / user / n must not affect the key")
	}
}

func TestKeyCoversEverythingThatChangesTheAnswer(t *testing.T) {
	base := keyOf(t, baseRequest)
	variants := map[string]string{
		"system prompt":  strings.Replace(baseRequest, "你是客服", "你是理财顾问", 1),
		"temperature":    strings.Replace(baseRequest, `"temperature":0.7`, `"temperature":0.2`, 1),
		"max_tokens":     strings.Replace(baseRequest, `"max_tokens":256`, `"max_tokens":512`, 1),
		"model":          strings.Replace(baseRequest, `"qwen"`, `"deepseek"`, 1),
		"history":        strings.Replace(baseRequest, `{"role":"user"`, `{"role":"user","content":"你好"},{"role":"assistant","content":"您好"},{"role":"user"`, 1),
		"unknown fields": strings.Replace(baseRequest, `"max_tokens":256`, `"max_tokens":256,"chat_template_kwargs":{"enable_thinking":false}`, 1),
	}
	for name, body := range variants {
		if keyOf(t, body) == base {
			t.Errorf("changing %s must change the key", name)
		}
	}
}

func TestKeyIncludesNamespaceKindAndVaryHeaders(t *testing.T) {
	body := gjson.Parse(baseRequest)
	ignored := toSet(defaultIgnoredFields)
	k1, _ := BuildKey("ns", kindChat, nil, body, ignored)
	k2, _ := BuildKey("ns2", kindChat, nil, body, ignored)
	k3, _ := BuildKey("ns", kindCompletion, nil, body, ignored)
	k4, _ := BuildKey("ns", kindChat, []string{"x-tenant=a"}, body, ignored)
	k5, _ := BuildKey("ns", kindChat, []string{"x-tenant=b"}, body, ignored)
	if k1 == k2 || k1 == k3 || k1 == k4 || k4 == k5 {
		t.Fatal("namespace, kind and vary headers must all partition the key space")
	}
}

func TestKeyRejectsPathologicalNesting(t *testing.T) {
	deep := `{"messages":` + strings.Repeat("[", 100) + strings.Repeat("]", 100) + `}`
	if _, err := BuildKey("", kindChat, nil, gjson.Parse(deep), nil); err == nil {
		t.Fatal("deeply nested JSON must be rejected")
	}
}

func TestAppendJSONStringEscaping(t *testing.T) {
	got := quoteJSON("a\"b\\c\n\t\x01中\xff")
	want := `"a\"b\\c\n\t\u0001中�"`
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}
