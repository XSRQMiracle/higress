package main

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func semanticConfigForTest() Config {
	return Config{namespace: "ns", ignoredFields: toSet(defaultIgnoredFields), l3: &semanticConfig{maxQuestionChars: 50}}
}

func candidate(t *testing.T, body string) (string, string, bool) {
	t.Helper()
	return semanticCandidate(semanticConfigForTest(), kindChat, []byte(body), gjson.Parse(body))
}

func TestSemanticCandidateSharesContextAcrossParaphrases(t *testing.T) {
	a := `{"model":"m","temperature":0.2,"messages":[{"role":"system","content":"你是银行客服"},{"role":"user","content":"信用卡逾期怎么办？"}]}`
	b := `{"model":"m","temperature":0.2,"messages":[{"role":"system","content":"你是银行客服"},{"role":"user","content":"请问信用卡逾期了该怎么办呢？"}]}`
	qa, ctxA, okA := candidate(t, a)
	qb, ctxB, okB := candidate(t, b)
	if !okA || !okB || qa != "信用卡逾期怎么办？" || qb == qa {
		t.Fatalf("questions: %q %q (%v %v)", qa, qb, okA, okB)
	}
	if ctxA != ctxB {
		t.Fatal("paraphrases with the same context must share the context key")
	}
	other := strings.Replace(a, "你是银行客服", "你是理财顾问", 1)
	if _, ctxC, _ := candidate(t, other); ctxC == ctxA {
		t.Fatal("a different system prompt must change the context key")
	}
	hotter := strings.Replace(a, `"temperature":0.2`, `"temperature":0.9`, 1)
	if _, ctxD, _ := candidate(t, hotter); ctxD == ctxA {
		t.Fatal("different sampling parameters must change the context key")
	}
}

func TestSemanticCandidateRejectsUnsuitableRequests(t *testing.T) {
	cases := map[string]string{
		"last message is not from the user": `{"model":"m","messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"}]}`,
		"question too long":                 `{"model":"m","messages":[{"role":"user","content":"` + strings.Repeat("长", 60) + `"}]}`,
		"empty question":                    `{"model":"m","messages":[{"role":"user","content":"   "}]}`,
		"no messages":                       `{"model":"m","prompt":"hi"}`,
	}
	for name, body := range cases {
		if _, _, ok := candidate(t, body); ok {
			t.Errorf("%s: must not be a semantic candidate", name)
		}
	}
	if _, _, ok := semanticCandidate(Config{}, kindChat, []byte(`{"messages":[{"role":"user","content":"hi"}]}`), gjson.Parse(`{"messages":[{"role":"user","content":"hi"}]}`)); ok {
		t.Fatal("without l3 configured nothing is a candidate")
	}
}

func TestPointIDIsUUID(t *testing.T) {
	key := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if got := pointID(key); got != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Fatalf("point id = %s", got)
	}
}
