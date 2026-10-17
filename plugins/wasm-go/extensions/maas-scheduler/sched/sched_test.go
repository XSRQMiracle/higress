package sched

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func TestEstimateAndCutTokens(t *testing.T) {
	if got := EstimateTokens("abcde"); got != 2 {
		t.Fatalf("abcde = %d tokens, want 2", got)
	}
	if got := EstimateTokens("你好 world"); got != 5 { // 你 好 空白 worl d
		t.Fatalf("got %d tokens, want 5", got)
	}
	head, n := CutByTokens("你好世界abc", 3)
	if head != "你好世" || n != 3 {
		t.Fatalf("cut = %q (%d)", head, n)
	}
	if head, n := CutByTokens("短", 10); head != "短" || n != 1 {
		t.Fatalf("cut beyond end = %q (%d)", head, n)
	}
}

func chatBody(system, history, last string) gjson.Result {
	messages := []string{`{"role":"system","content":` + fmt.Sprintf("%q", system) + `}`}
	if history != "" {
		messages = append(messages, `{"role":"user","content":`+fmt.Sprintf("%q", history)+`}`,
			`{"role":"assistant","content":"好的"}`)
	}
	messages = append(messages, `{"role":"user","content":`+fmt.Sprintf("%q", last)+`}`)
	return gjson.Parse(`{"model":"m","max_tokens":128,"messages":[` + strings.Join(messages, ",") + `]}`)
}

func levelByName(f Features, name string) (Level, bool) {
	for _, l := range f.Levels {
		if l.Name == name {
			return l, true
		}
	}
	return Level{}, false
}

func TestExtractFeaturesLevels(t *testing.T) {
	system := strings.Repeat("规", 300)
	doc := strings.Repeat("文", 900)
	a := ExtractFeatures(chatBody(system, "", doc+"\n\n请问第一个问题？"), "chat", 256, 64)
	b := ExtractFeatures(chatBody(system, "", doc+"\n\n换一个长得多的问题，内容完全不同，长度也不一样？"), "chat", 256, 64)
	c := ExtractFeatures(chatBody(system, "", strings.Repeat("另", 900)+"\n\n请问第一个问题？"), "chat", 256, 64)
	short := ExtractFeatures(chatBody(system, "", "你好"), "chat", 256, 64)

	da, okA := levelByName(a, "document")
	db, okB := levelByName(b, "document")
	dc, _ := levelByName(c, "document")
	if !okA || !okB || da.Hash != db.Hash {
		t.Fatalf("same document with different questions must share the document level: %+v / %+v", a.Levels, b.Levels)
	}
	if da.Hash == dc.Hash {
		t.Fatal("different documents must not share the document level")
	}
	if a.Levels[0].Name != "template" || a.Levels[0].Hash != short.Levels[0].Hash || a.TemplateKey != short.TemplateKey {
		t.Fatal("requests with the same system prompt must share the template level")
	}
	if _, ok := levelByName(short, "document"); ok {
		t.Fatal("a short question must not form a document level")
	}
	if da.Tokens < 900 || a.PromptTokens <= da.Tokens || a.MaxTokens != 128 {
		t.Fatalf("token accounting: document %d, prompt %d, max %d", da.Tokens, a.PromptTokens, a.MaxTokens)
	}
}

func TestExtractFeaturesHistoryAndBlockFallback(t *testing.T) {
	f := ExtractFeatures(chatBody("sys", "第一轮的问题", "第二轮的问题"), "chat", 256, 64)
	if _, ok := levelByName(f, "history"); !ok {
		t.Fatalf("multi-turn request must have a history level: %+v", f.Levels)
	}
	// 没有空行时按 block 对齐：问题长度变化不跨越 block 边界时，头部相同
	doc := strings.Repeat("文", 1000)
	x := ExtractFeatures(chatBody("sys", "", doc+"问题甲"), "chat", 256, 64)
	y := ExtractFeatures(chatBody("sys", "", doc+"问题乙比较长一些"), "chat", 256, 64)
	dx, _ := levelByName(x, "document")
	dy, _ := levelByName(y, "document")
	if dx.Hash != dy.Hash || dx.Tokens != dy.Tokens || dx.Tokens < 768 {
		t.Fatalf("block-aligned heads differ: %+v vs %+v", dx, dy)
	}
	completion := ExtractFeatures(gjson.Parse(`{"model":"m","prompt":"`+doc+`问题"}`), "completion", 256, 64)
	if _, ok := levelByName(completion, "document"); !ok || completion.PromptTokens != 1002 {
		t.Fatalf("completion prompt levels: %+v", completion)
	}
}

func TestRingSpreadsKeysAndMovesFewOnScaleOut(t *testing.T) {
	addrs := []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"}
	ring := NewRing(addrs, 64)
	all := func(string) bool { return true }
	owner := map[uint64]string{}
	counts := map[string]int{}
	for k := uint64(0); k < 3000; k++ {
		home, _ := ring.Home(k, all)
		owner[k] = home
		counts[home]++
	}
	for _, a := range addrs {
		if share := float64(counts[a]) / 3000; share < 0.2 || share > 0.47 {
			t.Fatalf("unbalanced ring: %v", counts)
		}
	}
	bigger := NewRing(append(addrs, "10.0.0.4:8000"), 64)
	moved := 0
	for k, before := range owner {
		if after, _ := bigger.Home(k, all); after != before {
			moved++
		}
	}
	if share := float64(moved) / 3000; share < 0.12 || share > 0.4 {
		t.Fatalf("adding one of four nodes should move about 1/4 of keys, moved %.2f", share)
	}
	if home, ok := ring.Home(42, func(a string) bool { return a == "10.0.0.2:8000" }); !ok || home != "10.0.0.2:8000" {
		t.Fatal("home must skip replicas that are not accepted")
	}
	if _, ok := ring.Home(42, func(string) bool { return false }); ok {
		t.Fatal("no accepted replica must return false")
	}
}

func TestPresenceIndex(t *testing.T) {
	now := time.Unix(1000, 0)
	p := NewPresenceIndex(time.Minute, 3)
	levels := []Level{{Name: "template", Hash: 1, Tokens: 100}, {Name: "document", Hash: 2, Tokens: 5000}}
	p.Record("a", levels, now)
	if got := p.DeepestHit("a", levels, now); got != 5000 {
		t.Fatalf("deepest hit = %d", got)
	}
	if got := p.DeepestHit("b", levels, now); got != 0 || !p.AnyHas(levels[1], now) {
		t.Fatalf("b must have no hit while the level exists somewhere (%d)", got)
	}
	p.Invalidate("a", levels, 100)
	if got := p.DeepestHit("a", levels, now); got != 100 {
		t.Fatalf("after invalidation the deepest hit should fall back to the template, got %d", got)
	}
	if got := p.DeepestHit("a", levels, now.Add(2*time.Minute)); got != 0 {
		t.Fatalf("entries must expire after the TTL, got %d", got)
	}
	for h := uint64(10); h < 14; h++ {
		p.Record("a", []Level{{Hash: h, Tokens: 1}}, now)
	}
	if p.Len() != 3 {
		t.Fatalf("index must be bounded to 3 entries, has %d", p.Len())
	}
}

func TestCostModelConverges(t *testing.T) {
	m := &CostModel{BaseMs: 30, MsPerToken: 0.05}
	rnd := rand.New(rand.NewSource(3))
	for i := 0; i < 400; i++ {
		tokens := float64(rnd.Intn(8000))
		if i%2 == 0 { // 一半是短 prompt 样本，用来标定固定开销
			tokens = float64(rnd.Intn(200))
		}
		m.Observe(tokens, 50+0.2*tokens, 0.1)
	}
	if math.Abs(m.BaseMs-50)/50 > 0.15 || math.Abs(m.MsPerToken-0.2)/0.2 > 0.1 {
		t.Fatalf("model did not converge: %+v", m)
	}
}

func TestOutputPredictor(t *testing.T) {
	p := NewOutputPredictor(0.5, 200, 2)
	p.Observe(1, 40)
	p.Observe(1, 60)
	if got := p.Predict(1, 0); got != 50 {
		t.Fatalf("per-template estimate = %d, want 50", got)
	}
	if got := p.Predict(9, 30); got != 30 {
		t.Fatalf("estimate must be capped by max_tokens, got %d", got)
	}
}

func TestBreakerTripsOnSlowNodeAndRecovers(t *testing.T) {
	cfg := BreakerConfig{SlowRatio: 2.5, ErrorRate: 0.5, MinSamples: 5, Alpha: 0.3, BaseCooldown: 5 * time.Second, MaxCooldown: time.Minute}
	b := &Breaker{}
	now := time.Unix(0, 0)
	for i := 0; i < 5; i++ {
		b.Observe(now, false, 120, 30, false, cfg)
	}
	if b.State != Open {
		t.Fatalf("a node 4x slower than the fleet must trip, state %v tpot %.1f", b.State, b.TPOTMs)
	}
	if ok, _ := b.Allow(now.Add(time.Second)); ok {
		t.Fatal("open breaker must reject during cooldown")
	}
	ok, probe := b.Allow(now.Add(6 * time.Second))
	if !ok || !probe || b.State != HalfOpen {
		t.Fatal("after cooldown one probe must be allowed")
	}
	b.ProbeInflight = true
	if ok, _ := b.Allow(now.Add(6 * time.Second)); ok {
		t.Fatal("only one probe at a time")
	}
	b.Observe(now.Add(7*time.Second), false, 120, 30, true, cfg)
	if b.State != Open || b.Cooldown != 10*time.Second {
		t.Fatalf("failed probe must reopen with doubled cooldown, state %v cooldown %v", b.State, b.Cooldown)
	}
	b.Allow(now.Add(20 * time.Second))
	b.Observe(now.Add(21*time.Second), false, 32, 30, true, cfg)
	if b.State != Closed || b.TPOTMs != 32 {
		t.Fatalf("successful probe must close the breaker and reset stats: %+v", b)
	}
}

func TestBreakerTripsOnErrors(t *testing.T) {
	cfg := BreakerConfig{SlowRatio: 2.5, ErrorRate: 0.5, MinSamples: 3, Alpha: 0.5, BaseCooldown: time.Second, MaxCooldown: time.Minute}
	b := &Breaker{}
	for i := 0; i < 3; i++ {
		b.Observe(time.Unix(0, 0), true, 0, 0, false, cfg)
	}
	if b.State != Open {
		t.Fatalf("error rate %.2f must trip the breaker", b.ErrRate)
	}
	if got := FleetMedianTPOT([]*Breaker{{TPOTMs: 10, Samples: 5}, {TPOTMs: 30, Samples: 5}, {TPOTMs: 99, Samples: 1}}, 3); got != 20 {
		t.Fatalf("median = %v, want 20 (ignoring under-sampled breakers)", got)
	}
}

func endpoint(addr string, inflight int) Endpoint {
	return Endpoint{Address: addr, Inflight: inflight, KVUsage: 0.2, Model: &CostModel{BaseMs: 30, MsPerToken: 0.12}}
}

var selectCfg = SelectConfig{LoadFactor: 1.25, MinCapacity: 4, AffinityTokens: 1024, ToleranceMs: 300, DecodeMsPerSeq: 0.3, KVHighWatermark: 0.9}

func docRequest(hash uint64) Features {
	return Features{
		PromptTokens: 8300,
		Levels:       []Level{{Name: "template", Hash: 1, Tokens: 800}, {Name: "document", Hash: hash, Tokens: 8200}},
	}
}

func TestSelectPrefersReplicaWithCachedPrefix(t *testing.T) {
	now := time.Unix(0, 0)
	presence := NewPresenceIndex(time.Hour, 100)
	req := docRequest(77)
	presence.Record("b", req.Levels, now)
	eps := []Endpoint{endpoint("a", 2), endpoint("b", 3), endpoint("c", 2)}
	d, ok := Select(eps, req, presence, NewRing([]string{"a", "b", "c"}, 32), selectCfg, now, rand.New(rand.NewSource(1)))
	if !ok || d.Address != "b" || d.Reason != ReasonPrefixHit || d.ExpectedHitTokens != 8200 || d.Uncached != 100 {
		t.Fatalf("should pick the replica holding the 8K-token prefix: %+v", d)
	}
}

// 低负载时 ⌈(1+ε)·平均值⌉ 只有 1：没有绝对下限的话，持有者只要有一个在途请求就会被视为"满"，亲和失效。
func TestSelectKeepsAffinityAtLowLoad(t *testing.T) {
	now := time.Unix(0, 0)
	presence := NewPresenceIndex(time.Hour, 100)
	req := docRequest(77)
	presence.Record("b", req.Levels, now)
	eps := []Endpoint{endpoint("a", 0), endpoint("b", 1), endpoint("c", 0)}
	if d, _ := Select(eps, req, presence, nil, selectCfg, now, rand.New(rand.NewSource(1))); d.Address != "b" {
		t.Fatalf("with one request in flight the holder must keep the prefix: %+v", d)
	}
	strict := selectCfg
	strict.MinCapacity = 0
	if d, _ := Select(eps, req, presence, nil, strict, now, rand.New(rand.NewSource(1))); d.Address == "b" {
		t.Fatalf("without a minimum capacity the plain CHWBL cap (1) excludes the holder: %+v", d)
	}
}

// 持有者正忙（排队 prefill 很多）但未超过有界负载上限时，仍然选择持有者，避免文档在多个副本上重复缓存。
func TestSelectKeepsAffinityToBusyHolderWithinCap(t *testing.T) {
	now := time.Unix(0, 0)
	presence := NewPresenceIndex(time.Hour, 100)
	req := docRequest(77)
	presence.Record("b", req.Levels, now)
	busy := endpoint("b", 3)
	busy.PendingPrefill = 20000
	d, _ := Select([]Endpoint{endpoint("a", 2), busy, endpoint("c", 2)}, req, presence, nil, selectCfg, now, rand.New(rand.NewSource(1)))
	if d.Address != "b" || d.Reason != ReasonPrefixHit {
		t.Fatalf("a busy holder within the load cap must keep the request: %+v", d)
	}
}

// 最深的文档层是新的，但较浅的长历史层在某副本上：选择该副本（多轮对话的长历史同样值得复用）。
func TestSelectFallsBackToShallowerHeldLevel(t *testing.T) {
	now := time.Unix(0, 0)
	presence := NewPresenceIndex(time.Hour, 100)
	levels := []Level{{Name: "template", Hash: 1, Tokens: 800}, {Name: "history", Hash: 2, Tokens: 3000}, {Name: "document", Hash: 3, Tokens: 9000}}
	presence.Record("c", levels[:2], now)
	req := Features{PromptTokens: 9100, Levels: levels}
	d, _ := Select([]Endpoint{endpoint("a", 0), endpoint("b", 0), endpoint("c", 0)}, req, presence, NewRing([]string{"a", "b", "c"}, 32), selectCfg, now, rand.New(rand.NewSource(1)))
	if d.Address != "c" || d.Reason != ReasonPrefixHit || d.ExpectedHitTokens != 3000 {
		t.Fatalf("should reuse the replica holding the long history: %+v", d)
	}
}

func TestSelectBoundedLoadOverridesCacheAffinity(t *testing.T) {
	now := time.Unix(0, 0)
	presence := NewPresenceIndex(time.Hour, 100)
	req := docRequest(77)
	presence.Record("b", req.Levels, now)
	// 总在途 12+1，上界 ⌈1.25×13/3⌉ = 6：b 已有 10 个在途请求，必须让出去
	ring := NewRing([]string{"a", "b", "c"}, 32)
	eps := []Endpoint{endpoint("a", 1), endpoint("b", 10), endpoint("c", 1)}
	d, _ := Select(eps, req, presence, ring, selectCfg, now, rand.New(rand.NewSource(1)))
	want, _ := ring.Home(req.Levels[1].Hash, func(addr string) bool { return addr != "b" })
	if d.Address == "b" || d.Address != want || d.Reason != ReasonAffinity {
		t.Fatalf("an overloaded holder must spill to the next replica on the ring (%s): %+v", want, d)
	}
}

func TestSelectNewPrefixGoesToConsistentHome(t *testing.T) {
	now := time.Unix(0, 0)
	ring := NewRing([]string{"a", "b", "c"}, 64)
	homes := map[string]int{}
	for doc := uint64(100); doc < 160; doc++ {
		req := docRequest(doc)
		want, _ := ring.Home(req.Levels[1].Hash, func(string) bool { return true })
		for trial := 0; trial < 2; trial++ { // 两个互不共享状态的 worker 算出同一个家
			d, _ := Select([]Endpoint{endpoint("a", 0), endpoint("b", 0), endpoint("c", 0)}, req,
				NewPresenceIndex(time.Hour, 100), ring, selectCfg, now, rand.New(rand.NewSource(int64(trial))))
			if d.Address != want || d.Reason != ReasonAffinity {
				t.Fatalf("doc %d: got %+v, want home %s", doc, d, want)
			}
		}
		homes[want]++
	}
	if len(homes) != 3 {
		t.Fatalf("different documents should spread over all replicas: %v", homes)
	}
}

// 开启容忍度时：新前缀的家副本 prefill 积压过多（预测代价高出容忍度），按代价选择积压少的副本。
func TestSelectNewPrefixAvoidsBackloggedHome(t *testing.T) {
	now := time.Unix(0, 0)
	ring := NewRing([]string{"a", "b", "c"}, 64)
	req := docRequest(4242)
	home, _ := ring.Home(req.Levels[1].Hash, func(string) bool { return true })
	eps := []Endpoint{endpoint("a", 1), endpoint("b", 1), endpoint("c", 1)}
	for i := range eps {
		if eps[i].Address == home {
			eps[i].PendingPrefill = 20000 // 约 2.4s 的 prefill 积压
		}
	}
	d, _ := Select(eps, req, NewPresenceIndex(time.Hour, 10), ring, selectCfg, now, rand.New(rand.NewSource(1)))
	if d.Address == home || d.Reason != ReasonCost {
		t.Fatalf("a cold prefix must not queue behind a backlogged home %s: %+v", home, d)
	}
	strict := selectCfg
	strict.ToleranceMs = -1 // 默认：总是选家，保证放置一致
	if d, _ := Select(eps, req, NewPresenceIndex(time.Hour, 10), ring, strict, now, rand.New(rand.NewSource(1))); d.Address != home {
		t.Fatalf("with tolerance disabled the home must always win: %+v", d)
	}
}

func TestSelectShortPromptUsesLeastCost(t *testing.T) {
	now := time.Unix(0, 0)
	req := Features{PromptTokens: 900, Levels: []Level{{Name: "template", Hash: 1, Tokens: 800}}}
	eps := []Endpoint{endpoint("a", 3), endpoint("b", 0), endpoint("c", 2)}
	d, _ := Select(eps, req, NewPresenceIndex(time.Hour, 10), NewRing([]string{"a", "b", "c"}, 32), selectCfg, now, rand.New(rand.NewSource(1)))
	if d.Address != "b" || d.Reason != ReasonCost {
		t.Fatalf("short prompts should not be pinned by affinity: %+v", d)
	}
}

func TestAdmissible(t *testing.T) {
	req := Features{PromptTokens: 6000}
	cases := []struct {
		name string
		e    Endpoint
		hit  int
		want bool
	}{
		{"healthy", Endpoint{KVUsage: 0.3, KVCapacityTokens: 100000}, 0, true},
		{"above watermark", Endpoint{KVUsage: 0.95, KVCapacityTokens: 100000}, 0, false},
		{"would overflow kv", Endpoint{KVUsage: 0.85, KVCapacityTokens: 10000}, 0, false},
		{"fits thanks to prefix hit", Endpoint{KVUsage: 0.85, KVCapacityTokens: 10000}, 5800, true},
		{"unknown kv", Endpoint{KVUsage: -1}, 0, true},
		{"long waiting queue", Endpoint{KVUsage: 0.1, Waiting: 20}, 0, false},
	}
	cfg := selectCfg
	cfg.MaxWaiting = 8
	for _, c := range cases {
		if got := Admissible(c.e, req, 500, c.hit, cfg); got != c.want {
			t.Errorf("%s: got %v", c.name, got)
		}
	}
}
