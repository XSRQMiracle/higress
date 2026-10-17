package sched

import (
	"math"
	"math/rand"
	"time"
)

// Endpoint 是一次调度决策看到的副本状态。
type Endpoint struct {
	Address          string
	Waiting          float64    // vLLM waiting 队列长度（健康检查拉取，最多滞后一个检查周期）
	Running          float64    // vLLM running 请求数（decode batch 大小）
	KVUsage          float64    // KV cache 占用率（显存水位），小于 0 表示未知
	KVCapacityTokens int        // KV cache 总容量（num_gpu_blocks × block_size），0 表示未知
	Inflight         int        // 本网关转发到该副本、尚未结束的请求数（实时）
	PendingPrefill   float64    // 已转发、尚未产出首 token 的请求的未命中 prompt token 之和（实时）
	Model            *CostModel // 该副本的 TTFT 预测模型
	Probe            bool       // 熔断器半开时的探测名额
}

type SelectConfig struct {
	LoadFactor      float64 // 有界负载系数 1+ε：任一副本的在途请求数不超过 ⌈(1+ε)·平均值⌉
	AffinityTokens  int     // 前缀至少这么长才做亲和（短前缀在所有副本上都很快会被缓存，亲和得不偿失）
	ToleranceMs     float64 // 没有持有者时，家副本的预测代价比最优副本高出不超过该值才选家；小于 0 表示总是选家（默认）
	DecodeMsPerSeq  float64 // λ：每多一个并发序列，对 decode 速度（以及新请求首 token）的边际代价
	KVHighWatermark float64 // 显存水位上限：KV 占用率达到该值的副本不再接收新请求
	MaxWaiting      float64 // 后端 waiting 队列超过该值的副本不再接收新请求（0 表示不限制）
}

type Decision struct {
	Address           string  `json:"address"`
	Reason            string  `json:"reason"`
	PredictedTTFTMs   float64 `json:"predicted_ttft_ms"`
	ExpectedHitTokens int     `json:"expected_hit_tokens"`
	PrefillTokens     float64 `json:"prefill_tokens"` // 排队 + 未命中 prefill，TTFT 观测到后用于标定模型
	Uncached          int     `json:"uncached_tokens"`
	Probe             bool    `json:"probe"`
	Candidates        int     `json:"candidates"`
}

const (
	ReasonCost      = "cost"       // 无长前缀：按预测代价选择
	ReasonPrefixHit = "prefix_hit" // 选中持有长前缀 KV 的副本
	ReasonAffinity  = "affinity"   // 新的长前缀：一致性哈希确定的家副本（满则顺延）
)

// Admissible 是显存水位准入：副本当前 KV 占用低于水位线，并且按"未命中的 prompt + 预测输出长度"
// 估算，接收本请求后 KV 不会被撑满（撑满会触发 vLLM 抢占重算，造成严重的尾延迟）。
// hitTokens 是该副本上预计命中的前缀长度，这部分 KV 已经存在，不再额外占用。
func Admissible(e Endpoint, req Features, predictedOutput, hitTokens int, cfg SelectConfig) bool {
	if cfg.MaxWaiting > 0 && e.Waiting > cfg.MaxWaiting {
		return false
	}
	if e.KVUsage < 0 {
		return true
	}
	if e.KVUsage >= cfg.KVHighWatermark {
		return false
	}
	if e.KVCapacityTokens > 0 {
		need := max(req.PromptTokens-hitTokens, 0) + predictedOutput
		return e.KVUsage+float64(need)/float64(e.KVCapacityTokens) < 1
	}
	return true
}

type scored struct {
	endpoint Endpoint
	hit      int
	uncached int
	prefill  float64
	ttft     float64
	cost     float64
}

// Select 在已通过健康、熔断与准入过滤的候选副本中选择一个：
//
//  1. 有界负载（CHWBL）：在途请求数已达 ⌈(1+ε)·(总在途+1)/n⌉ 的副本视为"满"，不参与选择（全部满时不限制）；
//  2. 长前缀亲和：从最深的长前缀（≥ AffinityTokens）开始逐层查找持有该前缀 KV 的未满副本，找到就在持有者中
//     选预测代价最小的（prefix_hit）。这里刻意不让"持有者正忙"推翻亲和：贪心地把长前缀请求派往暂时空闲的
//     副本，会让同一份文档在多个副本上重复缓存、互相挤占 KV，冷 prefill 反而更多。负载均衡由有界负载约束保证。
//     一层都没有持有者时（新前缀，或持有者已满），用一致性哈希把它送到"家"副本（家已满则沿环顺延，affinity），
//     保证不同 worker、不同网关的放置一致。ToleranceMs >= 0 时，家的预测代价超过最优副本 + ToleranceMs 则改按
//     代价选择；长上下文 RAG 压测中这会削弱局部性（前缀命中 69.5% → 61.6%），所以默认关闭。
//  3. 没有长前缀的请求按预测代价选择：J_i = T̂_i + λ·max(running_i, inflight_i)，
//     其中 T̂_i = BaseMs_i + MsPerToken_i·(PendingPrefill_i + PromptTokens − Ĥ_i)，Ĥ_i 是副本 i 上预计命中的前缀长度。
func Select(eps []Endpoint, req Features, presence *PresenceIndex, ring *Ring, cfg SelectConfig, now time.Time, rnd *rand.Rand) (Decision, bool) {
	if len(eps) == 0 {
		return Decision{}, false
	}
	total := 1
	for _, e := range eps {
		total += e.Inflight
	}
	capacity := int(math.Ceil(cfg.LoadFactor * float64(total) / float64(len(eps))))
	within := make([]Endpoint, 0, len(eps))
	for _, e := range eps {
		if e.Inflight < capacity {
			within = append(within, e)
		}
	}
	if len(within) == 0 {
		within = eps
	}

	scores := make([]scored, len(within))
	byAddr := make(map[string]int, len(within))
	for i, e := range within {
		s := scored{endpoint: e}
		if presence != nil {
			s.hit = presence.DeepestHit(e.Address, req.Levels, now)
		}
		s.uncached = max(req.PromptTokens-s.hit, 1)
		s.prefill = e.PendingPrefill + float64(s.uncached)
		s.ttft = e.Model.Predict(s.prefill)
		s.cost = s.ttft + cfg.DecodeMsPerSeq*math.Max(e.Running, float64(e.Inflight))
		scores[i] = s
		byAddr[e.Address] = i
	}
	// pickMin 返回满足条件的候选中代价最小者的下标（随机打破平局），没有满足条件的返回 -1。
	pickMin := func(accept func(s scored) bool) int {
		best, ties := -1, 0
		for i, s := range scores {
			if !accept(s) {
				continue
			}
			switch {
			case best < 0 || s.cost < scores[best].cost-1e-9:
				best, ties = i, 1
			case math.Abs(s.cost-scores[best].cost) <= 1e-9:
				ties++
				if rnd.Intn(ties) == 0 {
					best = i
				}
			}
		}
		return best
	}
	decision := func(i int, reason string) (Decision, bool) {
		s := scores[i]
		return Decision{
			Address:           s.endpoint.Address,
			Reason:            reason,
			PredictedTTFTMs:   s.ttft,
			ExpectedHitTokens: s.hit,
			PrefillTokens:     s.prefill,
			Uncached:          s.uncached,
			Probe:             s.endpoint.Probe,
			Candidates:        len(eps),
		}, true
	}

	if presence != nil {
		deepestLong := -1
		for i := len(req.Levels) - 1; i >= 0 && req.Levels[i].Tokens >= cfg.AffinityTokens; i-- {
			if deepestLong < 0 {
				deepestLong = i
			}
			need := req.Levels[i].Tokens
			if holder := pickMin(func(s scored) bool { return s.hit >= need }); holder >= 0 {
				return decision(holder, ReasonPrefixHit)
			}
		}
		if deepestLong >= 0 && ring != nil {
			home, ok := ring.Home(req.Levels[deepestLong].Hash, func(addr string) bool { _, in := byAddr[addr]; return in })
			if ok {
				best := pickMin(func(scored) bool { return true })
				if cfg.ToleranceMs < 0 || scores[byAddr[home]].cost <= scores[best].cost+cfg.ToleranceMs {
					return decision(byAddr[home], ReasonAffinity)
				}
				return decision(best, ReasonCost)
			}
		}
	}
	return decision(pickMin(func(scored) bool { return true }), ReasonCost)
}
