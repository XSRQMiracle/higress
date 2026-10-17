package sched

import (
	"sort"
	"time"
)

type BreakerState int

const (
	Closed BreakerState = iota
	Open
	HalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	}
	return "closed"
}

type BreakerConfig struct {
	SlowRatio    float64       // 副本 TPOT 的 EWMA 超过集群中位数的倍数即视为慢节点
	ErrorRate    float64       // 错误率（5xx / 连接失败）EWMA 阈值
	MinSamples   int           // 样本数不足时不判定，避免冷启动误判
	Alpha        float64       // EWMA 平滑系数
	BaseCooldown time.Duration // 第一次熔断的冷却时间
	MaxCooldown  time.Duration // 连续熔断时冷却时间指数增长的上限
}

// Breaker 是单个副本的熔断器：CLOSED → OPEN → HALF_OPEN → CLOSED / OPEN。
//
// 判据用 TPOT 而不是 TTFT：TTFT 强烈依赖 prompt 长度和前缀命中，TPOT 主要由副本自身的解码速度决定，
// 与集群中位数比较能消除负载整体升高的影响，只挑出"比同伴慢得多"的节点。
type Breaker struct {
	State         BreakerState  `json:"-"`
	TPOTMs        float64       `json:"tpot_ms"`
	ErrRate       float64       `json:"error_rate"`
	Samples       int           `json:"samples"`
	Trips         int           `json:"trips"`
	OpenedAt      time.Time     `json:"-"`
	Cooldown      time.Duration `json:"-"`
	ProbeInflight bool          `json:"probe_inflight"`
}

// Allow 返回副本能否接收新请求，以及该请求是否作为半开状态下的探测请求。
// OPEN 状态冷却结束后转为 HALF_OPEN，同一时刻只放行一个探测请求。
func (b *Breaker) Allow(now time.Time) (allowed, probe bool) {
	switch b.State {
	case Closed:
		return true, false
	case Open:
		if now.Sub(b.OpenedAt) < b.Cooldown {
			return false, false
		}
		b.State = HalfOpen
	}
	if b.ProbeInflight {
		return false, false
	}
	return true, true
}

func (b *Breaker) trip(now time.Time, cfg BreakerConfig) {
	cooldown := cfg.BaseCooldown
	if b.State != Closed && b.Cooldown > 0 { // 半开探测失败：冷却时间翻倍
		cooldown = min(b.Cooldown*2, cfg.MaxCooldown)
	}
	b.State = Open
	b.OpenedAt = now
	b.Cooldown = cooldown
	b.Trips++
}

func (b *Breaker) slow(tpotMs, fleetMedianMs float64, cfg BreakerConfig) bool {
	return fleetMedianMs > 0 && tpotMs > cfg.SlowRatio*fleetMedianMs
}

// Observe 记录一次请求结果。tpotMs <= 0 表示没有 TPOT 样本（非流式或输出过短）。
func (b *Breaker) Observe(now time.Time, failed bool, tpotMs, fleetMedianMs float64, probe bool, cfg BreakerConfig) {
	if probe {
		b.ProbeInflight = false
	}
	errSample := 0.0
	if failed {
		errSample = 1
	}
	b.ErrRate = (1-cfg.Alpha)*b.ErrRate + cfg.Alpha*errSample
	if tpotMs > 0 {
		if b.TPOTMs == 0 {
			b.TPOTMs = tpotMs
		} else {
			b.TPOTMs = (1-cfg.Alpha)*b.TPOTMs + cfg.Alpha*tpotMs
		}
	}
	b.Samples++
	switch b.State {
	case HalfOpen:
		if !probe {
			return
		}
		if failed || b.slow(tpotMs, fleetMedianMs, cfg) {
			b.trip(now, cfg)
			return
		}
		// 探测成功：恢复，并用这次的样本重置统计，避免旧的慢样本立刻再次触发熔断
		b.State = Closed
		b.Cooldown = 0
		b.ErrRate = 0
		b.TPOTMs = tpotMs
		b.Samples = 1
	case Closed:
		if b.Samples >= cfg.MinSamples && (b.ErrRate > cfg.ErrorRate || b.slow(b.TPOTMs, fleetMedianMs, cfg)) {
			b.trip(now, cfg)
		}
	}
}

// FleetMedianTPOT 计算有足够样本的副本 TPOT 中位数，作为慢节点判定的基准。
func FleetMedianTPOT(breakers []*Breaker, minSamples int) float64 {
	var values []float64
	for _, b := range breakers {
		if b.TPOTMs > 0 && b.Samples >= minSamples {
			values = append(values, b.TPOTMs)
		}
	}
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	mid := len(values) / 2
	if len(values)%2 == 1 {
		return values[mid]
	}
	return (values[mid-1] + values[mid]) / 2
}
