package sched

// CostModel 是单个副本的首 token 时延（TTFT）预测模型：
//
//	T̂ = BaseMs + MsPerToken × (排队中的 prefill token + 本请求未命中前缀缓存的 prompt token)
//
// 两个参数都用观测到的 TTFT 在线标定（EWMA）：prefill 量小的样本主要反映固定开销 BaseMs
// （调度、一个 decode step、网络），prefill 量大的样本主要反映 prefill 速度 MsPerToken。
// 副本变慢时 MsPerToken 随之上升，代价模型会自然地少给它分流量。
type CostModel struct {
	BaseMs     float64 `json:"base_ms"`
	MsPerToken float64 `json:"ms_per_token"`
	Samples    int     `json:"samples"`
}

// smallPrefillTokens 以下的样本用于标定 BaseMs，以上的用于标定 MsPerToken。
const smallPrefillTokens = 256

func clamp(v, lo, hi float64) float64 {
	return max(lo, min(hi, v))
}

func (m *CostModel) Predict(prefillTokens float64) float64 {
	return m.BaseMs + m.MsPerToken*prefillTokens
}

// Observe 用一次实际 TTFT 更新模型。prefillTokens 是该请求被转发时估计的（排队 + 未命中）prefill token 数。
func (m *CostModel) Observe(prefillTokens, ttftMs, alpha float64) {
	if ttftMs <= 0 || prefillTokens < 0 {
		return
	}
	m.Samples++
	if prefillTokens < smallPrefillTokens {
		target := ttftMs - m.MsPerToken*prefillTokens
		m.BaseMs = clamp((1-alpha)*m.BaseMs+alpha*target, 0.1, 60_000)
		return
	}
	target := (ttftMs - m.BaseMs) / prefillTokens
	m.MsPerToken = clamp((1-alpha)*m.MsPerToken+alpha*target, 1e-4, 100)
}

// OutputPredictor 按业务模板（template 层哈希）在线估计输出 token 数，用于预测请求在 KV cache 中
// 的峰值占用（prompt + 输出）。没有该模板的样本时使用全局估计，结果不超过请求的 max_tokens。
type OutputPredictor struct {
	alpha   float64
	global  float64
	byKey   map[uint64]float64
	maxKeys int
}

func NewOutputPredictor(alpha, initial float64, maxKeys int) *OutputPredictor {
	return &OutputPredictor{alpha: alpha, global: initial, byKey: map[uint64]float64{}, maxKeys: maxKeys}
}

func (p *OutputPredictor) Predict(key uint64, maxTokens int) int {
	estimate, ok := p.byKey[key]
	if !ok {
		estimate = p.global
	}
	n := max(1, int(estimate+0.5))
	if maxTokens > 0 {
		n = min(n, maxTokens)
	}
	return n
}

func (p *OutputPredictor) Observe(key uint64, tokens int) {
	if tokens <= 0 {
		return
	}
	v := float64(tokens)
	p.global = (1-p.alpha)*p.global + p.alpha*v
	if old, ok := p.byKey[key]; ok {
		p.byKey[key] = (1-p.alpha)*old + p.alpha*v
		return
	}
	if len(p.byKey) >= p.maxKeys { // 简单的容量上限：满了就整体重置
		p.byKey = map[uint64]float64{}
	}
	p.byKey[key] = v
}
