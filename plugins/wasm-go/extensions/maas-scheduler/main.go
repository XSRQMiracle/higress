// maas-scheduler：MaaS 网关的推理副本调度插件（阶段 2）。
//
// 依赖两项 Higress 扩展能力：
//   - proxywasm.GetUpstreamHosts()：读取 cluster 中每个副本的健康状态，以及健康检查拉到的 vLLM /metrics
//     （cluster 的 health_check 需开启 store_metrics: true）；
//   - proxywasm.SetUpstreamOverrideHost()：绕过 Envoy 自带负载均衡，把请求钉到选中的副本。
//
// 默认策略 cost 的决策流程（核心算法在 sched 包）：
//  1. 特征提取：分层稳定前缀（模板 / 历史 / 文档头部）、prompt 长度、按模板预测的输出长度；
//  2. 过滤：健康检查 → 熔断器（慢节点 / 高错误率的副本暂时摘除，冷却后半开探测）→
//     显存水位准入（KV 占用超过水位线，或接收本请求后 KV 会被撑满的副本不接收新请求）；
//  3. 选择：有界负载约束下最小化预测代价 J = T̂_TTFT + λ·并发数；长前缀第一次出现时用一致性哈希
//     确定"家"副本，使共享该前缀的后续请求都能命中同一份 KV 缓存；
//  4. 排队：所有副本都未通过准入时，请求在网关排队，有容量时放行，超时后尽力而为或返回 429；
//  5. 反馈：首 token 时延标定代价模型，TPOT 与错误率驱动熔断器，usage 中的实际前缀命中纠正前缀索引，
//     输出 token 数更新输出长度预测。
//
// 另保留 header / round_robin / least_waiting 三种策略作为对照组；配置 policyHeader 后可按请求切换。
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/maas-scheduler/sched"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
)

const (
	pluginName          = "maas-scheduler"
	ctxStateKey         = "maas_sched_state"
	maxRequestBodyBytes = 16 << 20
	tailKeepBytes       = 1024

	reasonTarget          = "target_header"
	reasonTargetNotFound  = "target_not_found"
	reasonNoHealthyHost   = "no_healthy_host"
	reasonHostsError      = "hosts_unavailable"
	reasonOverrideFailed  = "override_failed"
	reasonBestEffort      = "best_effort"
	reasonBreakerFailOpen = "breaker_fail_open"
	reasonRejected        = "rejected"
	reasonColdSpill       = "cold_spill" // 选中的副本已有冷 prefill，换到有空闲名额的副本

	// 请求在网关排队的原因（响应头 x-maas-wait-reason）
	waitKVWatermark = "kv_watermark"
	waitCoalesce    = "coalesce"  // 等同一长前缀的冷 prefill 产出首 token
	waitColdSlot    = "cold_slot" // 所有副本的冷 prefill 名额都已占满
)

// requestState 记录单个请求从调度到响应结束的全过程。
type requestState struct {
	ctx             wrapper.HttpContext
	policy          Policy
	kind            string
	features        sched.Features
	predictedOutput int
	queued          bool
	enqueuedAt      time.Time
	done            bool
	decision        sched.Decision
	picked          string
	reason          string
	releasedAt      time.Time
	queueMs         int64
	firstByteAt     time.Time
	pendingTokens   int64
	probe           bool
	waitReason      string
	coldSlot        bool   // 占用了所选副本的一个冷 prefill 名额（首 token 到达时归还）
	warmingHash     uint64 // 本请求是该长前缀的冷 prefill 领头者，首 token 到达时清除 warming 标记
	warmingAddr     string
	status          int
	events          int
	tail            []byte
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

func getState(ctx wrapper.HttpContext) *requestState {
	st, _ := ctx.GetContext(ctxStateKey).(*requestState)
	return st
}

func onHttpRequestHeaders(ctx wrapper.HttpContext, config Config) types.Action {
	path := wrapper.GetRequestPathWithoutQuery()
	if config.debugPath != "" && path == config.debugPath {
		ctx.DontReadRequestBody()
		serveDebug(config)
		return types.ActionContinue
	}
	st := &requestState{ctx: ctx, policy: config.policy, kind: "chat"}
	if strings.HasSuffix(path, "/completions") && !strings.HasSuffix(path, "/chat/completions") {
		st.kind = "completion"
	}
	ctx.SetContext(ctxStateKey, st)
	if target, _ := proxywasm.GetHttpRequestHeader(config.targetHeader); target != "" {
		ctx.DontReadRequestBody()
		_ = proxywasm.RemoveHttpRequestHeader(config.targetHeader)
		pickTarget(st, config, target)
		return types.ActionContinue
	}
	if config.policyHeader != "" {
		if value, _ := proxywasm.GetHttpRequestHeader(config.policyHeader); value != "" {
			if p, ok := parsePolicy(value); ok {
				st.policy = p
			}
			_ = proxywasm.RemoveHttpRequestHeader(config.policyHeader)
		}
	}
	if st.policy == PolicyHeader {
		ctx.DontReadRequestBody()
		return types.ActionContinue
	}
	if !hasPositiveContentLength() {
		ctx.DontReadRequestBody()
		decide(st, config, false)
		return types.ActionContinue
	}
	// 有请求体的请求在 body 阶段决策。必须挂起请求头：若直接 Continue，
	// router 会在插件做出选择之前就选定上游主机，override 不再生效。
	ctx.SetRequestBodyBufferLimit(maxRequestBodyBytes)
	return types.HeaderStopIteration
}

func onHttpRequestBody(ctx wrapper.HttpContext, config Config, body []byte) types.Action {
	st := getState(ctx)
	if st == nil || st.picked != "" {
		return types.ActionContinue
	}
	if st.policy == PolicyCost {
		st.features = sched.ExtractFeatures(gjson.ParseBytes(body), st.kind, config.blockTokens, config.tailTokens)
		st.predictedOutput = config.state.predictor.Predict(st.features.TemplateKey, st.features.MaxTokens)
	}
	if decide(st, config, true) {
		return types.ActionContinue
	}
	if config.queueEnabled && len(config.state.queue) < config.maxQueueLength {
		st.queued = true
		st.enqueuedAt = time.Now()
		config.state.queue = append(config.state.queue, st)
		config.metrics.queued.Increment(1)
		switch st.waitReason {
		case waitCoalesce:
			config.metrics.coalesced.Increment(1)
		case waitColdSlot:
			config.metrics.coldWaits.Increment(1)
		}
		return types.ActionPause
	}
	decide(st, config, false)
	return types.ActionContinue
}

// decide 为请求选择副本并设置 override。返回 false 表示所有健康副本都没有通过显存水位 / 排队准入，
// 由调用方决定排队还是尽力而为；其余情况（包括回退到 Envoy 负载均衡）都返回 true。
func decide(st *requestState, config Config, admission bool) bool {
	hosts, err := loadHosts()
	if err != nil {
		fallback(st, config, reasonHostsError)
		return true
	}
	config.state.syncMembers(hosts, config.virtualNodes)
	switch st.policy {
	case PolicyRoundRobin:
		if picked, ok := PickRoundRobin(hosts, &config.state.rr); ok {
			override(st, config, sched.Decision{Address: picked, Reason: string(PolicyRoundRobin)})
		} else {
			fallback(st, config, reasonNoHealthyHost)
		}
		return true
	case PolicyLeastWaiting:
		inflight := make(map[string]int, len(hosts))
		for _, h := range hosts {
			inflight[h.Address] = int(sharedGet(inflightKey(h.Address)))
		}
		if picked, ok := PickLeastWaiting(hosts, inflight, config.state.rnd); ok {
			override(st, config, sched.Decision{Address: picked, Reason: string(PolicyLeastWaiting)})
		} else {
			fallback(st, config, reasonNoHealthyHost)
		}
		return true
	}

	now := time.Now()
	long, hasLong := deepestLong(st.features.Levels, config.selectCfg.AffinityTokens)
	if hasLong && config.cold.enabled {
		if p, ok := readPlacement(long.Hash); ok {
			age := time.Duration(now.UnixMilli()-p.At) * time.Millisecond
			switch {
			case p.Warming && age < config.cold.maxWarm:
				// 同一长前缀正在 p.Addr 上冷 prefill：除非本 worker 知道别的副本已经持有它，否则等首 token
				if admission && config.cold.coalesce && !heldElsewhere(config.state.presence, long, p.Addr, now) {
					st.waitReason = waitCoalesce
					return false
				}
			case !p.Warming && age < config.cold.placementTTL:
				// 其他 worker（或本 worker 之前）放置的长前缀：导入本 worker 的前缀索引，让请求找到持有者
				config.state.presence.Record(p.Addr, levelsUpTo(st.features.Levels, long), time.UnixMilli(p.At))
			}
		}
	}
	eps, healthy, breakerBlocked := buildEndpoints(st, config, hosts, now, admission, true)
	reason := ""
	if len(eps) == 0 && healthy > 0 && breakerBlocked == healthy {
		// 熔断器把所有健康副本都摘掉了：宁可放行也不能让整个服务不可用
		eps, _, _ = buildEndpoints(st, config, hosts, now, admission, false)
		reason = reasonBreakerFailOpen
	}
	if len(eps) == 0 {
		if healthy > 0 && admission {
			st.waitReason = waitKVWatermark
			return false
		}
		fallback(st, config, reasonNoHealthyHost)
		return true
	}
	d, ok := sched.Select(eps, st.features, config.state.presence, config.state.ring, config.selectCfg, now, config.state.rnd)
	if !ok {
		fallback(st, config, reasonNoHealthyHost)
		return true
	}

	// 冷 prefill 并发限制：一个副本同时做多个冷 prefill 不会更快（chunked prefill 共享同一份 token 预算），
	// 只会让后到的请求排在前面的 prefill 后面。把新的冷 prefill 交给有空闲名额的副本，都满了就在网关排队，
	// 等任一副本的冷 prefill 产出首 token（迟绑定）。前缀命中的请求不受限制，否则会被迫离开持有者。
	cold := config.cold.enabled && d.Uncached >= config.cold.minTokens
	if cold {
		if admission && reason == "" && d.Reason != sched.ReasonPrefixHit {
			if !sharedTryAcquire(coldKey(d.Address), config.cold.maxPerReplica) {
				free := make([]sched.Endpoint, 0, len(eps))
				for _, e := range eps {
					if e.Address != d.Address && sharedGet(coldKey(e.Address)) < config.cold.maxPerReplica {
						free = append(free, e)
					}
				}
				alt, found := sched.Select(free, st.features, config.state.presence, config.state.ring, config.selectCfg, now, config.state.rnd)
				if !found || !sharedTryAcquire(coldKey(alt.Address), config.cold.maxPerReplica) {
					st.waitReason = waitColdSlot
					return false
				}
				d = alt
				d.Reason = reasonColdSpill
			}
		} else {
			sharedAdd(coldKey(d.Address), 1) // 计数但不受上限约束
		}
	}
	// 本请求要在所选副本上冷 prefill 这段长前缀：竞选领头者。另一个 worker 刚刚抢先时，放弃名额改为跟随者
	warming := hasLong && config.cold.enabled && cold && d.ExpectedHitTokens < long.Tokens
	claim := warming && admission && config.cold.coalesce
	if claim && !claimWarming(long.Hash, d.Address, now.UnixMilli(), config.cold.maxWarm) {
		sharedAdd(coldKey(d.Address), -1)
		st.waitReason = waitCoalesce
		return false
	}
	if reason != "" {
		d.Reason = reason
	} else if !admission {
		d.Reason = reasonBestEffort
	}
	if !override(st, config, d) {
		if cold {
			sharedAdd(coldKey(d.Address), -1)
		}
		if claim {
			finishWarming(long.Hash, d.Address, now.UnixMilli())
		}
		return true
	}
	st.coldSlot = cold
	if hasLong && config.cold.enabled {
		switch {
		case warming:
			if !claim {
				writePlacement(placement{Hash: long.Hash, Addr: d.Address, At: now.UnixMilli(), Warming: true})
			}
			st.warmingHash, st.warmingAddr = long.Hash, d.Address
		default: // 刷新放置提示，供其他 worker 找到持有者
			writePlacement(placement{Hash: long.Hash, Addr: d.Address, At: now.UnixMilli()})
		}
	}
	return true
}

// deepestLong 返回值得做亲和的最深前缀层（各层按深度递增，最深的一层也最长）。
func deepestLong(levels []sched.Level, minTokens int) (sched.Level, bool) {
	if n := len(levels); n > 0 && levels[n-1].Tokens >= minTokens {
		return levels[n-1], true
	}
	return sched.Level{}, false
}

func levelsUpTo(levels []sched.Level, last sched.Level) []sched.Level {
	for i, l := range levels {
		if l.Hash == last.Hash {
			return levels[:i+1]
		}
	}
	return levels
}

func heldElsewhere(presence *sched.PresenceIndex, level sched.Level, warmingAddr string, now time.Time) bool {
	for _, addr := range presence.Holders(level, now) {
		if addr != warmingAddr {
			return true
		}
	}
	return false
}

// endColdPrefill 在冷 prefill 产出首 token（或请求结束）时归还名额、清除 warming 标记，
// 并立即重新评估排队中的请求（合并等待者与等名额的冷 prefill）。
// processQueue 会切换生效上下文，最后切回当前请求，保证调用方之后的 hostcall 仍作用于它。
func endColdPrefill(st *requestState, config Config) {
	if !st.coldSlot && st.warmingHash == 0 {
		return
	}
	if st.coldSlot {
		sharedAdd(coldKey(st.picked), -1)
		st.coldSlot = false
	}
	if st.warmingHash != 0 {
		finishWarming(st.warmingHash, st.warmingAddr, time.Now().UnixMilli())
		st.warmingHash = 0
	}
	processQueue(config)
	st.ctx.Path()
}

// buildEndpoints 把健康副本转成调度候选，依次经过熔断器与显存水位准入过滤。
func buildEndpoints(st *requestState, config Config, hosts []HostSnapshot, now time.Time, admission, useBreaker bool) (eps []sched.Endpoint, healthy, breakerBlocked int) {
	for _, h := range hosts {
		if !h.Healthy() {
			continue
		}
		healthy++
		probe := false
		if useBreaker && config.breakerEnabled {
			allowed, p := config.state.breaker(h.Address).Allow(now)
			if !allowed {
				breakerBlocked++
				continue
			}
			probe = p
		}
		kv := -1.0
		if h.HasKV {
			kv = h.KVUsage
		}
		e := sched.Endpoint{
			Address:          h.Address,
			Waiting:          h.Waiting,
			Running:          h.Running,
			KVUsage:          kv,
			KVCapacityTokens: h.KVCapacity,
			Inflight:         int(sharedGet(inflightKey(h.Address))),
			PendingPrefill:   float64(sharedGet(prefillKey(h.Address))),
			Model:            config.state.model(h.Address, config.initialModel),
			Probe:            probe,
		}
		if admission {
			hit := config.state.presence.DeepestHit(h.Address, st.features.Levels, now)
			if !sched.Admissible(e, st.features, st.predictedOutput, hit, config.selectCfg) {
				continue
			}
		}
		eps = append(eps, e)
	}
	return eps, healthy, breakerBlocked
}

func pickTarget(st *requestState, config Config, target string) {
	hosts, err := loadHosts()
	if err != nil {
		fallback(st, config, reasonHostsError)
		return
	}
	for _, host := range hosts {
		if host.Address == target {
			override(st, config, sched.Decision{Address: target, Reason: reasonTarget})
			return
		}
	}
	fallback(st, config, reasonTargetNotFound)
}

func override(st *requestState, config Config, d sched.Decision) bool {
	if err := proxywasm.SetUpstreamOverrideHost([]byte(d.Address)); err != nil {
		fallback(st, config, reasonOverrideFailed)
		return false
	}
	now := time.Now()
	st.picked, st.reason, st.decision, st.releasedAt = d.Address, d.Reason, d, now
	if !st.enqueuedAt.IsZero() {
		st.queueMs = now.Sub(st.enqueuedAt).Milliseconds()
	}
	sharedAdd(inflightKey(d.Address), 1)
	if d.Uncached > 0 {
		st.pendingTokens = int64(d.Uncached)
		sharedAdd(prefillKey(d.Address), st.pendingTokens)
	}
	if d.Probe {
		st.probe = true
		config.state.breaker(d.Address).ProbeInflight = true
		config.metrics.probes.Increment(1)
	}
	if len(st.features.Levels) > 0 {
		config.state.presence.Record(d.Address, st.features.Levels, now)
	}
	config.metrics.decisions.Increment(1)
	switch d.Reason {
	case sched.ReasonAffinity:
		config.metrics.affinity.Increment(1)
	case sched.ReasonPrefixHit:
		config.metrics.prefixHits.Increment(1)
	case reasonColdSpill:
		config.metrics.coldSpills.Increment(1)
	}
	return true
}

// fallback 不做 override，由 Envoy cluster 自身的负载均衡兜底，保证调度插件失效时请求仍可用。
func fallback(st *requestState, config Config, reason string) {
	st.reason = reason
	config.metrics.fallbacks.Increment(1)
	proxywasm.LogDebugf("[%s] fallback to envoy load balancing: %s", pluginName, reason)
}

func releasePending(st *requestState) {
	if st.pendingTokens > 0 {
		sharedAdd(prefillKey(st.picked), -st.pendingTokens)
		st.pendingTokens = 0
	}
}

func onHttpResponseHeaders(ctx wrapper.HttpContext, config Config) types.Action {
	st := getState(ctx)
	if st == nil {
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	if config.debugHeaders && st.reason != "" {
		_ = proxywasm.AddHttpResponseHeader("x-maas-reason", st.reason)
	}
	if st.picked == "" {
		ctx.DontReadResponseBody()
		return types.ActionContinue
	}
	status, _ := proxywasm.GetHttpResponseHeader(":status")
	st.status, _ = strconv.Atoi(status)
	if config.debugHeaders {
		_ = proxywasm.AddHttpResponseHeader("x-maas-picked", st.picked)
		if st.policy == PolicyCost {
			_ = proxywasm.AddHttpResponseHeader("x-maas-predicted-ttft-ms", strconv.FormatFloat(st.decision.PredictedTTFTMs, 'f', 1, 64))
			_ = proxywasm.AddHttpResponseHeader("x-maas-expected-hit-tokens", strconv.Itoa(st.decision.ExpectedHitTokens))
		}
		if !st.enqueuedAt.IsZero() {
			_ = proxywasm.AddHttpResponseHeader("x-maas-queue-ms", strconv.FormatInt(st.queueMs, 10))
			_ = proxywasm.AddHttpResponseHeader("x-maas-wait-reason", st.waitReason)
		}
	}
	if contentType, _ := proxywasm.GetHttpResponseHeader("content-type"); !strings.Contains(contentType, "text/event-stream") {
		ctx.DontReadResponseBody() // 非流式响应观测不到首 token，不参与时延标定
		releasePending(st)
		endColdPrefill(st, config)
	}
	return types.ActionContinue
}

// onHttpStreamingResponseBody 只观测、不修改响应：第一个数据分块的到达时间即首 token 时间。
func onHttpStreamingResponseBody(ctx wrapper.HttpContext, config Config, chunk []byte, _ bool) []byte {
	st := getState(ctx)
	if st == nil || st.picked == "" {
		return chunk
	}
	if st.firstByteAt.IsZero() && len(chunk) > 0 {
		st.firstByteAt = time.Now()
		releasePending(st)
		if st.policy == PolicyCost && st.status == 200 {
			ttftMs := float64(st.firstByteAt.Sub(st.releasedAt).Microseconds()) / 1000
			config.state.model(st.picked, config.initialModel).Observe(st.decision.PrefillTokens, ttftMs, config.costAlpha)
		}
		endColdPrefill(st, config)
	}
	st.events += bytes.Count(chunk, []byte("data:"))
	st.tail = append(st.tail, chunk...)
	if len(st.tail) > tailKeepBytes {
		st.tail = st.tail[len(st.tail)-tailKeepBytes:]
	}
	return chunk
}

func onHttpStreamDone(ctx wrapper.HttpContext, config Config) {
	st := getState(ctx)
	if st == nil {
		return
	}
	st.done = true
	if st.picked == "" {
		if st.queued { // 排队期间客户端断开
			removeFromQueue(config.state, st)
		}
		return
	}
	releasePending(st)
	sharedAdd(inflightKey(st.picked), -1)
	if st.coldSlot { // 未产出首 token 就结束（失败或客户端断开）
		sharedAdd(coldKey(st.picked), -1)
		st.coldSlot = false
	}
	if st.warmingHash != 0 {
		finishWarming(st.warmingHash, st.warmingAddr, time.Now().UnixMilli())
		st.warmingHash = 0
	}
	now := time.Now()
	completion, cached, hasUsage := parseUsage(st.tail)
	outputTokens := completion
	if !hasUsage {
		outputTokens = max(st.events-3, 0) // role 帧、结束帧与 [DONE] 不是 token
	}
	if st.policy == PolicyCost && outputTokens > 0 {
		config.state.predictor.Observe(st.features.TemplateKey, outputTokens)
	}
	tpotMs := 0.0
	if !st.firstByteAt.IsZero() && outputTokens > 1 {
		tpotMs = float64(now.Sub(st.firstByteAt).Microseconds()) / 1000 / float64(outputTokens-1)
	}
	if config.breakerEnabled {
		b := config.state.breaker(st.picked)
		trips := b.Trips
		median := sched.FleetMedianTPOT(config.state.allBreakers(), config.breakerCfg.MinSamples)
		b.Observe(now, st.status == 0 || st.status >= 500, tpotMs, median, st.probe, config.breakerCfg)
		if b.Trips > trips {
			config.metrics.breakerTrips.Increment(1)
			proxywasm.LogWarnf("[%s] circuit opened for %s: tpot %.1fms vs fleet median %.1fms, error rate %.2f, cooldown %s",
				pluginName, st.picked, b.TPOTMs, median, b.ErrRate, b.Cooldown)
		}
	}
	// 后端实际命中的前缀远少于预期，说明 KV 已被驱逐：纠正前缀索引，避免继续把请求引向这里
	if hasUsage && st.decision.ExpectedHitTokens >= 2*config.blockTokens && cached < st.decision.ExpectedHitTokens/2 {
		config.state.presence.Invalidate(st.picked, st.features.Levels, cached)
		config.metrics.invalidations.Increment(1)
	}
	// 有请求结束意味着有容量释放，立即尝试放行排队中的请求
	processQueue(config)
}

// processQueue 重新评估排队中的请求：有副本通过准入就放行；超过最长排队时间则尽力而为或返回 429。
func processQueue(config Config) {
	state := config.state
	if len(state.queue) == 0 {
		return
	}
	now := time.Now()
	waiting := state.queue
	state.queue = nil
	for _, st := range waiting {
		if st.done || st.picked != "" || !st.queued {
			continue
		}
		st.ctx.Path() // wrapper 在这里切换生效上下文，之后的 hostcall 都作用于该请求
		if decide(st, config, true) {
			resume(st)
			continue
		}
		if now.Sub(st.enqueuedAt) < config.maxQueue {
			state.queue = append(state.queue, st)
			continue
		}
		config.metrics.queueTimeouts.Increment(1)
		if config.rejectOnTimeout {
			st.queued = false
			st.reason = reasonRejected
			config.metrics.rejected.Increment(1)
			_ = proxywasm.SendHttpResponseWithDetail(429, pluginName+".overloaded",
				[][2]string{{"content-type", "application/json"}, {"retry-after", "1"}},
				[]byte(`{"error":{"message":"all model replicas are above the KV cache watermark","type":"overloaded"}}`), -1)
			continue
		}
		decide(st, config, false)
		resume(st)
	}
}

func resume(st *requestState) {
	st.queued = false
	_ = proxywasm.ResumeHttpRequest()
}

func removeFromQueue(state *schedulerState, target *requestState) {
	kept := state.queue[:0]
	for _, st := range state.queue {
		if st != target {
			kept = append(kept, st)
		}
	}
	state.queue = kept
}

// parseUsage 从流式响应末尾的 usage 帧中提取 completion_tokens 与 cached_tokens。
func parseUsage(tail []byte) (completion, cached int, ok bool) {
	completion, ok = intAfter(tail, `"completion_tokens":`)
	cached, _ = intAfter(tail, `"cached_tokens":`)
	return completion, cached, ok
}

func intAfter(data []byte, key string) (int, bool) {
	i := bytes.LastIndex(data, []byte(key))
	if i < 0 {
		return 0, false
	}
	rest := data[i+len(key):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	n, err := strconv.Atoi(string(rest[:end]))
	return n, err == nil
}

// hasPositiveContentLength 判断是否值得等待请求体。只认正数 Content-Length（与 ai-endpoint-picker 一致）：
// wrapper.HasRequestBody() 在只有 Content-Type 时也返回 true，而没有 body 的请求一旦挂起请求头，
// Envoy 不会再回调 body 阶段，请求会一直卡住。
func hasPositiveContentLength() bool {
	value, err := proxywasm.GetHttpRequestHeader("content-length")
	if err != nil {
		return false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return err == nil && n > 0
}

func loadHosts() ([]HostSnapshot, error) {
	raw, err := proxywasm.GetUpstreamHosts()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, errors.New("upstream cluster has no hosts")
	}
	return ParseHosts(raw), nil
}

type breakerDebug struct {
	State           string  `json:"state"`
	TPOTMs          float64 `json:"tpot_ms"`
	ErrorRate       float64 `json:"error_rate"`
	Samples         int     `json:"samples"`
	Trips           int     `json:"trips"`
	CooldownSeconds float64 `json:"cooldown_seconds"`
}

type hostDebug struct {
	HostSnapshot
	GatewayInflight int64            `json:"gateway_inflight"`
	PendingPrefill  int64            `json:"pending_prefill_tokens"`
	ColdPrefills    int64            `json:"cold_prefills"`
	Model           *sched.CostModel `json:"cost_model,omitempty"`
	Breaker         *breakerDebug    `json:"breaker,omitempty"`
}

type debugResponse struct {
	Policy          Policy      `json:"policy"`
	Hosts           []hostDebug `json:"hosts"`
	FleetMedianTPOT float64     `json:"fleet_median_tpot_ms"`
	PresenceEntries int         `json:"presence_entries"`
	QueueLength     int         `json:"queue_length"`
	Error           string      `json:"error,omitempty"`
	Note            string      `json:"note"`
}

// serveDebug 返回插件看到的副本列表、指标与调度状态（代价模型、熔断器、前缀索引属于处理该请求的 worker）。
func serveDebug(config Config) {
	state := config.state
	out := debugResponse{
		Policy:          config.policy,
		FleetMedianTPOT: sched.FleetMedianTPOT(state.allBreakers(), config.breakerCfg.MinSamples),
		PresenceEntries: state.presence.Len(),
		QueueLength:     len(state.queue),
		Note:            "gateway_inflight and pending_prefill_tokens are process-wide; models, breakers and presence belong to the worker that served this request",
	}
	hosts, err := loadHosts()
	if err != nil {
		out.Error = err.Error()
	}
	for _, h := range hosts {
		d := hostDebug{
			HostSnapshot:    h,
			GatewayInflight: sharedGet(inflightKey(h.Address)),
			PendingPrefill:  sharedGet(prefillKey(h.Address)),
			ColdPrefills:    sharedGet(coldKey(h.Address)),
			Model:           state.models[h.Address],
		}
		if b, ok := state.breakers[h.Address]; ok {
			d.Breaker = &breakerDebug{State: b.State.String(), TPOTMs: b.TPOTMs, ErrorRate: b.ErrRate, Samples: b.Samples, Trips: b.Trips, CooldownSeconds: b.Cooldown.Seconds()}
		}
		out.Hosts = append(out.Hosts, d)
	}
	body, _ := json.Marshal(out)
	_ = proxywasm.SendHttpResponseWithDetail(200, pluginName+".debug", [][2]string{{"content-type", "application/json"}}, body, -1)
}

type pluginMetrics struct {
	decisions     proxywasm.MetricCounter
	fallbacks     proxywasm.MetricCounter
	affinity      proxywasm.MetricCounter
	prefixHits    proxywasm.MetricCounter
	coalesced     proxywasm.MetricCounter
	coldSpills    proxywasm.MetricCounter
	coldWaits     proxywasm.MetricCounter
	queued        proxywasm.MetricCounter
	queueTimeouts proxywasm.MetricCounter
	rejected      proxywasm.MetricCounter
	breakerTrips  proxywasm.MetricCounter
	probes        proxywasm.MetricCounter
	invalidations proxywasm.MetricCounter
}

func newPluginMetrics() *pluginMetrics {
	return &pluginMetrics{
		decisions:     proxywasm.DefineCounterMetric("maas_scheduler_decisions_total"),
		fallbacks:     proxywasm.DefineCounterMetric("maas_scheduler_fallback_total"),
		affinity:      proxywasm.DefineCounterMetric("maas_scheduler_affinity_total"),
		prefixHits:    proxywasm.DefineCounterMetric("maas_scheduler_prefix_hit_total"),
		coalesced:     proxywasm.DefineCounterMetric("maas_scheduler_coalesced_total"),
		coldSpills:    proxywasm.DefineCounterMetric("maas_scheduler_cold_spill_total"),
		coldWaits:     proxywasm.DefineCounterMetric("maas_scheduler_cold_wait_total"),
		queued:        proxywasm.DefineCounterMetric("maas_scheduler_queued_total"),
		queueTimeouts: proxywasm.DefineCounterMetric("maas_scheduler_queue_timeout_total"),
		rejected:      proxywasm.DefineCounterMetric("maas_scheduler_rejected_total"),
		breakerTrips:  proxywasm.DefineCounterMetric("maas_scheduler_breaker_trips_total"),
		probes:        proxywasm.DefineCounterMetric("maas_scheduler_probes_total"),
		invalidations: proxywasm.DefineCounterMetric("maas_scheduler_presence_invalidations_total"),
	}
}
