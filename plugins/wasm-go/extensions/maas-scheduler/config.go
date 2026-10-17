package main

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/alibaba/higress/plugins/wasm-go/extensions/maas-scheduler/sched"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
)

type Policy string

const (
	PolicyHeader       Policy = "header"
	PolicyRoundRobin   Policy = "round_robin"
	PolicyLeastWaiting Policy = "least_waiting"
	PolicyCost         Policy = "cost"
)

func parsePolicy(s string) (Policy, bool) {
	switch p := Policy(s); p {
	case PolicyHeader, PolicyRoundRobin, PolicyLeastWaiting, PolicyCost:
		return p, true
	}
	return "", false
}

type Config struct {
	policy          Policy
	policyHeader    string // 非空时允许用该请求头临时指定策略（消融实验用）
	targetHeader    string
	debugHeaders    bool
	debugPath       string
	blockTokens     int
	tailTokens      int
	virtualNodes    int
	selectCfg       sched.SelectConfig
	initialModel    sched.CostModel
	costAlpha       float64
	breakerEnabled  bool
	breakerCfg      sched.BreakerConfig
	queueEnabled    bool
	maxQueue        time.Duration
	cold            coldConfig
	maxQueueLength  int
	rejectOnTimeout bool
	state           *schedulerState
	metrics         *pluginMetrics
}

// coldConfig 控制冷 prefill（未命中前缀缓存的 prompt 很长的请求）的处理。
type coldConfig struct {
	enabled       bool
	minTokens     int           // 未命中的 prompt token 达到该值即视为冷 prefill
	maxPerReplica int64         // 每个副本同时进行的冷 prefill 上限，满了就换副本或在网关排队
	coalesce      bool          // 同一长前缀正在冷 prefill 时，后续请求等它产出首 token 再转发
	maxWarm       time.Duration // warming 标记的最长有效期，防止异常请求一直挡住后续请求
	placementTTL  time.Duration // 放置表中已完成记录作为"持有者提示"的有效期
}

// schedulerState 是本 worker（WASM VM）内的调度状态；在途请求数等需要跨 worker 一致的计数放在 SharedData（见 shared.go）。
type schedulerState struct {
	presence  *sched.PresenceIndex
	ring      *sched.Ring
	models    map[string]*sched.CostModel
	breakers  map[string]*sched.Breaker
	predictor *sched.OutputPredictor
	queue     []*requestState
	rr        uint64
	rnd       *rand.Rand
}

var (
	activeConfig   *Config // 供定时器回调使用（定时器运行在插件根上下文，拿不到请求级的配置参数）
	tickRegistered bool
)

func floatOr(v gjson.Result, def float64) float64 {
	if v.Exists() {
		return v.Float()
	}
	return def
}

func intOr(v gjson.Result, def int64) int64 {
	if v.Exists() {
		return v.Int()
	}
	return def
}

func boolOr(v gjson.Result, def bool) bool {
	if v.Exists() {
		return v.Bool()
	}
	return def
}

func parseConfig(json gjson.Result, c *Config) error {
	c.policy = PolicyCost
	if raw := json.Get("policy").String(); raw != "" {
		p, ok := parsePolicy(raw)
		if !ok {
			return fmt.Errorf("unknown policy %q, expected header|round_robin|least_waiting|cost", raw)
		}
		c.policy = p
	}
	c.policyHeader = json.Get("policyHeader").String()
	c.targetHeader = json.Get("targetHeader").String()
	if c.targetHeader == "" {
		c.targetHeader = "x-maas-target"
	}
	c.debugHeaders = boolOr(json.Get("debugHeaders"), true)
	c.debugPath = "/maas/debug/hosts"
	if p := json.Get("debugPath"); p.Exists() {
		c.debugPath = p.String()
	}

	prefix := json.Get("prefix")
	c.blockTokens = int(intOr(prefix.Get("blockTokens"), 256))
	c.tailTokens = int(intOr(prefix.Get("tailTokens"), 256))
	presenceTTL := time.Duration(intOr(prefix.Get("presenceTTLSeconds"), 600)) * time.Second
	maxPresence := int(intOr(prefix.Get("maxPresenceEntries"), 50000))
	if c.blockTokens <= 0 || c.tailTokens < 0 || presenceTTL <= 0 || maxPresence <= 0 {
		return errors.New("prefix.blockTokens, presenceTTLSeconds and maxPresenceEntries must be positive")
	}

	affinity := json.Get("affinity")
	c.virtualNodes = int(intOr(affinity.Get("virtualNodes"), 64))
	cost := json.Get("cost")
	admission := json.Get("admission")
	c.selectCfg = sched.SelectConfig{
		LoadFactor:      floatOr(affinity.Get("loadFactor"), 1.25),
		MinCapacity:     int(intOr(affinity.Get("minCapacity"), 4)),
		AffinityTokens:  int(intOr(affinity.Get("minTokens"), 1024)),
		ToleranceMs:     floatOr(affinity.Get("toleranceMs"), -1),
		DecodeMsPerSeq:  floatOr(cost.Get("decodeMsPerSeq"), 0.3),
		KVHighWatermark: floatOr(admission.Get("kvHighWatermark"), 0.92),
		MaxWaiting:      floatOr(admission.Get("maxWaiting"), 0),
	}
	if c.virtualNodes <= 0 || c.selectCfg.LoadFactor < 1 {
		return errors.New("affinity.virtualNodes must be positive and affinity.loadFactor must be >= 1")
	}
	if c.selectCfg.KVHighWatermark <= 0 || c.selectCfg.KVHighWatermark > 1 {
		return errors.New("admission.kvHighWatermark must be within (0, 1]")
	}
	c.initialModel = sched.CostModel{BaseMs: floatOr(cost.Get("baseMs"), 30), MsPerToken: floatOr(cost.Get("msPerToken"), 0.12)}
	c.costAlpha = floatOr(cost.Get("ewmaAlpha"), 0.2)
	if c.costAlpha <= 0 || c.costAlpha > 1 {
		return errors.New("cost.ewmaAlpha must be within (0, 1]")
	}

	c.queueEnabled = boolOr(admission.Get("queue"), true)
	c.maxQueue = time.Duration(intOr(admission.Get("maxQueueMs"), 3000)) * time.Millisecond
	c.maxQueueLength = int(intOr(admission.Get("maxQueueLength"), 1024))
	switch onTimeout := admission.Get("onTimeout").String(); onTimeout {
	case "", "best_effort":
	case "reject":
		c.rejectOnTimeout = true
	default:
		return fmt.Errorf("admission.onTimeout must be best_effort or reject, got %q", onTimeout)
	}

	coldConf := json.Get("coldPrefill")
	c.cold = coldConfig{
		enabled:       boolOr(coldConf.Get("enabled"), true),
		minTokens:     int(intOr(coldConf.Get("minTokens"), 1024)),
		maxPerReplica: intOr(coldConf.Get("maxPerReplica"), 1),
		coalesce:      boolOr(coldConf.Get("coalesce"), true),
		maxWarm:       time.Duration(intOr(coldConf.Get("maxWarmMs"), 15000)) * time.Millisecond,
		placementTTL:  presenceTTL,
	}
	if c.cold.minTokens <= 0 || c.cold.maxPerReplica <= 0 || c.cold.maxWarm <= 0 {
		return errors.New("coldPrefill.minTokens, maxPerReplica and maxWarmMs must be positive")
	}

	breaker := json.Get("breaker")
	c.breakerEnabled = boolOr(breaker.Get("enabled"), true)
	c.breakerCfg = sched.BreakerConfig{
		SlowRatio:    floatOr(breaker.Get("slowRatio"), 2.5),
		ErrorRate:    floatOr(breaker.Get("errorRate"), 0.5),
		MinSamples:   int(intOr(breaker.Get("minSamples"), 8)),
		Alpha:        floatOr(breaker.Get("ewmaAlpha"), 0.2),
		BaseCooldown: time.Duration(intOr(breaker.Get("baseCooldownSeconds"), 5)) * time.Second,
		MaxCooldown:  time.Duration(intOr(breaker.Get("maxCooldownSeconds"), 60)) * time.Second,
	}
	if c.breakerCfg.SlowRatio <= 1 || c.breakerCfg.Alpha <= 0 || c.breakerCfg.Alpha > 1 || c.breakerCfg.BaseCooldown <= 0 {
		return errors.New("breaker.slowRatio must be > 1, ewmaAlpha within (0, 1], baseCooldownSeconds positive")
	}

	c.state = &schedulerState{
		presence:  sched.NewPresenceIndex(presenceTTL, maxPresence),
		models:    map[string]*sched.CostModel{},
		breakers:  map[string]*sched.Breaker{},
		predictor: sched.NewOutputPredictor(0.1, 200, 10000),
		rnd:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	c.metrics = newPluginMetrics()
	if c.cold.enabled {
		initPlacementSlots()
	}
	activeConfig = c
	if !tickRegistered {
		// 每 100ms 检查一次排队中的请求：放行有容量的、处理超时的（另外每个请求结束时也会立即检查一次）
		wrapper.RegisterTickFunc(100, func() {
			if activeConfig != nil {
				processQueue(*activeConfig)
			}
		})
		tickRegistered = true
	}
	return nil
}

func (s *schedulerState) model(addr string, initial sched.CostModel) *sched.CostModel {
	m, ok := s.models[addr]
	if !ok {
		copied := initial
		m = &copied
		s.models[addr] = m
	}
	return m
}

func (s *schedulerState) breaker(addr string) *sched.Breaker {
	b, ok := s.breakers[addr]
	if !ok {
		b = &sched.Breaker{}
		s.breakers[addr] = b
	}
	return b
}

func (s *schedulerState) allBreakers() []*sched.Breaker {
	out := make([]*sched.Breaker, 0, len(s.breakers))
	for _, b := range s.breakers {
		out = append(out, b)
	}
	return out
}

// syncMembers 在副本集合变化时重建一致性哈希环，并清理已下线副本的状态。
// 环包含所有已知副本（不论健康与否），临时不健康的副本由 Home 的 accept 跳过，不会让大量前缀换家。
func (s *schedulerState) syncMembers(hosts []HostSnapshot, virtualNodes int) {
	addrs := make([]string, 0, len(hosts))
	for _, h := range hosts {
		addrs = append(addrs, h.Address)
	}
	if s.ring != nil && sameMembers(s.ring.Members(), addrs) {
		return
	}
	current := map[string]struct{}{}
	zero := make([]byte, 8)
	for _, a := range addrs {
		current[a] = struct{}{}
		for _, key := range []string{inflightKey(a), prefillKey(a), coldKey(a)} {
			ensureSharedKey(key, zero)
		}
	}
	if s.ring != nil {
		for _, old := range s.ring.Members() {
			if _, ok := current[old]; !ok {
				s.presence.RemoveEndpoint(old)
				delete(s.models, old)
				delete(s.breakers, old)
			}
		}
	}
	s.ring = sched.NewRing(addrs, virtualNodes)
}

func sameMembers(sorted, addrs []string) bool {
	if len(sorted) != len(addrs) {
		return false
	}
	set := make(map[string]struct{}, len(sorted))
	for _, a := range sorted {
		set[a] = struct{}{}
	}
	for _, a := range addrs {
		if _, ok := set[a]; !ok {
			return false
		}
	}
	return true
}
