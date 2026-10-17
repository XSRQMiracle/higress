package main

import (
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

// HostSnapshot 是插件从 GetUpstreamHosts() 得到的单个 endpoint 视图。
type HostSnapshot struct {
	Address      string  `json:"address"`
	Health       string  `json:"health"`
	Replica      string  `json:"replica,omitempty"`
	HasMetrics   bool    `json:"has_metrics"`
	MetricsBytes int     `json:"metrics_bytes"`
	Waiting      float64 `json:"waiting"`
	Running      float64 `json:"running"`
	KVUsage      float64 `json:"kv_cache_usage"`
	HasKV        bool    `json:"has_kv"`
	KVCapacity   int     `json:"kv_capacity_tokens"` // num_gpu_blocks × block_size，0 表示未知
}

func (h HostSnapshot) Healthy() bool {
	return h.Health == "Healthy"
}

// ParseHosts 解析 GetUpstreamHosts() 的返回值。每一项是 {地址, 元数据 JSON}：
// 元数据中的 health_status 来自 Envoy 主动健康检查；cluster 开启 store_metrics 后，
// metrics 字段是最近一次健康检查的响应体，即 vLLM /metrics 的 Prometheus 文本。
// 返回结果按地址排序，保证轮询等策略在不同请求间看到稳定的顺序。
func ParseHosts(raw [][2]string) []HostSnapshot {
	hosts := make([]HostSnapshot, 0, len(raw))
	for _, item := range raw {
		address := strings.TrimSpace(item[0])
		if address == "" {
			continue
		}
		host := HostSnapshot{Address: address, Health: "Unparsed"}
		if gjson.Valid(item[1]) {
			meta := gjson.Parse(item[1])
			host.Health = meta.Get("health_status").String()
			if metrics := meta.Get("metrics"); metrics.Type == gjson.String && metrics.Str != "" {
				parsed := ParseVLLMMetrics(metrics.Str)
				host.MetricsBytes = len(metrics.Str)
				host.HasMetrics = parsed.HasQueue
				host.Waiting = parsed.Waiting
				host.Running = parsed.Running
				host.KVUsage = parsed.KVUsage
				host.HasKV = parsed.HasKV
				host.KVCapacity = parsed.NumGPUBlocks * parsed.BlockSize
				host.Replica = parsed.Replica
			}
		}
		hosts = append(hosts, host)
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].Address < hosts[j].Address })
	return hosts
}

// VLLMMetrics 是调度用到的 vLLM 指标子集。
type VLLMMetrics struct {
	Waiting  float64
	Running  float64
	KVUsage  float64
	HasQueue bool
	HasKV    bool
	// vllm:cache_config_info 的标签：KV cache 的 block 数与 block 大小，相乘得到容量（token）
	NumGPUBlocks int
	BlockSize    int
	Replica      string // mock-vllm 特有的 mock_replica_info，便于把地址对应到副本
}

// ParseVLLMMetrics 逐行扫描 Prometheus 文本，只提取需要的几个指标族。
// 同一指标存在多个标签组合（如多 engine）时，队列长度求和，KV 占用取最大值。
func ParseVLLMMetrics(text string) VLLMMetrics {
	var m VLLMMetrics
	var legacyKV float64
	var hasLegacyKV bool
	for len(text) > 0 {
		var line string
		if i := strings.IndexByte(text, '\n'); i >= 0 {
			line, text = text[:i], text[i+1:]
		} else {
			line, text = text, ""
		}
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		name, labels, value, ok := splitSample(line)
		if !ok {
			continue
		}
		switch name {
		case "vllm:num_requests_waiting":
			m.Waiting += value
			m.HasQueue = true
		case "vllm:num_requests_running":
			m.Running += value
		case "vllm:kv_cache_usage_perc":
			m.KVUsage = math.Max(m.KVUsage, value)
			m.HasKV = true
		case "vllm:gpu_cache_usage_perc": // vLLM V0 / 早期 V1 的旧名称
			legacyKV = math.Max(legacyKV, value)
			hasLegacyKV = true
		case "vllm:cache_config_info":
			m.NumGPUBlocks, _ = strconv.Atoi(labelValue(labels, "num_gpu_blocks"))
			m.BlockSize, _ = strconv.Atoi(labelValue(labels, "block_size"))
		case "mock_replica_info":
			m.Replica = labelValue(labels, "replica")
		}
	}
	if !m.HasKV && hasLegacyKV {
		m.KVUsage, m.HasKV = legacyKV, true
	}
	return m
}

// splitSample 把 `name{labels} value [timestamp]` 拆成三部分。
func splitSample(line string) (name, labels string, value float64, ok bool) {
	i := strings.IndexAny(line, "{ \t")
	if i <= 0 {
		return "", "", 0, false
	}
	name, rest := line[:i], line[i:]
	if rest[0] == '{' {
		end := strings.IndexByte(rest, '}')
		if end < 0 {
			return "", "", 0, false
		}
		labels, rest = rest[1:end], rest[end+1:]
	}
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return "", "", 0, false
	}
	v, err := strconv.ParseFloat(parts[0], 64)
	if err != nil || math.IsNaN(v) {
		return "", "", 0, false
	}
	return name, labels, v, true
}

func labelValue(labels, key string) string {
	prefix := key + `="`
	for _, part := range strings.Split(labels, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, prefix) && strings.HasSuffix(part, `"`) {
			return part[len(prefix) : len(part)-1]
		}
	}
	return ""
}

func healthyHosts(hosts []HostSnapshot) []HostSnapshot {
	healthy := make([]HostSnapshot, 0, len(hosts))
	for _, host := range hosts {
		if host.Healthy() {
			healthy = append(healthy, host)
		}
	}
	return healthy
}

// PickRoundRobin 在健康 endpoint 间轮询。
func PickRoundRobin(hosts []HostSnapshot, counter *uint64) (string, bool) {
	healthy := healthyHosts(hosts)
	if len(healthy) == 0 {
		return "", false
	}
	picked := healthy[*counter%uint64(len(healthy))].Address
	*counter++
	return picked, true
}

// PickLeastWaiting 依次比较：vLLM waiting 队列长度 → 本网关在途请求数 → KV cache 占用。
// 健康检查拉取的指标最多滞后一个检查周期，在途请求数则是实时的，用它在两次刷新之间纠偏；
// 各项都相同时随机挑选，避免同一刷新周期内的请求全部涌向同一个副本。
func PickLeastWaiting(hosts []HostSnapshot, inflight map[string]int, rng *rand.Rand) (string, bool) {
	var best []HostSnapshot
	for _, host := range healthyHosts(hosts) {
		if len(best) == 0 {
			best = append(best, host)
			continue
		}
		switch c := compareLoad(host, best[0], inflight); {
		case c < 0:
			best = append(best[:0], host)
		case c == 0:
			best = append(best, host)
		}
	}
	if len(best) == 0 {
		return "", false
	}
	return best[rng.Intn(len(best))].Address, true
}

func compareLoad(a, b HostSnapshot, inflight map[string]int) int {
	if c := compareFloat(a.Waiting, b.Waiting); c != 0 {
		return c
	}
	if ia, ib := inflight[a.Address], inflight[b.Address]; ia != ib {
		if ia < ib {
			return -1
		}
		return 1
	}
	return compareFloat(a.KVUsage, b.KVUsage)
}

func compareFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
