package main

import (
	"math/rand"
	"slices"
	"testing"
)

const sampleMetrics = `# HELP vllm:num_requests_running Number of requests in model execution batches.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{engine="0",model_name="mock-qwen"} 3.0
vllm:num_requests_waiting{engine="0",model_name="mock-qwen"} 2.0
vllm:num_requests_waiting{engine="1",model_name="mock-qwen"} 1.0
vllm:gpu_cache_usage_perc{engine="0",model_name="mock-qwen"} 0.5
vllm:kv_cache_usage_perc{engine="0",model_name="mock-qwen"} 0.25
vllm:cache_config_info{block_size="16",enable_prefix_caching="True",engine="0",num_gpu_blocks="8192"} 1.0
mock_replica_info{replica="mock-vllm-1",slow_factor="1.0"} 1.0
`

func hostMeta(health, metrics string) string {
	if metrics == "" {
		return `{"health_status":"` + health + `"}`
	}
	escaped := ""
	for _, r := range metrics {
		switch r {
		case '\n':
			escaped += `\n`
		case '"':
			escaped += `\"`
		default:
			escaped += string(r)
		}
	}
	return `{"health_status":"` + health + `","metrics":"` + escaped + `"}`
}

func TestParseVLLMMetrics(t *testing.T) {
	m := ParseVLLMMetrics(sampleMetrics)
	if m.Waiting != 3 || m.Running != 3 {
		t.Fatalf("queue = (%v, %v), want (3, 3)", m.Waiting, m.Running)
	}
	if !m.HasKV || m.KVUsage != 0.25 {
		t.Fatalf("kv = %v, want current metric 0.25 to win over legacy", m.KVUsage)
	}
	if m.Replica != "mock-vllm-1" {
		t.Fatalf("replica = %q", m.Replica)
	}
	if m.NumGPUBlocks != 8192 || m.BlockSize != 16 {
		t.Fatalf("cache config = %d blocks x %d", m.NumGPUBlocks, m.BlockSize)
	}
}

func TestParseVLLMMetricsLegacyKVAndGarbage(t *testing.T) {
	m := ParseVLLMMetrics("garbage line\nvllm:gpu_cache_usage_perc 0.7\nvllm:num_requests_waiting{x=\"1\"} NaN\n{}\n")
	if !m.HasKV || m.KVUsage != 0.7 {
		t.Fatalf("legacy kv = %v (has=%v), want 0.7", m.KVUsage, m.HasKV)
	}
	if m.HasQueue {
		t.Fatalf("NaN sample must be ignored")
	}
}

func TestParseHosts(t *testing.T) {
	hosts := ParseHosts([][2]string{
		{"10.0.0.2:8000", hostMeta("Healthy", sampleMetrics)},
		{"10.0.0.1:8000", hostMeta("Unhealthy", "")},
		{" ", hostMeta("Healthy", "")},
		{"10.0.0.3:8000", "not json"},
	})
	if len(hosts) != 3 {
		t.Fatalf("got %d hosts, want 3", len(hosts))
	}
	if hosts[0].Address != "10.0.0.1:8000" || hosts[0].Healthy() {
		t.Fatalf("hosts must be sorted and keep health: %+v", hosts[0])
	}
	if !hosts[1].HasMetrics || hosts[1].Replica != "mock-vllm-1" || hosts[1].Waiting != 3 || hosts[1].KVCapacity != 8192*16 {
		t.Fatalf("metrics not parsed: %+v", hosts[1])
	}
	if hosts[2].Health != "Unparsed" {
		t.Fatalf("invalid metadata must be marked: %+v", hosts[2])
	}
}

func TestPickRoundRobinSkipsUnhealthy(t *testing.T) {
	hosts := []HostSnapshot{
		{Address: "a", Health: "Healthy"},
		{Address: "b", Health: "Unhealthy"},
		{Address: "c", Health: "Healthy"},
	}
	var counter uint64
	got := []string{}
	for i := 0; i < 4; i++ {
		picked, ok := PickRoundRobin(hosts, &counter)
		if !ok {
			t.Fatal("expected a pick")
		}
		got = append(got, picked)
	}
	if want := []string{"a", "c", "a", "c"}; !slices.Equal(got, want) {
		t.Fatalf("round robin = %v, want %v", got, want)
	}
	if _, ok := PickRoundRobin([]HostSnapshot{{Address: "x", Health: "Unhealthy"}}, &counter); ok {
		t.Fatal("no healthy host must not be picked")
	}
}

func TestPickLeastWaitingOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	hosts := []HostSnapshot{
		{Address: "a", Health: "Healthy", Waiting: 2},
		{Address: "b", Health: "Healthy", Waiting: 0, KVUsage: 0.9},
		{Address: "c", Health: "Healthy", Waiting: 0, KVUsage: 0.1},
		{Address: "d", Health: "Unhealthy", Waiting: 0},
	}
	// waiting 相同时先比本网关在途数：c 在途更多，因此选 b。
	if picked, _ := PickLeastWaiting(hosts, map[string]int{"c": 2}, rng); picked != "b" {
		t.Fatalf("picked %s, want b (fewer in-flight)", picked)
	}
	// 在途数也相同时比 KV 占用。
	if picked, _ := PickLeastWaiting(hosts, map[string]int{}, rng); picked != "c" {
		t.Fatalf("picked %s, want c (lower kv usage)", picked)
	}
}

func TestPickLeastWaitingRandomTie(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	hosts := []HostSnapshot{{Address: "a", Health: "Healthy"}, {Address: "b", Health: "Healthy"}}
	seen := map[string]bool{}
	for i := 0; i < 64; i++ {
		picked, _ := PickLeastWaiting(hosts, nil, rng)
		seen[picked] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("ties must be broken randomly, saw %v", seen)
	}
}
