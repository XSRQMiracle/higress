package main

import (
	"encoding/binary"
	"errors"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
)

// 在途请求数与待 prefill token 数需要在所有 Envoy worker 之间一致，否则每个 worker 只看到自己转发的
// 那一部分，有界负载与代价模型都会失真。这里用 proxy-wasm 的 SharedData（进程内共享的键值存储，
// 带 CAS 版本号）实现无锁计数器。
//
// 锁粒度：每个"副本 × 计数类型"一个 key，不同副本的更新互不冲突；同一副本的并发更新通过 CAS 重试
// 解决。LLM 请求的 QPS 远低于 CAS 冲突开始显著的量级，实际几乎不重试。
const (
	inflightKeyPrefix = "maas:sched:inflight:"
	prefillKeyPrefix  = "maas:sched:prefill:"
	coldKeyPrefix     = "maas:sched:cold:"
	maxCASRetries     = 16
)

func inflightKey(addr string) string { return inflightKeyPrefix + addr }
func prefillKey(addr string) string  { return prefillKeyPrefix + addr }
func coldKey(addr string) string     { return coldKeyPrefix + addr } // 正在进行的冷 prefill 数

func decodeCounter(data []byte) int64 {
	if len(data) != 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(data))
}

// ensureSharedKey 在键不存在时先创建它。proxy-wasm 对不存在的键返回的 CAS 版本号是 0，而以 0 写入表示
// "不做 CAS 校验"：两个 worker 同时首次写入同一个键时都会成功，CAS 保护失效。预先创建后版本号非 0，
// 之后的"读-判断-写"才真正原子。并发创建只会写入相同的初始值，没有副作用。
func ensureSharedKey(key string, initial []byte) {
	if _, _, err := proxywasm.GetSharedData(key); errors.Is(err, types.ErrorStatusNotFound) {
		_ = proxywasm.SetSharedData(key, initial, 0)
	}
}

// sharedAdd 原子地给计数器加上 delta（结果不小于 0），返回新值。
func sharedAdd(key string, delta int64) int64 {
	buf := make([]byte, 8)
	for i := 0; i < maxCASRetries; i++ {
		data, cas, err := proxywasm.GetSharedData(key)
		if err != nil && !errors.Is(err, types.ErrorStatusNotFound) {
			return 0
		}
		value := max(decodeCounter(data)+delta, 0)
		binary.LittleEndian.PutUint64(buf, uint64(value))
		err = proxywasm.SetSharedData(key, buf, cas)
		if err == nil {
			return value
		}
		if !errors.Is(err, types.ErrorStatusCasMismatch) {
			return value
		}
	}
	return 0
}

// sharedTryAcquire 在计数器小于 limit 时原子地加 1 并返回 true（"读-判断-写"在同一个 CAS 版本上完成，
// 不同 worker 不会同时拿到最后一个名额）。
func sharedTryAcquire(key string, limit int64) bool {
	buf := make([]byte, 8)
	for i := 0; i < maxCASRetries; i++ {
		data, cas, err := proxywasm.GetSharedData(key)
		if err != nil && !errors.Is(err, types.ErrorStatusNotFound) {
			return false
		}
		value := decodeCounter(data)
		if value >= limit {
			return false
		}
		binary.LittleEndian.PutUint64(buf, uint64(value+1))
		err = proxywasm.SetSharedData(key, buf, cas)
		if err == nil {
			return true
		}
		if !errors.Is(err, types.ErrorStatusCasMismatch) {
			return false
		}
	}
	return false
}

func sharedGet(key string) int64 {
	data, _, err := proxywasm.GetSharedData(key)
	if err != nil {
		return 0
	}
	return decodeCounter(data)
}
