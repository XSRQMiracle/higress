package main

import (
	"encoding/binary"
	"errors"
	"strconv"
	"time"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
)

// 进程级"长前缀放置表"：记录每个长前缀最近被放到哪个副本，以及它是否还在冷 prefill（warming）。
//
// 两个用途：
//   - 冷前缀请求合并：同一前缀正在某副本上冷 prefill 时，后续请求在网关等它产出首 token，
//     再作为前缀命中转发过去，而不是在同一副本上重复计算同一段前缀；
//   - 跨 worker 的放置提示：前缀索引是每个 worker 各自维护的，一个 worker 放置的长前缀，
//     其他 worker 通过这张表也能找到持有者，避免同一份文档在多个副本上重复缓存。
//
// 实现为 SharedData 上的直接映射表：前缀哈希对 placementSlots 取模定位槽位，槽内保存完整哈希用于校验。
// 槽位冲突时后写覆盖先写，只会让冲突的前缀失去一次提示，不影响正确性；内存占用固定。
const placementSlots = 8192

type placement struct {
	Hash    uint64
	Addr    string
	At      int64 // 写入时间，Unix 毫秒
	Warming bool
}

func placementKey(hash uint64) string {
	return "maas:sched:place:" + strconv.FormatUint(hash%placementSlots, 10)
}

// encodePlacement 的布局：hash(8) | at(8) | warming(1) | addr
func encodePlacement(p placement) []byte {
	buf := make([]byte, 17+len(p.Addr))
	binary.LittleEndian.PutUint64(buf[0:8], p.Hash)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(p.At))
	if p.Warming {
		buf[16] = 1
	}
	copy(buf[17:], p.Addr)
	return buf
}

func decodePlacement(data []byte) (placement, bool) {
	if len(data) <= 17 {
		return placement{}, false
	}
	return placement{
		Hash:    binary.LittleEndian.Uint64(data[0:8]),
		At:      int64(binary.LittleEndian.Uint64(data[8:16])),
		Warming: data[16] == 1,
		Addr:    string(data[17:]),
	}, true
}

// initPlacementSlots 预先创建所有槽位（见 ensureSharedKey），插件启动时调用。
func initPlacementSlots() {
	empty := []byte{0}
	for slot := uint64(0); slot < placementSlots; slot++ {
		ensureSharedKey(placementKey(slot), empty)
	}
}

// readPlacement 返回该前缀的放置记录；槽位为空或被其他前缀占用时返回 false。
func readPlacement(hash uint64) (placement, bool) {
	data, _, err := proxywasm.GetSharedData(placementKey(hash))
	if err != nil {
		return placement{}, false
	}
	p, ok := decodePlacement(data)
	return p, ok && p.Hash == hash
}

func writePlacement(p placement) {
	_ = proxywasm.SetSharedData(placementKey(p.Hash), encodePlacement(p), 0)
}

// claimWarming 原子地把该前缀标记为"正在 addr 上冷 prefill"，即竞选领头者。
// 已有其他请求的有效 warming 记录时返回 false（本请求应作为跟随者等待）。
// "读-判断-写"在同一个 CAS 版本上完成，两个 worker 同时到达时只有一个能成为领头者。
func claimWarming(hash uint64, addr string, nowMs int64, maxWarm time.Duration) bool {
	key := placementKey(hash)
	record := encodePlacement(placement{Hash: hash, Addr: addr, At: nowMs, Warming: true})
	for i := 0; i < maxCASRetries; i++ {
		data, cas, err := proxywasm.GetSharedData(key)
		if err != nil && !errors.Is(err, types.ErrorStatusNotFound) {
			return true // 共享存储不可用时不合并，按普通请求处理
		}
		if p, ok := decodePlacement(data); ok && p.Hash == hash && p.Warming && nowMs-p.At < maxWarm.Milliseconds() {
			return false
		}
		err = proxywasm.SetSharedData(key, record, cas)
		if err == nil || !errors.Is(err, types.ErrorStatusCasMismatch) {
			return true
		}
	}
	return true
}

// finishWarming 在冷 prefill 产出首 token（或失败）时清除 warming 标记。用 CAS 保证只修改本请求写入的记录：
// 期间如果槽位已被其他前缀或其他副本的放置覆盖，就不再改动。
func finishWarming(hash uint64, addr string, nowMs int64) {
	key := placementKey(hash)
	for i := 0; i < maxCASRetries; i++ {
		data, cas, err := proxywasm.GetSharedData(key)
		if err != nil {
			return
		}
		p, ok := decodePlacement(data)
		if !ok || p.Hash != hash || p.Addr != addr || !p.Warming {
			return
		}
		p.Warming = false
		p.At = nowMs
		if proxywasm.SetSharedData(key, encodePlacement(p), cas) == nil {
			return
		}
	}
}
