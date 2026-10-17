package sched

import (
	"container/list"
	"time"
)

// PresenceIndex 记录"哪个前缀最近被送到了哪个副本"，用来估计副本上的 KV 前缀缓存命中长度 Ĥ。
//
// 这是对后端真实 KV 缓存的近似：条目按 TTL 过期、总数按 LRU 限制；当后端实际命中的 token 数
// 明显少于预期时（说明已被驱逐），调用方会通过 Invalidate 纠正。
type PresenceIndex struct {
	ttl     time.Duration
	max     int
	entries map[uint64]*presenceEntry
	lru     *list.List // 元素为 level hash，队头最近使用
}

type presenceEntry struct {
	seen map[string]time.Time
	elem *list.Element
}

func NewPresenceIndex(ttl time.Duration, maxEntries int) *PresenceIndex {
	return &PresenceIndex{ttl: ttl, max: maxEntries, entries: map[uint64]*presenceEntry{}, lru: list.New()}
}

func (p *PresenceIndex) Len() int {
	return len(p.entries)
}

// Record 记录请求的各层前缀已经被送到 addr。
func (p *PresenceIndex) Record(addr string, levels []Level, now time.Time) {
	for _, level := range levels {
		e, ok := p.entries[level.Hash]
		if !ok {
			e = &presenceEntry{seen: map[string]time.Time{}}
			e.elem = p.lru.PushFront(level.Hash)
			p.entries[level.Hash] = e
		} else {
			p.lru.MoveToFront(e.elem)
		}
		e.seen[addr] = now
	}
	for len(p.entries) > p.max {
		oldest := p.lru.Back()
		delete(p.entries, oldest.Value.(uint64))
		p.lru.Remove(oldest)
	}
}

func (p *PresenceIndex) fresh(e *presenceEntry, addr string, now time.Time) bool {
	t, ok := e.seen[addr]
	return ok && now.Sub(t) <= p.ttl
}

// DeepestHit 返回 addr 上预计能命中的前缀 token 数（最深的、仍然有效的一层）。
func (p *PresenceIndex) DeepestHit(addr string, levels []Level, now time.Time) int {
	for i := len(levels) - 1; i >= 0; i-- {
		if e, ok := p.entries[levels[i].Hash]; ok && p.fresh(e, addr, now) {
			return levels[i].Tokens
		}
	}
	return 0
}

// AnyHas 判断该层前缀是否在任意副本上有效（为假说明是新前缀，需要确定一个"家"）。
func (p *PresenceIndex) AnyHas(level Level, now time.Time) bool {
	e, ok := p.entries[level.Hash]
	if !ok {
		return false
	}
	for addr := range e.seen {
		if p.fresh(e, addr, now) {
			return true
		}
	}
	return false
}

// Holders 返回该层前缀仍然有效的持有者。
func (p *PresenceIndex) Holders(level Level, now time.Time) []string {
	e, ok := p.entries[level.Hash]
	if !ok {
		return nil
	}
	var out []string
	for addr := range e.seen {
		if p.fresh(e, addr, now) {
			out = append(out, addr)
		}
	}
	return out
}

// Invalidate 在后端反馈的实际命中远低于预期时，删除 addr 上比实际命中更深的各层记录。
func (p *PresenceIndex) Invalidate(addr string, levels []Level, actualHitTokens int) {
	for _, level := range levels {
		if level.Tokens > actualHitTokens {
			if e, ok := p.entries[level.Hash]; ok {
				delete(e.seen, addr)
			}
		}
	}
}

// RemoveEndpoint 清除已经下线的副本。
func (p *PresenceIndex) RemoveEndpoint(addr string) {
	for _, e := range p.entries {
		delete(e.seen, addr)
	}
}
