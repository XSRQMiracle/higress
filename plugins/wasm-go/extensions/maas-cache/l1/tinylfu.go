// Package l1 实现 maas-cache 的一级缓存：单个 WASM VM（即一个 Envoy worker）内的进程内缓存。
//
// 支持两种淘汰策略：
//   - lru：经典 LRU，作为对照组；
//   - tinylfu：W-TinyLFU（Einziger et al., 2017；Caffeine 的默认策略）。新条目先进入容量约 1% 的
//     窗口 LRU；被窗口淘汰的"候选者"要和主区（SLRU：probation 20% + protected 80%）的
//     淘汰者比较访问频率，只有更频繁才能进入主区。频率由带老化的 Count-Min Sketch 估计。
//
// MaaS 流量里约一半是只出现一次的新问题（one-hit wonder）。纯 LRU 会被它们不断冲刷，
// 把真正的热点问题挤出去；W-TinyLFU 的准入过滤能让热点长期留在 L1。
//
// WASM VM 是单线程的，因此这里不加锁。
package l1

import (
	"container/list"
	"fmt"
	"hash/fnv"
)

type Policy string

const (
	PolicyLRU     Policy = "lru"
	PolicyTinyLFU Policy = "tinylfu"
)

type segment uint8

const (
	segWindow segment = iota
	segProbation
	segProtected
)

type item[V any] struct {
	key     string
	hash    uint64
	value   V
	segment segment
}

// Stats 是缓存自身的计数，用于调试接口和实验分析。
type Stats struct {
	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	Evictions uint64 `json:"evictions"`
	Rejected  uint64 `json:"rejected"` // TinyLFU 准入失败而被丢弃的候选者
}

type Cache[V any] struct {
	policy       Policy
	capacity     int
	windowCap    int
	protectedCap int
	data         map[string]*list.Element
	window       *list.List // LRU 策略下就是整个缓存
	probation    *list.List
	protected    *list.List
	sketch       *countMinSketch
	stats        Stats
}

// New 创建容量为 capacity 个条目的缓存。
func New[V any](policy Policy, capacity int) (*Cache[V], error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("l1 capacity must be positive, got %d", capacity)
	}
	c := &Cache[V]{
		policy:    policy,
		capacity:  capacity,
		data:      make(map[string]*list.Element, capacity),
		window:    list.New(),
		probation: list.New(),
		protected: list.New(),
	}
	switch policy {
	case PolicyLRU:
		c.windowCap = capacity
	case PolicyTinyLFU:
		c.windowCap = max(1, capacity/100)
		c.protectedCap = (capacity - c.windowCap) * 8 / 10
		c.sketch = newCountMinSketch(capacity)
	default:
		return nil, fmt.Errorf("unknown l1 policy %q, expected lru|tinylfu", policy)
	}
	return c, nil
}

func (c *Cache[V]) Len() int {
	return len(c.data)
}

func (c *Cache[V]) Stats() Stats {
	return c.stats
}

func (c *Cache[V]) Policy() Policy {
	return c.policy
}

func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

// Get 查询并记录一次访问（TinyLFU 的频率统计对命中和未命中都计数）。
func (c *Cache[V]) Get(key string) (V, bool) {
	var zero V
	h := hashKey(key)
	if c.sketch != nil {
		c.sketch.increment(h)
	}
	elem, ok := c.data[key]
	if !ok {
		c.stats.Misses++
		return zero, false
	}
	c.stats.Hits++
	c.onAccess(elem)
	return elem.Value.(*item[V]).value, true
}

// Put 写入条目。调用方通常在 Get 未命中之后调用，因此这里不再重复计频。
func (c *Cache[V]) Put(key string, value V) {
	if elem, ok := c.data[key]; ok {
		elem.Value.(*item[V]).value = value
		c.onAccess(elem)
		return
	}
	it := &item[V]{key: key, hash: hashKey(key), value: value, segment: segWindow}
	c.data[key] = c.window.PushFront(it)
	if c.window.Len() <= c.windowCap {
		return
	}
	candidate := c.window.Remove(c.window.Back()).(*item[V])
	if c.policy == PolicyLRU {
		delete(c.data, candidate.key)
		c.stats.Evictions++
		return
	}
	c.admit(candidate)
}

// onAccess 处理命中后的位置调整：窗口内移到队头；probation 命中则晋升到 protected，
// protected 超出配额时把最久未访问的条目降级回 probation。
func (c *Cache[V]) onAccess(elem *list.Element) {
	it := elem.Value.(*item[V])
	switch it.segment {
	case segWindow:
		c.window.MoveToFront(elem)
	case segProtected:
		c.protected.MoveToFront(elem)
	case segProbation:
		c.probation.Remove(elem)
		it.segment = segProtected
		c.data[it.key] = c.protected.PushFront(it)
		if c.protected.Len() > c.protectedCap {
			demoted := c.protected.Remove(c.protected.Back()).(*item[V])
			demoted.segment = segProbation
			c.data[demoted.key] = c.probation.PushFront(demoted)
		}
	}
}

// admit 决定被窗口淘汰的候选者能否进入主区。
func (c *Cache[V]) admit(candidate *item[V]) {
	mainCap := c.capacity - c.windowCap
	if c.probation.Len()+c.protected.Len() < mainCap {
		candidate.segment = segProbation
		c.data[candidate.key] = c.probation.PushFront(candidate)
		return
	}
	victimElem := c.probation.Back()
	victimList := c.probation
	if victimElem == nil { // probation 为空时从 protected 尾部淘汰
		victimElem, victimList = c.protected.Back(), c.protected
	}
	if victimElem == nil { // 主区容量为 0（capacity 很小时）
		delete(c.data, candidate.key)
		c.stats.Rejected++
		return
	}
	victim := victimElem.Value.(*item[V])
	if c.sketch.estimate(candidate.hash) > c.sketch.estimate(victim.hash) {
		victimList.Remove(victimElem)
		delete(c.data, victim.key)
		c.stats.Evictions++
		candidate.segment = segProbation
		c.data[candidate.key] = c.probation.PushFront(candidate)
		return
	}
	delete(c.data, candidate.key)
	c.stats.Rejected++
}

// countMinSketch：4 行 4-bit 语义的计数器（饱和于 15），每累计 sampleSize 次计数就把所有计数减半，
// 让频率估计偏向近期（老化），避免历史热点永久占据缓存。
type countMinSketch struct {
	rows       [4][]uint8
	mask       uint64
	additions  int
	sampleSize int
}

func newCountMinSketch(capacity int) *countMinSketch {
	width := 16
	for width < capacity*4 {
		width <<= 1
	}
	s := &countMinSketch{mask: uint64(width - 1), sampleSize: 10 * capacity}
	for i := range s.rows {
		s.rows[i] = make([]uint8, width)
	}
	return s
}

func (s *countMinSketch) index(hash uint64, row int) uint64 {
	h1, h2 := hash&0xffffffff, (hash>>32)|1
	return (h1 + uint64(row)*h2) & s.mask
}

func (s *countMinSketch) increment(hash uint64) {
	for row := range s.rows {
		if i := s.index(hash, row); s.rows[row][i] < 15 {
			s.rows[row][i]++
		}
	}
	s.additions++
	if s.additions >= s.sampleSize {
		s.reset()
	}
}

func (s *countMinSketch) estimate(hash uint64) uint8 {
	least := uint8(15)
	for row := range s.rows {
		least = min(least, s.rows[row][s.index(hash, row)])
	}
	return least
}

func (s *countMinSketch) reset() {
	for row := range s.rows {
		for i := range s.rows[row] {
			s.rows[row][i] >>= 1
		}
	}
	s.additions /= 2
}
