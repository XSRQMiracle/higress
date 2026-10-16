package l1

import (
	"fmt"
	"testing"
)

func mustNew(t *testing.T, policy Policy, capacity int) *Cache[int] {
	t.Helper()
	c, err := New[int](policy, capacity)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// access 模拟 maas-cache 的使用方式：先查 L1，未命中再写入。
func access(c *Cache[int], key string) bool {
	if _, ok := c.Get(key); ok {
		return true
	}
	c.Put(key, len(key))
	return false
}

func TestNewRejectsBadConfig(t *testing.T) {
	if _, err := New[int](PolicyLRU, 0); err == nil {
		t.Fatal("zero capacity must be rejected")
	}
	if _, err := New[int]("arc", 10); err == nil {
		t.Fatal("unknown policy must be rejected")
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	c := mustNew(t, PolicyLRU, 2)
	c.Put("a", 1)
	c.Put("b", 2)
	c.Get("a")
	c.Put("c", 3)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	for _, key := range []string{"a", "c"} {
		if _, ok := c.Get(key); !ok {
			t.Fatalf("%s should still be cached", key)
		}
	}
	if c.Len() != 2 || c.Stats().Evictions != 1 {
		t.Fatalf("len=%d stats=%+v", c.Len(), c.Stats())
	}
}

func TestTinyLFUNeverExceedsCapacity(t *testing.T) {
	c := mustNew(t, PolicyTinyLFU, 50)
	for i := 0; i < 5000; i++ {
		access(c, fmt.Sprintf("k%d", i%173))
		if c.Len() > 50 {
			t.Fatalf("len %d exceeds capacity", c.Len())
		}
	}
}

func TestPutUpdatesExistingValue(t *testing.T) {
	for _, policy := range []Policy{PolicyLRU, PolicyTinyLFU} {
		c := mustNew(t, policy, 10)
		c.Put("a", 1)
		c.Put("a", 2)
		if v, ok := c.Get("a"); !ok || v != 2 || c.Len() != 1 {
			t.Fatalf("%s: got (%v, %v), len %d", policy, v, ok, c.Len())
		}
	}
}

// 50% 热点（少量 key 反复出现）+ 50% 只出现一次的新问题：W-TinyLFU 的热点命中率应明显高于 LRU。
func TestTinyLFUResistsOneHitWonders(t *testing.T) {
	const capacity, hotKeys, rounds = 100, 60, 4000
	hitRate := func(policy Policy) float64 {
		c := mustNew(t, policy, capacity)
		hotHits, hotAccesses := 0, 0
		for i := 0; i < rounds; i++ {
			if i%2 == 0 {
				hotAccesses++
				if access(c, fmt.Sprintf("hot-%d", (i/2)%hotKeys)) {
					hotHits++
				}
			} else {
				access(c, fmt.Sprintf("new-%d", i))
			}
		}
		return float64(hotHits) / float64(hotAccesses)
	}
	lru, tinylfu := hitRate(PolicyLRU), hitRate(PolicyTinyLFU)
	if tinylfu < 0.9 {
		t.Fatalf("tinylfu hot hit rate %.2f, want >= 0.9", tinylfu)
	}
	if tinylfu <= lru+0.3 {
		t.Fatalf("tinylfu (%.2f) should clearly beat lru (%.2f) on a scan-heavy mix", tinylfu, lru)
	}
}

func TestProbationHitIsPromotedToProtected(t *testing.T) {
	c := mustNew(t, PolicyTinyLFU, 100) // 窗口 1 个，主区 99 个
	c.Put("a", 1)
	c.Put("b", 2) // a 被挤出窗口，主区有空位直接进入 probation
	elem := c.data["a"]
	if elem.Value.(*item[int]).segment != segProbation {
		t.Fatal("a should be in probation")
	}
	c.Get("a")
	if c.data["a"].Value.(*item[int]).segment != segProtected {
		t.Fatal("a should be promoted to protected after a hit")
	}
}

func TestSketchAgesCounters(t *testing.T) {
	s := newCountMinSketch(4) // sampleSize = 40
	h := hashKey("x")
	for i := 0; i < 20; i++ {
		s.increment(h)
	}
	if got := s.estimate(h); got != 15 {
		t.Fatalf("counter should saturate at 15, got %d", got)
	}
	for i := 0; i < 20; i++ {
		s.increment(hashKey(fmt.Sprintf("y%d", i)))
	}
	if got := s.estimate(h); got > 8 {
		t.Fatalf("counter should be halved after sampleSize additions, got %d", got)
	}
}
