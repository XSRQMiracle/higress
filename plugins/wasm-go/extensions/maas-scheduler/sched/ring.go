package sched

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// Ring 是带虚拟节点的一致性哈希环。对每个新前缀，Home 给出一个确定的"家"副本：
// 不同 worker、不同网关实例对同一前缀算出的家相同，不需要共享状态；增删副本时只有约 1/n 的前缀换家。
type Ring struct {
	points []ringPoint
	addrs  []string
}

type ringPoint struct {
	hash uint64
	addr string
}

// mix64 是 splitmix64 的终结函数，把 FNV 输出打散，让虚拟节点在环上分布更均匀。
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return mix64(h.Sum64())
}

func NewRing(addrs []string, virtualNodes int) *Ring {
	sorted := append([]string(nil), addrs...)
	sort.Strings(sorted)
	r := &Ring{addrs: sorted, points: make([]ringPoint, 0, len(sorted)*virtualNodes)}
	for _, addr := range sorted {
		for v := 0; v < virtualNodes; v++ {
			r.points = append(r.points, ringPoint{hash: hashString(addr + "#" + strconv.Itoa(v)), addr: addr})
		}
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i].hash < r.points[j].hash })
	return r
}

// Members 返回环上的副本（已排序），用于判断副本集合是否变化。
func (r *Ring) Members() []string {
	return r.addrs
}

// Home 实现有界负载一致性哈希（Mirrokni et al., 2018）的查找：从 key 在环上的位置顺时针前进，
// 返回第一个 accept 为 true 的副本。accept 通常是"健康、通过准入、且负载未超过 (1+ε)·平均值"，
// 因此热点前缀的家满载时会顺延到下一个副本，单个副本的负载上界为 ⌈(1+ε)·m/n⌉。
func (r *Ring) Home(key uint64, accept func(addr string) bool) (string, bool) {
	if len(r.points) == 0 {
		return "", false
	}
	key = mix64(key)
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= key })
	seen := make(map[string]struct{}, len(r.addrs))
	for i := 0; i < len(r.points) && len(seen) < len(r.addrs); i++ {
		p := r.points[(start+i)%len(r.points)]
		if _, ok := seen[p.addr]; ok {
			continue
		}
		seen[p.addr] = struct{}{}
		if accept(p.addr) {
			return p.addr, true
		}
	}
	return "", false
}
