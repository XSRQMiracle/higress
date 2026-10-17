package main

import (
	"math"
	"unicode"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

// Class 是一个语义类别：用若干样例问题描述，样例向量的平均方向（质心）代表这个类别。
type Class struct {
	Name     string   `json:"name"`
	Pool     string   `json:"pool"`
	Examples []string `json:"examples"`
	centroid []float64
}

// parseVector 把 JSON 数组解析成向量。
func parseVector(raw gjson.Result) []float64 {
	if !raw.IsArray() {
		return nil
	}
	values := raw.Array()
	vec := make([]float64, len(values))
	for i, v := range values {
		vec[i] = v.Float()
	}
	return vec
}

func normalize(vec []float64) []float64 {
	var sum float64
	for _, v := range vec {
		sum += v * v
	}
	norm := math.Sqrt(sum)
	if norm == 0 {
		return vec
	}
	out := make([]float64, len(vec))
	for i, v := range vec {
		out[i] = v / norm
	}
	return out
}

// centroid 计算一组向量的单位化平均向量。
func centroid(vectors [][]float64) []float64 {
	if len(vectors) == 0 {
		return nil
	}
	sum := make([]float64, len(vectors[0]))
	for _, v := range vectors {
		if len(v) != len(sum) {
			return nil
		}
		for i, x := range normalize(v) {
			sum[i] += x
		}
	}
	return normalize(sum)
}

func cosine(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return -1
	}
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return -1
	}
	return dot / math.Sqrt(na*nb)
}

// classify 返回与查询向量最相近的类别（余弦相似度最大的质心）。
// 最高相似度低于 minSimilarity 时返回 nil：问题与所有类别都不像，交给默认模型池。
func classify(query []float64, classes []*Class, minSimilarity float64) (*Class, float64) {
	var best *Class
	bestSim := -1.0
	for _, c := range classes {
		if c.centroid == nil {
			continue
		}
		if sim := cosine(query, c.centroid); sim > bestSim {
			best, bestSim = c, sim
		}
	}
	if best == nil || bestSim < minSimilarity {
		return nil, bestSim
	}
	return best, bestSim
}

// estimateTokens 粗略估计 token 数：汉字 1 字 1 个，其余字符约 4 个 1 个。只用于判断"长上下文"。
func estimateTokens(text string) int {
	cjk, other := 0, 0
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRuneInString(text[i:])
		if unicode.Is(unicode.Han, r) {
			cjk++
		} else {
			other++
		}
		i += size
	}
	return cjk + (other+3)/4
}
