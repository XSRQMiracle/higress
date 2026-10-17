package main

// 语义命中判定的离线评估：在标注好的问题对（testdata/semantic_pairs.jsonl）上，比较
// "只看向量相似度"与"相似度 + 关键要素校验（testdata/guard.json，与 envoy.yaml 中的配置一致）"
// 在不同阈值下的精确率与召回率。需要运行中的 embedder：
//
//	MAAS_EMBEDDER_URL=http://localhost:8080 go test -run TestSemanticPairsEval -v
//
// 标注集只有一百来对，关键词表也是针对同一场景整理的，结果用于选阈值和说明问题，不代表线上效果。

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
)

type labeledPair struct {
	Kind string `json:"kind"`
	A    string `json:"a"`
	B    string `json:"b"`
}

func (p labeledPair) same() bool { return p.Kind == "paraphrase" }

func embedTexts(t *testing.T, url string, texts []string) map[string][]float64 {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"model": "bge-small-zh-v1.5", "input": texts})
	resp, err := http.Post(strings.TrimRight(url, "/")+"/v1/embeddings", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || len(out.Data) != len(texts) {
		t.Fatalf("embedding failed: %v (%d vectors)", err, len(out.Data))
	}
	vectors := map[string][]float64{}
	for _, d := range out.Data {
		vectors[texts[d.Index]] = d.Embedding
	}
	return vectors
}

func TestSemanticPairsEval(t *testing.T) {
	url := os.Getenv("MAAS_EMBEDDER_URL")
	if url == "" {
		t.Skip("set MAAS_EMBEDDER_URL to run the offline evaluation")
	}
	f, err := os.Open("testdata/semantic_pairs.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var pairs []labeledPair
	seen := map[string]bool{}
	var texts []string
	for scanner := bufio.NewScanner(f); scanner.Scan(); {
		var p labeledPair
		if err := json.Unmarshal(scanner.Bytes(), &p); err != nil {
			t.Fatal(err)
		}
		pairs = append(pairs, p)
		for _, s := range []string{p.A, p.B} {
			if !seen[s] {
				seen[s] = true
				texts = append(texts, s)
			}
		}
	}
	vectors := embedTexts(t, url, texts)
	g := loadTestGuard(t)
	scores := make([]float64, len(pairs))
	verdicts := make([]string, len(pairs))
	for i, p := range pairs {
		scores[i] = cosine(vectors[p.A], vectors[p.B])
		verdicts[i] = g.conflict(p.A, p.B)
	}

	fmt.Printf("%d pairs (%d paraphrases)\n", len(pairs), countSame(pairs))
	fmt.Println("threshold | vector only: P / R / false hits | vector + guard: P / R / false hits")
	for _, th := range []float64{0.75, 0.80, 0.82, 0.84, 0.85, 0.86, 0.88, 0.90, 0.92, 0.94} {
		p1, r1, fp1 := evaluate(pairs, scores, nil, th)
		p2, r2, fp2 := evaluate(pairs, scores, verdicts, th)
		fmt.Printf("  %.2f    |   %.3f / %.3f / %2d            |   %.3f / %.3f / %2d\n", th, p1, r1, fp1, p2, r2, fp2)
	}

	fmt.Println("\nper pair (sorted by score):")
	order := make([]int, len(pairs))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return scores[order[a]] > scores[order[b]] })
	for _, i := range order {
		v := verdicts[i]
		if v == "" {
			v = "pass"
		}
		fmt.Printf("  %.4f %-10s guard=%-8s %s | %s\n", scores[i], pairs[i].Kind, v, pairs[i].A, pairs[i].B)
	}
}

func countSame(pairs []labeledPair) int {
	n := 0
	for _, p := range pairs {
		if p.same() {
			n++
		}
	}
	return n
}

// evaluate 返回阈值 th 下的精确率、召回率与误命中数；verdicts 为 nil 时不做要素校验。
func evaluate(pairs []labeledPair, scores []float64, verdicts []string, th float64) (precision, recall float64, falseHits int) {
	tp, fp, positives := 0, 0, 0
	for i, p := range pairs {
		if p.same() {
			positives++
		}
		hit := scores[i] >= th && (verdicts == nil || verdicts[i] == "")
		switch {
		case hit && p.same():
			tp++
		case hit:
			fp++
		}
	}
	precision = 1
	if tp+fp > 0 {
		precision = float64(tp) / float64(tp+fp)
	}
	return precision, float64(tp) / float64(positives), fp
}

func cosine(a, b []float64) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / math.Sqrt(na*nb)
}
