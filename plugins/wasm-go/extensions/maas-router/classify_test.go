package main

import (
	"math"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCentroidAndCosine(t *testing.T) {
	c := centroid([][]float64{{1, 0}, {0, 1}})
	if math.Abs(c[0]-c[1]) > 1e-9 || math.Abs(c[0]*c[0]+c[1]*c[1]-1) > 1e-9 {
		t.Fatalf("centroid must be the unit mean direction, got %v", c)
	}
	if got := cosine([]float64{1, 0}, []float64{0, 1}); math.Abs(got) > 1e-9 {
		t.Fatalf("orthogonal vectors: %v", got)
	}
	if got := cosine([]float64{1, 2}, []float64{2, 4}); math.Abs(got-1) > 1e-9 {
		t.Fatalf("parallel vectors: %v", got)
	}
	if got := cosine([]float64{1}, []float64{1, 2}); got != -1 {
		t.Fatalf("dimension mismatch must not match: %v", got)
	}
	if centroid([][]float64{{1, 0}, {1}}) != nil {
		t.Fatal("mixed dimensions must be rejected")
	}
}

func TestClassifyPicksNearestCentroidAboveThreshold(t *testing.T) {
	faq := &Class{Name: "faq", Pool: "small", centroid: normalize([]float64{1, 0.1, 0})}
	analysis := &Class{Name: "analysis", Pool: "large", centroid: normalize([]float64{0, 0.2, 1})}
	pending := &Class{Name: "pending", Pool: "large"} // 质心尚未就绪的类别不参与
	classes := []*Class{faq, analysis, pending}
	if c, sim := classify([]float64{0.9, 0.2, 0.1}, classes, 0.5); c != faq || sim < 0.9 {
		t.Fatalf("expected faq, got %v (%.3f)", c, sim)
	}
	if c, _ := classify([]float64{0.1, 0.1, 1}, classes, 0.5); c != analysis {
		t.Fatalf("expected analysis, got %v", c)
	}
	if c, sim := classify([]float64{0, 1, 0}, classes, 0.5); c != nil {
		t.Fatalf("a query unlike every class must fall back to the default pool, got %v (%.3f)", c, sim)
	}
}

func TestParseVectorAndEstimateTokens(t *testing.T) {
	if v := parseVector(gjson.Parse(`[0.5,-1,2]`)); len(v) != 3 || v[1] != -1 {
		t.Fatalf("parsed %v", v)
	}
	if v := parseVector(gjson.Parse(`{"a":1}`)); v != nil {
		t.Fatal("non-array must not parse")
	}
	if got := estimateTokens("你好abcdefgh"); got != 4 { // 2 个汉字 + 8 个字母 / 4
		t.Fatalf("estimate = %d", got)
	}
}

func TestRequestText(t *testing.T) {
	chat := gjson.Parse(`{"messages":[{"role":"system","content":"S"},{"role":"user","content":"Q1"},{"role":"assistant","content":"A"},{"role":"user","content":[{"type":"text","text":"Q2"}]}]}`)
	if q, all := requestText(chat); q != "Q2" || all != "SQ1AQ2" {
		t.Fatalf("question %q prompt %q", q, all)
	}
	if q, all := requestText(gjson.Parse(`{"prompt":["P"]}`)); q != "P" || all != "P" {
		t.Fatalf("completion prompt %q %q", q, all)
	}
}
