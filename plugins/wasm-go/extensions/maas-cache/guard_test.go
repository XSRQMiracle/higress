package main

import (
	"os"
	"reflect"
	"testing"

	"github.com/tidwall/gjson"
)

func TestExtractQuantities(t *testing.T) {
	cases := map[string][]quantity{
		"信用卡逾期一天会影响征信吗":   {{"1", "天"}},
		"信用卡逾期1天对征信有影响吗":  {{"1", "天"}},
		"转账50,000元要手续费吗":  {{"50000", "元"}},
		"贷款10万一年利息多少":     {{"100000", ""}, {"1", "年"}},
		"年化利率3.5%的理财":     {{"3.5", "%"}},
		"收益率百分之五十":        {{"50", "%"}},
		"逾期3个月会被起诉吗":      {{"3", "月"}},
		"两个小时内到账吗":        {{"2", "时"}},
		"一亿五千万元":          {{"150000000", "元"}},
		"十五天内还款":          {{"15", "天"}},
		"二〇二四年的利率":        {{"2024", "年"}},
		"3月5号还款":          {{"3", "月"}, {"5", "天"}},
		"５０００元":           {{"5000", "元"}},
		"十分钟后重试":          {{"10", "分"}},
		"一星期能到账吗":         {{"1", "周"}},
		"帮我看一下，万一不行就一起办理": nil,
		"这个问题十分重要":        nil,
		"千万不要泄露验证码":       nil,
	}
	for text, want := range cases {
		if got := extractQuantities(text); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v", text, got, want)
		}
	}
}

func TestSameQuantities(t *testing.T) {
	cases := []struct {
		a, b string
		same bool
	}{
		{"转账5000要手续费吗", "转账5000元要手续费吗", true}, // 只有一边写了单位
		{"逾期一天", "逾期1天", true},
		{"30年和20年", "20年和30年", true},
		{"逾期一天", "逾期一年", false},
		{"转账5000元", "转账50000元", false},
		{"逾期一天会怎样", "逾期会怎样", false}, // 具体的问题与笼统的问题不同
		{"三年期定期", "五年期定期", false},
	}
	for _, c := range cases {
		if got := sameQuantities(extractQuantities(c.a), extractQuantities(c.b)); got != c.same {
			t.Errorf("%s | %s: got %v, want %v", c.a, c.b, got, c.same)
		}
	}
}

func TestNegationCount(t *testing.T) {
	cases := map[string]int{
		"信用卡晚还一天会不会上征信": 0,
		"转账到其他银行收不收手续费": 0,
		"有没有手续费":        0,
		"是否需要本人到场":      0,
		"可不可以提前还款":      0,
		"不好意思，请问怎么挂失":   0,
		"非常感谢":          0,
		"哪些情况不会影响征信":    1,
		"没有工作证明可以办信用卡吗": 1,
		"收不到验证码短信":      1,
		"未激活的信用卡怎么使用":   1,
		"不是不能办，是暂时不能办":  3,
	}
	for text, want := range cases {
		if got := negationCount(text); got != want {
			t.Errorf("%s: got %d, want %d", text, got, want)
		}
	}
}

func loadTestGuard(t *testing.T) *guard {
	t.Helper()
	data, err := os.ReadFile("testdata/guard.json")
	if err != nil {
		t.Fatal(err)
	}
	return parseGuard(gjson.ParseBytes(data))
}

func TestGuardConflict(t *testing.T) {
	g := loadTestGuard(t)
	cases := []struct {
		a, b, want string
	}{
		{"信用卡逾期一天会影响征信吗？", "信用卡晚还一天会不会上征信？", ""},
		{"如何申请提高信用卡额度？", "信用卡怎么申请提额？", ""}, // 同一义项的同义词
		{"怎么注销手机银行？", "手机银行如何关闭？", ""},
		{"信用卡逾期一天会影响征信吗？", "信用卡逾期一年会影响征信吗？", "number"},
		{"哪些理财产品保本？", "哪些理财产品不保本？", "negation"},
		{"招商银行信用卡逾期怎么办？", "工商银行信用卡逾期怎么办？", "term"},
		{"网银怎么开通？", "网银怎么注销？", "term"},
		{"存款利息怎么计算？", "贷款利息怎么计算？", "term"},
		{"Visa 卡境外消费", "visa 卡境外消费", ""},
	}
	for _, c := range cases {
		if got := g.conflict(c.a, c.b); got != c.want {
			t.Errorf("%s | %s: got %q, want %q", c.a, c.b, got, c.want)
		}
	}
	var none *guard
	if none.conflict("逾期一天", "逾期一年") != "" {
		t.Fatal("a nil guard accepts everything")
	}
	if parseGuard(gjson.Parse(`{"enabled":false}`)) != nil {
		t.Fatal("enabled=false disables the guard")
	}
	if g := parseGuard(gjson.Parse(`{}`)); !g.numbers || !g.negation || len(g.terms) != 0 {
		t.Fatalf("defaults: %+v", g)
	}
}
