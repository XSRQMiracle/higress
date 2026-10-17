package main

// L3 语义命中的关键要素校验。
//
// 向量相似只说明两个问题"说的是同一件事"，不保证细节一致。金融问答里，金额、期限等数字，否定，
// 以及银行、币种、"开通/注销"这类要素一变，答案就跟着变，而这些差异在向量上几乎体现不出来
// （bge-small-zh-v1.5：「逾期一天」与「逾期一年」余弦 0.946，「转账5000元」与「转账50000元」0.971）。
// 所以向量检索只负责召回候选，候选的问题原文还要与当前问题逐项比对，任何一项不一致都不算命中：
//  1. 数字：抽取阿拉伯数字与带单位的中文数字（含"万""亿"等量级、"百分之"），规范化成 (数值, 单位)，
//     两边必须一一对应；单位只在两边都写了时比较（"5000" 与 "5000元" 一致，"一天" 与 "一年" 不一致）；
//  2. 否定：去掉"会不会""有没有""是否"这类正反问结构后，两边否定词的个数必须相同；
//  3. 关键词：配置的每个义项（某家银行、某个币种、"开通"、"关闭/注销"……）可以有多个同义词，
//     两边必须同时提到或同时不提。
//
// 校验宁可错杀：把同义改写判成不一致只是多一次推理，把不同的问题判成一致则会返回错误答案。

import (
	"strconv"
	"strings"
)

type guard struct {
	numbers  bool
	negation bool
	terms    [][]string // 义项 → 同义词（小写）
}

// conflict 返回两个问题在关键要素上的差异：number / negation / term；一致时返回空字符串。
func (g *guard) conflict(a, b string) string {
	if g == nil {
		return ""
	}
	if g.numbers && !sameQuantities(extractQuantities(a), extractQuantities(b)) {
		return "number"
	}
	if g.negation && negationCount(a) != negationCount(b) {
		return "negation"
	}
	a, b = strings.ToLower(a), strings.ToLower(b)
	for _, synonyms := range g.terms {
		if mentions(a, synonyms) != mentions(b, synonyms) {
			return "term"
		}
	}
	return ""
}

func mentions(text string, synonyms []string) bool {
	for _, s := range synonyms {
		if strings.Contains(text, s) {
			return true
		}
	}
	return false
}

// quantity 是规范化后的数量：value 如 "1"、"3.5"、"50000"，unit 如 "天"、"元"、"%"，可以为空。
type quantity struct {
	value string
	unit  string
}

var (
	cnDigits = map[rune]float64{'零': 0, '〇': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
	cnScales = map[rune]float64{'十': 10, '百': 100, '千': 1000, '万': 1e4, '亿': 1e8}
	// 单位及其规范写法。中文数字后面必须紧跟单位才算数量，"一下""一起""万一"里的数字字不算。
	units = map[rune]string{
		'天': "天", '日': "天", '号': "天", '周': "周", '月': "月", '年': "年", '岁': "岁", '时': "时", '秒': "秒",
		'元': "元", '块': "元", '角': "角", '分': "分", '%': "%", '％': "%", '倍': "倍", '期': "期",
		'次': "次", '笔': "笔", '张': "张", '个': "个", '份': "份", '位': "位", '户': "户",
	}
)

func isCNNumeral(r rune) bool {
	_, digit := cnDigits[r]
	_, scale := cnScales[r]
	return digit || scale
}

func isDigit(r rune) bool { return r >= '0' && r <= '9' }

// extractQuantities 按出现顺序抽取文本中的数量。
func extractQuantities(text string) []quantity {
	rs := []rune(strings.Map(func(r rune) rune {
		if r >= '０' && r <= '９' { // 全角数字
			return r - '０' + '0'
		}
		return r
	}, text))
	var out []quantity
	for i := 0; i < len(rs); {
		percent := hasPrefix(rs, i, "百分之")
		if percent {
			i += 3
		}
		switch {
		case i < len(rs) && isDigit(rs[i]):
			value, next := readArabic(rs, i)
			unit, end := readUnit(rs, next)
			if percent {
				unit = "%"
			}
			out = append(out, quantity{value: formatNumber(value), unit: unit})
			i = end
		case i < len(rs) && isCNNumeral(rs[i]):
			j := i
			for j < len(rs) && isCNNumeral(rs[j]) {
				j++
			}
			unit, end := readUnit(rs, j)
			// "十分重要"里的"十分"是副词，"十分钟"才是数量
			adverb := string(rs[i:j]) == "十" && unit == "分" && !hasPrefix(rs, j+1, "钟")
			if (unit == "" && !percent) || adverb {
				i = j
				continue
			}
			if percent {
				unit = "%"
			}
			out = append(out, quantity{value: formatNumber(parseCNNumber(rs[i:j])), unit: unit})
			i = end
		case !percent:
			i++
		}
	}
	return out
}

func hasPrefix(rs []rune, i int, prefix string) bool {
	for _, p := range prefix {
		if i >= len(rs) || rs[i] != p {
			return false
		}
		i++
	}
	return true
}

// readArabic 读取 "50,000"、"3.5"、"10万"、"5千万" 这样的数字，返回数值与结束位置。
func readArabic(rs []rune, i int) (float64, int) {
	var b strings.Builder
	dot := false
scan:
	for ; i < len(rs); i++ {
		switch {
		case isDigit(rs[i]):
			b.WriteRune(rs[i])
		case rs[i] == '.' && !dot && i+1 < len(rs) && isDigit(rs[i+1]):
			dot = true
			b.WriteRune('.')
		case rs[i] == ',' && i+3 < len(rs) && isDigit(rs[i+1]) && isDigit(rs[i+2]) && isDigit(rs[i+3]):
			// 千分位分隔符
		default:
			break scan
		}
	}
	value, _ := strconv.ParseFloat(b.String(), 64)
	for ; i < len(rs) && rs[i] != '十'; i++ {
		scale, ok := cnScales[rs[i]]
		if !ok {
			break
		}
		value *= scale
	}
	return value, i
}

// parseCNNumber 解析中文数字："一百二十" → 120，"一亿五千万" → 150000000；
// 没有量级字的数字串按逐位读法处理（"二〇二四" → 2024）。口语里省略的末位量级（"两万五"）不做推断。
func parseCNNumber(rs []rune) float64 {
	hasScale := false
	for _, r := range rs {
		if _, ok := cnScales[r]; ok {
			hasScale = true
			break
		}
	}
	if !hasScale {
		var v float64
		for _, r := range rs {
			v = v*10 + cnDigits[r]
		}
		return v
	}
	var total, section, number float64
	for _, r := range rs {
		if d, ok := cnDigits[r]; ok {
			number = d
			continue
		}
		switch scale := cnScales[r]; scale {
		case 1e4:
			section += number
			if section == 0 {
				section = 1
			}
			total += section * scale
			section = 0
		case 1e8:
			section += number
			if section == 0 && total == 0 {
				section = 1
			}
			total = (total + section) * scale
			section = 0
		default: // 十、百、千
			if number == 0 { // "十五" 即 "一十五"
				number = 1
			}
			section += number * scale
		}
		number = 0
	}
	return total + section + number
}

// readUnit 读取数字后面的单位："三个月" 取 "月"，"两个小时" 取 "时"，"一星期" 取 "周"。
func readUnit(rs []rune, i int) (string, int) {
	for i < len(rs) && rs[i] == ' ' {
		i++
	}
	switch {
	case hasPrefix(rs, i, "个"):
		if unit, end := readUnit(rs, i+1); unit != "" && unit != "个" {
			return unit, end
		}
	case hasPrefix(rs, i, "小时"):
		return "时", i + 2
	case hasPrefix(rs, i, "星期"):
		return "周", i + 2
	}
	if i < len(rs) {
		if unit, ok := units[rs[i]]; ok {
			return unit, i + 1
		}
	}
	return "", i
}

func formatNumber(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// sameQuantities 判断两组数量能否一一对应：数值相同，且单位相同或至少一边没写单位。
// 先配对单位完全相同的，再让没写单位的去配对剩下的。
func sameQuantities(a, b []quantity) bool {
	if len(a) != len(b) {
		return false
	}
	usedA, usedB := make([]bool, len(a)), make([]bool, len(b))
	for _, exact := range []bool{true, false} {
		for i, qa := range a {
			if usedA[i] {
				continue
			}
			for j, qb := range b {
				if usedB[j] || qa.value != qb.value {
					continue
				}
				if qa.unit == qb.unit || !exact && (qa.unit == "" || qb.unit == "") {
					usedA[i], usedB[j] = true, true
					break
				}
			}
		}
	}
	for _, used := range usedA {
		if !used {
			return false
		}
	}
	return true
}

// 不表示否定的固定搭配
var neutralNegations = []string{"是否", "能否", "可否", "与否", "非常", "无论", "不论", "不管", "不好意思"}

// negationCount 统计否定词个数，"会不会""有没有""可不可以"这类正反问不计。
func negationCount(text string) int {
	for _, w := range neutralNegations {
		text = strings.ReplaceAll(text, w, "")
	}
	rs := []rune(text)
	n := 0
	for i := 0; i < len(rs); i++ {
		if i+2 < len(rs) && (rs[i+1] == '不' || rs[i+1] == '没') && rs[i] == rs[i+2] {
			i += 2
			continue
		}
		switch rs[i] {
		case '不', '没', '未', '无', '非', '别', '勿', '莫', '否':
			n++
		}
	}
	return n
}
