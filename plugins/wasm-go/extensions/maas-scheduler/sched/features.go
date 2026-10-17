// Package sched 是 maas-scheduler 的调度核心，不依赖 proxy-wasm，可以直接单元测试。
//
// 本文件从 OpenAI 兼容请求中提取调度特征：分层前缀哈希、prompt 长度估计、业务模板键。
package sched

import (
	"hash"
	"hash/fnv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

// Level 是请求的一层"稳定前缀"。层与层之间是包含关系，越深越长：
//   - template：模型 + tools + 开头的 system 消息，同一业务场景的请求共享；
//   - history：再加上最后一条 user 消息之前的所有轮次，多轮对话的后续请求共享；
//   - document：再加上最后一条 user 消息的"头部"——按 block 对齐、并刻意留出至少 tailTokens 个 token
//     的尾巴不计入。RAG 请求通常是"长文档 + 短问题"，留尾巴后同一文档、不同问题的请求得到相同的哈希。
//
// 最后一个问题本身几乎不会被复用，所以不参与路由键。
type Level struct {
	Name   string `json:"name"`
	Hash   uint64 `json:"hash"`
	Tokens int    `json:"tokens"` // 该层前缀的估算 token 数（包含更浅的各层）
}

type Features struct {
	Levels       []Level `json:"levels"`
	PromptTokens int     `json:"prompt_tokens"`
	TemplateKey  uint64  `json:"template_key"` // 输出长度预测按业务模板分组
	MaxTokens    int     `json:"max_tokens"`   // 0 表示请求未指定
}

// 每条消息在 chat 模板中的额外开销（<|im_start|>、角色名、<|im_end|> 等）。
const messageOverheadTokens = 5

// EstimateTokens 用与 mock-vllm 分词器一致的规则估算 token 数：汉字与全角符号 1 字 1 个，
// 字母数字每 4 个字符 1 个，连续空白 1 个，其余符号 1 个。对中文与真实 tokenizer 同一量级。
func EstimateTokens(s string) int {
	n, _ := scanTokens(s, -1)
	return n
}

// CutByTokens 返回 s 中不超过 maxTokens 个 token 的最长前缀（在 token 边界处截断）。
func CutByTokens(s string, maxTokens int) (string, int) {
	n, cut := scanTokens(s, maxTokens)
	return s[:cut], n
}

// scanTokens 统计 token 数；limit >= 0 时在第 limit+1 个 token 开始处停止，返回截断位置。
func scanTokens(s string, limit int) (int, int) {
	tokens, alnum := 0, 0
	inSpace := false
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		starts := false
		switch {
		case r < utf8.RuneSelf && (unicode.IsLetter(r) || unicode.IsDigit(r)):
			starts = alnum == 0
			alnum = (alnum + 1) % 4
			inSpace = false
		case unicode.IsSpace(r):
			starts = !inSpace
			inSpace = true
			alnum = 0
		default:
			starts = true
			inSpace = false
			alnum = 0
		}
		if starts {
			if limit >= 0 && tokens == limit {
				return tokens, i
			}
			tokens++
		}
		i += size
	}
	return tokens, len(s)
}

func messageText(message gjson.Result) string {
	content := message.Get("content")
	if content.IsArray() {
		var text string
		content.ForEach(func(_, part gjson.Result) bool {
			if part.Get("type").String() == "text" {
				text += part.Get("text").String()
			}
			return true
		})
		return text
	}
	return content.String()
}

func writeField(h hash.Hash64, parts ...string) {
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
}

// ExtractFeatures 解析请求体。kind 为 chat 或 completion。
func ExtractFeatures(body gjson.Result, kind string, blockTokens, tailTokens int) Features {
	h := fnv.New64a()
	writeField(h, "model", body.Get("model").String())
	f := Features{MaxTokens: int(body.Get("max_completion_tokens").Int())}
	if f.MaxTokens == 0 {
		f.MaxTokens = int(body.Get("max_tokens").Int())
	}
	tokens := 0
	addLevel := func(name string) {
		if n := len(f.Levels); n > 0 && f.Levels[n-1].Tokens == tokens {
			return // 没有新增内容，不重复成层
		}
		f.Levels = append(f.Levels, Level{Name: name, Hash: h.Sum64(), Tokens: tokens})
	}
	// addHead 确定最后一条 user 消息里"可复用的头部"：
	//  1. 优先按最后一个空行切分——RAG 模板通常是"文档 + 空行 + 问题"，这样头部与问题长度无关；
	//  2. 否则退化为按 block 对齐、并留出至少 tailTokens 的尾巴（问题长度跨越 block 边界时可能分成两组）。
	addHead := func(text string) {
		total := EstimateTokens(text)
		if cut := strings.LastIndex(text, "\n\n"); cut > 0 {
			head := text[:cut]
			if n := EstimateTokens(head); n >= blockTokens && total-n <= 2*tailTokens {
				writeField(h, "head", head)
				tokens += n
				addLevel("document")
				tokens += total - n
				return
			}
		}
		if headTokens := (total - tailTokens) / blockTokens * blockTokens; headTokens >= blockTokens {
			head, n := CutByTokens(text, headTokens)
			writeField(h, "head", head)
			tokens += n
			addLevel("document")
			tokens += total - n
		} else {
			tokens += total
		}
	}

	if kind != "chat" {
		f.TemplateKey = h.Sum64()
		prompt := body.Get("prompt")
		if prompt.IsArray() {
			prompt = prompt.Get("0")
		}
		addHead(prompt.String())
		f.PromptTokens = tokens
		return f
	}

	if tools := body.Get("tools"); tools.Exists() {
		writeField(h, "tools", tools.Raw)
		tokens += EstimateTokens(tools.Raw)
	}
	messages := body.Get("messages").Array()
	lastUser := -1
	for i, m := range messages {
		if m.Get("role").String() == "user" {
			lastUser = i
		}
	}
	i := 0
	for ; i < len(messages); i++ {
		role := messages[i].Get("role").String()
		if role != "system" && role != "developer" {
			break
		}
		writeField(h, role, messageText(messages[i]), messages[i].Get("tool_calls").Raw)
		tokens += EstimateTokens(messageText(messages[i])) + messageOverheadTokens
	}
	addLevel("template")
	f.TemplateKey = h.Sum64()
	for ; i < lastUser; i++ {
		writeField(h, messages[i].Get("role").String(), messageText(messages[i]), messages[i].Get("tool_calls").Raw)
		tokens += EstimateTokens(messageText(messages[i])) + messageOverheadTokens
	}
	addLevel("history")
	if lastUser >= 0 {
		writeField(h, "user")
		tokens += messageOverheadTokens
		addHead(messageText(messages[lastUser]))
		for i = lastUser + 1; i < len(messages); i++ {
			tokens += EstimateTokens(messageText(messages[i])) + messageOverheadTokens
		}
	}
	f.PromptTokens = tokens
	return f
}
