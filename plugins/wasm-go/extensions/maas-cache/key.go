package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

const maxCanonicalDepth = 64

var errTooDeep = errors.New("request JSON nests too deeply")

// defaultIgnoredFields 是不影响模型输出的顶层字段，计算缓存键时剔除，
// 使"同一个问题以流式 / 非流式、带或不带 usage 统计"等写法共享同一条缓存。
// 其余字段（包括未知字段）一律参与计算：宁可少命中，也不能把不同请求的答案混在一起。
var defaultIgnoredFields = []string{"stream", "stream_options", "user", "metadata", "store", "n", "priority", "request_id", "service_tier"}

// BuildKey 基于"规范化后的完整请求"计算缓存键：
//
//	sha256(namespace · 接口类型 · vary 请求头 · canonical(body − ignoredFields))
//
// 与 ai-cache 默认只取最后一条消息不同，system prompt、历史轮次、tools 与采样参数都会影响键，
// 因此不同场景下的同一句提问不会串答案。canonical JSON 对对象键排序、去掉多余空白、
// 统一字符串转义，所以字段顺序或空白不同的等价请求能命中同一条缓存。
func BuildKey(namespace, kind string, vary []string, body gjson.Result, ignored map[string]struct{}) (string, error) {
	var buf bytes.Buffer
	buf.WriteString(namespace)
	buf.WriteByte(0)
	buf.WriteString(kind)
	buf.WriteByte(0)
	for _, v := range vary {
		buf.WriteString(v)
		buf.WriteByte(0)
	}
	if err := writeCanonicalObject(&buf, body, ignored, 0); err != nil {
		return "", err
	}
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:]), nil
}

func writeCanonicalObject(buf *bytes.Buffer, obj gjson.Result, skip map[string]struct{}, depth int) error {
	if depth > maxCanonicalDepth {
		return errTooDeep
	}
	type field struct {
		key   string
		value gjson.Result
	}
	var fields []field
	obj.ForEach(func(k, v gjson.Result) bool {
		if _, ignored := skip[k.String()]; !ignored {
			fields = append(fields, field{k.String(), v})
		}
		return true
	})
	sort.Slice(fields, func(i, j int) bool { return fields[i].key < fields[j].key })
	buf.WriteByte('{')
	for i, f := range fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		appendJSONString(buf, f.key)
		buf.WriteByte(':')
		if err := writeCanonical(buf, f.value, depth+1); err != nil {
			return err
		}
	}
	buf.WriteByte('}')
	return nil
}

func writeCanonical(buf *bytes.Buffer, v gjson.Result, depth int) error {
	if depth > maxCanonicalDepth {
		return errTooDeep
	}
	switch {
	case v.IsObject():
		return writeCanonicalObject(buf, v, nil, depth)
	case v.IsArray():
		buf.WriteByte('[')
		var err error
		i := 0
		v.ForEach(func(_, elem gjson.Result) bool {
			if i > 0 {
				buf.WriteByte(',')
			}
			i++
			err = writeCanonical(buf, elem, depth+1)
			return err == nil
		})
		if err != nil {
			return err
		}
		buf.WriteByte(']')
	case v.Type == gjson.String:
		appendJSONString(buf, v.Str)
	case v.Type == gjson.Number:
		// 数字保留原文：不同写法（1 与 1.0）只会导致少命中，绝不会把不同的值当成相同。
		buf.WriteString(v.Raw)
	case v.Type == gjson.True:
		buf.WriteString("true")
	case v.Type == gjson.False:
		buf.WriteString("false")
	default:
		buf.WriteString("null")
	}
	return nil
}

// appendJSONString 以与 vLLM（Python json.dumps(ensure_ascii=False)）一致的风格转义字符串：
// 非 ASCII 字符原样输出，只转义引号、反斜杠和控制字符，非法 UTF-8 替换为 U+FFFD。
func appendJSONString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			buf.WriteString(`�`)
		case r == '"':
			buf.WriteString(`\"`)
		case r == '\\':
			buf.WriteString(`\\`)
		case r == '\n':
			buf.WriteString(`\n`)
		case r == '\r':
			buf.WriteString(`\r`)
		case r == '\t':
			buf.WriteString(`\t`)
		case r < 0x20:
			buf.WriteString(`\u00`)
			buf.WriteByte("0123456789abcdef"[r>>4])
			buf.WriteByte("0123456789abcdef"[r&0xf])
		default:
			buf.WriteString(s[i : i+size])
		}
		i += size
	}
	buf.WriteByte('"')
}

func quoteJSON(s string) string {
	var buf bytes.Buffer
	appendJSONString(&buf, s)
	return buf.String()
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
