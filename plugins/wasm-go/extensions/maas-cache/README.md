# maas-cache

MaaS 网关的多级响应缓存插件（赛题七，阶段 1 精确匹配 + 阶段 3 语义缓存）。放在 maas-router、maas-scheduler 之前：命中的请求按 vLLM 的响应格式直接回放，不再经过分发、调度和推理后端。

查找顺序：L1 进程内 W-TinyLFU → L2 Redis（精确匹配）→ L3 语义匹配（向量召回 + 关键要素校验，命中后回放 L2 中的那条缓存）。

## 与 ai-cache 的区别

| | ai-cache | maas-cache |
|---|---|---|
| 缓存键 | 默认只取最后一条消息，不同 system prompt 会串答案 | 规范化后的完整请求（messages、tools、采样参数等）的 SHA-256 |
| 流式回放 | 单个 chunk，`model` 为 `from-cache`，usage 全 0 | 按原始 token 分块逐帧回放，保留 model、finish_reason、usage |
| 层级 | Redis（+ 向量库） | L1 进程内 W-TinyLFU + L2 Redis + L3 向量库 |
| 语义命中 | 只比较最后一条消息的向量相似度 | 按上下文键过滤（system prompt、历史、采样参数都相同），再校验数字、否定、关键词 |
| 不可缓存 | 只跳过 tool_calls | n>1、logprobs、best_of、echo、tool_calls、异常结束、超长响应 |

## 配置

```json
{
  "namespace": "qwen2.5-14b@v1",
  "l1": {"policy": "tinylfu", "capacity": 4096, "maxEntryBytes": 262144},
  "l2": {"serviceName": "redis.dns", "servicePort": 6379, "timeoutMs": 200, "ttlSeconds": 3600},
  "debugHeaders": true
}
```

| 字段 | 默认值 | 说明 |
|---|---|---|
| `namespace` | 空 | 参与缓存键计算；更换模型版本时修改即可让旧缓存整体失效 |
| `chatPaths` / `completionPaths` | `/v1/chat/completions` / `/v1/completions` | 生效的接口 |
| `ignoredFields` | stream、stream_options、user、metadata、store、n、priority 等 | 不影响输出、计算缓存键时剔除的顶层字段；其余字段（含未知字段）都参与 |
| `varyHeaders` | `[]` | 参与缓存键的请求头，例如按租户隔离 |
| `bypassHeaders` | `x-maas-cache-bypass`、`x-higress-skip-ai-cache` | 值为 on/true/1 时不读也不写缓存 |
| `maxTemperature` | `-1`（不限制） | 超过该温度的请求不缓存 |
| `finishReasons` | `stop`、`length` | 只缓存以这些原因结束的响应 |
| `l1.policy` | `tinylfu` | `tinylfu` 或 `lru`（对照实验用） |
| `l2.*` | — | Redis 集群名为 `outbound|<servicePort>||<serviceName>` |

响应头 `x-maas-cache` 取值：`L1` / `L2` / `L3`（命中）、`miss`、`bypass`、`skip:<原因>`。`GET /maas/debug/cache` 返回处理该请求的 worker 的 L1 统计。

指标：`maas_cache_{lookups,hits_l1,hits_l2,misses,bypass,skipped,stores,store_skipped,l2_errors}_total`，直方图 `maas_cache_hit_latency_ms`（命中请求从收到请求头到回放完成的耗时）。

## L3 语义缓存（阶段 3）

精确匹配未命中时，把最后一个问题向量化，在向量库里找"上下文完全相同、只是问题换了说法"的已缓存请求：

1. **候选条件**：chat 接口，最后一条消息来自 user，问题不超过 `maxQuestionChars` 个字符。
2. **上下文键**：只比较问题本身是不够的，同一句"这款产品风险大吗"在不同的 system prompt、对话历史、采样参数下答案不同。所以把请求体去掉最后一个问题后同样做规范化哈希，得到上下文键 `ctx`，检索时按 `ctx` 过滤，只在上下文完全相同的请求之间做语义匹配。
3. **召回**：向量化（OpenAI 兼容的 `/v1/embeddings`）→ Qdrant 检索余弦相似度 ≥ `threshold` 的前 `candidates` 个点。
4. **关键要素校验**（[guard.go](guard.go)）：按相似度从高到低，把候选的问题原文与当前问题逐项比对，第一个通过的才算命中：
   - 数字：抽取阿拉伯数字与带单位的中文数字（含"万""亿"等量级、"百分之"），规范化成 (数值, 单位) 后必须一一对应；
   - 否定：去掉"会不会""有没有""是否"这类正反问结构后，否定词个数必须相同；
   - 关键词：`keyTerms` 中的每个义项（一个词或一组同义词，例如某家银行、某个币种、"开通"、"关闭 / 注销"）两边必须同时提到或同时不提。
5. **命中**：按候选记录的精确键读出 L1/L2 中的缓存条目回放，响应头 `x-maas-cache: L3`、`x-maas-semantic-score`；同时以当前请求的精确键写回 L1/L2，同样的说法下次直接精确命中。
6. **登记**：未命中的请求在响应写入 L1/L2 后，把 {向量, ctx, 精确键, 问题原文} 写入向量库（点 ID 由精确键导出，同一请求重复写入是幂等的）。向量库只保存"向量 → 精确键"的映射，L2 条目过期或被淘汰后，对应的语义命中自然失效；检索到这样的点时插件会把它删除并继续检查下一个候选，免得它一直挡在有效候选前面。

向量化、检索任何一步超时或失败都按未命中放行。问题向量写入 Envoy filter state `maas_query_embedding`，后面的 maas-router 直接复用，同一请求只向量化一次。

### 为什么需要要素校验

向量相似只说明两个问题"说的是同一件事"，不保证细节一致。在 101 对人工标注的银行客服问题对（[testdata/semantic_pairs.jsonl](testdata/semantic_pairs.jsonl)，40 对同义改写，61 对不同问题，其中 41 对只差数字、否定、实体或动作）上，bge-small-zh-v1.5 给「转账5000元要手续费吗」与「转账50000元要手续费吗」打出 0.971，「什么情况下需要交违约金」与「什么情况下不需要交违约金」0.951，都高于大多数真正的同义改写：

| 阈值 | 只看相似度：精确率 / 召回率 / 误命中 | 相似度 + 要素校验：精确率 / 召回率 / 误命中 |
|---|---|---|
| 0.80 | 0.450 / 0.900 / 44 | 0.878 / 0.900 / 5 |
| 0.85 | 0.409 / 0.675 / 39 | 0.871 / 0.675 / 4 |
| **0.88** | 0.407 / 0.550 / 32 | **1.000 / 0.550 / 0** |
| 0.90 | 0.366 / 0.375 / 26 | 1.000 / 0.375 / 0 |
| 0.94 | 0.522 / 0.300 / 11 | 1.000 / 0.300 / 0 |

只看相似度时，阈值提到 0.94 仍有 11 次误命中，召回率却只剩 0.3；加上要素校验后 0.88 即可做到零误命中，默认阈值据此取 0.88。0.80～0.86 区间剩下的误命中是"还款日 / 账单日""起存金额 / 利率"这类同一对象的不同属性，规则难以覆盖，所以阈值不再往下调。校验宁可错杀：把同义改写判成不一致只是多一次推理，把不同的问题判成一致则会返回错误答案。

标注集只有一百来对，关键词表也是针对同一场景整理的，这组数字用于选阈值和说明问题，不代表线上效果。复现：

```bash
MAAS_EMBEDDER_URL=http://localhost:8080 go test -run TestSemanticPairsEval -v .
```

### 配置

```json
"l3": {
  "embedding": {"serviceName": "embedder.dns", "servicePort": 8080, "model": "bge-small-zh-v1.5", "timeoutMs": 100},
  "vector": {"serviceName": "qdrant.dns", "servicePort": 6333, "collection": "maas_semantic_cache", "dims": 512, "timeoutMs": 100},
  "threshold": 0.88,
  "candidates": 3,
  "maxQuestionChars": 512,
  "guard": {"numbers": true, "negation": true, "keyTerms": [["工商银行", "工行"], "美元", "欧元", "开通", ["关闭", "注销", "取消"]]}
}
```

| 字段 | 默认值 | 说明 |
|---|---|---|
| `embedding.*` | — | 向量化服务；`path` 默认 `/v1/embeddings` |
| `vector.*` | — | Qdrant；集合不存在时由插件创建（余弦距离，`dims` 维，并为 `ctx` 建关键字索引） |
| `threshold` | `0.88` | 召回的最低余弦相似度 |
| `candidates` | `3` | 每次检索取回的候选数 |
| `maxQuestionChars` | `512` | 超过该长度的问题不做语义匹配 |
| `guard` | 数字、否定校验开启，无关键词 | `{"enabled": false}` 关闭校验 |
| `bypassHeaders` | `x-maas-semantic-bypass` | 值为 on/true/1 时只跳过 L3（仍查写 L1/L2），用于消融实验 |

L3 依赖 L2：向量库只保存到 L2 精确键的映射。

观测：未命中但有候选被要素校验拒绝时，响应头带 `x-maas-semantic-reject`（`number` / `negation` / `term`）与该候选的 `x-maas-semantic-score`。指标 `maas_cache_{hits_l3,l3_lookups,l3_rejects,l3_stale,l3_stores,l3_errors}_total`，直方图 `maas_cache_l3_lookup_ms`（向量化 + 检索耗时，即 L3 给未命中请求增加的延迟）。

## 构建与测试

```bash
go test ./...
GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o main.wasm .
```

端到端验证：`maas/tools/loadgen/verify_phase1.py --docker-checks`（L1/L2）、`maas/tools/loadgen/verify_phase3.py --docker-checks`（L3）。
