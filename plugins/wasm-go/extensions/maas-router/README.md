# maas-router

MaaS 网关的语义分发插件（赛题七，阶段 3）。客户端把 `model` 写成 `higress/auto` 时，按问题的语义选择模型池：寒暄和常见问题交给小模型，分析、代码、长文档交给大模型。插件改写请求体中的 `model`，写入 `x-maas-pool` 请求头，Envoy 按这个请求头把请求路由到对应的 upstream cluster，后面的 maas-scheduler 再在池内的副本之间调度。

插件链：`maas-cache → maas-router → maas-scheduler → router`。缓存命中的请求不会走到这里；放在缓存之后，问题向量就可以复用缓存插件在 L3 查找时已经算好的结果。

## 决策流程

```
请求 ──► 显式指定已知模型？──是──► 该模型所在的池（reason=model）
          │否
          ▼
        model == autoModel？──否──► 不处理
          │是
          ▼
        估算 prompt token ≥ longPromptTokens ──► longPromptPool（long_prompt）
        最后一条 user 消息匹配规则正则      ──► 规则指定的池（rule）
        语义分类：最相近类别的相似度 ≥ minSimilarity ──► 该类别的池（semantic）
        其余                              ──► defaultPool（semantic_none / default）
```

1. **长上下文**：按"汉字 1 字 1 个 token、其余字符 4 个 1 个 token"估算全部消息的长度，超过 `longPromptTokens` 直接送 `longPromptPool`（默认就是 `defaultPool`）。小模型的上下文窗口和长文档理解能力有限，这一步也省掉了对长文本的向量化。
2. **规则**：正则按顺序匹配最后一条 user 消息，适合"代码""SQL""推导"这类关键词明确、但语义上可能被归成常见问题的请求。
3. **语义分类**：每个类别用若干样例问题描述。设样例 $e_{k,1..m}$ 的向量为 $v(\cdot)$，类别质心
   $c_k = \mathrm{normalize}\big(\sum_i \mathrm{normalize}(v(e_{k,i}))\big)$；
   问题 $q$ 的类别 $k^* = \arg\max_k \cos(v(q), c_k)$，最高相似度低于 `minSimilarity` 时视为"哪类都不像"，交给 `defaultPool`。
   - 问题向量优先复用 maas-cache 写入的 Envoy filter state `maas_query_embedding`（同一请求只向量化一次），没有才调用 embedder；
   - 质心由每个 worker 在第一次需要分类时批量向量化所有样例得到，之后不再请求；初始化期间的其他请求走 `defaultPool`（`warming`）；
   - embedder 超时或出错时走 `defaultPool`（`embed_error`），请求本身不受影响。
4. **改写与路由**：用 sjson 改写请求体的 `model` 并同步 `content-length`（后面的 maas-scheduler 依赖正数 Content-Length 决定是否读取请求体，所以更新而不是删除），写入 `poolHeader`。请求头变化后 Envoy 重新匹配路由。显式指定模型的请求只写 `poolHeader`，客户端自带的同名请求头会被覆盖。
5. **输出长度预测**：按类别统计响应 usage 中 `completion_tokens` 的 EWMA（α = 0.1），通过 `x-maas-predicted-output-tokens` 交给 maas-scheduler，用于显存水位准入时估算请求的 KV 峰值占用。

## 配置

```json
{
  "autoModel": "higress/auto",
  "poolHeader": "x-maas-pool",
  "defaultPool": "large",
  "pools": [
    {"name": "large", "model": "qwen-27b"},
    {"name": "small", "model": "qwen-7b"}
  ],
  "longPromptTokens": 2000,
  "embedding": {"serviceName": "embedder.dns", "servicePort": 8080, "model": "bge-small-zh-v1.5", "timeoutMs": 100},
  "minSimilarity": 0.55,
  "classes": [
    {"name": "chitchat", "pool": "small", "examples": ["你好", "谢谢你的帮助", "你是谁"]},
    {"name": "faq", "pool": "small", "examples": ["信用卡逾期了怎么办", "转账限额是多少"]},
    {"name": "analysis", "pool": "large", "examples": ["请分析这份财报中的主要风险点", "比较这两款理财产品的收益和风险"]}
  ],
  "rules": [{"pattern": "(?i)(代码|python|sql|推导|step by step)", "pool": "large"}]
}
```

| 字段 | 默认值 | 说明 |
|---|---|---|
| `autoModel` | `higress/auto` | 请求的 model 为该值时启用语义分发 |
| `poolHeader` | `x-maas-pool` | 写入选中模型池名称的请求头，Envoy 路由按它匹配 |
| `pools` | 必填 | 模型池名称与该池的模型名；显式指定这些模型的请求直接送往对应池 |
| `defaultPool` | 必填 | 兜底的池 |
| `longPromptPool` / `longPromptTokens` | `defaultPool` / `2000` | 长上下文请求送往的池与阈值 |
| `rules` | `[]` | 按顺序匹配的正则规则 |
| `classes` | `[]` | 语义类别：名称、池、样例问题；为空时不做语义分类 |
| `embedding.*` | — | OpenAI 兼容的向量化服务（`/v1/embeddings`），集群名为 `outbound|<servicePort>||<serviceName>`；配置了 `classes` 时必填 |
| `minSimilarity` | `0.55` | 最相近类别的相似度低于它时走 `defaultPool` |
| `debugHeaders` | `true` | 是否返回 `x-maas-route-*` 响应头 |

Envoy 侧需要按 `poolHeader` 匹配路由，例如：

```yaml
routes:
  - match: {prefix: "/", headers: [{name: x-maas-pool, string_match: {exact: small}}]}
    route: {cluster: vllm_small}
  - match: {prefix: "/"}
    route: {cluster: vllm}
```

完整示例见 [maas/deploy/envoy.yaml](../../../../maas/deploy/envoy.yaml)。

## 观测

- 响应头（`debugHeaders: true`）：`x-maas-route-pool`、`x-maas-route-reason`（`model` / `semantic` / `semantic_none` / `rule` / `long_prompt` / `default` / `warming` / `embed_error`）、`x-maas-route-class`、`x-maas-route-similarity`。
- `GET /maas/debug/router`：模型池、类别、处理该请求的 worker 上质心是否就绪，以及各类别输出长度的 EWMA。
- 指标：`maas_router_{auto,explicit,semantic,rule,long_prompt,default,embedding_reused,embed_errors}_total`，以及每个池一个 `maas_router_pool_<name>_total`。

## 构建与测试

```bash
go test ./...                 # 质心、余弦相似度、分类与 token 估算
GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o main.wasm .
```

端到端验证：`maas/tools/loadgen/verify_phase3.py`。
