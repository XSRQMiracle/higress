# maas-cache

MaaS 网关的多级响应缓存插件（赛题七，阶段 1：精确匹配）。放在 maas-scheduler 之前：命中的请求按 vLLM 的响应格式直接回放，不再经过调度和推理后端。

## 与 ai-cache 的区别

| | ai-cache | maas-cache |
|---|---|---|
| 缓存键 | 默认只取最后一条消息，不同 system prompt 会串答案 | 规范化后的完整请求（messages、tools、采样参数等）的 SHA-256 |
| 流式回放 | 单个 chunk，`model` 为 `from-cache`，usage 全 0 | 按原始 token 分块逐帧回放，保留 model、finish_reason、usage |
| 层级 | Redis（+ 向量库） | L1 进程内 W-TinyLFU + L2 Redis |
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

响应头 `x-maas-cache` 取值：`L1` / `L2`（命中）、`miss`、`bypass`、`skip:<原因>`。`GET /maas/debug/cache` 返回处理该请求的 worker 的 L1 统计。

指标：`maas_cache_{lookups,hits_l1,hits_l2,misses,bypass,skipped,stores,store_skipped,l2_errors}_total`。

## 构建与测试

```bash
go test ./...
GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o main.wasm .
```

端到端验证：`maas/tools/loadgen/verify_phase1.py --docker-checks`。
