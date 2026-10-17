# maas-scheduler

MaaS 网关的推理副本调度插件（赛题七，阶段 2 + 冷 prefill 优化）。在同一个 upstream cluster 的多个 vLLM 副本之间选择处理请求的副本，目标是降低首 token 时延（TTFT）的尾部、稳定 TPOT，并在副本过载或变慢时自动规避。

依赖两项 Higress 扩展能力：`proxywasm.GetUpstreamHosts()`（读取每个副本的健康状态与健康检查拉到的 vLLM `/metrics`，需要 `store_metrics: true`）与 `proxywasm.SetUpstreamOverrideHost()`（把请求钉到选中的副本）。

## 决策流程（默认策略 `cost`）

```
请求体 ──► 特征提取 ──► 过滤 ──► 选择 ──► override ──► 响应观测 ──► 反馈
          分层前缀       健康      有界负载            首 token     代价模型标定
          prompt 长度    熔断器    预测代价            TPOT / 错误  熔断器
          输出长度预测   显存水位  前缀亲和            usage        前缀索引纠错
                          │                                       输出长度预测
                          ├─ 冷 prefill：同前缀合并等待 / 每副本限并发 / 换副本
                          └─ 全部不达标 → 网关排队 → 放行 / 超时尽力而为 / 429
```

1. **分层稳定前缀**（`sched/features.go`）：模板层（模型 + tools + system）、历史层（最后一条 user 消息之前的轮次）、文档层（最后一条 user 消息里可复用的头部：优先按最后一个空行切分"文档 + 问题"，否则按 block 对齐并留出尾巴）。最后的问题几乎不会被复用，不参与路由键。
2. **过滤**：
   - 熔断器（`sched/breaker.go`）：副本 TPOT 的 EWMA 超过集群中位数 `slowRatio` 倍，或错误率 EWMA 超过 `errorRate`，即摘除 `baseCooldown`；冷却后半开、只放行一个探测请求，探测仍慢则冷却时间翻倍。所有副本都被熔断时放行，避免整体不可用。
   - 显存水位准入（`Admissible`）：KV 占用率达到 `kvHighWatermark`，或按"prompt 全部未命中 + 预测输出长度"估算接收后会撑满 KV（会触发 vLLM 抢占重算）的副本不接收新请求；waiting 队列超过 `maxWaiting` 同理。
3. **选择**（`sched/select.go`）：
   - 有界负载：在途请求数达到 max(⌈(1+ε)·(总在途+1)/n⌉, `minCapacity`) 的副本不参与（ε = `loadFactor` − 1）。`minCapacity` 防止低负载时上限只有 1、持有者一有请求就被视为"满"；
   - 预测代价：`J_i = T̂_i + λ·max(running_i, inflight_i)`，`T̂_i = BaseMs_i + MsPerToken_i·(PendingPrefill_i + PromptTokens − Ĥ_i)`，Ĥ_i 是副本 i 上预计命中的前缀长度（前缀驻留索引给出）；
   - 长前缀亲和：从最深的长前缀（≥ `minTokens`）开始逐层查找持有该前缀 KV 的未满副本，有就在持有者中选代价最小的（`prefix_hit`）；都没有时用带虚拟节点的一致性哈希把它送到"家"副本，家已满则沿环顺延（`affinity`）。不同 worker、不同网关实例对同一前缀算出相同的家，不需要共享状态。可选的 `toleranceMs` 让家副本 prefill 积压过多时改按代价放置，但长上下文 RAG 压测中它削弱了局部性，默认关闭。
   - 持有者正忙时也不放弃亲和：贪心地把长前缀请求派往暂时空闲的副本，会让同一份文档在多个副本上重复缓存、互相挤占 KV，冷 prefill 反而更多（第一版实现就踩了这个坑，见 maas/README.md 的阶段 2 结果）。负载均衡交给有界负载约束保证；没有长前缀的请求才完全按预测代价选择。
4. **冷 prefill 优化**（未命中前缀的 prompt ≥ `coldPrefill.minTokens` 的请求）：
   - 冷前缀请求合并：同一长前缀正在某副本上冷 prefill 时，后续请求在网关等它产出首 token，再作为前缀命中转发到同一副本。领头者通过 SharedData 上的 CAS 竞选产生，跨 worker 也只有一个；
   - 冷 prefill 并发限制：每个副本同时最多 `maxPerReplica` 个冷 prefill（名额用 CAS 原子申请）。选中的副本没有名额时换到有名额的副本，都没有就在网关排队，等任一副本的冷 prefill 产出首 token（迟绑定）。chunked prefill 让同一副本上的多个冷 prefill 共享 token 预算，并发并不会更快，只会让后到的请求排在前面的后面；
   - 长前缀放置表：SharedData 上固定槽位的直接映射表，记录每个长前缀放在哪个副本、是否还在冷 prefill，同时作为跨 worker 的持有者提示。
5. **排队**：所有健康副本都未通过准入、需要等待合并或冷 prefill 名额时，请求在网关挂起（每 100ms 以及每个请求的首 token / 结束时重新评估），超过 `maxQueueMs` 后按 `onTimeout` 尽力而为或返回 429。
6. **反馈**：首个响应分块到达即 TTFT，用于在线标定每个副本的 `BaseMs` / `MsPerToken`；流结束时用 TPOT 与状态码驱动熔断器；usage 中的 `cached_tokens` 远低于预期时，说明 KV 已被驱逐，纠正前缀索引；输出 token 数按模板更新输出长度预测。

在途请求数、待 prefill token 数、冷 prefill 数与长前缀放置表保存在 proxy-wasm SharedData 中（每个"副本 × 计数类型"一个 key，CAS 更新；键在首次使用前预先创建，否则 proxy-wasm 对不存在的键返回 CAS 版本号 0，以 0 写入不做校验，两个 worker 会同时"抢到"），所有 Envoy worker 看到同一份状态；前缀索引、代价模型与熔断器是每个 worker 独立维护的。

## 配置

```json
{
  "policy": "cost",
  "policyHeader": "x-maas-policy",
  "prefix": {"blockTokens": 256, "tailTokens": 256, "presenceTTLSeconds": 600, "maxPresenceEntries": 50000},
  "affinity": {"virtualNodes": 64, "loadFactor": 1.25, "minCapacity": 4, "minTokens": 1024, "toleranceMs": -1},
  "cost": {"baseMs": 30, "msPerToken": 0.12, "decodeMsPerSeq": 0.3, "ewmaAlpha": 0.2},
  "admission": {"kvHighWatermark": 0.92, "maxWaiting": 16, "queue": true, "maxQueueMs": 3000, "maxQueueLength": 1024, "onTimeout": "best_effort"},
  "coldPrefill": {"enabled": true, "minTokens": 1024, "maxPerReplica": 1, "coalesce": true, "maxWarmMs": 15000},
  "breaker": {"enabled": true, "slowRatio": 2.5, "errorRate": 0.5, "minSamples": 8, "ewmaAlpha": 0.2, "baseCooldownSeconds": 5, "maxCooldownSeconds": 60}
}
```

| 字段 | 默认值 | 说明 |
|---|---|---|
| `policy` | `cost` | `cost`（上述流程）；对照组：`least_waiting`（waiting → 在途数 → KV 占用）、`round_robin`、`header`（只处理 `targetHeader`） |
| `policyHeader` | 空 | 非空时允许用该请求头临时指定策略，用于在同一个网关上做消融实验 |
| `targetHeader` | `x-maas-target` | 带此请求头的请求钉到指定副本（调试用） |
| `prefix.*` | 见上 | 文档层的 block 大小、留出的尾巴长度、前缀索引的 TTL 与容量 |
| `affinity.*` | 见上 | 一致性哈希虚拟节点数、有界负载系数 1+ε、负载上限的绝对下限、值得做亲和的最短前缀、新前缀选家的代价容忍度（小于 0 表示总是选家） |
| `coldPrefill.*` | 见上 | 冷 prefill 的阈值、每副本并发上限、是否合并同前缀请求、warming 标记的最长有效期 |
| `cost.*` | 见上 | 代价模型初值（会在线标定）、λ、EWMA 系数 |
| `admission.*` | 见上 | 显存水位、waiting 上限、排队开关、最长排队时间、超时动作 `best_effort` / `reject` |
| `breaker.*` | 见上 | 慢节点倍数、错误率阈值、最少样本数、冷却时间 |

cluster 需要配置访问 `/metrics` 的主动健康检查，并开启 Higress Envoy 扩展字段 `store_metrics`（`HealthCheck` 第 127 号字段）。`no_traffic_interval` 默认 60s，要调到与 `interval` 相同，否则空闲时指标严重滞后。完整示例见 [maas/deploy/envoy.yaml](../../../../maas/deploy/envoy.yaml)。

## 观测

- 响应头（`debugHeaders: true`）：`x-maas-picked`、`x-maas-reason`（`cost` / `prefix_hit` / `affinity` / `cold_spill` / `best_effort` / `breaker_fail_open` / ……）、`x-maas-predicted-ttft-ms`、`x-maas-expected-hit-tokens`；在网关排过队的请求还有 `x-maas-queue-ms` 与 `x-maas-wait-reason`（`coalesce` / `cold_slot` / `kv_watermark`）。
- `GET /maas/debug/hosts`：各副本的健康状态、vLLM 指标、KV 容量、进程级在途数 / 待 prefill token / 冷 prefill 数、代价模型参数、熔断器状态，以及前缀索引条目数与排队长度。
- 指标：`maas_scheduler_{decisions,fallback,affinity,prefix_hit,coalesced,cold_spill,cold_wait,queued,queue_timeout,rejected,breaker_trips,probes,presence_invalidations}_total`。

## 构建与测试

```bash
go test ./...                 # sched 包覆盖特征提取、一致性哈希、前缀索引、代价模型、熔断器与选择逻辑
GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o main.wasm .
```

端到端验证：`maas/tools/loadgen/verify_phase2.py`、`maas/tools/loadgen/verify_cold_prefill.py`。
