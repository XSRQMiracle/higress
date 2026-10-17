# MaaS 智能路由与多级缓存网关（赛题七）

基于 Higress 二次开发的 MaaS 网关。本目录放部署配置、仿真后端与压测工具；网关插件位于 `plugins/wasm-go/extensions/`：

- [maas-cache](../plugins/wasm-go/extensions/maas-cache/README.md)：多级响应缓存（L1 进程内 W-TinyLFU + L2 Redis + L3 语义缓存，L3 带关键要素校验），按 vLLM 格式原样回放
- [maas-router](../plugins/wasm-go/extensions/maas-router/README.md)：语义分发（长上下文 / 规则 / 向量质心分类，把请求送往大小模型池）
- [maas-scheduler](../plugins/wasm-go/extensions/maas-scheduler/README.md)：推理副本调度（预测 TTFT 代价模型 + 有界负载一致性哈希前缀亲和 + 显存水位准入与排队 + 慢节点熔断）

请求链路：`client → maas-cache →（未命中）→ maas-router（选模型池）→ maas-scheduler（选副本）→ vLLM 副本`。

## 当前进度

阶段 0（链路打通）：

- [x] docker-compose 一键启动：Higress gateway（静态 envoy.yaml + WASM 插件）+ 3 个 mock-vllm 副本 + Redis
- [x] mock-vllm：模拟 vLLM 的连续批处理、chunked prefill、KV block 池、前缀缓存、抢占，暴露同名 `/metrics`，支持故障注入
- [x] maas-scheduler（能力验证版）：验证 `GetUpstreamHosts` + `store_metrics`、`SetUpstreamOverrideHost`
- [x] loadgen：50% 热点 + 50% 新请求的合成负载，开环 / 闭环，输出 TTFT / TPOT / ITL 分位数
- [x] 自动验证脚本与基线压测脚本

阶段 1（多级缓存）：

- [x] maas-cache：完整请求规范化缓存键、L1 W-TinyLFU（可切换 LRU 对照）+ L2 Redis、按原始 token 分块回放、流式 / 非流式互转、不可缓存请求过滤、故障放行
- [x] loadgen 按"网关缓存命中 / 未命中"分组统计，思考模型的 reasoning token 计入首 token
- [x] 阶段 1 自动验证（`verify_phase1.py`）与压测（`phase1_bench.sh`）
- [ ] 缓存 epoch 失效接口、并发未命中合并（后续；L3 语义缓存见阶段 3）

阶段 2（智能路由）：

- [x] 分层稳定前缀（模板 / 历史 / 文档头部）与前缀驻留索引，后端实际命中反馈纠错
- [x] 有界负载一致性哈希：新长前缀确定"家"副本，热点自动顺延
- [x] 预测 TTFT 代价模型（在线标定 BaseMs / MsPerToken）与按模板的输出长度预测
- [x] 显存水位准入与网关排队（恢复即放行，超时尽力而为或 429）
- [x] 慢节点 / 高错误率熔断（TPOT 相对集群中位数，半开探测，冷却时间指数退避）
- [x] SharedData 进程级在途计数；`x-maas-policy` 请求头切换策略做消融实验
- [x] 阶段 2 自动验证（`verify_phase2.py`）与长上下文 RAG 压测（`phase2_bench.sh`）

冷 prefill 优化：

- [x] 冷前缀请求合并：同一长前缀正在冷 prefill 时，后续请求等首 token 后作为前缀命中转发（领头者 CAS 竞选，跨 worker 唯一）
- [x] 按副本限制并发冷 prefill：名额满则换副本，全满则网关排队、迟绑定
- [x] 进程级长前缀放置表（SharedData 直接映射表），兼作跨 worker 的持有者提示
- [x] 有界负载上限增加绝对下限 `minCapacity`，修复低负载时亲和失效
- [x] 自动验证（`verify_cold_prefill.py`）

阶段 3（语义分发、L3 语义缓存、监控）：

- [x] embedder：OpenAI 兼容的向量化服务（bge-small-zh-v1.5 int8 ONNX，单条约 1ms）
- [x] maas-router：`model=higress/auto` 时按长上下文 / 规则 / 语义类别选择模型池，改写 model 并由 Envoy 按请求头路由；复用缓存插件算好的问题向量；按类别预测输出长度交给调度插件
- [x] maas-cache L3：按上下文键过滤的向量检索（Qdrant），命中后回放 L2 条目并按精确键写回；数字 / 否定 / 关键词要素校验
- [x] 语义命中判定的离线评估（101 对标注问题，阈值与要素校验的精确率 / 召回率）
- [x] Prometheus + Grafana 面板（缓存、调度、副本、语义分发）
- [x] 阶段 3 自动验证（`verify_phase3.py`）与消融压测（`phase3_bench.sh`）

## 目录

```
maas/
├── deploy/
│   ├── docker-compose.yml   # gateway + mock-vllm×3 + mock-vllm-small + redis + embedder + qdrant
│   │                        # + prometheus + grafana（+ loadgen，profile: tools）
│   ├── envoy.yaml           # 静态 Envoy 配置：网关入口、轮询基线、模型池路由、健康检查与 store_metrics
│   ├── prometheus/          # 抓取网关插件指标、vLLM /metrics、Qdrant
│   ├── grafana/             # 数据源与面板（dashboards/maas.json 由 gen_dashboard.py 生成）
│   └── up.sh                # 编译插件并启动
├── tools/
│   ├── mock-vllm/           # vLLM 模拟器（Python + aiohttp）
│   ├── embedder/            # 向量化服务（bge-small-zh-v1.5 ONNX，镜像构建时下载模型）
│   └── loadgen/             # 压测（loadgen.py）、负载生成（workload.py）、对比（compare.py）、
│                            # 各阶段验证（verify_phase*.py）与压测脚本（phase*_bench.sh）
└── results/                 # 压测结果（只提交 summary.json）
```

## 快速开始

前置条件：Docker（OrbStack 或 Docker Desktop）；Go ≥ 1.24（没有 Go 时 `up.sh` 会用 `golang:1.24` 镜像编译插件）；Python ≥ 3.10 + aiohttp，或直接用 [uv](https://docs.astral.sh/uv/) / loadgen 镜像运行工具。

```bash
cd maas/deploy && ./up.sh
```

启动后会打印插件看到的副本列表。端口：

| 端口 | 说明 |
|---|---|
| 10000 | 网关入口：maas-cache → maas-router → maas-scheduler；`x-maas-cache-bypass: on` 跳过缓存，`x-maas-semantic-bypass: on` 只跳过 L3 |
| 10001 | 基线：纯 Envoy 轮询，不加载插件（相当于 K8s Service 直连多副本） |
| 8001–8003 | 直连大模型池 mock-vllm-0/1/2（`mock-qwen`，含 `/admin/*` 故障注入接口） |
| 8004 | 直连小模型池 mock-vllm-small（`mock-qwen-small`，单步耗时约为大模型的 1/3） |
| 8080 | embedder（`/v1/embeddings`） |
| 6333 | Qdrant（L3 向量库） |
| 3000 | Grafana 面板「MaaS 网关」（匿名只读） |
| 9090 | Prometheus |
| 9901 | Envoy admin |
| 6379 | Redis（maas-cache 的 L2） |

embedder 镜像构建时从 Hugging Face 下载模型，国内网络可以设置 `HF_ENDPOINT=https://hf-mirror.com`、`PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple` 后再执行 `up.sh`。

验证 store_metrics、OverrideHost、副本下线剔除与慢节点分流：

```bash
cd maas/tools/loadgen
uv run --with aiohttp python verify_phase0.py
# 或在 compose 网络内运行
cd maas/deploy && docker compose run --rm loadgen verify_phase0.py --gateway http://gateway:10000 --admin-template 'http://{replica}:8000'
```

验证缓存的命中、回放保真度、缓存键正确性与故障放行（`--docker-checks` 会重启网关、临时停掉 Redis）：

```bash
cd maas/tools/loadgen
uv run --with aiohttp python verify_phase1.py --docker-checks
```

验证前缀亲和、有界负载、显存水位准入与排队、慢节点熔断（约 2 分钟，会临时注入故障）：

```bash
cd maas/tools/loadgen
uv run --with aiohttp python verify_phase2.py
```

验证冷前缀请求合并与冷 prefill 并发限制：

```bash
cd maas/tools/loadgen
uv run --with aiohttp python verify_cold_prefill.py
```

验证语义分发、L3 语义缓存与要素校验、监控面板（`--docker-checks` 会临时停掉 embedder 和 Qdrant）：

```bash
cd maas/tools/loadgen
uv run --with aiohttp python verify_phase3.py --docker-checks
```

压测（每轮开始前清空副本前缀缓存与网关缓存）：

```bash
cd maas/tools/loadgen
PY="uv run --quiet --python 3.12 --with aiohttp python" ./phase0_bench.sh   # 纯轮询 vs 调度插件（绕过缓存）
PY="uv run --quiet --python 3.12 --with aiohttp python" ./phase1_bench.sh   # 加入缓存后的闭环 / 开环对比，约 18 分钟
PY="uv run --quiet --python 3.12 --with aiohttp python" ./phase2_bench.sh   # 长上下文 RAG 负载下的调度对比，约 20 分钟
PY="uv run --quiet --python 3.12 --with aiohttp python" ./phase3_bench.sh   # 客服问答负载下 L3 与语义分发的消融，约 18 分钟
# 决赛口径：DURATION=600
```

## 阶段 0 结果

### 能力验证（`verify_phase0.py`，全部通过）

| 检查项 | 结果 |
|---|---|
| store_metrics | 插件通过 `GetUpstreamHosts()` 读到 3 个副本的健康状态和完整 `/metrics`（约 10KB/副本） |
| OverrideHost（请求头阶段） | `x-maas-target` 指定的请求 15/15 落到目标副本；目标不存在时回退给 Envoy 负载均衡 |
| OverrideHost（body 阶段） | 插件按策略选副本，`x-maas-picked` 与实际副本 0 次不一致 |
| 副本下线 / 恢复 | 约 0.2s 被剔除，下线期间 0 个请求落到该副本；恢复后不到 0.5s 重新加入 |
| 慢节点（4 倍） | least_waiting 下该副本承接的流量从 33% 降到 13%（只靠排队信号，尚无延迟熔断） |

### 基线压测（`phase0_bench.sh`，mock 后端，闭环并发 48，60s + 10s 预热）

负载为 `workload.py` 默认值：50% 热点请求（200 个，Zipf 分布）、4 个约 800 token 的 system prompt、10% 请求附带约 8K token 的长文档（共 8 篇），`max_tokens=256`。

| 配置 | TTFT P50 | TTFT P90 | TTFT P99 | TPOT 均值 | 吞吐 |
|---|---|---|---|---|---|
| 纯 Envoy 轮询（:10001） | 63 ms | 292 ms | 1243 ms | 32.1 ms | 7.87 req/s |
| maas-scheduler least_waiting（:10000） | 66 ms | 102 ms | 1212 ms | 32.1 ms | 7.79 req/s |
| 单副本直连（并发 16，参考） | 69 ms | 83 ms | 1201 ms | 33.4 ms | 2.49 req/s |

- least_waiting 消除了排队造成的长尾，TTFT P90 降低 65%；但 P99 只降 2.5%，TPOT 不变，按决赛口径还够不到基础档。
- 逐请求明细显示，P99 的尾部全部是长文档第一次落到某个副本时的完整 prefill（约 8.4K 个未命中 token，耗时约 1.2s）。8 篇文档 × 3 个副本，两组配置分别出现 23 次和 24 次这样的冷 prefill。轮询基线还额外有短请求排在长 prefill 后面造成的排队长尾（最长 2.1s）。
- 下一步的收益点：阶段 2 的前缀亲和把同一文档固定到同一副本，冷 prefill 理论上从 24 次降到 8 次；阶段 1 的响应缓存让热点请求直接绕过后端。

原始数据：`maas/results/phase0/*/summary.json`（逐请求明细 `requests.jsonl` 不入库，重跑脚本即可生成）。

## 阶段 1 结果

### 功能验证（`verify_phase1.py --docker-checks`，全部通过）

| 检查项 | 结果 |
|---|---|
| 命中与回放保真度 | 第二次请求命中 L1；回放的 SSE 事件数（27）、内容分块、finish_reason、usage、model 与原始响应完全一致 |
| 首字节时间 | 命中 3ms，未命中 108ms |
| 流式 ↔ 非流式 | 同一条缓存可以回放成非流式 JSON，内容一致 |
| 缓存键 | 字段顺序不同、带 `user` 字段的等价请求命中；system prompt、temperature、max_tokens 不同则未命中 |
| 不可缓存请求 | `n=2`、`logprobs` 直接放行（`skip:n` / `skip:logprobs`）；带绕过请求头的请求由后端处理 |
| L2 | 重启网关清空所有 worker 的 L1 后，从 Redis 命中，内容一致 |
| Redis 故障 | 停掉 Redis 期间请求全部返回 200，只会未命中或命中本 worker 的 L1 |

### 压测（`phase1_bench.sh`，mock 后端，180s + 30s 预热）

负载与阶段 0 相同：50% 热点 + 50% 新请求，10% 的请求附带 8K token 文档。

| 负载模型 | 配置 | TTFT P50 / P90 / P99 | TPOT 均值 | 吞吐 | 网关缓存命中 | 后端前缀命中 |
|---|---|---|---|---|---|---|
| 闭环，并发 48 | 纯轮询 | 65 / 449 / 1222 ms | 34.0 ms | 7.98 req/s | — | 83.2% |
| | 只调度（绕过缓存） | 65 / 85 / 1202 ms | 32.6 ms | 8.38 req/s | — | 83.0% |
| | 缓存 + 调度 | 55 / 77 / 1202 ms | 19.1 ms | 13.90 req/s | 43.9% | 64.2% |
| 开环，16 req/s | 纯轮询 | 84 / 1159 / 1873 ms | 55.1 ms | 15.38 req/s | — | 83.0% |
| | 缓存 + 调度 | 52 / 86 / 1227 ms | 19.3 ms | 15.44 req/s | 45.5% | 66.9% |

命中缓存的请求 TTFT P99 约 10ms，TPOT 约为 0。按决赛口径（相对纯轮询基线）：

- **开环**：P99 TTFT 提升 34.5%，平均 TPOT 提升 65.1%，达到"卓越"档。缓存挡掉约 45% 的请求，后端负载随之下降，未命中请求的 TPOT 也从 55ms 降到 35ms。
- **闭环**：吞吐提升 74%，平均 TPOT 提升 43.8%，但 P99 TTFT 只提升 1.7%，达不到基础档。命中的请求几乎不耗时，虚拟用户立刻发出下一个请求，后端的在途请求数不变，而且全是新请求：后端前缀命中率从 83% 降到 64%，未命中请求的 TTFT P90 从 449ms 升到 614ms。P99 仍由长文档的冷 prefill（约 1.2s）决定。

结论：决赛压测如果按固定 QPS 发请求，缓存已经能拿到高分；如果按固定并发，必须靠阶段 2 的前缀亲和与准入控制降低未命中请求的尾延迟。压测模型需要尽快向出题方确认。

原始数据：`maas/results/phase1/*/summary.json`。

## 阶段 2 结果

### 功能验证（`verify_phase2.py`，全部通过）

| 检查项 | 结果 |
|---|---|
| 前缀亲和 | 同一文档、不同问题的 8 个请求全部落到同一副本（首个决策原因 `affinity`），后 7 个的后端前缀命中率 99.1%；对照 least_waiting 分散到 3 个副本，命中率 70.9% |
| 一致性哈希 | 不同文档分散到全部 3 个副本 |
| 有界负载 | 同一文档的 30 个并发请求分摊到多个副本（约 13 / 13 / 4），没有全部压到持有者 |
| 显存水位准入 | 上报 KV 占用 ≥ 95% 的副本，20 个请求中收到 0 个 |
| 排队 | 所有副本超过水位时请求在网关排队，水位恢复后 1.1–1.6s 放行；一直不恢复则 3s 后尽力而为 |
| 慢节点熔断 | 变慢 4 倍的副本流量降到 3% 以下，熔断器 open；恢复后经半开探测重新承接约 1/3 的流量 |

### 压测（`phase2_bench.sh`，mock 后端，150s + 30s 预热）

负载：40% 的请求附带 6K token 文档，共 24 篇（约 144K token，超过单个副本 131K token 的 KV 容量），其余同前（50% 热点请求）。这是刻意制造的 KV 压力场景：任何路由策略都无法让每个副本缓存全部文档。

| 负载模型 | 配置 | TTFT P50 / P90 / P99 | TPOT 均值 | 吞吐 | 网关缓存命中 | 后端前缀命中 |
|---|---|---|---|---|---|---|
| 闭环，并发 48 | 纯轮询 | 79 / 1107 / 1751 ms | 46.9 ms | 5.10 req/s | — | 53.2% |
| | least_waiting（绕过缓存） | 75 / 926 / 1470 ms | 47.1 ms | 5.13 req/s | — | 53.0% |
| | cost（绕过缓存） | 68 / 918 / 1709 ms | 39.3 ms | 6.21 req/s | — | 69.5% |
| | 缓存 + cost | 57 / 915 / 1695 ms | 25.2 ms | 10.05 req/s | 43.0% | 59.5% |
| 开环，4 req/s | 纯轮询 | 71 / 1108 / 1715 ms | 40.0 ms | 4.11 req/s | — | 52.7% |
| | 缓存 + cost | 57 / 900 / 1145 ms | 18.7 ms | 4.19 req/s | 38.7% | 58.5% |

按决赛口径（相对纯轮询基线）：

- **开环**：P99 TTFT 提升 33.2%，平均 TPOT 提升 53.1%，达到"卓越"档。
- **闭环**：吞吐提升 97%，平均 TPOT 提升 46%，但 P99 TTFT 只提升 3%。
- **调度算法本身**（cost 对比 least_waiting，都不带缓存）：后端前缀命中率提升 16.5 个百分点，吞吐提升 21%，平均 TPOT 提升 16%，但 P99 TTFT 变差 16%。

### 算法演进（闭环，不带缓存）

| 版本 | 规则 | 后端前缀命中 | 吞吐 | TPOT 均值 | P99 TTFT |
|---|---|---|---|---|---|
| v1 | 所有请求按预测代价贪心选择 | 55.6% | 5.25 req/s | 45.9 ms | 1614 ms |
| **v2（默认）** | 长前缀优先送持有者，超过有界负载才沿环溢出；新前缀一律送家副本 | **69.5%** | **6.21 req/s** | **39.3 ms** | 1709 ms |
| v3 | v2 + 新前缀的家副本积压过多时按代价放置（`toleranceMs: 300`） | 61.6% | 5.64 req/s | 42.9 ms | 1761 ms |

- **v1 的问题**：持有文档缓存的副本一旦有 prefill 排队，按单个请求算 TTFT 时就不如空闲副本，于是请求被派往别处。结果 24 篇文档中 20 篇缓存在全部 3 个副本上，副本之间互相挤占 KV，文档请求中 57% 是冷 prefill，局部最优导致全局变差。
- **v2**：负载均衡交给有界负载约束，不再让"持有者正忙"推翻亲和。冷 prefill 占全部请求的比例从 least_waiting 的 27%（v1 为 26%）降到 17%。
- **v3**：削弱了局部性，没有带来收益，因此默认关闭（`toleranceMs` 保留为可选开关）。

### 已知局限：闭环下的 P99 TTFT

v2 的 P99 尾部全部来自 6K 文档的冷 prefill（单次约 0.9s，几个同时排在一个副本上时更长）。冷 prefill 仍占全部请求的 17%，其中 71% 发生在之前缓存过该文档、但文档已被后端驱逐的副本上。这个负载下文档总量超过 KV 容量，驱逐无法避免。亲和把同一文档的请求集中到一个副本，文档被驱逐后，这些请求的冷 prefill 也集中在同一副本上，所以尾部比分散路由更长。可以继续尝试的方向：

- 合并冷前缀请求：同一前缀正在某副本上冷 prefill 时，让后续请求等它产出首 token 后再转发，变成命中；
- 按副本限制同时进行的冷 prefill 数量（以 prefill token 为单位的有界负载）；
- 在后端用 LMCache 等方案把 KV 卸载到 CPU 内存，减少驱逐；
- 弄清决赛的文档规模：如果 KV 足够容纳热门前缀，冷 prefill 会少得多。

前两项已在后续的冷 prefill 优化中实现，见下一节。

原始数据：`maas/results/phase2/*/summary.json`（`-v1`、`-v2`、`-v3` 为对照版本；不带后缀的是加入冷 prefill 优化后的结果，见下一节）。

## 冷 prefill 优化结果

针对阶段 2 遗留的"固定并发下 P99 TTFT 被冷 prefill 拖住"的问题。

### 功能验证（`verify_cold_prefill.py`，连续两次全部通过）

| 检查项 | 结果 |
|---|---|
| 冷前缀请求合并 | 同一新文档的 4 个并发请求，后端只冷 prefill 一次。3 个跟随者在网关等领头者产出首 token（约 420ms），然后全部落到同一副本并命中前缀（未命中只剩 27 token），TTFT 约 490ms；不合并时它们会各自重新 prefill，TTFT 约 880ms |
| 冷 prefill 并发限制 | 6 篇新文档同时到达：每个副本同时最多 1 个长 prefill（mock 记录的峰值为 1），3 个请求在网关等名额或被换到空闲副本；对照组 least_waiting 每个副本峰值为 2 |
| 普通请求不受影响 | 同时发出的 5 个短请求都无需排队 |
| 名额归还 | 请求结束后各副本的冷 prefill 计数全部回到 0 |

验证过程中修复了两个问题：
- **两个领头者**：最初 4 个请求里出现了 2 个领头者。proxy-wasm 对不存在的键返回的 CAS 版本号是 0，以 0 写入不做校验，两个 worker 同时首次写入同一个键时都能成功。改为在插件启动时预先创建所有槽位和计数器。
- **低负载时亲和失效**：有界负载上限 ⌈(1+ε)·(在途+1)/n⌉ 在低负载时只有 1，持有者一有在途请求就被视为"满"，跟随者被派到别的副本重新冷 prefill。增加了 `minCapacity`（默认 4）作为绝对下限。

### 压测（`phase2_bench.sh`，与阶段 2 相同的长上下文 RAG 负载）

| 负载模型 | 配置 | TTFT P50 / P90 / P99 | TPOT 均值 | 吞吐 | 后端前缀命中 |
|---|---|---|---|---|---|
| 闭环，并发 48 | 纯轮询 | 79 / 1107 / 1751 ms | 46.9 ms | 5.10 req/s | 53.2% |
| | least_waiting（绕过缓存） | 75 / 926 / 1470 ms | 47.1 ms | 5.13 req/s | 53.0% |
| | cost，阶段 2（v2） | 68 / 918 / 1709 ms | 39.3 ms | 6.21 req/s | 69.5% |
| | cost + 冷 prefill 优化 | 71 / 919 / **1483** ms | 40.2 ms | 6.03 req/s | 70.6% |
| | 缓存 + cost，阶段 2（v2） | 57 / 915 / 1695 ms | 25.2 ms | 10.05 req/s | 59.5% |
| | 缓存 + cost + 冷 prefill 优化 | 57 / 901 / **1413** ms | **23.5** ms | **10.88** req/s | 63.9% |
| 开环，4 req/s | 纯轮询 | 71 / 1108 / 1715 ms | 40.0 ms | 4.11 req/s | 52.7% |
| | 缓存 + cost，阶段 2（v2） | 57 / 900 / 1145 ms | 18.7 ms | 4.19 req/s | 58.5% |
| | 缓存 + cost + 冷 prefill 优化 | 57 / 886 / **913** ms | **17.3** ms | 4.19 req/s | 60.0% |

按决赛口径（相对纯轮询基线）：

- **闭环**：P99 TTFT 提升 19.3%（v2 为 3.2%），平均 TPOT 提升 49.9%，吞吐提升 114%，从"未达到基础档"升到"进阶"档；
- **开环**：P99 TTFT 提升 46.7%（v2 为 33.2%），平均 TPOT 提升 56.8%，保持"卓越"档；
- **调度算法本身**（cost 对比 least_waiting，都不带缓存）：P99 TTFT 由差 16% 变为持平（−0.9%），TPOT 提升 14.5%，吞吐提升 17.6%。

冷 prefill 的数量几乎没变（占全部请求的 17.4% → 16.3%，主要来自 KV 驱逐），变化在于它们不再在同一副本上叠加：冷 prefill 的 TTFT P90 从 1562ms 降到 1133ms，加缓存时从 1639ms 降到 1332ms。闭环下 P99 的尾部仍是冷 prefill，要进一步提升需要减少 KV 驱逐本身（后端 KV 卸载或更大的 KV 容量）。

## 阶段 3 结果

### 功能验证（`verify_phase3.py --docker-checks`，全部通过）

| 检查项 | 结果 |
|---|---|
| 语义分发 | 寒暄（与类别质心的相似度 0.69）、常见问题（0.62）送小模型池，分析类（0.78）送大模型池；含"Python"的请求按规则、2800 字的长文本按长度送大模型池；响应中的 model 与处理副本都与所选池一致 |
| 显式指定模型 | 直接送往该模型所在的池；客户端伪造的 `x-maas-pool` 被覆盖 |
| 向量复用 | 缓存插件在 L3 查找时算出的问题向量被路由插件直接复用，同一请求只向量化一次 |
| L3 命中 | 「信用卡逾期1天对征信有影响吗？」命中「信用卡逾期一天会影响征信吗？」的缓存（相似度 0.9645）：16 个分块与 usage 都和原响应一致，首字节 21ms（未命中 46ms）；同一说法再问一次直接 L1 命中；非流式请求同样命中 |
| 要素校验 | 「逾期一年」（相似度 0.948）因数字不同、「不需要交违约金」（0.954）因否定不同被拒绝，照常转发后端 |
| 精确性 | 不同的问题、不同 system prompt 下的同一个问题都不命中 |
| 故障放行 | 停掉 embedder：请求照常完成，语义分发回落到默认池（`embed_error`），恢复后自动恢复；停掉 Qdrant：跳过 L3，路由仍复用向量，恢复后 L3 自动恢复。故障期间每个未命中请求要多等外呼超时（embedder 故障时缓存、路由各等 100ms，首字节约 250ms），还没有做连续失败后的快速失败 |
| 失效候选 | L2 条目被删除后，向量库中对应的点在下一次检索到时被删除，之后的改写问题命中下一个候选 |
| 监控 | Prometheus 的 6 个抓取目标全部 up；Grafana 面板「MaaS 网关」共 25 个 panel，压测期间所有查询都有数据 |

验证中发现并修复的问题：

- **向量相似不等于答案相同**：bge-small-zh-v1.5 给「逾期一天 / 逾期一年」打 0.948、「需要 / 不需要交违约金」打 0.954，都高于最初的阈值 0.9，会直接返回错误答案。为 L3 增加了关键要素校验（数字、否定、关键词），并用 101 对人工标注的问题对选定阈值：只看相似度时阈值 0.90 的精确率只有 0.37（26 次误命中），提到 0.94 仍有 11 次；加上要素校验后阈值 0.88 零误命中、召回率 0.55。详见 [maas-cache 的说明](../plugins/wasm-go/extensions/maas-cache/README.md#为什么需要要素校验)。
- **规则误伤**：路由规则里的"证明"本意是数学证明，却把「怎么开具存款证明」这类常见问题送进大模型池，已从规则中去掉。
- **失效的向量点挡住有效候选**：L2 条目过期后，向量库里的点仍在；如果它相似度最高，后面的改写问题每次都先选中它、读 L2 落空、按未命中处理。改为删除这个点并继续检查下一个候选。

### 压测（`phase3_bench.sh`，客服问答负载，90s + 20s 预热）

负载：50% 热点请求换成银行客服的真实问法（27 个意图：20 个常见问题、3 个寒暄、4 个分析，每个意图两种说法，一半请求再随机加上"请问""你好，""谢谢"之类的客套话；4 个 system prompt），50% 是和前几个阶段相同的新请求（其中 10% 附带 8K token 文档）。热点请求按语义类别限制输出长度（寒暄 32、常见问题 128、分析 256 token）。

| 配置 | 说明 |
|---|---|
| direct | 纯 Envoy 轮询直连 3 个大模型副本（10001 端口） |
| no-dispatch | 网关：L1/L2/L3 缓存 + 调度，请求直接指定大模型，不做语义分发 |
| exact | 网关：L1/L2 缓存 + 语义分发 + 调度，跳过 L3（`x-maas-semantic-bypass: on`） |
| semantic | 网关全开：L1/L2/L3 缓存 + 语义分发 + 调度 |

闭环，并发 32：

| 配置 | TTFT P50 / P90 / P99 | TPOT 均值 / P50 | 吞吐 | 网关缓存命中（L1+L2 / L3） | 热点请求命中率 | 热点 TTFT P50 |
|---|---|---|---|---|---|---|
| direct | 57 / 80 / 1194 ms | 29.7 / 27.4 ms | 7.02 req/s | — | — | 53.8 ms |
| no-dispatch | 54 / 80 / 1175 ms | 17.2 / 26.6 ms | 10.55 req/s | 41.5%（22.0% / 19.4%） | 85.9% | 7.9 ms |
| exact | 35 / 74 / 1166 ms | 15.4 / 10.3 ms | 11.92 req/s | 23.5%（23.5% / —） | 48.5% | 17.8 ms |
| semantic | **29** / 74 / 1165 ms | **12.6** / 9.9 ms | **13.93** req/s | **43.7%**（24.9% / 18.9%） | **90.6%** | **5.9** ms |

开环，6 req/s：

| 配置 | TTFT P50 / P90 / P99 | TPOT 均值 / P50 | 网关缓存命中（L1+L2 / L3） | 热点请求命中率 | 热点 TTFT P50 / P99 | 新请求 TTFT P50 |
|---|---|---|---|---|---|---|
| direct | 52 / 158 / 1194 ms | 28.9 / 26.8 ms | — | — | 47.9 / 873 ms | 57.3 ms |
| no-dispatch | 57 / 82 / 1178 ms | 16.7 / 25.1 ms | 38.4%（18.1% / 20.3%） | 78.2% | 12.4 / 68.5 ms | 70.0 ms |
| exact | 38 / 74 / 1167 ms | 15.1 / 10.2 ms | 17.9%（17.9% / —） | 36.5% | 25.8 / 65.6 ms | 61.4 ms |
| semantic | **33** / 75 / 1171 ms | **12.4** / 9.9 ms | **38.9%**（18.2% / 20.7%） | **79.3%** | **11.8** / 64.0 ms | 62.8 ms |

结论：

- **L3 语义缓存**（exact → semantic）：同一个意图换了说法、加了客套话，精确匹配就失效了。加上 L3 后，热点请求的命中率从 48.5% 升到 90.6%（闭环）、从 36.5% 升到 79.3%（开环），热点请求的 TTFT P50 从 17.8ms 降到 5.9ms（闭环）、从 25.8ms 降到 11.8ms（开环），闭环吞吐提升 16.8%。要素校验在闭环一轮中拒绝了 68 个相似度达标的候选。
- **L3 的代价**：每个未命中请求都要先做一次向量化 + 检索，压测负载下平均 7.5～8.7ms。开环时后端不忙，这部分延迟就露了出来：新请求（全部未命中）的 TTFT P50 在 no-dispatch 下是 70.0ms，直连是 57.3ms。对 27B 模型、32K 上下文的真实 TTFT（数百毫秒到秒级）来说这点开销不大；如果流量里几乎没有换说法的重复（例如只有逐字重复的请求），可以关掉 L3（`l3.enabled: false`）。
- **语义分发**（no-dispatch → semantic）：寒暄和常见问题分流到小模型池，大模型池只处理分析类、长文档和新请求。闭环下 TTFT P50 从 54ms 降到 29ms、TPOT P50 从 26.6ms 降到 9.9ms、吞吐提升 32%；开环下 TTFT P50 从 57ms 降到 33ms。这一项多用了一个小模型副本，不是同等硬件下的对比，用来说明分类结果确实把请求送到了合适的池。
- **P99 TTFT 几乎不变**：四种配置、两种负载模型的 P99 都在 1.17～1.19s。约 5% 的请求是附带 8K token 新文档的冷 prefill，P99 正好落在它们上面，缓存和分发只缩短了不需要冷 prefill 的那部分请求（阶段 2 的冷 prefill 优化针对的是同一文档被反复引用的场景）。热点请求自己的 P99 从直连的 873ms 降到 64～69ms（开环）。

## mock-vllm

单步耗时模型（毫秒），再乘以故障注入的 `slow_factor` 与全局 `time_scale`：

```
step_ms = step_base_ms + step_per_seq_ms × 本步序列数 + prefill_per_token_ms × 本步 prefill token 数
```

默认参数大致对应单卡 A100 上的 14B 模型（空载 decode 约 20ms/token，prefill 约 8k token/s），拿到真机数据后通过命令行或环境变量（如 `MOCK_STEP_BASE_MS`）覆盖。其他常用参数：`--max-num-seqs`、`--max-num-batched-tokens`、`--num-gpu-blocks`、`--block-size`、`--enable-prefix-caching`、`--output-len-mean`、`--time-scale`（调小可加速实验）。

运行时故障注入：

```bash
curl -X POST localhost:8002/admin/fault -d '{"slow_factor": 3}'      # 慢节点
curl -X POST localhost:8002/admin/fault -d '{"error_rate": 0.2}'     # 20% 请求返回 500
curl -X POST localhost:8002/admin/fault -d '{"down": true}'          # 下线（/metrics、/health 返回 503）
curl -X POST localhost:8002/admin/fault -d '{"down": false, "slow_factor": 1, "error_rate": 0}'
curl localhost:8002/admin/state                                       # 队列、KV 占用、前缀命中率
curl -X POST localhost:8002/admin/reset_prefix_cache
```

每个响应都带 `x-mock-replica` 头，`usage.prompt_tokens_details.cached_tokens` 给出前缀缓存命中的 token 数，可用来检验路由与前缀亲和的效果。

## loadgen

```bash
python loadgen.py --url http://localhost:10000 --mode closed --concurrency 48 --duration 600 --out ../../results/gw
python loadgen.py --url http://localhost:10000 --mode open --rate 8 --duration 600 --out ../../results/gw-open
python compare.py ../../results/base/summary.json ../../results/gw/summary.json
```

负载结构由 `workload.py` 生成，可通过参数调整：`--repeat-ratio`（重复请求占比，默认 0.5）、`--hot-pool-size` / `--hot-zipf-s`（热点池大小与偏斜度）、`--num-system-prompts` / `--system-tokens`（共享前缀）、`--long-ratio` / `--long-tokens`（长上下文）、`--max-tokens` / `--ignore-eos`（输出长度）、`--question-style faq` / `--decorate-ratio`（热点请求换成银行客服的真实问法，每个意图两种说法，再随机加客套话）、`--model higress/auto`（语义分发）。`--admin http://localhost:9901` 会记录本轮网关插件指标的增量（L3 查找 / 命中 / 校验拒绝、查找耗时、模型池分流）。指标口径与 `vllm bench serve` 一致：TPOT = (E2E − TTFT) / (输出 token 数 − 1)。
