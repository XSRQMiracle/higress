# MaaS 智能路由与多级缓存网关（赛题七）

基于 Higress 二次开发的 MaaS 网关。本目录放部署配置、仿真后端与压测工具；网关插件位于 `plugins/wasm-go/extensions/`：

- [maas-cache](../plugins/wasm-go/extensions/maas-cache/README.md)：多级响应缓存（L1 进程内 W-TinyLFU + L2 Redis），按 vLLM 格式原样回放
- [maas-scheduler](../plugins/wasm-go/extensions/maas-scheduler/README.md)：推理副本调度（当前为能力验证版 + least_waiting 策略）

请求链路：`client → maas-cache →（未命中）→ maas-scheduler → vLLM 副本`。

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
- [ ] L3 语义缓存、缓存 epoch 失效接口、并发未命中合并（后续）

## 目录

```
maas/
├── deploy/
│   ├── docker-compose.yml   # gateway + mock-vllm×3 + redis（+ loadgen，profile: tools）
│   ├── envoy.yaml           # 静态 Envoy 配置：网关入口、轮询基线、健康检查与 store_metrics
│   └── up.sh                # 编译插件并启动
├── tools/
│   ├── mock-vllm/           # vLLM 模拟器（Python + aiohttp）
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
| 10000 | 网关入口：maas-cache → maas-scheduler；请求头 `x-maas-cache-bypass: on` 可跳过缓存 |
| 10001 | 基线：纯 Envoy 轮询，不加载插件（相当于 K8s Service 直连多副本） |
| 8001–8003 | 直连 mock-vllm-0/1/2（含 `/admin/*` 故障注入接口） |
| 9901 | Envoy admin |
| 6379 | Redis（maas-cache 的 L2） |

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

压测（每轮开始前清空副本前缀缓存与网关缓存）：

```bash
cd maas/tools/loadgen
PY="uv run --quiet --python 3.12 --with aiohttp python" ./phase0_bench.sh   # 纯轮询 vs 调度插件（绕过缓存）
PY="uv run --quiet --python 3.12 --with aiohttp python" ./phase1_bench.sh   # 加入缓存后的闭环 / 开环对比，约 18 分钟
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

负载结构由 `workload.py` 生成，可通过参数调整：`--repeat-ratio`（重复请求占比，默认 0.5）、`--hot-pool-size` / `--hot-zipf-s`（热点池大小与偏斜度）、`--num-system-prompts` / `--system-tokens`（共享前缀）、`--long-ratio` / `--long-tokens`（长上下文）、`--max-tokens` / `--ignore-eos`（输出长度）。指标口径与 `vllm bench serve` 一致：TPOT = (E2E − TTFT) / (输出 token 数 − 1)。
