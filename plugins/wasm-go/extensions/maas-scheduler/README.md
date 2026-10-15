# maas-scheduler

MaaS 网关的推理副本调度插件（赛题七）。当前是**阶段 0 能力验证版**，用来确认后续调度算法依赖的两项 Higress 扩展能力在静态 Envoy 配置下可用：

| 能力 | 用途 | 验证方式 |
|---|---|---|
| `proxywasm.GetUpstreamHosts()` | 读取当前路由 cluster 中每个副本的健康状态，以及健康检查拉到的 `/metrics` 文本 | `GET /maas/debug/hosts` |
| `proxywasm.SetUpstreamOverrideHost()` | 绕过 Envoy 自带负载均衡，把请求钉到指定副本 | 请求头 `x-maas-target: <ip:port>` |

阶段 2 会在此基础上加入预测 TTFT 代价模型、层级前缀一致性哈希（CHWBL）、显存水位准入与慢节点熔断。

## 配置

```json
{
  "policy": "least_waiting",
  "targetHeader": "x-maas-target",
  "debugHeaders": true,
  "debugPath": "/maas/debug/hosts"
}
```

| 字段 | 默认值 | 说明 |
|---|---|---|
| `policy` | `least_waiting` | `header`：只处理带 `targetHeader` 的请求，其余交给 Envoy；`round_robin`：插件内轮询；`least_waiting`：依次比较 vLLM waiting 队列、本网关在途请求数、KV cache 占用，全部相同则随机 |
| `targetHeader` | `x-maas-target` | 任意策略下，带此请求头的请求都会被钉到指定副本（地址不在 cluster 中时回退） |
| `debugHeaders` | `true` | 在响应中添加 `x-maas-picked`（选中的副本）与 `x-maas-reason`（决策原因） |
| `debugPath` | `/maas/debug/hosts` | 返回插件看到的副本列表与解析出的指标；置空可关闭 |

## 依赖的 cluster 配置

`least_waiting` 依赖每个副本的 vLLM 指标。cluster 需要配置访问 `/metrics` 的主动健康检查，并开启 Higress Envoy 扩展字段 `store_metrics`（`HealthCheck` 第 127 号字段）：

```yaml
health_checks:
  - timeout: 1s
    interval: 0.5s
    no_traffic_interval: 0.5s   # 默认 60s，空闲时指标会严重滞后
    unhealthy_threshold: 2
    healthy_threshold: 1
    http_health_check: { path: /metrics }
    store_metrics: true
```

完整示例见 [maas/deploy/envoy.yaml](../../../../maas/deploy/envoy.yaml)。

## 实现要点

- **override 的时机**：有请求体的请求在 `onHttpRequestHeaders` 返回 `HeaderStopIteration`，到 body 阶段再选副本。若在请求头阶段直接 Continue，router 会在插件做出选择之前就选定上游主机，override 不再生效。
- **指标时效**：健康检查指标最多滞后一个检查周期，因此用本网关的实时在途请求数在两次刷新之间纠偏，并对完全相同的候选随机选择，避免同一周期内的请求全部涌向同一副本。
- **失败放行**：读取副本列表失败、没有健康副本、override 失败时都不拦截请求，交给 cluster 的 `lb_policy` 兜底，原因写入 `x-maas-reason`。
- **WASM 的 worker 隔离**：每个 Envoy worker 有独立的 VM，轮询计数器与在途请求数只在本 worker 内可见。

## 指标

- `maas_scheduler_decisions_total`：成功 override 的请求数
- `maas_scheduler_fallback_total`：回退到 Envoy 负载均衡的请求数

## 构建与测试

```bash
go test ./...
GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o main.wasm .
```
