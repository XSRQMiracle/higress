#!/usr/bin/env python3
"""生成 Grafana 面板定义 dashboards/maas.json。修改面板时改这个脚本再重新生成：

  python3 gen_dashboard.py
"""
import json
import os
import sys

DS = {"type": "prometheus", "uid": "maas-prometheus"}
panels, pid, y = [], [1], [0]

def nid():
    pid[0] += 1
    return pid[0]

def row(title):
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y[0]}})
    y[0] += 1

def target(expr, legend, ref):
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref, "range": True}

def stat(title, expr, unit, x, w=4, decimals=1, thresholds=None):
    panels.append({
        "type": "stat", "title": title, "id": nid(), "datasource": DS,
        "gridPos": {"h": 4, "w": w, "x": x, "y": y[0]},
        "targets": [target(expr, title, "A")],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "colorMode": "value", "graphMode": "area", "textMode": "auto"},
        "fieldConfig": {"defaults": {"unit": unit, "decimals": decimals,
                                     "thresholds": thresholds or {"mode": "absolute", "steps": [{"color": "green", "value": None}]}},
                        "overrides": []},
    })

def series(title, exprs, unit, x, w=12, h=8, stack=False, desc=""):
    panels.append({
        "type": "timeseries", "title": title, "id": nid(), "datasource": DS, "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": y[0]},
        "targets": [target(e, l, chr(65 + i)) for i, (e, l) in enumerate(exprs)],
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True}, "tooltip": {"mode": "multi", "sort": "desc"}},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 25 if stack else 8,
                                                              "stacking": {"mode": "normal" if stack else "none", "group": "A"}}},
                        "overrides": []},
    })

def q(metric, quantile, by="le"):
    return f"histogram_quantile({quantile}, sum(rate({metric}_bucket[1m])) by ({by}))"

def rate(metric):
    return f"sum(rate({metric}[1m]))"

hits = f"({rate('maas_cache_hits_l1_total')} + {rate('maas_cache_hits_l2_total')} + {rate('maas_cache_hits_l3_total')})"

row("总览")
stat("网关 QPS", 'sum(rate(envoy_http_downstream_rq_total{envoy_http_conn_manager_prefix="maas_gateway"}[1m]))', "reqps", 0, 4, 2)
stat("缓存命中率", f"{hits} / clamp_min({rate('maas_cache_lookups_total')}, 1e-9)", "percentunit", 4, 4, 1)
stat("语义缓存命中占比", f"{rate('maas_cache_hits_l3_total')} / clamp_min({hits}, 1e-9)", "percentunit", 8, 4, 1)
stat("后端 TTFT P99", q("maas_scheduler_ttft_ms", 0.99), "ms", 12, 4, 0,
     {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 1000}, {"color": "red", "value": 2000}]})
stat("TPOT P50", q("maas_scheduler_tpot_ms", 0.5), "ms", 16, 4, 0)
stat("网关排队 P99", q("maas_scheduler_queue_ms", 0.99), "ms", 20, 4, 0)
y[0] += 4

row("多级缓存")
series("各级缓存命中与未命中（次/秒）", [
    (rate("maas_cache_hits_l1_total"), "L1 进程内"), (rate("maas_cache_hits_l2_total"), "L2 Redis"),
    (rate("maas_cache_hits_l3_total"), "L3 语义"), (rate("maas_cache_misses_total"), "未命中"),
    (rate("maas_cache_bypass_total"), "绕过"), (rate("maas_cache_skipped_total"), "不可缓存")], "reqps", 0, stack=True)
series("命中回放与语义检索耗时", [
    (q("maas_cache_hit_latency_ms", 0.5), "命中回放 P50"), (q("maas_cache_hit_latency_ms", 0.99), "命中回放 P99"),
    (q("maas_cache_l3_lookup_ms", 0.5), "L3 向量化+检索 P50"), (q("maas_cache_l3_lookup_ms", 0.99), "L3 向量化+检索 P99")], "ms", 12,
    desc="L3 耗时是语义缓存给未命中请求额外增加的首 token 延迟")
y[0] += 8
series("L3 语义缓存（次/秒）", [
    (rate("maas_cache_l3_lookups_total"), "语义查找"), (rate("maas_cache_hits_l3_total"), "语义命中"),
    (rate("maas_cache_l3_rejects_total"), "要素校验拒绝的候选"), (rate("maas_cache_l3_stores_total"), "登记到向量库"),
    (rate("maas_cache_l3_errors_total"), "向量化 / 检索失败")], "reqps", 0, w=24,
    desc="要素校验拒绝：相似度达到阈值，但数字、否定或关键词与当前问题不一致的候选")
y[0] += 8

row("智能调度")
series("调度决策（次/秒）", [
    (rate("maas_scheduler_prefix_hit_total"), "前缀命中持有者"), (rate("maas_scheduler_affinity_total"), "新前缀送往家副本"),
    (rate("maas_scheduler_cold_spill_total"), "冷 prefill 换副本"), (rate("maas_scheduler_fallback_total"), "回退 Envoy 负载均衡"),
    (f"{rate('maas_scheduler_decisions_total')} - {rate('maas_scheduler_prefix_hit_total')} - {rate('maas_scheduler_affinity_total')} - {rate('maas_scheduler_cold_spill_total')}", "按代价选择")],
    "reqps", 0, stack=True)
series("后端时延（插件观测）", [
    (q("maas_scheduler_ttft_ms", 0.5), "TTFT P50"), (q("maas_scheduler_ttft_ms", 0.99), "TTFT P99"),
    (q("maas_scheduler_tpot_ms", 0.5), "TPOT P50"), (q("maas_scheduler_tpot_ms", 0.99), "TPOT P99")], "ms", 12)
y[0] += 8
series("排队与冷 prefill 优化（次/秒）", [
    (rate("maas_scheduler_coalesced_total"), "冷前缀合并等待"), (rate("maas_scheduler_cold_wait_total"), "等冷 prefill 名额"),
    (rate("maas_scheduler_queued_total"), "网关排队（全部原因）"), (rate("maas_scheduler_queue_timeout_total"), "排队超时")], "reqps", 0)
series("熔断与探测（5 分钟内次数）", [
    ("sum(increase(maas_scheduler_breaker_trips_total[5m]))", "熔断触发"), ("sum(increase(maas_scheduler_probes_total[5m]))", "半开探测"),
    ("sum(increase(maas_scheduler_presence_invalidations_total[5m]))", "前缀索引纠错")], "short", 12)
y[0] += 8

row("推理副本（vLLM 指标）")
series("running / waiting", [
    ('sum by (instance) (vllm:num_requests_running)', "{{instance}} running"),
    ('sum by (instance) (vllm:num_requests_waiting)', "{{instance}} waiting")], "short", 0, w=8)
series("KV cache 占用（显存水位）", [('max by (instance) (vllm:kv_cache_usage_perc)', "{{instance}}")], "percentunit", 8, w=8)
series("后端前缀缓存命中率", [
    ('sum by (instance) (rate(vllm:prefix_cache_hits_total[1m])) / clamp_min(sum by (instance) (rate(vllm:prefix_cache_queries_total[1m])), 1)', "{{instance}}")],
    "percentunit", 16, w=8)
y[0] += 8
series("生成吞吐（token/秒，按模型池）", [('sum by (pool) (rate(vllm:generation_tokens_total[1m]))', "{{pool}}")], "short", 0)
series("后端 TTFT P99（vLLM 自身统计，按模型池）", [
    ('histogram_quantile(0.99, sum by (le, pool) (rate(vllm:time_to_first_token_seconds_bucket[1m])))', "{{pool}}")], "s", 12)
y[0] += 8

row("语义分发")
series("模型池分流（次/秒）", [(rate("maas_router_pool_small_total"), "小模型池"), (rate("maas_router_pool_large_total"), "大模型池")], "reqps", 0, stack=True)
series("分发依据（次/秒）", [
    (rate("maas_router_semantic_total"), "语义分类"), (rate("maas_router_rule_total"), "规则"),
    (rate("maas_router_long_prompt_total"), "长上下文"), (rate("maas_router_default_total"), "默认池"),
    (rate("maas_router_explicit_total"), "显式指定模型"), (rate("maas_router_embedding_reused_total"), "复用缓存插件的向量")], "reqps", 12)
y[0] += 8

dashboard = {
    "uid": "maas-gateway", "title": "MaaS 网关", "tags": ["maas"], "timezone": "browser", "schemaVersion": 39, "version": 1,
    "refresh": "5s", "time": {"from": "now-15m", "to": "now"}, "editable": True, "panels": panels,
}
out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboards", "maas.json")
with open(out, "w", encoding="utf-8") as fh:
    json.dump(dashboard, fh, ensure_ascii=False, indent=2)
    fh.write("\n")
print(f"{len(panels)} panels -> {out}")
