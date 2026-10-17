#!/usr/bin/env python3
"""MaaS 网关压测工具：按决赛口径测量 TTFT / TPOT / 吞吐。

两种负载模型（决赛压测模型待确认，所以两种都要测）：
  * 闭环 --mode closed --concurrency C：C 个虚拟用户，收到完整响应后立即发下一个请求；
  * 开环 --mode open --rate R：按泊松过程以平均 R 个/秒发请求，不受响应快慢影响。

指标口径与 vllm bench serve 一致：
  TTFT = 发出请求 → 收到第一个非空内容 token
  TPOT = (端到端时延 − TTFT) / (输出 token 数 − 1)，输出 token 数优先取 usage.completion_tokens
  ITL  = 相邻两个内容 chunk 的间隔（蓄水池抽样，最多保留 --itl-samples 个）

示例：
  python loadgen.py --url http://localhost:10000 --mode closed --concurrency 48 --duration 120 --out ../../results/gw
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import random
import re
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass, fields

import aiohttp

from workload import Workload, WorkloadConfig


@dataclass
class Result:
    idx: int
    kind: str
    key: str
    system_id: int
    doc_id: int | None
    start: float  # 相对压测开始的秒数
    warmup: bool
    status: int = 0
    error: str | None = None
    ttft: float | None = None
    e2e: float | None = None
    tpot: float | None = None
    output_tokens: int = 0
    prompt_tokens: int = 0
    cached_tokens: int = 0
    chunks: int = 0
    replica: str | None = None  # mock-vllm 的 x-mock-replica
    picked: str | None = None  # maas-scheduler 的 x-maas-picked
    reason: str | None = None  # maas-scheduler 的 x-maas-reason
    cache: str | None = None  # maas-cache（阶段 1）的 x-maas-cache


class Reservoir:
    """固定容量的蓄水池抽样，避免长时间压测把所有 ITL 样本都留在内存里。"""

    def __init__(self, capacity: int, seed: int):
        self.capacity = capacity
        self.samples: list[float] = []
        self.seen = 0
        self.rng = random.Random(seed)

    def add(self, value: float) -> None:
        self.seen += 1
        if len(self.samples) < self.capacity:
            self.samples.append(value)
        else:
            j = self.rng.randrange(self.seen)
            if j < self.capacity:
                self.samples[j] = value


class _Discard:
    def add(self, value: float) -> None:
        pass


async def send_one(session: aiohttp.ClientSession, url: str, body: dict, headers: dict, res: Result, itl) -> None:
    t0 = time.perf_counter()
    last = None
    try:
        async with session.post(url, json=body, headers=headers) as resp:
            res.status = resp.status
            res.replica = resp.headers.get("x-mock-replica")
            res.picked = resp.headers.get("x-maas-picked")
            res.reason = resp.headers.get("x-maas-reason")
            res.cache = resp.headers.get("x-maas-cache")
            if resp.status != 200:
                res.error = f"http {resp.status}: {(await resp.text())[:120]}"
                return
            async for raw in resp.content:
                line = raw.strip()
                if not line.startswith(b"data:"):
                    continue
                data = line[5:].strip()
                if data == b"[DONE]":
                    break
                event = json.loads(data)
                usage = event.get("usage")
                if usage:
                    res.output_tokens = usage.get("completion_tokens") or 0
                    res.prompt_tokens = usage.get("prompt_tokens") or 0
                    res.cached_tokens = (usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
                for choice in event.get("choices") or []:
                    delta = choice.get("delta") or {}
                    # 思考模型先输出 reasoning_content（部分 vLLM 版本字段名为 reasoning），同样计入首 token
                    text = delta.get("content") or delta.get("reasoning_content") or delta.get("reasoning") or choice.get("text")
                    if text:
                        now = time.perf_counter()
                        if res.ttft is None:
                            res.ttft = now - t0
                        else:
                            itl.add(now - last)
                        last = now
                        res.chunks += 1
        res.e2e = time.perf_counter() - t0
        if not res.output_tokens:
            res.output_tokens = res.chunks
        if res.ttft is None:
            res.error = "no content token received"
        elif res.output_tokens > 1:
            res.tpot = (res.e2e - res.ttft) / (res.output_tokens - 1)
    except asyncio.TimeoutError:
        res.error = "timeout"
    except (aiohttp.ClientError, json.JSONDecodeError) as exc:
        res.error = f"{type(exc).__name__}: {exc}"[:160]


_METRIC_LINE = re.compile(r"^(maas_[a-zA-Z0-9_]*)(?:\{[^}]*\})?\s+(\S+)$")


async def gateway_metrics(session: aiohttp.ClientSession, admin: str) -> dict[str, float]:
    """读取网关插件的自定义指标（Envoy admin 的 /stats/prometheus 中 maas_ 开头的序列，同名求和）。"""
    async with session.get(admin.rstrip("/") + "/stats/prometheus") as resp:
        text = await resp.text()
    out: dict[str, float] = {}
    for line in text.splitlines():
        m = _METRIC_LINE.match(line)
        if m and not m.group(1).endswith("_bucket"):
            out[m.group(1)] = out.get(m.group(1), 0.0) + float(m.group(2))
    return out


async def run(args: argparse.Namespace, workload: Workload) -> tuple[list[Result], Reservoir, float, dict]:
    url = args.url.rstrip("/") + args.endpoint
    headers = dict(h.split(":", 1) for h in args.header)
    headers = {k.strip(): v.strip() for k, v in headers.items()}
    itl = Reservoir(args.itl_samples, args.seed)
    results: list[Result] = []
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        before = await gateway_metrics(session, args.admin) if args.admin else {}
        t_start = time.perf_counter()
        deadline = t_start + args.warmup + args.duration
        budget = args.num_requests or math.inf

        def new_result() -> tuple[Result, dict]:
            spec = workload.next()
            elapsed = time.perf_counter() - t_start
            res = Result(
                idx=len(results), kind=spec.kind, key=spec.key, system_id=spec.system_id, doc_id=spec.doc_id,
                start=elapsed, warmup=elapsed < args.warmup,
            )
            results.append(res)
            return res, spec.body

        if args.mode == "closed":
            async def worker() -> None:
                while time.perf_counter() < deadline and len(results) < budget:
                    res, body = new_result()
                    await send_one(session, url, body, headers, res, _Discard() if res.warmup else itl)

            await asyncio.gather(*(worker() for _ in range(args.concurrency)))
        else:
            rng = random.Random(args.seed + 1)
            pending: set[asyncio.Task] = set()
            next_at = time.perf_counter()
            while len(results) < budget:
                now = time.perf_counter()
                if next_at > now:
                    await asyncio.sleep(next_at - now)
                if time.perf_counter() >= deadline:
                    break
                next_at += rng.expovariate(args.rate)
                res, body = new_result()
                if len(pending) >= args.max_inflight:  # 客户端自身过载：记为错误而不是推迟，保持到达过程不变
                    res.error = "client max_inflight reached"
                    continue
                task = asyncio.create_task(send_one(session, url, body, headers, res, _Discard() if res.warmup else itl))
                pending.add(task)
                task.add_done_callback(pending.discard)
            if pending:
                await asyncio.wait(pending, timeout=args.timeout)
        wall = time.perf_counter() - t_start
        delta = {}
        if args.admin:
            await asyncio.sleep(5.5)  # Envoy 每 5 秒汇总一次直方图
            after = await gateway_metrics(session, args.admin)
            delta = {k: round(v - before.get(k, 0.0), 3) for k, v in sorted(after.items()) if v != before.get(k, 0.0)}
    for res in results:
        if res.error is None and res.e2e is None:
            res.error = "unfinished"
    return results, itl, wall, delta


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * p / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def describe(values: list[float], scale: float = 1.0) -> dict:
    if not values:
        return {"n": 0}
    return {
        "n": len(values),
        "mean": sum(values) / len(values) * scale,
        "p50": percentile(values, 50) * scale,
        "p90": percentile(values, 90) * scale,
        "p99": percentile(values, 99) * scale,
        "max": max(values) * scale,
    }


_GATEWAY_HITS = ("L1", "L2", "L3")

# 分组：all 全部；hot / new 按负载类型；gw_hit / gw_miss 按是否命中网关缓存（x-maas-cache 响应头）
_GROUPS = {
    "all": lambda r: True,
    "hot": lambda r: r.kind == "hot",
    "new": lambda r: r.kind == "new",
    "gw_hit": lambda r: r.cache in _GATEWAY_HITS,
    "gw_miss": lambda r: r.cache not in _GATEWAY_HITS,
}


def summarize(args: argparse.Namespace, results: list[Result], itl: Reservoir, wall: float, gateway: dict) -> dict:
    measured = [r for r in results if not r.warmup]
    ok = [r for r in measured if r.error is None]
    window = max((r.start + r.e2e for r in ok), default=args.warmup) - args.warmup
    window = window if window > 0 else max(wall - args.warmup, 1e-9)
    groups = {}
    for name, member in _GROUPS.items():
        rows = [r for r in ok if member(r)]
        if name.startswith("gw_") and not any(r.cache for r in ok):
            continue  # 没有经过 maas-cache（例如基线），不输出缓存分组
        groups[name] = {
            "requests": len([r for r in measured if member(r)]),
            "ok": len(rows),
            "ttft_ms": describe([r.ttft for r in rows if r.ttft is not None], 1000),
            "tpot_ms": describe([r.tpot for r in rows if r.tpot is not None], 1000),
            "e2e_s": describe([r.e2e for r in rows if r.e2e is not None]),
            "output_tokens": sum(r.output_tokens for r in rows),
        }
    # 后端前缀缓存命中率只统计真正到达后端的请求（网关缓存命中的回放会把 cached_tokens 记为全部 prompt）
    backend = [r for r in ok if r.cache not in _GATEWAY_HITS]
    prompt_tokens = sum(r.prompt_tokens for r in backend)
    gateway_hits = sum(1 for r in ok if r.cache in _GATEWAY_HITS)
    return {
        # 请求头的值可能含鉴权信息，只记录名字
        "config": {**{k: v for k, v in vars(args).items() if k != "header"}, "header_names": [h.split(":", 1)[0].strip() for h in args.header]},
        "wall_seconds": wall,
        "window_seconds": window,
        "throughput_rps": len(ok) / window,
        "output_tokens_per_s": groups["all"]["output_tokens"] / window,
        "error_count": len(measured) - len(ok),
        "errors": dict(Counter((r.error or "")[:80] for r in measured if r.error).most_common(5)),
        "backend_prefix_cache_hit_rate": (sum(r.cached_tokens for r in backend) / prompt_tokens) if prompt_tokens else None,
        "gateway_cache_hit_rate": gateway_hits / len(ok) if ok else None,
        "gateway_cache": dict(Counter(r.cache or "none" for r in ok)),
        "replicas": dict(sorted(Counter(r.replica or "unknown" for r in ok).items())),
        "scheduler_reasons": dict(Counter(r.reason or "none" for r in ok)),
        "itl_ms": describe(itl.samples, 1000),
        "groups": groups,
        # 整轮（含预热）网关插件指标的增量，需要 --admin
        "gateway_metrics": gateway,
    }


def _fmt(stats: dict, key: str) -> str:
    value = stats.get(key)
    return "-" if value is None else f"{value:.0f}" if value >= 100 else f"{value:.1f}"


def print_summary(summary: dict) -> None:
    cfg = summary["config"]
    load = f"concurrency={cfg['concurrency']}" if cfg["mode"] == "closed" else f"rate={cfg['rate']}/s"
    print(f"\n== {cfg['label'] or cfg['url']} | {cfg['mode']}-loop {load} | {cfg['duration']}s (+{cfg['warmup']}s warmup) ==")
    print(f"{'group':<7} {'ok/req':>11}  {'TTFT p50/p90/p99 ms':>22}  {'TPOT mean/p50/p99 ms':>22}  {'E2E p99 s':>9}")
    for name, g in summary["groups"].items():
        t, p, e = g["ttft_ms"], g["tpot_ms"], g["e2e_s"]
        print(
            f"{name:<7} {g['ok']:>5}/{g['requests']:<5}  "
            f"{_fmt(t, 'p50'):>6} /{_fmt(t, 'p90'):>6} /{_fmt(t, 'p99'):>6}  "
            f"{_fmt(p, 'mean'):>6} /{_fmt(p, 'p50'):>6} /{_fmt(p, 'p99'):>6}  {_fmt(e, 'p99'):>9}"
        )
    hit = summary["backend_prefix_cache_hit_rate"]
    gw_hit = summary["gateway_cache_hit_rate"]
    print(
        f"throughput {summary['throughput_rps']:.2f} req/s, {summary['output_tokens_per_s']:.0f} out tok/s | "
        f"gateway cache hit {'-' if gw_hit is None else f'{gw_hit:.1%}'} | "
        f"backend prefix-cache hit {'-' if hit is None else f'{hit:.1%}'} | errors {summary['error_count']} {summary['errors'] or ''}"
    )
    print(f"replicas {summary['replicas']} | scheduler {summary['scheduler_reasons']} | gateway cache {summary['gateway_cache']}")
    m = summary.get("gateway_metrics") or {}
    if m.get("maas_cache_l3_lookups_total"):
        lookup_ms = m.get("maas_cache_l3_lookup_ms_sum", 0) / max(m.get("maas_cache_l3_lookup_ms_count", 0), 1)
        print(
            f"L3: lookups {m['maas_cache_l3_lookups_total']:.0f}, hits {m.get('maas_cache_hits_l3_total', 0):.0f}, "
            f"guard rejects {m.get('maas_cache_l3_rejects_total', 0):.0f}, errors {m.get('maas_cache_l3_errors_total', 0):.0f}, "
            f"mean lookup {lookup_ms:.1f} ms"
        )
    pools = {k[len("maas_router_pool_"):-len("_total")]: int(v) for k, v in m.items() if k.startswith("maas_router_pool_")}
    if pools:
        print(f"router pools {pools}")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:10000", help="网关或后端的基础地址")
    parser.add_argument("--endpoint", default="/v1/chat/completions")
    parser.add_argument("--label", default="", help="结果标题，便于区分 baseline / gateway")
    parser.add_argument("--mode", choices=["closed", "open"], default="closed")
    parser.add_argument("--concurrency", type=int, default=32, help="闭环并发数")
    parser.add_argument("--rate", type=float, default=5.0, help="开环平均 QPS（泊松到达）")
    parser.add_argument("--duration", type=float, default=60.0, help="统计窗口时长（秒），决赛口径为 600")
    parser.add_argument("--warmup", type=float, default=10.0, help="预热时长（秒），这段时间内发出的请求不计入统计")
    parser.add_argument("--num-requests", type=int, default=0, help="限制总请求数（0 表示只按时长）")
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--max-inflight", type=int, default=4096, help="开环模式下客户端的最大在途请求数")
    parser.add_argument("--itl-samples", type=int, default=200000)
    parser.add_argument("--header", action="append", default=[], help="附加请求头，格式 'name: value'，可重复")
    parser.add_argument("--out", default="", help="输出目录：写入 requests.jsonl 与 summary.json")
    parser.add_argument("--admin", default="", help="网关 Envoy admin 地址（如 http://localhost:9901），记录本轮插件指标的增量")
    for f in fields(WorkloadConfig):
        flag = "--" + f.name.replace("_", "-")
        if f.type in ("bool", bool):
            parser.add_argument(flag, action=argparse.BooleanOptionalAction, default=f.default)
        else:
            kind = {"int": int, "float": float, "str": str}[f.type]
            parser.add_argument(flag, type=kind, default=f.default)
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    workload = Workload(WorkloadConfig(**{f.name: getattr(args, f.name) for f in fields(WorkloadConfig)}))
    results, itl, wall, gateway = asyncio.run(run(args, workload))
    summary = summarize(args, results, itl, wall, gateway)
    print_summary(summary)
    if args.out:
        os.makedirs(args.out, exist_ok=True)
        with open(os.path.join(args.out, "requests.jsonl"), "w", encoding="utf-8") as fh:
            for res in results:
                fh.write(json.dumps(asdict(res), ensure_ascii=False) + "\n")
        with open(os.path.join(args.out, "summary.json"), "w", encoding="utf-8") as fh:
            json.dump(summary, fh, ensure_ascii=False, indent=2)
        print(f"results written to {args.out}")
    if summary["groups"]["all"]["ok"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
