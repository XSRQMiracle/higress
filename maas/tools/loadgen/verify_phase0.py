#!/usr/bin/env python3
"""阶段 0 自动验证：store_metrics、OverrideHost、副本下线剔除与恢复，以及慢节点下的分流情况。

在宿主机运行（默认地址对应 docker-compose 的端口映射）：
  python verify_phase0.py
在 compose 网络内运行：
  docker compose run --rm loadgen python verify_phase0.py --gateway http://gateway:10000 --admin-template 'http://{replica}:8000'
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections import Counter

import aiohttp

# docker-compose 中 mock-vllm-N 的管理端口映射到宿主机 800(N+1)
_HOST_ADMIN = {"mock-vllm-0": "http://localhost:8001", "mock-vllm-1": "http://localhost:8002", "mock-vllm-2": "http://localhost:8003"}
_CHAT = {"model": "mock-qwen", "max_tokens": 4, "messages": [{"role": "user", "content": "ping"}]}


class Checker:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def check(self, ok: bool, message: str) -> None:
        print(("  PASS " if ok else "  FAIL ") + message)
        if not ok:
            self.failures.append(message)


async def debug_hosts(session: aiohttp.ClientSession, gateway: str) -> dict:
    async with session.get(f"{gateway}/maas/debug/hosts") as resp:
        return await resp.json()


async def chat(session: aiohttp.ClientSession, gateway: str, headers: dict | None = None) -> tuple[int, dict]:
    async with session.post(f"{gateway}/v1/chat/completions", json=_CHAT, headers=headers or {}) as resp:
        await resp.read()
        return resp.status, {k.lower(): v for k, v in resp.headers.items()}


async def set_fault(session: aiohttp.ClientSession, admin: str, patch: dict) -> None:
    async with session.post(f"{admin}/admin/fault", json=patch) as resp:
        resp.raise_for_status()


async def wait_health(session: aiohttp.ClientSession, gateway: str, address: str, healthy: bool, timeout: float) -> float | None:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        hosts = (await debug_hosts(session, gateway))["hosts"]
        state = next((h["health"] for h in hosts if h["address"] == address), None)
        if (state == "Healthy") == healthy:
            return time.monotonic() - start
        await asyncio.sleep(0.05)
    return None


async def burst(session: aiohttp.ClientSession, gateway: str, concurrency: int, seconds: float) -> Counter:
    """闭环压一小段时间，统计各副本处理的请求数。"""
    body = {"model": "mock-qwen", "max_tokens": 64, "ignore_eos": True, "messages": [{"role": "user", "content": "请介绍一下信用卡分期的费用"}]}
    counts: Counter = Counter()
    deadline = time.monotonic() + seconds

    async def worker() -> None:
        while time.monotonic() < deadline:
            async with session.post(f"{gateway}/v1/chat/completions", json=body) as resp:
                await resp.read()
                if resp.status == 200:
                    counts[resp.headers.get("x-mock-replica")] += 1

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    return counts


async def main(args: argparse.Namespace) -> int:
    gw = args.gateway.rstrip("/")
    c = Checker()

    def admin(replica: str) -> str:
        return args.admin_template.format(replica=replica) if args.admin_template else _HOST_ADMIN[replica]

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as s:
        print("[1] store_metrics：网关能否读到每个副本的健康状态和 /metrics")
        hosts = (await debug_hosts(s, gw))["hosts"]
        c.check(len(hosts) >= 2, f"插件看到 {len(hosts)} 个副本")
        for h in hosts:
            c.check(
                h["health"] == "Healthy" and h["has_metrics"] and bool(h.get("replica")),
                f"{h['address']} -> {h.get('replica')} health={h['health']} metrics={h['metrics_bytes']}B "
                f"waiting={h['waiting']} running={h['running']} kv={h['kv_cache_usage']}",
            )
        addr2rep = {h["address"]: h["replica"] for h in hosts}
        rep2addr = {r: a for a, r in addr2rep.items()}

        print("[2] OverrideHost：带 x-maas-target 的请求是否落到指定副本")
        for address, replica in sorted(addr2rep.items(), key=lambda kv: kv[1]):
            got: Counter = Counter()
            for _ in range(args.per_target):
                status, headers = await chat(s, gw, {"x-maas-target": address})
                got[headers.get("x-mock-replica") if status == 200 else f"http {status}"] += 1
            c.check(got == Counter({replica: args.per_target}), f"target {address} ({replica}) -> {dict(got)}")
        status, headers = await chat(s, gw, {"x-maas-target": "203.0.113.1:8000"})
        c.check(
            status == 200 and headers.get("x-maas-reason") == "target_not_found",
            f"未知目标回退给 Envoy 负载均衡：status={status} reason={headers.get('x-maas-reason')}",
        )

        print("[3] 策略选择：不带请求头时由插件在 body 阶段选副本")
        got = Counter()
        mismatch = 0
        for _ in range(args.policy_requests):
            status, headers = await chat(s, gw)
            got[headers.get("x-mock-replica")] += 1
            if addr2rep.get(headers.get("x-maas-picked")) != headers.get("x-mock-replica"):
                mismatch += 1
        c.check(mismatch == 0, f"{args.policy_requests} 个请求分布 {dict(got)}，x-maas-picked 与实际副本不一致 {mismatch} 次")

        replicas = sorted(rep2addr)
        victim = replicas[1 % len(replicas)]
        print(f"[4] 副本下线：{victim} 健康检查失败后不再被选中，恢复后重新加入")
        await set_fault(s, admin(victim), {"down": True})
        try:
            took = await wait_health(s, gw, rep2addr[victim], False, 10)
            c.check(took is not None, f"{victim} 被标记为不健康，用时 {took:.2f}s" if took is not None else f"{victim} 10s 内未被标记为不健康")
            got = Counter()
            for _ in range(args.policy_requests):
                status, headers = await chat(s, gw)
                got[headers.get("x-mock-replica") if status == 200 else f"http {status}"] += 1
            c.check(got[victim] == 0 and sum(v for k, v in got.items() if k in rep2addr) == args.policy_requests, f"下线期间请求分布 {dict(got)}")
        finally:
            await set_fault(s, admin(victim), {"down": False})
        took = await wait_health(s, gw, rep2addr[victim], True, 10)
        c.check(took is not None, f"{victim} 恢复健康，用时 {took:.2f}s" if took is not None else f"{victim} 10s 内未恢复")

        slow = replicas[-1]
        print(f"[5] 慢节点（信息项）：{slow} 变慢 {args.slow_factor:g} 倍时 least_waiting 的分流情况")
        baseline = await burst(s, gw, args.burst_concurrency, args.burst_seconds)
        await set_fault(s, admin(slow), {"slow_factor": args.slow_factor})
        try:
            degraded = await burst(s, gw, args.burst_concurrency, args.burst_seconds)
        finally:
            await set_fault(s, admin(slow), {"slow_factor": 1.0})
        for label, dist in (("正常", baseline), ("慢节点", degraded)):
            total = sum(dist.values()) or 1
            print(f"  INFO {label}：{dict(sorted(dist.items()))}，{slow} 占比 {dist[slow] / total:.1%}")

    print("\n结果：" + ("全部通过" if not c.failures else f"{len(c.failures)} 项失败"))
    return 1 if c.failures else 0


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway", default="http://localhost:10000")
    parser.add_argument("--admin-template", default="", help="副本管理地址模板，如 'http://{replica}:8000'；为空时用宿主机端口映射")
    parser.add_argument("--per-target", type=int, default=5)
    parser.add_argument("--policy-requests", type=int, default=30)
    parser.add_argument("--slow-factor", type=float, default=4.0)
    parser.add_argument("--burst-concurrency", type=int, default=24)
    parser.add_argument("--burst-seconds", type=float, default=10.0)
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
