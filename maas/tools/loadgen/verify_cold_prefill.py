#!/usr/bin/env python3
"""冷 prefill 优化的自动验证：冷前缀请求合并、按副本限制并发冷 prefill、普通请求不受影响。

所有请求都带 x-maas-cache-bypass: on，只验证调度插件。依赖 mock-vllm 的 /admin/state 中的
long_prefill_peak（单个副本同时进行的长 prefill 数的峰值）。
  python verify_cold_prefill.py
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
import uuid
from collections import Counter

import aiohttp

_HOST_ADMIN = {"mock-vllm-0": "http://localhost:8001", "mock-vllm-1": "http://localhost:8002", "mock-vllm-2": "http://localhost:8003"}
BYPASS = {"x-maas-cache-bypass": "on"}
CHARS = "的一是在不了有和人这中大为上个国我以要他时来用们生到作地于出就分对成会可主发年动同工也能下过子说产种面而方后多定行学法所"


class Checker:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def check(self, ok: bool, message: str) -> None:
        print(("  PASS " if ok else "  FAIL ") + message)
        if not ok:
            self.failures.append(message)


def document(seed: str, length: int = 3000) -> str:
    rng = random.Random(seed)
    return "".join(rng.choice(CHARS) for _ in range(length))


def body(user: str, max_tokens: int = 8) -> dict:
    return {
        "model": "mock-qwen", "stream": True, "stream_options": {"include_usage": True}, "max_tokens": max_tokens,
        "messages": [{"role": "system", "content": "你是银行智能客服，请严格依据参考文档回答。"}, {"role": "user", "content": user}],
    }


class Reply:
    def __init__(self, status: int, headers: dict, usage: dict, ttft: float):
        self.status, self.headers, self.usage, self.ttft = status, headers, usage, ttft

    @property
    def uncached(self) -> int:
        return self.usage.get("prompt_tokens", 0) - (self.usage.get("prompt_tokens_details") or {}).get("cached_tokens", 0)


async def chat(s: aiohttp.ClientSession, gw: str, payload: dict, headers: dict | None = None) -> Reply:
    start = time.perf_counter()
    ttft = None
    usage = {}
    async with s.post(f"{gw}/v1/chat/completions", json=payload, headers={**BYPASS, **(headers or {})}) as resp:
        async for raw in resp.content:
            line = raw.decode("utf-8", "replace").strip()
            if not line.startswith("data: {"):
                continue
            event = json.loads(line[6:])
            usage = event.get("usage") or usage
            if ttft is None and any((c.get("delta") or {}).get("content") for c in event.get("choices") or []):
                ttft = time.perf_counter() - start
        return Reply(resp.status, {k.lower(): v for k, v in resp.headers.items()}, usage, ttft or 0.0)


async def admin_json(s: aiohttp.ClientSession, url: str, method: str = "GET") -> dict:
    async with s.request(method, url) as resp:
        return await resp.json()


async def main(args: argparse.Namespace) -> int:
    gw = args.gateway.rstrip("/")
    c = Checker()
    run = uuid.uuid4().hex[:6]
    admins = {r: (args.admin_template.format(replica=r) if args.admin_template else u) for r, u in _HOST_ADMIN.items()}

    async def reset_peaks() -> None:
        for url in admins.values():
            await admin_json(s, f"{url}/admin/reset_stats", "POST")

    async def peaks() -> dict:
        return {r: (await admin_json(s, f"{url}/admin/state"))["long_prefill_peak"] for r, url in admins.items()}

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as s:
        print("[1] 冷前缀请求合并：同一新文档的 4 个并发请求只在后端冷 prefill 一次")
        doc = document(f"coalesce-{run}")
        replies = await asyncio.gather(*(chat(s, gw, body(doc + f"\n\n问题 {i}：{uuid.uuid4().hex}")) for i in range(4)))
        cold = [r for r in replies if r.uncached > 1000]
        waited = [r for r in replies if r.headers.get("x-maas-wait-reason") == "coalesce"]
        c.check(all(r.status == 200 for r in replies), f"状态码 {[r.status for r in replies]}")
        c.check(len(cold) == 1, f"后端冷 prefill 次数 {len(cold)}（未命中 token：{[r.uncached for r in replies]}）")
        c.check(len(waited) == 3, f"等待领头请求的跟随者 {len(waited)} 个，排队 {[r.headers.get('x-maas-queue-ms') for r in waited]} ms")
        c.check(len({r.headers.get('x-mock-replica') for r in replies}) == 1, f"全部落在 {Counter(r.headers.get('x-mock-replica') for r in replies)}")
        lead = max(r.ttft for r in cold) if cold else 0
        print(f"  INFO TTFT：领头 {lead * 1000:.0f}ms，跟随者 {[round(r.ttft * 1000) for r in waited]}ms")

        print(f"[2] 冷 prefill 并发限制：6 篇新文档同时到达，每个副本同时最多 {args.max_per_replica} 个冷 prefill")
        await reset_peaks()
        shorts = [chat(s, gw, body(f"普通短问题 {uuid.uuid4().hex}")) for _ in range(5)]
        colds = [chat(s, gw, body(document(f"limit-{run}-{i}") + "\n\n请总结。")) for i in range(6)]
        replies = await asyncio.gather(*colds, *shorts)
        cold_replies, short_replies = replies[:6], replies[6:]
        peak = await peaks()
        c.check(all(r.status == 200 for r in replies), f"状态码 {[r.status for r in replies]}")
        c.check(max(peak.values()) <= args.max_per_replica, f"各副本长 prefill 并发峰值 {peak}")
        reasons = Counter(r.headers.get("x-maas-wait-reason") or "-" for r in cold_replies)
        c.check(reasons["cold_slot"] >= 6 - len(admins) * args.max_per_replica, f"冷请求的排队原因 {dict(reasons)}，决策原因 {dict(Counter(r.headers.get('x-maas-reason') for r in cold_replies))}")
        c.check(all(not r.headers.get("x-maas-wait-reason") for r in short_replies), f"5 个普通请求无需排队，TTFT {[round(r.ttft * 1000) for r in short_replies]} ms")
        debug = await admin_json(s, f"{gw}/maas/debug/hosts")
        c.check(all(h["cold_prefills"] == 0 for h in debug["hosts"]), f"结束后冷 prefill 名额全部归还：{[h['cold_prefills'] for h in debug['hosts']]}")

        print("[3] 对照：least_waiting 策略不限制冷 prefill 并发")
        await reset_peaks()
        replies = await asyncio.gather(*(chat(s, gw, body(document(f"control-{run}-{i}") + "\n\n请总结。"), {"x-maas-policy": "least_waiting"}) for i in range(6)))
        peak = await peaks()
        print(f"  INFO 各副本长 prefill 并发峰值 {peak}，TTFT {[round(r.ttft * 1000) for r in replies]} ms")

    print("\n结果：" + ("全部通过" if not c.failures else f"{len(c.failures)} 项失败"))
    return 1 if c.failures else 0


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway", default="http://localhost:10000")
    parser.add_argument("--admin-template", default="", help="副本管理地址模板，如 'http://{replica}:8000'；为空时用宿主机端口映射")
    parser.add_argument("--max-per-replica", type=int, default=1, help="与网关 coldPrefill.maxPerReplica 一致")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
