#!/usr/bin/env python3
"""阶段 2 自动验证：前缀亲和、有界负载、显存水位准入与排队、慢节点熔断（约 2 分钟）。

所有请求都带 x-maas-cache-bypass: on，只验证调度插件，不受响应缓存影响。
  python verify_phase2.py
  docker compose run --rm loadgen verify_phase2.py --gateway http://gateway:10000 --admin-template 'http://{replica}:8000'
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
SYSTEM = "你是银行智能客服，请严格依据参考文档回答。"
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


def doc_request(doc: str, question: str, max_tokens: int = 8) -> dict:
    return {
        "model": "mock-qwen",
        "stream": True,
        "stream_options": {"include_usage": True},
        "max_tokens": max_tokens,
        "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": doc + "\n\n" + question}],
    }


class Reply:
    def __init__(self, status: int, headers: dict, usage: dict | None):
        self.status = status
        self.headers = headers
        self.usage = usage or {}

    @property
    def replica(self) -> str | None:
        return self.headers.get("x-mock-replica")

    @property
    def cached(self) -> int:
        return (self.usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0


async def chat(s: aiohttp.ClientSession, gw: str, body: dict, headers: dict | None = None) -> Reply:
    async with s.post(f"{gw}/v1/chat/completions", json=body, headers={**BYPASS, **(headers or {})}) as resp:
        raw = await resp.read()
        usage = None
        for line in raw.decode("utf-8", "replace").splitlines():
            if line.startswith("data: {"):
                event = json.loads(line[6:])
                usage = event.get("usage") or usage
        return Reply(resp.status, {k.lower(): v for k, v in resp.headers.items()}, usage)


async def set_fault(s: aiohttp.ClientSession, admin: str, patch: dict) -> None:
    async with s.post(f"{admin}/admin/fault", json=patch) as resp:
        resp.raise_for_status()


async def burst(s: aiohttp.ClientSession, gw: str, concurrency: int, seconds: float) -> list[tuple[float, str]]:
    """闭环压一段时间，返回 (完成时刻, 副本) 列表。每个请求的问题都不同，max_tokens 固定。"""
    out: list[tuple[float, str]] = []
    start = time.monotonic()
    deadline = start + seconds

    async def worker() -> None:
        while time.monotonic() < deadline:
            body = {
                "model": "mock-qwen", "max_tokens": 48, "ignore_eos": True, "stream": True,
                "messages": [{"role": "user", "content": f"请介绍一下理财产品 {uuid.uuid4().hex}"}],
            }
            reply = await chat(s, gw, body)
            if reply.status == 200:
                out.append((time.monotonic() - start, reply.replica))

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    return out


def share(samples: list[tuple[float, str]], replica: str, since: float) -> tuple[float, int]:
    window = [r for t, r in samples if t >= since]
    return (sum(1 for r in window if r == replica) / len(window) if window else 0.0), len(window)


async def main(args: argparse.Namespace) -> int:
    gw = args.gateway.rstrip("/")
    c = Checker()
    run = uuid.uuid4().hex[:6]

    def admin(replica: str) -> str:
        return args.admin_template.format(replica=replica) if args.admin_template else _HOST_ADMIN[replica]

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as s:
        async with s.get(f"{gw}/maas/debug/hosts") as resp:
            debug = await resp.json()
        replicas = sorted(h["replica"] for h in debug["hosts"])
        print("[1] 调度插件状态")
        c.check(debug["policy"] == "cost", f"默认策略 {debug['policy']}")
        c.check(all(h["kv_capacity_tokens"] > 0 for h in debug["hosts"]), f"读到各副本 KV 容量：{[h['kv_capacity_tokens'] for h in debug['hosts']]}")

        print("[2] 前缀亲和：同一文档、不同问题的请求落到同一副本，并命中后端前缀缓存")
        doc = document(f"affinity-{run}")
        replies = [await chat(s, gw, doc_request(doc, f"问题 {i}：{uuid.uuid4().hex}")) for i in range(8)]
        placed = Counter(r.replica for r in replies)
        c.check(len(placed) == 1, f"8 个请求的分布 {dict(placed)}，首个请求决策原因 {replies[0].headers.get('x-maas-reason')}")
        later = replies[1:]
        ratio = sum(r.cached for r in later) / max(1, sum(r.usage.get("prompt_tokens", 0) for r in later))
        c.check(ratio > 0.9, f"后 7 个请求的后端前缀命中率 {ratio:.1%}，网关预计命中 {later[-1].headers.get('x-maas-expected-hit-tokens')} token")
        doc2 = document(f"baseline-{run}")
        base = [await chat(s, gw, doc_request(doc2, f"问题 {i}：{uuid.uuid4().hex}"), {"x-maas-policy": "least_waiting"}) for i in range(8)]
        base_ratio = sum(r.cached for r in base[1:]) / max(1, sum(r.usage.get("prompt_tokens", 0) for r in base[1:]))
        print(f"  INFO 对照 least_waiting：分布 {dict(Counter(r.replica for r in base))}，后 7 个请求的前缀命中率 {base_ratio:.1%}")

        print("[3] 不同文档分散到不同副本（一致性哈希）")
        spread = Counter()
        for i in range(12):
            reply = await chat(s, gw, doc_request(document(f"spread-{run}-{i}"), "请总结这篇文档。"))
            spread[reply.replica] += 1
        c.check(len(spread) == len(replicas) and max(spread.values()) <= 8, f"12 篇文档的分布 {dict(spread)}")

        print("[4] 有界负载：同一文档的 30 个并发请求不会全部压到一个副本")
        hot = document(f"hot-{run}")
        await chat(s, gw, doc_request(hot, "预热"))
        replies = await asyncio.gather(*(chat(s, gw, doc_request(hot, f"并发问题 {uuid.uuid4().hex}", max_tokens=64)) for _ in range(30)))
        dist = Counter(r.replica for r in replies)
        c.check(max(dist.values()) < 30 * 0.75, f"并发分布 {dict(dist)}")

        victim = replicas[1]
        print(f"[5] 显存水位准入：{victim} 上报 KV 占用 ≥ 95% 后不再接收新请求")
        await set_fault(s, admin(victim), {"kv_usage_offset": 0.95})
        try:
            await asyncio.sleep(1.5)  # 等健康检查刷新指标
            dist = Counter()
            for i in range(20):
                reply = await chat(s, gw, doc_request(document(f"kv-{run}-{i}", 400), "问题"))
                dist[reply.replica] += 1
            c.check(dist[victim] == 0 and sum(dist.values()) == 20, f"20 个请求的分布 {dict(dist)}")
        finally:
            await set_fault(s, admin(victim), {"kv_usage_offset": 0.0})

        print("[6] 排队：所有副本都超过水位时请求在网关排队，水位恢复后放行；一直不恢复则超时后尽力而为")
        for replica in replicas:
            await set_fault(s, admin(replica), {"kv_usage_offset": 0.95})
        await asyncio.sleep(1.5)
        pending = asyncio.create_task(chat(s, gw, doc_request(document(f"queue-{run}", 400), "排队请求")))
        await asyncio.sleep(1.0)
        for replica in replicas:
            await set_fault(s, admin(replica), {"kv_usage_offset": 0.0})
        released = await pending
        queue_ms = int(released.headers.get("x-maas-queue-ms", "0"))
        c.check(
            released.status == 200 and 800 <= queue_ms < 2900 and released.headers.get("x-maas-reason") != "best_effort",
            f"水位恢复后放行：status={released.status} 排队 {queue_ms}ms reason={released.headers.get('x-maas-reason')}",
        )
        for replica in replicas:
            await set_fault(s, admin(replica), {"kv_usage_offset": 0.95})
        try:
            await asyncio.sleep(1.5)
            timed_out = await chat(s, gw, doc_request(document(f"timeout-{run}", 400), "超时请求"))
            queue_ms = int(timed_out.headers.get("x-maas-queue-ms", "0"))
            c.check(
                timed_out.status == 200 and queue_ms >= 2900 and timed_out.headers.get("x-maas-reason") == "best_effort",
                f"超时后尽力而为：status={timed_out.status} 排队 {queue_ms}ms reason={timed_out.headers.get('x-maas-reason')}",
            )
        finally:
            for replica in replicas:
                await set_fault(s, admin(replica), {"kv_usage_offset": 0.0})
        await asyncio.sleep(1.5)

        slow = replicas[-1]
        print(f"[7] 慢节点熔断：{slow} 变慢 {args.slow_factor:g} 倍后被摘除，恢复后经半开探测重新加入")
        await set_fault(s, admin(slow), {"slow_factor": args.slow_factor})
        try:
            samples = await burst(s, gw, args.concurrency, args.slow_seconds)
            slow_share, n = share(samples, slow, args.slow_seconds / 2)
            async with s.get(f"{gw}/maas/debug/hosts") as resp:
                states = {h["replica"]: (h.get("breaker") or {}).get("state") for h in (await resp.json())["hosts"]}
            c.check(slow_share < 0.1, f"变慢后半程 {n} 个请求中慢节点占比 {slow_share:.1%}（均匀分布为 {1 / len(replicas):.0%}）")
            c.check(states.get(slow) in ("open", "half_open"), f"熔断器状态 {states}")
        finally:
            await set_fault(s, admin(slow), {"slow_factor": 1.0})
        samples = await burst(s, gw, args.concurrency, args.recover_seconds)
        recovered, n = share(samples, slow, args.recover_seconds - 8)
        c.check(recovered > 0.15, f"恢复后最后 8 秒 {n} 个请求中该副本占比 {recovered:.1%}")

    print("\n结果：" + ("全部通过" if not c.failures else f"{len(c.failures)} 项失败"))
    return 1 if c.failures else 0


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway", default="http://localhost:10000")
    parser.add_argument("--admin-template", default="", help="副本管理地址模板，如 'http://{replica}:8000'；为空时用宿主机端口映射")
    parser.add_argument("--slow-factor", type=float, default=4.0)
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--slow-seconds", type=float, default=24.0)
    parser.add_argument("--recover-seconds", type=float, default=36.0)
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
