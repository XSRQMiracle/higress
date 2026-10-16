#!/usr/bin/env python3
"""阶段 1 自动验证：maas-cache 的命中、回放保真度、缓存键正确性、不可缓存请求与故障放行。

在宿主机运行（默认地址对应 docker-compose 的端口映射）：
  python verify_phase1.py                      # 基础检查
  python verify_phase1.py --docker-checks      # 额外检查 L2（重启网关清空 L1）与 Redis 故障放行，需要 docker compose
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import uuid

import aiohttp

HITS = ("L1", "L2", "L3")
DEPLOY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "deploy")


class Checker:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def check(self, ok: bool, message: str) -> None:
        print(("  PASS " if ok else "  FAIL ") + message)
        if not ok:
            self.failures.append(message)


class Reply:
    def __init__(self, status: int, headers: dict, events: list[dict], done: bool, body: dict | None, first_byte: float):
        self.status = status
        self.headers = headers
        self.events = events  # 流式响应的 JSON 事件（不含 [DONE]）
        self.done = done
        self.body = body  # 非流式响应的 JSON
        self.first_byte = first_byte

    @property
    def cache(self) -> str | None:
        return self.headers.get("x-maas-cache")

    def pieces(self) -> list[str]:
        out = []
        for event in self.events:
            for choice in event.get("choices") or []:
                text = (choice.get("delta") or {}).get("content")
                if text:
                    out.append(text)
        return out

    def finish_reason(self) -> str | None:
        for event in self.events:
            for choice in event.get("choices") or []:
                if choice.get("finish_reason"):
                    return choice["finish_reason"]
        if self.body:
            return self.body["choices"][0]["finish_reason"]
        return None

    def usage(self) -> dict | None:
        for event in self.events:
            if event.get("usage"):
                return event["usage"]
        return self.body.get("usage") if self.body else None


async def call(session: aiohttp.ClientSession, gateway: str, body: dict, headers: dict | None = None) -> Reply:
    start = time.perf_counter()
    async with session.post(f"{gateway}/v1/chat/completions", json=body, headers=headers or {}) as resp:
        raw = await resp.content.read(1)
        first_byte = time.perf_counter() - start
        raw += await resp.read()
        hdrs = {k.lower(): v for k, v in resp.headers.items()}
        if not body.get("stream"):
            return Reply(resp.status, hdrs, [], False, json.loads(raw) if resp.status == 200 else None, first_byte)
        events, done = [], False
        for line in raw.decode("utf-8").splitlines():
            if line.startswith("data:"):
                data = line[5:].strip()
                if data == "[DONE]":
                    done = True
                else:
                    events.append(json.loads(data))
        return Reply(resp.status, hdrs, events, done, None, first_byte)


def request(question: str, system: str = "你是银行智能客服，请简洁回答。", stream: bool = True, **extra) -> dict:
    body = {
        "model": "mock-qwen",
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": question}],
        "max_tokens": 24,
        "temperature": 0.7,
        "stream": stream,
    }
    if stream:
        body["stream_options"] = {"include_usage": True}
    body.update(extra)
    return body


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=DEPLOY_DIR, check=True, capture_output=True)


async def wait_ready(session: aiohttp.ClientSession, gateway: str, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with session.get(f"{gateway}/maas/debug/hosts") as resp:
                if resp.status == 200:
                    hosts = (await resp.json())["hosts"]
                    if hosts and all(h["health"] == "Healthy" for h in hosts):
                        return
        except aiohttp.ClientError:
            pass
        await asyncio.sleep(0.3)
    raise TimeoutError("gateway not ready")


async def main(args: argparse.Namespace) -> int:
    gw = args.gateway.rstrip("/")
    c = Checker()
    run_id = uuid.uuid4().hex[:8]  # 保证每次验证的问题都是新的，不受上一次运行留下的缓存影响

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as s:
        print("[1] 流式请求：第一次未命中，之后命中缓存，回放与原始响应逐帧一致")
        base = request(f"信用卡逾期会影响征信吗？#{run_id}")
        first = await call(s, gw, base)
        c.check(first.status == 200 and first.cache == "miss", f"首次请求 status={first.status} x-maas-cache={first.cache}")
        await asyncio.sleep(0.2)  # 写缓存发生在响应结束时，留出 Redis 写入时间
        second = await call(s, gw, base)
        c.check(second.cache in HITS, f"再次请求 x-maas-cache={second.cache}")
        c.check(second.pieces() == first.pieces() and len(first.pieces()) > 0, f"内容分块一致：{len(first.pieces())} 块 vs {len(second.pieces())} 块")
        c.check(len(second.events) == len(first.events) and second.done, f"SSE 事件数一致：{len(first.events)} vs {len(second.events)}，[DONE]={second.done}")
        c.check(second.events[0]["choices"][0]["delta"].get("role") == "assistant", "首帧携带 role=assistant")
        c.check(second.finish_reason() == first.finish_reason(), f"finish_reason 一致：{first.finish_reason()}")
        fu, su = first.usage() or {}, second.usage() or {}
        c.check(
            su.get("completion_tokens") == fu.get("completion_tokens") and su.get("prompt_tokens") == fu.get("prompt_tokens"),
            f"usage 一致：completion {fu.get('completion_tokens')}/{su.get('completion_tokens')}，prompt {fu.get('prompt_tokens')}/{su.get('prompt_tokens')}",
        )
        c.check(second.events[0].get("model") == first.events[0].get("model"), f"model 一致：{first.events[0].get('model')}")
        c.check(
            second.first_byte < first.first_byte,
            f"首字节时间：命中 {second.first_byte * 1000:.1f}ms，未命中 {first.first_byte * 1000:.1f}ms",
        )

        print("[2] 非流式请求命中同一条缓存（流式 ↔ 非流式互转）")
        plain = await call(s, gw, request(f"信用卡逾期会影响征信吗？#{run_id}", stream=False))
        content = plain.body["choices"][0]["message"]["content"] if plain.body else None
        c.check(plain.cache in HITS and content == "".join(first.pieces()), f"x-maas-cache={plain.cache}，内容一致={content == ''.join(first.pieces())}")

        print("[3] 缓存键规范化：字段顺序不同、带 user 字段的等价请求同样命中")
        shuffled = dict(reversed(list(base.items())))
        shuffled["user"] = "u-123"
        again = await call(s, gw, shuffled)
        c.check(again.cache in HITS, f"x-maas-cache={again.cache}")

        print("[4] 缓存键正确性：会改变答案的字段不同，必须未命中")
        variants = {
            "system prompt": request(f"信用卡逾期会影响征信吗？#{run_id}", system="你是理财顾问。"),
            "temperature": request(f"信用卡逾期会影响征信吗？#{run_id}", temperature=0.1),
            "max_tokens": request(f"信用卡逾期会影响征信吗？#{run_id}", max_tokens=25),
        }
        for name, body in variants.items():
            reply = await call(s, gw, body)
            c.check(reply.cache == "miss", f"不同 {name} → x-maas-cache={reply.cache}")

        print("[5] 不可缓存的请求直接放行")
        for name, extra, want in (("n=2", {"n": 2}, "skip:n"), ("logprobs", {"logprobs": True}, "skip:logprobs")):
            reply = await call(s, gw, request(f"不可缓存 {name} #{run_id}", **extra))
            c.check(reply.status == 200 and reply.cache == want, f"{name} → status={reply.status} x-maas-cache={reply.cache}")
        reply = await call(s, gw, base, {"x-maas-cache-bypass": "on"})
        c.check(reply.cache == "bypass" and bool(reply.headers.get("x-mock-replica")), f"绕过请求头 → x-maas-cache={reply.cache}，由 {reply.headers.get('x-mock-replica')} 处理")

        if args.docker_checks:
            print("[6] L2：重启网关清空所有 worker 的 L1 后，从 Redis 命中")
            compose("restart", "gateway")
            await wait_ready(s, gw)
            reply = await call(s, gw, base)
            c.check(reply.cache == "L2" and reply.pieces() == first.pieces(), f"重启后 x-maas-cache={reply.cache}，内容一致={reply.pieces() == first.pieces()}")

            print("[7] Redis 故障放行：停掉 Redis 后新请求照常由后端处理")
            compose("stop", "redis")
            try:
                fresh = request(f"Redis 故障期间的新问题 #{run_id}")
                replies = [await call(s, gw, fresh) for _ in range(3)]
                statuses = [r.status for r in replies]
                caches = [r.cache for r in replies]
                c.check(all(st == 200 for st in statuses), f"3 个请求状态 {statuses}，x-maas-cache {caches}")
                c.check(all(cache in ("miss", "L1") for cache in caches), "Redis 不可用时只会未命中或命中本 worker 的 L1")
            finally:
                compose("start", "redis")

    print("\n结果：" + ("全部通过" if not c.failures else f"{len(c.failures)} 项失败"))
    return 1 if c.failures else 0


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway", default="http://localhost:10000")
    parser.add_argument("--docker-checks", action="store_true", help="执行需要 docker compose 的 L2 与 Redis 故障检查")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
