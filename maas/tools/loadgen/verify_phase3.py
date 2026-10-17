#!/usr/bin/env python3
"""阶段 3 自动验证：语义分发（maas-router）、L3 语义缓存与要素校验、故障放行、监控面板。

在宿主机运行（默认地址对应 docker-compose 的端口映射）：
  python verify_phase3.py                      # 语义分发、L3、监控
  python verify_phase3.py --docker-checks      # 额外停掉 embedder / Qdrant，验证故障放行，需要 docker compose
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time
import uuid

import aiohttp

DEPLOY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "deploy")
POOLS = {"small": ("mock-qwen-small", {"mock-vllm-small"}), "large": ("mock-qwen", {"mock-vllm-0", "mock-vllm-1", "mock-vllm-2"})}
BYPASS = {"x-maas-cache-bypass": "on"}
METRIC_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)$")


class Checker:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def check(self, ok: bool, message: str) -> None:
        print(("  PASS " if ok else "  FAIL ") + message)
        if not ok:
            self.failures.append(message)


class Reply:
    def __init__(self, status: int, headers: dict, events: list[dict], body: dict | None, first_byte: float, total: float):
        self.status, self.headers, self.events, self.body = status, headers, events, body
        self.first_byte, self.total = first_byte, total

    def h(self, name: str) -> str | None:
        return self.headers.get(name)

    def pieces(self) -> list[str]:
        if self.body:
            return [self.body["choices"][0]["message"]["content"]]
        return [c["delta"]["content"] for e in self.events for c in e.get("choices") or [] if (c.get("delta") or {}).get("content")]

    def text(self) -> str:
        return "".join(self.pieces())

    def model(self) -> str | None:
        if self.body:
            return self.body.get("model")
        return self.events[0].get("model") if self.events else None

    def usage(self) -> dict:
        if self.body:
            return self.body.get("usage") or {}
        return next((e["usage"] for e in self.events if e.get("usage")), {})


async def chat(s: aiohttp.ClientSession, gw: str, question: str, *, model: str = "mock-qwen", system: str | None = None,
               stream: bool = True, headers: dict | None = None, max_tokens: int = 16) -> Reply:
    messages = ([{"role": "system", "content": system}] if system else []) + [{"role": "user", "content": question}]
    body = {"model": model, "messages": messages, "max_tokens": max_tokens, "temperature": 0.2, "stream": stream}
    if stream:
        body["stream_options"] = {"include_usage": True}
    start = time.perf_counter()
    async with s.post(f"{gw}/v1/chat/completions", json=body, headers=headers or {}) as resp:
        raw = await resp.content.read(1)
        first_byte = time.perf_counter() - start
        raw += await resp.read()
        hdrs = {k.lower(): v for k, v in resp.headers.items()}
    total = time.perf_counter() - start
    if resp.status != 200:
        return Reply(resp.status, hdrs, [], None, first_byte, total)
    if not stream:
        return Reply(resp.status, hdrs, [], json.loads(raw), first_byte, total)
    events = [json.loads(line[5:]) for line in raw.decode().splitlines() if line.startswith("data:") and line[5:].strip() != "[DONE]"]
    return Reply(resp.status, hdrs, events, None, first_byte, total)


async def gateway_metrics(s: aiohttp.ClientSession, admin: str) -> dict[str, float]:
    async with s.get(f"{admin}/stats/prometheus") as resp:
        text = await resp.text()
    out: dict[str, float] = {}
    for line in text.splitlines():
        m = METRIC_LINE.match(line)
        if m and m.group(1).startswith("maas_"):
            out[m.group(1)] = out.get(m.group(1), 0.0) + float(m.group(3))
    return out


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=DEPLOY_DIR, check=True, capture_output=True)


async def wait_until(probe, timeout: float = 30, interval: float = 0.5) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if await probe():
                return True
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass
        await asyncio.sleep(interval)
    return False


async def main(args: argparse.Namespace) -> int:
    gw, admin = args.gateway.rstrip("/"), args.admin.rstrip("/")
    c = Checker()
    run = uuid.uuid4().hex[:8]  # 每次运行使用新的问题与上下文，不受上一次留下的缓存影响

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as s:
        print("[1] 语义分发：model=higress/auto 时按问题语义选择模型池（绕过缓存，路由插件自己向量化）")
        cases = [
            ("你好呀，请问你是谁？", "small", "semantic", "chitchat"),
            ("信用卡账单分期以后还能提前还清吗？", "small", "semantic", "faq"),
            ("请帮我分析一下这家公司近三年的现金流和偿债风险，并给出投资建议", "large", "semantic", "analysis"),
            ("用 Python 写一个计算等额本息月供的函数", "large", "rule", None),
            ("请阅读下面的材料并总结要点：" + "银行业务相关的长篇参考资料。" * 200, "large", "long_prompt", None),
        ]
        for question, pool, reason, cls in cases:
            r = await chat(s, gw, f"{question} #{run}", model="higress/auto", headers=BYPASS)
            model, replicas = POOLS[pool]
            label = question if len(question) < 40 else question[:20] + "…（长文本）"
            c.check(
                r.status == 200 and r.h("x-maas-route-pool") == pool and r.h("x-maas-route-reason") == reason
                and r.h("x-maas-route-class") == cls and r.h("x-mock-replica") in replicas and r.model() == model,
                f"{label} → pool={r.h('x-maas-route-pool')} reason={r.h('x-maas-route-reason')} class={r.h('x-maas-route-class')} "
                f"sim={r.h('x-maas-route-similarity')} replica={r.h('x-mock-replica')} model={r.model()}",
            )

        print("[2] 显式指定模型：直接送往对应模型池，客户端伪造的 x-maas-pool 会被覆盖")
        for model, pool, extra in (("mock-qwen-small", "small", {}), ("mock-qwen", "large", {"x-maas-pool": "small"})):
            r = await chat(s, gw, f"显式模型 {model} #{run}", model=model, headers={**BYPASS, **extra})
            c.check(
                r.status == 200 and r.h("x-maas-route-pool") == pool and r.h("x-maas-route-reason") == "model" and r.h("x-mock-replica") in POOLS[pool][1],
                f"model={model}{' + 伪造 x-maas-pool: small' if extra else ''} → pool={r.h('x-maas-route-pool')} replica={r.h('x-mock-replica')}",
            )

        print("[3] 向量复用：缓存插件在 L3 查找时算好的问题向量，被路由插件直接复用")
        before = await gateway_metrics(s, admin)
        r = await chat(s, gw, f"银行卡丢了应该怎么挂失 {run}", model="higress/auto")
        after = await gateway_metrics(s, admin)
        reused = after.get("maas_router_embedding_reused_total", 0) - before.get("maas_router_embedding_reused_total", 0)
        c.check(r.status == 200 and r.h("x-maas-cache") == "miss" and r.h("x-maas-route-reason") == "semantic" and reused >= 1,
                f"x-maas-cache={r.h('x-maas-cache')} route={r.h('x-maas-route-pool')}/{r.h('x-maas-route-class')}，复用计数 +{reused:.0f}")

        print("[4] L3 语义缓存：同一上下文中换个说法的问题命中，回放与原始响应一致")
        system = f"你是银行智能客服，请简洁回答。会话 {run}"
        base = await chat(s, gw, "信用卡逾期一天会影响征信吗？", system=system)
        c.check(base.status == 200 and base.h("x-maas-cache") == "miss", f"原问题 x-maas-cache={base.h('x-maas-cache')}")
        await asyncio.sleep(0.5)  # 向量写入是异步的（wait=false）
        para = await chat(s, gw, "信用卡逾期1天对征信有影响吗？", system=system)
        score = float(para.h("x-maas-semantic-score") or 0)
        c.check(para.h("x-maas-cache") == "L3" and score >= args.threshold, f"改写问题 x-maas-cache={para.h('x-maas-cache')} 相似度={score:.4f}")
        c.check(para.pieces() == base.pieces() and len(para.events) == len(base.events), f"回放内容与分块一致：{len(base.pieces())} 块 vs {len(para.pieces())} 块")
        c.check(para.usage().get("completion_tokens") == base.usage().get("completion_tokens"), f"usage 一致：{base.usage().get('completion_tokens')} / {para.usage().get('completion_tokens')}")
        c.check(para.first_byte < base.first_byte, f"首字节时间：L3 命中 {para.first_byte * 1000:.1f}ms，未命中 {base.first_byte * 1000:.1f}ms")
        await asyncio.sleep(0.3)
        again = await chat(s, gw, "信用卡逾期1天对征信有影响吗？", system=system)
        c.check(again.h("x-maas-cache") in ("L1", "L2"), f"同一改写再问一次 → x-maas-cache={again.h('x-maas-cache')}（L3 命中时已按精确键写回）")
        plain = await chat(s, gw, "信用卡逾期1天会影响征信吗？", system=system, stream=False)
        c.check(plain.h("x-maas-cache") == "L3" and plain.text() == base.text(), f"非流式改写 → x-maas-cache={plain.h('x-maas-cache')}，内容一致={plain.text() == base.text()}")

        print("[5] 要素校验：相似度达标、但数字或否定不同的问题不能命中")
        near = await chat(s, gw, "信用卡逾期一年会影响征信吗？", system=system)
        near_score = float(near.h("x-maas-semantic-score") or 0)
        c.check(near.status == 200 and near.h("x-maas-cache") == "miss" and near.h("x-maas-semantic-reject") == "number" and near_score >= args.threshold,
                f"「逾期一年」→ x-maas-cache={near.h('x-maas-cache')} reject={near.h('x-maas-semantic-reject')} 相似度={near_score:.4f}")
        await chat(s, gw, "什么情况下需要交违约金？", system=system)
        await asyncio.sleep(0.5)
        neg = await chat(s, gw, "什么情况下不需要交违约金？", system=system)
        c.check(neg.h("x-maas-cache") == "miss" and neg.h("x-maas-semantic-reject") == "negation",
                f"「不需要交违约金」→ x-maas-cache={neg.h('x-maas-cache')} reject={neg.h('x-maas-semantic-reject')} 相似度={neg.h('x-maas-semantic-score')}")

        print("[6] 精确性：不同的问题、不同的上下文都不能命中")
        other = await chat(s, gw, "信用卡年费怎么免除？", system=system)
        c.check(other.h("x-maas-cache") == "miss" and not other.h("x-maas-semantic-reject"), f"不同的问题 → x-maas-cache={other.h('x-maas-cache')}")
        ctx = await chat(s, gw, "信用卡逾期1天对征信有影响吗？", system=system + "（理财顾问）")
        c.check(ctx.h("x-maas-cache") == "miss", f"同一问题、不同 system prompt → x-maas-cache={ctx.h('x-maas-cache')}")

        if args.docker_checks:
            await failure_checks(s, args, c, run)

        print("[9] 监控：Prometheus 抓取目标、网关自定义指标与 Grafana 面板")
        async with s.get(f"{args.prometheus}/api/v1/targets") as resp:
            targets = (await resp.json())["data"]["activeTargets"]
        down = [t["scrapeUrl"] for t in targets if t["health"] != "up"]
        c.check(len(targets) > 0 and not down, f"{len(targets)} 个抓取目标，异常 {down or '无'}")
        await asyncio.sleep(6)  # 等下一轮抓取
        wanted = ["maas_cache_hits_l3_total", "maas_cache_l3_rejects_total", "maas_router_pool_small_total", "maas_scheduler_ttft_ms_count"]
        values = {}
        for name in wanted:
            # 取 10 分钟内的最大值：--docker-checks 重启过网关，计数器会从 0 重新开始
            async with s.get(f"{args.prometheus}/api/v1/query", params={"query": f"max(max_over_time({name}[10m]))"}) as resp:
                result = (await resp.json())["data"]["result"]
            values[name] = float(result[0]["value"][1]) if result else 0.0
        c.check(all(v > 0 for v in values.values()), "Prometheus 中的指标：" + "，".join(f"{k}={v:.0f}" for k, v in values.items()))
        async with s.get(f"{args.grafana}/api/health") as resp:
            health = await resp.json()
        async with s.get(f"{args.grafana}/api/search", params={"query": "MaaS"}) as resp:
            found = [d for d in await resp.json() if d.get("type") == "dash-db"]
        panels = 0
        if found:
            async with s.get(f"{args.grafana}/api/dashboards/uid/{found[0]['uid']}") as resp:
                panels = len((await resp.json())["dashboard"]["panels"])
        c.check(health.get("database") == "ok" and panels > 0, f"Grafana {health.get('version')}：面板「{found[0]['title'] if found else '-'}」共 {panels} 个 panel")

    print("\n结果：" + ("全部通过" if not c.failures else f"{len(c.failures)} 项失败"))
    return 1 if c.failures else 0


async def failure_checks(s: aiohttp.ClientSession, args: argparse.Namespace, c: Checker, run: str) -> None:
    gw, admin = args.gateway.rstrip("/"), args.admin.rstrip("/")
    print("[7] 故障放行：停掉 embedder，请求照常处理（L3 跳过，语义分发回落到默认模型池）")
    before = await gateway_metrics(s, admin)
    compose("stop", "embedder")
    try:
        r = await chat(s, gw, f"embedder 故障期间的问题 {run}", model="higress/auto")
        after = await gateway_metrics(s, admin)
        errors = after.get("maas_cache_l3_errors_total", 0) - before.get("maas_cache_l3_errors_total", 0)
        c.check(r.status == 200 and r.h("x-maas-cache") == "miss" and r.h("x-maas-route-reason") == "embed_error" and r.h("x-maas-route-pool") == "large",
                f"status={r.status} x-maas-cache={r.h('x-maas-cache')} route={r.h('x-maas-route-pool')}/{r.h('x-maas-route-reason')} "
                f"首字节 {r.first_byte * 1000:.0f}ms，L3 错误计数 +{errors:.0f}")
    finally:
        compose("start", "embedder")

    async def semantic_back() -> bool:
        r = await chat(s, gw, f"你好，在吗 {uuid.uuid4().hex[:6]}", model="higress/auto", headers=BYPASS)
        return r.h("x-maas-route-reason") == "semantic"

    c.check(await wait_until(semantic_back, timeout=60, interval=1), "embedder 恢复后语义分发自动恢复")

    print("[8] 故障放行：停掉 Qdrant，请求照常处理（L3 跳过，路由仍复用向量）")
    compose("stop", "qdrant")
    try:
        r = await chat(s, gw, f"Qdrant 故障期间的问题 {run}", model="higress/auto")
        c.check(r.status == 200 and r.h("x-maas-cache") == "miss" and r.h("x-maas-route-reason") == "semantic",
                f"status={r.status} x-maas-cache={r.h('x-maas-cache')} route={r.h('x-maas-route-pool')}/{r.h('x-maas-route-reason')} 首字节 {r.first_byte * 1000:.0f}ms")
    finally:
        compose("start", "qdrant")

    async def l3_back() -> bool:
        system = f"Qdrant 恢复检查 {run} {uuid.uuid4().hex[:6]}"  # 每次尝试用新的上下文
        await chat(s, gw, "银行卡丢了怎么挂失？", system=system)
        await asyncio.sleep(0.5)
        r = await chat(s, gw, "银行卡丢失了应该如何挂失？", system=system)
        return r.h("x-maas-cache") == "L3"

    c.check(await wait_until(l3_back, timeout=60, interval=1), "Qdrant 恢复后 L3 自动恢复（集合数据保留）")

    print("[8c] 失效候选：缓存条目已过期的向量点被删除，语义命中回退到下一个候选")
    system = f"失效候选检查 {run}"
    code = "".join(chr(ord("A") + b % 26) for b in uuid.uuid4().bytes[:6])  # 不含数字，不影响要素校验
    first, second, third = f"银行卡丢了怎么挂失？（客户 {code}）", f"银行卡丢失了应该如何挂失？（客户 {code}）", f"请问银行卡丢了怎么挂失？（客户 {code}）"
    await chat(s, gw, first, system=system)
    await asyncio.sleep(0.5)
    points = await scroll_points(s, args, first)
    c.check(len(points) == 1, f"原问题登记到向量库：{len(points)} 个点")
    if points:
        compose("exec", "-T", "redis", "redis-cli", "DEL", "maas:cache:v1:" + points[0]["payload"]["key"])
        compose("restart", "gateway")  # 同时清空各 worker 的 L1，让这个点彻底失效

        async def ready() -> bool:
            async with s.get(f"{gw}/maas/debug/hosts") as resp:
                return resp.status == 200

        await wait_until(ready, timeout=30)
        await asyncio.sleep(1.5)  # 等定时任务确认集合存在
        before = await gateway_metrics(s, admin)
        r2 = await chat(s, gw, second, system=system)
        after = await gateway_metrics(s, admin)
        stale = after.get("maas_cache_l3_stale_total", 0) - before.get("maas_cache_l3_stale_total", 0)
        await asyncio.sleep(0.5)
        left = await scroll_points(s, args, first)
        c.check(r2.h("x-maas-cache") == "miss" and stale == 1 and not left, f"改写问题 → x-maas-cache={r2.h('x-maas-cache')}，失效候选 +{stale:.0f}，原问题的点剩 {len(left)} 个")
        r3 = await chat(s, gw, third, system=system)
        c.check(r3.h("x-maas-cache") == "L3" and r3.text() == r2.text(), f"再换个说法 → x-maas-cache={r3.h('x-maas-cache')}，回放的是上一个改写问题的答案={r3.text() == r2.text()}")


async def scroll_points(s: aiohttp.ClientSession, args: argparse.Namespace, question: str) -> list[dict]:
    """按问题原文在向量库中查找登记的点。"""
    body = {"filter": {"must": [{"key": "q", "match": {"value": question}}]}, "limit": 10, "with_payload": True}
    async with s.post(f"{args.qdrant.rstrip('/')}/collections/{args.collection}/points/scroll", json=body) as resp:
        return (await resp.json())["result"]["points"]


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway", default="http://localhost:10000")
    parser.add_argument("--admin", default="http://localhost:9901", help="Envoy admin 地址")
    parser.add_argument("--prometheus", default="http://localhost:9090")
    parser.add_argument("--grafana", default="http://localhost:3000")
    parser.add_argument("--qdrant", default="http://localhost:6333")
    parser.add_argument("--collection", default="maas_semantic_cache")
    parser.add_argument("--threshold", type=float, default=0.88, help="与网关 l3.threshold 一致")
    parser.add_argument("--docker-checks", action="store_true", help="停掉 embedder / Qdrant 验证故障放行，需要 docker compose")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
