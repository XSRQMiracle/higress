#!/usr/bin/env python3
"""mock-vllm：面向网关实验的 vLLM 服务模拟器。

只模拟对"路由 + 缓存网关"有影响的部分：
  * OpenAI 兼容接口 /v1/chat/completions、/v1/completions（流式 / 非流式），
    返回格式、usage（含 prompt_tokens_details.cached_tokens）与 vLLM 保持一致；
  * 连续批处理（continuous batching）+ chunked prefill：每个 step 先调度 running 队列，
    再按 FCFS 接纳 waiting 队列，每步 token 总数不超过 max_num_batched_tokens；
  * 分页 KV cache block 池 + 自动前缀缓存（APC）：完整的 prompt block 按链式哈希登记，
    新请求命中最长前缀即可跳过这部分 prefill；显存（block）不足时按 vLLM V1 的方式
    抢占最后进入 running 的请求并重算（recompute preemption）；
  * 与 vLLM 同名的 Prometheus 指标（/metrics），供网关健康检查 store_metrics 拉取；
  * 运行时故障注入（POST /admin/fault）：慢节点、错误率、请求挂起、整机下线。

单步耗时模型（毫秒），再乘以 slow_factor（故障注入）与 time_scale（整体加速/减速）：
    step_ms = step_base_ms + step_per_seq_ms * 本步序列数 + prefill_per_token_ms * 本步 prefill token 数
默认参数大致对应单卡 A100 上的 14B 模型：空载 decode 约 20ms/token，prefill 约 8k token/s。
真机标定后用命令行参数覆盖即可。
"""
from __future__ import annotations

import argparse
import asyncio
import bisect
import enum
import hashlib
import json
import logging
import math
import os
import random
import re
import socket
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field, fields, replace

from aiohttp import web

log = logging.getLogger("mock-vllm")


# --------------------------------------------------------------------------------------
# 配置
# --------------------------------------------------------------------------------------


@dataclass
class EngineConfig:
    replica_id: str
    served_model_name: str = "mock-qwen"
    max_model_len: int = 32768
    max_num_seqs: int = 128
    max_num_batched_tokens: int = 2048
    block_size: int = 16
    num_gpu_blocks: int = 8192
    enable_prefix_caching: bool = True
    step_base_ms: float = 20.0
    step_per_seq_ms: float = 0.3
    prefill_per_token_ms: float = 0.12
    output_len_mean: int = 200
    output_len_sigma: float = 0.6
    time_scale: float = 1.0
    # True：相同输入（prompt + 采样参数）总是生成相同输出，便于校验缓存回放的正确性。
    # False：temperature > 0 的请求每次输出都不同，更接近真实采样。
    deterministic: bool = True


@dataclass
class FaultConfig:
    slow_factor: float = 1.0  # 整机变慢倍数，>1 即"慢节点"
    error_rate: float = 0.0  # 请求直接返回 500 的概率
    hang_rate: float = 0.0  # 请求挂起的概率（流式请求会先发出响应头再挂起）
    hang_seconds: float = 60.0  # 挂起时长
    extra_latency_ms: float = 0.0  # 每个请求进入引擎前的固定额外延迟
    kv_usage_offset: float = 0.0  # 只叠加到上报的 KV 占用率上，模拟显存被其他负载占用（验证网关的水位准入）
    down: bool = False  # 整机下线：除 /admin/* 外所有接口返回 503，健康检查随之失败

    def update(self, patch: dict) -> None:
        """用 JSON 局部更新故障配置。先在副本上校验，任何字段非法都抛 ValueError 且不改动当前配置。"""
        types = {f.name: f.type for f in fields(self)}
        candidate = replace(self)
        for key, value in patch.items():
            if key not in types:
                raise ValueError(f"unknown fault field: {key}")
            if types[key] in ("bool", bool):
                if not isinstance(value, bool):
                    raise ValueError(f"{key} must be a bool")
            else:
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError(f"{key} must be a number")
                if value < 0:
                    raise ValueError(f"{key} must be >= 0")
                value = float(value)
            setattr(candidate, key, value)
        if not 0 <= candidate.error_rate <= 1 or not 0 <= candidate.hang_rate <= 1 or not 0 <= candidate.kv_usage_offset <= 1:
            raise ValueError("error_rate, hang_rate and kv_usage_offset must be within [0, 1]")
        if candidate.slow_factor <= 0:
            raise ValueError("slow_factor must be > 0")
        for name in types:
            setattr(self, name, getattr(candidate, name))


# --------------------------------------------------------------------------------------
# 近似分词与 chat 模板
# --------------------------------------------------------------------------------------

# 规则：特殊 token 整体算 1 个；CJK 字符 1 字 1 token；字母数字每 4 个字符 1 token；
# 连续空白 1 token；其余标点 1 字符 1 token。对中文接近真实 tokenizer 的量级，且完全确定。
_TOKEN_RE = re.compile(
    r"<\|[a-z_]+\|>"
    r"|[　-〿㐀-䶿一-鿿豈-﫿＀-￯]"
    r"|[A-Za-z0-9]{1,4}"
    r"|\s+"
    r"|[^\sA-Za-z0-9]"
)


def tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text)


def _message_text(content) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):  # 多模态 content parts：只保留文本
        return "".join(p.get("text", "") for p in content if isinstance(p, dict) and p.get("type") == "text")
    return json.dumps(content, ensure_ascii=False, sort_keys=True)


def render_chat(messages: list, tools) -> str:
    """按 Qwen 的 ChatML 模板渲染，保证"多轮对话的前几轮"是"后续请求"的文本前缀。"""
    parts = []
    if tools:
        parts.append("<|im_start|>system\n# Tools\n" + json.dumps(tools, ensure_ascii=False, sort_keys=True) + "<|im_end|>\n")
    for message in messages:
        role = message.get("role", "user")
        text = _message_text(message.get("content"))
        if message.get("tool_calls"):
            text += json.dumps(message["tool_calls"], ensure_ascii=False, sort_keys=True)
        parts.append(f"<|im_start|>{role}\n{text}<|im_end|>\n")
    parts.append("<|im_start|>assistant\n")
    return "".join(parts)


def chain_block_hashes(tokens: list[str], block_size: int) -> list[bytes]:
    """对每个完整 block 计算链式哈希：h_i = H(h_{i-1} || tokens_i)。

    链式哈希保证"第 i 个 block 命中"蕴含"前 i-1 个 block 都相同"，与 vLLM APC 的语义一致。
    """
    hashes = []
    parent = b"mock-vllm"
    for start in range(0, len(tokens) - block_size + 1, block_size):
        h = hashlib.blake2b(parent, digest_size=8)
        h.update("\x1f".join(tokens[start : start + block_size]).encode("utf-8"))
        parent = h.digest()
        hashes.append(parent)
    return hashes


# 生成内容用的字表：常用汉字 + 少量标点。只要求确定、可分词、长度可控。
_VOCAB = list(
    "的一是在不了有和人这中大为上个国我以要他时来用们生到作地于出就分对成会可主发年动同工也能下过子说产种面而方后多定行学法所"
    "民得经十三之进着等部度家电力里如水化高自二理起小物现实加量都两体制机当使点从业本去把性好应开它合还因由其些然前外天政四日那"
    "社义事平形相全表间样与关各重新线内数正心反你明看原又么利比或但质气第向道命此变条只没结解问意建月公无系军很情者最立代想已通"
) + list("，，，。。、")


# --------------------------------------------------------------------------------------
# KV block 池（含前缀缓存）
# --------------------------------------------------------------------------------------


class Block:
    __slots__ = ("bid", "ref", "hash")

    def __init__(self, bid: int):
        self.bid = bid
        self.ref = 0
        self.hash: bytes | None = None


class BlockPool:
    """简化版 vLLM V1 BlockPool。

    * free 队列按 LRU 排序，ref==0 的 block 都在里面（包括仍保留前缀内容、可被复用的 block）；
    * 分配新 block 时从队头取，若它带有哈希则视为"驱逐"一个前缀缓存条目；
    * 显存占用率 = 1 - 空闲 block 数 / 总 block 数（与 vllm:kv_cache_usage_perc 的定义一致）。
    """

    def __init__(self, num_blocks: int):
        self.num_blocks = num_blocks
        self.blocks = [Block(i) for i in range(num_blocks)]
        self.free: OrderedDict[int, Block] = OrderedDict((b.bid, b) for b in self.blocks)
        self.cached: dict[bytes, Block] = {}
        self.evictions = 0

    @property
    def num_free(self) -> int:
        return len(self.free)

    def usage(self) -> float:
        return 1.0 - len(self.free) / self.num_blocks

    def lookup(self, hashes: list[bytes]) -> list[Block]:
        """返回最长前缀命中的 block 列表（不修改引用计数）。"""
        hit = []
        for h in hashes:
            block = self.cached.get(h)
            if block is None:
                break
            hit.append(block)
        return hit

    def touch(self, blocks: list[Block]) -> None:
        for block in blocks:
            if block.ref == 0:
                self.free.pop(block.bid, None)
            block.ref += 1

    def allocate(self, n: int) -> list[Block]:
        if n > len(self.free):
            raise RuntimeError("allocate called without enough free blocks")
        out = []
        for _ in range(n):
            _, block = self.free.popitem(last=False)
            if block.hash is not None:
                if self.cached.get(block.hash) is block:
                    del self.cached[block.hash]
                block.hash = None
                self.evictions += 1
            block.ref = 1
            out.append(block)
        return out

    def release(self, blocks: list[Block]) -> None:
        # 逆序释放：序列尾部的 block 先进入 free 队列、也就先被驱逐，前缀部分保留得更久。
        for block in reversed(blocks):
            block.ref -= 1
            if block.ref == 0:
                self.free[block.bid] = block

    def register(self, block: Block, h: bytes) -> None:
        if block.hash is None and h not in self.cached:
            block.hash = h
            self.cached[h] = block

    def reset_prefix_cache(self) -> int:
        dropped = 0
        for block in self.free.values():
            if block.hash is not None:
                if self.cached.get(block.hash) is block:
                    del self.cached[block.hash]
                block.hash = None
                dropped += 1
        return dropped


# --------------------------------------------------------------------------------------
# 引擎：调度 + 执行
# --------------------------------------------------------------------------------------


class State(enum.Enum):
    WAITING = "waiting"
    RUNNING = "running"
    FINISHED = "finished"
    ABORTED = "aborted"


_FINISH = object()


@dataclass(eq=False)
class Seq:
    rid: str
    prompt_len: int
    prompt_hashes: list[bytes]
    max_tokens: int
    plan_len: int  # 计划生成的 token 数（模拟 EOS 出现的位置）
    rng: random.Random
    arrival: float
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    state: State = State.WAITING
    num_out: int = 0
    num_computed: int = 0  # 已算好 KV 的 token 数
    num_hashed: int = 0  # 已登记到前缀缓存的 prompt block 数
    blocks: list[Block] = field(default_factory=list)
    cached_tokens: int = 0  # 首次被调度时命中的前缀 token 数（写入 usage）
    admitted_at: float | None = None
    first_token_at: float | None = None
    last_token_at: float | None = None
    preemptions: int = 0
    finish_reason: str | None = None

    @property
    def num_tokens(self) -> int:
        return self.prompt_len + self.num_out

    def next_token(self) -> str:
        return self.rng.choice(_VOCAB)


class Histogram:
    def __init__(self, name: str, help_: str, buckets: list[float]):
        self.name = name
        self.help = help_
        self.buckets = buckets
        self.counts = [0] * len(buckets)
        self.sum = 0.0
        self.count = 0

    def observe(self, value: float) -> None:
        self.sum += value
        self.count += 1
        index = bisect.bisect_left(self.buckets, value)
        if index < len(self.buckets):
            self.counts[index] += 1

    def render(self, labels: str) -> list[str]:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} histogram"]
        cumulative = 0
        for bound, count in zip(self.buckets, self.counts):
            cumulative += count
            lines.append(f'{self.name}_bucket{{{labels},le="{bound}"}} {cumulative}')
        lines.append(f'{self.name}_bucket{{{labels},le="+Inf"}} {self.count}')
        lines.append(f"{self.name}_sum{{{labels}}} {self.sum}")
        lines.append(f"{self.name}_count{{{labels}}} {self.count}")
        return lines


_LATENCY_BUCKETS = [0.005, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 20.0, 40.0, 80.0]
_ITL_BUCKETS = [0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.5]


class Engine:
    def __init__(self, cfg: EngineConfig, fault: FaultConfig):
        self.cfg = cfg
        self.fault = fault
        self.pool = BlockPool(cfg.num_gpu_blocks)
        self.waiting: deque[Seq] = deque()
        self.running: list[Seq] = []
        self.wakeup = asyncio.Event()
        self.steps = 0
        self.prefix_queries = 0
        self.prefix_hits = 0
        self.num_preemptions = 0
        self.prompt_tokens = 0
        self.generation_tokens = 0
        self.success = {"stop": 0, "length": 0, "abort": 0}
        self.h_ttft = Histogram("vllm:time_to_first_token_seconds", "Histogram of time to first token in seconds.", _LATENCY_BUCKETS)
        self.h_itl = Histogram("vllm:inter_token_latency_seconds", "Histogram of inter-token latency in seconds.", _ITL_BUCKETS)
        self.h_e2e = Histogram("vllm:e2e_request_latency_seconds", "Histogram of e2e request latency in seconds.", _LATENCY_BUCKETS)
        self.h_queue = Histogram("vllm:request_queue_time_seconds", "Histogram of time spent in WAITING phase for request.", _LATENCY_BUCKETS)

    # ---------------- 对外接口 ----------------

    def blocks_for(self, num_tokens: int) -> int:
        return -(-num_tokens // self.cfg.block_size)

    def plan_output_len(self, rng: random.Random, max_tokens: int, ignore_eos: bool, min_tokens: int) -> int:
        """按对数正态分布抽样输出长度（模拟 EOS 位置），ignore_eos 时固定为 max_tokens。"""
        if ignore_eos:
            return max_tokens
        sigma = self.cfg.output_len_sigma
        mu = math.log(max(1, self.cfg.output_len_mean)) - sigma * sigma / 2
        planned = int(round(rng.lognormvariate(mu, sigma)))
        return min(max(planned, min_tokens, 1), max_tokens)

    def submit(self, seq: Seq) -> None:
        self.waiting.append(seq)
        self.wakeup.set()

    def abort(self, seq: Seq) -> None:
        """客户端断开：从队列移除并释放 KV。"""
        if seq.state is State.WAITING:
            try:
                self.waiting.remove(seq)
            except ValueError:
                pass
        elif seq.state is State.RUNNING:
            self.running.remove(seq)
            self.pool.release(seq.blocks)
            seq.blocks = []
        else:
            return
        seq.state = State.ABORTED
        self.success["abort"] += 1

    # ---------------- 调度 ----------------

    def _preempt(self, victim: Seq) -> None:
        self.running.remove(victim)
        self.pool.release(victim.blocks)
        victim.blocks = []
        victim.num_computed = 0
        victim.num_hashed = 0
        victim.state = State.WAITING
        victim.preemptions += 1
        self.waiting.appendleft(victim)
        self.num_preemptions += 1

    def _schedule(self) -> list[tuple[Seq, int]]:
        cfg = self.cfg
        budget = cfg.max_num_batched_tokens
        batch: list[tuple[Seq, int]] = []
        preempted = False

        # 1) running 队列：decode 每条 1 个 token，未完成的 prefill 按剩余预算分块。
        index = 0
        while index < len(self.running) and budget > 0:
            seq = self.running[index]
            n = min(seq.num_tokens - seq.num_computed, budget)
            need = self.blocks_for(seq.num_computed + n) - len(seq.blocks)
            while need > self.pool.num_free:
                victim = self.running[-1]  # 抢占最后进入 running 的请求（优先级最低）
                self._preempt(victim)
                preempted = True
                if victim is seq:
                    break
            if seq.state is not State.RUNNING:
                break
            if need > 0:
                seq.blocks.extend(self.pool.allocate(need))
            batch.append((seq, n))
            budget -= n
            index += 1

        # 2) waiting 队列：本步发生过抢占则不再接纳新请求（与 vLLM V1 一致）。
        while not preempted and self.waiting and budget > 0 and len(self.running) < cfg.max_num_seqs:
            seq = self.waiting[0]
            hit = self.pool.lookup(seq.prompt_hashes) if cfg.enable_prefix_caching else []
            if len(hit) * cfg.block_size > seq.num_tokens - 1:
                hit = hit[:-1]  # 整个 prompt 都命中时，至少重算最后一个 block 以产出第一个 token
            computed = len(hit) * cfg.block_size
            n = min(seq.num_tokens - computed, budget)
            need = self.blocks_for(computed + n) - len(hit)
            reusable_free = sum(1 for block in hit if block.ref == 0)
            if self.pool.num_free - reusable_free < need:
                break
            self.waiting.popleft()
            self.pool.touch(hit)
            seq.blocks = hit + self.pool.allocate(need)
            seq.num_computed = computed
            seq.num_hashed = len(hit)
            seq.state = State.RUNNING
            now = time.monotonic()
            if seq.admitted_at is None:
                seq.admitted_at = now
                seq.cached_tokens = computed
                self.prompt_tokens += seq.prompt_len
                self.h_queue.observe(now - seq.arrival)
            self.prefix_queries += seq.prompt_len
            self.prefix_hits += computed
            self.running.append(seq)
            batch.append((seq, n))
            budget -= n
        return batch

    def _advance(self, batch: list[tuple[Seq, int]]) -> None:
        now = time.monotonic()
        for seq, n in batch:
            if seq.state is not State.RUNNING:  # 本步执行期间被 abort
                continue
            seq.num_computed += n
            if self.cfg.enable_prefix_caching:
                full = min(seq.num_computed, seq.prompt_len) // self.cfg.block_size
                for i in range(seq.num_hashed, full):
                    self.pool.register(seq.blocks[i], seq.prompt_hashes[i])
                seq.num_hashed = max(seq.num_hashed, full)
            if seq.num_computed < seq.num_tokens:
                continue  # prefill 还没做完
            token = seq.next_token()
            seq.num_out += 1
            self.generation_tokens += 1
            if seq.first_token_at is None:
                seq.first_token_at = now
                self.h_ttft.observe(now - seq.arrival)
            else:
                self.h_itl.observe(now - seq.last_token_at)
            seq.last_token_at = now
            seq.queue.put_nowait(token)
            if seq.num_out >= seq.plan_len:
                self._finish(seq, now)

    def _finish(self, seq: Seq, now: float) -> None:
        seq.state = State.FINISHED
        self.running.remove(seq)
        self.pool.release(seq.blocks)
        seq.blocks = []
        seq.finish_reason = "length" if seq.num_out >= seq.max_tokens else "stop"
        self.success[seq.finish_reason] += 1
        self.h_e2e.observe(now - seq.arrival)
        seq.queue.put_nowait(_FINISH)

    async def run(self) -> None:
        cfg = self.cfg
        while True:
            try:
                if not self.running and not self.waiting:
                    self.wakeup.clear()
                    await self.wakeup.wait()
                    continue
                batch = self._schedule()
                if not batch:
                    await asyncio.sleep(0.002)
                    continue
                prefill_tokens = sum(n for seq, n in batch if n > 1 or seq.num_out == 0)
                step_ms = cfg.step_base_ms + cfg.step_per_seq_ms * len(batch) + cfg.prefill_per_token_ms * prefill_tokens
                await asyncio.sleep(step_ms * self.fault.slow_factor * cfg.time_scale / 1000.0)
                self._advance(batch)
                self.steps += 1
            except asyncio.CancelledError:
                raise
            except Exception:  # 调度器出错不应让整个副本停摆
                log.exception("engine step failed")
                await asyncio.sleep(0.01)

    # ---------------- 观测 ----------------

    def render_metrics(self) -> str:
        cfg = self.cfg
        labels = f'engine="0",model_name="{cfg.served_model_name}"'
        usage = min(1.0, self.pool.usage() + self.fault.kv_usage_offset)
        lines = [
            "# HELP vllm:num_requests_running Number of requests in model execution batches.",
            "# TYPE vllm:num_requests_running gauge",
            f"vllm:num_requests_running{{{labels}}} {float(len(self.running))}",
            "# HELP vllm:num_requests_waiting Number of requests waiting to be processed.",
            "# TYPE vllm:num_requests_waiting gauge",
            f"vllm:num_requests_waiting{{{labels}}} {float(len(self.waiting))}",
            "# HELP vllm:kv_cache_usage_perc KV-cache usage. 1 means 100 percent usage.",
            "# TYPE vllm:kv_cache_usage_perc gauge",
            f"vllm:kv_cache_usage_perc{{{labels}}} {usage}",
            "# HELP vllm:gpu_cache_usage_perc GPU KV-cache usage (legacy name). 1 means 100 percent usage.",
            "# TYPE vllm:gpu_cache_usage_perc gauge",
            f"vllm:gpu_cache_usage_perc{{{labels}}} {usage}",
            "# HELP vllm:prefix_cache_queries Prefix cache queries, in terms of number of queried tokens.",
            "# TYPE vllm:prefix_cache_queries counter",
            f"vllm:prefix_cache_queries_total{{{labels}}} {float(self.prefix_queries)}",
            "# HELP vllm:prefix_cache_hits Prefix cache hits, in terms of number of cached tokens.",
            "# TYPE vllm:prefix_cache_hits counter",
            f"vllm:prefix_cache_hits_total{{{labels}}} {float(self.prefix_hits)}",
            "# HELP vllm:num_preemptions Cumulative number of preemption from the engine.",
            "# TYPE vllm:num_preemptions counter",
            f"vllm:num_preemptions_total{{{labels}}} {float(self.num_preemptions)}",
            "# HELP vllm:prompt_tokens Number of prefill tokens processed.",
            "# TYPE vllm:prompt_tokens counter",
            f"vllm:prompt_tokens_total{{{labels}}} {float(self.prompt_tokens)}",
            "# HELP vllm:generation_tokens Number of generation tokens processed.",
            "# TYPE vllm:generation_tokens counter",
            f"vllm:generation_tokens_total{{{labels}}} {float(self.generation_tokens)}",
            "# HELP vllm:request_success Count of successfully processed requests.",
            "# TYPE vllm:request_success counter",
        ]
        for reason in ("stop", "length", "abort"):
            lines.append(f'vllm:request_success_total{{{labels},finished_reason="{reason}"}} {float(self.success[reason])}')
        for histogram in (self.h_ttft, self.h_itl, self.h_e2e, self.h_queue):
            lines.extend(histogram.render(labels))
        lines += [
            "# HELP vllm:cache_config_info Information of the LLMEngine CacheConfig",
            "# TYPE vllm:cache_config_info gauge",
            f'vllm:cache_config_info{{block_size="{cfg.block_size}",enable_prefix_caching="{cfg.enable_prefix_caching}",'
            f'engine="0",num_gpu_blocks="{cfg.num_gpu_blocks}"}} 1.0',
            "# HELP mock_replica_info Identity of this mock-vllm replica (lets the gateway map address to replica).",
            "# TYPE mock_replica_info gauge",
            f'mock_replica_info{{replica="{cfg.replica_id}",slow_factor="{self.fault.slow_factor}"}} 1.0',
        ]
        return "\n".join(lines) + "\n"

    def state_snapshot(self) -> dict:
        return {
            "replica": self.cfg.replica_id,
            "running": len(self.running),
            "waiting": len(self.waiting),
            "kv_cache_usage_perc": round(self.pool.usage(), 4),
            "free_blocks": self.pool.num_free,
            "cached_blocks": len(self.pool.cached),
            "prefix_cache_hit_rate": round(self.prefix_hits / self.prefix_queries, 4) if self.prefix_queries else None,
            "preemptions": self.num_preemptions,
            "evictions": self.pool.evictions,
            "steps": self.steps,
            "requests": dict(self.success),
            "config": asdict(self.cfg),
            "fault": asdict(self.fault),
        }


# --------------------------------------------------------------------------------------
# HTTP 层
# --------------------------------------------------------------------------------------


def _error(status: int, message: str, err_type: str = "BadRequestError") -> web.Response:
    return web.json_response({"object": "error", "message": message, "type": err_type, "param": None, "code": status}, status=status)


def _sse(payload: dict) -> bytes:
    return b"data: " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n\n"


async def handle_chat(request: web.Request) -> web.StreamResponse:
    try:
        body = await request.json()
    except Exception:
        return _error(400, "request body is not valid JSON")
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        return _error(400, "messages must be a non-empty list")
    prompt_text = render_chat(messages, body.get("tools"))
    return await _serve(request, body, "chat", prompt_text, tokenize(prompt_text))


async def handle_completions(request: web.Request) -> web.StreamResponse:
    try:
        body = await request.json()
    except Exception:
        return _error(400, "request body is not valid JSON")
    prompt = body.get("prompt")
    if isinstance(prompt, list) and len(prompt) == 1 and isinstance(prompt[0], str):
        prompt = prompt[0]
    if isinstance(prompt, str):
        return await _serve(request, body, "completion", prompt, tokenize(prompt))
    if isinstance(prompt, list) and prompt and all(isinstance(t, int) and not isinstance(t, bool) for t in prompt):
        tokens = [str(t) for t in prompt]
        return await _serve(request, body, "completion", "\x1f".join(tokens), tokens)
    return _error(400, "prompt must be a string or a list of token ids")


def _positive_int(value, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


async def _serve(request: web.Request, body: dict, kind: str, prompt_text: str, tokens: list[str]) -> web.StreamResponse:
    engine: Engine = request.app["engine"]
    cfg, fault, rnd = engine.cfg, engine.fault, request.app["rnd"]
    arrival = time.monotonic()
    replica_header = {"x-mock-replica": cfg.replica_id}

    if fault.error_rate > 0 and rnd.random() < fault.error_rate:
        resp = _error(500, "mock injected error", "InternalServerError")
        resp.headers.update(replica_header)
        return resp

    prompt_len = len(tokens)
    room = cfg.max_model_len - prompt_len
    try:
        raw_max = body.get("max_completion_tokens", body.get("max_tokens"))
        max_tokens = room if raw_max is None else _positive_int(raw_max, "max_tokens")
        min_tokens = body.get("min_tokens") or 0
        if min_tokens:
            min_tokens = _positive_int(min_tokens, "min_tokens")
    except ValueError as exc:
        return _error(400, str(exc))
    if room <= 0 or max_tokens > room:
        return _error(
            400,
            f"This model's maximum context length is {cfg.max_model_len} tokens. However, you requested "
            f"{prompt_len + max_tokens} tokens ({prompt_len} in the messages, {max_tokens} in the completion).",
        )
    if engine.blocks_for(prompt_len + max_tokens) > cfg.num_gpu_blocks:
        return _error(400, "request does not fit in the KV cache of this replica")

    # 相同输入 → 相同输出：种子由 prompt 与影响输出的参数决定。
    seed_material = json.dumps(
        [prompt_text, max_tokens, body.get("temperature"), body.get("top_p"), body.get("seed")], ensure_ascii=False
    )
    if not cfg.deterministic and (body.get("temperature") or 0) > 0 and body.get("seed") is None:
        seed_material += uuid.uuid4().hex
    rng = random.Random(hashlib.blake2b(seed_material.encode("utf-8"), digest_size=8).digest())
    plan_len = engine.plan_output_len(rng, max_tokens, bool(body.get("ignore_eos")), min_tokens)
    seq = Seq(
        rid=uuid.uuid4().hex,
        prompt_len=prompt_len,
        prompt_hashes=chain_block_hashes(tokens, cfg.block_size) if cfg.enable_prefix_caching else [],
        max_tokens=max_tokens,
        plan_len=plan_len,
        rng=rng,
        arrival=arrival,
    )

    stream = bool(body.get("stream"))
    include_usage = stream and bool((body.get("stream_options") or {}).get("include_usage"))
    model = body.get("model") or cfg.served_model_name
    created = int(time.time())
    response_id = ("chatcmpl-" if kind == "chat" else "cmpl-") + seq.rid
    hang = fault.hang_rate > 0 and rnd.random() < fault.hang_rate

    resp: web.StreamResponse | None = None
    if stream:
        # 与 vLLM 一致：流式请求立刻返回 200 和响应头，此时第一个 token 还没生成。
        resp = web.StreamResponse(
            status=200,
            headers={"Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-cache", **replica_header},
        )
        await resp.prepare(request)

    try:
        if fault.extra_latency_ms > 0:
            await asyncio.sleep(fault.extra_latency_ms * cfg.time_scale / 1000.0)
        if hang:
            await asyncio.sleep(fault.hang_seconds)
        engine.submit(seq)
        if stream:
            await _stream_tokens(resp, seq, kind, response_id, created, model, include_usage)
            return resp
        text = []
        while True:
            item = await seq.queue.get()
            if item is _FINISH:
                break
            text.append(item)
        usage = _usage(seq)
        if kind == "chat":
            payload = {
                "id": response_id,
                "object": "chat.completion",
                "created": created,
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "".join(text), "tool_calls": []},
                        "logprobs": None,
                        "finish_reason": seq.finish_reason,
                        "stop_reason": None,
                    }
                ],
                "usage": usage,
            }
        else:
            payload = {
                "id": response_id,
                "object": "text_completion",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "text": "".join(text), "logprobs": None, "finish_reason": seq.finish_reason, "stop_reason": None}],
                "usage": usage,
            }
        return web.json_response(payload, headers=replica_header, dumps=lambda o: json.dumps(o, ensure_ascii=False))
    finally:
        if seq.state in (State.WAITING, State.RUNNING):
            engine.abort(seq)


def _usage(seq: Seq) -> dict:
    return {
        "prompt_tokens": seq.prompt_len,
        "total_tokens": seq.prompt_len + seq.num_out,
        "completion_tokens": seq.num_out,
        "prompt_tokens_details": {"cached_tokens": seq.cached_tokens},
    }


async def _stream_tokens(resp: web.StreamResponse, seq: Seq, kind: str, response_id: str, created: int, model: str, include_usage: bool) -> None:
    obj = "chat.completion.chunk" if kind == "chat" else "text_completion"

    def chunk(choice: dict) -> bytes:
        return _sse({"id": response_id, "object": obj, "created": created, "model": model, "choices": [choice]})

    first = True
    while True:
        item = await seq.queue.get()
        if item is _FINISH:
            break
        if kind == "chat":
            data = chunk({"index": 0, "delta": {"content": item}, "logprobs": None, "finish_reason": None})
            if first:
                role = chunk({"index": 0, "delta": {"role": "assistant", "content": ""}, "logprobs": None, "finish_reason": None})
                data = role + data
        else:
            data = chunk({"index": 0, "text": item, "logprobs": None, "finish_reason": None, "stop_reason": None})
        first = False
        await resp.write(data)

    if kind == "chat":
        tail = chunk({"index": 0, "delta": {}, "logprobs": None, "finish_reason": seq.finish_reason, "stop_reason": None})
    else:
        tail = chunk({"index": 0, "text": "", "logprobs": None, "finish_reason": seq.finish_reason, "stop_reason": None})
    if include_usage:
        tail += _sse({"id": response_id, "object": obj, "created": created, "model": model, "choices": [], "usage": _usage(seq)})
    await resp.write(tail + b"data: [DONE]\n\n")
    await resp.write_eof()


async def handle_models(request: web.Request) -> web.Response:
    cfg: EngineConfig = request.app["engine"].cfg
    return web.json_response(
        {
            "object": "list",
            "data": [
                {"id": cfg.served_model_name, "object": "model", "created": int(time.time()), "owned_by": "mock-vllm", "max_model_len": cfg.max_model_len}
            ],
        }
    )


async def handle_health(request: web.Request) -> web.Response:
    return web.Response(status=200)


async def handle_metrics(request: web.Request) -> web.Response:
    engine: Engine = request.app["engine"]
    return web.Response(text=engine.render_metrics(), content_type="text/plain", charset="utf-8")


async def handle_state(request: web.Request) -> web.Response:
    return web.json_response(request.app["engine"].state_snapshot())


async def handle_fault(request: web.Request) -> web.Response:
    engine: Engine = request.app["engine"]
    try:
        patch = await request.json()
        if not isinstance(patch, dict):
            raise ValueError("body must be a JSON object")
        engine.fault.update(patch)
    except Exception as exc:
        return _error(400, f"invalid fault config: {exc}")
    log.warning("fault config updated: %s", asdict(engine.fault))
    return web.json_response(asdict(engine.fault))


async def handle_reset_prefix_cache(request: web.Request) -> web.Response:
    dropped = request.app["engine"].pool.reset_prefix_cache()
    return web.json_response({"dropped_blocks": dropped})


@web.middleware
async def fault_middleware(request: web.Request, handler):
    engine: Engine = request.app["engine"]
    if engine.fault.down and not request.path.startswith("/admin/"):
        resp = _error(503, "replica is down (injected fault)", "ServiceUnavailableError")
    else:
        resp = await handler(request)
    if not resp.prepared:
        resp.headers.setdefault("x-mock-replica", engine.cfg.replica_id)
    return resp


def build_app(cfg: EngineConfig, fault: FaultConfig, seed: int | None) -> web.Application:
    app = web.Application(client_max_size=64 * 1024 * 1024, middlewares=[fault_middleware])
    app["engine"] = Engine(cfg, fault)
    app["rnd"] = random.Random(seed)

    async def start_engine(app: web.Application) -> None:
        app["engine_task"] = asyncio.create_task(app["engine"].run())

    async def stop_engine(app: web.Application) -> None:
        app["engine_task"].cancel()

    app.on_startup.append(start_engine)
    app.on_cleanup.append(stop_engine)
    app.router.add_post("/v1/chat/completions", handle_chat)
    app.router.add_post("/v1/completions", handle_completions)
    app.router.add_get("/v1/models", handle_models)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/metrics", handle_metrics)
    app.router.add_get("/admin/state", handle_state)
    app.router.add_post("/admin/fault", handle_fault)
    app.router.add_post("/admin/reset_prefix_cache", handle_reset_prefix_cache)
    app.router.add_post("/reset_prefix_cache", handle_reset_prefix_cache)
    return app


def _str2bool(value: str) -> bool:
    if value.lower() in ("1", "true", "yes", "on"):
        return True
    if value.lower() in ("0", "false", "no", "off"):
        return False
    raise argparse.ArgumentTypeError(f"expected a boolean, got {value!r}")


def parse_args(argv=None) -> argparse.Namespace:
    """每个引擎参数都可以用命令行或环境变量设置：--time-scale 0.5 等价于 MOCK_TIME_SCALE=0.5，
    故障参数 --fault-slow-factor 3 等价于 MOCK_FAULT_SLOW_FACTOR=3（命令行优先）。"""
    parser = argparse.ArgumentParser(description="vLLM-compatible serving simulator for gateway experiments")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("MOCK_PORT", 8000)))
    parser.add_argument("--seed", type=int, default=None, help="RNG seed for fault injection decisions")
    parser.add_argument("--log-level", default=os.environ.get("MOCK_LOG_LEVEL", "INFO"))
    parser.add_argument("--replica-id", default=os.environ.get("MOCK_REPLICA_ID", socket.gethostname()))

    def add(flag: str, env: str, kind, default) -> None:
        raw = os.environ.get(env)
        parser.add_argument(flag, type=kind, default=kind(raw) if raw is not None else default)

    for f in fields(EngineConfig):
        if f.name == "replica_id":
            continue
        kind = _str2bool if f.type in ("bool", bool) else {"int": int, "float": float, "str": str}.get(f.type, f.type)
        add("--" + f.name.replace("_", "-"), "MOCK_" + f.name.upper(), kind, f.default)
    for f in fields(FaultConfig):
        kind = _str2bool if f.type in ("bool", bool) else float
        add("--fault-" + f.name.replace("_", "-"), "MOCK_FAULT_" + f.name.upper(), kind, f.default)
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = EngineConfig(replica_id=args.replica_id, **{f.name: getattr(args, f.name) for f in fields(EngineConfig) if f.name != "replica_id"})
    fault = FaultConfig()
    fault.update({f.name: getattr(args, "fault_" + f.name) for f in fields(FaultConfig)})
    log.info("starting replica %s: %s", cfg.replica_id, asdict(cfg))
    web.run_app(build_app(cfg, fault, args.seed), host=args.host, port=args.port, access_log=None, print=None)


if __name__ == "__main__":
    main()
