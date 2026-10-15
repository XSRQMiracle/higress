"""可复现的合成负载：模拟决赛"50% 重复请求 + 50% 随机新请求"的流量结构。

一条请求由三部分组成：
  * system prompt：从 num_system_prompts 个模板中按 Zipf 分布选取（模拟少数业务场景占大头），
    同一模板的请求共享前缀，可被推理引擎的前缀缓存（APC）复用；
  * 长文档（可选）：按 long_ratio 概率在问题前附加一篇长文档（模拟 32K 上下文的 RAG 场景），
    文档从 doc_pool_size 篇中选取，同一文档的请求也共享前缀；
  * 用户问题：hot 请求从 hot_pool_size 个热点问题里按 Zipf 分布抽取（完全相同的请求体），
    new 请求每次都生成全新的问题（带唯一编号，保证不重复）。
"""
from __future__ import annotations

import bisect
import itertools
import random
from dataclasses import dataclass, field

# 合成文本字表。内容本身没有语义，只需要长度可控、确定可复现。
_CHARS = (
    "的一是在不了有和人这中大为上个国我以要他时来用们生到作地于出就分对成会可主发年动同工也能下过子说产种面而方后多定行学法所"
    "民得经十三之进着等部度家电力里如水化高自二理起小物现实加量都两体制机当使点从业本去把性好应开它合还因由其些然前外天政四日那"
    "社义事平形相全表间样与关各重新线内数正心反你明看原又么利比或但质气第向道命此变条只没结解问意建月公无系军很情者最立代想已通"
)
_TOPICS = ["信用卡", "房贷", "理财产品", "转账限额", "账户冻结", "基金定投", "保险理赔", "跨境汇款", "积分兑换", "贷款审批", "数字人民币", "手机银行"]
_ASPECTS = ["办理流程", "费用标准", "风险提示", "所需材料", "到账时间", "常见问题", "额度调整", "注意事项"]


def _text(rng: random.Random, length: int) -> str:
    out = []
    for i in range(length):
        out.append(rng.choice(_CHARS))
        if i % 23 == 22:
            out.append("，" if i % 3 else "。")
    return "".join(out)


def _zipf_cdf(n: int, s: float) -> list[float]:
    weights = [1.0 / (rank**s) for rank in range(1, n + 1)]
    total = sum(weights)
    cdf, acc = [], 0.0
    for w in weights:
        acc += w / total
        cdf.append(acc)
    cdf[-1] = 1.0
    return cdf


@dataclass
class WorkloadConfig:
    seed: int = 42
    repeat_ratio: float = 0.5  # hot（重复）请求占比
    hot_pool_size: int = 200
    hot_zipf_s: float = 1.0
    num_system_prompts: int = 4
    system_zipf_s: float = 1.0
    system_tokens: int = 800
    question_tokens_min: int = 20
    question_tokens_max: int = 120
    long_ratio: float = 0.1
    long_tokens: int = 8000
    doc_pool_size: int = 8
    max_tokens: int = 256
    ignore_eos: bool = False
    temperature: float = 0.0
    model: str = "mock-qwen"


@dataclass
class RequestSpec:
    kind: str  # "hot" | "new"
    key: str  # hot 请求为热点编号，new 请求为唯一编号
    system_id: int
    doc_id: int | None
    body: dict = field(repr=False)


class Workload:
    def __init__(self, cfg: WorkloadConfig):
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        build = random.Random(cfg.seed * 7919 + 1)
        self.system_prompts = [
            f"你是银行智能客服助手（场景 {i}）。请严格依据以下业务规范回答用户问题：" + _text(build, cfg.system_tokens)
            for i in range(cfg.num_system_prompts)
        ]
        self.docs = [f"【参考文档 {i}】" + _text(build, cfg.long_tokens) for i in range(cfg.doc_pool_size)]
        self.system_cdf = _zipf_cdf(cfg.num_system_prompts, cfg.system_zipf_s)
        self.hot_cdf = _zipf_cdf(cfg.hot_pool_size, cfg.hot_zipf_s)
        self.hot_pool = [self._make(build, "hot", str(i)) for i in range(cfg.hot_pool_size)]
        self.counter = itertools.count()

    def _pick(self, rng: random.Random, cdf: list[float]) -> int:
        return bisect.bisect_left(cdf, rng.random())

    def _make(self, rng: random.Random, kind: str, key: str) -> RequestSpec:
        cfg = self.cfg
        system_id = self._pick(rng, self.system_cdf)
        doc_id = rng.randrange(cfg.doc_pool_size) if cfg.doc_pool_size and rng.random() < cfg.long_ratio else None
        question = (
            f"请问{rng.choice(_TOPICS)}的{rng.choice(_ASPECTS)}是什么？（问题 {kind}-{key}）"
            + _text(rng, rng.randint(cfg.question_tokens_min, cfg.question_tokens_max))
        )
        user = question if doc_id is None else self.docs[doc_id] + "\n\n" + question
        body = {
            "model": cfg.model,
            "messages": [{"role": "system", "content": self.system_prompts[system_id]}, {"role": "user", "content": user}],
            "max_tokens": cfg.max_tokens,
            "temperature": cfg.temperature,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        if cfg.ignore_eos:
            body["ignore_eos"] = True
        return RequestSpec(kind=kind, key=key, system_id=system_id, doc_id=doc_id, body=body)

    def next(self) -> RequestSpec:
        if self.rng.random() < self.cfg.repeat_ratio:
            return self.hot_pool[self._pick(self.rng, self.hot_cdf)]
        return self._make(self.rng, "new", str(next(self.counter)))
