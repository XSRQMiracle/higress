"""可复现的合成负载：模拟决赛"50% 重复请求 + 50% 随机新请求"的流量结构。

一条请求由三部分组成：
  * system prompt：从 num_system_prompts 个模板中按 Zipf 分布选取（模拟少数业务场景占大头），
    同一模板的请求共享前缀，可被推理引擎的前缀缓存（APC）复用；
  * 长文档（可选）：按 long_ratio 概率在问题前附加一篇长文档（模拟 32K 上下文的 RAG 场景），
    文档从 doc_pool_size 篇中选取，同一文档的请求也共享前缀；
  * 用户问题：hot 请求从 hot_pool_size 个热点问题里按 Zipf 分布抽取（完全相同的请求体），
    new 请求每次都生成全新的问题（带唯一编号，保证不重复）。

question_style="faq" 时（阶段 3），hot 请求换成银行客服的真实问法：每个意图有两种说法（同义改写），
再按 decorate_ratio 随机加上"请问""你好，""谢谢"这类客套话，同一个意图会以很多种字面形式出现——
字面完全相同的由 L1/L2 精确命中，换了说法的只能靠 L3 语义命中。意图的类别（闲聊 / 常见问题 / 分析）
是 maas-router 语义分发的依据，也决定输出长度上限。new 请求不变。
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

# (类别, 同一意图的两种说法)。faq 类的说法对取自 maas-cache 的标注集，向量相似度都 ≥ 0.88 且能通过要素校验。
_FAQ_INTENTS = [
    ("faq", ["信用卡自动还款怎么设置？", "怎么设置信用卡的自动还款？"]),
    ("faq", ["怎么注销手机银行？", "手机银行如何注销？"]),
    ("faq", ["房贷可以提前还款吗？", "房贷能提前还吗？"]),
    ("faq", ["信用卡逾期一天会影响征信吗？", "信用卡逾期1天对征信有影响吗？"]),
    ("faq", ["信用卡分期的手续费怎么算？", "信用卡分期付款的手续费是怎么计算的？"]),
    ("faq", ["存款利息怎么计算？", "存款的利息是怎么算的？"]),
    ("faq", ["提前还房贷需要违约金吗？", "房贷提前还款要交违约金吗？"]),
    ("faq", ["忘记了银行卡密码怎么办？", "银行卡密码忘了该怎么处理？"]),
    ("faq", ["银行卡丢了怎么挂失？", "银行卡丢失了应该如何挂失？"]),
    ("faq", ["怎么修改手机银行的登录密码？", "手机银行登录密码如何更改？"]),
    ("faq", ["信用卡被盗刷了怎么办？", "信用卡遭到盗刷应该怎么处理？"]),
    ("faq", ["定期存款可以提前取出吗？", "定期存款能不能提前支取？"]),
    ("faq", ["信用卡年费怎么免除？", "怎样才能免掉信用卡年费？"]),
    ("faq", ["账户被冻结了怎么办？", "我的银行账户冻结了该如何处理？"]),
    ("faq", ["信用卡积分可以兑换什么？", "信用卡积分能换哪些东西？"]),
    ("faq", ["怎么开具存款证明？", "存款证明如何办理？"]),
    ("faq", ["银行卡有效期过了怎么办？", "银行卡过期了该怎么处理？"]),
    ("faq", ["转账转错了能撤回吗？", "转错账了可以追回吗？"]),
    ("faq", ["如何申请提高信用卡额度？", "信用卡怎么申请提额？"]),
    ("faq", ["怎么查询我的信用卡账单？", "信用卡账单在哪里查看？"]),
    ("chitchat", ["你好", "您好"]),
    ("chitchat", ["谢谢你的帮助", "非常感谢你的帮助"]),
    ("chitchat", ["你是谁？", "请问你是谁？"]),
    ("analysis", ["请分析一下利率下调对房贷月供的影响", "利率下调以后，我的房贷月供会有什么变化？请分析一下"]),
    ("analysis", ["比较一下定期存款和货币基金哪个更适合我", "定期存款和货币基金相比各有什么优缺点？哪个更适合我"]),
    ("analysis", ["帮我分析一下提前还房贷划不划算", "提前还房贷到底划不划算？请帮我分析"]),
    ("analysis", ["解释一下通货膨胀对储蓄有什么影响", "通货膨胀会怎样影响我的存款？请解释一下"]),
]
_PREFIXES = ["请问", "你好，", "您好，请问", "我想咨询一下，", "麻烦问一下，", "不好意思，"]
_SUFFIXES = ["谢谢", "急，在线等", "请尽快回复"]
_CLASS_MAX_TOKENS = {"chitchat": 32, "faq": 128}  # analysis 用 max_tokens


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
    question_style: str = "synthetic"  # synthetic | faq
    decorate_ratio: float = 0.5  # faq 模式下给热点问题加客套话的概率


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
        if cfg.question_style not in ("synthetic", "faq"):
            raise ValueError(f"unknown question_style {cfg.question_style!r}")
        if cfg.question_style == "faq":
            self.intents = list(_FAQ_INTENTS)
            build.shuffle(self.intents)  # 热度排名打乱，各类别的意图都有机会排在前面
            self.hot_cdf = _zipf_cdf(len(self.intents), cfg.hot_zipf_s)
            self.hot_pool = []
        else:
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
        return RequestSpec(kind=kind, key=key, system_id=system_id, doc_id=doc_id, body=self._body(system_id, user, cfg.max_tokens))

    def _body(self, system_id: int, user: str, max_tokens: int) -> dict:
        cfg = self.cfg
        body = {
            "model": cfg.model,
            "messages": [{"role": "system", "content": self.system_prompts[system_id]}, {"role": "user", "content": user}],
            "max_tokens": max_tokens,
            "temperature": cfg.temperature,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        if cfg.ignore_eos:
            body["ignore_eos"] = True
        return body

    def _faq(self, rng: random.Random) -> RequestSpec:
        intent = self._pick(rng, self.hot_cdf)
        cls, phrasings = self.intents[intent]
        question = rng.choice(phrasings)
        if cls != "chitchat" and rng.random() < self.cfg.decorate_ratio:
            if rng.random() < 0.5:
                question = rng.choice(_PREFIXES) + question
            else:
                question = question + rng.choice(_SUFFIXES)
        system_id = self._pick(rng, self.system_cdf)
        max_tokens = _CLASS_MAX_TOKENS.get(cls, self.cfg.max_tokens)
        return RequestSpec(kind="hot", key=f"{cls}-{intent}", system_id=system_id, doc_id=None, body=self._body(system_id, question, max_tokens))

    def next(self) -> RequestSpec:
        if self.rng.random() < self.cfg.repeat_ratio:
            if self.cfg.question_style == "faq":
                return self._faq(self.rng)
            return self.hot_pool[self._pick(self.rng, self.hot_cdf)]
        return self._make(self.rng, "new", str(next(self.counter)))
