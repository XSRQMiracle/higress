#!/usr/bin/env python3
"""对比两次压测结果（baseline vs 网关），按决赛评分口径输出提升幅度。

  python compare.py ../../results/phase0/baseline-rr/summary.json ../../results/phase0/gateway/summary.json

决赛档位（相较 baseline）：卓越 P99 TTFT 提升 ≥30% 且 TPOT 提升 ≥20%；进阶 15–30% / 10–20%；基础 5–15% / <10%。
TPOT 的统计口径（均值 / 中位数 / P99）尚待出题方确认，这里按均值定档，同时列出 P50、P99 供参考。
"""
from __future__ import annotations

import json
import sys


def _get(summary: dict, path: str):
    node = summary
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


# (显示名, 字段路径, 越小越好)
_ROWS = [
    ("TTFT P50 (ms)", "ttft_ms.p50", True),
    ("TTFT P99 (ms)", "ttft_ms.p99", True),
    ("TPOT mean (ms)", "tpot_ms.mean", True),
    ("TPOT P50 (ms)", "tpot_ms.p50", True),
    ("TPOT P99 (ms)", "tpot_ms.p99", True),
    ("E2E P99 (s)", "e2e_s.p99", True),
]


def change(base, new, lower_is_better: bool):
    """返回"提升"比例：时延类为下降比例，吞吐类为上升比例。"""
    if base in (None, 0) or new is None:
        return None
    return (base - new) / base if lower_is_better else (new - base) / base


def grade(ttft_gain, tpot_gain) -> str:
    if ttft_gain is None or tpot_gain is None:
        return "无法定档（缺少数据）"
    if ttft_gain >= 0.30 and tpot_gain >= 0.20:
        return "卓越（30 分）"
    if ttft_gain >= 0.15 and tpot_gain >= 0.10:
        return "进阶（20 分）"
    if ttft_gain >= 0.05:
        return "基础（10 分）"
    return "未达到基础档"


def main(argv: list[str]) -> None:
    if len(argv) != 2:
        sys.exit(__doc__)
    with open(argv[0], encoding="utf-8") as fh:
        base = json.load(fh)
    with open(argv[1], encoding="utf-8") as fh:
        new = json.load(fh)
    base_label = base["config"].get("label") or base["config"]["url"]
    new_label = new["config"].get("label") or new["config"]["url"]
    print(f"baseline: {base_label}\ncandidate: {new_label}\n")
    print(f"{'group':<5} {'metric':<16} {'baseline':>10} {'candidate':>10} {'提升':>8}")
    for group in ("all", "hot", "new"):
        for name, path, lower in _ROWS:
            b = _get(base, f"groups.{group}.{path}")
            n = _get(new, f"groups.{group}.{path}")
            c = change(b, n, lower)
            print(
                f"{group:<5} {name:<16} {'-' if b is None else f'{b:.1f}':>10} {'-' if n is None else f'{n:.1f}':>10} "
                f"{'-' if c is None else f'{c:+.1%}':>8}"
            )
    for name, path in (("throughput req/s", "throughput_rps"), ("output tok/s", "output_tokens_per_s")):
        b, n = _get(base, path), _get(new, path)
        c = change(b, n, False)
        print(f"{'all':<5} {name:<16} {b:>10.2f} {n:>10.2f} {'-' if c is None else f'{c:+.1%}':>8}")
    ttft_gain = change(_get(base, "groups.all.ttft_ms.p99"), _get(new, "groups.all.ttft_ms.p99"), True)
    tpot_gain = change(_get(base, "groups.all.tpot_ms.mean"), _get(new, "groups.all.tpot_ms.mean"), True)
    print(f"\n决赛档位估计（P99 TTFT {'-' if ttft_gain is None else f'{ttft_gain:+.1%}'}，"
          f"mean TPOT {'-' if tpot_gain is None else f'{tpot_gain:+.1%}'}）：{grade(ttft_gain, tpot_gain)}")


if __name__ == "__main__":
    main(sys.argv[1:])
