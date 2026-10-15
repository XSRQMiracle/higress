#!/usr/bin/env bash
# 阶段 0 基线压测：纯 Envoy 轮询（:10001）vs maas-scheduler least_waiting（:10000），另测单副本直连作参考。
# 每轮开始前清空各副本的前缀缓存，保证起点一致。可用环境变量调整：
#   PY="uv run --quiet --python 3.12 --with aiohttp python" DURATION=600 CONC=48 ./phase0_bench.sh
set -euo pipefail
cd "$(dirname "$0")"

PY=${PY:-python3}
OUT=${OUT:-../../results/phase0}
DURATION=${DURATION:-60}
WARMUP=${WARMUP:-10}
CONC=${CONC:-48}
GATEWAY=${GATEWAY:-http://localhost:10000}
BASELINE=${BASELINE:-http://localhost:10001}
DIRECT=${DIRECT:-http://localhost:8001}
ADMINS=${ADMINS:-"http://localhost:8001 http://localhost:8002 http://localhost:8003"}

reset_prefix_cache() {
  for admin in $ADMINS; do
    curl -fsS -X POST "$admin/admin/reset_prefix_cache" >/dev/null
  done
}

run() {
  local name=$1
  shift
  reset_prefix_cache
  $PY loadgen.py --label "$name" --duration "$DURATION" --warmup "$WARMUP" --out "$OUT/$name" "$@"
}

run baseline-rr --url "$BASELINE" --mode closed --concurrency "$CONC"
# 网关入口加入 maas-cache 之后，用绕过请求头只测调度插件，保证与阶段 0 的结果可比
run gateway-least-waiting --url "$GATEWAY" --mode closed --concurrency "$CONC" --header "x-maas-cache-bypass: on"
run direct-single-replica --url "$DIRECT" --mode closed --concurrency $((CONC / 3))

echo
$PY compare.py "$OUT/baseline-rr/summary.json" "$OUT/gateway-least-waiting/summary.json"
