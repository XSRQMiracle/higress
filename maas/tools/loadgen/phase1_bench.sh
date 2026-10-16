#!/usr/bin/env bash
# 阶段 1 压测：纯轮询基线 vs 网关（只调度，带绕过缓存的请求头）vs 网关（缓存 + 调度），闭环与开环各一组。
# 每轮开始前清空所有缓存：各副本的前缀缓存、Redis（L2），并重启网关清空各 worker 的 L1。
# 可用环境变量调整，例如决赛口径：DURATION=600 ./phase1_bench.sh
set -euo pipefail
cd "$(dirname "$0")"

PY=${PY:-python3}
OUT=${OUT:-../../results/phase1}
DEPLOY=${DEPLOY:-../../deploy}
DURATION=${DURATION:-180}
WARMUP=${WARMUP:-30}
CONC=${CONC:-48}
RATE=${RATE:-16}
GATEWAY=${GATEWAY:-http://localhost:10000}
BASELINE=${BASELINE:-http://localhost:10001}
ADMINS=${ADMINS:-"http://localhost:8001 http://localhost:8002 http://localhost:8003"}

reset_caches() {
  for admin in $ADMINS; do
    curl -fsS -X POST "$admin/admin/reset_prefix_cache" >/dev/null
  done
  docker compose -f "$DEPLOY/docker-compose.yml" exec -T redis redis-cli FLUSHALL >/dev/null
  docker compose -f "$DEPLOY/docker-compose.yml" restart gateway >/dev/null
  for _ in $(seq 1 60); do
    curl -fsS "$GATEWAY/maas/debug/hosts" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "gateway not ready after restart" >&2
  exit 1
}

run() {
  local name=$1
  shift
  reset_caches
  $PY loadgen.py --label "$name" --duration "$DURATION" --warmup "$WARMUP" --out "$OUT/$name" "$@"
}

run baseline-rr --url "$BASELINE" --mode closed --concurrency "$CONC"
run gateway-nocache --url "$GATEWAY" --mode closed --concurrency "$CONC" --header "x-maas-cache-bypass: on"
run gateway-cache --url "$GATEWAY" --mode closed --concurrency "$CONC"
run baseline-rr-open --url "$BASELINE" --mode open --rate "$RATE"
run gateway-cache-open --url "$GATEWAY" --mode open --rate "$RATE"

echo
echo "#### 闭环：纯轮询基线 vs 缓存 + 调度"
$PY compare.py "$OUT/baseline-rr/summary.json" "$OUT/gateway-cache/summary.json"
echo
echo "#### 闭环：只调度 vs 缓存 + 调度（缓存本身的贡献）"
$PY compare.py "$OUT/gateway-nocache/summary.json" "$OUT/gateway-cache/summary.json"
echo
echo "#### 开环 ${RATE} req/s：纯轮询基线 vs 缓存 + 调度"
$PY compare.py "$OUT/baseline-rr-open/summary.json" "$OUT/gateway-cache-open/summary.json"
