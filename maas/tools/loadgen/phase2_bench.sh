#!/usr/bin/env bash
# 阶段 2 压测：长上下文 RAG 负载下比较 纯轮询基线 / least_waiting / cost 调度（均绕过缓存）/ 缓存 + cost 调度。
# 默认负载：40% 的请求附带 6K token 文档，共 24 篇（约 144K token，超过单个副本 131K token 的 KV 容量），
# 用来体现前缀亲和"每个副本只缓存一部分文档、总缓存容量随副本数增长"的效果。
#   PARTS="closed open" RATE=6 ./phase2_bench.sh
#   RUNS="gw-cost gw-full" ./phase2_bench.sh     # 只重跑指定的几轮（对比使用已有的其他结果）
set -euo pipefail
cd "$(dirname "$0")"

PY=${PY:-python3}
OUT=${OUT:-../../results/phase2}
DEPLOY=${DEPLOY:-../../deploy}
DURATION=${DURATION:-150}
WARMUP=${WARMUP:-30}
CONC=${CONC:-48}
RATE=${RATE:-6}
PARTS=${PARTS:-"closed open"}
RUNS=${RUNS:-}
WORKLOAD=${WORKLOAD:-"--long-ratio 0.4 --doc-pool-size 24 --long-tokens 6000"}
GATEWAY=${GATEWAY:-http://localhost:10000}
BASELINE=${BASELINE:-http://localhost:10001}
ADMINS=${ADMINS:-"http://localhost:8001 http://localhost:8002 http://localhost:8003"}

reset_caches() {
  for admin in $ADMINS; do
    curl -fsS -X POST "$admin/admin/reset_prefix_cache" >/dev/null
  done
  docker compose -f "$DEPLOY/docker-compose.yml" exec -T redis redis-cli FLUSHALL >/dev/null
  # 重启网关：清空 L1 缓存与调度插件的前缀索引、代价模型，保证每轮起点一致
  docker compose -f "$DEPLOY/docker-compose.yml" restart gateway >/dev/null
  for _ in $(seq 1 60); do
    curl -fsS "$GATEWAY/maas/debug/hosts" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "gateway not ready after restart" >&2
  exit 1
}

want() {
  [[ -z "$RUNS" || " $RUNS " == *" $1 "* ]]
}

run() {
  local name=$1
  shift
  want "$name" || return 0
  reset_caches
  # shellcheck disable=SC2086
  $PY loadgen.py --label "$name" --duration "$DURATION" --warmup "$WARMUP" --out "$OUT/$name" $WORKLOAD "$@"
}

BYPASS=(--header "x-maas-cache-bypass: on")

if [[ " $PARTS " == *" closed "* ]]; then
  run baseline-rr --url "$BASELINE" --mode closed --concurrency "$CONC"
  run gw-least-waiting --url "$GATEWAY" --mode closed --concurrency "$CONC" "${BYPASS[@]}" --header "x-maas-policy: least_waiting"
  run gw-cost --url "$GATEWAY" --mode closed --concurrency "$CONC" "${BYPASS[@]}"
  run gw-full --url "$GATEWAY" --mode closed --concurrency "$CONC"
  echo
  echo "#### 闭环：least_waiting vs cost 调度（都不带缓存，调度算法本身的贡献）"
  $PY compare.py "$OUT/gw-least-waiting/summary.json" "$OUT/gw-cost/summary.json"
  echo
  echo "#### 闭环：纯轮询基线 vs 缓存 + cost 调度"
  $PY compare.py "$OUT/baseline-rr/summary.json" "$OUT/gw-full/summary.json"
fi

if [[ " $PARTS " == *" open "* ]]; then
  run baseline-rr-open --url "$BASELINE" --mode open --rate "$RATE"
  run gw-full-open --url "$GATEWAY" --mode open --rate "$RATE"
  echo
  echo "#### 开环 ${RATE} req/s：纯轮询基线 vs 缓存 + cost 调度"
  $PY compare.py "$OUT/baseline-rr-open/summary.json" "$OUT/gw-full-open/summary.json"
fi
