#!/usr/bin/env bash
# 阶段 3 压测：银行客服问答负载（question-style=faq：热点问题有多种说法）下的消融实验。
#   direct       纯 Envoy 轮询直连大模型池（10001 端口，无插件）
#   exact        网关全开，但跳过 L3（x-maas-semantic-bypass），只有 L1/L2 精确缓存
#   semantic     网关全开：L1/L2/L3 缓存 + 语义分发 + 调度
#   no-dispatch  同 semantic，但请求直接指定大模型（不做语义分发，全部进大模型池）
#
#   ./phase3_bench.sh                         # 闭环与开环各跑一遍
#   PARTS=open RATE=8 ./phase3_bench.sh
#   RUNS="semantic" ./phase3_bench.sh         # 只重跑指定的几轮
set -euo pipefail
cd "$(dirname "$0")"

PY=${PY:-python3}
OUT=${OUT:-../../results/phase3}
DEPLOY=${DEPLOY:-../../deploy}
DURATION=${DURATION:-90}
WARMUP=${WARMUP:-20}
CONC=${CONC:-32}
RATE=${RATE:-6}
PARTS=${PARTS:-"closed open"}
RUNS=${RUNS:-}
WORKLOAD=${WORKLOAD:-"--question-style faq --repeat-ratio 0.5"}
GATEWAY=${GATEWAY:-http://localhost:10000}
BASELINE=${BASELINE:-http://localhost:10001}
ADMIN=${ADMIN:-http://localhost:9901}
QDRANT=${QDRANT:-http://localhost:6333}
COLLECTION=${COLLECTION:-maas_semantic_cache}
ADMINS=${ADMINS:-"http://localhost:8001 http://localhost:8002 http://localhost:8003 http://localhost:8004"}

reset_caches() {
  for admin in $ADMINS; do
    curl -fsS -X POST "$admin/admin/reset_prefix_cache" >/dev/null
  done
  docker compose -f "$DEPLOY/docker-compose.yml" exec -T redis redis-cli FLUSHALL >/dev/null
  # 删除向量集合；网关重启后由 maas-cache 的定时任务重新创建
  curl -fsS -X DELETE "$QDRANT/collections/$COLLECTION" >/dev/null
  # 重启网关：清空 L1、调度插件的前缀索引与代价模型、路由插件的类别质心，保证每轮起点一致
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

GW=(--url "$GATEWAY" --admin "$ADMIN")
NO_L3=(--header "x-maas-semantic-bypass: on")

compare_all() {
  local suffix=$1
  echo
  echo "#### 直连轮询 vs 网关（不做语义分发，同样 3 个大模型副本）"
  $PY compare.py "$OUT/direct$suffix/summary.json" "$OUT/no-dispatch$suffix/summary.json"
  echo
  echo "#### 只有精确缓存 vs 加上 L3 语义缓存（都做语义分发）"
  $PY compare.py "$OUT/exact$suffix/summary.json" "$OUT/semantic$suffix/summary.json"
  echo
  echo "#### 不做语义分发 vs 语义分发（多了小模型池）"
  $PY compare.py "$OUT/no-dispatch$suffix/summary.json" "$OUT/semantic$suffix/summary.json"
}

if [[ " $PARTS " == *" closed "* ]]; then
  LOOP=(--mode closed --concurrency "$CONC")
  run direct --url "$BASELINE" "${LOOP[@]}" --model mock-qwen
  run exact "${GW[@]}" "${LOOP[@]}" --model higress/auto "${NO_L3[@]}"
  run semantic "${GW[@]}" "${LOOP[@]}" --model higress/auto
  run no-dispatch "${GW[@]}" "${LOOP[@]}" --model mock-qwen
  compare_all ""
fi

if [[ " $PARTS " == *" open "* ]]; then
  LOOP=(--mode open --rate "$RATE")
  run direct-open --url "$BASELINE" "${LOOP[@]}" --model mock-qwen
  run exact-open "${GW[@]}" "${LOOP[@]}" --model higress/auto "${NO_L3[@]}"
  run semantic-open "${GW[@]}" "${LOOP[@]}" --model higress/auto
  run no-dispatch-open "${GW[@]}" "${LOOP[@]}" --model mock-qwen
  compare_all "-open"
fi
