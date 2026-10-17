#!/usr/bin/env bash
# 一键启动 MaaS 网关环境：编译 WASM 插件 → docker compose 启动 → 打印调度插件看到的副本列表。
# 用法：./up.sh            （额外参数会透传给 docker compose up，例如 ./up.sh --force-recreate）
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)
PLUGINS=(maas-cache maas-router maas-scheduler)

for plugin in "${PLUGINS[@]}"; do
  dir="$ROOT/plugins/wasm-go/extensions/$plugin"
  echo ">> 编译 $plugin.wasm"
  if command -v go >/dev/null 2>&1; then
    (cd "$dir" && GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o main.wasm .)
  else
    # 本机没有 Go 时用官方镜像编译（需要能访问 Go module 代理，可通过 GOPROXY 指定国内源）
    docker run --rm -v "$dir":/src -w /src -e GOOS=wasip1 -e GOARCH=wasm \
      -e GOPROXY="${GOPROXY:-https://proxy.golang.org,direct}" golang:1.24 \
      go build -buildmode=c-shared -o main.wasm .
  fi
done

echo ">> 启动容器"
docker compose up -d --build "$@"

echo ">> 等待网关就绪"
for _ in $(seq 1 60); do
  if curl -fsS localhost:10000/maas/debug/hosts >/dev/null 2>&1; then
    curl -fsS localhost:10000/maas/debug/hosts | python3 -m json.tool
    exit 0
  fi
  sleep 1
done
echo "网关 60 秒内没有就绪，查看日志：docker compose logs gateway" >&2
exit 1
