#!/usr/bin/env bash
# 本地开发启动 wa-server (PG 存储 + Redis + 内嵌 API)。
# 依赖服务: podman-compose -f wa-server/deploy/compose/docker-compose.yml up -d
#
# 用法: ./wa-server/deploy/scripts/run-local.sh
# 环境变量参考 wa-server/.env.example;缺省走下面默认值。
set -euo pipefail

export DATABASE_URL="${DATABASE_URL:-postgres://postgres:123456@localhost:5432/mydb}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
export POD_ID="${POD_ID:-pod-1}"
export API_ADDR="${API_ADDR:-0.0.0.0:8080}"

exec cargo run -p wa-server
