#!/usr/bin/env bash
# Demo 数据注入脚本
# 用法: ./deploy/scripts/demo-inject.sh [command]
#
# Commands:
#   pair    <phone>       注入配对码任务 (默认使用测试号 15551234567)
#   pair-qr                注入 QR 配对任务
#   send   <jid> <text>   发送文本消息
#   react  <jid> <msg_id> <emoji>  发送表情回应
#   status                 查看当前 registry 和队列状态
#   events                 阻塞监听 wa-events 事件流
#   clean                  清空所有队列 (谨慎使用)
#   seed                   注入一批假任务演示队列工作 (不会真正发消息,因为没配对)
#
# 示例:
#   ./deploy/scripts/demo-inject.sh pair 8618666206882
#   ./deploy/scripts/demo-inject.sh events
#   ./deploy/scripts/demo-inject.sh seed

set -euo pipefail

# 用容器内的 redis-cli 避免本机没装
REDIS_CLI=(podman exec -i wa-redis redis-cli)

# JID hash -> shard (0..15). 用 crc32 保证同一 JID 总进同一分片。
shard_for_jid() {
    local jid="$1"
    python3 -c "import binascii,sys; print(binascii.crc32(sys.argv[1].encode()) % 16)" "$jid"
}

now_ts() { date +%s; }

cmd_pair() {
    local phone="${1:-8618666206882}"
    local jid="${phone}@s.whatsapp.net"
    local shard
    shard=$(shard_for_jid "$jid")
    local task_id="pair-$(date +%s%N | tail -c 8)"
    local payload
    payload=$(cat <<EOF
{
  "task_id": "$task_id",
  "type": "pair_code",
  "jid": "$jid",
  "created_at": $(now_ts),
  "payload": { "phone_number": "$phone" }
}
EOF
)
    echo "-> 推入配对任务: jid=$jid shard=$shard phone=$phone"
    echo "$payload" | "${REDIS_CLI[@]}" -x LPUSH "wa-queue:$shard" >/dev/null
    echo "✓ 已推入 wa-queue:$shard"
    echo "  另开终端运行: $0 events  来接收配对码"
}

cmd_pair_qr() {
    local phone="${1:-15551234567}"
    local jid="${phone}@s.whatsapp.net"
    local shard
    shard=$(shard_for_jid "$jid")
    local task_id="pqr-$(date +%s%N | tail -c 8)"
    local payload
    payload=$(cat <<EOF
{
  "task_id": "$task_id",
  "type": "pair_qr",
  "jid": "$jid",
  "created_at": $(now_ts),
  "payload": null
}
EOF
)
    echo "-> 推入 QR 配对任务: jid=$jid shard=$shard"
    echo "$payload" | "${REDIS_CLI[@]}" -x LPUSH "wa-queue:$shard" >/dev/null
    echo "✓ 已推入 wa-queue:$shard"
    echo "  另开终端运行: $0 events  来接收 QR 码"
}

cmd_send() {
    local jid="${1:?用法: send <jid> <text>}"
    local text="${2:?用法: send <jid> <text>}"
    local shard
    shard=$(shard_for_jid "$jid")
    local task_id="msg-$(date +%s%N | tail -c 8)"
    local payload
    payload=$(cat <<EOF
{
  "task_id": "$task_id",
  "type": "send_message",
  "jid": "$jid",
  "created_at": $(now_ts),
  "payload": { "to": "$jid", "text": "$text" }
}
EOF
)
    echo "-> 推入发消息任务: jid=$jid shard=$shard text=$text"
    echo "$payload" | "${REDIS_CLI[@]}" -x LPUSH "wa-queue:$shard" >/dev/null
    echo "✓ 已推入 wa-queue:$shard"
}

cmd_react() {
    local jid="${1:?用法: react <jid> <msg_id> <emoji>}"
    local msg_id="${2:?用法: react <jid> <msg_id> <emoji>}"
    local emoji="${3:?用法: react <jid> <msg_id> <emoji>}"
    local shard
    shard=$(shard_for_jid "$jid")
    local task_id="rct-$(date +%s%N | tail -c 8)"
    local payload
    payload=$(cat <<EOF
{
  "task_id": "$task_id",
  "type": "react",
  "jid": "$jid",
  "created_at": $(now_ts),
  "payload": { "to": "$jid", "message_id": "$msg_id", "emoji": "$emoji" }
}
EOF
)
    echo "-> 推入表情回应任务: jid=$jid msg_id=$msg_id emoji=$emoji"
    echo "$payload" | "${REDIS_CLI[@]}" -x LPUSH "wa-queue:$shard" >/dev/null
    echo "✓ 已推入 wa-queue:$shard"
}

cmd_status() {
    echo "=== Redis 队列状态 ==="
    local any=0
    for i in $(seq 0 15); do
        local len
        len=$("${REDIS_CLI[@]}" LLEN "wa-queue:$i" 2>/dev/null || echo "0")
        if [ "$len" != "0" ]; then
            echo "  wa-queue:$i = $len 条待处理"
            any=1
        fi
    done
    [ "$any" = "0" ] && echo "  (所有分片均为空)"
    echo ""
    echo "=== wa-registry (JID -> pod 映射) ==="
    local reg
    reg=$("${REDIS_CLI[@]}" HGETALL wa-registry 2>/dev/null)
    if [ -z "$reg" ]; then
        echo "  (空,没有活跃 session)"
    else
        echo "$reg"
    fi
    echo ""
    echo "=== wa-events 队列长度 ==="
    "${REDIS_CLI[@]}" LLEN wa-events 2>/dev/null
    echo ""
    echo "=== 活跃 lease ==="
    local leases
    leases=$("${REDIS_CLI[@]}" --scan --pattern 'wa-registry:lease:*' 2>/dev/null || true)
    [ -z "$leases" ] && echo "  (无)" || echo "$leases"
}

cmd_events() {
    echo "=== 阻塞监听 wa-events (Ctrl+C 退出) ==="
    "${REDIS_CLI[@]}" BRPOP wa-events 0
}

cmd_clean() {
    echo "⚠️  清空所有 wa-* 队列"
    for i in $(seq 0 15); do
        "${REDIS_CLI[@]}" DEL "wa-queue:$i" >/dev/null
    done
    "${REDIS_CLI[@]}" DEL wa-events >/dev/null
    "${REDIS_CLI[@]}" DEL wa-registry >/dev/null
    for k in $("${REDIS_CLI[@]}" --scan --pattern 'wa-registry:lease:*' 2>/dev/null); do
        "${REDIS_CLI[@]}" DEL "$k" >/dev/null
    done
    for k in $("${REDIS_CLI[@]}" --scan --pattern 'wa-inbox:*' 2>/dev/null); do
        "${REDIS_CLI[@]}" DEL "$k" >/dev/null
    done
    echo "✓ 已清空"
}

cmd_seed() {
    echo "=== 注入 5 个测试任务 (不同 JID) ==="
    local jids=(
        "15550000001@s.whatsapp.net"
        "15550000002@s.whatsapp.net"
        "15550000003@s.whatsapp.net"
        "15550000004@s.whatsapp.net"
        "15550000005@s.whatsapp.net"
    )
    for jid in "${jids[@]}"; do
        cmd_send "$jid" "demo message from seed script"
    done
    echo ""
    echo "✓ 注入完成。这些任务因为没有配对,会被 server 当作非配对任务,"
    echo "  查 wa-registry 找不到 owner,记一条 warn 日志后丢弃。"
    echo "  要真正跑通,先运行: $0 pair <your_phone>"
}

# --- main ---
sub="${1:-help}"
case "$sub" in
    pair)     shift; cmd_pair "$@";;
    pair-qr)  shift; cmd_pair_qr "$@";;
    send)     shift; cmd_send "$@";;
    react)    shift; cmd_react "$@";;
    status)   cmd_status;;
    events)   cmd_events;;
    clean)    cmd_clean;;
    seed)     cmd_seed;;
    *)
        cat <<'USAGE'
用法: demo-inject.sh <command> [args]

命令:
  pair   <phone>                    注入配对码任务,手机号会生成配对码
  pair-qr [<phone>]                 注入 QR 码配对任务
  send   <jid> <text>               发送文本消息任务
  react  <jid> <msg_id> <emoji>     发送表情回应任务
  status                            查看队列/registry/事件状态
  events                            阻塞监听 wa-events 事件流
  clean                             清空所有 wa-* 队列 (谨慎)
  seed                              注入 5 个测试任务演示

示例:
  ./deploy/scripts/demo-inject.sh pair 8618666206882
  ./deploy/scripts/demo-inject.sh events
  ./deploy/scripts/demo-inject.sh send 8618666206882@s.whatsapp.net "你好"
USAGE
        ;;
esac
