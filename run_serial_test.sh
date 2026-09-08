#!/usr/bin/env bash
# 串口矩阵乘测试 — 一键收发
# 自动启动 uart-matmul 接收端（后台）与发送端（前台），验证完整链路。
# 两个串口需通过 null-modem 交叉串口线物理连接。
#
# 用法: ./run_serial_test.sh [RECEIVER_PORT] [SENDER_PORT]
# 用法: ./run_serial_test.sh [RECEIVER_PORT] [SENDER_PORT] [ROWS_A] [COLS_A] [COLS_B] [COUNT]
# 示例: ./run_serial_test.sh /dev/ttyS0 /dev/ttyUSB0 4 5 3 10

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RECEIVER_PORT="${1:-/dev/ttyS0}"
SENDER_PORT="${2:-/dev/ttyUSB0}"
ROWS_A="${3:-2}"
COLS_A="${4:-3}"
COLS_B="${5:-2}"
COUNT="${6:-5}"

BIN="$ROOT/target/release/uart-matmul"
if [ ! -x "$BIN" ]; then
    echo "Building uart-matmul..."
    cargo build --release --manifest-path "$ROOT/Cargo.toml"
fi

echo "============================================"
echo " uart-matmul 串口矩阵乘法测试"
echo "============================================"
echo " 接收端:  $RECEIVER_PORT (receiver)"
echo " 发送端:  $SENDER_PORT (sender)"
echo " 矩阵:    A=${ROWS_A}x${COLS_A}  B=${COLS_A}x${COLS_B}"
echo " 请求数:  $COUNT"
echo "============================================"

cleanup() {
    echo ""
    echo "[TEST] 清理中..."
    kill $RECEIVER_PID 2>/dev/null || true
    wait $RECEIVER_PID 2>/dev/null || true
    echo "[TEST] 完成"
}

trap cleanup EXIT INT TERM

# 启动接收端
echo ""
echo "[TEST] 启动接收端..."
"$BIN" --port "$RECEIVER_PORT" &
RECEIVER_PID=$!
sleep 2

# 启动发送端
echo "[TEST] 启动发送端..."
"$BIN" --port "$SENDER_PORT" --mode sender \
    --rows-a "$ROWS_A" --cols-a "$COLS_A" --cols-b "$COLS_B" \
    --count "$COUNT"

SENDER_EXIT=$?

kill $RECEIVER_PID 2>/dev/null || true
wait $RECEIVER_PID 2>/dev/null || true

if [ $SENDER_EXIT -eq 0 ]; then
    echo ""
    echo "[TEST] 测试通过"
else
    echo ""
    echo "[TEST] 测试失败 (exit code: $SENDER_EXIT)"
    exit $SENDER_EXIT
fi
