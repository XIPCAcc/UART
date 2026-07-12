#!/usr/bin/env bash
# uart-matmul test script
# 用法: ./run_test.sh [RECEIVER_PORT] [SENDER_PORT]
#
# 默认使用 /dev/ttyS0 作为接收端，/dev/ttyUSB0 作为发送端。
# 两个端口需要物理上通过 null-modem 交叉串口线连接。
#
# 示例:
#   ./run_test.sh                          # 使用默认端口
#   ./run_test.sh /dev/ttyS0 /dev/ttyUSB0  # 指定端口
#   ./run_test.sh /dev/ttyS0 /dev/ttyUSB0 2x3 3x2 10  # 指定矩阵维度和请求数

set -euo pipefail

RECEIVER_PORT="${1:-/dev/ttyS0}"
SENDER_PORT="${2:-/dev/ttyUSB0}"
ROWS_A="${3:-2}"
COLS_A="${4:-3}"
COLS_B="${5:-2}"
COUNT="${6:-5}"

BINARY="./target/release/uart-matmul"
if [ ! -f "$BINARY" ]; then
    BINARY="./target/debug/uart-matmul"
fi

if [ ! -f "$BINARY" ]; then
    echo "Building uart-matmul..."
    cargo build --release
    BINARY="./target/release/uart-matmul"
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
"$BINARY" --port "$RECEIVER_PORT" &
RECEIVER_PID=$!
sleep 2

# 启动发送端
echo "[TEST] 启动发送端..."
"$BINARY" --port "$SENDER_PORT" --mode sender \
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