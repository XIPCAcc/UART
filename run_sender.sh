#!/usr/bin/env bash
# 发方脚本 - 每次运行前重新编译，然后发送矩阵乘法请求
# 用法: ./run_sender.sh [PORT] [ROWS_A] [COLS_A] [COLS_B] [COUNT] [BAUD]
# 示例: ./run_sender.sh /dev/ttyUSB0 2 3 2 5 115200

set -euo pipefail

PORT="${1:-/dev/ttyUSB0}"
ROWS_A="${2:-2}"
COLS_A="${3:-3}"
COLS_B="${4:-2}"
COUNT="${5:-5}"
BAUD="${6:-115200}"

echo "============================================"
echo " uart-matmul 发方"
echo "============================================"
echo " 端口:   $PORT"
echo " 波特率:  $BAUD"
echo " 矩阵:   A=${ROWS_A}x${COLS_A}  B=${COLS_A}x${COLS_B}"
echo " 请求数: $COUNT"
echo "============================================"

echo ""
echo "[BUILD] 重新编译中..."
cargo build --release

echo ""
echo "[RUN] 启动发送..."
./target/release/uart-matmul \
    --port "$PORT" \
    --baud "$BAUD" \
    --mode sender \
    --rows-a "$ROWS_A" \
    --cols-a "$COLS_A" \
    --cols-b "$COLS_B" \
    --count "$COUNT"