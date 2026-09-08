#!/usr/bin/env bash
# 串口矩阵乘测试 — 发送端
# 运行 uart-matmul 的 sender 模式：发送矩阵乘法请求并打印结果。
# 需与接收端经 null-modem 交叉串口线连接。
#
# 用法: ./run_serial_sender.sh [PORT] [ROWS_A] [COLS_A] [COLS_B] [COUNT] [BAUD]
# 示例: ./run_serial_sender.sh /dev/ttyUSB0 2 3 2 5 115200
#
# 配对使用：
#   一键测试 ./run_serial_test.sh
#   或分开跑 终端1 ./run_serial_receiver.sh  终端2 ./run_serial_sender.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${1:-/dev/ttyUSB0}"
ROWS_A="${2:-2}"
COLS_A="${3:-3}"
COLS_B="${4:-2}"
COUNT="${5:-2}"
BAUD="${6:-115200}"

BIN="$ROOT/target/release/uart-matmul"
if [ ! -x "$BIN" ]; then
    echo "[BUILD] 编译 uart-matmul (release)..."
    cargo build --release --manifest-path "$ROOT/Cargo.toml"
fi

echo "============================================"
echo " uart-matmul 串口矩阵乘 — 发送端"
echo "============================================"
echo " 端口:   $PORT"
echo " 波特率: $BAUD"
echo " 矩阵:   A=${ROWS_A}x${COLS_A}  B=${COLS_A}x${COLS_B}"
echo " 请求数: $COUNT"
echo "============================================"

"$BIN" --port "$PORT" --baud "$BAUD" --mode sender \
    --rows-a "$ROWS_A" --cols-a "$COLS_A" --cols-b "$COLS_B" \
    --count "$COUNT"
