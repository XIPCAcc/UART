#!/usr/bin/env bash
# 收方脚本 - 运行在接收端，持续监听串口
# 用法: ./run_receiver.sh [PORT] [BAUD]
# 示例: ./run_receiver.sh /dev/ttyS0 115200

set -euo pipefail

PORT="${1:-/dev/ttyS0}"
BAUD="${2:-115200}"

echo "============================================"
echo " uart-matmul 收方"
echo "============================================"
echo " 端口:  $PORT"
echo " 波特率: $BAUD"
echo " 按 Ctrl+C 停止"
echo "============================================"

cargo run --release -- --port "$PORT" --baud "$BAUD"