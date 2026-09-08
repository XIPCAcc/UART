#!/usr/bin/env bash
# 串口矩阵乘测试 — 接收端
# 运行 uart-matmul 的 receiver 模式：持续监听串口、收帧并计算矩阵乘。
# 需与发送端经 null-modem 交叉串口线连接。
#
# 用法: ./run_serial_receiver.sh [PORT] [BAUD]
# 示例: ./run_serial_receiver.sh /dev/ttyS0 115200
#
# 配对使用：
#   一键测试 ./run_serial_test.sh
#   或分开跑 终端1 ./run_serial_receiver.sh  终端2 ./run_serial_sender.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${1:-/dev/ttyS0}"
BAUD="${2:-115200}"

BIN="$ROOT/target/release/uart-matmul"
if [ ! -x "$BIN" ]; then
    echo "[BUILD] 编译 uart-matmul (release)..."
    cargo build --release --manifest-path "$ROOT/Cargo.toml"
fi

echo "============================================"
echo " uart-matmul 串口矩阵乘 — 接收端"
echo "============================================"
echo " 端口:  $PORT"
echo " 波特率: $BAUD"
echo " 按 Ctrl+C 停止"
echo "============================================"

"$BIN" --port "$PORT" --baud "$BAUD" --mode receiver
