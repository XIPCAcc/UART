#!/bin/bash
# 构建基准用二进制。
#
# 默认关闭 verbose-log（零日志开销）：每行日志都会引入 write 系统调用并可能
# 阻塞在 stderr 上，污染 syscall / wall 统计。只有调试时才用 LOG=1 打开。
#
# 用法：
#   ./build.sh            # 基准口径（verbose-log 关闭）
#   LOG=1 ./build.sh      # 调试口径（verbose-log 开启，勿用于基准）
#
# 输出：target/release/crawler-daemon
set -euo pipefail

cd "$(dirname "$0")/../.."

if [ "${LOG:-0}" = "1" ]; then
    echo "[bench] verbose-log 开启（调试用，勿用于基准）"
    cargo build --release --features verbose-log
else
    echo "[bench] verbose-log 关闭（基准口径）"
    cargo build --release
fi
echo "[bench] -> $(pwd)/target/release/crawler-daemon"
