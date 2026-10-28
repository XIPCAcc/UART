#!/bin/bash
# 构建基准用二进制。
#
# 默认关闭 verbose-log（零日志开销）：每行日志都会引入 write 系统调用并可能
# 阻塞在 stderr 上，污染 syscall / wall 统计。只有调试时才用 LOG=1 打开。
#
# 三个 worker 变体（裸 clone3 / std::thread / 常驻池）在代码层互斥，各自用
# 独立 target 目录构建，避免 cargo 同名产物互相覆盖、也避免 feature 切换
# 导致的整树重编。
#
# 用法：
#   ./build.sh              # 裸 clone3（默认）
#   ./build.sh thread       # std::thread 动态扩容
#   ./build.sh pool         # 常驻线程池
#   ./build.sh all          # 依次构建三个变体
#   LOG=1 ./build.sh pool   # 调试口径（verbose-log 开启，勿用于基准）
#
# 输出：target/bench/<variant>/release/crawler-daemon
set -euo pipefail

cd "$(dirname "$0")/../.."

LOG_ON=${LOG:-0}

build_one() {
    local variant=$1
    local workers=""
    case "$variant" in
        raw)    workers="" ;;
        thread) workers="workers-thread" ;;
        pool)   workers="workers-pool" ;;
        *) echo "未知变体: $variant（可选 raw|thread|pool|all）" >&2; return 2 ;;
    esac

    local dir="target/bench/$variant"
    local args=(--release)
    if [ "$LOG_ON" = "1" ]; then
        args+=(--features verbose-log)
        echo "[bench] 构建 $variant：verbose-log 开启（调试用，勿用于基准）"
    else
        echo "[bench] 构建 $variant：verbose-log 关闭（基准口径）"
    fi
    [ -n "$workers" ] && args+=(--features "$workers")

    CARGO_TARGET_DIR="$dir" cargo build "${args[@]}"
    echo "[bench] -> $(pwd)/$dir/release/crawler-daemon"
}

TARGET=${1:-raw}
if [ "$TARGET" = "all" ]; then
    for v in raw thread pool; do build_one "$v"; done
else
    build_one "$TARGET"
fi
