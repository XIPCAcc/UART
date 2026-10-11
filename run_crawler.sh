#!/usr/bin/env bash
# 多核爬虫一键运行脚本
#
# 自动完成：
#   1. cargo build --release
#   2. 若目标端口没有服务监听，自动启动 test_server.py 作为被测 HTTP 服务
#      （脚本退出时自动清理）
#   3. taskset 绑核运行 crawler，固定时长后发 SIGTERM 验证优雅退出
#
# 用法：
#   ./run_crawler.sh [INTERVAL_MS] [BURST] [DURATION_S] [ADDR] [PATH] [CPUS]
#
# 示例：
#   ./run_crawler.sh                          # 默认 500ms 投放 10 个，跑 10s
#   ./run_crawler.sh 200 50 15                # 200ms 投放 50 个，跑 15s
#   ./run_crawler.sh 1000 5 10 127.0.0.1:19090 / 0-3

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$ROOT/target/release/uart-matmul"

INTERVAL="${1:-3000}"
BURST="${2:-32}"
DURATION="${3:-3}"
ADDR="${4:-127.0.0.1:19090}"
REQ_PATH="${5:-/}"
CPUS="${6:-}"

PORT="${ADDR##*:}"
SRV_PID=""
NEED_CLEANUP_SRV=0

cleanup() {
    if [ "$NEED_CLEANUP_SRV" = "1" ] && [ -n "$SRV_PID" ]; then
        kill "$SRV_PID" 2>/dev/null || true
        wait "$SRV_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# ── 1. 编译 ─────────────────────────────────────────────────
echo "==> Building (release) ..."
(cd "$ROOT" && cargo build --release) 2>&1 | tail -2
[ -x "$BIN" ] || { echo "ERROR: 二进制不存在: $BIN" >&2; exit 1; }

# ── 2. 确定绑核范围 ─────────────────────────────────────────
if [ -z "$CPUS" ]; then
    N=$(nproc)
    CPUS="0-$((N - 1))"
fi
echo "==> CPU affinity: $CPUS (available parallelism will be $(taskset -c "$CPUS" nproc))"

# ── 3. 检查/启动被测 HTTP 服务 ──────────────────────────────
if ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":$PORT\$"; then
    echo "==> $ADDR 已有服务监听，直接使用"
else
    echo "==> $ADDR 无监听，启动测试服务 test_server.py ..."
    python3 "$ROOT/test_server.py" "$PORT" &
    SRV_PID=$!
    NEED_CLEANUP_SRV=1
    for _ in $(seq 1 50); do
        if ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":$PORT\$"; then
            break
        fi
        sleep 0.1
    done
fi

# ── 4. 运行爬虫（固定时长，SIGTERM 触发优雅退出与汇总）──────
echo "==> Running crawler: interval=${INTERVAL}ms burst=${BURST} duration=${DURATION}s target=$ADDR$REQ_PATH"
echo ""
taskset -c "$CPUS" timeout -s TERM "$DURATION" "$BIN" \
    --mode crawler \
    --addr "$ADDR" \
    --path "$REQ_PATH" \
    --interval "$INTERVAL" \
    --burst "$BURST" > crawler-$(date +%Y%m%d%H%M%S).log 2>&1
RC=$?
echo ""
if [ "$RC" = 0 ] || [ "$RC" = 124 ]; then
    echo "==> Done (timeout rc=$RC)"
else
    echo "==> crawler exited with rc=$RC" >&2
fi
