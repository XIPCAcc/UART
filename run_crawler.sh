#!/usr/bin/env bash
# 爬虫守护进程一键运行脚本
#
# 自动完成：
#   1. cargo build --release
#   2. 若目标端口没有服务监听，自动启动 test_server.py 作为被测 HTTP 服务
#      （脚本退出时自动清理）
#   3. taskset 绑核以后台守护进程方式启动 crawler（--listen 监听外部请求）
#   4. 按固定间隔通过 nc 向守护进程提交请求：每轮一条连接，批量发送 REQS 行
#      （守护进程按行解析，逐行回 ACK 并 spawn 爬虫任务），共 ROUNDS 轮
#   5. 投放完毕后发 SIGTERM 验证优雅退出，打印汇总日志
#
# 用法：
#   ./run_crawler.sh [REQS] [INTERVAL_MS] [ADDR] [PATH] [LISTEN] [CPUS] [ROUNDS]
#
# 示例：
#   ./run_crawler.sh                                  # 默认 3 轮 x 10 个请求，轮间隔 1s
#   ./run_crawler.sh 500 20                           # 3 轮 x 500 个请求，轮间隔 20ms
#   ./run_crawler.sh 1000 10 127.0.0.1:19090 / 127.0.0.1:9090 0-3 5

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$ROOT/target/release/crawler-daemon"

REQS="${1:-32}"
INTERVAL="${2:-10000}"
ADDR="${3:-127.0.0.1:19090}"
REQ_PATH="${4:-/}"
LISTEN="${5:-127.0.0.1:9090}"
CPUS="${6:-}"
ROUNDS="${7:-3}"

PORT="${ADDR##*:}"
LISTEN_HOST="${LISTEN%:*}"
LISTEN_PORT="${LISTEN##*:}"
SRV_PID=""
NEED_CLEANUP_SRV=0
DM_PID=""
LOG="crawler-$(date +%Y%m%d%H%M%S).log"

cleanup() {
    if [ -n "$DM_PID" ]; then
        kill "$DM_PID" 2>/dev/null || true
        wait "$DM_PID" 2>/dev/null || true
    fi
    if [ "$NEED_CLEANUP_SRV" = "1" ] && [ -n "$SRV_PID" ]; then
        kill "$SRV_PID" 2>/dev/null || true
        wait "$SRV_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# ── 1. 编译（带详细日志，便于观察运行过程）─────────────────────
echo "==> Building (release, verbose-log) ..."
(cd "$ROOT" && cargo build --release --features verbose-log) 2>&1 | tail -2
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

# ── 4. 后台启动爬虫守护进程 ─────────────────────────────────
echo "==> Starting crawler daemon on $LISTEN (log: $LOG) ..."
taskset -c "$CPUS" "$BIN" --mode crawler --listen "$LISTEN" > "$LOG" 2>&1 &
DM_PID=$!
# 等监听端口就绪
for _ in $(seq 1 50); do
    if ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":$LISTEN_PORT\$"; then
        break
    fi
    sleep 0.1
done
if ! ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":$LISTEN_PORT\$"; then
    echo "ERROR: 守护进程未能监听 $LISTEN" >&2
    exit 1
fi

# ── 5. 提交爬虫请求：每轮一条连接批量发送 REQS 行 ───────────
echo "==> Submitting $ROUNDS rounds x $REQS requests (interval=${INTERVAL}ms target=$ADDR$REQ_PATH) ..."
for i in $(seq 1 "$ROUNDS"); do
    # 守护进程按行解析：一个连接可携带多个请求，每行一个 <host:port> <path>
    for j in $(seq 1 "$REQS"); do
        echo "$ADDR $REQ_PATH"
    done | nc -w1 "$LISTEN_HOST" "$LISTEN_PORT" > /dev/null
    [ "$INTERVAL" -gt 0 ] && sleep "$(awk "BEGIN{print $INTERVAL/1000}")"
done

# ── 6. 收尾：等待在途任务完成，SIGTERM 守护进程，打印汇总 ───
sleep 1
kill -TERM "$DM_PID" 2>/dev/null || true
wait "$DM_PID" 2>/dev/null || true
DM_PID=""
echo ""
echo "==> Daemon log tail:"
grep -E "\[CRAWLER\]|\[DAEMON\]" "$LOG" | tail -5
echo "==> Done (full log: $LOG)"
