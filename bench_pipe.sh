#!/usr/bin/env bash
# UINTR 命名管道（FIFO）性能测试
#
# 测试内容：
#   1. 吞吐量：64KB chunk × 4096 = 256MB，统计 MiB/s（pipe-sender / pipe-receiver）
#   2. 延迟：  8B ping-pong × 100000 轮，统计 RTT µs/轮（pipe-latency-client / pipe-latency-server）
#
# 用法：
#   ./bench_pipe.sh                 # 吞吐量 + 延迟
#   ./bench_pipe.sh --throughput    # 仅吞吐量
#   ./bench_pipe.sh --latency       # 仅延迟
#   ./bench_pipe.sh --no-build      # 跳过自动编译
#   RECV_CPU=14 SEND_CPU=15 ./bench_pipe.sh   # 覆盖绑核

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${BASE:-$ROOT/target/release}"
PIPE_RECV="${PIPE_RECV:-$BASE/pipe-receiver}"
PIPE_SEND="${PIPE_SEND:-$BASE/pipe-sender}"
LAT_SERVER="${LAT_SERVER:-$BASE/pipe-latency-server}"
LAT_CLIENT="${LAT_CLIENT:-$BASE/pipe-latency-client}"
RECV_CPU="${RECV_CPU:-14}"
SEND_CPU="${SEND_CPU:-15}"

RUN_THROUGHPUT=1
RUN_LATENCY=1
SKIP_BUILD=0
PIDS=()

FIFOS="/tmp/uintr-pipe.fifo /tmp/uintr-pipe.sock /tmp/uintr-pipe-lat-1.fifo /tmp/uintr-pipe-lat-2.fifo /tmp/uintr-pipe-lat.sock"

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    rm -f $FIFOS
}
trap cleanup EXIT

# ── 参数解析 ────────────────────────────────────────────────
while [ $# -gt 0 ]; do
    case "$1" in
        --throughput) RUN_LATENCY=0 ;;
        --latency)    RUN_THROUGHPUT=0 ;;
        --no-build)   SKIP_BUILD=1 ;;
        -h|--help)
            echo "Usage: $0 [--throughput] [--latency] [--no-build]"
            echo "Env:  RECV_CPU SEND_CPU BASE PIPE_RECV PIPE_SEND LAT_SERVER LAT_CLIENT"
            exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
    shift
done

# ── 编译 ────────────────────────────────────────────────────
if [ "$SKIP_BUILD" != 1 ]; then
    echo "── Building workspace (release) ──"
    (cd "$ROOT" && cargo build --release --workspace) 2>&1 | tail -3
fi
for b in "$PIPE_RECV" "$PIPE_SEND" "$LAT_SERVER" "$LAT_CLIENT"; do
    [ -x "$b" ] || { echo "ERROR: 二进制不存在: $b" >&2; exit 1; }
done

# 在 RECV_CPU 后台启动对端进程，返回 pid
start_peer() { # $1=bin  $2=日志文件
    taskset -c "$RECV_CPU" "$1" >"$2" 2>&1 &
    echo $!
}

# 等待对端自然退出（最多 ~10s），未退则强杀
wait_peer() { # $1=pid
    local pid="$1" i
    for i in $(seq 1 200); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.05
    done
    if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
    wait "$pid" 2>/dev/null || true
}

# 打印并清理对端日志
show_log() { # $1=日志文件  $2=标题
    echo "── $2 日志 ──"
    cat "$1" 2>/dev/null || echo "(无输出)"
    rm -f "$1"
}

# ── 吞吐量测试 ───────────────────────────────────────────────
run_throughput() {
    echo ""
    echo "═══ 吞吐量测试: 64KB × 4096 = 256MB ═══"
    echo ""
    rm -f /tmp/uintr-pipe.fifo /tmp/uintr-pipe.sock

    local log=/tmp/pipe-recv.log
    local pid
    pid=$(start_peer "$PIPE_RECV" "$log")
    sleep 0.5

    # 发送方（前台）：写完 256MB 后等接收方 ack，二者先后退出
    taskset -c "$SEND_CPU" "$PIPE_SEND" 2>&1
    wait_peer "$pid"
    show_log "$log" "receiver"
}

# ── 延迟测试 ─────────────────────────────────────────────────
run_latency() {
    echo ""
    echo "═══ 延迟测试: 8B ping-pong, 2000 warmup + 100000 rounds ═══"
    echo ""
    rm -f /tmp/uintr-pipe-lat-1.fifo /tmp/uintr-pipe-lat-2.fifo /tmp/uintr-pipe-lat.sock

    local log=/tmp/pipe-lat-server.log
    local pid
    pid=$(start_peer "$LAT_SERVER" "$log")
    sleep 0.5

    taskset -c "$SEND_CPU" "$LAT_CLIENT" 2>&1
    wait_peer "$pid"
    show_log "$log" "server"
}

# ── 执行 ─────────────────────────────────────────────────────
if [ "$RUN_THROUGHPUT" = 1 ]; then
    run_throughput
fi
if [ "$RUN_LATENCY" = 1 ]; then
    run_latency
fi

echo ""
echo "── Done ──"
