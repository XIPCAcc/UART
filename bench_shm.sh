#!/usr/bin/env bash
# UINTR 共享内存（shm）吞吐量测试
#
# 测试内容：
#   吞吐量：64KB chunk × 4096 = 256MB，统计 MiB/s（shm-sender / shm-receiver）
#   起跑同步：sender 先经通道发送 GO 标记，双方以 GO 到达时刻对齐计时（与 bench_pipe 一致）
#
# 用法：
#   ./bench_shm.sh                 # 运行吞吐量测试
#   ./bench_shm.sh --no-build      # 跳过自动编译
#   RECV_CPU=14 SEND_CPU=15 ./bench_shm.sh   # 覆盖绑核

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="${BASE:-$ROOT/target/release}"
SHM_RECV="${SHM_RECV:-$BASE/shm-receiver}"
SHM_SEND="${SHM_SEND:-$BASE/shm-sender}"
RECV_CPU="${RECV_CPU:-14}"
SEND_CPU="${SEND_CPU:-15}"

SKIP_BUILD=0
PIDS=()

FILES="/tmp/uintr-shm.sock"

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    rm -f $FILES
}
trap cleanup EXIT

# ── 参数解析 ────────────────────────────────────────────────
while [ $# -gt 0 ]; do
    case "$1" in
        --no-build) SKIP_BUILD=1 ;;
        -h|--help)
            echo "Usage: $0 [--no-build]"
            echo "Env:  RECV_CPU SEND_CPU BASE SHM_RECV SHM_SEND"
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
for b in "$SHM_RECV" "$SHM_SEND"; do
    [ -x "$b" ] || { echo "ERROR: 二进制不存在: $b" >&2; exit 1; }
done

# 在 RECV_CPU 后台启动接收方，返回 pid
start_recv() { # $1=日志文件
    taskset -c "$RECV_CPU" "$SHM_RECV" >"$1" 2>&1 &
    echo $!
}

# 等待接收方自然退出（最多 ~10s），未退则强杀
wait_recv() { # $1=pid
    local pid="$1" i
    for i in $(seq 1 200); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.05
    done
    if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
    wait "$pid" 2>/dev/null || true
}

# ── 吞吐量测试 ───────────────────────────────────────────────
run_throughput() {
    echo ""
    echo "═══ 吞吐量测试: 64KB × 4096 = 256MB ═══"
    echo ""
    rm -f /tmp/uintr-shm.sock

    local log=/tmp/shm-recv.log
    local pid
    pid=$(start_recv "$log")
    sleep 0.5

    # 发送方（前台）：发 GO 标记后写 256MB
    taskset -c "$SEND_CPU" "$SHM_SEND" 2>&1
    wait_recv "$pid"

    echo "── receiver 日志 ──"
    cat "$log" 2>/dev/null || echo "(无输出)"
    rm -f "$log"
}

# ── 执行 ─────────────────────────────────────────────────────
run_throughput

echo ""
echo "── Done ──"
