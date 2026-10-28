#!/bin/bash
# 系统调用统计：strace -f -c 统计一次突发（burst 条请求）的 syscall 次数，
# 并按请求数折算。多轮可稀释进程启动摊销（execve/openat/statx 等约 50 次）。
#
# 用法：
#   ./syscall.sh <name> <binary> <burst> [rounds] [-- daemon 额外参数...]
#
# 示例：
#   ./syscall.sh old    ./target/release/uart-matmul 32 5 -- --mode crawler
#   ./syscall.sh thread /tmp/cmp/thread/crawler-daemon 32 5
#
# 环境变量：PORT=9091（与 bench.sh 的 9090 分开，避免互相干扰）
#           SRV=127.0.0.1:19092
#
# 注意：
#   1. 被测二进制应为 verbose-log 关闭的构建（见 build.sh），否则日志 write
#      会混入统计。
#   2. ptrace 会使每次系统调用停两次，wall 数据在 strace 下不可用，只取计数。
set -u

if [ $# -lt 3 ]; then
    echo "用法: $0 <name> <binary> <burst> [rounds] [-- daemon 额外参数...]" >&2
    exit 2
fi

NAME=$1; BIN=$2; BURST=$3; shift 3
ROUNDS=1
if [ "${1:-}" != "" ] && [ "${1:-}" != "--" ] && [ "${1:-}" -eq "${1:-}" ] 2>/dev/null; then
    ROUNDS=$1; shift
fi
EXTRA=()
if [ "${1:-}" = "--" ]; then shift; EXTRA=("$@"); fi

PORT=${PORT:-9091}
SRV=${SRV:-127.0.0.1:19092}
TMP=${TMPDIR:-/tmp}
ERR=$TMP/syscall_${NAME}.err
TRACE=$TMP/syscall_${NAME}.trace
rm -f "$ERR" "$TRACE"

daemon_pid() { ss -ltnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K\d+' | head -1; }
cleanup() {
    p=$(daemon_pid); [ -n "$p" ] && kill -9 "$p" 2>/dev/null
    for _ in $(seq 1 50); do
        ss -ltn 2>/dev/null | grep -q ":$PORT " || return 0
        sleep 0.1
    done
}

if [ ! -x "$BIN" ]; then
    echo "错误: 二进制不存在或不可执行: $BIN" >&2
    exit 1
fi
# syscall 计数必须关闭详细日志，否则日志的 write 会混入统计
if strings "$BIN" 2>/dev/null | grep -q '\[DAEMON\] req#'; then
    echo "警告: $BIN 含详细日志字符串（可能用 --features verbose-log 构建），" >&2
    echo "      syscall 计数会被日志 write 污染；请用 build.sh 默认构建。" >&2
fi

cleanup
strace -f -c -o "$TRACE" "$BIN" --listen "127.0.0.1:$PORT" "${EXTRA[@]}" >"$ERR" 2>&1 &
SPID=$!
for _ in $(seq 1 200); do
    ss -ltn 2>/dev/null | grep -q ":$PORT " && break
    sleep 0.05
done
sleep 0.3

r=0
while [ "$r" -lt "$ROUNDS" ]; do
    before=$(grep -c '\[BATCH\]' "$ERR" 2>/dev/null || true)
    yes "$SRV /" | head -n "$BURST" | nc -N -w5 127.0.0.1 "$PORT" >/dev/null 2>&1
    waited=0
    while [ "$waited" -le 200 ]; do
        now=$(grep -c '\[BATCH\]' "$ERR" 2>/dev/null || true)
        [ "$now" -gt "$before" ] && break
        sleep 0.1
        waited=$((waited + 1))
    done
    r=$((r + 1))
done

# 先 TERM daemon，strace 随被跟踪进程退出后输出 -c 汇总
dp=$(daemon_pid); [ -n "$dp" ] && kill -TERM "$dp" 2>/dev/null
for _ in $(seq 1 100); do kill -0 "$SPID" 2>/dev/null || break; sleep 0.1; done
kill -9 "$SPID" 2>/dev/null; wait "$SPID" 2>/dev/null
cleanup

TOTAL=$(awk '$NF == "total" {print $4}' "$TRACE")
REQ=$((BURST * ROUNDS))
echo "=== $NAME: burst=$BURST rounds=$ROUNDS requests=$REQ total_syscalls=$TOTAL ==="
if [ -n "$TOTAL" ] && [ "$REQ" -gt 0 ]; then
    awk -v t="$TOTAL" -v r="$REQ" 'BEGIN { printf "每请求: %.2f 次\n", t/r }'
fi
echo "--- syscall 明细（strace -c 原始表，按耗时降序）---"
# 直接输出原表：strace 的 %time 列可能为空，自行切列容易错位
cat "$TRACE"
echo "注：多轮可稀释进程启动摊销（execve/openat/statx 等约 50 次）；"
echo "    ptrace 会让每次系统调用停两次，wall 数据在此口径下不可用。"
echo "完整汇总: $TRACE"
