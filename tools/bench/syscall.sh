#!/bin/bash
# strace -f -c 系统调用计数。
#
# 注意：ptrace 会让每次系统调用停两次，此口径下 wall 不可用；
# -c 表的 % time / seconds / usecs/call 同样失真，只有 calls / errors 有效。
#
# 用法：
#   ./syscall.sh [burst] [rounds] [-- daemon 额外参数...]
#
# rounds 用于稀释进程启动摊销（execve/openat 等约 50 次），建议 ≥5。
#
# 环境变量：
#   BIN=target/release/crawler-daemon   被测二进制（verbose-log 必须关闭）
#   PORT=9091（与 bench.sh 的 9090 分开）
#   SRV=127.0.0.1:19092
set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BURST=${1:-32}
ROUNDS=${2:-5}
BIN=${BIN:-$ROOT/target/release/crawler-daemon}
PORT=${PORT:-9091}
SRV=${SRV:-127.0.0.1:19092}
TMP=${TMPDIR:-/tmp}

shift $(( $# >= 1 ? 1 : 0 )) 2>/dev/null || true
shift $(( $# >= 1 ? 1 : 0 )) 2>/dev/null || true
EXTRA=()
if [ "${1:-}" = "--" ]; then shift; EXTRA=("$@"); fi

[ -x "$BIN" ] || { echo "ERROR: 二进制不存在: $BIN（先跑 ./build.sh）" >&2; exit 1; }
if strings "$BIN" | grep -q '\[DAEMON\] req#'; then
    echo "WARN: 该二进制带 verbose-log，日志 write 会混入计数" >&2
fi
if ! ss -ltn 2>/dev/null | grep -q ":${SRV##*:} "; then
    echo "WARN: 目标端 $SRV 未监听，所有任务将连接失败" >&2
fi

rm -f "$TMP/syscall.trace" "$TMP/syscall.err"

strace -f -c -o "$TMP/syscall.trace" \
    "$BIN" --listen "127.0.0.1:$PORT" ${EXTRA[@]+"${EXTRA[@]}"} \
    >"$TMP/syscall.err" 2>&1 &
SPID=$!
for _ in $(seq 1 200); do
    ss -ltn 2>/dev/null | grep -q ":$PORT " && break
    sleep 0.05
done
sleep 0.3

for r in $(seq 1 "$ROUNDS"); do
    before=$(grep -c '\[BATCH\]' "$TMP/syscall.err" 2>/dev/null || true)
    yes "$SRV /" | head -n "$BURST" | nc -N -w5 127.0.0.1 "$PORT" >/dev/null 2>&1
    for _ in $(seq 1 200); do
        now=$(grep -c '\[BATCH\]' "$TMP/syscall.err" 2>/dev/null || true)
        [ "$now" -gt "$before" ] && break
        sleep 0.1
    done
done

# 先 TERM daemon：strace 随被跟踪进程退出后才输出 -c 汇总；
# 直接杀 strace 会丢统计
dp=$(ss -ltnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K\d+' | head -1)
[ -n "$dp" ] && kill -TERM "$dp" 2>/dev/null
for _ in $(seq 1 100); do kill -0 "$SPID" 2>/dev/null || break; sleep 0.1; done
kill -9 "$SPID" 2>/dev/null; wait "$SPID" 2>/dev/null

total=$(awk '$NF=="total"{print $(NF-2)}' "$TMP/syscall.trace")
reqs=$((BURST * ROUNDS))
echo "=== burst=$BURST rounds=$ROUNDS total_syscalls=${total:-?} (${reqs} 请求，约 $(echo "scale=1; ${total:-0}/$reqs" | bc)/请求) ==="
echo "--- strace -c 完整表 ---"
cat "$TMP/syscall.trace"
