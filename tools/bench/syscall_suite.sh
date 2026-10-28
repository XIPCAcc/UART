#!/bin/bash
# 四方案 strace -f -c 系统调用计数（论文复测用）。
#
# 正确退出顺序：先 TERM daemon（用 ss 找 pid），strace 随被跟踪进程退出后
# 自动输出 -c 汇总；直接 kill strace 会丢失统计。
# ptrace 会让每次系统调用停两次，此口径下 wall 不可用，只取计数。
#
# 用法：
#   ./syscall_suite.sh [burst] [-- daemon 公共额外参数...]
#
# 环境变量：
#   BIN_OLD / BIN_RAW / BIN_THREAD / BIN_POOL   各方案二进制（空则跳过）
#   OLD_ARGS="--mode crawler"
#   PORT=9091（与 suite.sh 的 9090 分开）
#   SRV=127.0.0.1:19092
#   OUT=/tmp/syscall_suite.txt
#
# 注意：二进制应为 verbose-log 关闭的构建，否则日志 write 混入计数。
set -u

BURST=${1:-200}
[ "${1:-}" = "--" ] && BURST=200
shift || true
EXTRA=()
if [ "${1:-}" = "--" ]; then shift; EXTRA=("$@"); fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN_OLD=${BIN_OLD:-/tmp/gen1/uart-matmul}
BIN_RAW=${BIN_RAW:-$ROOT/target/bench/raw/release/crawler-daemon}
BIN_THREAD=${BIN_THREAD:-$ROOT/target/bench/thread/release/crawler-daemon}
BIN_POOL=${BIN_POOL:-$ROOT/target/bench/pool/release/crawler-daemon}
OLD_ARGS=${OLD_ARGS:---mode crawler}
PORT=${PORT:-9091}
SRV=${SRV:-127.0.0.1:19092}
OUT=${OUT:-/tmp/syscall_suite.txt}
TMP=${TMPDIR:-/tmp}

: > "$OUT"

daemon_pid() { ss -ltnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K\d+' | head -1; }
cleanup() {
    local p
    p=$(daemon_pid); [ -n "$p" ] && kill -9 "$p" 2>/dev/null
    for _ in $(seq 1 50); do
        ss -ltn 2>/dev/null | grep -q ":$PORT " || return 0
        sleep 0.1
    done
}

one() {  # name binary extra_args...
    local name=$1 bin=$2; shift 2
    if [ ! -x "$bin" ]; then
        echo "=== $name: 跳过（二进制不存在）===" >> "$OUT"
        return
    fi
    cleanup
    rm -f "$TMP"/ss_$name.err
    strace -f -c -o "$TMP/ss_$name.trace" "$bin" --listen "127.0.0.1:$PORT" "$@" \
        >"$TMP/ss_$name.err" 2>&1 &
    local spid=$!
    local i
    for i in $(seq 1 200); do
        ss -ltn 2>/dev/null | grep -q ":$PORT " && break
        sleep 0.05
    done
    sleep 0.3
    yes "$SRV /" | head -n "$BURST" | nc -N -w5 127.0.0.1 "$PORT" >/dev/null 2>&1
    for i in $(seq 1 200); do
        grep -q '\[BATCH\]' "$TMP/ss_$name.err" 2>/dev/null && break
        sleep 0.1
    done
    local wall
    wall=$(grep -oP '\[BATCH\] wall=\K\d+' "$TMP/ss_$name.err" 2>/dev/null | tail -1)
    local dp
    dp=$(daemon_pid)
    [ -n "$dp" ] && kill -TERM "$dp" 2>/dev/null
    for i in $(seq 1 100); do kill -0 "$spid" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$spid" 2>/dev/null; wait "$spid" 2>/dev/null
    echo "=== $name (burst=$BURST, strace wall=${wall}us) ===" >> "$OUT"
    cat "$TMP/ss_$name.trace" >> "$OUT"
    cleanup
    sleep 1
}

if ! ss -ltn 2>/dev/null | grep -q ":${SRV##*:} "; then
    echo "警告: 目标端 $SRV 未监听，请先启动测试服务端" >&2
fi

one old    "$BIN_OLD"    $OLD_ARGS ${EXTRA[@]+"${EXTRA[@]}"}
one raw    "$BIN_RAW"    ${EXTRA[@]+"${EXTRA[@]}"}
one thread "$BIN_THREAD" ${EXTRA[@]+"${EXTRA[@]}"}
one pool   "$BIN_POOL"   ${EXTRA[@]+"${EXTRA[@]}"}
echo "=== STRACE DONE ===" >> "$OUT"
echo "结果: $OUT"
