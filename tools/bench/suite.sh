#!/bin/bash
# 四方案 × 多负载 批量墙钟基准（论文复测用）。
#
# 每个方案开跑前等 TIME_WAIT 降到阈值以下：上一方案残留的 TIME_WAIT 会
# 系统性拉高后续方案的连接建立耗时，跨方案污染曾把结论带偏。
# 每轮墙钟由 daemon 内部的 [BATCH] 计时给出（IN_FLIGHT 0→1 记起点、
# 归 0 打印），部分方案一批被拆成多段时按片段求和。
#
# 用法：
#   ./suite.sh [-- daemon 公共额外参数...]
#
# 环境变量：
#   LOADS="32:100 200:20 1000:5"     负载表，每项 burst:rounds
#   BIN_OLD / BIN_RAW / BIN_THREAD / BIN_POOL   各方案二进制（空则跳过该方案）
#   OLD_ARGS="--mode crawler"        old 基线额外的启动参数
#   PORT=9090   SRV=127.0.0.1:19092
#   TW_MAX=500   TIME_WAIT 阈值
#   OUT=/tmp/suite_result.csv
#
# 注意：二进制应为 verbose-log 关闭的构建，否则日志 write 污染统计。
set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOADS=${LOADS:-"32:100 200:20 1000:5"}
BIN_OLD=${BIN_OLD:-/tmp/gen1/uart-matmul}
BIN_RAW=${BIN_RAW:-$ROOT/target/bench/raw/release/crawler-daemon}
BIN_THREAD=${BIN_THREAD:-$ROOT/target/bench/thread/release/crawler-daemon}
BIN_POOL=${BIN_POOL:-$ROOT/target/bench/pool/release/crawler-daemon}
OLD_ARGS=${OLD_ARGS:---mode crawler}
PORT=${PORT:-9090}
SRV=${SRV:-127.0.0.1:19092}
TW_MAX=${TW_MAX:-500}
OUT=${OUT:-/tmp/suite_result.csv}
TMP=${TMPDIR:-/tmp}

EXTRA=()
if [ "${1:-}" = "--" ]; then shift; EXTRA=("$@"); fi

: > "$OUT"
DPID=""

wait_tw_clear() {
    local i n
    for i in $(seq 1 180); do
        n=$(ss -tan 2>/dev/null | grep -c 'TIME-WAIT')
        [ "$n" -lt "$TW_MAX" ] && return 0
        sleep 0.5
    done
    echo "WARN: TIME_WAIT 仍有 $(ss -tan | grep -c 'TIME-WAIT') 条" >> "$OUT"
}

cpu_ticks() { awk '{print $14+$15}' "/proc/$1/stat" 2>/dev/null || echo 0; }

start() {  # name binary extra_args...
    local name=$1 bin=$2; shift 2
    "$bin" --listen "127.0.0.1:$PORT" "$@" >>"$TMP/suite_$name.err" 2>&1 &
    DPID=$!
    local i
    for i in $(seq 1 200); do
        ss -ltn 2>/dev/null | grep -q ":$PORT " && break
        sleep 0.02
    done
    sleep 0.1
}

run() {  # name burst rounds binary extra_args...
    local name=$1 burst=$2 rounds=$3 bin=$4; shift 4
    [ -x "$bin" ] || { echo "跳过 $name（二进制不存在: $bin）" >> "$OUT"; return; }
    local hangs=0 r=1
    rm -f "$TMP"/suite_$name.err*
    start "$name" "$bin" "$@"
    local c0; c0=$(cpu_ticks "$DPID")
    while [ "$r" -le "$rounds" ]; do
        local before; before=$(grep -c '\[BATCH\]' "$TMP/suite_$name.err" 2>/dev/null || true)
        yes "$SRV /" | head -n "$burst" | nc -N -w5 127.0.0.1 "$PORT" >/dev/null 2>&1
        local t=0 progressed=0
        while true; do
            local now; now=$(grep -c '\[BATCH\]' "$TMP/suite_$name.err" 2>/dev/null || true)
            if [ "$now" -gt "$before" ]; then progressed=1; break; fi
            sleep 0.05; t=$((t + 1))
            if [ "$t" -gt 200 ]; then
                hangs=$((hangs + 1))
                kill -9 "$DPID" 2>/dev/null; wait "$DPID" 2>/dev/null
                mv "$TMP/suite_$name.err" "$TMP/suite_$name.err.h$hangs"
                start "$name" "$bin" "$@"; c0=$(cpu_ticks "$DPID")
                break
            fi
        done
        if [ "$progressed" = 1 ]; then
            local w
            w=$(tail -n +$((before + 1)) "$TMP/suite_$name.err" \
                | grep -oP '\[BATCH\] wall=\K\d+' | paste -sd+ | bc)
            echo "$name,burst=$burst,round=$r,wall_us=$w" >> "$OUT"
            r=$((r + 1))
        fi
    done
    local c1; c1=$(cpu_ticks "$DPID")
    echo "$name,burst=$burst,cpu_ticks=$((c1 - c0)),hangs=$hangs" >> "$OUT"
    kill -TERM "$DPID" 2>/dev/null; wait "$DPID" 2>/dev/null
}

if ! ss -ltn 2>/dev/null | grep -q ":${SRV##*:} "; then
    echo "警告: 目标端 $SRV 未监听，请先启动测试服务端" >&2
fi

for LOAD in $LOADS; do
    B=${LOAD%%:*}; R=${LOAD##*:}
    for V in old raw thread pool; do
        wait_tw_clear
        case $V in
            old)    run old    "$B" "$R" "$BIN_OLD"    $OLD_ARGS ${EXTRA[@]+"${EXTRA[@]}"} ;;
            raw)    run raw    "$B" "$R" "$BIN_RAW"    ${EXTRA[@]+"${EXTRA[@]}"} ;;
            thread) run thread "$B" "$R" "$BIN_THREAD" ${EXTRA[@]+"${EXTRA[@]}"} ;;
            pool)   run pool   "$B" "$R" "$BIN_POOL"   ${EXTRA[@]+"${EXTRA[@]}"} ;;
        esac
    done
done
echo "=== ALL DONE ===" >> "$OUT"
echo "结果: $OUT"
