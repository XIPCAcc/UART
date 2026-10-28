#!/bin/bash
# 批墙钟测量：发 burst 条请求 × rounds 轮，取 daemon 内部 [BATCH] 计时。
#
# 墙钟由 daemon 自身打印（IN_FLIGHT 0→1 记批起点、归 0 打印 wall），
# 不经外部观测，无 harness 判定常数。
#
# 用法：
#   ./bench.sh [burst] [rounds] [-- daemon 额外参数...]
#
# 环境变量：
#   BIN=target/release/crawler-daemon   被测二进制（应用 build.sh 的默认构建，
#                                       verbose-log 必须关闭，否则 write 污染统计）
#   PORT=9090   SRV=127.0.0.1:19092     daemon 监听 / 爬虫目标
#   OUT=/tmp/bench_result.csv
set -u

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BURST=${1:-32}
ROUNDS=${2:-100}
BIN=${BIN:-$ROOT/target/release/crawler-daemon}
PORT=${PORT:-9090}
SRV=${SRV:-127.0.0.1:19092}
OUT=${OUT:-/tmp/bench_result.csv}
TMP=${TMPDIR:-/tmp}

shift $(( $# >= 1 ? 1 : 0 )) 2>/dev/null || true
shift $(( $# >= 1 ? 1 : 0 )) 2>/dev/null || true
EXTRA=()
if [ "${1:-}" = "--" ]; then shift; EXTRA=("$@"); fi

[ -x "$BIN" ] || { echo "ERROR: 二进制不存在: $BIN（先跑 ./build.sh）" >&2; exit 1; }
if strings "$BIN" | grep -q '\[DAEMON\] req#'; then
    echo "WARN: 该二进制带 verbose-log，日志 write 会污染统计" >&2
fi
if ! ss -ltn 2>/dev/null | grep -q ":${SRV##*:} "; then
    echo "WARN: 目标端 $SRV 未监听，所有任务将连接失败" >&2
fi

: > "$OUT"
rm -f "$TMP"/bench.err*

"$BIN" --listen "127.0.0.1:$PORT" ${EXTRA[@]+"${EXTRA[@]}"} >"$TMP/bench.err" 2>&1 &
DPID=$!
trap 'kill -9 "$DPID" 2>/dev/null' EXIT
for _ in $(seq 1 200); do
    ss -ltn 2>/dev/null | grep -q ":$PORT " && break
    sleep 0.02
done
sleep 0.1

hangs=0
for r in $(seq 1 "$ROUNDS"); do
    before=$(grep -c '\[BATCH\]' "$TMP/bench.err" 2>/dev/null || true)
    yes "$SRV /" | head -n "$BURST" | nc -N -w5 127.0.0.1 "$PORT" >/dev/null 2>&1
    t=0
    while true; do
        now=$(grep -c '\[BATCH\]' "$TMP/bench.err" 2>/dev/null || true)
        [ "$now" -gt "$before" ] && break
        sleep 0.05; t=$((t + 1))
        if [ "$t" -gt 200 ]; then
            hangs=$((hangs + 1))
            echo "WARN: 第 $r 轮挂起，重启 daemon" >&2
            kill -9 "$DPID" 2>/dev/null; wait "$DPID" 2>/dev/null
            "$BIN" --listen "127.0.0.1:$PORT" ${EXTRA[@]+"${EXTRA[@]}"} >>"$TMP/bench.err" 2>&1 &
            DPID=$!
            sleep 0.3
            continue 2
        fi
    done
    # 一批可能被拆成多段（批内归零），按片段求和
    w=$(tail -n +$((before + 1)) "$TMP/bench.err" \
        | grep -oP '\[BATCH\] wall=\K\d+' | paste -sd+ | bc)
    echo "burst=$BURST,round=$r,wall_us=$w" >> "$OUT"
done

kill -TERM "$DPID" 2>/dev/null; wait "$DPID" 2>/dev/null
trap - EXIT

# 统计（mawk 无 asort，用 sort -n 排序后取位次）
mapfile -t v < <(grep -oP 'wall_us=\K\d+' "$OUT" | sort -n)
n=${#v[@]}
if [ "$n" -gt 0 ]; then
    med=${v[$(( (n - 1) / 2 ))]}
    p25=${v[$(( (n - 1) / 4 ))]}
    p75=${v[$(( (3 * (n - 1)) / 4 ))]}
    sum=$(IFS=+; echo "$((${v[*]}))")
    echo "burst=$BURST rounds=$n hangs=$hangs"
    echo "median=${med}us  mean=$((sum / n))us  min=${v[0]}us  max=${v[$((n-1))]}us  p25=${p25}us  p75=${p75}us"
else
    echo "无有效数据（hangs=$hangs）"
fi
echo "结果: $OUT"
