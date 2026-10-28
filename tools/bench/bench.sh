#!/bin/bash
# 突发批墙钟基准：用 daemon 内部的 [BATCH] 计时（IN_FLIGHT 0→1 记起点、
# 归 0 打印墙钟），避免外部「CPU 静默判定」引入的判定常数污染。
#
# 用法：
#   ./bench.sh <name> <binary> <burst> <rounds> [-- daemon 额外参数...]
#
# 示例：
#   ./bench.sh old  ./target/release/uart-matmul 32 100 -- --mode crawler
#   ./bench.sh raw  /tmp/cmp/raw/crawler-daemon   32 100
#
# 环境变量：
#   PORT=9090               daemon 控制端口
#   SRV=127.0.0.1:19092     爬虫目标地址（测试用 echo 服务端）
#   TW_WAIT=1               开跑前等待 TIME_WAIT 降到 TW_MAX 以下
#   TW_MAX=500              TIME_WAIT 阈值
#   HANG_SECS=10            单轮无进展判挂起（超时则 kill 并重启 daemon）
#
# 注意：被测二进制应使用默认构建（verbose-log 关闭），见 build.sh。
set -u

if [ $# -lt 4 ]; then
    echo "用法: $0 <name> <binary> <burst> <rounds> [-- daemon 额外参数...]" >&2
    exit 2
fi

NAME=$1; BIN=$2; BURST=$3; ROUNDS=$4; shift 4
EXTRA=()
if [ "${1:-}" = "--" ]; then shift; EXTRA=("$@"); fi

PORT=${PORT:-9090}
SRV=${SRV:-127.0.0.1:19092}
TW_WAIT=${TW_WAIT:-1}
TW_MAX=${TW_MAX:-500}
HANG_SECS=${HANG_SECS:-10}

TMP=${TMPDIR:-/tmp}
ERR=$TMP/bench_${NAME}.err
CSV=$TMP/bench_${NAME}.csv
rm -f "$ERR" "$CSV"
: > "$CSV"

daemon_pid() { ss -ltnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K\d+' | head -1; }
cpu_ticks()  { awk '{print $14+$15}' "/proc/$1/stat" 2>/dev/null || echo 0; }

wait_tw_clear() {
    [ "$TW_WAIT" = "1" ] || return 0
    for _ in $(seq 1 180); do
        n=$(ss -tan 2>/dev/null | grep -c 'TIME-WAIT')
        [ "$n" -lt "$TW_MAX" ] && return 0
        sleep 0.5
    done
    echo "WARN: TIME_WAIT 仍有 $(ss -tan | grep -c 'TIME-WAIT') 条" >&2
}

start_daemon() {
    "$BIN" --listen "127.0.0.1:$PORT" "${EXTRA[@]}" >>"$ERR" 2>&1 &
    DPID=$!
    for _ in $(seq 1 200); do
        ss -ltn 2>/dev/null | grep -q ":$PORT " && break
        sleep 0.05
    done
    sleep 0.1
}

if [ ! -x "$BIN" ]; then
    echo "错误: 二进制不存在或不可执行: $BIN" >&2
    exit 1
fi
# 基准口径必须关闭详细日志：verbose-log 构建的 write/stderr 会污染统计
if strings "$BIN" 2>/dev/null | grep -q '\[DAEMON\] req#'; then
    echo "警告: $BIN 含详细日志字符串（可能用 --features verbose-log 构建），" >&2
    echo "      基准数据会被日志的 write 与 stderr 阻塞污染；请用 build.sh 默认构建。" >&2
fi
if ! ss -ltn 2>/dev/null | grep -q ":${SRV##*:} "; then
    echo "警告: 目标端 $SRV 未监听，请先启动测试服务端" >&2
fi

wait_tw_clear
start_daemon
C0=$(cpu_ticks "$DPID")

hangs=0
r=1
while [ "$r" -le "$ROUNDS" ]; do
    before=$(grep -c '\[BATCH\]' "$ERR" 2>/dev/null || true)
    yes "$SRV /" | head -n "$BURST" | nc -N -w5 127.0.0.1 "$PORT" >/dev/null 2>&1

    waited=0
    progressed=0
    max_wait=$((HANG_SECS * 20))
    while [ "$waited" -le "$max_wait" ]; do
        now=$(grep -c '\[BATCH\]' "$ERR" 2>/dev/null || true)
        if [ "$now" -gt "$before" ]; then progressed=1; break; fi
        sleep 0.05
        waited=$((waited + 1))
    done

    if [ "$progressed" = "1" ]; then
        # 该轮新增的 [BATCH] 行求和（部分方案可能把一批拆成多段）
        w=$(tail -n +$((before + 1)) "$ERR" | grep -oP '\[BATCH\] wall=\K\d+' | paste -sd+ | bc)
        echo "$NAME,burst=$BURST,round=$r,wall_us=$w" >> "$CSV"
        r=$((r + 1))
    else
        hangs=$((hangs + 1))
        echo "WARN: $NAME 第 $r 轮 ${HANG_SECS}s 无进展，判挂起，重启 daemon" >&2
        kill -9 "$DPID" 2>/dev/null; wait "$DPID" 2>/dev/null
        mv "$ERR" "$ERR.hang$hangs"
        start_daemon
        C0=$(cpu_ticks "$DPID")
    fi
done

C1=$(cpu_ticks "$DPID")
kill -TERM "$DPID" 2>/dev/null
wait "$DPID" 2>/dev/null

echo "$NAME,burst=$BURST,cpu_ticks=$((C1 - C0)),hangs=$hangs"

echo "--- 汇总（$NAME, burst=$BURST, rounds=$ROUNDS, hangs=$hangs）---"
awk -F'wall_us=' '
/wall_us=/ { v[n++] = $2 + 0 }
END {
    if (n == 0) { print "无数据"; exit }
    for (i = 0; i < n; i++)
        for (j = i + 1; j < n; j++)
            if (v[j] < v[i]) { t = v[i]; v[i] = v[j]; v[j] = t }
    s = 0
    for (i = 0; i < n; i++) s += v[i]
    printf "median=%dus  mean=%dus  min=%dus  max=%dus  p25=%dus  p75=%dus\n",
        v[int(n/2)], s/n, v[0], v[n-1], v[int(n/4)], v[int(n*3/4)]
}' "$CSV"
echo "原始数据: $CSV"
