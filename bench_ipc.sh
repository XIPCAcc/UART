#!/usr/bin/env bash
# IPC 对比测试脚本：UINTR (uart) vs tokio (ipc-tokio) 共享内存异步 IPC
#
# 自动完成：
#   1. 接收方绑定 RECV_CPU 后台启动，等待 socket 就绪
#   2. 发送方绑定 SEND_CPU 运行，提取 Message rate
#   3. 每个配置重复 RUNS 次，取中位数
#   4. 输出对比表格
#
# 依赖：两个项目需先 build --release
#   cd /home/zwp/uart && cargo build --release
#   cd /home/zwp/ipc-tokio && cargo build --release
#
# 用法：
#   ./bench_ipc.sh                         # 默认: 3 次, 100000 条, sizes=256/1024/8192/65536
#   ./bench_ipc.sh --runs 5 --count 50000 --sizes "256 8192"
#   RECV_CPU=14 SEND_CPU=15 ./bench_ipc.sh # 覆盖绑核（默认 14/15，最高频核）

set -u

UART_BIN="${UART_BIN:-/home/zwp/uart/target/release/uart-matmul}"
TOKIO_BIN="${TOKIO_BIN:-/home/zwp/ipc-tokio/target/release/ipc-tokio}"
RECV_CPU="${RECV_CPU:-14}"
SEND_CPU="${SEND_CPU:-15}"

RUNS=3
COUNT=100000
SIZES="64 128 256 512 1024 4096 8192 16384 32768 65536"
KEEP_TURBO=0
SKIP_BUILD=0

# 禁止 turbo（消除睿频抖动；默认开启，--keep-turbo 跳过）
NO_TURBO_FILE=/sys/devices/system/cpu/intel_pstate/no_turbo
TURBO_ORIG=""
# 记录所有启动的接收方 pid，供退出时清理
RECV_PIDS=()

# 清理现场：杀掉残留接收方、删除 socket 文件、恢复 turbo
cleanup() {
    for pid in "${RECV_PIDS[@]:-}"; do
        kill -KILL "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
    done
    rm -f /tmp/uintr-ipc.sock /tmp/ipc-tokio.sock
    turbo_restore
}
trap cleanup EXIT

turbo_off() {
    if [[ ! -e "$NO_TURBO_FILE" ]]; then
        echo "WARN: 未检测到 intel_pstate（$NO_TURBO_FILE 不存在），跳过禁止 turbo" >&2
        return 1
    fi
    TURBO_ORIG=$(cat "$NO_TURBO_FILE")
    if echo 1 > "$NO_TURBO_FILE" 2>/dev/null; then
        return 0
    elif command -v sudo >/dev/null 2>&1 && echo 1 | sudo -n tee "$NO_TURBO_FILE" >/dev/null 2>&1; then
        return 0
    fi
    echo "WARN: 无法写 $NO_TURBO_FILE（需要 root），继续测试（结果可能受 turbo 影响）" >&2
    return 1
}

turbo_restore() {
    if [[ -n "$TURBO_ORIG" ]]; then
        echo "$TURBO_ORIG" > "$NO_TURBO_FILE" 2>/dev/null \
            || echo "$TURBO_ORIG" | sudo -n tee "$NO_TURBO_FILE" >/dev/null 2>&1
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --runs)       RUNS="$2";  shift 2 ;;
        --count)      COUNT="$2"; shift 2 ;;
        --sizes)      SIZES="$2"; shift 2 ;;
        --keep-turbo) KEEP_TURBO=1; shift ;;
        --no-build)   SKIP_BUILD=1; shift ;;
        -h|--help)
            echo "Usage: $0 [--runs N] [--count N] [--sizes \"256 1024 8192 65536\"] [--keep-turbo] [--no-build]"
            echo "默认先自动 cargo build --release 两个项目（--no-build 跳过），并在测试期间禁止 turbo"
            echo "Env:  UART_BIN TOKIO_BIN RECV_CPU SEND_CPU"
            exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

# 自动编译（默认开启，--no-build 跳过）
if [[ "$SKIP_BUILD" -eq 0 ]]; then
    echo ">>> 编译 uart (release)..."
    (cd "$(dirname "$UART_BIN")/../.." && cargo build --release) 2>&1 | tail -3 || exit 1
    echo ">>> 编译 ipc-tokio (release)..."
    (cd "$(dirname "$TOKIO_BIN")/../.." && cargo build --release) 2>&1 | tail -3 || exit 1
fi

# 检查二进制
for b in "$UART_BIN" "$TOKIO_BIN"; do
    if [[ ! -x "$b" ]]; then
        echo "ERROR: not found / not executable: $b" >&2
        echo "请先编译或确认二进制路径（Env: UART_BIN TOKIO_BIN）" >&2
        exit 1
    fi
done

# 中位数（过滤非数字行，防御某次运行失败）
median() {
    sort -n | awk 'NR==1 {m=$1} {a[NR]=$1}
        END {if (NR%2) print a[(NR+1)/2]; else print (a[NR/2]+a[NR/2+1])/2}'
}

# 运行一次：$1=uintr|tokio  $2=msg_size
# 输出 "rate|data_intr|back_intr|s_sleep|r_sleep"：
#   rate      = Message rate (msg/s)
#   data_intr = 发送方发出的 senduipi 次数（数据通知方向）
#   back_intr = 接收方发出的 senduipi 次数（背压通知方向）
#   s_sleep   = 发送方睡眠后被唤醒的次数
#   r_sleep   = 接收方睡眠后被唤醒的次数
run_once() {
    local scheme="$1" size="$2" bin sock recv_mode sender_args recv_pid waited
    local out rate intr back s_sleep r_sleep recv_log

    if [[ "$scheme" == "uintr" ]]; then
        bin="$UART_BIN"; sock="/tmp/uintr-ipc.sock"
        recv_mode="--mode ipc-receiver"
        sender_args="--mode ipc-sender -c $COUNT --msg-size $size"
    else
        bin="$TOKIO_BIN"; sock="/tmp/ipc-tokio.sock"
        recv_mode="--mode receiver"
        sender_args="--mode sender -c $COUNT --msg-size $size"
    fi

    rm -f "$sock"
    recv_log="/tmp/ipc_recv_$$.log"
    rm -f "$recv_log"
    taskset -c "$RECV_CPU" "$bin" $recv_mode >"$recv_log" 2>&1 &
    recv_pid=$!
    RECV_PIDS+=("$recv_pid")

    # 等待 socket 就绪（最多 5s）
    waited=0
    while [[ ! -S "$sock" ]]; do
        sleep 0.05; waited=$((waited + 50))
        if [[ $waited -ge 5000 ]]; then
            echo "ERROR: receiver socket not ready" >&2
            kill -KILL "$recv_pid" 2>/dev/null
            wait "$recv_pid" 2>/dev/null
            return 1
        fi
    done

    # 跑发送方，捕获完整输出
    out=$(taskset -c "$SEND_CPU" "$bin" $sender_args 2>&1)
    rate=$(echo "$out" | grep "Message rate" | awk '{print $3}')
    intr=$(echo "$out" | grep "senduipi" | grep -oE '[0-9]+' | head -1)
    s_sleep=$(echo "$out" | grep "睡眠唤醒" | grep -oE '[0-9]+' | head -1)

    # 用 SIGINT 结束接收方（uart 与 tokio 都监听 INT），等它打印统计后退出
    kill -INT "$recv_pid" 2>/dev/null
    wait "$recv_pid" 2>/dev/null
    back=$(grep "senduipi" "$recv_log" 2>/dev/null | grep -oE '[0-9]+' | tail -1)
    r_sleep=$(grep "睡眠唤醒" "$recv_log" 2>/dev/null | grep -oE '[0-9]+' | tail -1)
    rm -f "$recv_log"

    [[ -z "$rate" ]] && { echo "ERROR: no Message rate captured" >&2; return 1; }
    echo "$rate|${intr:-0}|${back:-0}|${s_sleep:-0}|${r_sleep:-0}"
}

# 测试开始前禁止 turbo（退出时由 cleanup 恢复）
if [[ "$KEEP_TURBO" -eq 0 ]]; then
    turbo_off && echo "turbo 已禁止（测试结束后恢复为 $TURBO_ORIG）"
fi

echo "=== IPC 对比测试 ==="
echo "方案: UINTR(uart) vs tokio(ipc-tokio) | 核: recv=$RECV_CPU send=$SEND_CPU | runs=$RUNS count=$COUNT | turbo=$([ "$KEEP_TURBO" -eq 1 ] && echo on || echo off)"
echo ""
echo "列含义: sI=发送方中断数 rI=接收方中断数 sS=发送方睡眠次数 rS=接收方睡眠次数 (各3列=3轮)"

# 全 ASCII 表头：printf %-Ns 按字节填充，中文占 2 显示宽会导致表头与数据错位
printf "%-5s %-7s" "scheme" "size"
for ((i=1; i<=RUNS; i++)); do printf " %-8s" "run$i"; done
printf " %-10s %-9s" "med/s" "MiB/s"
for ((i=1; i<=RUNS; i++)); do printf " %-7s" "sI$i"; done
for ((i=1; i<=RUNS; i++)); do printf " %-7s" "rI$i"; done
for ((i=1; i<=RUNS; i++)); do printf " %-7s" "sS$i"; done
for ((i=1; i<=RUNS; i++)); do printf " %-7s" "rS$i"; done
printf "\n"
printf "%s\n" "---------------------------------------------------------------------------------------------------------------------------------------------------"

for size in $SIZES; do
    for scheme in uintr tokio; do
        rates=()
        for ((i=1; i<=RUNS; i++)); do
            r=$(run_once "$scheme" "$size")
            rates+=("$r")
        done
        med=$(printf "%s\n" "${rates[@]}" | cut -d'|' -f1 | grep -E '^[0-9]+(\.[0-9]+)?$' | median)
        [[ -z "$med" ]] && med="N/A"

        # 分别收集每一轮的发送方/接收方中断数、睡眠次数
        intrs=(); backs=(); s_sleeps=(); r_sleeps=()
        for r in "${rates[@]}"; do
            intrs+=("$(echo "$r" | cut -d'|' -f2)")
            backs+=("$(echo "$r" | cut -d'|' -f3)")
            s_sleeps+=("$(echo "$r" | cut -d'|' -f4)")
            r_sleeps+=("$(echo "$r" | cut -d'|' -f5)")
        done

        printf "%-5s %-7s" "$scheme" "$size"
        for r in "${rates[@]}"; do
            rate_f=$(echo "$r" | cut -d'|' -f1)
            if [[ "$rate_f" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                printf " %-8.0f" "$rate_f"
            else
                printf " %-8s" "FAIL"
            fi
        done
        if [[ "$med" == "N/A" ]]; then
            printf " %-10s %-9s" "N/A" "N/A"
        else
            mib=$(awk -v r="$med" -v s="$size" 'BEGIN { printf "%.1f", r * s / 1048576 }')
            printf " %-10.0f %-9s" "$med" "$mib"
        fi
        for v in "${intrs[@]:-}"; do printf " %-7s" "${v:-0}"; done
        for v in "${backs[@]:-}"; do printf " %-7s" "${v:-0}"; done
        for v in "${s_sleeps[@]:-}"; do printf " %-7s" "${v:-0}"; done
        for v in "${r_sleeps[@]:-}"; do printf " %-7s" "${v:-0}"; done
        printf "\n"
    done
done

echo ""
echo "完成。如需对比 Average duration，可加 --count 减小压力后单独跑并查看完整输出。"
