# 基准测试脚本

对比四种 worker 策略的墙钟与系统调用开销：

| 变体 | 含义 | 构建方式 |
|---|---|---|
| `old` | 基线（commit `ed0725e` ） | 需从旧 commit 单独构建 |
| `raw` | 裸 `clone3` + 动态扩缩容 | `cargo build --release` |
| `thread` | `std::thread::spawn` + 动态扩缩容 | `--features workers-thread` |
| `pool` | 常驻线程池 + park/unpark | `--features workers-pool` |

## 脚本清单

| 脚本 | 作用 | 端口 |
|---|---|---|
| `build.sh` | 构建基准用二进制（默认关闭 verbose-log） | — |
| `bench.sh` | 单方案 × 单负载，批墙钟 | 9090 |
| `suite.sh` | 四方案 × 多负载，批墙钟 | 9090 |
| `syscall.sh` | 单方案 × 单负载，`strace -f -c` 计数 | 9091 |
| `syscall_suite.sh` | 四方案，`strace -f -c` 计数 | 9091 |

## 前置条件

**1. 测试目标服务**：daemon 会向 `127.0.0.1:19092` 发 HTTP 请求，必须先起测试服务端：

```bash
/tmp/echo &                  # 监听 127.0.0.1:19092
ss -ltn | grep 19092         # 确认在听
```

没有服务端时脚本只会告警，所有爬虫任务连接失败，数据无意义。

**2. old 二进制**：`/tmp/gen1/uart-matmul`。它必须满足：

```bash
git switch --detach ed0725e1586102d229ccab28030a699ed9666b7c
./tools/bench/build.sh 
./tools/bench/syscall.sh old ./target/release/uart-matmul 32 5 -- --mode crawler
```

## build.sh — 构建

```bash
./tools/bench/build.sh              # raw（裸 clone3，默认）
./tools/bench/build.sh thread
./tools/bench/build.sh pool
./tools/bench/build.sh all          # 三个变体依次构建
LOG=1 ./tools/bench/build.sh pool   # 调试口径（verbose-log 开启，勿用于基准）  
```

输出：`target/bench/{raw,thread,pool}/release/crawler-daemon`

三个 worker feature 在代码层互斥，各变体用独立 `CARGO_TARGET_DIR` 构建，避免同名产物互相覆盖、也避免 feature 切换导致整树重编。`old` 不在当前代码树内，必须外部提供。

`verbose-log` 是独立维度，与 worker 变体正交：默认关闭，每行日志会引入一次 `write` 并可能阻塞在 stderr，污染 syscall 与墙钟统计（实测日志开/关差 4.8 倍 syscall）。

## bench.sh — 单方案墙钟

```bash
./bench.sh <name> <binary> <burst> <rounds> [-- daemon 额外参数...]
```

```bash
./bench.sh raw    ./target/bench/raw/release/crawler-daemon    32 100
./bench.sh old    /tmp/gen1/uart-matmul                        32 100 -- --mode crawler
```

墙钟取自 daemon 内部的 `[BATCH]` 计时（`IN_FLIGHT` 0→1 记批起点、归 0 打印），不是外部观测——外部「CPU 静默判定」会引入固定常数，曾把结论带偏。输出 CSV 加 `median / mean / min / max / p25 / p75` 汇总。

每轮发 `burst` 条请求后等 `[BATCH]` 行数增加；超过 `HANG_SECS` 无进展则判挂起，kill 并重启 daemon（专为 old 的 ET 丢唤醒缺陷设计）。

环境变量：`PORT`（9090）、`SRV`、`TW_WAIT`、`TW_MAX`、`HANG_SECS`（10）。

## suite.sh — 四方案墙钟

```bash
./suite.sh [-- daemon 公共额外参数...]
```

无位置参数，负载通过 `LOADS` 指定（格式 `burst:rounds`，空格分隔）。

```bash
LOADS="16:5" ./suite.sh                                    # 快速验证
LOADS="32:100 200:20 1000:5" ./suite.sh                    # 默认（论文口径，耗时长）
OUT=./result.csv LOADS="32:20" ./suite.sh                  # 指定输出
BIN_OLD= ./suite.sh                                        # old 置空则跳过该方案
```

每个方案开跑前等 `TIME_WAIT < TW_MAX`：上一方案残留的 TIME_WAIT 会系统性拉高后续方案的连接建立耗时，跨方案污染曾把结论带偏。结果逐轮追加到 CSV，每方案末尾一行记录 CPU jiffies 与挂起次数。

## syscall.sh — 单方案计数

```bash
./tools/bench/syscall.sh <name> <binary> <burst> [rounds] [-- daemon 额外参数...]
```

```bash
./tools/bench/syscall.sh raw    ./target/bench/raw/release/crawler-daemon    32 5
```

`rounds` 用于稀释进程启动摊销（`execve`/`openat`/`statx` 等约 50 次），建议 ≥5。输出总次数、每请求折算和完整 `strace -c` 表。

退出顺序很重要：先 `kill -TERM` daemon，等 strace 随被跟踪进程自行退出并输出 `-c` 汇总；直接杀 strace 会丢统计。

## syscall_suite.sh — 四方案计数

```bash
./syscall_suite.sh [burst] [-- daemon 公共额外参数...]
```

```bash
./syscall_suite.sh 32                # 每方案 32 请求
./syscall_suite.sh 200               # 默认口径
BIN_POOL= ./syscall_suite.sh 32      # 跳过 pool
```

结果追加到 `OUT`，每方案前有 `=== name (burst=N, ...) ===` 分隔行。

## 快速开始

```bash
./tools/bench/build.sh all                       # 构建 raw / thread / pool 三个变体
/tmp/echo &                          # 启动测试服务端
LOADS="16:5" ./tools/bench/suite.sh              # 墙钟：四方案各 5 轮（先小负载验证链路）
LOADS="32:100 200:20 1000:5" ./tools/bench/suite.sh   # 墙钟：四方案各 100 轮（先小负载验证链路）+ 200 轮（先大负载验证链路）+ 1000 轮（先超负载验证链路）+ 5 轮（先超负载验证链路）
./tools/bench/syscall_suite.sh 200               # syscall：四方案各 200 请求
```

## 环境变量

| 变量 | 默认 | 适用脚本 |
|---|---|---|
| `LOADS` | `32:100 200:20 1000:5` | suite.sh |
| `BIN_OLD` / `BIN_RAW` / `BIN_THREAD` / `BIN_POOL` | 见下 | suite.sh、syscall_suite.sh |
| `OLD_ARGS` | `--mode crawler` | suite.sh、syscall_suite.sh |
| `PORT` | 9090 / 9091 | 全部 |
| `SRV` | `127.0.0.1:19092` | 全部 |
| `TW_MAX` / `TW_WAIT` | 500 / 1 | bench.sh、suite.sh |
| `HANG_SECS` | 10 | bench.sh |
| `OUT` | 见「输出文件」 | suite.sh、syscall_suite.sh |

`BIN_*` 默认值：

```
BIN_OLD    /tmp/gen1/uart-matmul
BIN_RAW    <repo>/target/bench/raw/release/crawler-daemon
BIN_THREAD <repo>/target/bench/thread/release/crawler-daemon
BIN_POOL   <repo>/target/bench/pool/release/crawler-daemon
```

任一 `BIN_*` 置空会跳过该方案；路径不存在也会跳过并写入结果文件。

## 输出文件

| 脚本 | 默认输出 |
|---|---|
| `bench.sh` | `/tmp/bench_<name>.csv` + stderr 汇总 |
| `suite.sh` | `/tmp/suite_result.csv` |
| `syscall.sh` | `/tmp/syscall_<name>.trace` + 终端表 |
| `syscall_suite.sh` | `/tmp/syscall_suite.txt` |

CSV 每行格式：`name,burst=<N>,round=<R>,wall_us=<µs>`，方案末尾：`name,burst=<N>,cpu_ticks=<n>,hangs=<n>`。

## 注意事项

1. **必须关闭 verbose-log**。`bench.sh` / `syscall.sh` 会用 `strings` 检查二进制是否含 `[DAEMON] req#`，含则告警。

2. **strace 下的墙钟不可用**。`ptrace` 让每次系统调用停两次，futex 从几十纳秒抬到几十微秒。`-c` 表的 `% time` / `seconds` / `usecs/call` 三列同样不可信，只有 `calls` 与 `errors` 有效。

3. **墙钟与 strace 不能并行**。端口虽分开（9090 / 9091），但 `ptrace` 开销会干扰墙钟测量，必须串行。

4. **短负载下 `cpu_ticks` 可能为 0**。`/proc/<pid>/stat` 的 jiffies 粒度是 10 ms，burst 小、轮数少时累计不足一格。

5. **二进制身份**。运行期没有 feature 开关，变体只由路径区分。确认方式：`strace -c` 看 futex 次数（raw/thread 个位数、pool 数百），或 `strings` 判断 verbose-log 是否开启。文件大小不可靠。

## 常见问题

**所有方案都跳过** — `BIN_*` 路径不存在。先 `./build.sh all`，old 需单独提供 `/tmp/gen1/uart-matmul`。

**某方案 hangs 不为 0** — daemon 挂起后被脚本重启。old 带 ET 丢唤醒缺陷，突发较小时可能复现；其余变体不应出现。

**每请求 syscall 偏高** — `burst × rounds` 太小，启动摊销（约 50 次）占比过大。提高 `rounds`。

**墙钟数据离散大** — TIME_WAIT 未清空或系统有其他负载。确认 `TW_WAIT=1` 生效，并保持机器空闲。
