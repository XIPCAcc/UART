# 基准测试脚本

针对当前版本（`std::thread` 动态扩缩容）的批墙钟与系统调用测量。

## 脚本清单

| 脚本 | 作用 | 端口 |
|---|---|---|
| `build.sh` | 构建基准用二进制（默认关闭 verbose-log） | — |
| `bench.sh` | 批墙钟（daemon 内 `[BATCH]` 计时） | 9090 |
| `syscall.sh` | `strace -f -c` 系统调用计数 | 9091 |

## 前置条件

测试目标服务：daemon 会向 `127.0.0.1:19092` 发 HTTP 请求，必须先起服务端：

```bash
/tmp/echo &                  # 监听 127.0.0.1:19092
ss -ltn | grep 19092         # 确认在听
```

没有服务端时脚本只会告警，所有任务连接失败，数据无意义。

## 快速开始

```bash
./tools/bench/build.sh              # 1. 构建（verbose-log 关闭）
/tmp/echo &                         # 2. 启动测试服务端
./tools/bench/bench.sh 32 100       # 3. 墙钟：32 请求 × 100 轮
./tools/bench/syscall.sh 200 5      # 4. syscall：200 请求 × 5 轮
```

## build.sh — 构建

```bash
./tools/bench/build.sh          # 基准口径（verbose-log 关闭）
LOG=1 ./tools/bench/build.sh    # 调试口径（verbose-log 开启，勿用于基准）
```

输出：`target/release/crawler-daemon`

`verbose-log` 默认关闭：每行日志会引入一次 `write` 并可能阻塞在 stderr，污染 syscall 与墙钟统计。

## bench.sh — 批墙钟

```bash
./bench.sh [burst] [rounds] [-- daemon 额外参数...]
```

```bash
./bench.sh            # 默认 32 × 100
./bench.sh 1000 5     # 大突发
BIN=/path/to/bin ./bench.sh 32 50   # 测其他二进制
```

墙钟取自 daemon 内部的 `[BATCH]` 计时（`IN_FLIGHT` 0→1 记批起点、归 0 打印），不经外部观测。每轮发 `burst` 条请求后等 `[BATCH]` 行数增加；约 10 秒无进展判挂起，kill 并重启 daemon，挂起次数计入汇总。

输出：每轮一行写入 `/tmp/bench_result.csv`，结尾打印 `median / mean / min / max / p25 / p75`。

## syscall.sh — 系统调用计数

```bash
./syscall.sh [burst] [rounds] [-- daemon 额外参数...]
```

```bash
./syscall.sh          # 默认 32 × 5
./syscall.sh 200 5    # 正式口径
```

`rounds` 用于稀释进程启动摊销（`execve`/`openat` 等约 50 次），建议 ≥5。输出总次数、每请求折算和完整 `strace -c` 表。

退出顺序：先 `kill -TERM` daemon，strace 随被跟踪进程退出后自动输出 `-c` 汇总；直接杀 strace 会丢统计。

## 环境变量

| 变量 | 默认 | 适用脚本 |
|---|---|---|
| `BIN` | `<repo>/target/release/crawler-daemon` | bench.sh、syscall.sh |
| `PORT` | 9090 / 9091 | 两者 |
| `SRV` | `127.0.0.1:19092` | 两者 |
| `OUT` | `/tmp/bench_result.csv` | bench.sh |

## 注意事项

1. **必须关闭 verbose-log**。脚本会用 `strings` 检查二进制是否含 `[DAEMON] req#`，含则告警。

2. **strace 下的墙钟不可用**。`ptrace` 让每次系统调用停两次。`-c` 表的 `% time` / `seconds` / `usecs/call` 三列同样失真，只有 `calls` 与 `errors` 有效。

3. **墙钟与 strace 不能并行**。端口虽分开（9090 / 9091），但 `ptrace` 开销会干扰墙钟测量，必须串行。

4. **`errors` 列不等于故障**。`connect` 的 `EINPROGRESS`、`read` 的 `EAGAIN`、`futex` 的 `EAGAIN` 都是非阻塞/竞争路径的正常返回。

5. **每请求 syscall 偏高** — `burst × rounds` 太小，启动摊销占比过大。提高两者。
