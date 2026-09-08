# uart-matmul — Cargo workspace

基于 Intel 用户态中断（UINTR）的异步运行时 + 串口矩阵乘应用 + 各项 IPC/管道基准测试。

```
uart/
├── Cargo.toml              # workspace 清单（lib + 应用 + 各测试 crate）
├── handler.c               # UINTR 中断处理程序（各测试 crate 的 build.rs 编译）
├── ipc.md                  # IPC 统一接口与测试规范文档
│
├── runtime/                # uintr-runtime 库 crate：异步运行时
│   └── src/
│       ├── lib.rs
│       ├── executor.rs     # 单线程 async 执行器（Treiber 栈 + UMONITOR/UMWAIT 休眠）
│       ├── reactor.rs      # epoll I/O 事件循环
│       ├── signal.rs       # SIGINT/SIGTERM → TERM 原子标志
│       ├── sys.rs          # libc FFI（termios/epoll/signal/raw IO）
│       ├── trace.rs        # 无锁 stderr/stdout 日志（防中断上下文死锁）
│       ├── error.rs        # AppError/FrameError/ComputeError
│       ├── shm.rs          # 共享内存 SPSC 通道（ShmChannel/Sender/Receiver）
│       ├── pipe.rs         # FIFO 通道（PipeSender/PipeReceiver）
│       ├── uintr_core.rs   # UintrToken / WAKE_FLAG（无锁 waker）
│       └── uintr/          # syscall、async_wait(.await 原语)、connection(fd 传递)、benchmark
│
├── uart/                   # uart-matmul bin：串口矩阵乘应用（默认 bin）
│   ├── main.rs / cli.rs    # 仅 receiver / sender 两种串口模式
│   ├── receiver.rs         # 接收端：收帧 → 矩阵乘 → 回写
│   ├── sender.rs           # 发送端：请求流水线
│   ├── async_serial.rs / serial_io.rs / protocol.rs / frame_reader.rs / compute.rs / rng.rs
│
├── test-uintr/             # UINTR ping-pong 测试
│   └── src/bin/uintr-sender.rs, uintr-receiver.rs
├── test-shm/               # 共享内存 + UINTR 通道测试
│   └── src/bin/shm-sender.rs, shm-receiver.rs
├── test-pipe/              # FIFO 吞吐量测试（64KB×4096=256MB）
│   └── src/bin/pipe-sender.rs, pipe-receiver.rs
├── test-pipe-latency/      # FIFO 延迟测试（8B ping-pong, 100000 轮）
│   └── src/bin/pipe-latency-client.rs, pipe-latency-server.rs
│
├── bench_pipe.sh / bench_shm.sh   # 一键基准测试脚本
├── run_serial_test.sh / run_serial_sender.sh / run_serial_receiver.sh   # 串口矩阵乘测试脚本
```

## 编译

```bash
# 全部 crate（release）
cargo build --release --workspace

# 只编应用 uart-matmul 及其依赖（默认）
cargo build --release
```

各测试 crate 的 `build.rs` 会编译根目录 `handler.c`（`-muintr`），并把 UINTR 中断处理程序
`ui_handler` 与本 crate 的 `rust_interrupt_callback` 链接到同一可执行文件。

## 串口矩阵乘应用（uart-matmul）

### 接收端（默认模式）

```bash
./target/release/uart-matmul --port /dev/ttyS0 --baud 115200
```

### 发送端

```bash
./target/release/uart-matmul --port /dev/ttyUSB0 --mode sender \
    --rows-a 2 --cols-a 3 --cols-b 2 --count 5
```

参数表：

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `--port` | `-p` | 串口设备路径 | `/dev/ttyS0` |
| `--baud` | `-b` | 波特率 | `115200` |
| `--mode` | `-m` | `receiver`（默认）或 `sender` | `receiver` |
| `--rows-a` / `--cols-a` / `--cols-b` | | 矩阵维度（仅 sender） | `2`/`3`/`2` |
| `--count` | `-c` | 发送请求次数（仅 sender） | `1` |

两个串口通过 null-modem 线物理连接后：

```bash
./run_serial_test.sh                              # 默认 /dev/ttyS0 ↔ /dev/ttyUSB0
./run_serial_test.sh /dev/ttyS0 /dev/ttyUSB0 4 5 3 10
# 或分开跑：终端1 ./run_serial_receiver.sh；终端2 ./run_serial_sender.sh
```

### 帧协议

```
请求帧: [0xAA][LEN:u16 LE][ROWS_A][COLS_A][ROWS_B][COLS_B][float32 LE...][CRC8]
响应帧: [0xAA][LEN:u16 LE][ROWS][COLS][0x00][0x00][float32 LE...][CRC8]
错误帧: [0xAA][0x01 0x00][ERR_CODE][CRC8]
```

- 帧头 `0xAA`，长度 2 字节小端，CRC-8/MAXIM 覆盖长度与 payload。
- 错误码：`0x01` 维度不匹配 / `0x02` CRC 失败 / `0x03` 帧格式错误 / `0x04` 数据溢出。

## UINTR 基准测试

每种测试都是独立二进制，建议 `taskset` 绑核运行，接收方在 14、发送方在 15（最高频核）。

### 吞吐量：共享内存 IPC / FIFO

```bash
./bench_pipe.sh                 # FIFO：吞吐 + 延迟
./bench_pipe.sh --throughput    # 仅吞吐
./bench_pipe.sh --latency       # 仅延迟
./bench_shm.sh                  # shm 吞吐量（与 bench_pipe 同口径）
```

手动运行示例：

```bash
# FIFO 吞吐（256MB / 64KB chunk，代码内固定）
taskset -c 14 ./target/release/pipe-receiver
taskset -c 15 ./target/release/pipe-sender

# FIFO 延迟（8B ping-pong）
taskset -c 14 ./target/release/pipe-latency-server
taskset -c 15 ./target/release/pipe-latency-client

# shm 通道（固定 256MB / 64KB chunk）
taskset -c 14 ./target/release/shm-receiver
taskset -c 15 ./target/release/shm-sender

# UINTR ping-pong
taskset -c 14 ./target/release/uintr-receiver
taskset -c 15 ./target/release/uintr-sender -c 100000
```

测试规范细节见 [ipc.md](ipc.md)。可调环境变量：`RECV_CPU`/`SEND_CPU`（绑核）、`BASE`
（二进制目录）、脚本各自的二进制路径变量。

## UINTR 内核支持说明

- Intel UINTR kernel bypass 需要修改版内核（`/home/zwp/uintr-linux-kernel`），
  串口驱动在 RX 中断时跳过内核读 RBR、改为 `uintr_notify()` 通知用户态。
- 运行前若 `/dev/ttyS0` 被 `serial-getty@ttyS0.service` 占用需先 `systemctl stop --now serial-getty@ttyS0.service`（必要时 mask）。

## 注意事项

- 中断 handler 路径禁止 I/O、锁与 `eprintln!`，日志请用运行时的 `trace`（直接 `libc::write`）。
- 运行时依赖仅 Rust 标准库 + raw libc FFI（应用无第三方依赖）。
