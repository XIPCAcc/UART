# uart-matmul

通过串口进行矩阵乘法运算的 Linux 守护进程。支持三种模式：接收端（计算服务）、发送端（请求客户端）和爬虫守护进程（TCP 监听外部请求，多核异步执行）。

## 特性

- 零外部依赖，仅使用 Rust 标准库和 raw libc FFI
- 自研多核 async 运行时：全局就绪队列 + worker 动态扩缩容，epoll 事件驱动，空闲时零 CPU 占用
- 爬虫守护进程：TCP 监听外部请求，每行请求触发一个爬虫协程任务
- 支持 16550 兼容 UART 串口
- 支持 Intel 用户态中断（UINTR）kernel bypass
- 自定义帧协议，带 CRC-8 校验

## 编译

```bash
cargo build --release
```

## 用法

### 接收端（默认模式）

监听串口，接收矩阵乘法请求，计算后返回结果：

```bash
# 在 ttyS0 上监听
./target/release/uart-matmul --port /dev/ttyS0

# 指定波特率
./target/release/uart-matmul --port /dev/ttyS0 --baud 115200
```

### 发送端

向串口发送矩阵乘法请求，等待并打印结果：

```bash
# 默认 2x3 * 3x2 矩阵，发送 1 次
./target/release/uart-matmul --port /dev/ttyUSB0 --mode sender

# 指定矩阵维度和发送次数
./target/release/uart-matmul --port /dev/ttyUSB0 --mode sender \
    --rows-a 4 --cols-a 5 --cols-b 3 --count 10
```

### 爬虫守护进程

TCP 监听外部爬虫请求，每个请求创建一个异步爬虫协程（多核 worker 动态扩缩容）：

```bash
# 在 9090 端口监听
./target/release/uart-matmul --mode crawler --listen 0.0.0.0:9090
```

请求协议为纯文本，每行一个任务：`<host:port> [path]`（path 缺省为 `/`）。
一个 TCP 连接可携带多行请求，守护进程逐行回 ACK 并投放任务：

```bash
# 单个请求
echo "127.0.0.1:8080 /path" | nc 127.0.0.1 9090
# → OK #0 127.0.0.1:8080/path

# 一条连接批量提交多个请求
printf '127.0.0.1:8080 /\n127.0.0.1:8080 /other\n' | nc -w1 127.0.0.1 9090

# 非法请求
echo "bad" | nc 127.0.0.1 9090
# → ERR usage: <host:port> [path]
```

一键压测脚本（自动启动测试 HTTP 服务、守护进程，批量提交请求后 SIGTERM 优雅退出）：

```bash
./run_crawler.sh                   # 默认 3 轮 x 32 个请求，轮间隔 10s
./run_crawler.sh 500 20            # 3 轮 x 500 个请求，轮间隔 20ms
./run_crawler.sh 1000 10 127.0.0.1:19090 / 127.0.0.1:9090 0-3 5
#                REQS INTERVAL_MS  ADDR            PATH  LISTEN        CPUS ROUNDS
```

### 命令行参数

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `--port` | `-p` | 串口设备路径 | `/dev/ttyS0` |
| `--baud` | `-b` | 波特率 | `115200` |
| `--mode` | `-m` | 运行模式：`receiver` / `sender` / `crawler` | `receiver` |
| `--rows-a` | | 矩阵 A 的行数（仅 sender） | `2` |
| `--cols-a` | | 矩阵 A 的列数 / B 的行数（仅 sender） | `3` |
| `--cols-b` | | 矩阵 B 的列数（仅 sender） | `2` |
| `--count` | `-c` | 发送请求次数（仅 sender） | `1` |
| `--listen` | `-l` | 爬虫守护进程监听地址（仅 crawler） | `127.0.0.1:9090` |
| `--help` | `-h` | 打印帮助 | |

## 测试

### 快速测试脚本

两个串口通过 null-modem 线物理连接后，运行：

```bash
# 使用默认端口 /dev/ttyS0 和 /dev/ttyUSB0
./run_test.sh

# 指定端口
./run_test.sh /dev/ttyS0 /dev/ttyUSB0

# 指定端口、矩阵维度和请求数
./run_test.sh /dev/ttyS0 /dev/ttyUSB0 4 5 3 10
```

### 手动测试

终端 1（接收端）：
```bash
./target/release/uart-matmul --port /dev/ttyS0
```

终端 2（发送端）：
```bash
./target/release/uart-matmul --port /dev/ttyUSB0 --mode sender -c 5
```

## 帧协议

```
请求帧: [0xAA][LEN:u16 LE][ROWS_A][COLS_A][ROWS_B][COLS_B][float32 LE...][CRC8]
响应帧: [0xAA][LEN:u16 LE][ROWS][COLS][0x00][0x00][float32 LE...][CRC8]
错误帧: [0xAA][0x01 0x00][ERR_CODE][CRC8]
```

- 帧头: `0xAA`
- 长度: 2 字节小端，payload 字节数
- Payload: 矩阵维度和 float32 数据
- CRC-8/MAXIM: 覆盖长度字段和 payload

### 错误码

| 代码 | 含义 |
|------|------|
| `0x01` | 维度不匹配 |
| `0x02` | CRC 校验失败 |
| `0x03` | 帧格式错误 |
| `0x04` | 数据溢出 |

## UINTR Kernel Bypass

该项目支持通过 Intel 用户态中断实现 kernel bypass 串口通信。相关代码：

- `kernel/uart_uintr.c` — 独立内核模块，接管 UART IRQ 并通知用户态
- 内核修改参考 `/home/zwp/uintr-linux-kernel`，修改了 8250 串口驱动以支持 UINTR

### 内核改动

修改了三个文件：

1. `include/linux/serial_8250.h` — 在 `uart_8250_port` 结构体中添加 `uvec_file` 字段
2. `drivers/tty/serial/8250/8250_port.c` — IRQ 处理函数中，检测到 RX 中断时跳过内核读取 RBR，改为调用 `uintr_notify()` 通知用户态
3. `drivers/tty/serial/8250/8250_core.c` — 添加 `uvec_fd` 和 `uvec_port` 模块参数，初始化时设置 UINTR bypass

### 加载内核模块

```bash
# 加载独立 UINTR 模块
insmod kernel/uart_uintr.ko ioport=0x3F8 irq=4 uvec_fd=<fd>

# 或通过 8250 驱动参数
insmod 8250.ko uvec_fd=<fd> uvec_port=0
```

## 项目结构

```
uart/
├── src/
│   ├── main.rs          # 入口，CLI 解析，运行模式分发
│   ├── cli.rs           # 命令行参数解析
│   ├── executor.rs      # 多核 async 执行器（全局队列 + worker 动态扩缩容）
│   ├── reactor.rs       # epoll 事件循环 + 定时器 + eventfd 自唤醒
│   ├── tcp.rs           # 异步 TCP：AsyncTcpStream / AsyncTcpListener
│   ├── crawler.rs       # 爬虫守护进程（TCP 监听 → spawn 爬虫协程）
│   ├── logger.rs        # 全局时间戳日志（log! 宏）
│   ├── receiver.rs      # 接收端模式
│   ├── sender.rs        # 发送端模式
│   ├── compute.rs       # 矩阵乘法实现
│   ├── protocol.rs      # 帧协议编解码，CRC-8
│   ├── frame_reader.rs  # 帧解析器（状态机）
│   ├── serial_io.rs     # 串口打开/配置/读写
│   ├── async_serial.rs  # 异步串口封装
│   ├── sys.rs           # libc FFI 绑定（termios, epoll, socket, signal）
│   └── error.rs         # 错误类型定义
├── kernel/
│   └── uart_uintr.c     # UINTR 内核模块
├── tools/
│   └── send_matrix.py   # Python 发送端工具（参考）
├── run_test.sh          # 串口收发测试脚本
├── run_crawler.sh       # 爬虫守护进程压测脚本
├── test_server.py       # 测试用 HTTP 服务
├── Cargo.toml
└── README.md
```

## 注意事项

- 串口需要以 raw 模式打开（8N1，无流控）
- 如果 `/dev/ttyS0` 被 `serial-getty@ttyS0.service` 占用，需先停用：`systemctl stop serial-getty@ttyS0.service && systemctl mask serial-getty@ttyS0.service`
- 两个串口之间需要 null-modem 交叉线物理连接
- 波特率双方必须一致