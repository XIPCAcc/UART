# UINTR 用户态中断异步 IPC 运行时 — 设计文档

## 1. 项目目标

在 Intel 用户态中断（UINTR, User Interprocessor Interrupts）与 WAITPKG
（UMONITOR/UMWAIT）硬件特性之上，构建一个**纯 Rust std（无第三方 crate）**的
单线程 async 运行时，并实现两种跨进程 IPC 通道：

| 通道 | 数据面 | 通知面 |
|---|---|---|
| `shm` | memfd + mmap 共享内存 SPSC 环形缓冲区（64 KiB） | **senduipi 用户态中断** |
| `pipe` | POSIX FIFO 命名管道（内核 64 KiB 管道缓冲） | **senduipi 用户态中断** |

并与两个对照组做公平对比：

- `ipc-tokio`（/home/zwp/ipc-tokio）：同样的共享内存环形缓冲区，通知面为
  **eventfd + epoll（tokio）**；
- `tokio-pipe`（/home/zwp/tokio-pipe）：FIFO + tokio。

三方数据面参数完全一致（环形缓冲/管道缓冲均为 64 KiB，chunk 均为 64 KiB），
性能差异只来自通知机制。

硬约束：仅依赖 Rust 标准库 + libc FFI；串口以 `O_NOCTTY` 打开；所有 UINTR
相关 syscall 走定制内核（/home/zwp/uintr-linux-kernel）；用户态中断处理
程序必须无锁、无 I/O、不可 panic。

---

## 2. 工作区结构

```
uart/
├── runtime/                  # crate: uintr-runtime（库）
│   └── src/
│       ├── lib.rs            # 模块入口
│       ├── executor.rs       # 单线程 async 执行器（TransferStack + UMWAIT 休眠）
│       ├── reactor.rs        # epoll reactor（串口等常规 fd I/O 使用）
│       ├── uintr_core.rs     # UintrToken：seq 序号 + 无锁 AtomicPtr<Waker>
│       ├── uintr/
│       │   ├── syscall.rs    # UINTR syscall 封装 + senduipi/stui/clui/umonitor/umwait
│       │   ├── async_wait.rs # UintrFuture：token.seq 驱动的 await
│       │   ├── connection.rs # Unix socket + SCM_RIGHTS 传递 fd
│       │   └── benchmark.rs  # UINTR 微基准
│       ├── shm.rs            # 共享内存 SPSC 通道（ShmChannel/Sender/Receiver）
│       ├── pipe.rs           # FIFO 通道（PipeSender/PipeReceiver）
│       ├── signal.rs         # SIGINT/SIGTERM（raw libc::signal + 轮询）
│       ├── trace.rs          # 直接 libc::write 的无锁 stderr（中断上下文安全）
│       ├── sys.rs / error.rs
├── handler.c                 # C 中断处理程序 ui_handler（interrupt 属性）
├── test-shm/                 # bins: shm-sender, shm-receiver（吞吐量）
├── test-pipe/                # bins: pipe-sender, pipe-receiver（FIFO 吞吐量）
├── test-pipe-latency/        # bins: pipe-latency-client/server（8B ping-pong 延迟）
├── test-uintr/               # bins: uintr-sender, uintr-receiver（中断微基准）
├── uart/                     # 串口矩阵乘法应用（uart-matmul）
├── bench_shm.sh / bench_pipe.sh
└── run_serial_*.sh
```

每个测试 crate 的 `build.rs` 把根目录 `handler.c` 编译进本 crate，保证
`rust_interrupt_callback` 符号与 `ui_handler` 在同一链接单元解析。

---

## 3. 硬件与内核依赖

### 3.1 指令

| 指令 | 作用 |
|---|---|
| `senduipi idx` | 向已注册的接收方投递用户态中断（用户态指令，无 syscall） |
| `stui` / `clui` | 开/关用户态中断接收（UIF 标志位） |
| `uiret` | 中断返回 |
| `umonitor addr` | arm 地址监控硬件，监控 `addr` 的写操作 |
| `umwait state, deadline` | 进入 C0.x 优化休眠态，被监控地址写或 TSC 超时唤醒 |

### 3.2 定制内核 syscall（x86_64 号 471–476）

| 号 | 调用 | 说明 |
|---|---|---|
| 471 | `uintr_register_handler(handler, flags)` | 注册用户态中断入口 |
| 472 | `uintr_unregister_handler()` | |
| 473 | `uintr_create_fd(vector, flags)` | 创建本端 UINTR fd（可传给对端） |
| 474 | `uintr_register_sender(fd, flags)` | 把对端 fd 注册为发送目标，返回 uipi_index |
| 475 | `uintr_unregister_sender(fd)` | |
| 476 | `uintr_wait(usec, flags)` | 内核态等待（超时上限 `UINTR_WAIT_MAX_USEC = 10^7` µs） |

**关键约束：`uintr_register_handler(ui_handler, 0)` 的 flags 必须为 0。**
非 0（如 `UINTR_HANDLER_FLAG_WAITING_ANY`）会使阻塞式 syscall 被 UINTR
打断而返回 EINTR；flags=0 后中断只在用户态指令边界投递。

---

## 4. 运行时设计

### 4.1 执行器（executor.rs）

单线程事件循环，参考 Embassy 的 TaskStorage/TaskHeader 模式：

- **Task**：`TaskHeader { state: AtomicU8, run_queue_item }` + `Pin<Box<dyn Future>>`，
  由 `Rc` 管理。
- **state 位标志**：`STATE_SPAWNED`（任务存活）、`STATE_RUN_QUEUED`（已在运行队列，
  防止 double-wake 重复入队）。任务 Ready 时清除 SPAWNED，此后任何残留唤醒直接丢弃，
  避免 "async fn resumed after completion" panic。
- **RunQueue = TransferStack（Treiber 无锁栈）**：
  - `push_was_empty`：CAS 循环入栈（x86_64 `lock cmpxchg`）；
  - `take_all`：`swap` 批量取出（`lock xchg`）。
  中断 handler 与执行器本线程都会入队，故必须无锁。
- **Waker**：自定义 `RawWakerVTable`，`wake_by_ref` 把 task 压入 TransferStack。

`block_on()` 事件循环：

```
loop {
    drain_queue();                       // take_all → 逐个 poll
    if pending == 0 || SIGTERM { break; }
    // 快路径（RCU 读端语义）：队列非空直接 continue，不碰中断开关
    if !queue.is_empty() { continue; }
    // 慢路径：确需休眠
    clui();                              // 关用户态中断
    if !queue.is_empty() { stui(); continue; }   // CLUI 期间handler已入队
    WAKE_FLAG = 0;
    umonitor(&WAKE_FLAG);                // arm 监控
    stui();                              // 开中断
    umwait(0, u64::MAX);                 // C0.2 深度休眠，无限超时
}
```

休眠**只依赖** handler 对 `WAKE_FLAG` 的 `store(1)` 唤醒（UMONITOR 监控该地址），
不使用 TSC 超时。CLUI→复查→UMONITOR→STUI→UMWAIT 的临界区顺序消除了
"复查为空、中断在 UMONITOR 前到达" 的丢唤醒窗口。

### 4.2 UintrToken（uintr_core.rs）— 中断与 async 的桥

```rust
pub struct Inner {
    seq: AtomicU32,          // handler 每次中断 fetch_add(1)
    consumed_seq: AtomicU32, // future 已消费到的序号
    waker: AtomicPtr<Waker>, // 无锁 waker 槽位
}
```

- **`set_pending()`（中断上下文调用）**：`seq.fetch_add(1, Release)` →
  读 waker 指针，非空则 `wake_by_ref()`。全程无锁；`wake_by_ref` 外包
  `catch_unwind`，panic 绝不冒泡进中断上下文。
- **waker 槽位用 `AtomicPtr<Waker>`**：`WAKER_EMPTY = usize::MAX` 作哨兵
  （合法 Box 指针对齐 ≥8，不与 MAX 重叠）。注册采用**单次 `compare_exchange`
  strong**（不用 weak 循环），首次注册后 waker 一直复用，避免每次 poll 的
  Box 分配/释放，也避免 swap 取走方案的所有权混乱。
- **`clear_waker()`**：future 返回 Ready 前 `swap(WAKER_EMPTY)` 取走并释放 waker，
  防止任务结束后残留 waker 被后续中断唤醒。

### 4.3 UintrFuture（async_wait.rs）

```
poll:
    if seq != consumed_seq {          // 已有未消费中断
        consumed_seq = seq; clear_waker(); Ready
    }
    register_waker(cx.waker());       // 惰性一次性注册
    双重检查 seq != consumed_seq      // 中断可能在注册间隙到达
        → Ready；否则 Pending
```

seq 序号机制解决了"中断在任务睡眠之前到达"的竞态：中断带来的 seq 增量不会
丢失，future 下次 poll 立即看到 Ready。

### 4.4 中断投递路径

```
对端 senduipi
  → 本端 CPU 硬件陷入 ui_handler (handler.c, __attribute__((interrupt)))
      → rust_interrupt_callback (各测试 crate 的 glue.rs)
          → TOKEN.set_pending()       // seq++，waker.wake_by_ref() → task 入 TransferStack
          → WAKE_FLAG.store(1)        // 解除 UMONITOR/UMWAIT
      → uiret
  → 执行器醒来 drain_queue → poll 对应 task
```

handler 约束：**无锁原子操作、无 I/O、无 Mutex、不可 panic、不可 unwind**。

### 4.5 中断上下文安全约束（踩坑固化）

- **禁止 `eprintln!`/`std::io`**：std stderr 持有不可重入 pthread mutex，
  中断若在持锁期间触发且 handler 路径再打印（panic hook 等）→ 单线程死锁。
  一律用 `trace.rs` 的直接 `libc::write(STDERR_FILENO, ...)`。
- **禁止 `Mutex<Option<Waker>>`**：中断重入死锁；用 `AtomicPtr<Waker>`。
- **`stui()` 必须推迟到所有阻塞式 fd 交换/FIFO 打开之后**：避免
  accept/recvmsg/sendmsg 被 EINTR 打断。
- 整数格式化中 `ptr::copy_nonoverlapping` 源/目标重叠是 UB，会 panic abort。

---

## 5. 异步 IPC 设计方案

第 4 章给出运行时的机械部件（执行器、token、future、中断路径），本章说明
如何把一条**跨进程字节通道**整体组织成 Rust `async/await` 抽象——这是 shm
（第 6 章）与 pipe（第 7 章）共同遵循的方案。

### 5.1 设计目标

1. **跨进程 SPSC 字节流，async API 形态对齐 tokio**：

   ```rust
   impl Sender { pub async fn write(&self, buf: &[u8]) -> io::Result<()>; }
   impl Receiver { pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize>; }
   ```

   调用方可以像 `tokio::io::AsyncRead/AsyncWrite` 一样在 async fn 中直接
   `.await`，与执行器上的其他任务组合。
2. **快速路径零 syscall、零内核参与**：数据读写只是共享内存上的原子量
   load/store + `copy_nonoverlapping`；"通知对端"是用户态 `senduipi` 指令，
   不陷入内核。数据路径上唯一的内核交互只剩建连阶段的 fd 交换。
3. **背压自然传播**：环形缓冲区的"满 / 空"是仅有的两个挂起点，不需要
   显式流控消息。
4. **机制可替换、便于公平对照**：数据面（shm 环 / FIFO）、通知面
   （senduipi / eventfd）、调度面（本运行时 / tokio）三层解耦，ipc-tokio
   对照组复用同一通道层代码结构，只替换通知面。

### 5.2 三层解耦

| 层 | 职责 | shm 实现 | pipe 实现 |
|---|---|---|---|
| 数据面 | 非阻塞字节搬运，返回 0/ EAGAIN 表示"暂时不可用" | 共享内存 SPSC 环 `read_available`/`write_available` | `read`/`write` fd（O_NONBLOCK） |
| 通知面 | 传递 1 bit 事件："有数据了"/"有空间了" | `senduipi`（双向各一个 uipi_index） | 同左（对照组为 eventfd write） |
| 调度面 | 挂起当前 task、中断到达后唤醒重 poll | `UintrToken` + `UintrFuture` + 执行器 UMWAIT | 同左 |

端到端数据流与控制流：

```
应用 async task
  │  sender.write(buf).await / receiver.read(buf).await
  ▼
通道层（shm.rs / pipe.rs）
  │  非阻塞尝试：write_available / read_available（纯内存，返回 0 ≙ EAGAIN）
  │  有进展 → 同一次 poll 内继续尝试（不让出执行器，自然形成批量/流水）
  │  不可用 → senduipi 通知对端 → uintr(token).await
  ▼
UintrFuture（async_wait.rs）
  │  poll：seq != consumed_seq ? Ready（消费序号、clear_waker）
  │                            : 注册 waker → Pending
  ▼
执行器（executor.rs）
  │  Pending → 任务让出；运行队列空 → CLUI → UMONITOR(WAKE_FLAG) → STUI → UMWAIT
  │
  │  ═══════════ 对端 senduipi（用户态指令，跨 CPU 投递）═══════════
  ▼
ui_handler (handler.c) → rust_interrupt_callback (glue.rs)
  │  token.set_pending()：seq.fetch_add(1) + waker.wake_by_ref() → task 入 TransferStack
  │  WAKE_FLAG.store(1)：UMONITOR 监控命中，UMWAIT 立即退出
  ▼
执行器 drain_queue → 任务重新 poll → UintrFuture 见序号变化 → Ready
  ▼
通道层从挂起点继续：重新尝试非阻塞读写，直到完成或再次被对端卡住
```

### 5.3 通道的异步状态机

通道方法本质是"**非阻塞尝试循环 + 唯一 await 点**"。关键决策：**一次 poll
内只要非阻塞操作有进展就不返回 Pending**，任务持续推进到 buf 写完 / 读到
数据 / 真正被对端卡住为止。这样单次唤醒就能排空或填满整个 ring，水位通知
策略（第 6.3 节）才能把 await 次数从 O(chunks) 降到 O(睡眠周期)。

```
Sender::write(buf):                    Receiver::read(buf):
  loop {                                 loop {
    n = write_available(剩余);             n = read_available(buf);
    if n > 0 { 推进; continue; }           if n > 0 { return Ready(n); }
    // ring 满：                          // ring 空：
    senduipi(数据通知);                    senduipi(空间通知);
    uintr(背压token).await;  ──┐           uintr(数据token).await;  ──┐
  }                           │          }                           │
  buf 写完 → senduipi(数据通知) │                                            │
                               │          （await 仅在对端动作后 Ready，◀┘）
              （await 在对端消费发空间中断后 Ready）◀┘
```

对比"每 chunk await 一次"的写法：那会在 256MB 传输中产生 4096 次任务挂起/
唤醒；本方案挂起只发生在 ring 满/空水位，连续读写期间完全不打断对端。

### 5.4 跨进程挂起–唤醒时序（背压一轮）

以发送方写满、被接收方放行一次为例（CPU15 = sender，CPU14 = receiver）：

```
Sender (CPU15)                              Receiver (CPU14)
─────────────────                           ─────────────────
1. write_available() → 0（ring 满）
2. senduipi(数据索引)  ────────────────▶   3. 硬件陷入 ui_handler
                                              set_pending(数据token):
                                                seq++，waker 入队
                                              WAKE_FLAG=1 → uiret
4. poll UintrFuture: seq==consumed
   注册 waker → Pending
5. 队列空 → CLUI → UMONITOR → STUI
   → UMWAIT 休眠                         6. 任务重 poll：read_available()
                                              循环拷贝，消费整 ring
                                           7. ring 读空 → senduipi(空间索引)
8. 硬件陷入 ui_handler  ◀────────────────
   set_pending(背压token):
     seq++，waker.wake_by_ref() → task 入队
   WAKE_FLAG=1 → uiret
9. UMWAIT 被监控地址写唤醒
   drain_queue → UintrFuture poll:
     seq != consumed → Ready → clear_waker
10. write 循环继续（ring 已有空间）
```

接收方等数据的路径完全对称（方向反转）。一次唤醒周期的开销构成：
`senduipi` 指令 + 硬件中断投递 + handler（数条原子指令）+ UMWAIT 退出
C-state + 任务入队/出队 + 重 poll——**全程无 syscall、无内核调度决策**。
对照组 eventfd 路径则为：`write(eventfd)` syscall → 内核置事件 →
`epoll_wait` 返回 → tokio 调度器入队。

### 5.5 通知语义：1-bit 事件 + 用户态序号

- `senduipi` **不携带任何数据**，只表示"有事了"；可读/可写多少字节由醒来
  的一方自己读共享内存位置量（或对 fd 做非阻塞 read）得到。通知与数据
  解耦，中断合并不影响正确性。
- 多次中断在 `token.seq` 上累加为多次自增；`UintrFuture` 只判断
  `seq != consumed_seq`，不维护事件队列。中断不会积压、也不会因合并丢失：
  醒来后通道层的 `loop` 会反复尝试非阻塞操作，直到再次返回 0/EAGAIN 才
  再次挂起——即"**电平驱动重试**"，漏一个中断也不会死锁（水位点的对端
  通知兜底，见 6.4）。
- 这与 eventfd 的计数器语义等价，但 seq 在用户态，读写无 syscall。

### 5.6 与 epoll / eventfd 异步模型的对照

| 维度 | tokio（epoll + eventfd） | 本方案（UINTR + UMWAIT） |
|---|---|---|
| 挂起 | 非阻塞操作 EAGAIN → future Pending → 线程 park 在 `epoll_wait` | 非阻塞操作返回 0 → future Pending → `umwait` |
| 通知 | `write(eventfd)` 系统调用，内核置就绪事件 | `senduipi` 用户态指令，硬件直接投递 |
| 唤醒 | `epoll_wait` 返回 → 调度器把 task 入队 | 中断 handler 里 `waker.wake_by_ref()` 直接入队 |
| 休眠退出 | 内核调度 + syscall 返回 | UMONITOR 监控 `WAKE_FLAG` 存储，硬件唤醒 |
| waker / Future | 标准 `RawWakerVTable` | **完全相同**，task 生态兼容 |

通道层伪代码在两套方案下逐行对应（满/空才 notify、await 等待），这保证了
第 8 章的性能差异只来自通知机制本身。

### 5.7 异步设计的正确性不变量

1. **不丢唤醒**（sleep/wake 竞态）三道防线：
   - `token.seq` 序号：中断早于 waker 注册到达也会被 latch，future 首次
     poll 即 Ready；
   - `UintrFuture` 注册 waker 后双重检查 seq；
   - 执行器 CLUI 临界区包裹"队列空检查 → UMONITOR arm"，中断不可能落在
     检查与 arm 之间。
2. **不唤醒死任务**：future Ready 前 `clear_waker()` 回收 waker；task
   完成后清 `STATE_SPAWNED`，残留唤醒直接丢弃。
3. **中断上下文安全**：handler 路径只允许无锁原子操作（seq、waker 指针、
   WAKE_FLAG），无 I/O、无 Mutex、不可 panic（`catch_unwind` 兜底）。
4. **背压对称闭合**：sender 满 → 必发数据通知后睡；receiver 读空 → 必发
   空间通知后睡。任何一方睡眠时，对端在对应水位点必有一次通知，无循环
   等待。
5. **waker 生命周期**：`AtomicPtr<Waker>` 单次 CAS 注册、`wake_by_ref`
   复用、`clear_waker`/Drop 对称回收，无 use-after-free、无泄漏。

> pipe 通道（第 7 章）原样复用本章方案，唯一区别是数据面的非阻塞原语从
> "环形缓冲拷贝"换成"fd 上的非阻塞 read/write（EAGAIN 即挂起）"。

---

## 6. shm 通道设计（核心）

### 6.1 数据面：共享内存 SPSC 环形缓冲区

```rust
#[repr(C)]
pub struct ShmChannel {
    write_pos: AtomicU64,                       // 仅发送方写
    read_pos:  AtomicU64,                       // 仅接收方写
    data: UnsafeCell<[u8; RING_CAPACITY]>,      // 64 KiB
}
```

- 建立：发送方 `memfd_create("uintr-shm", MFD_CLOEXEC)` → `ftruncate` →
  `mmap(MAP_SHARED)`；fd 经 Unix socket `SCM_RIGHTS` 传给接收方，接收方
  同样 mmap。结构体 `#[repr(C)]`、只含原子量与字节数组、**无指针**，
  可安全跨进程映射。
- **位置计数单调递增（wrapping u64），不取模回绕**；索引时
  `pos & (RING_CAPACITY - 1)`（容量为 2 的幂）。已用空间 =
  `write_pos - read_pos`，天然区分满（差 = 64KiB）与空（差 = 0）。
- 内存序：数据写入用 `write_pos.store(Release)` 发布，对端
  `write_pos.load(Acquire)` 获取；读侧对称（`read_pos` Release/Acquire）。
  SPSC 下两个位置量各只有一个写者， Relaxed 读自身位置即可。
- **拷贝**：回绕处分两段 `ptr::copy_nonoverlapping`（替代逐字节循环），
  编译器可展开为宽字/向量拷贝：

```rust
let first = n.min(RING_CAPACITY - start);
copy_nonoverlapping(src.as_ptr(), data.add(start), first);
if first < n {
    copy_nonoverlapping(src.as_ptr().add(first), data, n - first);
}
```

### 6.2 通知面：双向 senduipi

连接建立后，双方各持有：

- `uipi_index`：对端的发送目标索引（`uintr_register_sender(对端uintrfd)` 返回），
  用于 `senduipi`；
- `token`：本端等待的 UintrToken（handler 置位）。

方向：

```
Sender ──数据中断(senduipi receiver_index)──▶ Receiver
Sender ◀──背压中断(senduipi sender_index)──── Receiver
```

### 6.3 通知策略（批量/水位驱动，与 ipc-tokio eventfd 版对齐）

**Sender::write(buf)**：

```
while 未写完:
    n = write_available(剩余buf)
    n > 0 → 继续写（不通知，形成流水）
    n == 0（ring 满）→
        notify_receiver()          // senduipi：来读
        uintr(token).await         // 睡等背压（空间）中断
buf 全部写完 → notify_receiver()   // 补发一次，保证最后一批被收到
```

**Receiver::read(buf)**：

```
loop:
    n = read_available(buf)
    n > 0 → 立即返回（不通知，连续读空为止）
    n == 0（ring 空）→
        notify_sender()            // senduipi：有空间了
        uintr(token).await         // 睡等数据中断
```

即：**写满才通知数据、读空才通知空间**。通知次数 ≈ 睡眠/唤醒次数，
而非每 chunk 一次。`interrupts_sent()` 与 `sleeps()` 计数用于基准输出。

### 6.4 正确性与死锁分析

- **满/空两个阻塞点对称**：
  - 发送方满 → 发数据通知 → 等空间；接收方读空后必发空间通知；
  - 接收方空 → 发空间通知 → 等数据；发送方写满（或写完）后必发数据通知。
  任何一方睡眠时，对端在对应的水位点都会发中断，无循环等待。
- **中断早于睡眠到达不丢失**：中断体现为 `token.seq` 增量，
  `UintrFuture::poll` 先比对 seq 再注册 waker（含注册后双重检查），
   seq 已变则直接 Ready，不会睡死。
- **执行器层丢唤醒防护**：CLUI 包裹"队列空检查 → UMONITOR"，
  handler 的入队与 `WAKE_FLAG.store(1)` 不可能落在检查与 arm 之间。
- **SPSC 数据无竞争**：发送方只写 `[write_pos, …)`，接收方只读
  `[read_pos, write_pos)` 且只写 `read_pos`，区间不重叠。

### 6.5 连接建立流程（test-shm）

```
Receiver                                  Sender
UINTR handler 注册(flags=0)               同左
uintr_create_fd → rfd                     uintr_create_fd → sfd
bind /tmp/uintr-shm.sock                  connect
                                          create_shm() → shm_fd
        ◀── SCM_RIGHTS: shm_fd ──
        ◀── SCM_RIGHTS: sfd ──
        ─── SCM_RIGHTS: rfd ──▶
mmap(shm_fd)                              （close shm_fd，映射保留）
register_sender(sfd) → idx_背压           register_sender(rfd) → idx_数据
stui()                                    stui()
读 GOSTART! 标记 → t0                     写 GOSTART! → t0
累计读 256MB                              64KB×4096 写 256MB
```

`stui()` 放在 fd 交换完成之后，避免阻塞 socket 调用被 EINTR。

---

## 7. pipe 通道（pipe.rs）

数据面直接用 POSIX FIFO（`mkfifo` + `open(O_RDONLY/O_WRONLY | O_NONBLOCK)`），
缓冲、原子写、EOF 全部由内核承担；通知面复用与 shm 完全相同的双向
senduipi + token 模式：write 返回 EAGAIN 时通知对端并 `uintr(token).await`
等背压；read 返回 EAGAIN 时通知对端并等数据。FIFO 打开顺序用握手 socket
上的 1 字节 ready 同步，避免读端先开导致伪 EOF（read 返回 0）。

---

## 8. 测试方法学

### 8.1 吞吐量（bench_shm.sh / bench_pipe.sh）

- 固定数据量：**256 MB = 64 KiB chunk × 4096**，载荷 0x5A；
- **GO 标记同步**：发送方先经被测通道写 8 字节 `GOSTART!`，接收方循环
  读满 8 字节并校验后取 `t0`，发送方写完 GO 后取 `t0`；GO 不计入 256MB；
- 绑核：`taskset -c 14` 接收方、`taskset -c 15` 发送方（RECV_CPU/SEND_CPU 可覆盖）；
- 输出：MiB/s + 双方 senduipi 次数 + 睡眠唤醒次数。

### 8.2 延迟（test-pipe-latency）

- 8 字节 ping-pong：client 发 → server 原样回显；
- **2000 轮 warmup + 100000 轮计时**，输出平均 RTT（µs）与 msg/s。

### 8.3 公平性

shm-UINTR、ipc-tokio shm、内核 FIFO 三者缓冲容量统一 64 KiB、chunk 统一
64 KiB；shm 与 ipc-tokio 的通知策略（满/空才通知）与数据面代码结构一一
对应，差异仅为 senduipi vs eventfd+epoll。

---

## 9. 性能结果与分析

| 方案 | 吞吐量 | 延迟 RTT |
|---|---|---|
| tokio-pipe（FIFO + epoll） | ~1438 MiB/s | ~9.07 µs |
| UINTR pipe（FIFO + senduipi） | ~1430–1508 MiB/s | ~4.76–8.5 µs |
| shm-UINTR（逐块通知，旧策略） | 442 MiB/s | — |

**shm 早期瓶颈**：ring 容量（64KB）= chunk 大小，且每 chunk 都通知/睡眠，
发送方与接收方逐块锁步——256MB 产生 4096 次 sender 睡眠 + 4097 次
receiver 睡眠，每次睡眠/唤醒周期（UMONITOR/UMWAIT 进出 C-state + waker
入队/出队）约 141 µs。FIFO 因内核缓冲支持读写重叠，同期仅睡眠 7 次。

**优化措施**（已落地）：

1. 通知改为水位驱动（写满/读空才通知，§6.3），连续读写期间不打断对端；
2. 环形拷贝改两段式 `copy_nonoverlapping`；
3. waker 槽位改 AtomicPtr 单次注册 + `wake_by_ref` 复用，消除唤醒路径的
   Box 分配；
4. 执行器休眠加 RCU 快路径，队列非空时不执行 CLUI/STUI。

> 注：更大的 ring（如 1 MiB）能进一步减少睡眠次数，但会破坏与对照组
> 64 KiB 的公平性，故不采用。

---

## 10. 关键设计决策小结

1. **数据面与通知面分离**：shm 只负责字节环形缓冲，UINTR 只负责"有事了"
   的二进制信号；丢通知由 token.seq 序号与执行器 CLUI 临界区共同兜底。
2. **中断 handler 极简**：只做 seq++、waker 入队、WAKE_FLAG=1 三件事，
   全部无锁无 I/O。
3. **async 语义零成本接入**：UintrFuture 就是标准的 seq 比对 future，
   通道 API 与 tokio 读写形态一致，便于公平对照。
4. **无第三方 crate**：executor/reactor/信号/日志/参数解析/矩阵运算全部
   手写，libc FFI 直连，既满足约束也避免了 std 锁在中断上下文的死锁。
5. **flags=0 + 延迟 stui**：保证 UINTR 不干扰阻塞式 syscall，是整套机制
   能稳定运行的前提。
