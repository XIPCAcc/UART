# 多核 async 运行时与爬虫 — 设计文档

## 1. 概述

一个**仅依赖 Rust 标准库**（无 tokio / mio / clap 等外部 crate）的多核协作式异步运行时，
及运行其上的定时批量 HTTP 爬虫。运行时在启动时探测可用 CPU 核数作为 worker 上限，
运行中根据全局就绪队列深度**动态扩容**起新 worker 线程，任务不足时动态 worker
**立即退出释放 CPU 核**；常驻 worker 0 在空闲时亲自驱动 epoll reactor。

- 并发原语：`Future` + 自实现的 `Waker`/`RawWakerVTable` + 非阻塞 I/O
- I/O 多路复用：`epoll`（边沿触发）+ `eventfd`（跨线程唤醒）+ 定时器最小堆
- 目标场景：任务量随时间波动（爬虫固定间隔投放、突发 burst）

## 2. 约束与设计目标

| 项 | 决策 |
|---|---|
| 依赖 | 仅 Rust std；epoll/eventfd/socket/termios 全部 raw libc FFI（`src/sys.rs`） |
| 核数探测 | `std::thread::available_parallelism()`（受 taskset 亲和性影响） |
| 任务模型 | 协作式 future，必须 `Send + 'static`（可被任意 worker poll） |
| 扩容粒度 | 每次入队最多起 1 个 worker；burst 内连续入队即可扩到上限 |
| 缩容策略 | 动态 worker 发现队列空 → 立即退出，**无宽限等待** |
| reactor | 不设专用 IO 线程，worker 0 空闲时兼任 |
| 信号 | SIGINT/SIGTERM 置全局原子标志，最迟 100ms 被 worker 0 轮询到 |

## 3. 总体架构

```
                         ┌────────────────────────────────────┐
                         │  Executor (全局单例, &'static)       │
                         │  queue: Mutex<VecDeque<Arc<Task>>>  │
                         │  active_workers / max_workers       │
                         │  reactor: Reactor                   │
                         └───────────────┬────────────────────┘
            push/wake（Mutex + poke）    │
        ┌──────────────────────┬─────────┴──────────┬──────────────────┐
        ▼                      ▼                    ▼                  ▼
  worker 0（常驻）       worker 1 … N（动态）   epoll 实例          eventfd
  队列空时: reactor.wait()  有任务: pop→poll   EPOLLIN/OUT (ET)    跨线程唤醒
  队列非空: 每 1ms          队列空: 立即 exit   定时器最小堆         worker 0
    wait(0) 非阻塞 tick                                              ──────
        │
        └─ epoll_wait 事件/定时器到期 → waker.wake() → 任务回队列
```

## 4. 核心组件

### 4.1 Executor（`src/executor.rs`）

**线程组成**

- **worker 0**：`block_on` 的调用线程，常驻不退出。队列空时阻塞在
  `reactor.wait(100ms)` 等 I/O 事件/定时器/eventfd；队列非空时每 `IO_TICK=1ms`
  插入一次 `wait(0)` 非阻塞轮询，防止等 I/O 的任务被就绪任务饿死
  （对齐 tokio worker 的周期 park）
- **动态 worker（id ≥ 1）**：扩容时创建，`pop_front()` 返回 `None` 即
  `active_workers -= 1` 并退出线程。无线程等待态
- 不预起线程：空队列即退出的策略下预起只会空耗创建/销毁成本

**就绪队列**：`Mutex<VecDeque<Arc<Task>>>` 全局单队列。每个 worker 每轮只
`pop_front()` 取一个任务，poll 结束再取下一个（无批量 take_all）。

**扩容判定 `maybe_grow(qlen)`**（带批处理阈值，抑制空建空退）

```
若 shutdown → 不扩
desired = ceil(qlen / GROW_TASKS_PER_WORKER)   // GROW_TASKS_PER_WORKER = 4
have = active_workers + 1                      // worker 0 常驻，始终算 1 个
若 desired <= have → 不扩
否则 CAS(active_workers: a → a+1) 抢到名额 → spawn_worker()
名额上限 max_workers - 1（worker 0 已占一个核）
```

- worker 0 始终算 1 个劳动力：它正在 poll 时 poll 完即回队列，阻塞在
  epoll_wait 时由 push 的无条件 poke 叫醒——任务在 worker 0 上被 spawn
  （如 crawler 协程投放 burst）时不会误判"无空闲劳动力"
- 队列深度按每 4 个任务配 1 个 worker 批量推算：零星回队的 I/O 任务由
  现有 worker 消化，只有任务持续堆积才扩容
- 用 `compare_exchange_weak` 循环处理多线程同时扩容的竞争
- burst 投放是 crawler 协程同步 for 循环中连续 spawn，队列深度迅速累积，
  desired 随之阶梯式增长，一批投放内即可扩到 队列深度/4 对应数量（受核数
  上限封顶）；任务首次 poll 多为 connect Pending，堆积的队列由扩出的
  worker 并行消化
- 计数与线程真实存活有极短窗口，极端情况可能多起一个 worker：不会超上限，
  多余 worker 发现队列空会立即退出

**Task 状态机**（每 task 一把 `Mutex`，poll 期间不持锁）

| 字段 | 含义 |
|---|---|
| `future: Option<Pin<Box<dyn Future + Send>>>` | pending 时存回，poll 时 take 出 |
| `queued` | 已在就绪队列（防重复入队 / double-wake） |
| `polling` | 正被某 worker poll |
| `resched` | poll 期间被唤醒 → poll 方负责重新入队（延迟重排） |

唤醒路径 `wake_by_ref_`：

1. 已完成（future=None 且非 polling）→ 丢弃
2. `polling` 中 → 只置 `resched`，不入队（杜绝两个 worker 并发 poll 同一 future）
3. 已 `queued` → 跳过
4. 否则置 queued 并 `push_task`（入队 + poke + 扩容判定）

**观测计数**：`spawned_total` / `completed_total` 仅 Relaxed 原子计数，
由 worker 0 每 `STATUS_INTERVAL=1s` 打印 `[STATUS]` 行。

### 4.2 Reactor（`src/reactor.rs`）

`Mutex<Inner>` 提供 `&self` 多线程安全接口：

- **fd 管理**：`add(fd)` 设非阻塞 + ET 注册 EPOLLIN；`modify_readable/writable`
  按方向追加掩码并登记 `Waker`（token = `fd*2` 读 / `fd*2+1` 写）
- **事件收割 `wait(cap_ms)`**：
  1. 锁内按定时器堆顶算 epoll_wait 超时（无定时器用 cap_ms）
  2. 解锁进内核 `epoll_wait`
  3. 锁内取出就绪 token 的 waker + 弹出到期定时器，**锁外** `wake()`（减锁争用）
- **定时器**：`BinaryHeap`（逆序比较，最早 deadline 在堆顶）；
  `Sleep::poll` 未到点则 `register_timer(deadline, waker)` 返回 Pending
- **eventfd**：水平触发，data=`u64::MAX` 特殊 token。三个 poke 点：
  - 任务入队（worker 0 可能在 epoll_wait）
  - 注册新定时器（可能比当前超时更早）
  - shutdown / TERM
  被读到后 `eventfd_drain` 清零；冗余 poke 只累积计数无副作用
- **TERM 处理**：worker 0 每次 wait 返回后检查 `signal::TERM`，置位则
  `wake_all()` 唤醒全部等 I/O/定时器的任务；future（如 `Sleep`）在 poll 开头
  自查 TERM 直接 Ready，顶层任务感知后正常走完退出路径

### 4.3 非阻塞 TCP（`src/tcp.rs`）

`AsyncTcpStream::connect(addr)`：

1. `socket()` + `set_nonblocking` + `connect()`（预期 EINPROGRESS）
2. 向 reactor 注册 EPOLLOUT，poll 等可写
3. 可写后 `getsockopt(SO_ERROR)` 确认连接建立

`read` / `write_all` 同样以 `WouldBlock → Pending 注册 → 事件唤醒重 poll` 的
标准 future 模式实现，供任意 worker 驱动。

### 4.4 爬虫（`src/crawler.rs`）

- 顶层协程循环：检查 TERM → 投放 `burst` 个 `crawl_one` 任务到全局队列 →
  `sleep_ms(interval).await`
- `crawl_one`：非阻塞 TCP connect → 发 HTTP/1.0 GET（`Connection: close`）→
  读到 EOF → 首块数据解析状态行状态码
- 统计（全局原子）：成功数、失败数、总字节数；TERM 退出时打印汇总
- 固定时间间隔 + 每轮固定任务数，形成"周期性突发负载"，正好压测扩缩容

### 4.5 信号（`src/signal.rs`）

`libc::signal()` 注册 SIGINT/SIGTERM 处理函数，只做一件事：
`TERM.store(true)`。所有等待点通过轮询/重 poll 感知，不在 handler 中做复杂操作。

## 5. 关键流程

### 5.1 一次 burst 的扩容时序（burst=32，8 核）

```
worker 0 poll crawler 协程，同步投放（have = active + 1）：
  spawn#1..#4  → qlen≤4，desired=1 ≤ have=1 → 不扩容，任务留队列
  spawn#5      → qlen=5, desired=2 > 1 → worker 1 创建
  spawn#9      → desired=3 > 2 → worker 2 创建
  …每 4 个任务新增 1 个 worker，qlen=25 时扩到 worker 7（达上限 cap=7）
crawler 协程 sleep_ms(interval).await → Pending
worker 0 回队列与动态 worker 一起 drain；任务多为 connect Pending 挂到 reactor
队列被取空后动态 worker 立即 exit（缩容）
I/O 就绪后 worker 0 的 epoll 收割事件 → waker 推任务回队列
  → 回队任务零星时 desired 不超现有劳动力，不重建；堆积才再扩容
```

### 5.2 关闭流程

```
SIGTERM → TERM=true
worker 0 wait 返回后看到 TERM → wake_all()
Sleep 重新 poll 看到 TERM → Ready → crawler 循环退出（打印汇总）→ 顶层 future Ready
→ initiate_shutdown()（is_shutdown=true + poke）
worker 0 退出 worker_loop；动态 worker 在下次循环顶部看到标志退出
block_on join 全部 worker 句柄 → reactor.shutdown()
task panic 路径：catch_unwind 捕获 → 记录 payload → initiate_shutdown
  → join 后 resume_unwind 重新抛出
```

## 6. 文件结构

| 文件 | 职责 |
|---|---|
| `src/main.rs` | 入口，按 `--mode` 分发 receiver/sender/crawler |
| `src/cli.rs` | 手工参数解析（无 clap） |
| `src/executor.rs` | 多核执行器：队列、worker 池、扩缩容、Task/Waker |
| `src/reactor.rs` | epoll + 定时器 + eventfd + `sleep_ms` |
| `src/tcp.rs` | 非阻塞异步 TCP（connect/read/write_all） |
| `src/crawler.rs` | 定时批量 HTTP 爬虫与结果统计 |
| `src/signal.rs` | SIGINT/SIGTERM → 原子 TERM |
| `src/sys.rs` | epoll/eventfd/socket 等 raw FFI 封装 |
| `src/serial.rs`、`src/receiver.rs`、`src/sender.rs` | 串口收发/矩阵乘（既有模式） |
| `test_server.py` | 最小 HTTP/1.0 测试服务 |
| `run_crawler.sh` | 一键编译 + 起测试服务 + 绑核运行 |

## 7. 构建与运行

```bash
cargo build --release

# 一键脚本（自动编译、自动起 test_server.py、绑核、固定时长后 SIGTERM）
./run_crawler.sh [INTERVAL_MS] [BURST] [DURATION_S] [ADDR] [PATH] [CPUS]
./run_crawler.sh 500 10 10                    # 默认：500ms 投放 10 个，跑 10s
./run_crawler.sh 1 200 6                       # 高压：1ms 投放 200 个
./run_crawler.sh 5000 2 12                     # 长间隔：观察缩容

# 手工运行
python3 test_server.py 19090 &
taskset -c 0-3 ./target/release/uart-matmul \
    --mode crawler --addr 127.0.0.1:19090 --interval 500 --burst 10
```

参数：`--addr HOST:PORT`、`--path /`、`--interval/-i 毫秒`、`--burst 每轮任务数`。
`taskset` 改变 CPU 亲和性后，`available_parallelism()` 随之变化，即 worker 上限。

## 8. 日志观测

| 前缀 | 含义 |
|---|---|
| `[WORKER] create id=N (active workers: X, max: M)` | 扩容：新 worker 线程创建 |
| `[WORKER] exit id=N queue empty (active workers: X)` | 缩容：队列空，线程退出释放核 |
| `[STATUS] queue=Q workers=X/M spawned=S completed=C in-flight=F` | 每秒状态行（worker 0） |
| `[CRAWLER] dispatched N tasks (workers: X)` | 一轮投放 |
| `[CRAWL] #seq OK/FAIL status=.. bytes=.. elapsed=..` | 单任务抓取结果 |
| `[CRAWLER] summary: ok=.. fail=.. total_bytes=..` | TERM 退出汇总 |

`in-flight = spawned - completed`，稳态下应约等于 reactor 上等待 I/O/定时器
的任务数（含顶层 crawler 协程自身）。

## 9. 已知局限与可改进

- **全局单队列 + Mutex**：worker 数很少（=核数）时争用可忽略；超大核数可改
  分片队列 + work-stealing
- **立即缩容无宽限**：等 I/O 期间队列空也会销毁线程，响应回来再重建；
  线程创建开销（数十 µs）相对网络抓取可接受，若以短 I/O 为主可加短宽限/保活数
- **单 reactor 由 worker 0 驱动**：I/O 注册/唤醒经一把 Mutex；超高 fd 规模可
  分片 epoll
- **无 coop budget**：单个 future 内若不放 await 点会长时间占住 worker（爬虫任务
  均为短轮次，暂无问题）
- ET 模式下注册的是 fd 级 waker（非任务级多路复用），满足当前连接生命周期模型
