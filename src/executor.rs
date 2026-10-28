// 多核 async 执行器 — 全局就绪队列 + 动态扩缩容 worker 池
//
// 架构：
//   - block_on() 启动时按 available_parallelism() 确定最大 worker 数 max_workers
//     （核数上限）。调用线程作为常驻 worker 0，它同时兼任 reactor：
//     队列空时由 worker 0 亲自调用 reactor.wait()（epoll_wait），
//     不再单独创建 IO 线程；I/O 事件就绪后 waker 把任务推回全局队列，
//     由 worker 0 drain 并按需扩容分给动态 worker
//   - per-worker 本地就绪队列（max_workers 个 Vyukov MPMC 环，各 256 槽）
//     + 一个全局溢出环（2^16）：入队轮询分发到各本地队列，把旧版「全部
//     worker 抢同一个队头缓存行」的 CAS 热点摊到 N 个缓存行；worker 取任务
//     顺序：本地 → 溢出 → 扫描窃取他人队列。不变量：worker 只有在全队列
//     扫描为空后才退出/休眠，且 pusher 推完必继续自己的循环扫描，任务不会
//     滞留在无人消费的队列里
//   - 无 eventfd：任务入队不唤醒 worker 0（push 方本身就是 worker，push 后
//     会继续 pop，队列非空期间不存在「无人消费」的窗口）；关闭/shutdown
//     标志由 worker 0 的 epoll_wait 超时（WORKER_IO_WAIT_MS）兜底轮询
//
// 动态 worker 用 std::thread::spawn 创建（栈大小 WORKER_STACK_SIZE），
// 退出后由 block_on 收尾路径 join 干净
//
// 动态扩缩容（负载随时间波动，如定时投放的爬虫任务）：
//   - 扩容：任务入队时按 队列深度 / GROW_TASKS_PER_WORKER 向上取整推算所需
//     worker 数，超过现有劳动力（动态 worker + 空闲的 worker 0）才起新线程，
//     抑制"来一个任务建一个线程、首次 poll Pending 后又空退"的抖动
//   - 缩容：动态 worker（id != 0）一旦发现队列空就立即减少 active 计数并
//     退出线程释放 CPU 核，不做宽限等待；下一波任务到来时由扩容路径重新
//     创建线程。worker 0 永不退出，队列空时阻塞在 epoll_wait 等事件
//   - 忙时 tick（类似 tokio 的周期 park）：即使队列持续非空，worker 0 每隔
//     IO_TICK 也会用 timeout=0 非阻塞轮询一次 epoll，收割已就绪的 I/O 事件，
//     避免等 I/O 的任务被持续到达的就绪任务饿死
//   - worker 0 在空闲轮次 join 已退出的动态 worker（回收线程句柄）
//
// Task 并发协议（无锁状态机，单 AtomicU8 CAS，poll 不持锁）：
//   - QUEUED:   在就绪队列中（防止重复入队）
//   - POLLING:  正被某 worker 独占 poll（future 已被取出，归该 worker 所有）
//   - RESCHED:  poll 期间被唤醒 → 由 poll 方在结束后重新入队
//   - DONE:     future 已完成，此后唤醒一律丢弃
//   同一 task 任意时刻最多一个 worker 在 poll，杜绝并发 poll 同一 future。
//
// 退出：顶层任务完成 → initiate_shutdown()（仅置标志）→ worker 0 最迟在
//   epoll_wait 超时后看到标志退出；动态 worker 由 block_on 收割干净后返回。

use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::{Duration, Instant};

use crate::log;
use crate::reactor::Reactor;
use crate::signal;

// ── 全局执行器指针 ─────────────────────────────────────────────

static EXECUTOR_PTR: std::sync::atomic::AtomicPtr<Executor> =
    std::sync::atomic::AtomicPtr::new(null_mut());

pub(crate) fn executor() -> &'static Executor {
    let p = EXECUTOR_PTR.load(Ordering::Relaxed);
    assert!(!p.is_null(), "executor() called before block_on");
    unsafe { &*p }
}

/// worker 0 阻塞在 epoll_wait 的超时上限（ms）：
/// 保证 SIGINT/SIGTERM 最迟在该间隔内被检查到
const WORKER_IO_WAIT_MS: i32 = 10000;

/// worker 0 忙时驱动 reactor 的 tick 间隔：
/// 队列持续非空时每隔该时长非阻塞轮询一次 epoll（timeout=0），
/// 让 I/O 事件不被就绪任务饿死（对应 tokio worker 的周期 park）
const IO_TICK: Duration = Duration::from_millis(1);

/// worker 0 打印运行状态行的间隔（队列深度/worker 数/任务计数）
const STATUS_INTERVAL: Duration = Duration::from_secs(1);

/// 扩容批处理阈值：每攒够这么多个排队任务才"值得"新增一个 worker。
/// 没有该阈值时，任务一个到达就起一个线程，而任务首次 poll 往往立刻 Pending
/// （等 connect/IO），worker 转头发现队列空又马上退出——频繁空建空退。
/// 现在按 队列深度 / GROW_TASKS_PER_WORKER 向上取整 推算需要的 worker 数，
/// 零星回队的 I/O 任务由现有 worker 消化，只有持续堆积才扩容。
const GROW_TASKS_PER_WORKER: usize = 4;

/// 全局溢出环容量（2 的幂，初始化时一次性分配）。
/// 本地队列全部满时才用到，正常工况不会触发
const QUEUE_CAP: usize = 1 << 16;

/// 每个 worker 本地队列容量（2 的幂）。burst 经轮询分发后每队列
/// 典型深度远小于此；满时回退去下一个队列，最终兜底全局溢出环
const LOCAL_CAP: usize = 1 << 8;

/// 入队轮询分发计数器（全局共享，取模决定目标本地队列）
static PUSH_RR: AtomicUsize = AtomicUsize::new(0);

/// worker 启动时分配本地队列下标的计数器（worker 0 固定为 0）
static Q_ASSIGN: AtomicUsize = AtomicUsize::new(1);

/// 动态 worker 栈大小。crawler future 栈帧很小，2MB 与 std 线程默认一致；
/// 虚拟内存惰性分配，不占物理页
const WORKER_STACK_SIZE: usize = 2 << 20;

// ── Vyukov 有界 MPMC 无锁队列 ──────────────────────────────────
// 经典算法（Dmitry Vyukov bounded MPMC queue）：
// 每槽一个序号 seq（初始等于槽位置），生产者/消费者用序号握手：
//   生产者：slot.seq == pos    → 槽空可占；seq < pos    → 满
//   消费者：slot.seq == pos+1 → 槽有效；seq > pos+1    → 空
// 序号单调递增回绕，不需 ABA 处理（和位置耦合）；定长数组无内存回收问题。
// 头尾指针分缓存行，防 false sharing。

struct Slot {
    seq:  AtomicUsize,
    data: std::cell::UnsafeCell<usize>, // Arc::into_raw 转成的 usize
}

#[repr(align(64))]
struct CachePadded(AtomicUsize);

struct Ring {
    slots: Box<[Slot]>,
    mask:  usize,
    head:  CachePadded, // dequeue_pos
    tail:  CachePadded, // enqueue_pos
}

// 安全性：Vyukov 算法保证任意时刻槽位访问串行化（序号握手）
unsafe impl Send for Ring {}
unsafe impl Sync for Ring {}

impl Ring {
    fn new(cap: usize) -> Self {
        assert!(cap.is_power_of_two() && cap >= 2);
        let mut slots = Vec::with_capacity(cap);
        for i in 0..cap {
            slots.push(Slot {
                seq:  AtomicUsize::new(i),
                data: std::cell::UnsafeCell::new(0),
            });
        }
        Ring {
            slots: slots.into_boxed_slice(),
            mask: cap - 1,
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
        }
    }

    /// 入队（Arc::into_raw 的指针）。满时自旋等待消费者腾出槽位
    /// （仅用于全局溢出环，正常工况不会触发）
    fn push_ptr(&self, ptr: usize) {
        let mut pos = self.tail.0.load(Ordering::Relaxed);
        let mut backoff = 1u32; // CAS 竞争退避：失败后空转 1→2→…→64 次再重试
        loop {
            let slot = &self.slots[pos & self.mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos) as isize;
            if diff == 0 {
                // 期望序号匹配 → 尝试 claim 这个槽
                match self.tail.0.compare_exchange_weak(
                    pos,
                    pos + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        unsafe { *slot.data.get() = ptr };
                        slot.seq.store(pos + 1, Ordering::Release);
                        return;
                    }
                    Err(p) => {
                        pos = p;
                        for _ in 0..backoff {
                            std::hint::spin_loop();
                        }
                        backoff = (backoff << 1).min(64);
                    }
                }
            } else if diff < 0 {
                // 满：seq < pos 说明消费者还没释放到这一位置，自旋等待
                std::hint::spin_loop();
                pos = self.tail.0.load(Ordering::Relaxed);
            } else {
                // 这个位置还没轮到我（别的生产者已 claim 但未写入）
                pos = self.tail.0.load(Ordering::Relaxed);
            }
        }
    }

    /// 非阻塞入队：满时返回 false 而非自旋（本地队列用，调用方负责回退）
    fn try_push_ptr(&self, ptr: usize) -> bool {
        let mut pos = self.tail.0.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[pos & self.mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos) as isize;
            if diff == 0 {
                match self.tail.0.compare_exchange_weak(
                    pos,
                    pos + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        unsafe { *slot.data.get() = ptr };
                        slot.seq.store(pos + 1, Ordering::Release);
                        return true;
                    }
                    Err(p) => pos = p,
                }
            } else if diff < 0 {
                return false; // 满
            } else {
                pos = self.tail.0.load(Ordering::Relaxed);
            }
        }
    }

    /// 出队。空时返回 None。
    fn pop(&self) -> Option<Arc<Task>> {
        let mut pos = self.head.0.load(Ordering::Relaxed);
        let mut backoff = 1u32; // 同 push：竞争退避
        loop {
            let slot = &self.slots[pos & self.mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos + 1) as isize;
            if diff == 0 {
                // 期望序号匹配 → 尝试 claim 这个槽
                match self.head.0.compare_exchange_weak(
                    pos,
                    pos + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let ptr = unsafe { *slot.data.get() };
                        slot.seq.store(pos + self.mask + 1, Ordering::Release);
                        return Some(unsafe { Arc::from_raw(ptr as *const Task) });
                    }
                    Err(p) => {
                        pos = p;
                        for _ in 0..backoff {
                            std::hint::spin_loop();
                        }
                        backoff = (backoff << 1).min(64);
                    }
                }
            } else if diff < 0 {
                return None; // 空
            } else {
                // 别的消费者已 claim
                pos = self.head.0.load(Ordering::Relaxed);
            }
        }
    }
}

// ── Task（无锁状态机）──────────────────────────────────────────
//
// state 位（AtomicU8，所有转换用 CAS 完成，无锁）：
//   QUEUED   在就绪队列中（防止重复入队）
//   POLLING  正被某 worker 独占 poll（future 已被取出，归该 worker 所有）
//   RESCHED  poll 期间被唤醒 → poll 方结束后负责重新入队
//   DONE     future 已完成（此后唤醒一律丢弃）
//
// future 放在 UnsafeCell：只有成功 CAS 获得 POLLING 的 worker 能取出/放回
// future，唤醒路径永不触碰——POLLING 位即 future 的独占所有权凭证。

const ST_QUEUED:  u8 = 1 << 0;
const ST_POLLING: u8 = 1 << 1;
const ST_RESCHED: u8 = 1 << 2;
const ST_DONE:    u8 = 1 << 3;

struct Task {
    #[allow(dead_code)]
    id:     usize,
    state:  AtomicU8,
    future: UnsafeCell<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
}

// 安全性：future 的访问由 state 状态机独占控制（见上）
unsafe impl Send for Task {}
unsafe impl Sync for Task {}

static TASK_ID: AtomicUsize = AtomicUsize::new(0);

// ── 执行器 ─────────────────────────────────────────────────────

pub struct Executor {
    /// per-worker 本地就绪队列（下标 0 属 worker 0）
    locals:      Box<[Ring]>,
    /// 全局溢出环：本地队列全满时的兜底
    overflow:    Ring,
    /// 全队列任务总数（入队 +1 / 出队 -1）：扩容启发与状态观测用
    queued_total: AtomicUsize,
    pub reactor: Reactor,
    /// 启动时确定的可用核数（worker 上限）
    max_workers: usize,
    /// 动态 worker 数（不含 worker 0）
    active_workers: AtomicUsize,
    is_shutdown: AtomicBool,
    next_worker_id: AtomicUsize,
    /// 动态 worker 的线程句柄，收尾与到期回收用
    thread_handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
    /// 累计 spawn 的任务数（观测用）
    spawned_total: AtomicUsize,
    /// 累计执行完成（Poll::Ready）的任务数（观测用）
    completed_total: AtomicUsize,
    /// worker 内 task panic 的 payload，block_on 末尾重新抛出
    panicked: Mutex<Option<Box<dyn std::any::Any + Send>>>,
}

impl Executor {
    pub fn new() -> std::io::Result<Self> {
        let max_workers = std::thread::available_parallelism()
            .map(|v| v.get())
            .unwrap_or(1);
        Ok(Executor {
            locals:      (0..max_workers)
                .map(|_| Ring::new(LOCAL_CAP))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            overflow:    Ring::new(QUEUE_CAP),
            queued_total: AtomicUsize::new(0),
            reactor:     Reactor::new()?,
            max_workers,
            active_workers: AtomicUsize::new(0),
            is_shutdown:    AtomicBool::new(false),
            next_worker_id: AtomicUsize::new(1),
            thread_handles: Mutex::new(Vec::new()),
            spawned_total:   AtomicUsize::new(0),
            completed_total: AtomicUsize::new(0),
            panicked:       Mutex::new(None),
        })
    }

    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    /// 添加后台任务；Future 必须 Send（可能被任意 worker poll）
    pub fn spawn(&'static self, future: impl Future<Output = ()> + Send + 'static) {
        let id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        let task = Arc::new(Task {
            id,
            state:  AtomicU8::new(ST_QUEUED), // 创建即在队列中
            future: UnsafeCell::new(Some(Box::pin(future))),
        });
        self.spawned_total.fetch_add(1, Ordering::Relaxed);
        log!("[TASK] #{id} create");
        self.push_task(task);
    }

    /// 入队（无锁）+ 按需扩容。
    /// 入队只发生在某个 worker 的 poll 内部（spawn/wake），该 worker 返回
    /// 循环后会立刻 pop 消费；worker 0 若正阻塞在 epoll_wait，说明队列此刻
    /// 为空，其 epoll 集合里必有该任务的 fd，唤醒来自 I/O 事件本身。
    fn push_task(&'static self, task: Arc<Task>) {
        // 轮询分发到某个本地队列（摊平全局队头的 CAS 热点）；
        // 目标满则顺次试下一个，全满兜底全局溢出环
        let ptr = Arc::into_raw(task) as usize;
        let n = self.max_workers;
        let start = PUSH_RR.fetch_add(1, Ordering::Relaxed) % n;
        let mut k = 0;
        while k < n {
            if self.locals[(start + k) % n].try_push_ptr(ptr) {
                break;
            }
            k += 1;
        }
        if k == n {
            self.overflow.push_ptr(ptr);
        }
        let qlen = self.queued_total.fetch_add(1, Ordering::Relaxed) + 1;
        self.maybe_grow(qlen);
    }

    /// 取下一个任务：本地队列 → 全局溢出 → 扫描窃取他人队列。
    /// 返回 None 当且仅当所有队列此刻均为空（worker 据此退出/休眠，
    /// 配合「pusher 推完必继续扫描」保证无任务滞留）
    fn next_task(&self, my_q: usize) -> Option<Arc<Task>> {
        let t = if let Some(t) = self.locals[my_q].pop() {
            Some(t)
        } else if let Some(t) = self.overflow.pop() {
            Some(t)
        } else {
            let n = self.locals.len();
            let mut found = None;
            for k in 1..n {
                if let Some(t) = self.locals[(my_q + k) % n].pop() {
                    found = Some(t);
                    break;
                }
            }
            found
        };
        if t.is_some() {
            self.queued_total.fetch_sub(1, Ordering::Relaxed);
        }
        t
    }

    /// 扩容条件：按队列深度推算需要的 worker 数：
    ///   desired = ceil(qlen / GROW_TASKS_PER_WORKER)
    /// 现有劳动力 = 动态 worker + worker 0。worker 0 常驻、永远会回到队列
    /// （正在 poll 时 poll 完即回；阻塞在 epoll_wait 说明队列已空，入队方自己会继续 pop 消费），
    /// 因此始终算 1 个，避免任务在 worker 0 上 spawn 时误判"无空闲劳动力"
    /// 而空建一个马上退出的线程。desired 超过现有劳动力才起新 worker；
    /// 未达核数上限前每次调用最多起 1 个（burst 连续入队会逐步扩满）。
    fn maybe_grow(&'static self, qlen: usize) {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return;
        }
        let have = self.active_workers.load(Ordering::Relaxed) + 1; // +1 = worker 0
        let desired = (qlen + GROW_TASKS_PER_WORKER - 1) / GROW_TASKS_PER_WORKER;
        if desired <= have {
            return;
        }
        let cap = self.max_workers.saturating_sub(1); // 动态 worker 名额
        let mut a = self.active_workers.load(Ordering::Relaxed);
        while a < cap {
            match self.active_workers.compare_exchange_weak(
                a,
                a + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.spawn_worker();
                    return;
                }
                Err(x) => a = x,
            }
        }
    }

    /// 起一个动态 worker 线程（std::thread）；失败时回滚 active_workers 计数
    fn spawn_worker(&'static self) {
        let id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        log!(
            "[WORKER] create id={id} (active: {})",
            self.active_workers.load(Ordering::Relaxed)
        );
        match std::thread::Builder::new()
            .stack_size(WORKER_STACK_SIZE)
            .spawn(move || self.thread_worker_body(id))
        {
            Ok(h) => self.thread_handles.lock().unwrap().push(h),
            Err(e) => {
                crate::elog!("[WORKER] std::thread spawn failed: {e}");
                self.active_workers.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// worker 线程体：panic 兜底传播 + 退出前递减 active_workers
    fn thread_worker_body(&'static self, id: usize) {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.worker_loop(id);
        }));
        if let Err(p) = r {
            let mut g = self.panicked.lock().unwrap();
            if g.is_none() {
                *g = Some(p);
            }
            drop(g);
            self.initiate_shutdown();
        }
        self.active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    /// 置关闭标志。无 eventfd：worker 0 最迟在 epoll_wait 超时
    /// （WORKER_IO_WAIT_MS）后看到标志退出，动态 worker 每轮循环头自查。
    fn initiate_shutdown(&self) {
        self.is_shutdown.store(true, Ordering::Release);
    }

    /// 运行事件循环直到顶层 future 完成（或 TERM 信号使其提前完成）
    pub fn block_on<F, R>(&'static self, f: F) -> R
    where
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        EXECUTOR_PTR.store(self as *const _ as *mut Executor, Ordering::Relaxed);

        // 没有专用 IO 线程：worker 0（本线程）队列空时亲自跑 epoll_wait。
        // 不预起动态 worker：队列一空就立即退出，预起只会白白创建再销毁，
        // 其余 worker 全部由 spawn 的扩容路径按需创建。
        // 顶层 future 包成任务：完成时记录结果并触发全局关闭
        let result: Arc<Mutex<Option<R>>> = Arc::new(Mutex::new(None));
        let top = TopFuture {
            f: Box::pin(f),
            result: result.clone(),
        };
        self.spawn(top);

        self.worker_loop(0);

        // worker 0 退出说明已 shutdown：等其余 worker 归零后 join 干净再关 reactor。
        while self.active_workers.load(Ordering::Acquire) != 0 {
            std::hint::spin_loop();
        }
        let handles: Vec<_> = self.thread_handles.lock().unwrap().drain(..).collect();
        for h in handles {
            let _ = h.join();
        }
        self.reactor.shutdown();

        // task panic 传播给 block_on 调用者
        if let Some(p) = self.panicked.lock().unwrap().take() {
            std::panic::resume_unwind(p);
        }
        let r = result
            .lock()
            .unwrap()
            .take()
            .expect("top future did not produce a result");
        r
    }

    /// worker 主循环：取任务 → poll。
    ///   - 动态 worker（id != 0）：一旦发现队列空就立即退出线程释放核，
    ///     不做宽限等待；下一波任务由 spawn 的扩容路径重新起线程消化
    ///   - worker 0（调用线程）：永不退出，队列空时兼任 reactor，亲自阻塞在
    ///     epoll_wait 上等 I/O 事件（关闭靠超时轮询）；队列非空时
    ///     每隔 IO_TICK 也会非阻塞轮询一次 epoll（忙时 tick）；TERM 信号
    ///     最迟 WORKER_IO_WAIT_MS 被轮询到并唤醒所有等待中的任务
    fn worker_loop(&'static self, id: usize) {
        // 本地队列下标：worker 0 固定 0；动态 worker 启动时轮询分配
        // （不同活 worker 可能撞同一队列，Vyukov MPMC 下只是略增竞争，无害）
        let my_q = if id == 0 {
            0
        } else {
            Q_ASSIGN.fetch_add(1, Ordering::Relaxed) % self.max_workers
        };
        let mut last_io_tick = Instant::now();
        let mut last_status = Instant::now();
        loop {
            if self.is_shutdown.load(Ordering::Acquire) {
                return;
            }
            // worker 0 周期打印运行状态（队列深度/worker 数/任务计数）
            if id == 0 && last_status.elapsed() >= STATUS_INTERVAL {
                last_status = Instant::now();
                let qlen = self.queued_total.load(Ordering::Relaxed);
                let workers = self.active_workers.load(Ordering::Relaxed) + 1;
                let spawned = self.spawned_total.load(Ordering::Relaxed);
                let done = self.completed_total.load(Ordering::Relaxed);
                log!(
                    "[STATUS] queue~{qlen} workers={workers}/{} spawned={spawned} completed={done} in-flight={}",
                    self.max_workers,
                    spawned.saturating_sub(done)
                );
            }
            match self.next_task(my_q) {
                Some(t) => self.poll_task(id, t),
                None if id != 0 => {
                    // 任务不足：立即退出线程释放 CPU 核
                    // （active_workers 由线程入口 thread_worker_body 统一递减）
                    log!("[WORKER] exit id={id} queue empty");
                    return;
                }
                None => {
                    // worker 0：先 join 已退出的动态 worker，再兼任 reactor。
                    // 队列此刻为空即无待消费任务；新任务到达必伴随 epoll 事件
                    // （监听 fd 或任务 fd 就绪），epoll_wait 会立即返回。
                    self.reap_workers();
                    let alive = self.reactor.wait(WORKER_IO_WAIT_MS);
                    last_io_tick = Instant::now();
                    if !alive || self.is_shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    if signal::TERM.load(Ordering::Relaxed) {
                        // 唤醒等 I/O 的任务：它们重新 poll 时自查 TERM
                        self.reactor.wake_all();
                    }
                    // wait 返回期间 waker 已把就绪任务推回队列，回顶部 drain
                    continue;
                }
            }
            // ── 忙时 tick：worker 0 队列持续非空时也要周期驱动 reactor ──
            // 否则等 epoll 事件（网络/串口）的任务会被源源不断的
            // 就绪任务饿死。timeout=0 是非阻塞收割，不影响任务吞吐，
            // 只有距上次 tick 满 IO_TICK 才做一次（~1 次/ms）。
            if id == 0 && last_io_tick.elapsed() >= IO_TICK {
                let alive = self.reactor.wait(0);
                last_io_tick = Instant::now();
                if !alive || self.is_shutdown.load(Ordering::Acquire) {
                    return;
                }
                if signal::TERM.load(Ordering::Relaxed) {
                    self.reactor.wake_all();
                }
            }
        }
    }

    /// 回收已退出的动态 worker：join 掉句柄，防止无限累积。
    /// （线程栈由 glibc 的栈缓存兜底复用，无需手工 munmap）
    fn reap_workers(&self) {
        let mut handles = self.thread_handles.lock().unwrap();
        let mut i = 0;
        while i < handles.len() {
            if handles[i].is_finished() {
                let h = handles.swap_remove(i);
                let _ = h.join();
            } else {
                i += 1;
            }
        }
    }

    /// poll 一个任务；panic 时记录 payload 并触发全局关闭
    fn poll_task(&'static self, worker_id: usize, task: Arc<Task>) {
        let id = task.id;
        // 出队时任务处于 QUEUED；CAS 置 POLLING 后即独占 future
        // （此处不应有竞争：QUEUED 保证只有本线程能走到这）
        task.state
            .compare_exchange(
                ST_QUEUED,
                ST_POLLING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .expect("popped task not in QUEUED state");
        // 安全性：持有 POLLING 位，独占访问 future
        let mut fut = unsafe { &mut *task.future.get() }
            .take()
            .expect("queued task without future");
        log!("[TASK] #{id} poll start on worker {worker_id}");
        let waker = task_waker(task.clone());
        let mut cx = Context::from_waker(&waker);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fut.as_mut().poll(&mut cx)
        }));

        match result {
            Ok(Poll::Ready(())) => {
                // future 已 take 走随作用域 drop；置 DONE 后唤醒一律丢弃。
                // 此时不可能有 RESCHED：RESCHED 只在 POLLING 期间被设置，
                // 而 Ready 路径直接覆盖——若恰好有唤醒卡在 CAS 重试中，
                // 它会看到 DONE 并丢弃（见 wake_by_ref_）
                task.state.store(ST_DONE, Ordering::Release);
                self.completed_total.fetch_add(1, Ordering::Relaxed);
                log!("[TASK] #{id} ready");
            }
            Ok(Poll::Pending) => {
                // 安全性：仍持有 POLLING 位，独占放回 future
                unsafe { *task.future.get() = Some(fut) };
                // 一次性读出 poll 期间是否被唤醒并清 POLLING：
                // 有 RESCHED → 转为 QUEUED 由本线程重新入队；
                // 无 → 回到空闲态。此后新的唤醒会看到最新状态正确处理
                let prev = task
                    .state
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |s| {
                        debug_assert!(s & ST_POLLING != 0);
                        if s & ST_RESCHED != 0 {
                            Some(ST_QUEUED)
                        } else {
                            Some(0)
                        }
                    })
                    .unwrap();
                if prev & ST_RESCHED != 0 {
                    // poll 期间被唤醒：负责重新入队
                    log!("[TASK] #{id} pending (woken during poll, requeue)");
                    self.push_task(task);
                } else {
                    log!("[TASK] #{id} pending (waiting for io)");
                }
            }
            Err(p) => {
                task.state.store(ST_DONE, Ordering::Release);
                log!("[TASK] #{id} PANICKED, shutting down executor");
                *self.panicked.lock().unwrap() = Some(p);
                self.initiate_shutdown();
            }
        }
    }
}

// ── 顶层 future 包装 ───────────────────────────────────────────

struct TopFuture<R> {
    f:      Pin<Box<dyn Future<Output = R> + Send>>,
    result: Arc<Mutex<Option<R>>>,
}

impl<R: Send + 'static> Future for TopFuture<R> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // 安全性：TopFuture 本身被 pin，f 是 Pin<Box<..>>，移动 self 不会移动 f 指向的 future
        let f = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.f) };
        match f.poll(cx) {
            Poll::Ready(r) => {
                *self.result.lock().unwrap() = Some(r);
                executor().initiate_shutdown();
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// ── Waker 实现（Arc 版 RawWakerVTable）─────────────────────────

fn task_waker(task: Arc<Task>) -> Waker {
    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);
    let ptr = Arc::into_raw(task) as *const ();
    unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
}

unsafe fn clone_waker(data: *const ()) -> RawWaker {
    let rc = Arc::from_raw(data as *const Task);
    let cloned = rc.clone();
    let _ = Arc::into_raw(rc);
    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);
    RawWaker::new(Arc::into_raw(cloned) as *const (), &VTABLE)
}

unsafe fn wake(ptr: *const ()) {
    let task = Arc::from_raw(ptr as *const Task);
    task.wake_by_ref_();
}

unsafe fn wake_by_ref(ptr: *const ()) {
    let task = std::mem::ManuallyDrop::new(Arc::from_raw(ptr as *const Task));
    task.wake_by_ref_();
}

unsafe fn drop_waker(ptr: *const ()) {
    drop(Arc::from_raw(ptr as *const Task));
}

impl Task {
    /// 唤醒路径：可能运行在任意 worker 线程中。无锁 CAS 状态转换，不触碰
    /// future。CAS 失败即重读重试，整个循环通常一轮成功。
    fn wake_by_ref_(self: &Arc<Self>) {
        log!("wake_by_ref");
        let mut s = self.state.load(Ordering::Acquire);
        loop {
            if s & ST_DONE != 0 {
                return; // 已完成：丢弃这次唤醒
            }
            if s & ST_POLLING != 0 {
                // 正被 poll：只留 RESCHED 标记，由 poll 方结束后重新入队
                match self.state.compare_exchange_weak(
                    s,
                    s | ST_RESCHED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(x) => s = x,
                }
                continue;
            }
            if s & ST_QUEUED != 0 {
                return; // 已在队列中
            }
            // 空闲态：置 QUEUED 并入队
            match self.state.compare_exchange_weak(
                s,
                ST_QUEUED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(x) => s = x,
            }
        }
        executor().push_task(self.clone());
    }
}
