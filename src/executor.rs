// 多核 async 执行器 — 全局就绪队列 + 动态扩缩容 worker 池
//
// 架构：
//   - block_on() 启动时按 available_parallelism() 确定最大 worker 数 max_workers
//     （核数上限）。调用线程作为常驻 worker 0，它同时兼任 reactor：
//     队列空时由 worker 0 亲自调用 reactor.wait()（epoll_wait + 定时器），
//     不再单独创建 IO 线程；事件/定时器到期后 waker 把任务推回全局队列，
//     由 worker 0 drain 并按需扩容分给动态 worker
//   - 全局共享就绪队列：Mutex<VecDeque<Arc<Task>>>
//   - worker 0 与 epoll_wait 之间用 eventfd 唤醒：任何线程 push 任务/注册更早
//     的定时器/发起关闭时 poke，使正阻塞在 epoll_wait 的 worker 0 立即返回
//
// 动态扩缩容（负载随时间波动，如定时投放的爬虫任务）：
//   - 扩容：任务入队时若 队列深度 > 空闲 worker 数（worker 0 在 epoll_wait
//     算 1 个空闲）且未达 max，CAS 占名额后新起 worker 线程
//   - 缩容：动态 worker（id != 0）一旦发现队列空就立即减少 active 计数并
//     退出线程释放 CPU 核，不做宽限等待；下一波任务到来时由扩容路径重新
//     创建线程。worker 0 永不退出，队列空时阻塞在 epoll_wait 等事件
//   - 忙时 tick（类似 tokio 的周期 park）：即使队列持续非空，worker 0 每隔
//     IO_TICK 也会用 timeout=0 非阻塞轮询一次 epoll，收割已就绪的 I/O 事件
//     和到期定时器，避免等 I/O 的任务被持续到达的就绪任务饿死
//   - worker 0 在空闲轮次回收已退出 worker 的 JoinHandle
//
// Task 并发协议（每 task 一把 Mutex，poll 不持锁）：
//   - queued:   在就绪队列中（防止重复入队）
//   - polling:  正被某 worker poll（future 已被取出）
//   - resched:  poll 期间被唤醒 → 由 poll 方在结束后重新入队
//   同一 task 任意时刻最多一个 worker 在 poll，杜绝并发 poll 同一 future。
//
// 退出：顶层任务完成 → initiate_shutdown()（置标志 + poke eventfd）→
//   worker 0 与各动态 worker 退出并被 join。

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::{Duration, Instant};

use crate::reactor::Reactor;
use crate::signal;

// ── 全局执行器指针 ─────────────────────────────────────────────

static EXECUTOR_PTR: std::sync::atomic::AtomicPtr<Executor> =
    std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());

pub(crate) fn executor() -> &'static Executor {
    let p = EXECUTOR_PTR.load(Ordering::Relaxed);
    assert!(!p.is_null(), "executor() called before block_on");
    unsafe { &*p }
}

/// worker 0 阻塞在 epoll_wait 的超时上限（ms）：
/// 保证 SIGINT/SIGTERM 最迟在该间隔内被检查到
const WORKER_IO_WAIT_MS: i32 = 100;

/// worker 0 忙时驱动 reactor 的 tick 间隔：
/// 队列持续非空时每隔该时长非阻塞轮询一次 epoll（timeout=0），
/// 让 I/O 事件/定时器不被就绪任务饿死（对应 tokio worker 的周期 park）
const IO_TICK: Duration = Duration::from_millis(1);

/// worker 0 打印运行状态行的间隔（队列深度/worker 数/任务计数）
const STATUS_INTERVAL: Duration = Duration::from_secs(1);

// ── Task ───────────────────────────────────────────────────────

struct TaskState {
    /// None 表示已完成，或正被 worker 取出 poll（配合 polling 位区分）
    future:  Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    queued:  bool,
    polling: bool,
    resched: bool,
}

struct Task {
    #[allow(dead_code)]
    id:    usize,
    state: Mutex<TaskState>,
}

static TASK_ID: AtomicUsize = AtomicUsize::new(0);

// ── 执行器 ─────────────────────────────────────────────────────

pub struct Executor {
    queue:     Mutex<VecDeque<Arc<Task>>>,
    pub reactor: Reactor,
    /// 启动时确定的可用核数（worker 上限）
    max_workers: usize,
    /// 动态 worker 数（不含 worker 0）
    active_workers: AtomicUsize,
    /// worker 0 是否正阻塞在 epoll_wait（扩容判定时算 1 个空闲 worker）
    worker0_idle: AtomicBool,
    is_shutdown: AtomicBool,
    worker_handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
    next_worker_id: AtomicUsize,
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
            queue:     Mutex::new(VecDeque::new()),
            reactor:   Reactor::new()?,
            max_workers,
            active_workers: AtomicUsize::new(0),
            worker0_idle:   AtomicBool::new(false),
            is_shutdown:    AtomicBool::new(false),
            worker_handles: Mutex::new(Vec::new()),
            next_worker_id: AtomicUsize::new(1),
            spawned_total:   AtomicUsize::new(0),
            completed_total: AtomicUsize::new(0),
            panicked:       Mutex::new(None),
        })
    }

    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    /// 当前活跃的动态 worker 数（不含 worker 0）
    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Relaxed)
    }

    /// 添加后台任务；Future 必须 Send（可能被任意 worker poll）
    pub fn spawn(&'static self, future: impl Future<Output = ()> + Send + 'static) {
        let task = Arc::new(Task {
            id: TASK_ID.fetch_add(1, Ordering::Relaxed),
            state: Mutex::new(TaskState {
                future:  Some(Box::pin(future)),
                queued:  true, // 创建即在队列中
                polling: false,
                resched: false,
            }),
        });
        let n = self.spawned_total.fetch_add(1, Ordering::Relaxed) + 1;
        // 单任务不逐条打印（高频投放会刷屏）；任务量由 worker 0 的周期
        // [STATUS] 行展示，这里仅保留计数
        let _ = n;
        self.push_task(task);
    }

    /// 入队 + poke 阻塞在 epoll_wait 的 worker 0 + 按需扩容
    fn push_task(&'static self, task: Arc<Task>) {
        let qlen = {
            let mut q = self.queue.lock().unwrap();
            q.push_back(task);
            q.len()
        };
        // eventfd 唤醒：worker 0 可能正阻塞在 epoll_wait；它不在等待时
        // 计数也只是累积，下一轮被 drain，无副作用
        self.reactor.poke();
        self.maybe_grow(qlen);
    }

    /// 扩容条件：排队任务多于能立即消化的空闲 worker，且未达核数上限。
    /// 动态 worker 队列空即退出、没有等待态，唯一可能空闲的是 worker 0
    /// （它正阻塞在 epoll_wait 时算 1 个）。
    fn maybe_grow(&'static self, qlen: usize) {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return;
        }
        let idle = self.worker0_idle.load(Ordering::Relaxed) as usize;
        if qlen <= idle {
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

    fn spawn_worker(&'static self) {
        let id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "[WORKER] create id={id} (active workers: {}, max: {})",
            self.active_workers.load(Ordering::Relaxed) + 1,
            self.max_workers
        );
        let h = std::thread::spawn(move || self.worker_loop(id));
        self.worker_handles.lock().unwrap().push(h);
    }

    /// 置关闭标志并 poke 在 epoll_wait 中阻塞的 worker 0
    fn initiate_shutdown(&self) {
        self.is_shutdown.store(true, Ordering::Release);
        self.reactor.poke();
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

        // worker 0 退出说明已 shutdown：join 全部动态 worker（它们最迟在
        // 当前一个非阻塞 poll 结束后看到 shutdown 标志退出）
        for h in self.worker_handles.lock().unwrap().drain(..) {
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
    ///     epoll_wait 上等 I/O 事件、定时器到期或 eventfd poke；队列非空时
    ///     每隔 IO_TICK 也会非阻塞轮询一次 epoll（忙时 tick）；TERM 信号
    ///     最迟 WORKER_IO_WAIT_MS 被轮询到并唤醒所有等待中的任务
    fn worker_loop(&'static self, id: usize) {
        let mut last_io_tick = Instant::now();
        let mut last_status = Instant::now();
        loop {
            if self.is_shutdown.load(Ordering::Acquire) {
                return;
            }
            // worker 0 周期打印运行状态（队列深度/worker 数/任务计数）
            if id == 0 && last_status.elapsed() >= STATUS_INTERVAL {
                last_status = Instant::now();
                let qlen = self.queue.lock().unwrap().len();
                let workers = self.active_workers.load(Ordering::Relaxed) + 1;
                let spawned = self.spawned_total.load(Ordering::Relaxed);
                let done = self.completed_total.load(Ordering::Relaxed);
                eprintln!(
                    "[STATUS] queue={qlen} workers={workers}/{} spawned={spawned} completed={done} in-flight={}",
                    self.max_workers,
                    spawned.saturating_sub(done)
                );
            }
            let task = self.queue.lock().unwrap().pop_front();
            match task {
                Some(t) => self.poll_task(t),
                None if id != 0 => {
                    // 任务不足：立即退出线程释放 CPU 核
                    let prev = self.active_workers.fetch_sub(1, Ordering::Relaxed);
                    eprintln!(
                        "[WORKER] exit id={id} queue empty (active workers: {})",
                        prev
                    );
                    return;
                }
                None => {
                    // worker 0：先回收已退出 worker 的句柄，再兼任 reactor
                    self.reap_workers();
                    // 置 idle 后 push 方读到 idle=1 会少起一个 worker——
                    // 没问题：下面的 poke/eventfd 保证本线程立即醒来 drain。
                    // idle 只用于扩容决策，正确性由 eventfd 水平触发兜底，
                    // 故 Relaxed 足够。
                    self.worker0_idle.store(true, Ordering::Relaxed);
                    let alive = self.reactor.wait(WORKER_IO_WAIT_MS);
                    self.worker0_idle.store(false, Ordering::Relaxed);
                    last_io_tick = Instant::now();
                    if !alive || self.is_shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    if signal::TERM.load(Ordering::Relaxed) {
                        // 唤醒等 I/O/定时器的任务：它们重新 poll 时自查 TERM
                        self.reactor.wake_all();
                    }
                    // wait 返回期间 waker 已把就绪任务推回队列，回顶部 drain
                    continue;
                }
            }
            // ── 忙时 tick：worker 0 队列持续非空时也要周期驱动 reactor ──
            // 否则等 epoll 事件（网络/串口）和定时器的任务会被源源不断的
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

    /// 回收已退出的动态 worker 的 JoinHandle（worker 0 空闲时调用）
    fn reap_workers(&self) {
        let mut hs = self.worker_handles.lock().unwrap();
        let mut i = 0;
        while i < hs.len() {
            if hs[i].is_finished() {
                let h = hs.swap_remove(i);
                let _ = h.join();
            } else {
                i += 1;
            }
        }
    }

    /// poll 一个任务；panic 时记录 payload 并触发全局关闭
    fn poll_task(&'static self, task: Arc<Task>) {
        let mut fut = {
            let mut s = task.state.lock().unwrap();
            s.queued = false;
            let f = s.future.take().expect("queued task without future");
            s.polling = true;
            f
        };
        let waker = task_waker(task.clone());
        let mut cx = Context::from_waker(&waker);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fut.as_mut().poll(&mut cx)
        }));

        let mut s = task.state.lock().unwrap();
        s.polling = false;
        match result {
            Ok(Poll::Ready(())) => {
                // future 已 take 走，随作用域 drop；state 中 future 保持 None
                self.completed_total.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Poll::Pending) => {
                s.future = Some(fut);
                if s.resched {
                    // poll 期间被唤醒：负责重新入队
                    s.resched = false;
                    s.queued = true;
                    drop(s);
                    self.push_task(task);
                }
            }
            Err(p) => {
                eprintln!("[ERROR] task panicked, shutting down executor");
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
    /// 唤醒路径：可能运行在任意 worker 线程或 IO 线程中。
    /// 只持 task 锁做状态变更，不触碰 future。
    fn wake_by_ref_(self: &Arc<Self>) {
        {
            let mut s = self.state.lock().unwrap();
            if s.future.is_none() && !s.polling {
                return; // 已完成：丢弃这次唤醒
            }
            if s.polling {
                // 正被 poll：只留标记，由 poll 方结束后重新入队
                s.resched = true;
                return;
            }
            if s.queued {
                return; // 已在队列中
            }
            s.queued = true;
        }
        executor().push_task(self.clone());
    }
}
