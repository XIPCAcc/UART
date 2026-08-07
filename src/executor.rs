// 单线程 async 执行器 — 参考 Embassy TaskStorage / TaskHeader 模式
//
// 提供：
//   - block_on(): 纯事件循环，所有 task 通过 spawn 预先注入
//   - spawn:      添加后台任务到 RunQueue（可在 block_on 前调用）
//   - 基于 RawWakerVTable 的 Waker 实现，唤醒时将任务重新入队
//
// 退出条件：
//   - pending 计数器归零（所有 spawn 的 task 都已执行完毕）→ 正常退出
//   - SIGINT / SIGTERM 信号 → 中断退出
//
// RunQueue 使用 lock-free Treiber Stack (TransferStack)：
//   - push_was_empty: CAS 循环入栈，返回入栈前是否为空
//   - take_all:      原子 swap 批量取出全部节点
//   - x86_64 对应 lock cmpxchg (push) + lock xchg (take_all)
//
// TaskHeader.state 位标志防止重复入队 (double-wake)：
//   - STATE_SPAWNED:   任务已创建
//   - STATE_RUN_QUEUED: 任务已在 RunQueue 中，唤醒时跳过

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};

use std::task::{Context, RawWaker, RawWakerVTable, Waker};

use crate::reactor::Reactor;
use crate::signal;
use crate::uintr::syscall::{umonitor, umwait};
use crate::uintr_core::WAKE_FLAG;

// ── 全局执行器指针 ─────────────────────────────────────────────

static mut EXECUTOR_PTR: *const Executor = std::ptr::null();

pub(crate) fn executor() -> &'static Executor {
    unsafe { &*EXECUTOR_PTR }
}

// ── Task 状态标志 ──────────────────────────────────────────────

const STATE_SPAWNED:    u8 = 1 << 0;
const STATE_RUN_QUEUED: u8 = 1 << 1;

// ── RunQueueItem + TransferStack ───────────────────────────────

struct RunQueueItem {
    next: AtomicPtr<TaskHeader>,
}

struct TransferStack {
    head: AtomicPtr<TaskHeader>,
}

impl TransferStack {
    fn new() -> Self {
        Self { head: AtomicPtr::new(std::ptr::null_mut()) }
    }

    fn push_was_empty(&self, node: *mut TaskHeader) -> bool {
        let mut cur = self.head.load(Ordering::Relaxed);
        loop {
            unsafe { (*node).run_queue_item.next.store(cur, Ordering::Relaxed); }
            match self.head.compare_exchange_weak(
                cur, node,
                Ordering::Release, Ordering::Relaxed,
            ) {
                Ok(_) => return cur.is_null(),
                Err(actual) => cur = actual,
            }
        }
    }

    fn take_all(&self) -> *mut TaskHeader {
        self.head.swap(std::ptr::null_mut(), Ordering::Acquire)
    }

    fn is_empty(&self) -> bool {
        self.head.load(Ordering::Relaxed).is_null()
    }
}

// ── TaskHeader ─────────────────────────────────────────────────

#[repr(C)]
struct TaskHeader {
    state:           AtomicU8,
    run_queue_item:  RunQueueItem,
}

impl TaskHeader {
    fn new() -> Self {
        Self {
            state:           AtomicU8::new(STATE_SPAWNED),
            run_queue_item:  RunQueueItem { next: AtomicPtr::new(std::ptr::null_mut()) },
        }
    }
}

// ── Task ───────────────────────────────────────────────────────

static mut TASK_ID: usize = 0;

#[repr(C)]
pub(crate) struct Task {
    raw:    TaskHeader,
    id:     usize,
    future: RefCell<Pin<Box<dyn Future<Output = ()>>>>,
}

impl Task {
    fn new(id: usize, future: impl Future<Output = ()> + 'static) -> Self {
        Task {
            raw:    TaskHeader::new(),
            id,
            future: RefCell::new(Box::pin(future)),
        }
    }

    fn wake_by_ref_(self: &Rc<Self>) {
        let ex = executor();
        let prev = self.raw.state.fetch_or(STATE_RUN_QUEUED, Ordering::AcqRel);
        if prev & STATE_RUN_QUEUED != 0 {
            return;
        }
        let ptr = Rc::into_raw(self.clone()) as *mut TaskHeader;
        ex.queue.push_was_empty(ptr);
    }
}

// ── 执行器 ────────────────────────────────────────────────────

pub struct Executor {
    queue:   TransferStack,
    pub(crate) reactor: RefCell<Reactor>,
    pending: Cell<usize>,
}

impl Executor {
    pub fn new() -> std::io::Result<Self> {
        Ok(Executor {
            queue:   TransferStack::new(),
            reactor: RefCell::new(Reactor::new()?),
            pending: Cell::new(0),
        })
    }

    /// 添加后台任务；可在 block_on 之前多次调用
    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        let task_id = unsafe { TASK_ID };
        unsafe { TASK_ID += 1 };
        let task = Rc::new(Task::new(task_id, future));
        // 入队前标记 RUN_QUEUED，防止 waker 在 poll 前重复入队
        task.raw.state.store(STATE_SPAWNED | STATE_RUN_QUEUED, Ordering::Release);
        self.pending.set(self.pending.get() + 1);
        let ptr = Rc::into_raw(task) as *mut TaskHeader;
        self.queue.push_was_empty(ptr);
    }

    /// 纯事件循环 — 所有 task 需提前通过 spawn 注入
    ///
    /// 退出条件：
    ///   - 所有 task 执行完毕（pending == 0）
    ///   - 收到 SIGINT/SIGTERM 信号
    pub fn block_on(&self) {
        unsafe { EXECUTOR_PTR = self; }
        loop {
            self.drain_queue();
            if self.pending.get() == 0 {
                break;
            }
            if signal::TERM.load(Ordering::Relaxed) {
                break;
            }
            // ── RCU 风格快慢路径：队列非空时跳过 CLUI/STUI ──────────
            //
            // 快速路径（无 CLUI，高吞吐场景几乎每次都走这里）：
            //   直接原子读 queue.head。如果非空，说明有 task 待处理，
            //   直接 continue → drain_queue 处理，完全不用开关中断。
            //   这里「读 head」没有任何保护，是「RCU 读端」语义：
            //     - 读到非空 → 一定正确（task 入队后不会被并发移出，只有
            //       本线程自己的 drain_queue 会 take_all，单线程安全）
            //     - 读到空 → 可能是假空（UINTR 刚打断、handler 已入队，
            //       但 cache line 还没同步给我们）→ 进入慢路径复核
            //
            // 慢路径（确实为空要休眠时才走，CLUI 保护）：
            //   CLUI → 再检查 is_empty → 非空则 STUI+continue
            //   空 → WAKE_FLAG=0 → UMONITOR → STUI → UMWAIT
            if !self.queue.is_empty() {
                continue;
            }
            // ── 慢路径：只有真的需要休眠时才开关中断 ──
            unsafe {
                crate::uintr::syscall::clui();
            }
            if !self.queue.is_empty() {
                // CLUI 期间发现已有人塞 task（或快速路径假空）→ 不开 UMONITOR
                unsafe {
                    crate::uintr::syscall::stui();
                }
                continue;
            }
            // 在 CLUI 保护下重置 WAKE_FLAG 并 arm UMONITOR
            unsafe {
                umonitor(&WAKE_FLAG as *const _ as *const u8);
                crate::uintr::syscall::stui();
                // UMWAIT：C0.2 深度休眠，无限等待
                // 仅靠 UMONITOR 捕获 handler 的 WAKE_FLAG.store(1) 唤醒
                umwait(0, u64::MAX);
            }
        }
    }

    /// 从 TransferStack 循环取出全部就绪任务并依次 poll
    ///
    /// 使用双层循环：外层 take_all 直到栈空，确保 poll 期间新 spawn 的任务
    /// 也能被及时处理，避免"等待 I/O 的任务"和"刚 spawn 的计算任务"互锁。
    fn drain_queue(&self) {
        loop {
            let mut node = self.queue.take_all();
            if node.is_null() {
                break;
            }
            while !node.is_null() {
                let next = unsafe { (*node).run_queue_item.next.load(Ordering::Relaxed) };
                // 出队：清除 RUN_QUEUED，恢复可被重新唤醒
                unsafe { (*node).state.fetch_and(!STATE_RUN_QUEUED, Ordering::Release); }

                // ── 所有权策略：保证 refcount 对称 ───────────────
                // spawn 时：Rc::into_raw(task)   消耗1个 Rc，refcount 不变（实际是把所有权转成裸指针）
                // 这里：    Rc::from_raw(node)   把裸指针转回 Rc，获得所有权（refcount 仍为逻辑1）
                // clone 给 task_waker：refcount 变成 2
                // Waker 在 poll 结束后 drop：drop_waker 会把 refcount 减 1
                //   - 若 poll 内部没 clone waker（Ready 路径常见）→ 此时 refcount = 1
                //     然后本地 task_rc 在循环迭代结束 drop → refcount = 0，Task 释放 ✓
                //   - 若 poll 内部 clone了waker（Pending 路径）→ refcount = 3
                //     Waker drop → refcount=2，然后 task_rc drop → refcount=1
                //     剩余 1 份引用在 token.waker 中持有的克隆 waker 里，Task 继续存活 ✓
                let task_rc: Rc<Task> = unsafe { Rc::from_raw(node as *const Task) };
                let task: &Task = &task_rc;
                let w = task_waker(task_rc.clone());  // 显式 clone，Rc refcount++
                let mut task_cx = Context::from_waker(&w);
                let is_ready = task.future.borrow_mut().as_mut().poll(&mut task_cx).is_ready();
                if is_ready {
                    self.pending.set(self.pending.get() - 1);
                }
                // Waker `w` 在这里 drop → drop_waker →  Rc refcount--
                // task_rc 在这里也 drop → Rc refcount--
                // 总体对称，不会有 use-after-free 也不会有 leak

                node = next;
            }
        }
    }
}

// ── Waker 实现 ─────────────────────────────────────────────────

fn task_waker(task: Rc<Task>) -> Waker {
    let ptr = Rc::into_raw(task) as *const ();
    let vtable = &RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);
    unsafe { Waker::from_raw(RawWaker::new(ptr, vtable)) }
}

unsafe fn clone_waker(data: *const ()) -> RawWaker {
    let rc = Rc::from_raw(data as *const Task);
    let cloned = rc.clone();
    let _ = Rc::into_raw(rc);
    let ptr = Rc::into_raw(cloned) as *const ();
    let vtable = &RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);
    RawWaker::new(ptr, vtable)
}

unsafe fn wake(ptr: *const ()) {
    let task = Rc::from_raw(ptr as *const Task);
    task.wake_by_ref_();
}

unsafe fn wake_by_ref(ptr: *const ()) {
    let task = mem::ManuallyDrop::new(Rc::from_raw(ptr as *const Task));
    task.wake_by_ref_();
}

unsafe fn drop_waker(ptr: *const ()) {
    drop(Rc::from_raw(ptr as *const Task));
}
