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
        eprintln!("[DEBUG] executor: task {} woken", self.id);
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
        eprintln!("[DEBUG] executor: spawned task {} (pending={})", task_id, self.pending.get());
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
            // eprintln!("[DEBUG] executor: no tasks ready, entering epoll_wait (pending={})", self.pending.get());
            self.reactor.borrow_mut().wait(-1);
            // eprintln!("[DEBUG] executor: returned from epoll_wait");
        }
    }

    /// 从 TransferStack 批量取出全部就绪任务并依次 poll
    fn drain_queue(&self) {
        let mut node = self.queue.take_all();
        while !node.is_null() {
            let next = unsafe { (*node).run_queue_item.next.load(Ordering::Relaxed) };
            // 出队：清除 RUN_QUEUED，恢复可被重新唤醒
            unsafe { (*node).state.fetch_and(!STATE_RUN_QUEUED, Ordering::Release); }

            let task = unsafe { &*(node as *const Task) };
            let w = task_waker(unsafe { Rc::from_raw(node as *const Task) });
            let mut task_cx = Context::from_waker(&w);
            if task.future.borrow_mut().as_mut().poll(&mut task_cx).is_ready() {
                self.pending.set(self.pending.get() - 1);
                eprintln!("[DEBUG] executor: task {} completed (pending={})", task.id, self.pending.get());
            } else {
                eprintln!("[DEBUG] executor: task {} Pending", task.id);
            }

            node = next;
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
