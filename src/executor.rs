// 单线程 async 执行器 — 参考 mini-rust-runtime 的 Executor
//
// 提供：
//   - block_on: 运行事件循环直到顶层 future 完成
//   - spawn:    添加新的顶层任务到队列
//   - 基于 RawWakerVTable 的 Waker 实现，唤醒时将任务重新入队
//
// 任务通过全局 EXECUTOR_PTR 指针访问执行器（类似 scoped_tls）

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;

use std::task::{Context, RawWaker, RawWakerVTable, Waker};

use crate::reactor::Reactor;

static mut EXECUTOR_PTR: *const Executor = std::ptr::null();

pub(crate) fn executor() -> &'static Executor {
    unsafe { &*EXECUTOR_PTR }
}

// ── 执行器 ────────────────────────────────────────────────────
static mut TASK_ID: usize = 0;

pub struct Executor {
    queue: RefCell<VecDeque<Rc<Task>>>,
    pub(crate) reactor: RefCell<Reactor>,
}

struct Task {
    id: usize,
    future: RefCell<Pin<Box<dyn Future<Output = ()>>>>,
}

impl Executor {
    pub fn new() -> std::io::Result<Self> {
        Ok(Executor {
            queue: RefCell::new(VecDeque::new()),
            reactor: RefCell::new(Reactor::new()?),
        })
    }

    /// 添加后台任务
    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        let task_id = unsafe { TASK_ID };
        unsafe { TASK_ID += 1 };
        self.queue.borrow_mut().push_back(Rc::new(Task {
            id: task_id,
            future: RefCell::new(Box::pin(future)),
        }));
        eprintln!("[DEBUG] executor: spawned task {}", task_id);
        eprintln!("[DEBUG] executor: task queue size: {}", self.queue.borrow().len());
    }

    /// 运行事件循环，阻塞直到 f 完成
    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        unsafe { EXECUTOR_PTR = self; }

        let mut outer = Box::pin(f);
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);

        loop {
            // 轮询顶层 future
            if let std::task::Poll::Ready(result) = outer.as_mut().poll(&mut cx) {
                return result;
            }

            // 消费所有就绪任务
            while let Some(task) = self.queue.borrow_mut().pop_front() {
                let w = task_waker(task.clone());
                let mut task_cx = Context::from_waker(&w);
                let result = task.future.borrow_mut().as_mut().poll(&mut task_cx);
                if let std::task::Poll::Ready(()) = result {
                    eprintln!("[DEBUG] executor: task {} completed", task.id);
                } else {
                    eprintln!("[DEBUG] executor: task {} Pending", task.id);
                }
            }

            // 再次检查顶层 future
            if let std::task::Poll::Ready(result) = outer.as_mut().poll(&mut cx) {
                return result;
            }

            // 没有可执行的任务，阻塞等待 I/O
            eprintln!("[DEBUG] executor: no tasks ready, entering epoll_wait");
            self.reactor.borrow_mut().wait(-1);
            eprintln!("[DEBUG] executor: returned from epoll_wait");
        }
    }
}

// ── Waker 实现 ─────────────────────────────────────────────────

fn dummy_waker() -> Waker {
    fn dummy_raw_waker() -> RawWaker {
        static VTABLE: RawWakerVTable =
            RawWakerVTable::new(|_| dummy_raw_waker(), |_| {}, |_| {}, |_| {});
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe { Waker::from_raw(dummy_raw_waker()) }
}

fn task_waker(task: Rc<Task>) -> Waker {
    let ptr = Rc::into_raw(task) as *const ();
    let vtable = &RawWakerVTable::new(
        clone_waker,
        wake,
        wake_by_ref,
        drop_waker,
    );
    unsafe { Waker::from_raw(RawWaker::new(ptr, vtable)) }
}

unsafe fn clone_waker(data: *const ()) -> RawWaker {
    // 增加引用计数
    let rc = Rc::from_raw(data as *const Task);
    let cloned = rc.clone();
    let _ = Rc::into_raw(rc); // 恢复原始引用
    let ptr = Rc::into_raw(cloned) as *const ();
    let vtable = &RawWakerVTable::new(
        clone_waker,
        wake,
        wake_by_ref,
        drop_waker,
    );
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

impl Task {
    fn wake_by_ref_(self: &Rc<Self>) {
        let ex = executor();
        ex.queue.borrow_mut().push_back(self.clone());
        eprintln!("[DEBUG] executor: task {} woken", self.id);
    }
}