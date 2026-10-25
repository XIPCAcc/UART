// 基于 epoll 的 I/O 事件循环 — 多线程安全（&self 接口）
//
// 职责：
//   - 管理 epoll 实例，注册/修改/删除 fd（&self，内部用自旋锁同步）
//   - 将 Waker 与 fd@event 绑定，epoll 就绪时唤醒对应任务
//
// 调用模型：没有专用 IO 线程，wait() 由常驻 worker 0 在全局队列空时调用。
// 无 eventfd：任务入队方自己就是 worker（spawn/wake 只发生在 poll 内），
// push 后会继续 pop；关闭标志由 wait() 的 epoll_wait 超时兜底轮询
// （WORKER_IO_WAIT_MS 即关停延迟上界）。
// TERM 信号处理：worker 0 每次 wait 返回后检查 TERM，置位则 wake_all() 唤醒
// 全部等待中的任务；各 future（如串口 read）在 poll 开头自查 TERM，
// 提前返回让顶层任务感知信号并退出，进而触发执行器 shutdown。

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::io;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Waker;

use crate::sys;
use crate::log;

// ── 自旋锁 ─────────────────────────────────────────────────────
// 临界区均为纯内存操作（HashMap 增删查、读 shutdown），最坏情况
// 是 wait() 收割事件的一趟遍历——微秒级。竞争时 CAS 空转比 futex 进内核
// 睡眠/唤醒便宜几个数量级，且彻底消除该路径的系统调用。

struct SpinLock<T> {
    locked: AtomicBool,
    data:   UnsafeCell<T>,
}

struct SpinGuard<'a, T> {
    lock: &'a SpinLock<T>,
}

// 安全性：同一时刻只有一个持锁者能访问 data（AtomicBool 独占门）
unsafe impl<T: Send> Send for SpinLock<T> {}
unsafe impl<T: Send> Sync for SpinLock<T> {}

impl<T> SpinLock<T> {
    fn new(data: T) -> Self {
        SpinLock {
            locked: AtomicBool::new(false),
            data:   UnsafeCell::new(data),
        }
    }

    fn lock(&self) -> SpinGuard<'_, T> {
        while self
            .locked
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        SpinGuard { lock: self }
    }
}

impl<T> std::ops::Deref for SpinGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> std::ops::DerefMut for SpinGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for SpinGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

// ── 内部状态 ───────────────────────────────────────────────────

struct Inner {
    epfd:     RawFd,
    /// token → waker；token = fd*2 可读 / fd*2+1 可写
    wakers:    HashMap<u64, Waker>,
    /// fd → 当前已注册的 epoll 事件掩码
    fd_events: HashMap<RawFd, u32>,
    /// block_on 退出时置位，wait() 返回 false 让 worker 0 退出
    shutdown:  bool,
}

// ── Reactor ────────────────────────────────────────────────────

pub struct Reactor {
    inner: SpinLock<Inner>,
}

impl Reactor {
    pub fn new() -> io::Result<Self> {
        let epfd = sys::epoll_create()?;
        Ok(Reactor {
            inner: SpinLock::new(Inner {
                epfd,
                wakers: HashMap::new(),
                fd_events: HashMap::new(),
                shutdown: false,
            }),
        })
    }

    /// 将 fd 加入 epoll 管理（ET 模式，初始关注可读）。
    /// 调用方负责保证 fd 已是非阻塞：
    ///   - TCP 路径由 SOCK_NONBLOCK（socket/accept4）在创建时置好
    ///   - 串口路径由 open(O_NOCTTY|O_NONBLOCK) 置好
    /// 此处不再调 fcntl，避免每连接两次冗余系统调用。
    pub fn add(&self, fd: RawFd) -> io::Result<()> {
        let mut inner = self.inner.lock();
        let mut ev = unsafe { sys::EpollEvent::zeroed() };
        ev.events = sys::EPOLLIN | sys::EPOLLET;
        ev.data   = fd as u64;
        let r = unsafe { sys::epoll_ctl(inner.epfd, sys::EPOLL_CTL_ADD, fd, &ev) };
        if r < 0 {
            return Err(io::Error::last_os_error());
        }
        inner.fd_events.insert(fd, sys::EPOLLIN | sys::EPOLLET);
        Ok(())
    }

    /// 注册等待可读的 waker，将 EPOLLIN 加入事件掩码
    pub fn modify_readable(&self, fd: RawFd, waker: &Waker) {
        let mut inner = self.inner.lock();
        inner.wakers.insert(fd as u64 * 2, waker.clone());
        let cur = inner.fd_events.get(&fd).copied().unwrap_or(0);
        let new = cur | sys::EPOLLIN;
        if new != cur {
            inner.fd_events.insert(fd, new);
            let _ = sys::epoll_mod(inner.epfd, fd, new);
        }
    }

    /// 注册等待可写的 waker，将 EPOLLOUT 加入事件掩码
    pub fn modify_writable(&self, fd: RawFd, waker: &Waker) {
        let mut inner = self.inner.lock();
        inner.wakers.insert(fd as u64 * 2 + 1, waker.clone());
        let cur = inner.fd_events.get(&fd).copied().unwrap_or(0);
        let new = cur | sys::EPOLLOUT;
        if new != cur {
            inner.fd_events.insert(fd, new);
            let _ = sys::epoll_mod(inner.epfd, fd, new);
        }
    }

    /// 从 epoll 和 waker 映射中移除 fd
    pub fn delete(&self, fd: RawFd) {
        let mut inner = self.inner.lock();
        let _ = sys::epoll_del(inner.epfd, fd);
        inner.wakers.remove(&(fd as u64 * 2));
        inner.wakers.remove(&(fd as u64 * 2 + 1));
        inner.fd_events.remove(&fd);
    }

    /// 置 shutdown；此后 wait() 一律返回 false。
    /// 不唤醒阻塞中的 worker 0：它最迟在 epoll_wait 超时（WORKER_IO_WAIT_MS）
    /// 后返回并看到标志——这是无 eventfd 设计的关停延迟上界。
    pub fn shutdown(&self) {
        self.inner.lock().shutdown = true;
    }

    /// 唤醒所有注册中的 waker 并清空映射（TERM 时用：任务醒后自查 TERM 退出）
    pub fn wake_all(&self) {
        let wakers: Vec<Waker> = {
            let mut inner = self.inner.lock();
            inner.wakers.drain().map(|(_, w)| w).collect()
        };
        for w in wakers {
            w.wake();
        }
    }

    /// 阻塞等待 epoll 事件。
    /// cap_ms: 超时上限（worker 0 用它保证周期性检查 TERM）。
    /// 返回 false 表示已 shutdown（worker 0 应退出）；true 表示继续循环。
    pub fn wait(&self, cap_ms: i32) -> bool {
        let mut events: Vec<sys::EpollEvent> = (0..32)
            .map(|_| unsafe { sys::EpollEvent::zeroed() })
            .collect();

        // 锁内检查 shutdown 并取 epfd，解锁后再进内核 epoll_wait
        let epfd;
        {
            let inner = self.inner.lock();
            if inner.shutdown {
                return false;
            }
            epfd = inner.epfd;
        }
        log!("epoll_wait timeout: {}", cap_ms);
        let n = match sys::epoll_wait(epfd, &mut events, cap_ms) {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => return true, // 信号打断，继续循环
            Err(_) => return true,
        };

        let mut wakers = Vec::new();
        {
            let mut inner = self.inner.lock();
            if inner.shutdown {
                return false;
            }
            for ev in events.iter().take(n) {
                let fd = ev.data as RawFd;
                if ev.events & sys::EPOLLIN != 0 {
                    if let Some(w) = inner.wakers.remove(&(fd as u64 * 2)) {
                        wakers.push(w);
                    }
                }
                if ev.events & sys::EPOLLOUT != 0 {
                    if let Some(w) = inner.wakers.remove(&(fd as u64 * 2 + 1)) {
                        wakers.push(w);
                    }
                }
            }
        }
        // 锁外唤醒，减少锁争用
        for w in wakers {
            w.wake();
        }
        true
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        let inner = self.inner.lock();
        sys::close_fd(inner.epfd);
    }
}
