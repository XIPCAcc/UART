// 基于 epoll 的 I/O 事件循环 — 多线程安全（&self 接口），支持定时器与自唤醒
//
// 职责：
//   - 管理 epoll 实例，注册/修改/删除 fd（&self，内部用 Mutex 同步）
//   - 将 Waker 与 fd@event 绑定，epoll 就绪时唤醒对应任务
//   - 维护最小堆定时器，提供 sleep_ms().await 能力
//   - eventfd 自唤醒：任意线程可 poke 阻塞在 epoll_wait 的 worker 0
//     （新任务入队、新定时器、关闭）
//
// 调用模型：没有专用 IO 线程，wait() 由常驻 worker 0 在全局队列空时调用。
// TERM 信号处理：worker 0 每次 wait 返回后检查 TERM，置位则 wake_all() 唤醒
// 全部等待中的任务；各 future（Sleep/串口 read 等）在 poll 开头自查 TERM，
// 提前返回让顶层任务感知信号并退出，进而触发执行器 shutdown。

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::io;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::signal;
use crate::sys;
use crate::log;

// ── 定时器条目 ─────────────────────────────────────────────────

struct TimerEntry {
    deadline: Instant,
    waker:    Waker,
    seq:      u64, // 序列号，消除同一 deadline 的堆序歧义
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.seq == other.seq
    }
}
impl Eq for TimerEntry {}
impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // BinaryHeap 是最大堆：取逆序让最早 deadline 在堆顶
        other.deadline.cmp(&self.deadline).then(other.seq.cmp(&self.seq))
    }
}

// ── 内部状态 ───────────────────────────────────────────────────

struct Inner {
    epfd:     RawFd,
    eventfd:  RawFd,
    /// token → waker；token = fd*2 可读 / fd*2+1 可写
    wakers:    HashMap<u64, Waker>,
    /// fd → 当前已注册的 epoll 事件掩码
    fd_events: HashMap<RawFd, u32>,
    /// 最小堆定时器
    timers:    BinaryHeap<TimerEntry>,
    timer_seq: u64,
    /// block_on 退出时置位，wait() 返回 false 让 worker 0 退出
    shutdown:  bool,
}

// ── Reactor ────────────────────────────────────────────────────

pub struct Reactor {
    inner: Mutex<Inner>,
}

impl Reactor {
    pub fn new() -> io::Result<Self> {
        let epfd = sys::epoll_create()?;
        let eventfd = sys::event_fd()?;
        // eventfd 加入 epoll（水平触发），data = u64::MAX 作为特殊 token
        let mut ev = unsafe { sys::EpollEvent::zeroed() };
        ev.events = sys::EPOLLIN;
        ev.data   = u64::MAX;
        let r = unsafe { sys::epoll_ctl(epfd, sys::EPOLL_CTL_ADD, eventfd, &ev) };
        if r < 0 {
            sys::close_fd(eventfd);
            sys::close_fd(epfd);
            return Err(io::Error::last_os_error());
        }
        Ok(Reactor {
            inner: Mutex::new(Inner {
                epfd,
                eventfd,
                wakers: HashMap::new(),
                fd_events: HashMap::new(),
                timers: BinaryHeap::new(),
                timer_seq: 0,
                shutdown: false,
            }),
        })
    }

    /// 将 fd 加入 epoll 管理，并设为非阻塞模式（ET 模式，初始关注可读）
    pub fn add(&self, fd: RawFd) -> io::Result<()> {
        sys::set_nonblocking(fd)?;
        let mut inner = self.inner.lock().unwrap();
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
        let mut inner = self.inner.lock().unwrap();
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
        let mut inner = self.inner.lock().unwrap();
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
        let mut inner = self.inner.lock().unwrap();
        let _ = sys::epoll_del(inner.epfd, fd);
        inner.wakers.remove(&(fd as u64 * 2));
        inner.wakers.remove(&(fd as u64 * 2 + 1));
        inner.fd_events.remove(&fd);
    }

    /// 从任意线程唤醒阻塞在 epoll_wait 的 worker 0（任务入队/关闭时用）
    pub fn poke(&self) {
        let inner = self.inner.lock().unwrap();
        if !inner.shutdown {
            sys::eventfd_poke(inner.eventfd);
        }
    }

    /// 注册定时器：deadline 到后由 wait() 统一 wake
    pub fn register_timer(&self, deadline: Instant, waker: Waker) {
        let mut inner = self.inner.lock().unwrap();
        let seq = inner.timer_seq;
        inner.timer_seq += 1;
        inner.timers.push(TimerEntry { deadline, waker, seq });
        // 新定时器可能早于当前 epoll_wait 超时，poke 让 worker 0 重算
        sys::eventfd_poke(inner.eventfd);
    }

    /// 置 shutdown 并唤醒 worker 0；此后 wait() 一律返回 false
    pub fn shutdown(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.shutdown = true;
        sys::eventfd_poke(inner.eventfd);
    }

    /// 唤醒所有注册中的 waker 并清空映射（TERM 时用：任务醒后自查 TERM 退出）
    pub fn wake_all(&self) {
        let wakers: Vec<Waker> = {
            let mut inner = self.inner.lock().unwrap();
            let mut v: Vec<Waker> = inner.wakers.drain().map(|(_, w)| w).collect();
            while let Some(t) = inner.timers.pop() {
                v.push(t.waker);
            }
            v
        };
        for w in wakers {
            w.wake();
        }
    }

    /// 阻塞等待 epoll 事件 + 处理到期定时器。
    /// cap_ms: 超时上限（worker 0 用它保证周期性检查 TERM）。
    /// 返回 false 表示已 shutdown（worker 0 应退出）；true 表示继续循环。
    pub fn wait(&self, cap_ms: i32) -> bool {
        let mut events: Vec<sys::EpollEvent> = (0..32)
            .map(|_| unsafe { sys::EpollEvent::zeroed() })
            .collect();

        // 锁内算超时（最近到期定时器），解锁后再进内核 epoll_wait
        let epfd;
        let timeout;
        {
            let inner = self.inner.lock().unwrap();
            if inner.shutdown {
                return false;
            }
            epfd = inner.epfd;
            let now = Instant::now();
            timeout = match inner.timers.peek() {
                None => cap_ms,
                Some(t) => {
                    let ms = t.deadline.saturating_duration_since(now).as_millis();
                    ms.min(cap_ms.max(0) as u128) as i32
                }
            };
        }
        log!("epoll_wait timeout: {}", timeout);
        let n = match sys::epoll_wait(epfd, &mut events, timeout) {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => return true, // 信号打断，继续循环
            Err(_) => return true,
        };

        let mut wakers = Vec::new();
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.shutdown {
                return false;
            }
            for ev in events.iter().take(n) {
                if ev.data == u64::MAX {
                    sys::eventfd_drain(inner.eventfd); // 水平触发必须清零
                    continue;
                }
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
            // 收割到期定时器
            let now = Instant::now();
            while let Some(t) = inner.timers.peek() {
                if t.deadline <= now {
                    wakers.push(inner.timers.pop().unwrap().waker);
                } else {
                    break;
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
        let inner = self.inner.lock().unwrap();
        sys::close_fd(inner.eventfd);
        sys::close_fd(inner.epfd);
    }
}

// ── sleep future ───────────────────────────────────────────────

/// 异步休眠。每次 Pending 都重新注册定时器：
/// TERM 触发的 wake_all 会提前唤醒它，重新 poll 时若未到 deadline 需再次注册，
/// 若已置 TERM 则直接 Ready，让上层任务尽快感知信号退出。
pub struct Sleep {
    deadline: Instant,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if signal::TERM.load(std::sync::atomic::Ordering::Relaxed)
            || Instant::now() >= self.deadline
        {
            return Poll::Ready(());
        }
        crate::executor::executor()
            .reactor
            .register_timer(self.deadline, cx.waker().clone());
        Poll::Pending
    }
}

/// 异步休眠指定毫秒数
pub fn sleep_ms(ms: u64) -> Sleep {
    Sleep {
        deadline: Instant::now() + Duration::from_millis(ms),
    }
}
