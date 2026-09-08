// 基于 epoll 的 I/O 事件循环 — 参考 mini-rust-runtime 的 Reactor
//
// 职责：
//   - 管理 epoll 实例，注册/修改/删除 fd
//   - 将 Waker 与 fd@event 绑定，epoll 就绪时唤醒对应任务
//   - fcntl O_NONBLOCK 由 add() 自动设置
//   - 维护组合事件掩码，避免 EPOLLIN/EPOLLOUT 互相覆盖

use std::collections::HashMap;
use std::io;
use std::os::unix::io::RawFd;
use std::task::Waker;

use crate::sys;

pub struct Reactor {
    epfd: RawFd,
    /// token → waker 映射
    /// token = fd * 2       → 等待可读
    /// token = fd * 2 + 1   → 等待可写
    wakers: HashMap<u64, Waker>,
    /// fd → 当前已注册的 epoll 事件掩码
    fd_events: HashMap<RawFd, u32>,
}

impl Reactor {
    pub fn new() -> io::Result<Self> {
        let epfd = sys::epoll_create()?;
        Ok(Reactor {
            epfd,
            wakers: HashMap::new(),
            fd_events: HashMap::new(),
        })
    }

    /// 将 fd 加入 epoll 管理，并设为非阻塞模式
    pub fn add(&mut self, fd: RawFd) -> io::Result<()> {
        sys::set_nonblocking(fd)?;
        sys::epoll_add(self.epfd, fd)?;
        self.fd_events.insert(fd, sys::EPOLLIN);
        Ok(())
    }

    /// 注册等待可读的 waker，将 EPOLLIN 加入事件掩码
    pub fn modify_readable(&mut self, fd: RawFd, waker: &Waker) {
        let token = fd as u64 * 2;
        self.wakers.insert(token, waker.clone());
        let cur = self.fd_events.get(&fd).copied().unwrap_or(0);
        let new = cur | sys::EPOLLIN;
        if new != cur {
            self.fd_events.insert(fd, new);
            let _ = sys::epoll_mod(self.epfd, fd, new);
        }
    }

    /// 注册等待可写的 waker，将 EPOLLOUT 加入事件掩码
    pub fn modify_writable(&mut self, fd: RawFd, waker: &Waker) {
        let token = fd as u64 * 2 + 1;
        self.wakers.insert(token, waker.clone());
        let cur = self.fd_events.get(&fd).copied().unwrap_or(0);
        let new = cur | sys::EPOLLOUT;
        if new != cur {
            self.fd_events.insert(fd, new);
            let _ = sys::epoll_mod(self.epfd, fd, new);
        }
    }

    /// 阻塞等待 epoll 事件，并唤醒对应的 waker
    /// timeout_ms: 超时毫秒数，-1 表示无限等待
    pub fn wait(&mut self, timeout_ms: i32) {
        let mut events: Vec<sys::EpollEvent> = (0..32)
            .map(|_| unsafe { sys::EpollEvent::zeroed() })
            .collect();
        eprintln!("[DEBUG] reactor: calling epoll_wait (timeout={}), watching {} wakers", timeout_ms, self.wakers.len());
        let n = match sys::epoll_wait(self.epfd, &mut events, timeout_ms) {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                eprintln!("[DEBUG] reactor: epoll_wait interrupted");
                return;
            }
            Err(e) => {
                eprintln!("[DEBUG] reactor: epoll_wait error: {e}");
                return;
            }
        };

        eprintln!("[DEBUG] reactor: epoll_wait returned {} events", n);
        for i in 0..n {
            let ev = &events[i];
            let fd = ev.data as RawFd;
            let flags = ev.events;

            if flags & sys::EPOLLIN != 0 {
                let token = fd as u64 * 2;
                if let Some(waker) = self.wakers.remove(&token) {
                    waker.wake();
                }
            }
            if flags & sys::EPOLLOUT != 0 {
                let token = fd as u64 * 2 + 1;
                if let Some(waker) = self.wakers.remove(&token) {
                    waker.wake();
                }
            }
        }
    }

    /// 从 epoll 和 waker 映射中移除 fd
    pub fn delete(&mut self, fd: RawFd) {
        let _ = sys::epoll_del(self.epfd, fd);
        self.wakers.remove(&(fd as u64 * 2));
        self.wakers.remove(&(fd as u64 * 2 + 1));
        self.fd_events.remove(&fd);
    }
}