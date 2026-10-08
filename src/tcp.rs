// 异步 TCP 客户端 — 基于 raw FFI socket + epoll 的非阻塞连接/读写
//
// 非阻塞 connect 流程：
//   socket() → set_nonblocking → connect() 返回 EINPROGRESS
//   → 注册 fd 到 reactor，等 EPOLLOUT（connect 完成的唯一可靠信号）
//   → getsockopt(SO_ERROR) == 0 才算连接成功
//
// std::net::TcpStream::connect 是阻塞调用，不满足 worker 线程不能阻塞的要求，
// 因此这里直接用 sys 的裸 FFI。

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::executor::executor;
use crate::sys;

pub struct AsyncTcpStream {
    fd: RawFd,
}

impl AsyncTcpStream {
    /// 异步连接到 IPv4 地址
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let v4 = match addr {
            SocketAddr::V4(a) => a,
            SocketAddr::V6(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "only IPv4 is supported",
                ))
            }
        };
        let fd = sys::tcp_connect_v4(*v4.ip(), v4.port())?;
        // 加入 epoll（初始关注 EPOLLIN；connect 完成等 EPOLLOUT 由 ConnectFuture 注册）
        executor().reactor.add(fd)?;
        ConnectFuture { fd, registered: false }.await?;
        Ok(AsyncTcpStream { fd })
    }

    pub fn read<'a>(&'a self, buf: &'a mut [u8]) -> ReadFuture<'a> {
        ReadFuture { stream: self, buf }
    }

    pub fn write_all<'a>(&'a self, buf: &'a [u8]) -> WriteAllFuture<'a> {
        WriteAllFuture { stream: self, buf, pos: 0 }
    }
}

impl Drop for AsyncTcpStream {
    fn drop(&mut self) {
        executor().reactor.delete(self.fd);
        sys::close_fd(self.fd);
    }
}

// ── ConnectFuture ──────────────────────────────────────────────

struct ConnectFuture {
    fd: RawFd,
    registered: bool,
}

impl Future for ConnectFuture {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.registered {
            // 等 EPOLLOUT：非阻塞 connect 完成（成功或失败）的唯一可靠信号
            self.registered = true;
            executor().reactor.modify_writable(self.fd, cx.waker());
            return Poll::Pending;
        }
        match sys::socket_error(self.fd) {
            Ok(0) => Poll::Ready(Ok(())),
            Ok(code) => Poll::Ready(Err(io::Error::from_raw_os_error(code))),
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

// ── ReadFuture ─────────────────────────────────────────────────

pub struct ReadFuture<'a> {
    stream: &'a AsyncTcpStream,
    buf:    &'a mut [u8],
}

impl<'a> Future for ReadFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match sys::raw_read(self.stream.fd, self.buf) {
            Ok(n) => Poll::Ready(Ok(n)), // 0 = EOF（对端关闭）
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                executor().reactor.modify_readable(self.stream.fd, cx.waker());
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

// ── WriteAllFuture ─────────────────────────────────────────────

pub struct WriteAllFuture<'a> {
    stream: &'a AsyncTcpStream,
    buf:    &'a [u8],
    pos:    usize,
}

impl<'a> Future for WriteAllFuture<'a> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        while self.pos < self.buf.len() {
            match sys::raw_write(self.stream.fd, &self.buf[self.pos..]) {
                Ok(n) => self.pos += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    executor().reactor.modify_writable(self.stream.fd, cx.waker());
                    return Poll::Pending;
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
        Poll::Ready(Ok(()))
    }
}
