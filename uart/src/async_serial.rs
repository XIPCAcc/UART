// 异步串口 I/O — 基于 Future trait 的非阻塞串口读写
//
// 参考 mini-rust-runtime 的 TcpStream，将同步 read/write 包装为
// 返回 Future 的异步操作。当 I/O 返回 WouldBlock 时，通过 Reactor
// 注册 waker 并返回 Poll::Pending，事件循环唤醒后重新尝试。

use std::future::Future;
use std::io;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use crate::protocol::Frame;
use crate::sys;

use crate::executor::executor;

// ── 异步串口句柄 ──────────────────────────────────────────────

pub struct AsyncSerial {
    pub fd: RawFd,
}

impl AsyncSerial {
    pub fn open(path: &str, baud_rate: u32) -> io::Result<Self> {
        let fd = crate::serial_io::open(path, baud_rate)?;
        // 注册到 reactor（设为非阻塞 + 加入 epoll）
        executor().reactor.borrow_mut().add(fd)?;
        Ok(AsyncSerial { fd })
    }

    /// 异步读取，返回 Future
    pub fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFuture<'a> {
        ReadFuture { serial: self, buf }
    }

    /// 异步写入，返回 Future（保证写入全部字节）
    pub fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> WriteAllFuture<'a> {
        WriteAllFuture {
            serial: self,
            buf,
            pos: 0,
        }
    }
}

impl Drop for AsyncSerial {
    fn drop(&mut self) {
        executor().reactor.borrow_mut().delete(self.fd);
    }
}

// ── ReadFuture ─────────────────────────────────────────────────

pub struct ReadFuture<'a> {
    serial: &'a mut AsyncSerial,
    buf: &'a mut [u8],
}

impl<'a> Future for ReadFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 收到终止信号时立即返回错误，让外层循环退出
        if crate::signal::TERM.load(Ordering::Relaxed) {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::Interrupted, "terminated")));
        }
        match sys::raw_read(self.serial.fd, self.buf) {
            Ok(0) => {
                // 串口 read 返回 0 表示暂无数据（VMIN=1 时不应发生，但作为防御）
                executor()
                    .reactor
                    .borrow_mut()
                    .modify_readable(self.serial.fd, cx.waker());
                Poll::Pending
            }
            Ok(n) => {
                // 边缘触发模式：循环读取直到 EAGAIN，一次性消费所有可用数据
                let mut total = n;
                loop {
                    if total >= self.buf.len() {
                        break;
                    }
                    match sys::raw_read(self.serial.fd, &mut self.buf[total..]) {
                        Ok(0) => break,
                        Ok(m) => {
                            total += m;
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => {
                            return Poll::Ready(Err(e));
                        }
                    }
                }
                Poll::Ready(Ok(total))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                executor()
                    .reactor
                    .borrow_mut()
                    .modify_readable(self.serial.fd, cx.waker());
                Poll::Pending
            }
            Err(e) => {
                Poll::Ready(Err(e))
            }
        }
    }
}

// ── WriteAllFuture ─────────────────────────────────────────────

pub struct WriteAllFuture<'a> {
    serial: &'a mut AsyncSerial,
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Future for WriteAllFuture<'a> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        while self.pos < self.buf.len() {
            let fd = self.serial.fd;
            match sys::raw_write(fd, &self.buf[self.pos..]) {
                Ok(n) => {
                    self.pos += n;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    executor()
                        .reactor
                        .borrow_mut()
                        .modify_writable(fd, cx.waker());
                    return Poll::Pending;
                }
                Err(e) => {
                    return Poll::Ready(Err(e));
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

// ── write_frame 辅助函数 ──────────────────────────────────────

/// 异步写入一帧
pub async fn write_frame(serial: &mut AsyncSerial, frame: &Frame) -> io::Result<()> {
    let bytes = frame.to_bytes();
    serial.write_all(&bytes).await
}

// ── 基于 RawFd 的异步 I/O（供多协程共享 fd 使用）───────────────

pub struct ReadFdFuture<'a> {
    fd: RawFd,
    buf: &'a mut [u8],
}

impl<'a> Future for ReadFdFuture<'a> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match sys::raw_read(self.fd, self.buf) {
            Ok(n) => {
                Poll::Ready(Ok(n))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                executor()
                    .reactor
                    .borrow_mut()
                    .modify_readable(self.fd, cx.waker());
                Poll::Pending
            }
            Err(e) => {
                Poll::Ready(Err(e))
            }
        }
    }
}

pub struct WriteFdFuture<'a> {
    fd: RawFd,
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Future for WriteFdFuture<'a> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        while self.pos < self.buf.len() {
            match sys::raw_write(self.fd, &self.buf[self.pos..]) {
                Ok(n) => {
                    self.pos += n;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    executor()
                        .reactor
                        .borrow_mut()
                        .modify_writable(self.fd, cx.waker());
                    return Poll::Pending;
                }
                Err(e) => {
                    return Poll::Ready(Err(e));
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

/// 基于 RawFd 的异步读取
pub fn read_fd<'a>(fd: RawFd, buf: &'a mut [u8]) -> ReadFdFuture<'a> {
    ReadFdFuture { fd, buf }
}

/// 基于 RawFd 的异步写入全部
pub fn write_all_fd<'a>(fd: RawFd, buf: &'a [u8]) -> WriteFdFuture<'a> {
    WriteFdFuture { fd, buf, pos: 0 }
}