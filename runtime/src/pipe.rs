// 基于用户态中断（UINTR）的异步命名管道（FIFO）
//
// 数据面：POSIX 命名管道（mkfifo + open/read/write，O_NONBLOCK 非阻塞）。
//         与共享内存方案不同，数据缓冲、原子写入（<= PIPE_BUF）、EOF 检测
//         全部由内核 FIFO 承担，用户态无需再维护环形缓冲区与位置计数。
// 通知面：发送方写入后 senduipi 通知接收方「有数据可读」；
//         接收方读出后 senduipi 通知发送方「有空间可写」（背压释放）。
//         两端在 EAGAIN 时通过 uintr(token).await 挂起，等待对端中断唤醒。
//
// 相较 mio/epoll 方案：无需把 fd 注册进 reactor，读写就绪完全由对端
// 用户态中断驱动；仅复用标准库 open/read/write 与 uintr 通知。

use std::cell::Cell;
use std::ffi::CString;
use std::io;
use std::os::unix::io::RawFd;

use crate::uintr::async_wait::uintr;
use crate::uintr::syscall;
use crate::uintr_core::UintrToken;

/// 创建命名管道文件（已存在则忽略 EEXIST）
pub fn mkfifo(path: &str) -> io::Result<()> {
    let cpath = CString::new(path)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let ret = unsafe { libc::mkfifo(cpath.as_ptr(), 0o666) };
    if ret == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EEXIST) {
        return Ok(());
    }
    Err(err)
}

/// 打开 FIFO 读端（非阻塞；无写端时也立即成功）
pub fn open_receiver(path: &str) -> io::Result<RawFd> {
    open_fifo(path, libc::O_RDONLY)
}

/// 打开 FIFO 写端（非阻塞；无读端时返回 ENXIO，由调用方决定是否重试）
pub fn open_sender(path: &str) -> io::Result<RawFd> {
    open_fifo(path, libc::O_WRONLY)
}

fn open_fifo(path: &str, flags: libc::c_int) -> io::Result<RawFd> {
    let cpath = CString::new(path)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let fd = unsafe { libc::open(cpath.as_ptr(), flags | libc::O_NONBLOCK | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

/// 发送端：持有 FIFO 写端 fd、对端 UIPI 索引（数据通知）与背压 token
pub struct PipeSender {
    fd: RawFd,
    /// 发送方 → 接收方：写入数据后通知接收方有数据可读
    uipi_index: u64,
    /// 发送方等待接收方读走后发来的背压中断
    token: UintrToken,
    /// 统计：本端实际执行 senduipi 的次数
    sent_interrupts: Cell<u64>,
    /// 统计：本端睡眠后被唤醒的次数（背压等待）
    sleeps: Cell<u64>,
}

/// 接收端：持有 FIFO 读端 fd、数据 token 与对端 UIPI 索引（背压通知）
pub struct PipeReceiver {
    fd: RawFd,
    /// 接收方等待发送方写入后发来的数据中断
    token: UintrToken,
    /// 接收方 → 发送方：读出数据后通知发送方有空间可写
    uipi_index: u64,
    /// 统计：本端实际执行 senduipi 的次数
    sent_interrupts: Cell<u64>,
    /// 统计：本端睡眠后被唤醒的次数（数据等待）
    sleeps: Cell<u64>,
}

impl PipeSender {
    pub fn new(fd: RawFd, uipi_index: u64, token: UintrToken) -> Self {
        Self {
            fd,
            uipi_index,
            token,
            sent_interrupts: Cell::new(0),
            sleeps: Cell::new(0),
        }
    }

    /// 本端实际发送的 senduipi 次数（数据通知方向）
    pub fn interrupts_sent(&self) -> u64 {
        self.sent_interrupts.get()
    }

    /// 本端睡眠后被唤醒的次数（背压等待）
    pub fn sleeps(&self) -> u64 {
        self.sleeps.get()
    }

    /// 非阻塞地写一次 `buf`，返回实际写入字节数。
    ///
    /// FIFO 缓冲区满（EAGAIN）时挂起等待接收方读走后发来的背压中断；
    /// 一旦写入部分数据即通知接收方并返回。
    pub async fn poll_write(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let n = unsafe {
                libc::write(self.fd, buf.as_ptr() as *const libc::c_void, buf.len())
            };
            if n > 0 {
                unsafe { syscall::senduipi(self.uipi_index); }
                self.sent_interrupts.set(self.sent_interrupts.get() + 1);
                return Ok(n as usize);
            }
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    // FIFO 满：等待接收方读出后发来的背压中断
                    uintr(self.token.clone())
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    self.sleeps.set(self.sleeps.get() + 1);
                    continue;
                }
                return Err(err);
            }
            // n == 0：仅当 buf 为空时才会发生
            return Ok(0);
        }
    }

    /// 将 `buf` 全部写入，内部循环调用 [poll_write] 直到写完。
    pub async fn write_all(&self, buf: &[u8]) -> io::Result<()> {
        let mut written = 0;
        while written < buf.len() {
            written += self.poll_write(&buf[written..]).await?;
        }
        Ok(())
    }
}

impl PipeReceiver {
    pub fn new(fd: RawFd, token: UintrToken, uipi_index: u64) -> Self {
        Self {
            fd,
            token,
            uipi_index,
            sent_interrupts: Cell::new(0),
            sleeps: Cell::new(0),
        }
    }

    /// 本端实际发送的 senduipi 次数（背压通知方向）
    pub fn interrupts_sent(&self) -> u64 {
        self.sent_interrupts.get()
    }

    /// 本端睡眠后被唤醒的次数（数据等待）
    pub fn sleeps(&self) -> u64 {
        self.sleeps.get()
    }

    /// 非阻塞地读一次到 `buf`，返回实际读取字节数。
    ///
    /// 无数据（EAGAIN）时挂起等待发送方写入后发来的数据中断；
    /// 读到数据后通知发送方（释放背压）。返回 0 表示对端写端已关闭（EOF）。
    pub async fn poll_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = unsafe {
                libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
            };
            if n > 0 {
                unsafe { syscall::senduipi(self.uipi_index); }
                self.sent_interrupts.set(self.sent_interrupts.get() + 1);
                return Ok(n as usize);
            }
            if n == 0 {
                // EOF：写端已全部关闭
                return Ok(0);
            }
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                // 无数据：等待发送方写入后发来的数据中断
                uintr(self.token.clone())
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                self.sleeps.set(self.sleeps.get() + 1);
                continue;
            }
            return Err(err);
        }
    }

    /// 读满 `buf`，不够就一直等。返回 0 表示对端已关闭（EOF）。
    pub async fn read_exact(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read = 0;
        while read < buf.len() {
            let n = self.poll_read(&mut buf[read..]).await?;
            if n == 0 {
                return Ok(read); // EOF，返回已读字节数
            }
            read += n;
        }
        Ok(read)
    }
}

impl Drop for PipeSender {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd); }
    }
}

impl Drop for PipeReceiver {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd); }
    }
}
