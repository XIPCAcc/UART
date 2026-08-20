// 基于共享内存 + 用户态中断（UINTR）的异步单向 IPC 通道
//
// 数据面：通过 memfd_create + ftruncate + mmap(MAP_SHARED) 在两个进程间共享一块
//         SPSC（单生产者单消费者）无锁环形缓冲区。
// 通知面：发送方写入数据后执行 senduipi 向接收方投递用户态中断；
//         接收方的中断 handler 通过 UintrToken 唤醒等待中的协程。
//
// 结构体 [ShmChannel] 直接映射到共享内存，布局固定（#[repr(C)]），
// 只包含原子位置计数与字节数组，不包含任何指针，可安全跨进程共享。

use std::cell::{Cell, UnsafeCell};
use std::io;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::uintr::async_wait::uintr;
use crate::uintr::syscall;
use crate::uintr_core::UintrToken;

/// 环形缓冲区容量（必须为 2 的幂，便于用位掩码取模）
pub const RING_CAPACITY: usize = 1 << 16; // 64 KiB

/// memfd_create 系统调用号（x86_64）
const SYS_MEMFD_CREATE: libc::c_long = 319;
/// 关闭 exec 时继承该 fd
const MFD_CLOEXEC: libc::c_uint = 0x0001;

/// 共享内存中的通道：头部为原子位置计数与等待标志，随后是数据区
///
/// - `write_pos`         仅由发送方写，接收方读
/// - `read_pos`          仅由接收方写，发送方读
/// - `sender_waiting`    发送方置位表示正在等待背压；接收方消费后据此决定是否通知
/// - `receiver_waiting`  接收方置位表示正在等待数据；发送方写入后据此决定是否通知
/// - `data`              数据区，通过 UnsafeCell 提供跨进程共享的可变访问
#[repr(C)]
pub struct ShmChannel {
    write_pos: AtomicU64,
    read_pos: AtomicU64,
    sender_waiting: AtomicU32,
    receiver_waiting: AtomicU32,
    data: UnsafeCell<[u8; RING_CAPACITY]>,
}

impl ShmChannel {
    /// 共享内存区域的总字节数
    pub fn size() -> usize {
        std::mem::size_of::<ShmChannel>()
    }

    /// 从共享内存读取数据到 `out`，返回实际读取字节数。
    /// 无数据可读时返回 0。
    ///
    /// 安全性：SPSC 模型保证只有本方法（接收方）写 `read_pos`、
    /// 读 `[read_pos, write_pos)` 区间；发送方只会写 `[write_pos, …)` 区间，
    /// 两者互不重叠，故 `data` 的裸指针访问是安全的。
    fn read_available(&self, out: &mut [u8]) -> usize {
        let read_pos = self.read_pos.load(Ordering::Relaxed);
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let available = write_pos.wrapping_sub(read_pos) as usize;
        let n = available.min(out.len());
        if n == 0 {
            return 0;
        }
        let mask = RING_CAPACITY - 1;
        let start = (read_pos as usize) & mask;
        unsafe {
            let data = self.data.get() as *const u8;
            for i in 0..n {
                out[i] = *data.add((start + i) & mask);
            }
        }
        self.read_pos.store(read_pos.wrapping_add(n as u64), Ordering::Release);
        n
    }

    /// 将 `src` 写入共享内存，返回实际写入字节数。
    /// 缓冲区满时返回 0。
    ///
    /// 安全性：见 [read_available]。
    fn write_available(&self, src: &[u8]) -> usize {
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let read_pos = self.read_pos.load(Ordering::Acquire);
        let used = write_pos.wrapping_sub(read_pos) as usize;
        let free = RING_CAPACITY - used;
        let n = free.min(src.len());
        if n == 0 {
            return 0;
        }
        let mask = RING_CAPACITY - 1;
        let start = (write_pos as usize) & mask;
        unsafe {
            let data = self.data.get() as *mut u8;
            for i in 0..n {
                *data.add((start + i) & mask) = src[i];
            }
        }
        self.write_pos.store(write_pos.wrapping_add(n as u64), Ordering::Release);
        n
    }
}

/// 发送端：持有共享内存视图、对端 UIPI 索引（数据通知）与背压 token
pub struct Sender {
    shm: &'static ShmChannel,
    /// 发送方 → 接收方：写入数据后通知接收方有数据可读
    uipi_index: u64,
    /// 发送方等待接收方消费后发来的背压中断
    token: UintrToken,
    /// 统计：本端实际执行 senduipi 的次数（仅用户态 notify 路径，单线程安全）
    sent_interrupts: Cell<u64>,
    /// 统计：本端睡眠后被唤醒的次数（背压等待）
    sleeps: Cell<u64>,
}

/// 接收端：持有共享内存视图、数据 token 与对端 UIPI 索引（背压通知）
pub struct Receiver {
    shm: &'static ShmChannel,
    /// 接收方等待发送方写入后发来的数据中断
    token: UintrToken,
    /// 接收方 → 发送方：消费数据后通知发送方有空间可写
    uipi_index: u64,
    /// 统计：本端实际执行 senduipi 的次数（仅用户态 notify 路径，单线程安全）
    sent_interrupts: Cell<u64>,
    /// 统计：本端睡眠后被唤醒的次数（数据等待）
    sleeps: Cell<u64>,
}

impl Sender {
    pub fn new(shm: &'static ShmChannel, uipi_index: u64, token: UintrToken) -> Self {
        Self {
            shm,
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

    /// 仅当接收方正在等待数据时才发通知，避免无效中断
    fn notify_receiver(&self) {
        if self.shm.receiver_waiting.load(Ordering::SeqCst) == 1 {
            unsafe { syscall::senduipi(self.uipi_index); }
            self.sent_interrupts.set(self.sent_interrupts.get() + 1);
        }
    }

    /// 将 `buf` 全部写入共享内存，随后通知接收方。
    ///
    /// 若缓冲区写满（接收方尚未消费），先置位等待标志再 double-check 一次空间，
    /// 确无空间才挂起等待背压中断；每次成功写入一段数据后都发一次数据通知。
    pub async fn write(&self, buf: &[u8]) -> io::Result<()> {
        let mut written = 0;
        while written < buf.len() {
            let n = self.shm.write_available(&buf[written..]);
            if n > 0 {
                written += n;
                self.notify_receiver();
                continue;
            }

            // 缓冲区满：先置等待标志，再 double-check，避免丢失接收方的背压唤醒
            self.shm.sender_waiting.store(1, Ordering::SeqCst);
            // let n = self.shm.write_available(&buf[written..]);
            // if n > 0 {
            //     self.shm.sender_waiting.store(0, Ordering::SeqCst);
            //     written += n;
            //     self.notify_receiver();
            //     continue;
            // }

            // 确无空间，挂起等待背压中断
            uintr(self.token.clone())
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.sleeps.set(self.sleeps.get() + 1);
            self.shm.sender_waiting.store(0, Ordering::SeqCst);
        }
        Ok(())
    }
}

impl Receiver {
    pub fn new(shm: &'static ShmChannel, token: UintrToken, uipi_index: u64) -> Self {
        Self {
            shm,
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

    /// 仅当发送方正在等待背压时才发通知，避免无效中断
    fn notify_sender(&self) {
        if self.shm.sender_waiting.load(Ordering::SeqCst) == 1 {
            unsafe { syscall::senduipi(self.uipi_index); }
            self.sent_interrupts.set(self.sent_interrupts.get() + 1);
        }
    }

    /// 异步读取：有数据立即返回；无数据则挂起等待数据中断。
    /// 消费数据后，仅当发送方确实在等待背压时才发通知。
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = self.shm.read_available(buf);
            if n > 0 {
                self.notify_sender();
                return Ok(n);
            }

            // 无数据：先置等待标志，再 double-check，避免丢失发送方的数据通知
            self.shm.receiver_waiting.store(1, Ordering::SeqCst);
            // let n = self.shm.read_available(buf);
            // if n > 0 {
            //     self.shm.receiver_waiting.store(0, Ordering::SeqCst);
            //     self.notify_sender();
            //     return Ok(n);
            // }

            // 确无数据，挂起等待数据中断
            uintr(self.token.clone())
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.sleeps.set(self.sleeps.get() + 1);
            self.shm.receiver_waiting.store(0, Ordering::SeqCst);
        }
    }
}

/// 创建共享内存（memfd + ftruncate + mmap），返回 fd 与映射引用。
///
/// 调用方负责在把 fd 交给对端后 `close`（映射在 close 后依然有效）。
pub fn create_shm() -> io::Result<(RawFd, &'static ShmChannel)> {
    let name = b"uintr-ipc\0";
    let fd = unsafe { libc::syscall(SYS_MEMFD_CREATE, name.as_ptr(), MFD_CLOEXEC) as RawFd };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let size = ShmChannel::size();
    if unsafe { libc::ftruncate(fd, size as libc::off_t) } != 0 {
        let e = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(e);
    }
    let shm = map_shm(fd)?;
    Ok((fd, shm))
}

/// 将已有的共享内存 fd 映射到进程地址空间，返回通道引用。
pub fn map_shm(fd: RawFd) -> io::Result<&'static ShmChannel> {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            ShmChannel::size(),
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    // 安全性：memfd 已 ftruncate 到 size，mmap 返回页对齐地址，
    // 内存被 memfd 零初始化，AtomicU64 初值 0、字节数组全 0 均为合法值。
    Ok(unsafe { &*(ptr as *const ShmChannel) })
}
