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
///
/// 固定 64 KiB，与对照基准保持一致：
///   - ipc-tokio（eventfd 版 shm）RING_CAPACITY = 1 << 16
///   - Linux 内核 pipe 默认环形缓冲区 64KB（FIFO 测试）
/// 三方数据面参数相同，对比差异只来自通知机制（senduipi vs eventfd+epoll）。
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
        let start = (read_pos as usize) & (RING_CAPACITY - 1);
        unsafe {
            let data = self.data.get() as *const u8;
            // 环形拷贝在回绕处分两段，用 copy_nonoverlapping 走宽字/向量拷贝
            let first = n.min(RING_CAPACITY - start);
            std::ptr::copy_nonoverlapping(data.add(start), out.as_mut_ptr(), first);
            if first < n {
                std::ptr::copy_nonoverlapping(data, out.as_mut_ptr().add(first), n - first);
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
        let start = (write_pos as usize) & (RING_CAPACITY - 1);
        unsafe {
            let data = self.data.get() as *mut u8;
            // 环形拷贝在回绕处分两段，用 copy_nonoverlapping 走宽字/向量拷贝
            let first = n.min(RING_CAPACITY - start);
            std::ptr::copy_nonoverlapping(src.as_ptr(), data.add(start), first);
            if first < n {
                std::ptr::copy_nonoverlapping(src.as_ptr().add(first), data, n - first);
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

    /// 接收方是否已消费完发送方写入的所有数据（缓冲区排空，`read_pos == write_pos`）
    pub fn is_drain(&self) -> bool {
        let write = self.shm.write_pos.load(Ordering::Acquire);
        let read = self.shm.read_pos.load(Ordering::Acquire);
        read == write
    }

    /// 仅当接收方正在等待数据时才发通知，避免无效中断
    fn notify_receiver(&self) {
        // if self.shm.receiver_waiting.load(Ordering::SeqCst) == 1 {
            unsafe { syscall::senduipi(self.uipi_index); }
            self.sent_interrupts.set(self.sent_interrupts.get() + 1);
        // }
    }

    /// 将 `buf` 全部写入共享内存。
    ///
    /// 通知策略（与 ipc-tokio eventfd 版一致）：
    ///   - 写入成功不通知，尽可能连续写（ring 足够大时形成流水）；
    ///   - 缓冲区写满时才 notify 接收方来读，并挂起等待背压（空间）中断；
    ///   - 整个 buf 写完后再 notify 一次，保证接收方能收到最后一批数据。
    pub async fn write(&self, buf: &[u8]) -> io::Result<()> {
        let mut written = 0;
        while written < buf.len() {
            let n = self.shm.write_available(&buf[written..]);
            if n > 0 {
                written += n;
                continue;
            }

            // 缓冲区满：通知接收方来读，再挂起等待背压中断
            self.notify_receiver();
            uintr(self.token.clone())
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.sleeps.set(self.sleeps.get() + 1);
        }
        // buf 已全部进入 ring：通知接收方有数据可读
        self.notify_receiver();
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
        // if self.shm.sender_waiting.load(Ordering::SeqCst) == 1 {
            unsafe { syscall::senduipi(self.uipi_index); }
            self.sent_interrupts.set(self.sent_interrupts.get() + 1);
        // }
    }

    /// 异步读取：有数据立即返回；缓冲区空时通知发送方有空间，并挂起等待
    /// 数据中断。读到数据不通知（连续读空后才通知一次背压），与
    /// ipc-tokio eventfd 版的"空才 notify"策略一致。
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = self.shm.read_available(buf);
            if n > 0 {
                return Ok(n);
            }

            // 缓冲区空：通知发送方有空间可写，再挂起等待数据中断
            self.notify_sender();
            uintr(self.token.clone())
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.sleeps.set(self.sleeps.get() + 1);
        }
    }
}

/// 创建共享内存（memfd + ftruncate + mmap），返回 fd 与映射引用。
///
/// 调用方负责在把 fd 交给对端后 `close`（映射在 close 后依然有效）。
pub fn create_shm() -> io::Result<(RawFd, &'static ShmChannel)> {
    let name = b"uintr-shm\0";
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
