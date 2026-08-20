// 基于共享内存 + 用户态中断（UINTR）的单向 IPC 接收方进程
//
// 职责：
//   1. 初始化 UINTR 基础设施（注册 handler，创建 uintrfd，启用中断）
//   2. 通过 Unix Domain Socket 等待发送方连接，交换 fd
//   3. 映射共享内存，异步循环读取数据（无数据时被 UINTR 唤醒）

use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::ipc::{self, Receiver};
use crate::uintr::connection::{recv_fd, send_fd};
use crate::uintr::handler::set_handler_token;
use crate::uintr::async_wait;
use crate::uintr::syscall;
use crate::uintr::{UINTR_HANDLER_FLAG_WAITING_ANY, UINTR_VECTOR};

use crate::signal;

const SOCKET_PATH: &str = "/tmp/uintr-ipc.sock";

// C 语言中断处理程序声明（由 handler.c 提供）
unsafe extern "C" {
    fn ui_handler(ui_frame: *mut syscall::UintrFrame, vector: u64);
}

// ── 全局统计 ─────────────────────────────────────────────
//
// 接收方常被 SIGINT 终止：executor 的 block_on 检测到 TERM 会直接 break 返回，
// run() 协程 while 循环之后的代码不会执行，且协程 future 也不会被 drop
// （Executor 用裸指针持有任务，不回收）。因此统计必须写成全局原子变量，
// 由 main 在 block_on 返回后读取打印，保证在任何退出路径下都可见。
pub static RECV_BYTES: AtomicU64 = AtomicU64::new(0);
pub static RECV_INTR: AtomicU64 = AtomicU64::new(0);
pub static RECV_SLEEP: AtomicU64 = AtomicU64::new(0);

/// main 在 block_on 结束后调用：打印统计
pub fn print_stats(elapsed_secs: f64) {
    let bytes = RECV_BYTES.load(Ordering::Relaxed);
    let intr = RECV_INTR.load(Ordering::Relaxed);
    let sleep = RECV_SLEEP.load(Ordering::Relaxed);
    let mib = bytes as f64 / (1024.0 * 1024.0) / elapsed_secs;
    eprintln!(
        "[IPC] receiver: {bytes} bytes in {elapsed_secs:.3}s -> {mib:.2} MiB/s (含发送方结束后的空等时间)"
    );
    eprintln!("[receiver] senduipi(notify) 次数: {intr}");
    eprintln!("[receiver] 睡眠唤醒次数: {sleep}");
}

/// 接收方入口：持续异步读取消息直到收到退出信号
pub async fn run() {
    // ── 步骤 1: 初始化 UINTR token 与 handler ──
    let token = async_wait::init_token("ipc-receiver");
    set_handler_token(token.clone());

    if syscall::uintr_register_handler(ui_handler, UINTR_HANDLER_FLAG_WAITING_ANY).is_err() {
        return;
    }

    let uintrfd = match syscall::uintr_create_fd(UINTR_VECTOR as i32, 0) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    unsafe { syscall::stui(); }

    // ── 步骤 2: 等待发送方连接，交换 fd，拿到 shm fd ──
    let (shm_fd, sender_fd) = match wait_and_exchange(uintrfd) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("[IPC] receiver: {e}");
            return;
        }
    };

    // ── 步骤 3: 映射共享内存通道 ──
    let shm = match ipc::map_shm(shm_fd) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[IPC] receiver: map_shm failed: {e}");
            return;
        }
    };
    // mmap 后即可关闭 fd
    unsafe { libc::close(shm_fd); }

    // ── 步骤 4: 注册发送方为 UIPI 发送目标，得到背压通知索引 ──
    let uipi_index = match syscall::uintr_register_sender(sender_fd, 0) {
        Ok(index) => index as u64,
        Err(_) => return,
    };

    // ── 步骤 5: 异步循环读取（静默，仅统计字节数，避免日志拖慢吞吐）──
    let receiver = Receiver::new(shm, token, uipi_index);
    let mut buf = vec![0u8; 4096];
    while !signal::TERM.load(Ordering::Relaxed) {
        match receiver.read(&mut buf).await {
            Ok(n) if n > 0 => {
                RECV_BYTES.fetch_add(n as u64, Ordering::Relaxed);
                // 每轮读后同步一次 senduipi/睡眠计数（进程可能随时被 TERM 中断）
                RECV_INTR.store(receiver.interrupts_sent(), Ordering::Relaxed);
                RECV_SLEEP.store(receiver.sleeps(), Ordering::Relaxed);
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("[IPC] receiver: read failed: {e}");
                break;
            }
        }
    }
    // 统计打印由 main 在 block_on 返回后调用 print_stats() 完成
}

/// 同步等待发送方连接，依次接收 [shm_fd, 发送方 uintrfd]，并回传本端 uintrfd
fn wait_and_exchange(uintrfd: RawFd) -> Result<(RawFd, RawFd), String> {
    let _ = std::fs::remove_file(SOCKET_PATH);

    let listener = std::os::unix::net::UnixListener::bind(SOCKET_PATH)
        .map_err(|e| format!("bind socket: {e}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("set nonblocking: {e}"))?;

    let (socket, _addr) = loop {
        if signal::TERM.load(Ordering::Relaxed) {
            return Err("interrupted".into());
        }
        match listener.accept() {
            Ok(conn) => break conn,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(format!("accept: {e}")),
        }
    };

    let shm_fd = recv_fd(&socket).map_err(|e| format!("recv shm fd: {e}"))?;
    let sender_fd = recv_fd(&socket).map_err(|e| format!("recv uintr fd: {e}"))?;
    send_fd(&socket, uintrfd).map_err(|e| format!("send uintr fd: {e}"))?;

    Ok((shm_fd, sender_fd))
}
