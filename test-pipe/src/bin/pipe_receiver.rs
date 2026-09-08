// 基于用户态中断（UINTR）的异步命名管道（FIFO）接收方进程
//
// 职责：
//   1. 初始化 UINTR 基础设施（注册 handler，创建 uintrfd，启用中断）
//   2. 创建 FIFO 并打开读端，通过 Unix Domain Socket 等待发送方连接，交换 fd
//   3. 异步循环读取数据（无数据时被 UINTR 唤醒）

use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use uintr_runtime::executor::Executor;
use uintr_runtime::pipe::{self, PipeReceiver};
use uintr_runtime::signal;
use uintr_runtime::uintr::async_wait;
use uintr_runtime::uintr::connection::{recv_fd, send_fd};
use uintr_runtime::uintr::syscall;
use uintr_runtime::uintr::UINTR_VECTOR;

#[path = "../glue.rs"]
mod glue;
use glue::set_handler_token;

/// 单次读取 64 KB（与发送方 chunk 大小一致）
const CHUNK: usize = 64 * 1024;
/// 总传输量 256 MB
const TOTAL: usize = 256 * 1024 * 1024;

const FIFO_PATH: &str = "/tmp/uintr-pipe.fifo";
const SOCKET_PATH: &str = "/tmp/uintr-pipe.sock";

// C 语言中断处理程序声明（由 handler.c 提供，本 crate 的 build.rs 编译）
unsafe extern "C" {
    fn ui_handler(ui_frame: *mut syscall::UintrFrame, vector: u64);
}

fn main() {
    eprintln!("[INFO] UINTR Pipe Receiver mode, waiting for FIFO messages");
    if signal::register_signals().is_err() {
        return;
    }
    let ex = Executor::new().expect("executor init");
    ex.spawn(run());
    ex.block_on();

    eprintln!("[INFO] Shutting down");
}

// ── 全局统计 ─────────────────────────────────────────────
//
// 接收方常被 SIGINT 终止：executor 的 block_on 检测到 TERM 会直接 break 返回，
// run() 协程 while 循环之后的代码不会执行。因此统计写成全局原子变量，
// 由 main 在 block_on 返回后读取打印，保证在任何退出路径下都可见。
pub static RECV_BYTES: AtomicU64 = AtomicU64::new(0);
pub static RECV_INTR: AtomicU64 = AtomicU64::new(0);
pub static RECV_SLEEP: AtomicU64 = AtomicU64::new(0);

/// main 在 block_on 结束后调用：打印统计
pub fn print_stats(elapsed_secs: f64) {
    let bytes = RECV_BYTES.load(Ordering::Relaxed);
    let intr = RECV_INTR.load(Ordering::Relaxed);
    let sleep = RECV_SLEEP.load(Ordering::Relaxed);
    let mib = bytes as f64 / (1024.0 * 1024.0);
    eprintln!(
        "[PIPE] receiver: {mib:.1} MiB / {elapsed_secs:.3} s = {:.1} MiB/s",
        mib / elapsed_secs
    );
    eprintln!("[receiver] senduipi(notify) 次数: {intr}");
    eprintln!("[receiver] 睡眠唤醒次数: {sleep}");
}

/// 接收方入口：持续异步读取直到累计收到 TOTAL 字节或收到退出信号
pub async fn run() {
    // ── 步骤 1: 初始化 UINTR token 与 handler ──
    let token = async_wait::init_token("pipe-receiver");
    set_handler_token(token.clone());

    if syscall::uintr_register_handler(ui_handler, 0).is_err() {
        return;
    }

    let uintrfd = match syscall::uintr_create_fd(UINTR_VECTOR as i32, 0) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    // 注意：stui() 推迟到 fd 交换之后，避免 UINTR 内核导致
    //         阻塞的 accept/sendmsg/recvmsg 系统调用被 EINTR 中断

    // ── 步骤 2: 创建 FIFO 并打开读端 ──
    if let Err(e) = pipe::mkfifo(FIFO_PATH) {
        eprintln!("[PIPE] receiver: mkfifo failed: {e}");
        return;
    }
    let rfd = match pipe::open_receiver(FIFO_PATH) {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("[PIPE] receiver: open_receiver failed: {e}");
            return;
        }
    };

    // ── 步骤 3: 等待发送方连接，交换 uintrfd ──
    let sender_fd = match wait_and_exchange(uintrfd) {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("[PIPE] receiver: {e}");
            return;
        }
    };

    // ── 步骤 4: 注册发送方为 UIPI 发送目标，得到背压通知索引 ──
    let uipi_index = match syscall::uintr_register_sender(sender_fd, 0) {
        Ok(index) => index as u64,
        Err(_) => return,
    };

    // ── 步骤 5: 启用用户态中断 ──
    unsafe { syscall::stui(); }

    let receiver = PipeReceiver::new(rfd, token, uipi_index);

    // ── 步骤 5b: 读 sender 发来的 GO 标记，对齐双方 t0 起跑 ──
    // 能读到 GO ⇒ 发送方写端必然已打开（不再有「空 FIFO 无写端读 0」假 EOF）；
    // GO 不计入 TOTAL。
    let mut go = [0u8; 8];
    loop {
        if signal::TERM.load(Ordering::Relaxed) {
            return;
        }
        match receiver.read_exact(&mut go).await {
            Ok(8) => break,
            Ok(_) => {
                // 写端尚未打开或暂无数据（read 返回 0），稍等重试
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(e) => {
                eprintln!("[PIPE] receiver: read GO failed: {e}");
                return;
            }
        }
    }
    if &go != b"GOSTART!" {
        eprintln!("[PIPE] receiver: bad GO marker");
        return;
    }
    let t = std::time::Instant::now();

    let mut buf = vec![0u8; CHUNK];
    let mut got = 0usize;

    while got < TOTAL {
        if signal::TERM.load(Ordering::Relaxed) {
            break;
        }
        match receiver.poll_read(&mut buf).await {
            Ok(0) => break, // EOF：发送方已关闭写端
            Ok(n) => {
                got += n;
                RECV_BYTES.store(got as u64, Ordering::Relaxed);
                RECV_INTR.store(receiver.interrupts_sent(), Ordering::Relaxed);
                RECV_SLEEP.store(receiver.sleeps(), Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("[PIPE] receiver: read failed: {e}");
                break;
            }
        }
    }

    // ── 步骤 6: 发送 ack 中断通知发送方已读完所有数据 ──
    // 复用现有 UINTR 通道：接收方已注册发送方为 UIPI 目标（uipi_index）
    // unsafe { syscall::senduipi(uipi_index); }
    print_stats(t.elapsed().as_secs_f64());
    // 统计打印由 main 在 block_on 返回后调用 print_stats() 完成
}

/// 同步等待发送方连接，接收发送方 uintrfd，并回传本端 uintrfd
fn wait_and_exchange(uintrfd: RawFd) -> Result<RawFd, String> {
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

    let sender_fd = recv_fd(&socket).map_err(|e| format!("recv uintr fd: {e}"))?;
    send_fd(&socket, uintrfd).map_err(|e| format!("send uintr fd: {e}"))?;

    Ok(sender_fd)
}
