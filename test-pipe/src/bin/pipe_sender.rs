// 基于用户态中断（UINTR）的异步命名管道（FIFO）发送方进程
//
// 职责：
//   1. 初始化 UINTR 基础设施（注册 handler，创建 uintrfd，启用中断）
//   2. 通过 Unix Domain Socket 连接接收方，交换 uintrfd
//   3. 打开 FIFO 写端，异步循环写入消息（FIFO 满时被 UINTR 唤醒）

use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use uintr_runtime::executor::Executor;
use uintr_runtime::pipe::{self, PipeSender};
use uintr_runtime::signal;
use uintr_runtime::uintr::async_wait::{self, uintr};
use uintr_runtime::uintr::connection::{recv_fd, send_fd};
use uintr_runtime::uintr::syscall;
use uintr_runtime::uintr::UINTR_VECTOR;

#[path = "../glue.rs"]
mod glue;
use glue::set_handler_token;

/// 单次写入 64 KB（内核 pipe 默认环形缓冲区大小）
const CHUNK: usize = 64 * 1024;
/// 总传输量 256 MB
const TOTAL: usize = 256 * 1024 * 1024;

const FIFO_PATH: &str = "/tmp/uintr-pipe.fifo";
const SOCKET_PATH: &str = "/tmp/uintr-pipe.sock";

/// 起跑标记：写端就绪后由 sender 发送，双方以其到达时刻对齐 t0
const GO: [u8; 8] = *b"GOSTART!";

// C 语言中断处理程序声明（由 handler.c 提供，本 crate 的 build.rs 编译）
unsafe extern "C" {
    fn ui_handler(ui_frame: *mut syscall::UintrFrame, vector: u64);
}

fn main() {
    eprintln!("[INFO] UINTR Pipe Sender mode: 256MB / 64KB chunks");
    if signal::register_signals().is_err() {
        return;
    }
    let ex = Executor::new().expect("executor init");
    ex.spawn(run());
    ex.block_on();
    eprintln!("[INFO] Shutting down");
}

/// 发送方入口：发送 TOTAL 字节的 0x5A 数据（每次 CHUNK）
pub async fn run() {
    // ── 步骤 1: 初始化 UINTR token 与 handler ──
    let token = async_wait::init_token("pipe-sender");
    set_handler_token(token.clone());

    if syscall::uintr_register_handler(ui_handler, 0).is_err() {
        return;
    }

    let uintrfd = match syscall::uintr_create_fd(UINTR_VECTOR as i32, 0) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    // 注意：stui() 推迟到 fd 交换之后，避免 UINTR 内核导致
    //         阻塞的 sendmsg/recvmsg 系统调用被 EINTR 中断

    // ── 步骤 2: 连接接收方，交换 uintrfd ──
    let receiver_fd = match connect_and_exchange(uintrfd) {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("[PIPE] sender: {e}");
            return;
        }
    };

    // ── 步骤 3: 注册接收方为 UIPI 发送目标 ──
    let uipi_index = match syscall::uintr_register_sender(receiver_fd, 0) {
        Ok(index) => index as u64,
        Err(_) => return,
    };

    // ── 步骤 4: 打开 FIFO 写端（此时接收方已 open 读端）──
    let wfd = loop {
        if signal::TERM.load(Ordering::Relaxed) {
            return;
        }
        match pipe::open_sender(FIFO_PATH) {
            Ok(fd) => break fd,
            Err(ref e) if e.raw_os_error() == Some(libc::ENXIO) => {
                // 读端尚未打开，稍后重试
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                eprintln!("[PIPE] sender: open_sender failed: {e}");
                return;
            }
        }
    };

    // ── 步骤 5: 启用用户态中断 ──
    unsafe { syscall::stui(); }

    let sender = PipeSender::new(wfd, uipi_index, token);

    // ── 步骤 5b: 用 sender 发送 GO 标记并同步起跑 ──
    // 发送方发送 GO 时写端必然已打开；接收方读到 GO 才计时，
    // 双方以 GO 到达时刻对齐 t0。GO 不计入 TOTAL。
    if let Err(e) = sender.write_all(&GO).await {
        eprintln!("[PIPE] sender: write GO failed: {e}");
        return;
    }
    let start = Instant::now();

    let chunk = vec![0x5Au8; CHUNK];
    let mut sent = 0usize;

    while sent < TOTAL {
        if signal::TERM.load(Ordering::Relaxed) {
            break;
        }
        if let Err(e) = sender.write_all(&chunk).await {
            eprintln!("[PIPE] sender: write failed: {e}");
            break;
        }
        sent += CHUNK;
    }

    // ── 步骤 6: 等待接收方读完所有数据的 ack 中断 ──
    // 接收方读完 TOTAL 后 senduipi 通知本端，uintr().await 异步等待
    // let _ = uintr(ack_token).await;
    let elapsed = start.elapsed();

    // ── 步骤 7: 打印吞吐量 ──
    let secs = elapsed.as_secs_f64();
    let mib = sent as f64 / (1024.0 * 1024.0);
    eprintln!(
        "[PIPE] sender: {mib:.1} MiB / {secs:.3} s = {:.1} MiB/s",
        mib / secs
    );
    eprintln!(
        "[sender] senduipi(notify) 次数: {}",
        sender.interrupts_sent()
    );
    eprintln!("[sender] 睡眠唤醒次数: {}", sender.sleeps());
}

/// 同步连接到接收方，发送本端 uintrfd，并接收对端 uintrfd
fn connect_and_exchange(uintrfd: RawFd) -> Result<RawFd, String> {
    let socket = loop {
        if signal::TERM.load(Ordering::Relaxed) {
            return Err("interrupted".into());
        }
        match std::os::unix::net::UnixStream::connect(SOCKET_PATH) {
            Ok(s) => break s,
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionRefused
                        || e.kind() == std::io::ErrorKind::NotFound => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(format!("connect: {e}")),
        }
    };

    send_fd(&socket, uintrfd).map_err(|e| format!("send uintr fd: {e}"))?;
    let receiver_fd = recv_fd(&socket).map_err(|e| format!("recv uintr fd: {e}"))?;

    Ok(receiver_fd)
}
