// 基于共享内存 + 用户态中断（UINTR）的发送方进程（吞吐量测试）
//
// 与 test-pipe/pipe_sender 对齐的测试方法：
//   固定发送 TOTAL=256MB（CHUNK=64KB 分块）0x5A 数据；
//   起跑前先用 sender 发送 8 字节 GO 标记，双方以其到达时刻对齐 t0。

use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use uintr_runtime::executor::Executor;
use uintr_runtime::shm::{self, Sender};
use uintr_runtime::signal;
use uintr_runtime::uintr::async_wait;
use uintr_runtime::uintr::connection::{recv_fd, send_fd};
use uintr_runtime::uintr::syscall;
use uintr_runtime::uintr::UINTR_VECTOR;

#[path = "../glue.rs"]
mod glue;
use glue::set_handler_token;

/// 单次写入 64 KB（环形缓冲区容量，与 pipe 测试一致）
const CHUNK: usize = 64 * 1024;
/// 总传输量 256 MB
const TOTAL: usize = 256 * 1024 * 1024;

const SOCKET_PATH: &str = "/tmp/uintr-shm.sock";

/// 起跑标记：写端就绪后由 sender 发送，双方以其到达时刻对齐 t0
const GO: [u8; 8] = *b"GOSTART!";

// C 语言中断处理程序声明（由 handler.c 提供，本 crate 的 build.rs 编译）
unsafe extern "C" {
    fn ui_handler(ui_frame: *mut syscall::UintrFrame, vector: u64);
}

fn main() {
    eprintln!("[INFO] SHM Sender mode: 256MB / 64KB chunks");
    if signal::register_signals().is_err() {
        return;
    }
    let ex = Executor::new().expect("executor init");
    ex.spawn(run());
    ex.block_on();
    eprintln!("[INFO] Shutting down");
}

/// 发送方入口：先发 GO 标记同步起跑，再发送 TOTAL 字节 0x5A 数据
pub async fn run() {
    // ── 步骤 1: 初始化 UINTR token 与 handler ──
    let token = async_wait::init_token("shm-sender");
    set_handler_token(token.clone());

    if syscall::uintr_register_handler(ui_handler, 0).is_err() {
        return;
    }

    let uintrfd = match syscall::uintr_create_fd(UINTR_VECTOR as i32, 0) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    // 注意：stui() 推迟到 fd 交换之后，避免 EINTR

    // ── 步骤 2: 创建共享内存通道 ──
    let (shm_fd, shm) = match shm::create_shm() {
        Ok(x) => x,
        Err(e) => {
            eprintln!("[SHM] sender: create_shm failed: {e}");
            return;
        }
    };

    // ── 步骤 3: 连接接收方，交换 shm fd 与 uintrfd ──
    let receiver_fd = match connect_and_exchange(uintrfd, shm_fd) {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("[SHM] sender: {e}");
            return;
        }
    };
    // shm_fd 已通过 SCM_RIGHTS 发送给对端，本端可关闭（mmap 映射仍有效）
    unsafe { libc::close(shm_fd); }

    // ── 步骤 4: 注册接收方为 UIPI 发送目标 ──
    let uipi_index = match syscall::uintr_register_sender(receiver_fd, 0) {
        Ok(index) => index as u64,
        Err(_) => return,
    };

    // ── 步骤 5: 启用用户态中断 ──
    unsafe { syscall::stui(); }

    let sender = Sender::new(shm, uipi_index, token);

    // ── 步骤 5b: 用 sender 发送 GO 标记并同步起跑 ──
    // 发送 GO 时数据通路已就绪；接收方读到 GO 才计时，双方 t0 对齐。
    if let Err(e) = sender.write(&GO).await {
        eprintln!("[SHM] sender: write GO failed: {e}");
        return;
    }
    let start = Instant::now();

    // ── 步骤 6: 发送 TOTAL 字节 ──
    let chunk = vec![0x5Au8; CHUNK];
    let mut sent = 0usize;
    while sent < TOTAL {
        if signal::TERM.load(Ordering::Relaxed) {
            break;
        }
        if let Err(e) = sender.write(&chunk).await {
            eprintln!("[SHM] sender: write failed: {e}");
            break;
        }
        sent += CHUNK;
    }

    // ── 步骤 7: 打印吞吐量 ──
    let secs = start.elapsed().as_secs_f64();
    let mib = sent as f64 / (1024.0 * 1024.0);
    eprintln!("[SHM] sender: {mib:.1} MiB / {secs:.3} s = {:.1} MiB/s", mib / secs);
    eprintln!(
        "[sender] senduipi(notify) 次数: {}",
        sender.interrupts_sent()
    );
    eprintln!("[sender] 睡眠唤醒次数: {}", sender.sleeps());
}

/// 同步连接到接收方，依次发送 [shm_fd, 本端 uintrfd]，并接收对端 uintrfd
fn connect_and_exchange(uintrfd: RawFd, shm_fd: RawFd) -> Result<RawFd, String> {
    let socket = loop {
        if signal::TERM.load(Ordering::Relaxed) {
            return Err("interrupted".into());
        }
        match std::os::unix::net::UnixStream::connect(SOCKET_PATH) {
            Ok(s) => break s,
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionRefused
                        || e.kind() == std::io::ErrorKind::NotFound => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => return Err(format!("connect: {e}")),
        }
    };

    send_fd(&socket, shm_fd).map_err(|e| format!("send shm fd: {e}"))?;
    send_fd(&socket, uintrfd).map_err(|e| format!("send uintr fd: {e}"))?;
    let receiver_fd = recv_fd(&socket).map_err(|e| format!("recv uintr fd: {e}"))?;

    Ok(receiver_fd)
}
