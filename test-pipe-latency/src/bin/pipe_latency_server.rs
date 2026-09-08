// 基于用户态中断（UINTR）的异步命名管道延迟测试 — server
//
// 两条 FIFO 全双工，server 原样回显，client 统计 RTT。
//
//   FIFO1: client → server（client 写 8 字节，server 读 8 字节）
//   FIFO2: server → client（server 写 8 字节回显，client 读 8 字节）
//
// 每轮 ping-pong：client write_all(8B) → server read_exact(8B) + write_all(8B)
//                  → client read_exact(8B)

use std::io::Write;
use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::time::Duration;

use uintr_runtime::executor::Executor;
use uintr_runtime::pipe::{self, PipeReceiver, PipeSender};
use uintr_runtime::signal;
use uintr_runtime::uintr::async_wait;
use uintr_runtime::uintr::connection::{recv_fd, send_fd};
use uintr_runtime::uintr::syscall;
use uintr_runtime::uintr::UINTR_VECTOR;

#[path = "../glue.rs"]
mod glue;
use glue::set_handler_token;

/// FIFO1: client → server
const FIFO1_PATH: &str = "/tmp/uintr-pipe-lat-1.fifo";
/// FIFO2: server → client
const FIFO2_PATH: &str = "/tmp/uintr-pipe-lat-2.fifo";
/// 交换 uintrfd 的 socket
const SOCKET_PATH: &str = "/tmp/uintr-pipe-lat.sock";

/// 延迟预热轮数（不计入统计）
const WARMUP: usize = 2_000;
/// 延迟统计轮数
const ROUNDS: usize = 100_000;
/// 每轮消息大小
const MSG: [u8; 8] = [0xA5; 8];

// C 语言中断处理程序声明（由 handler.c 提供，本 crate 的 build.rs 编译）
unsafe extern "C" {
    fn ui_handler(ui_frame: *mut syscall::UintrFrame, vector: u64);
}

fn main() {
    eprintln!("[INFO] UINTR Pipe Latency Server mode");
    if signal::register_signals().is_err() {
        return;
    }
    let ex = Executor::new().expect("executor init");
    ex.spawn(run());
    ex.block_on();
    eprintln!("[INFO] Shutting down");
}

/// server 入口：原样回显，运行 WARMUP + ROUNDS 轮
pub async fn run() {
    // ── 步骤 1: 初始化 UINTR ──
    let token = async_wait::init_token("pipe-latency-server");
    set_handler_token(token.clone());

    if syscall::uintr_register_handler(ui_handler, 0).is_err() {
        return;
    }
    let uintrfd = match syscall::uintr_create_fd(UINTR_VECTOR as i32, 0) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    // 注意：stui() 推迟到 fd 交换之后，避免 EINTR

    // ── 步骤 2: 创建两条 FIFO ──
    if let Err(e) = pipe::mkfifo(FIFO1_PATH) {
        eprintln!("[PIPE-LAT] server: mkfifo1 failed: {e}");
        return;
    }
    if let Err(e) = pipe::mkfifo(FIFO2_PATH) {
        eprintln!("[PIPE-LAT] server: mkfifo2 failed: {e}");
        return;
    }

    // ── 步骤 3: 等待 client 连接，交换 uintrfd ──
    let (client_fd, mut socket) = match wait_and_exchange(uintrfd) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("[PIPE-LAT] server: {e}");
            return;
        }
    };

    // ── 步骤 4: 打开 FIFO1 读端 + FIFO2 写端 ──
    // FIFO1 读端：O_NONBLOCK 下立即成功
    let rfd = match pipe::open_receiver(FIFO1_PATH) {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("[PIPE-LAT] server: open FIFO1 read failed: {e}");
            return;
        }
    };
    // FIFO2 写端：需 client 已 open 读端，retry on ENXIO
    let wfd = loop {
        if signal::TERM.load(Ordering::Relaxed) { return; }
        match pipe::open_sender(FIFO2_PATH) {
            Ok(fd) => break fd,
            Err(ref e) if e.raw_os_error() == Some(libc::ENXIO) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                eprintln!("[PIPE-LAT] server: open FIFO2 write failed: {e}");
                return;
            }
        }
    };

    // 两端 FIFO 均已打开，通知 client 可以开始。
    // 否则 client 可能在我们 open FIFO2 写端之前读 FIFO2，
    // 得到「暂无写端」的 0（假 EOF），误判 server 退出。
    if let Err(e) = socket.write_all(&[1u8]) {
        eprintln!("[PIPE-LAT] server: notify client ready failed: {e}");
        return;
    }

    // ── 步骤 5: 注册 client 为 UIPI 发送目标 ──
    let uipi_index = match syscall::uintr_register_sender(client_fd, 0) {
        Ok(index) => index as u64,
        Err(_) => return,
    };

    // ── 步骤 6: 启用用户态中断 ──
    unsafe { syscall::stui(); }
    // 创建 receiver + sender（共用 uipi_index 和 token）──
    let rx = PipeReceiver::new(rfd, token.clone(), uipi_index);
    let tx = PipeSender::new(wfd, uipi_index, token);
    let mut buf = [0u8; 8];

    // ── 步骤 7: 原样回显 WARMUP + ROUNDS 轮 ──
    let total = WARMUP + ROUNDS;
    for _ in 0..total {
        if signal::TERM.load(Ordering::Relaxed) { break; }
        match rx.read_exact(&mut buf).await {
            Ok(n) if n == MSG.len() => {}
            Ok(n) => {
                eprintln!("[PIPE-LAT] server: short read {n}");
                break;
            }
            Err(e) => {
                eprintln!("[PIPE-LAT] server: read failed: {e}");
                break;
            }
        }
        if let Err(e) = tx.write_all(&buf).await {
            eprintln!("[PIPE-LAT] server: write failed: {e}");
            break;
        }
    }

    eprintln!(
        "[server] senduipi(notify) 次数: {}",
        tx.interrupts_sent()
    );
    eprintln!("[server] 睡眠唤醒次数: {}", tx.sleeps());
}

/// server 端：等待 client 连接，接收 client uintrfd，回传本端 uintrfd。
/// 返回 (对端 uintrfd, 连接 socket)——socket 留给后续 ready 握手。
fn wait_and_exchange(
    uintrfd: RawFd,
) -> Result<(RawFd, std::os::unix::net::UnixStream), String> {
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

    let client_fd = recv_fd(&socket).map_err(|e| format!("recv uintr fd: {e}"))?;
    send_fd(&socket, uintrfd).map_err(|e| format!("send uintr fd: {e}"))?;
    Ok((client_fd, socket))
}
