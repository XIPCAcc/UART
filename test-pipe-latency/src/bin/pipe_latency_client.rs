// 基于用户态中断（UINTR）的异步命名管道延迟测试 — client
//
// 两条 FIFO 全双工，server 原样回显，client 统计 RTT。
//
//   FIFO1: client → server（client 写 8 字节，server 读 8 字节）
//   FIFO2: server → client（server 写 8 字节回显，client 读 8 字节）
//
// 每轮 ping-pong：client write_all(8B) → server read_exact(8B) + write_all(8B)
//                  → client read_exact(8B)

use std::io::Read;
use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

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
    eprintln!("[INFO] UINTR Pipe Latency Client mode");
    if signal::register_signals().is_err() {
        return;
    }
    let ex = Executor::new().expect("executor init");
    ex.spawn(run());
    ex.block_on();
    eprintln!("[INFO] Shutting down");
}

/// client 入口：预热 + 计时 ping-pong，统计 RTT
pub async fn run() {
    // ── 步骤 1: 初始化 UINTR ──
    let token = async_wait::init_token("pipe-latency-client");
    set_handler_token(token.clone());

    if syscall::uintr_register_handler(ui_handler, 0).is_err() {
        return;
    }
    let uintrfd = match syscall::uintr_create_fd(UINTR_VECTOR as i32, 0) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    // 注意：stui() 推迟到 fd 交换之后，避免 EINTR

    // ── 步骤 2: 连接 server，交换 uintrfd ──
    let (server_fd, mut socket) = match connect_and_exchange(uintrfd) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("[PIPE-LAT] client: {e}");
            return;
        }
    };

    // ── 步骤 3: 注册 server 为 UIPI 发送目标 ──
    let uipi_index = match syscall::uintr_register_sender(server_fd, 0) {
        Ok(index) => index as u64,
        Err(_) => return,
    };

    // ── 步骤 4: 打开 FIFO1 写端 + FIFO2 读端 ──
    // FIFO1 写端：需 server 已 open 读端，retry on ENXIO
    let wfd = loop {
        if signal::TERM.load(Ordering::Relaxed) { return; }
        match pipe::open_sender(FIFO1_PATH) {
            Ok(fd) => break fd,
            Err(ref e) if e.raw_os_error() == Some(libc::ENXIO) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                eprintln!("[PIPE-LAT] client: open FIFO1 write failed: {e}");
                return;
            }
        }
    };
    // FIFO2 读端：O_NONBLOCK 下立即成功
    let rfd = match pipe::open_receiver(FIFO2_PATH) {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("[PIPE-LAT] client: open FIFO2 read failed: {e}");
            return;
        }
    };

    // 等待 server 的 ready 信号（server 已把 FIFO2 写端打开）。
    // 否则此刻读 FIFO2 会因「尚无写端」直接返回 0，被误判为 server 退出。
    let mut ready = [0u8; 1];
    if let Err(e) = socket.read_exact(&mut ready) {
        eprintln!("[PIPE-LAT] client: wait server ready failed: {e}");
        return;
    }

    // ── 步骤 5: 启用用户态中断 ──
    unsafe { syscall::stui(); }
    // 创建 sender + receiver（共用 uipi_index 和 token）──
    let tx = PipeSender::new(wfd, uipi_index, token.clone());
    let rx = PipeReceiver::new(rfd, token, uipi_index);
    let mut pong = [0u8; 8];

    // ── 步骤 6: 预热 ──
    for _ in 0..WARMUP {
        if signal::TERM.load(Ordering::Relaxed) { return; }
        if let Err(e) = tx.write_all(&MSG).await {
            eprintln!("[PIPE-LAT] client: warmup write failed: {e}");
            return;
        }
        match rx.read_exact(&mut pong).await {
            Ok(n) if n == MSG.len() => {}
            Ok(n) => {
                eprintln!("[PIPE-LAT] client: warmup short read {n} (server exited?)");
                return;
            }
            Err(e) => {
                eprintln!("[PIPE-LAT] client: warmup read failed: {e}");
                return;
            }
        }
    }

    // ── 步骤 7: 计时 ping-pong ──
    let start = Instant::now();
    for _ in 0..ROUNDS {
        if signal::TERM.load(Ordering::Relaxed) { break; }
        if let Err(e) = tx.write_all(&MSG).await {
            eprintln!("[PIPE-LAT] client: write failed: {e}");
            break;
        }
        match rx.read_exact(&mut pong).await {
            Ok(n) if n == MSG.len() => {}
            Ok(n) => {
                eprintln!("[PIPE-LAT] client: short read {n}");
                break;
            }
            Err(e) => {
                eprintln!("[PIPE-LAT] client: read failed: {e}");
                break;
            }
        }
    }
    let elapsed = start.elapsed();

    // ── 步骤 8: 打印 RTT ──
    let rtt_us = elapsed.as_micros() as f64 / ROUNDS as f64;
    let msgs_per_s = ROUNDS as f64 / elapsed.as_secs_f64();
    eprintln!(
        "[PIPE-LAT] client: RTT = {rtt_us:.2} µs/轮  ({msgs_per_s:.0} msg/s)"
    );
    eprintln!(
        "[client] senduipi(notify) 次数: {}",
        tx.interrupts_sent()
    );
    eprintln!("[client] 睡眠唤醒次数: {}", tx.sleeps());
}

/// client 端：连接 server，发送本端 uintrfd，接收对端 uintrfd。
/// 返回 (对端 uintrfd, 连接 socket)——socket 留给后续 ready 握手。
fn connect_and_exchange(
    uintrfd: RawFd,
) -> Result<(RawFd, std::os::unix::net::UnixStream), String> {
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
    let server_fd = recv_fd(&socket).map_err(|e| format!("recv uintr fd: {e}"))?;
    Ok((server_fd, socket))
}
