// 基于用户态中断（UINTR）的接收方协程
//
// 职责：
//   1. 初始化 UINTR 基础设施（注册 handler，创建 uintrfd，启用中断）
//   2. 通过 Unix Domain Socket 等待发送方连接，交换 uintrfd
//   3. 异步协程循环等待用户态中断

use std::os::unix::io::RawFd;
use std::time::Duration;

use crate::uintr::syscall;
use crate::uintr::connection::{send_fd, recv_fd};
use crate::uintr::async_wait;
use crate::uintr::handler::set_handler_token;
use crate::uintr::{UINTR_HANDLER_FLAG_WAITING_ANY, UINTR_VECTOR};

use crate::signal;

const SOCKET_PATH: &str = "/tmp/uintr-uart.sock";

// C 语言中断处理程序声明（由 uintr crate 的 handler.c 提供）
unsafe extern "C" {
    fn ui_handler(ui_frame: *mut syscall::UintrFrame, vector: u64);
}

/// 初始化 UINTR 接收方，完成与发送方的连接握手，启动异步中断等待协程
pub async fn run() {
    // ── 步骤 1: 初始化 UINTR token ──
    let token = async_wait::init_token("uart-receiver");
    set_handler_token(token.clone());
    eprintln!("[INFO] uintr-receiver: token initialized");

    // ── 步骤 2: 注册 UINTR 中断处理程序 ──
    match syscall::uintr_register_handler(ui_handler, UINTR_HANDLER_FLAG_WAITING_ANY) {
        Ok(res) => eprintln!("[INFO] uintr-receiver: handler registered ({})", res),
        Err(e) => {
            eprintln!("[ERROR] uintr-receiver: register handler failed: {}", e);
            return;
        }
    }

    // ── 步骤 3: 创建 uintrfd ──
    let uintrfd = match syscall::uintr_create_fd(UINTR_VECTOR as i32, 0) {
        Ok(fd) => {
            eprintln!("[INFO] uintr-receiver: created uintrfd={}", fd);
            fd
        }
        Err(e) => {
            eprintln!("[ERROR] uintr-receiver: create fd failed: {}", e);
            return;
        }
    };

    // ── 步骤 4: 启用用户态中断 ──
    unsafe { syscall::stui(); }
    eprintln!("[INFO] uintr-receiver: interrupts enabled");

    // ── 步骤 5: 等待发送方连接，交换文件描述符 ──
    let client_fd = match wait_for_client(uintrfd) {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("[ERROR] uintr-receiver: connection failed: {}", e);
            return;
        }
    };
    eprintln!("[INFO] uintr-receiver: received client fd={}", client_fd);

    // ── 步骤 6: 注册发送者 ──
    match syscall::uintr_register_sender(client_fd, 0) {
        Ok(index) => eprintln!("[INFO] uintr-receiver: registered sender, uipi_index={}", index),
        Err(e) => {
            eprintln!("[ERROR] uintr-receiver: register sender failed: {}", e);
            return;
        }
    }

    // ── 步骤 7: 异步等待用户态中断（协程） ──
    eprintln!("[INFO] uintr-receiver: waiting for interrupts...");
    let mut count = 0u64;
    while !signal::TERM.load(std::sync::atomic::Ordering::Relaxed) {
        match async_wait::uintr_wait().await {
            Ok(()) => {
                count += 1;
                eprintln!("[INFO] uintr-receiver: received interrupt #{}", count);
            }
            Err(e) => {
                eprintln!("[ERROR] uintr-receiver: wait error: {}", e);
                break;
            }
        }
    }

    eprintln!("[INFO] uintr-receiver: done, total interrupts received: {}", count);

    // 清理
    let _ = std::fs::remove_file(SOCKET_PATH);
}

/// 同步等待发送方连接，交换 uintrfd
fn wait_for_client(server_fd: RawFd) -> Result<RawFd, String> {
    let _ = std::fs::remove_file(SOCKET_PATH);

    let listener = std::os::unix::net::UnixListener::bind(SOCKET_PATH)
        .map_err(|e| format!("bind socket: {}", e))?;
    listener.set_nonblocking(true)
        .map_err(|e| format!("set nonblocking: {}", e))?;

    eprintln!("[INFO] uintr-receiver: listening on {}", SOCKET_PATH);

    let (socket, _addr) = loop {
        if signal::TERM.load(std::sync::atomic::Ordering::Relaxed) {
            return Err("interrupted".into());
        }
        match listener.accept() {
            Ok(conn) => break conn,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(format!("accept: {}", e)),
        }
    };
    eprintln!("[INFO] uintr-receiver: client connected");

    // 接收发送方的 uintrfd
    let client_fd = recv_fd(&socket)
        .map_err(|e| format!("recv_fd: {}", e))?;
    eprintln!("[INFO] uintr-receiver: received client fd={}", client_fd);

    // 发送本方的 uintrfd
    send_fd(&socket, server_fd)
        .map_err(|e| format!("send_fd: {}", e))?;
    eprintln!("[INFO] uintr-receiver: sent server fd={}", server_fd);

    Ok(client_fd)
}
