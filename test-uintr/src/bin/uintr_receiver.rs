// 基于用户态中断（UINTR）的接收方协程
//
// 职责：
//   1. 初始化 UINTR 基础设施（注册 handler，创建 uintrfd，启用中断）
//   2. 通过 Unix Domain Socket 等待发送方连接，交换 uintrfd
//   3. 异步协程循环等待用户态中断

use std::os::unix::io::RawFd;
use std::time::Duration;

use uintr_runtime::executor::Executor;
use uintr_runtime::signal;
use uintr_runtime::uintr::async_wait;
use uintr_runtime::uintr::connection::{recv_fd, send_fd};
use uintr_runtime::uintr::syscall;
use uintr_runtime::uintr::UINTR_VECTOR;

#[path = "../glue.rs"]
mod glue;
use glue::set_handler_token;

const SOCKET_PATH: &str = "/tmp/uintr-uart.sock";

// C 语言中断处理程序声明（由 handler.c 提供，本 crate 的 build.rs 编译）
unsafe extern "C" {
    fn ui_handler(ui_frame: *mut syscall::UintrFrame, vector: u64);
}

fn main() {
    eprintln!("[INFO] UINTR Receiver mode, waiting for user interrupts");
    if signal::register_signals().is_err() {
        return;
    }
    let ex = Executor::new().expect("executor init");
    ex.spawn(run());
    ex.block_on();
    eprintln!("[INFO] Shutting down");
}

/// 初始化 UINTR 接收方，完成与发送方的连接握手，启动异步中断等待协程
pub async fn run() {
    // ── 步骤 1: 初始化 UINTR token ──
    let token = async_wait::init_token("uart-receiver");
    set_handler_token(token.clone());

    // ── 步骤 2: 注册 UINTR 中断处理程序 ──
    if syscall::uintr_register_handler(ui_handler, 0).is_err() {
        return;
    }

    // ── 步骤 3: 创建 uintrfd ──
    let uintrfd = match syscall::uintr_create_fd(UINTR_VECTOR as i32, 0) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    // ── 步骤 4: 启用用户态中断 ──
    unsafe { syscall::stui(); }

    // ── 步骤 5: 等待发送方连接，交换文件描述符 ──
    let client_fd = match wait_for_client(uintrfd) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    // ── 步骤 6: 注册发送者 ──
    let uipi_index = match syscall::uintr_register_sender(client_fd, 0) {
        Ok(index) => index,
        Err(_) => return,
    };

    // ── 步骤 7: 异步等待用户态中断（协程） ──
    while !signal::TERM.load(std::sync::atomic::Ordering::Relaxed) {
        match async_wait::uintr_wait().await {
            Ok(()) => {
                // 发送响应中断回发送方
                unsafe { syscall::senduipi(uipi_index as u64); }
            }
            Err(_) => break,
        }
    }

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

    // 接收发送方的 uintrfd
    let client_fd = recv_fd(&socket)
        .map_err(|e| format!("recv_fd: {}", e))?;

    // 发送本方的 uintrfd
    send_fd(&socket, server_fd)
        .map_err(|e| format!("send_fd: {}", e))?;

    Ok(client_fd)
}
