// 基于用户态中断（UINTR）的发送方协程
//
// 职责：
//   1. 初始化 UINTR 基础设施（注册 handler，创建 uintrfd，启用中断）
//   2. 通过 Unix Domain Socket 连接到接收方，交换 uintrfd
//   3. 异步协程循环发送用户态中断

use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::time::Duration;

use uintr_runtime::executor::Executor;
use uintr_runtime::signal;
use uintr_runtime::uintr::async_wait;
use uintr_runtime::uintr::benchmark::Benchmarks;
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
    // 简单参数解析：--count/-c <N>  --interval-ms <MS>
    let args: Vec<String> = std::env::args().collect();
    let mut count = 1u32;
    let mut interval_ms = 0u64;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--count" | "-c" => {
                i += 1;
                if i < args.len() {
                    count = args[i].parse().unwrap_or(1);
                }
            }
            "--interval-ms" => {
                i += 1;
                if i < args.len() {
                    interval_ms = args[i].parse().unwrap_or(0);
                }
            }
            _ => {}
        }
        i += 1;
    }

    eprintln!("[INFO] UINTR Sender mode: count={count} interval_ms={interval_ms}");
    if signal::register_signals().is_err() {
        return;
    }
    let ex = Executor::new().expect("executor init");
    ex.spawn(run(count, interval_ms));
    ex.block_on();
    eprintln!("[INFO] Shutting down");
}

/// 初始化 UINTR 发送方，完成与接收方的连接握手，启动异步中断发送协程
pub async fn run(count: u32, interval_ms: u64) {
    // ── 步骤 1: 初始化 UINTR token（用于接收响应中断）──
    let token = async_wait::init_token("uart-sender");
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

    // ── 步骤 5: 连接到接收方，交换文件描述符 ──
    let receiver_fd = match connect_to_receiver(uintrfd) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    // ── 步骤 6: 注册发送者 ──
    let uipi_index = match syscall::uintr_register_sender(receiver_fd, 0) {
        Ok(index) => index,
        Err(_) => return,
    };

    // 给接收方一点时间准备好
    std::thread::sleep(Duration::from_millis(500));

    // ── 步骤 7: 循环发送用户态中断，等待响应，测量延迟 ──
    let mut bench = Benchmarks::new();
    bench.reset_total_start();

    for i in 0..count {
        if signal::TERM.load(Ordering::Relaxed) {
            break;
        }

        // 开始此轮计时
        bench.start_operation();

        // 发送中断
        unsafe { syscall::senduipi(uipi_index as u64); }

        // 异步等待接收方的响应中断
        match async_wait::uintr_wait().await {
            Ok(()) => {
                bench.end_operation();
            }
            Err(_) => break,
        }

        if interval_ms > 0 && i + 1 < count {
            std::thread::sleep(Duration::from_millis(interval_ms));
        }
    }

    // 输出基准测试结果
    let result = bench.evaluate();
    bench.print_results(&result);
}

/// 同步连接到接收方，交换 uintrfd
fn connect_to_receiver(client_fd: RawFd) -> Result<RawFd, String> {
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
            Err(e) => return Err(format!("connect: {}", e)),
        }
    };

    // 发送本方的 uintrfd
    send_fd(&socket, client_fd)
        .map_err(|e| format!("send_fd: {}", e))?;

    // 接收对方的 uintrfd
    let receiver_fd = recv_fd(&socket)
        .map_err(|e| format!("recv_fd: {}", e))?;

    Ok(receiver_fd)
}
