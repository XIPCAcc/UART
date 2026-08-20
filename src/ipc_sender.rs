// 基于共享内存 + 用户态中断（UINTR）的单向 IPC 发送方进程
//
// 职责：
//   1. 初始化 UINTR 基础设施（注册 handler，创建 uintrfd，启用中断）
//   2. 创建共享内存通道，并通过 Unix Domain Socket 与接收方交换 fd
//   3. 循环将消息写入共享内存，并 senduipi 通知接收方

use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::ipc::{self, Sender};
use crate::uintr::benchmark::Benchmarks;
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

/// 发送方入口：发送 `count` 条大小为 `msg_size` 的消息
pub async fn run(count: u32, msg_size: usize) {
    // ── 步骤 1: 初始化 UINTR token 与 handler（与 uintr_sender 对称）──
    let token = async_wait::init_token("ipc-sender");
    set_handler_token(token.clone());

    if syscall::uintr_register_handler(ui_handler, UINTR_HANDLER_FLAG_WAITING_ANY).is_err() {
        return;
    }

    let uintrfd = match syscall::uintr_create_fd(UINTR_VECTOR as i32, 0) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    unsafe { syscall::stui(); }

    // ── 步骤 2: 创建共享内存通道 ──
    let (shm_fd, shm) = match ipc::create_shm() {
        Ok(x) => x,
        Err(e) => {
            eprintln!("[IPC] sender: create_shm failed: {e}");
            return;
        }
    };

    // ── 步骤 3: 连接接收方，交换 shm fd 与 uintrfd ──
    let receiver_fd = match connect_and_exchange(uintrfd, shm_fd) {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("[IPC] sender: {e}");
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

    // ── 步骤 5: 循环写消息并通知（带性能统计）──
    let sender = Sender::new(shm, uipi_index, token);
    let mut buf = vec![0u8; msg_size];
    let mut bench = Benchmarks::new();
    bench.reset_total_start();

    for i in 0..count {
        if signal::TERM.load(Ordering::Relaxed) {
            break;
        }
        fill_message(&mut buf, i);
        bench.start_operation();
        if let Err(e) = sender.write(&buf).await {
            eprintln!("[IPC] sender: write failed: {e}");
            break;
        }
        bench.end_operation();
    }

    let result = bench.evaluate();
    bench.print_results(&result);

    let bytes_total = result.message_count as u64 * msg_size as u64;
    let secs = result.total_duration_ms / 1000.0;
    let mib = bytes_total as f64 / (1024.0 * 1024.0) / secs;
    eprintln!(
        "[IPC] sender: {bytes_total} bytes in {secs:.3}s -> {mib:.2} MiB/s"
    );
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

/// 用序号填充消息体：前 4 字节为大端序号，其余填充 0
fn fill_message(buf: &mut [u8], seq: u32) {
    for b in buf.iter_mut() {
        *b = 0;
    }
    buf[0] = (seq >> 24) as u8;
    buf[1] = (seq >> 16) as u8;
    buf[2] = (seq >> 8) as u8;
    buf[3] = seq as u8;
}
