// 基于共享内存 + 用户态中断（UINTR）的接收方进程（吞吐量测试）
//
// 与 test-pipe/pipe_receiver 对齐的测试方法：
//   固定接收 TOTAL=256MB（CHUNK=64KB 分块）0x5A 数据；
//   起跑前先读 sender 发来的 8 字节 GO 标记，读到后取 t0，再开始累计读取。

use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use uintr_runtime::executor::Executor;
use uintr_runtime::shm::{self, Receiver};
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

const SOCKET_PATH: &str = "/tmp/uintr-shm.sock";

// C 语言中断处理程序声明（由 handler.c 提供，本 crate 的 build.rs 编译）
unsafe extern "C" {
    fn ui_handler(ui_frame: *mut syscall::UintrFrame, vector: u64);
}

fn main() {
    eprintln!("[INFO] SHM Receiver mode, waiting for shared-memory messages");
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
// 接收方可能被 SIGINT 终止：executor 的 block_on 检测到 TERM 会直接 break 返回，
// run() 协程尾部代码可能不执行。因此统计写入全局原子变量，读取打印集中在此。
pub static RECV_BYTES: AtomicU64 = AtomicU64::new(0);
pub static RECV_INTR: AtomicU64 = AtomicU64::new(0);
pub static RECV_SLEEP: AtomicU64 = AtomicU64::new(0);

/// 打印统计（elapsed_secs 由调用方传入：正常完成时用 run 内 t0 计时）
pub fn print_stats(elapsed_secs: f64) {
    let bytes = RECV_BYTES.load(Ordering::Relaxed);
    let intr = RECV_INTR.load(Ordering::Relaxed);
    let sleep = RECV_SLEEP.load(Ordering::Relaxed);
    let mib = bytes as f64 / (1024.0 * 1024.0);
    eprintln!(
        "[SHM] receiver: {mib:.1} MiB / {elapsed_secs:.3} s = {:.1} MiB/s",
        mib / elapsed_secs
    );
    eprintln!("[receiver] senduipi(notify) 次数: {intr}");
    eprintln!("[receiver] 睡眠唤醒次数: {sleep}");
}

/// 接收方入口：先读 GO 标记对齐 t0，再累计读取直到收到 TOTAL 字节
pub async fn run() {
    // ── 步骤 1: 初始化 UINTR token 与 handler ──
    let token = async_wait::init_token("shm-receiver");
    set_handler_token(token.clone());

    if syscall::uintr_register_handler(ui_handler, 0).is_err() {
        return;
    }

    let uintrfd = match syscall::uintr_create_fd(UINTR_VECTOR as i32, 0) {
        Ok(fd) => fd,
        Err(_) => return,
    };

    // 注意：stui() 推迟到 fd 交换之后，避免 EINTR

    // ── 步骤 2: 等待发送方连接，交换 fd，拿到 shm fd ──
    let (shm_fd, sender_fd) = match wait_and_exchange(uintrfd) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("[SHM] receiver: {e}");
            return;
        }
    };

    // ── 步骤 3: 映射共享内存通道 ──
    let shm = match shm::map_shm(shm_fd) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[SHM] receiver: map_shm failed: {e}");
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

    // ── 步骤 5: 启用用户态中断 ──
    unsafe { syscall::stui(); }

    let receiver = Receiver::new(shm, token, uipi_index);

    // ── 步骤 5b: 读 sender 发来的 GO 标记，对齐双方 t0 起跑 ──
    // 能读到 GO ⇒ 数据通路已就绪；GO 不计入 TOTAL。
    let mut go = [0u8; 8];
    let mut have = 0usize;
    while have < go.len() {
        if signal::TERM.load(Ordering::Relaxed) {
            return;
        }
        match receiver.read(&mut go[have..]).await {
            Ok(n) if n > 0 => have += n,
            Ok(_) => {}
            Err(e) => {
                eprintln!("[SHM] receiver: read GO failed: {e}");
                return;
            }
        }
    }
    if &go != b"GOSTART!" {
        eprintln!("[SHM] receiver: bad GO marker");
        return;
    }
    let t = Instant::now();

    // ── 步骤 6: 累计读取直到 TOTAL ──
    let mut buf = vec![0u8; CHUNK];
    let mut got = 0usize;
    while got < TOTAL {
        if signal::TERM.load(Ordering::Relaxed) {
            break;
        }
        let need = (TOTAL - got).min(buf.len());
        match receiver.read(&mut buf[..need]).await {
            Ok(n) if n > 0 => {
                got += n;
                RECV_BYTES.store(got as u64, Ordering::Relaxed);
                RECV_INTR.store(receiver.interrupts_sent(), Ordering::Relaxed);
                RECV_SLEEP.store(receiver.sleeps(), Ordering::Relaxed);
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("[SHM] receiver: read failed: {e}");
                break;
            }
        }
    }

    // ── 步骤 7: 打印吞吐量（正常完成后由本函数打印）──
    print_stats(t.elapsed().as_secs_f64());
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
