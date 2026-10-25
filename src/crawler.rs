// 爬虫守护进程：TCP 监听外部请求，每个请求触发一个爬虫协程任务
//
// 行为：
//   - 在 --listen 地址上监听 TCP；请求为多行文本，每行一个爬虫任务：
//     `<host:port> [path]`，逐行解析、逐行回 ACK、逐行 spawn
//   - 每收到一行请求，回 ACK 并 spawn 一个 crawl_one 协程去抓目标
//   - 任务多时执行器自动扩容起新 worker，任务完成时空闲 worker 自动缩容退出
//   - 收到 SIGINT/SIGTERM 时 TERM 标志被置位，accept 被 wake_all 唤醒后返回
//     错误，退出 accept 循环并打印成功/失败/字节数汇总

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::elog;

use crate::cli::Config;
use crate::executor::executor;
use crate::log;
use crate::signal;
use crate::tcp::AsyncTcpListener;

// ── 全局抓取统计（观测用）─────────────────────────────────────

static OK_COUNT:    AtomicU64 = AtomicU64::new(0);
static FAIL_COUNT:  AtomicU64 = AtomicU64::new(0);
static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);
static SEQ:         AtomicU64 = AtomicU64::new(0);

pub async fn run(config: &Config) {
    let listen_addr: SocketAddr = match config.listen.parse() {
        Ok(a) => a,
        Err(e) => {
            elog!("[ERROR] invalid listen address: {e}");
            return;
        }
    };

    let listener = match AsyncTcpListener::bind(listen_addr) {
        Ok(l) => l,
        Err(e) => {
            elog!("[ERROR] listen on {listen_addr} failed: {e}");
            return;
        }
    };
    log!(
        "[CRAWLER] daemon listening on {listen_addr}, max_workers={}",
        executor().max_workers()
    );

    let mut req_seq = 0u32;
    loop {
        if signal::TERM.load(Ordering::Relaxed) {
            break;
        }
        match listener.accept().await {
            Ok((conn, peer)) => {
                req_seq += 1;
                // 每个连接一个协程：读请求 → 回 ACK → 投放爬虫任务
                executor().spawn(handle_request(conn, peer, req_seq));
            }
            Err(e) => {
                // TERM 唤醒后 accept 返回 Interrupted，属正常退出路径
                if e.kind() != std::io::ErrorKind::Interrupted {
                    elog!("[ERROR] accept failed: {e}");
                }
                break;
            }
        }
    }

    // 退出汇总
    elog!(
        "[CRAWLER] summary: ok={} fail={} total_bytes={} ({:.1} KiB)",
        OK_COUNT.load(Ordering::Relaxed),
        FAIL_COUNT.load(Ordering::Relaxed),
        TOTAL_BYTES.load(Ordering::Relaxed),
        TOTAL_BYTES.load(Ordering::Relaxed) as f64 / 1024.0
    );
}

/// 处理一个外部请求连接：按行读取（每行一个 `<host:port> [path]` 爬虫请求），
/// 逐行回 ACK 并投放爬虫任务，直到对端关闭（EOF 时处理无换行的最后一行）
async fn handle_request(conn: crate::tcp::AsyncTcpStream, peer: SocketAddr, req_seq: u32) {
    let mut buf = [0u8; 4096];
    let mut pending: Vec<u8> = Vec::new();
    loop {
        let n = match conn.read(&mut buf).await {
            Ok(0) => break, // EOF：对端关闭
            Ok(n) => n,
            Err(e) => {
                log!("[DAEMON] req#{req_seq} read from {peer} failed: {e}");
                return;
            }
        };
        pending.extend_from_slice(&buf[..n]);
        // 按行切分，逐行处理完整请求
        while let Some(pos) = pending.iter().position(|&b| b == b'\n') {
            let raw: Vec<u8> = pending.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&raw);
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let ack = dispatch_line(line, peer, req_seq);
            if conn.write_all(ack.as_bytes()).await.is_err() {
                return; // 对端已断开
            }
        }
    }
    // EOF 后处理最后一行无换行的残留
    let line = String::from_utf8_lossy(&pending);
    let line = line.trim();
    if !line.is_empty() {
        let ack = dispatch_line(line, peer, req_seq);
        let _ = conn.write_all(ack.as_bytes()).await;
    }
}

/// 处理一行爬虫请求：合法则 spawn 爬虫任务并返回 ACK 文本，非法则返回错误提示
static T0: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
static IN_FLIGHT: AtomicU64 = AtomicU64::new(0);
static BATCH_T0_NS: AtomicU64 = AtomicU64::new(0);

fn now_ns() -> u64 {
    T0.get_or_init(std::time::Instant::now).elapsed().as_nanos() as u64
}

async fn crawl_tracked(addr: SocketAddr, path: String, seq: u32) {
    crawl_one(addr, path, seq).await;
    if IN_FLIGHT.fetch_sub(1, Ordering::AcqRel) == 1 {
        let dt = now_ns() - BATCH_T0_NS.load(Ordering::Relaxed);
        eprintln!("[BATCH] wall={}us", dt / 1000);
    }
}

fn dispatch_line(line: &str, peer: SocketAddr, req_seq: u32) -> String {
    match parse_request(line) {
        Some((addr, path)) => {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed) as u32;
            log!("[DAEMON] req#{req_seq} from {peer}: crawl {addr}{path} as task #{seq}");
            let ack = format!("OK #{seq} {addr}{path}\n");
            if IN_FLIGHT.fetch_add(1, Ordering::AcqRel) == 0 {
                BATCH_T0_NS.store(now_ns(), Ordering::Relaxed);
            }
            executor().spawn(crawl_tracked(addr, path, seq));
            ack
        }
        None => {
            log!("[DAEMON] req#{req_seq} from {peer}: bad request {line:?}");
            "ERR usage: <host:port> [path]\n".to_string()
        }
    }
}

/// 解析请求行：`<host:port> [path]`，path 缺省为 "/"
fn parse_request(line: &str) -> Option<(SocketAddr, String)> {
    let mut parts = line.split_whitespace();
    let addr: SocketAddr = parts.next()?.parse().ok()?;
    let path = match parts.next() {
        Some(p) if p.starts_with('/') => p.to_string(),
        Some(p) => format!("/{p}"),
        None => "/".to_string(),
    };
    Some((addr, path))
}

async fn crawl_one(addr: std::net::SocketAddr, path: String, seq: u32) {
    let start = std::time::Instant::now();

    let stream = match crate::tcp::AsyncTcpStream::connect(addr).await {
        Ok(s) => s,
        Err(e) => {
            FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
            log!("[CRAWL] #{seq:<6} FAIL connect: {e}");
            return;
        }
    };

    // HTTP/1.0 GET：请求结尾要 \r\n\r\n
    let request = format!(
        "GET {} HTTP/1.0\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, addr
    );
    if let Err(e) = stream.write_all(request.as_bytes()).await {
        FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
        log!("[CRAWL] #{seq:<6} FAIL write: {e}");
        return;
    }

    // 读取响应直到 EOF（HTTP/1.0 Connection: close 保证对端发完数据后关闭）。
    // 第一块数据同时用来解析 HTTP 状态行（"HTTP/1.0 200 OK"）。
    let mut buf = [0u8; 4096];
    let mut total = 0usize;
    let mut status: Option<u16> = None;
    loop {
        match stream.read(&mut buf).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                if status.is_none() {
                    status = parse_status(&buf[..n]);
                }
                total += n;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => {
                FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
                log!("[CRAWL] #{seq:<6} FAIL read after {total}B: {e}");
                return;
            }
        }
    }

    TOTAL_BYTES.fetch_add(total as u64, Ordering::Relaxed);
    match status {
        Some(200..=299) => {
            OK_COUNT.fetch_add(1, Ordering::Relaxed);
            log!(
                "[CRAWL] #{seq:<6} OK   status={} bytes={total:<7} elapsed={:?}",
                status.unwrap(),
                start.elapsed()
            );
        }
        Some(code) => {
            FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
            log!(
                "[CRAWL] #{seq:<6} FAIL status={code} bytes={total:<7} elapsed={:?}",
                start.elapsed()
            );
        }
        None => {
            FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
            log!(
                "[CRAWL] #{seq:<6} FAIL no-status bytes={total:<7} elapsed={:?}",
                start.elapsed()
            );
        }
    }
}

/// 从响应首块数据解析 HTTP 状态码："HTTP/1.0 200 OK" → 200
fn parse_status(chunk: &[u8]) -> Option<u16> {
    // 找到状态行结尾
    let line_end = chunk.windows(2).position(|w| w == b"\r\n")?;
    let line = std::str::from_utf8(&chunk[..line_end]).ok()?;
    // "HTTP/1.0 200" 或 "HTTP/1.1 200"
    let mut parts = line.split(' ');
    let version = parts.next()?;
    if !version.starts_with("HTTP/") {
        return None;
    }
    parts.next()?.parse().ok()
}
