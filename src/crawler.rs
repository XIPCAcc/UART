// 多核爬虫：固定间隔定时投放任务，worker 动态扩缩容
//
// 行为：
//   - 每 `interval_ms` 毫秒投放 `burst` 个 crawl_one 任务到全局队列
//   - 任务多时执行器自动扩容起新 worker，任务完成时空闲 worker 自动缩容退出
//   - 收到 SIGINT/SIGTERM 时 TERM 标志被置位，crawler 协程通过 sleep_ms 感知，
//     退出前打印成功/失败/字节数汇总

use std::sync::atomic::{AtomicU64, Ordering};

use crate::cli::Config;
use crate::executor::executor;
use crate::log;
use crate::reactor::sleep_ms;
use crate::signal;

// ── 全局抓取统计（观测用）─────────────────────────────────────

static OK_COUNT:    AtomicU64 = AtomicU64::new(0);
static FAIL_COUNT:  AtomicU64 = AtomicU64::new(0);
static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);
static SEQ:         AtomicU64 = AtomicU64::new(0);

pub async fn run(config: &Config) {
    let (addr, path, interval_ms, burst) = match &config.mode {
        crate::cli::RunMode::Crawler { addr, path, interval_ms, burst } => {
            (addr.as_str(), path.as_str(), *interval_ms, *burst)
        }
        _ => return,
    };
    let addr: std::net::SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => {
            log!("[ERROR] invalid address: {e}");
            return;
        }
    };

    log!(
        "[CRAWLER] start target={addr} path={path} interval={interval_ms}ms burst={burst} max_workers={}",
        executor().max_workers()
    );

    loop {
        if signal::TERM.load(Ordering::Relaxed) {
            break;
        }

        // 投放一批任务
        for _ in 0..burst {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed) as u32;
            executor().spawn(crawl_one(addr, path.to_string(), seq));
        }
        log!(
            "[CRAWLER] dispatched {burst} tasks (workers: {})",
            executor().active_workers() + 1
        );

        sleep_ms(interval_ms).await;
    }

    // 退出汇总
    log!(
        "[CRAWLER] summary: ok={} fail={} total_bytes={} ({:.1} KiB)",
        OK_COUNT.load(Ordering::Relaxed),
        FAIL_COUNT.load(Ordering::Relaxed),
        TOTAL_BYTES.load(Ordering::Relaxed),
        TOTAL_BYTES.load(Ordering::Relaxed) as f64 / 1024.0
    );
}

async fn crawl_one(addr: std::net::SocketAddr, path: String, seq: u32) {
    let start = std::time::Instant::now();

    let mut stream = match crate::tcp::AsyncTcpStream::connect(addr).await {
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
