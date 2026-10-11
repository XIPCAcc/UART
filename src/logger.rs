// 全局日志：所有 eprintln! 统一走这里，自动带上自进程启动起的毫秒时间戳。
// 起点用 OnceLock<Instant> 固定（main 最开头 init），任意线程可并发调用。

use std::fmt;
use std::sync::OnceLock;
use std::time::Instant;

static START: OnceLock<Instant> = OnceLock::new();

/// 固定时间戳起点，必须在 main 最开头调用
pub fn init() {
    let _ = START.set(Instant::now());
}

pub fn log(args: fmt::Arguments<'_>) {
    let ms = match START.get() {
        Some(t) => t.elapsed().as_secs_f64() * 1000.0,
        None => 0.0,
    };
    eprintln!("[{:>10.5}ms]  {}", ms, args);
}

/// 用法与 eprintln! 完全一致：log!("... {} ...", x)
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        $crate::logger::log(format_args!($($arg)*))
    };
}
