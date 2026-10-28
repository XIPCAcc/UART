// 全局日志：所有 eprintln! 统一走这里，自动带上自进程启动起的毫秒时间戳。
// 起点用 OnceLock<Instant> 固定（main 最开头 init），任意线程可并发调用。
//
// 编译开关：默认 `log!` 编译为空（零成本，无格式化、无 write 调用）；
// 调试时 cargo build --features verbose-log。基准测试必须用默认构建，
// 否则每行日志都会引入 write 系统调用并阻塞在 stderr 上，污染 syscall/wall 统计。

use std::fmt;
use std::sync::OnceLock;
use std::time::Instant;

static START: OnceLock<Instant> = OnceLock::new();

/// 固定时间戳起点，必须在 main 最开头调用
pub fn init() {
    let _ = START.set(Instant::now());
}

#[cfg(feature = "verbose-log")]
pub fn log(args: fmt::Arguments<'_>) {
    let ms = match START.get() {
        Some(t) => t.elapsed().as_secs_f64() * 1000.0,
        None => 0.0,
    };
    eprintln!("[{:>10.5}ms]  {}", ms, args);
}

#[cfg(not(feature = "verbose-log"))]
#[inline(always)]
pub fn log(_args: fmt::Arguments<'_>) {}

/// 用法与 eprintln! 完全一致：log!("... {} ...", x)
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        $crate::logger::log(format_args!($($arg)*))
    };
}
