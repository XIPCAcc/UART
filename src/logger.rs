// 全局日志：所有输出统一走这里，log! 自动带上自进程启动起的毫秒时间戳。
// 起点用 OnceLock<Instant> 固定（main 最开头 init），任意线程可并发调用。
//
// 不用 std::io（eprintln! 的 stderr ReentrantLock 依赖 thread::current()
// → 惰性初始化带析构的 thread_local，而裸 clone3 worker 的 TCB 由 ld.so
// _dl_allocate_tls 分配、没有完整 pthread 描述符，注册 TLS 析构会越界写
// pthread 字段）：格式化进栈缓冲区后直接 write(2)。顺带消除 stderr 锁的
// futex 系统调用，panic hook 也复用同一路径。
//
// 编译开关：默认 `log!` 编译为空（零成本）；调试时
// cargo build --features verbose-log。`elog!` 不受开关控制始终输出。

use std::fmt;
use std::sync::OnceLock;
use std::time::Instant;

use crate::sys;

static START: OnceLock<Instant> = OnceLock::new();

/// 固定时间戳起点，必须在 main 最开头调用
pub fn init() {
    let _ = START.set(Instant::now());
}

/// 把 fmt 参数格式化进栈缓冲区，一次 write(2) 写出（超长截断）。
/// 无堆分配、无锁、无 TLS 依赖：裸 clone3 worker 与信号/panic 上下文安全。
pub fn write_stderr(args: fmt::Arguments<'_>) {
    struct Cursor<'a> {
        buf: &'a mut [u8],
        pos: usize,
    }
    impl fmt::Write for Cursor<'_> {
        fn write_str(&mut self, s: &str) -> fmt::Result {
            let n = s.len().min(self.buf.len() - self.pos);
            self.buf[self.pos..self.pos + n].copy_from_slice(&s.as_bytes()[..n]);
            self.pos += n;
            Ok(())
        }
    }
    let mut buf = [0u8; 2048];
    let pos = {
        let mut c = Cursor { buf: &mut buf, pos: 0 };
        use fmt::Write;
        let _ = c.write_fmt(args);
        let _ = c.write_str("\n");
        c.pos
    };
    let _ = sys::raw_write(2, &buf[..pos]);
}

#[cfg(feature = "verbose-log")]
pub fn log(args: fmt::Arguments<'_>) {
    let ms = match START.get() {
        Some(t) => t.elapsed().as_secs_f64() * 1000.0,
        None => 0.0,
    };
    write_stderr(format_args!("[{:>10.5}ms]  {}", ms, args));
}

#[cfg(not(feature = "verbose-log"))]
#[inline(always)]
pub fn log(_args: fmt::Arguments<'_>) {}

/// 关键输出（汇总/错误）：不受 verbose-log 开关控制，始终打印
pub fn elog(args: fmt::Arguments<'_>) {
    write_stderr(args);
}

/// 用法与 eprintln! 完全一致：log!("... {} ...", x)
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        $crate::logger::log(format_args!($($arg)*))
    };
}

/// 关键输出（汇总/错误）：不受 verbose-log 开关控制，始终打印
#[macro_export]
macro_rules! elog {
    ($($arg:tt)*) => {
        $crate::logger::elog(format_args!($($arg)*))
    };
}
