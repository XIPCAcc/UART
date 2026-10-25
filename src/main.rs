mod cli;
mod crawler;
mod error;
mod executor;
mod logger;
mod reactor;
mod signal;
mod sys;
mod tcp;

use crate::error::AppError;

fn main() -> Result<(), AppError> {
    // 全局日志时间戳起点：进程一启动就固定
    logger::init();

    // 自定义 panic hook：动态 worker 是裸 clone3 线程，默认 hook 的
    // eprintln! 会经 thread::current() 触碰不存在的 pthread 描述符字段。
    // 这里只走栈缓冲 + 直接 write(2)，任意线程上下文安全。
    std::panic::set_hook(Box::new(|info| {
        let payload = info.payload_as_str().unwrap_or("Box<dyn Any>");
        match info.location() {
            Some(loc) => logger::write_stderr(format_args!(
                "thread panicked at {}:{}:{}: {}",
                loc.file(),
                loc.line(),
                loc.column(),
                payload
            )),
            None => logger::write_stderr(format_args!("thread panicked: {payload}")),
        }
    }));

    // config 与 executor 都泄漏为 'static：多核 worker 线程可能随时访问它们，
    // 进程退出前不回收，避免生命周期问题
    let config: &'static cli::Config = Box::leak(Box::new(cli::parse_args()));
    signal::register_signals()?;

    let ex: &'static executor::Executor =
        Box::leak(Box::new(executor::Executor::new().map_err(AppError::Io)?));

    log!("Started");
    log!("[INFO] Crawler mode, max workers: {}", ex.max_workers());
    ex.block_on(crawler::run(config));

    log!("[INFO] Shutting down");
    Ok(())
}
