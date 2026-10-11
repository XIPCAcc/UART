mod async_serial;
mod cli;
mod compute;
mod crawler;
mod error;
mod executor;
mod frame_reader;
mod logger;
mod protocol;
mod reactor;
mod receiver;
mod rng;
mod sender;
mod serial_io;
mod signal;
mod sys;
mod tcp;

use crate::error::AppError;

fn main() -> Result<(), AppError> {
    // 全局日志时间戳起点：进程一启动就固定
    logger::init();

    // config 与 executor 都泄漏为 'static：多核 worker 线程可能随时访问它们，
    // 进程退出前不回收，避免生命周期问题
    let config: &'static cli::Config = Box::leak(Box::new(cli::parse_args()));
    signal::register_signals()?;

    let ex: &'static executor::Executor =
        Box::leak(Box::new(executor::Executor::new().map_err(AppError::Serial)?));

    log!("Started");

    match &config.mode {
        cli::RunMode::Receiver => {
            log!("[INFO] Opening {} @ {} baud", config.port, config.baud);
            log!("[INFO] Receiver mode, waiting for frames");
            ex.block_on(receiver::run(config));
        }
        cli::RunMode::Sender { rows_a, cols_a, cols_b, count } => {
            log!("[INFO] Opening {} @ {} baud", config.port, config.baud);
            log!("[INFO] Sender mode: A={rows_a}x{cols_a} B={cols_a}x{cols_b} count={count}");
            ex.block_on(sender::run(config, *rows_a, *cols_a, *cols_b, *count));
        }
        cli::RunMode::Crawler { .. } => {
            log!("[INFO] Crawler mode, max workers: {}", ex.max_workers());
            ex.block_on(crawler::run(config));
        }
    }

    log!("[INFO] Shutting down");
    Ok(())
}
