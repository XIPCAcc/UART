mod async_serial;
mod cli;
mod compute;
mod crawler;
mod error;
mod executor;
mod frame_reader;
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
    // config 与 executor 都泄漏为 'static：多核 worker 线程可能随时访问它们，
    // 进程退出前不回收，避免生命周期问题
    let config: &'static cli::Config = Box::leak(Box::new(cli::parse_args()));
    signal::register_signals()?;

    let ex: &'static executor::Executor =
        Box::leak(Box::new(executor::Executor::new().map_err(AppError::Serial)?));

    match &config.mode {
        cli::RunMode::Receiver => {
            eprintln!("[INFO] Opening {} @ {} baud", config.port, config.baud);
            eprintln!("[INFO] Receiver mode, waiting for frames");
            ex.block_on(receiver::run(config));
        }
        cli::RunMode::Sender { rows_a, cols_a, cols_b, count } => {
            eprintln!("[INFO] Opening {} @ {} baud", config.port, config.baud);
            eprintln!("[INFO] Sender mode: A={rows_a}x{cols_a} B={cols_a}x{cols_b} count={count}");
            ex.block_on(sender::run(config, *rows_a, *cols_a, *cols_b, *count));
        }
        cli::RunMode::Crawler { .. } => {
            eprintln!("[INFO] Crawler mode, max workers: {}", ex.max_workers());
            ex.block_on(crawler::run(config));
        }
    }

    eprintln!("[INFO] Shutting down");
    Ok(())
}
