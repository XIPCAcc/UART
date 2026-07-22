mod async_serial;
mod cli;
mod compute;
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

use crate::error::AppError;

fn main() -> Result<(), AppError> {
    let config = cli::parse_args();
    signal::register_signals()?;

    eprintln!("[INFO] Opening {} @ {} baud", config.port, config.baud);

    let ex = executor::Executor::new().map_err(|e| AppError::Serial(e))?;

    match config.mode {
        cli::RunMode::Receiver => {
            eprintln!("[INFO] Receiver mode, waiting for frames");
            ex.block_on(receiver::run(&config));
        }
        cli::RunMode::Sender { rows_a, cols_a, cols_b, count } => {
            eprintln!("[INFO] Sender mode: A={rows_a}x{cols_a} B={cols_a}x{cols_b} count={count}");
            ex.block_on(sender::run(&config, rows_a, cols_a, cols_b, count));
        }
    }

    eprintln!("[INFO] Shutting down");
    Ok(())
}