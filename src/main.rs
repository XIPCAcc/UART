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
mod trace;
mod uintr;
mod uintr_core;
mod uintr_receiver;
mod uintr_sender;

use crate::error::AppError;

fn main() -> Result<(), AppError> {
    let config = cli::parse_args();
    signal::register_signals()?;

    eprintln!("[INFO] Opening {} @ {} baud", config.port, config.baud);

    let ex = executor::Executor::new().map_err(|e| AppError::Serial(e))?;

    match &config.mode {
        cli::RunMode::Receiver => {
            eprintln!("[INFO] Receiver mode, waiting for frames");
            ex.spawn(receiver::run(config));
            ex.block_on();
        }
        cli::RunMode::Sender { rows_a, cols_a, cols_b, count } => {
            let (ra, ca, cb, cnt) = (*rows_a, *cols_a, *cols_b, *count);
            eprintln!("[INFO] Sender mode: A={ra}x{ca} B={ca}x{cb} count={cnt}");
            ex.spawn(sender::run(config, ra, ca, cb, cnt));
            ex.block_on();
        }
        cli::RunMode::UintrReceiver => {
            eprintln!("[INFO] UINTR Receiver mode, waiting for user interrupts");
            ex.spawn(uintr_receiver::run());
            ex.block_on();
        }
        cli::RunMode::UintrSender { count, interval_ms } => {
            let (cnt, interval) = (*count, *interval_ms);
            eprintln!("[INFO] UINTR Sender mode: count={cnt} interval_ms={interval}");
            ex.spawn(uintr_sender::run(cnt, interval));
            ex.block_on();
        }
    }

    eprintln!("[INFO] Shutting down");
    Ok(())
}
