// 串口矩阵乘应用（uart-matmul）
//
// 运行时（executor/reactor/signal/sys/trace/error）来自 uintr-runtime 库 crate，
// 通过 crate 根 re-export，使本 crate 内模块继续用 crate::xxx 访问。
//
// 仅保留两种串口模式：
//   - receiver（默认）：等待并计算串口帧
//   - sender：矩阵乘请求流水线发送方

mod async_serial;
mod cli;
mod compute;
mod frame_reader;
mod protocol;
mod receiver;
mod rng;
mod sender;
mod serial_io;

// 供本 crate 子模块以 crate::xxx 路径访问运行时
pub use uintr_runtime::{error, executor, reactor, signal, sys, trace};

fn main() -> Result<(), error::AppError> {
    let config = cli::parse_args();
    signal::register_signals()?;

    let ex = executor::Executor::new().map_err(|e| error::AppError::Serial(e))?;

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
    }

    eprintln!("[INFO] Shutting down");
    Ok(())
}
