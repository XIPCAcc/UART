use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::Parser;
use log::{error, info, warn};

mod compute;
mod error;
mod frame_reader;
mod protocol;
mod serial_io;

#[derive(Parser, Debug)]
#[command(name = "uart-matmul", about = "Serial port matrix multiplication daemon")]
struct Cli {
    /// Serial port device path
    #[arg(short, long, default_value = "/dev/ttyS0")]
    port: String,

    /// Baud rate
    #[arg(short, long, default_value_t = 115200)]
    baud: u32,

    /// Read timeout in milliseconds
    #[arg(short, long, default_value_t = 100)]
    timeout: u64,
}

fn main() -> Result<(), error::AppError> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let cli = Cli::parse();

    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term))
        .map_err(|e| error::AppError::Signal(e.to_string()))?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term))
        .map_err(|e| error::AppError::Signal(e.to_string()))?;

    info!("Opening {} @ {} baud, timeout {}ms", cli.port, cli.baud, cli.timeout);

    let mut port = serial_io::open(&cli.port, cli.baud, cli.timeout)?;
    let mut reader = frame_reader::FrameReader::new();

    info!("Ready, entering main loop");

    while !term.load(Ordering::Relaxed) {
        match reader.read_frame(port.as_mut()) {
            Ok(frame) => {
                info!("Received frame: {}", frame_summary(&frame));
                let response = match compute::process_frame(&frame) {
                    Ok(result) => protocol::Frame::Result {
                        dims: protocol::MatrixDims {
                            rows: result.rows,
                            cols: result.cols,
                        },
                        data: result.data,
                    },
                    Err(e) => {
                        warn!("Compute error: {}", e);
                        protocol::Frame::Error { code: e as u8 }
                    }
                };
                if let Err(e) = serial_io::write_frame(&mut port, &response) {
                    error!("Write error: {}", e);
                }
            }
            Err(error::FrameError::Timeout) => {
                // Normal timeout, continue loop
            }
            Err(error::FrameError::CrcMismatch { expected, actual }) => {
                warn!("CRC mismatch: expected {expected:#04x}, got {actual:#04x}");
                let err = protocol::Frame::Error {
                    code: error::ComputeError::CrcError as u8,
                };
                let _ = serial_io::write_frame(&mut port, &err);
            }
            Err(e) => {
                warn!("Frame read error: {}", e);
            }
        }
    }

    info!("Shutting down");
    Ok(())
}

fn frame_summary(frame: &protocol::Frame) -> String {
    match frame {
        protocol::Frame::Request { dims_a, dims_b, data } => {
            format!("Request A={}x{} B={}x{} ({} floats)", dims_a.rows, dims_a.cols, dims_b.rows, dims_b.cols, data.len())
        }
        protocol::Frame::Result { dims, data } => {
            format!("Result {}x{} ({} floats)", dims.rows, dims.cols, data.len())
        }
        protocol::Frame::Error { code } => {
            format!("Error code {code:#04x}")
        }
    }
}
