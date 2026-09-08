use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;

use crate::async_serial::{self, AsyncSerial};
use crate::cli::Config;
use crate::compute;
use crate::error;
use crate::executor::executor;
use crate::frame_reader;
use crate::protocol;
use crate::signal;

pub async fn run(config: Config) {
    let mut serial = match AsyncSerial::open(&config.port, config.baud) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[ERROR] Failed to open serial: {e}");
            return;
        }
    };

    let fd = serial.fd;
    let mut reader = frame_reader::FrameReader::new();
    let mut buf = [0u8; 512];

    while !signal::TERM.load(Ordering::Relaxed) {
        let n = match serial.read(&mut buf).await {
            Ok(0) => continue,
            Ok(n) => n,
            Err(e) => {
                eprintln!("[ERROR] Read error: {e}");
                break;
            }
        };

        reader.feed_data(&buf[..n]);

        loop {
            match reader.try_advance() {
                Some(Ok(frame)) => {
                    eprintln!("[INFO] Received frame: {}", frame_summary(&frame));
                    eprintln!("[DEBUG] reader: spawning compute_one_frame task");
                    executor().spawn(compute_one_frame(fd, frame));
                }
                Some(Err(error::FrameError::CrcMismatch { expected, actual })) => {
                    eprintln!(
                        "[WARN] CRC mismatch: expected {expected:#04x}, got {actual:#04x}"
                    );
                    executor().spawn(write_error_frame(fd, error::ComputeError::CrcError as u8));
                }
                Some(Err(e)) => {
                    eprintln!("[WARN] Frame read error: {e}");
                }
                None => break,
            }
        }
    }
}

async fn compute_one_frame(fd: RawFd, frame: protocol::Frame) {
    let seq = match &frame {
        protocol::Frame::Request { seq, .. } => *seq,
        _ => 0,
    };
    let response = match compute::process_frame(&frame) {
        Ok(result) => protocol::Frame::Result {
            seq,
            dims: protocol::MatrixDims {
                rows: result.rows,
                cols: result.cols,
            },
            data: result.data,
        },
        Err(e) => {
            eprintln!("[WARN] Compute error: {e}");
            protocol::Frame::Error { code: e as u8 }
        }
    };
    let bytes = response.to_bytes();
    if let Err(e) = async_serial::write_all_fd(fd, &bytes).await {
        eprintln!("[ERROR] Write error: {e}");
    }
}

async fn write_error_frame(fd: RawFd, code: u8) {
    let frame = protocol::Frame::Error { code };
    let bytes = frame.to_bytes();
    if let Err(e) = async_serial::write_all_fd(fd, &bytes).await {
        eprintln!("[ERROR] Write error: {e}");
    }
}

fn frame_summary(frame: &protocol::Frame) -> String {
    match frame {
        protocol::Frame::Request { seq, dims_a, dims_b, data } => {
            format!(
                "Request seq={} A={}x{} B={}x{} ({} floats)",
                seq, dims_a.rows, dims_a.cols, dims_b.rows, dims_b.cols, data.len()
            )
        }
        protocol::Frame::Result { seq, dims, data } => {
            format!("Result seq={} {}x{} ({} floats)", seq, dims.rows, dims.cols, data.len())
        }
        protocol::Frame::Error { code } => {
            format!("Error code {code:#04x}")
        }
    }
}