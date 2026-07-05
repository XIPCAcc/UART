use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

mod compute;
mod error;
mod frame_reader;
mod protocol;
mod serial_io;
mod sys;

fn main() -> Result<(), error::AppError> {
    let (port, baud) = parse_args();

    let term = Arc::new(AtomicBool::new(false));
    register_signals(&term)?;

    eprintln!("[INFO] Opening {port} @ {baud} baud");

    let fd = serial_io::open(&port, baud).map_err(error::AppError::Serial)?;
    let mut reader = frame_reader::FrameReader::new();

    eprintln!("[INFO] Ready, entering main loop");

    let result = run_main_loop(fd, &mut reader, &term);

    unsafe { sys::close(fd) };
    eprintln!("[INFO] Shutting down");
    result
}

fn run_main_loop(
    fd: RawFd,
    reader: &mut frame_reader::FrameReader,
    term: &AtomicBool,
) -> Result<(), error::AppError> {
    while !term.load(Ordering::Relaxed) {
        match reader.read_frame(fd) {
            Ok(Some(frame)) => {
                eprintln!("[INFO] Received frame: {}", frame_summary(&frame));
                let response = match compute::process_frame(&frame) {
                    Ok(result) => protocol::Frame::Result {
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
                if let Err(e) = serial_io::write_frame(fd, &response) {
                    eprintln!("[ERROR] Write error: {e}");
                }
            }
            Ok(None) => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error::FrameError::CrcMismatch { expected, actual }) => {
                eprintln!("[WARN] CRC mismatch: expected {expected:#04x}, got {actual:#04x}");
                let err = protocol::Frame::Error {
                    code: error::ComputeError::CrcError as u8,
                };
                let _ = serial_io::write_frame(fd, &err);
            }
            Err(e) => {
                eprintln!("[WARN] Frame read error: {e}");
            }
        }
    }
    Ok(())
}

fn frame_summary(frame: &protocol::Frame) -> String {
    match frame {
        protocol::Frame::Request { dims_a, dims_b, data } => {
            format!(
                "Request A={}x{} B={}x{} ({} floats)",
                dims_a.rows,
                dims_a.cols,
                dims_b.rows,
                dims_b.cols,
                data.len()
            )
        }
        protocol::Frame::Result { dims, data } => {
            format!("Result {}x{} ({} floats)", dims.rows, dims.cols, data.len())
        }
        protocol::Frame::Error { code } => {
            format!("Error code {code:#04x}")
        }
    }
}

// ── CLI parsing ─────────────────────────────────────────────

fn parse_args() -> (String, u32) {
    let args: Vec<String> = std::env::args().collect();
    let mut port = String::from("/dev/ttyS0");
    let mut baud = 115200u32;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                i += 1;
                if i < args.len() {
                    port = args[i].clone();
                }
            }
            "--baud" | "-b" => {
                i += 1;
                if i < args.len() {
                    baud = args[i].parse().unwrap_or(115200);
                }
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    (port, baud)
}

fn print_help() {
    println!(
        "uart-matmul - Serial port matrix multiplication daemon\n\
         \n\
         USAGE:\n    uart-matmul [OPTIONS]\n\
         \n\
         OPTIONS:\n    \
         -p, --port <PORT>        Serial port device [default: /dev/ttyS0]\n    \
         -b, --baud <RATE>        Baud rate [default: 115200]\n    \
         -h, --help               Print help"
    );
}

// ── signal handling ─────────────────────────────────────────

extern "C" fn handle_signal(_: std::ffi::c_int) {
    // We need to set a global, but we can't access the Arc from here.
    // Use a static AtomicBool.
    SIGNAL_FLAG.store(true, Ordering::Relaxed);
}

static SIGNAL_FLAG: AtomicBool = AtomicBool::new(false);

fn register_signals(term: &Arc<AtomicBool>) -> Result<(), error::AppError> {
    let prev_int = unsafe { sys::signal(sys::SIGINT, handle_signal as usize) };
    if prev_int == usize::MAX {
        return Err(error::AppError::Signal("SIGINT".into()));
    }
    let prev_term = unsafe { sys::signal(sys::SIGTERM, handle_signal as usize) };
    if prev_term == usize::MAX {
        return Err(error::AppError::Signal("SIGTERM".into()));
    }

    // Spawn a thread to poll the signal flag and propagate to the Arc
    let term_clone = Arc::clone(term);
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_millis(100));
            if SIGNAL_FLAG.load(Ordering::Relaxed) {
                term_clone.store(true, Ordering::Relaxed);
                break;
            }
        }
    });

    Ok(())
}