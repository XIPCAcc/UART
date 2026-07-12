use std::io;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

mod compute;
mod error;
mod frame_reader;
mod protocol;
mod serial_io;
mod sys;

struct Config {
    port: String,
    baud: u32,
    mode: RunMode,
}

enum RunMode {
    Receiver,
    Sender { rows_a: u8, cols_a: u8, cols_b: u8, count: u32 },
}

fn main() -> Result<(), error::AppError> {
    let config = parse_args();

    let term = Arc::new(AtomicBool::new(false));
    register_signals(&term)?;

    eprintln!("[INFO] Opening {} @ {} baud", config.port, config.baud);

    let fd = serial_io::open(&config.port, config.baud).map_err(error::AppError::Serial)?;

    let result = match config.mode {
        RunMode::Receiver => {
            let mut reader = frame_reader::FrameReader::new();
            eprintln!("[INFO] Receiver mode, waiting for frames");
            run_receiver(fd, &mut reader, &term)
        }
        RunMode::Sender { rows_a, cols_a, cols_b, count } => {
            eprintln!("[INFO] Sender mode: A={rows_a}x{cols_a} B={cols_a}x{cols_b} count={count}");
            run_sender(fd, rows_a, cols_a, cols_b, count, &term)
        }
    };

    unsafe { sys::close(fd) };
    eprintln!("[INFO] Shutting down");
    result
}

// ── Receiver ──────────────────────────────────────────────────

fn run_receiver(
    fd: RawFd,
    reader: &mut frame_reader::FrameReader,
    term: &AtomicBool,
) -> Result<(), error::AppError> {
    let epfd = sys::epoll_create().map_err(error::AppError::Serial)?;
    sys::epoll_add(epfd, fd).map_err(error::AppError::Serial)?;

    let mut events = [unsafe { sys::EpollEvent::zeroed() }; 1];

    while !term.load(Ordering::Relaxed) {
        match sys::epoll_wait(epfd, &mut events, 100) {
            Ok(0) => continue,
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                unsafe { sys::close(epfd) };
                return Err(error::AppError::Serial(e));
            }
        }

        loop {
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
                Ok(None) => break,
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
    }

    unsafe { sys::close(epfd) };
    Ok(())
}

// ── Sender ────────────────────────────────────────────────────

fn run_sender(
    fd: RawFd,
    rows_a: u8,
    cols_a: u8,
    cols_b: u8,
    count: u32,
    term: &AtomicBool,
) -> Result<(), error::AppError> {
    let mut reader = frame_reader::FrameReader::new();

    let epfd = sys::epoll_create().map_err(error::AppError::Serial)?;
    sys::epoll_add(epfd, fd).map_err(error::AppError::Serial)?;
    let mut events = [unsafe { sys::EpollEvent::zeroed() }; 1];

    for i in 0..count {
        if term.load(Ordering::Relaxed) {
            eprintln!("[INFO] Interrupted, stopping sender");
            break;
        }
        let frame = build_request(rows_a, cols_a, cols_b, i);
        eprintln!(
            "[INFO] Sending request {}/{}: A={}x{} B={}x{}",
            i + 1, count, rows_a, cols_a, cols_a, cols_b
        );
        serial_io::write_frame(fd, &frame).map_err(error::AppError::Serial)?;

        let response = recv_response(fd, &mut reader, epfd, &mut events, term)?;
        print_response(&response);
    }

    unsafe { sys::close(epfd) };
    Ok(())
}

fn recv_response(
    fd: RawFd,
    reader: &mut frame_reader::FrameReader,
    epfd: RawFd,
    events: &mut [sys::EpollEvent; 1],
    term: &AtomicBool,
) -> Result<protocol::Frame, error::AppError> {
    loop {
        if term.load(Ordering::Relaxed) {
            return Err(error::AppError::Signal("interrupted".into()));
        }
        match sys::epoll_wait(epfd, events, 100) {
            Ok(0) => continue,
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(error::AppError::Serial(e)),
        }

        match reader.read_frame(fd) {
            Ok(Some(frame)) => return Ok(frame),
            Ok(None) => continue,
            Err(e) => {
                eprintln!("[WARN] Read error: {e}");
                continue;
            }
        }
    }
}

fn build_request(rows_a: u8, cols_a: u8, cols_b: u8, seed: u32) -> protocol::Frame {
    let a_count = rows_a as usize * cols_a as usize;
    let b_count = cols_a as usize * cols_b as usize;
    let mut data = Vec::with_capacity(a_count + b_count);

    let mut rng = Lcg::new(seed.wrapping_add(0xDEADBEEF));
    for _ in 0..a_count {
        data.push(rng.next_f32());
    }
    for _ in 0..b_count {
        data.push(rng.next_f32());
    }

    protocol::Frame::Request {
        dims_a: protocol::MatrixDims { rows: rows_a, cols: cols_a },
        dims_b: protocol::MatrixDims { rows: cols_a, cols: cols_b },
        data,
    }
}

fn print_response(frame: &protocol::Frame) {
    match frame {
        protocol::Frame::Result { dims, data } => {
            println!("Result {}x{}:", dims.rows, dims.cols);
            for i in 0..dims.rows as usize {
                for j in 0..dims.cols as usize {
                    print!("{:>10.4} ", data[i * dims.cols as usize + j]);
                }
                println!();
            }
        }
        protocol::Frame::Error { code } => {
            eprintln!("Error: code {code:#04x}");
        }
        _ => {
            eprintln!("Unexpected frame type received");
        }
    }
}

/// Simple LCG pseudo-random generator (no rand crate needed)
struct Lcg {
    state: u32,
}

impl Lcg {
    fn new(seed: u32) -> Self {
        Self { state: seed }
    }

    fn next_u32(&mut self) -> u32 {
        self.state = self.state.wrapping_mul(1664525).wrapping_add(1013904223);
        self.state
    }

    fn next_f32(&mut self) -> f32 {
        (self.next_u32() & 0x007FFFFF) as f32 / 8388608.0
    }
}

// ── Utilities ─────────────────────────────────────────────────

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

// ── CLI parsing ───────────────────────────────────────────────

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut port = String::from("/dev/ttyS0");
    let mut baud = 115200u32;
    let mut mode = "receiver".to_string();
    let mut rows_a = 2u8;
    let mut cols_a = 3u8;
    let mut cols_b = 2u8;
    let mut count = 1u32;

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
            "--mode" | "-m" => {
                i += 1;
                if i < args.len() {
                    mode = args[i].clone();
                }
            }
            "--rows-a" => {
                i += 1;
                if i < args.len() {
                    rows_a = args[i].parse().unwrap_or(2);
                }
            }
            "--cols-a" => {
                i += 1;
                if i < args.len() {
                    cols_a = args[i].parse().unwrap_or(3);
                }
            }
            "--cols-b" => {
                i += 1;
                if i < args.len() {
                    cols_b = args[i].parse().unwrap_or(2);
                }
            }
            "--count" | "-c" => {
                i += 1;
                if i < args.len() {
                    count = args[i].parse().unwrap_or(1);
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

    let run_mode = match mode.as_str() {
        "sender" => RunMode::Sender { rows_a, cols_a, cols_b, count },
        _ => RunMode::Receiver,
    };

    Config { port, baud, mode: run_mode }
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
         -m, --mode <MODE>        Mode: receiver (default) or sender\n\
         \n\
         SENDER OPTIONS:\n    \
         --rows-a <N>             Rows of matrix A [default: 2]\n    \
         --cols-a <N>             Columns of A / rows of B [default: 3]\n    \
         --cols-b <N>             Columns of matrix B [default: 2]\n    \
         -c, --count <N>          Number of requests to send [default: 1]\n    \
         -h, --help               Print help\n\
         \n\
         EXAMPLES:\n    \
         # Receiver on ttyS0\n    \
         uart-matmul -p /dev/ttyS0\n\
         \n    \
         # Sender on ttyUSB0, 2x3 * 3x2, 5 requests\n    \
         uart-matmul -p /dev/ttyUSB0 --mode sender --rows-a 2 --cols-a 3 --cols-b 2 -c 5"
    );
}

// ── signal handling ───────────────────────────────────────────

extern "C" fn handle_signal(_: std::ffi::c_int) {
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