use std::sync::atomic::Ordering;

use crate::async_serial::{self, AsyncSerial};
use crate::cli::Config;
use crate::frame_reader;
use crate::protocol;
use crate::rng::Lcg;
use crate::signal;

pub async fn run(config: Config, rows_a: u8, cols_a: u8, cols_b: u8, count: u32) {
    let mut serial = match AsyncSerial::open(&config.port, config.baud) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[ERROR] Failed to open serial: {e}");
            return;
        }
    };

    let total = count as usize;

    // ── 阶段 1：批量发送所有请求 ──
    eprintln!("[INFO] Pipelining: sending {} requests...", total);
    for i in 0..total {
        if signal::TERM.load(Ordering::Relaxed) {
            eprintln!("[INFO] Interrupted during send");
            return;
        }
        let frame = build_request(rows_a, cols_a, cols_b, i as u8, i as u32);
        let frame_bytes = frame.to_bytes();
        eprintln!(
            "[INFO] Sending request seq={}/{}: A={}x{} B={}x{} ({} bytes)",
            i, total, rows_a, cols_a, cols_a, cols_b, frame_bytes.len()
        );

        if let Err(e) = async_serial::write_frame(&mut serial, &frame).await {
            eprintln!("[ERROR] Write error: {e}");
            return;
        }
    }

    // ── 阶段 2：批量接收所有响应（按 seq 匹配） ──
    eprintln!("[INFO] Receiving {} responses...", total);
    let mut responses: Vec<Option<protocol::Frame>> = (0..total).map(|_| None).collect();
    let mut received = 0usize;

    let mut reader = frame_reader::FrameReader::new();
    let mut buf = [0u8; 512];

    while received < total {
        if signal::TERM.load(Ordering::Relaxed) {
            eprintln!("[INFO] Interrupted during receive");
            return;
        }

        let n = match serial.read(&mut buf).await {
            Ok(0) => continue,
            Ok(n) => n,
            Err(e) => {
                eprintln!("[ERROR] Read error: {e}");
                return;
            }
        };

        reader.feed_data(&buf[..n]);

        while received < total {
            match reader.try_advance() {
                Some(Ok(response)) => {
                    let seq = match &response {
                        protocol::Frame::Result { seq, .. } => *seq as usize,
                        _ => {
                            print_response(&response);
                            received += 1;
                            continue;
                        }
                    };
                    if seq < total && responses[seq].is_none() {
                        eprintln!("[INFO] Got response seq={} ({}/{})", seq, received + 1, total);
                        responses[seq] = Some(response);
                        received += 1;
                    } else {
                        eprintln!("[WARN] Duplicate or out-of-range seq={}", seq);
                    }
                }
                Some(Err(e)) => {
                    eprintln!("[WARN] Read error: {e}");
                    continue;
                }
                None => break,
            }
        }
    }

    // 按 seq 顺序打印结果
    eprintln!("[INFO] All responses received, printing in order:");
    for (seq, resp) in responses.iter().enumerate() {
        match resp {
            Some(frame) => {
                print!("[seq={}] ", seq);
                print_response(frame);
            }
            None => {
                eprintln!("[WARN] Missing response for seq={}", seq);
            }
        }
    }
}

fn build_request(rows_a: u8, cols_a: u8, cols_b: u8, seq: u8, seed: u32) -> protocol::Frame {
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
        seq,
        dims_a: protocol::MatrixDims { rows: rows_a, cols: cols_a },
        dims_b: protocol::MatrixDims { rows: cols_a, cols: cols_b },
        data,
    }
}

fn print_response(frame: &protocol::Frame) {
    match frame {
        protocol::Frame::Result { dims, data, .. } => {
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