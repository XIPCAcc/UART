use std::collections::VecDeque;

use crate::error::FrameError;
use crate::protocol::{self, Frame, HEAD_BYTE, MAX_PAYLOAD_LEN};

enum ReadState {
    Syncing,
    ReadingLen,
    /// Waiting for payload (len bytes) + CRC (1 byte) in buffer
    ReadingData { len: u16 },
}

pub struct FrameReader {
    buffer: VecDeque<u8>,
    state: ReadState,
}

impl FrameReader {
    pub fn new() -> Self {
        Self {
            buffer: VecDeque::with_capacity(4096),
            state: ReadState::Syncing,
        }
    }

    /// 向内部缓冲区加入原始字节（由外部异步 I/O 提供）
    pub fn feed_data(&mut self, data: &[u8]) {
        if self.buffer.len() > MAX_PAYLOAD_LEN * 2 {
            self.buffer.clear();
            self.state = ReadState::Syncing;
        }
        let prev = self.buffer.len();
        self.buffer.extend(data);
        let state = self.state_name();
        let mut hex = String::with_capacity(data.len() * 3);
        for &b in data {
            hex.push_str(&format!("{b:02x} "));
        }
        hex.pop();
        eprintln!(
            "[FRAME] feed {}B [{hex}], buf {}→{}B, state={state}",
            data.len(),
            prev,
            self.buffer.len(),
        );
    }

    /// Try to advance the state machine using buffered data.
    /// Returns Some(result) when a complete frame is parsed or an error occurs,
    /// None when more data is needed.
    pub fn try_advance(&mut self) -> Option<Result<Frame, FrameError>> {
        let result = match &mut self.state {
            ReadState::Syncing => {
                while let Some(&b) = self.buffer.front() {
                    if b == HEAD_BYTE {
                        break;
                    }
                    self.buffer.pop_front();
                }
                if self.buffer.is_empty() {
                    eprintln!("[FRAME] try_advance → None (syncing, no {:#04x})", HEAD_BYTE);
                    return None;
                }
                self.buffer.pop_front();
                self.state = ReadState::ReadingLen;
                eprintln!("[FRAME] synced: found HEAD_BYTE → ReadingLen");
                self.try_advance()
            }
            ReadState::ReadingLen => {
                let buf_len = self.buffer.len();
                if buf_len < 2 {
                    eprintln!("[FRAME] try_advance → None (need 2B len, have {buf_len})");
                    return None;
                }
                let lo = self.buffer.pop_front().unwrap();
                let hi = self.buffer.pop_front().unwrap();
                let len = u16::from_le_bytes([lo, hi]);

                if len == 0 || len as usize > MAX_PAYLOAD_LEN {
                    eprintln!("[FRAME] bad len={len} → back to Syncing");
                    self.state = ReadState::Syncing;
                    return Some(Err(FrameError::Oversize(len as usize)));
                }
                eprintln!("[FRAME] len={len} → ReadingData");
                self.state = ReadState::ReadingData { len };
                self.try_advance()
            }
            ReadState::ReadingData { len } => {
                let total_needed = *len as usize + 1;
                if self.buffer.len() < total_needed {
                    eprintln!(
                        "[FRAME] try_advance → None (need {}B data+crc, have {})",
                        total_needed,
                        self.buffer.len(),
                    );
                    return None;
                }

                let payload: Vec<u8> = self.buffer.drain(..*len as usize).collect();
                let crc_byte = self.buffer.pop_front().unwrap();

                let mut crc_data = len.to_le_bytes().to_vec();
                crc_data.extend_from_slice(&payload);
                let expected_crc = protocol::crc8(&crc_data);

                self.state = ReadState::Syncing;

                if expected_crc != crc_byte {
                    eprintln!(
                        "[FRAME] CRC mismatch: expected=0x{expected_crc:02x} actual=0x{crc_byte:02x}"
                    );
                    return Some(Err(FrameError::CrcMismatch {
                        expected: expected_crc,
                        actual: crc_byte,
                    }));
                }

                match Frame::from_payload(&payload) {
                    Ok(frame) => {
                        eprintln!("[FRAME] ✓ parsed {} ({}B payload)", frame.kind_name(), payload.len());
                        Some(Ok(frame))
                    }
                    Err(e) => {
                        eprintln!("[FRAME] parse error: {e}");
                        Some(Err(e))
                    }
                }
            }
        };
        result
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            ReadState::Syncing => "Syncing",
            ReadState::ReadingLen => "ReadingLen",
            ReadState::ReadingData { .. } => "ReadingData",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::MatrixDims;

    #[test]
    fn test_error_frame_from_buffer() {
        let frame = Frame::Error { code: 0x01 };
        let bytes = frame.to_bytes();
        let mut input = vec![0x00, 0xFF, 0x55];
        input.extend_from_slice(&bytes);

        let mut reader = FrameReader::new();
        reader.buffer.extend(&input);

        let result = reader.try_advance();
        assert!(result.is_some());
        let frame_result = result.unwrap();
        assert!(frame_result.is_ok());
        match frame_result.unwrap() {
            Frame::Error { code } => assert_eq!(code, 0x01),
            _ => panic!("Expected Error frame"),
        }
    }

    #[test]
    fn test_request_frame_from_buffer() {
        let frame = Frame::Request {
            seq: 0,
            dims_a: MatrixDims { rows: 2, cols: 2 },
            dims_b: MatrixDims { rows: 2, cols: 2 },
            data: vec![1.0, 0.0, 0.0, 1.0, 2.0, 0.0, 0.0, 2.0],
        };
        let bytes = frame.to_bytes();

        let mut reader = FrameReader::new();
        reader.buffer.extend(&bytes);

        let result = reader.try_advance();
        assert!(result.is_some());
        let frame_result = result.unwrap();
        assert!(frame_result.is_ok());
        match frame_result.unwrap() {
            Frame::Request { dims_a, dims_b, .. } => {
                assert_eq!(dims_a.rows, 2);
                assert_eq!(dims_b.rows, 2);
            }
            _ => panic!("Expected Request frame"),
        }
    }

    #[test]
    fn test_incomplete_frame_returns_none() {
        let frame = Frame::Error { code: 0x01 };
        let bytes = frame.to_bytes();

        let mut reader = FrameReader::new();
        reader.buffer.extend(&bytes[..2]);

        let result = reader.try_advance();
        assert!(result.is_none());
    }

    #[test]
    fn test_crc_mismatch_detected() {
        let frame = Frame::Error { code: 0x01 };
        let mut bytes = frame.to_bytes();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;

        let mut reader = FrameReader::new();
        reader.buffer.extend(&bytes);

        let result = reader.try_advance();
        assert!(result.is_some());
        match result.unwrap() {
            Err(FrameError::CrcMismatch { .. }) => {}
            other => panic!("Expected CrcMismatch, got {:?}", other),
        }
    }
}