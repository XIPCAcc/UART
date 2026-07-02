use std::collections::VecDeque;

use serialport::SerialPort;

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

    pub fn read_frame(
        &mut self,
        port: &mut dyn SerialPort,
    ) -> Result<Frame, FrameError> {
        loop {
            if let Some(result) = self.try_advance() {
                return result;
            }
            self.fill_from_port(port)?;
        }
    }

    /// Try to advance the state machine using buffered data.
    /// Returns Some(result) when a complete frame is parsed or an error occurs,
    /// None when more data is needed.
    fn try_advance(&mut self) -> Option<Result<Frame, FrameError>> {
        match &mut self.state {
            ReadState::Syncing => {
                // Discard non-HEAD bytes
                while let Some(&b) = self.buffer.front() {
                    if b == HEAD_BYTE {
                        break;
                    }
                    self.buffer.pop_front();
                }
                if self.buffer.is_empty() {
                    return None;
                }
                // Found HEAD byte, consume it
                self.buffer.pop_front();
                self.state = ReadState::ReadingLen;
                self.try_advance() // tail-call: try next state immediately
            }
            ReadState::ReadingLen => {
                if self.buffer.len() < 2 {
                    return None;
                }
                let lo = self.buffer.pop_front().unwrap();
                let hi = self.buffer.pop_front().unwrap();
                let len = u16::from_le_bytes([lo, hi]);

                if len == 0 || len as usize > MAX_PAYLOAD_LEN {
                    self.state = ReadState::Syncing;
                    return Some(Err(FrameError::Oversize(len as usize)));
                }
                self.state = ReadState::ReadingData { len };
                self.try_advance()
            }
            ReadState::ReadingData { len } => {
                // Need len bytes of payload + 1 byte CRC
                let total_needed = *len as usize + 1;
                if self.buffer.len() < total_needed {
                    return None;
                }

                // Drain payload
                let payload: Vec<u8> = self.buffer.drain(..*len as usize).collect();
                let crc_byte = self.buffer.pop_front().unwrap();

                // CRC covers LEN bytes + payload
                let mut crc_data = len.to_le_bytes().to_vec();
                crc_data.extend_from_slice(&payload);
                let expected_crc = protocol::crc8(&crc_data);

                self.state = ReadState::Syncing;

                if expected_crc != crc_byte {
                    return Some(Err(FrameError::CrcMismatch {
                        expected: expected_crc,
                        actual: crc_byte,
                    }));
                }

                Some(Frame::from_payload(&payload))
            }
        }
    }

    fn fill_from_port(&mut self, port: &mut dyn SerialPort) -> Result<(), FrameError> {
        if self.buffer.len() > MAX_PAYLOAD_LEN * 2 {
            self.buffer.clear();
            self.state = ReadState::Syncing;
        }

        let mut tmp = [0u8; 512];
        match port.read(&mut tmp) {
            Ok(0) => Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "serial port returned 0 bytes",
            )
            .into()),
            Ok(n) => {
                self.buffer.extend(&tmp[..n]);
                Ok(())
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {
                Err(FrameError::Timeout)
            }
            Err(e) => Err(e.into()),
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
        // Prepend noise
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
        // Feed only first 2 bytes (HEAD + partial LEN)
        reader.buffer.extend(&bytes[..2]);

        let result = reader.try_advance();
        assert!(result.is_none());
    }

    #[test]
    fn test_crc_mismatch_detected() {
        let frame = Frame::Error { code: 0x01 };
        let mut bytes = frame.to_bytes();
        // Corrupt the CRC byte
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
