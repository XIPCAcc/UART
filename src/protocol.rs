use crate::error::FrameError;

pub const HEAD_BYTE: u8 = 0xAA;
pub const MAX_PAYLOAD_LEN: usize = 16384;

const CRC8_TABLE: [u8; 256] = generate_crc8_table();

const fn generate_crc8_table() -> [u8; 256] {
    let mut table = [0u8; 256];
    let mut i = 0u16;
    while i < 256 {
        let mut crc = i as u8;
        let mut j = 0;
        while j < 8 {
            if crc & 0x01 != 0 {
                crc = (crc >> 1) ^ 0x8C;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
}

pub fn crc8(data: &[u8]) -> u8 {
    let mut crc = 0u8;
    for &byte in data {
        crc = CRC8_TABLE[(crc ^ byte) as usize];
    }
    crc
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatrixDims {
    pub rows: u8,
    pub cols: u8,
}

#[derive(Debug, Clone)]
pub enum Frame {
    Request {
        seq: u8,
        dims_a: MatrixDims,
        dims_b: MatrixDims,
        data: Vec<f32>,
    },
    Result {
        seq: u8,
        dims: MatrixDims,
        data: Vec<f32>,
    },
    Error {
        code: u8,
    },
}

impl Frame {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Frame::Request { seq, dims_a, dims_b, data } => {
                let mut payload = Vec::with_capacity(5 + data.len() * 4);
                payload.push(*seq);
                payload.push(dims_a.rows);
                payload.push(dims_a.cols);
                payload.push(dims_b.rows);
                payload.push(dims_b.cols);
                for &v in data {
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                encode_frame(payload)
            }
            Frame::Result { seq, dims, data } => {
                let mut payload = Vec::with_capacity(5 + data.len() * 4);
                payload.push(*seq);
                payload.push(dims.rows);
                payload.push(dims.cols);
                payload.push(0x00);
                payload.push(0x00);
                for &v in data {
                    payload.extend_from_slice(&v.to_le_bytes());
                }
                encode_frame(payload)
            }
            Frame::Error { code } => {
                let payload = vec![*code];
                encode_frame(payload)
            }
        }
    }

    pub fn from_payload(payload: &[u8]) -> Result<Self, FrameError> {
        if payload.is_empty() {
            return Err(FrameError::InvalidFrame);
        }

        // Error frame: LEN == 1
        if payload.len() == 1 {
            return Ok(Frame::Error { code: payload[0] });
        }

        if payload.len() < 5 {
            return Err(FrameError::InvalidFrame);
        }

        let seq = payload[0];
        let dims_a = MatrixDims {
            rows: payload[1],
            cols: payload[2],
        };
        let dims_b = MatrixDims {
            rows: payload[3],
            cols: payload[4],
        };

        // Result frame: dims_b is all zeros
        if dims_b.rows == 0 && dims_b.cols == 0 {
            let dims = MatrixDims {
                rows: dims_a.rows,
                cols: dims_a.cols,
            };
            let expected = dims.rows as usize * dims.cols as usize;
            let data_bytes = &payload[5..];
            if data_bytes.len() != expected * 4 {
                return Err(FrameError::InvalidFrame);
            }
            let data = parse_f32_slice(data_bytes, expected)?;
            return Ok(Frame::Result { seq, dims, data });
        }

        // Request frame
        let a_count = dims_a.rows as usize * dims_a.cols as usize;
        let b_count = dims_b.rows as usize * dims_b.cols as usize;
        let expected_bytes = (a_count + b_count) * 4;
        let data_bytes = &payload[5..];
        if data_bytes.len() != expected_bytes {
            return Err(FrameError::InvalidFrame);
        }
        let data = parse_f32_slice(data_bytes, a_count + b_count)?;
        Ok(Frame::Request { seq, dims_a, dims_b, data })
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Frame::Request { .. } => "Request",
            Frame::Result { .. } => "Result",
            Frame::Error { .. } => "Error",
        }
    }
}

fn encode_frame(payload: Vec<u8>) -> Vec<u8> {
    let len = payload.len() as u16;
    let mut out = Vec::with_capacity(1 + 2 + payload.len() + 1);
    out.push(HEAD_BYTE);
    out.extend_from_slice(&len.to_le_bytes());

    // CRC covers LEN bytes + payload
    let mut crc_data = len.to_le_bytes().to_vec();
    crc_data.extend_from_slice(&payload);
    let crc = crc8(&crc_data);

    out.extend_from_slice(&payload);
    out.push(crc);
    out
}

fn parse_f32_slice(data: &[u8], count: usize) -> Result<Vec<f32>, FrameError> {
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let offset = i * 4;
        if offset + 4 > data.len() {
            return Err(FrameError::InvalidFrame);
        }
        let bytes: [u8; 4] = data[offset..offset + 4].try_into().map_err(|_| FrameError::InvalidFrame)?;
        result.push(f32::from_le_bytes(bytes));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc8_known() {
        // CRC-8/MAXIM for "123456789" should be 0xA1
        assert_eq!(crc8(b"123456789"), 0xA1);
    }

    #[test]
    fn test_request_frame_roundtrip() {
        let frame = Frame::Request {
            seq: 3,
            dims_a: MatrixDims { rows: 2, cols: 3 },
            dims_b: MatrixDims { rows: 3, cols: 2 },
            data: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
        };
        let bytes = frame.to_bytes();
        // Verify HEAD
        assert_eq!(bytes[0], HEAD_BYTE);
        // Parse back
        let len = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;
        let payload = &bytes[3..3 + len];
        let parsed = Frame::from_payload(payload).unwrap();
        match parsed {
            Frame::Request { seq, dims_a, dims_b, data } => {
                assert_eq!(seq, 3);
                assert_eq!(dims_a.rows, 2);
                assert_eq!(dims_a.cols, 3);
                assert_eq!(dims_b.rows, 3);
                assert_eq!(dims_b.cols, 2);
                assert_eq!(data.len(), 12);
                assert!((data[0] - 1.0).abs() < f32::EPSILON);
            }
            _ => panic!("Expected Request frame"),
        }
    }

    #[test]
    fn test_result_frame_roundtrip() {
        let frame = Frame::Result {
            seq: 7,
            dims: MatrixDims { rows: 2, cols: 2 },
            data: vec![58.0, 64.0, 139.0, 154.0],
        };
        let bytes = frame.to_bytes();
        let len = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;
        let payload = &bytes[3..3 + len];
        let parsed = Frame::from_payload(payload).unwrap();
        match parsed {
            Frame::Result { seq, dims, data } => {
                assert_eq!(seq, 7);
                assert_eq!(dims.rows, 2);
                assert_eq!(dims.cols, 2);
                assert_eq!(data.len(), 4);
            }
            _ => panic!("Expected Result frame"),
        }
    }

    #[test]
    fn test_error_frame_roundtrip() {
        let frame = Frame::Error { code: 0x01 };
        let bytes = frame.to_bytes();
        let len = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;
        let payload = &bytes[3..3 + len];
        let parsed = Frame::from_payload(payload).unwrap();
        match parsed {
            Frame::Error { code } => assert_eq!(code, 0x01),
            _ => panic!("Expected Error frame"),
        }
    }
}
