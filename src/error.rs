use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum FrameError {
    Io(std::io::Error),
    Timeout,
    CrcMismatch { expected: u8, actual: u8 },
    InvalidFrame,
    Oversize(usize),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Io(e) => write!(f, "I/O error: {e}"),
            FrameError::Timeout => write!(f, "read timeout, incomplete frame"),
            FrameError::CrcMismatch { expected, actual } => {
                write!(f, "CRC8 mismatch: expected {expected:#04x}, got {actual:#04x}")
            }
            FrameError::InvalidFrame => write!(f, "invalid frame format"),
            FrameError::Oversize(n) => write!(f, "frame too large: {n} bytes exceeds MAX_PAYLOAD_LEN"),
        }
    }
}

impl Error for FrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            FrameError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for FrameError {
    fn from(e: std::io::Error) -> Self {
        FrameError::Io(e)
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum ComputeError {
    DimensionMismatch = 0x01,
    CrcError = 0x02,
    InvalidFrame = 0x03,
    DataOverflow = 0x04,
}

impl fmt::Display for ComputeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComputeError::DimensionMismatch => write!(f, "dimension mismatch: COLS_A != ROWS_B"),
            ComputeError::CrcError => write!(f, "CRC verification failed"),
            ComputeError::InvalidFrame => write!(f, "malformed frame"),
            ComputeError::DataOverflow => write!(f, "data overflow"),
        }
    }
}

impl Error for ComputeError {}

#[derive(Debug)]
pub enum AppError {
    Serial(std::io::Error),
    Signal(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Serial(e) => write!(f, "serial port error: {e}"),
            AppError::Signal(s) => write!(f, "signal registration failed: {s}"),
        }
    }
}

impl Error for AppError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            AppError::Serial(e) => Some(e),
            _ => None,
        }
    }
}