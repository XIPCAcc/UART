use thiserror::Error;

#[derive(Error, Debug)]
pub enum FrameError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("read timeout, incomplete frame")]
    Timeout,

    #[error("CRC8 mismatch: expected {expected:#04x}, got {actual:#04x}")]
    CrcMismatch { expected: u8, actual: u8 },

    #[error("invalid frame format")]
    InvalidFrame,

    #[error("frame too large: {0} bytes exceeds MAX_PAYLOAD_LEN")]
    Oversize(usize),
}

#[derive(Error, Debug, Clone, Copy)]
#[repr(u8)]
pub enum ComputeError {
    #[error("dimension mismatch: COLS_A != ROWS_B")]
    DimensionMismatch = 0x01,

    #[error("CRC verification failed")]
    CrcError = 0x02,

    #[error("malformed frame")]
    InvalidFrame = 0x03,

    #[error("data overflow")]
    DataOverflow = 0x04,
}

#[derive(Error, Debug)]
pub enum AppError {
    #[error("serial port error: {0}")]
    Serial(#[from] serialport::Error),

    #[error("signal registration failed: {0}")]
    Signal(String),
}
