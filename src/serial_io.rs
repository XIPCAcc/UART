use std::io::Write;
use std::time::Duration;

use serialport::{SerialPort, SerialPortBuilder};

use crate::protocol::Frame;

pub fn open(
    path: &str,
    baud_rate: u32,
    timeout_ms: u64,
) -> Result<Box<dyn SerialPort>, serialport::Error> {
    let builder: SerialPortBuilder = serialport::new(path, baud_rate)
        .timeout(Duration::from_millis(timeout_ms))
        .data_bits(serialport::DataBits::Eight)
        .parity(serialport::Parity::None)
        .stop_bits(serialport::StopBits::One)
        .flow_control(serialport::FlowControl::None);
    builder.open()
}

pub fn write_frame(
    port: &mut Box<dyn SerialPort>,
    frame: &Frame,
) -> Result<(), std::io::Error> {
    let bytes = frame.to_bytes();
    port.write_all(&bytes)?;
    port.flush()?;
    Ok(())
}
