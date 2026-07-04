use std::ffi::CString;
use std::io;
use std::os::unix::io::RawFd;

use crate::protocol::Frame;
use crate::sys;

pub fn open(path: &str, baud_rate: u32, timeout_ms: u64) -> io::Result<RawFd> {
    let c_path = CString::new(path).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let fd = unsafe { sys::open(c_path.as_ptr(), sys::O_RDWR | sys::O_NOCTTY, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    sys::configure_serial(fd, baud_rate, timeout_ms)?;
    Ok(fd)
}

pub fn write_frame(fd: RawFd, frame: &Frame) -> io::Result<()> {
    let bytes = frame.to_bytes();
    let mut written = 0;
    while written < bytes.len() {
        let n = sys::raw_write(fd, &bytes[written..])?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
        }
        written += n;
    }
    // flush is implicit with raw write — no userspace buffering
    Ok(())
}