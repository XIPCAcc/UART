use std::ffi::CString;
use std::io;
use std::os::unix::io::RawFd;

use crate::sys;

pub fn open(path: &str, baud_rate: u32) -> io::Result<RawFd> {
    let c_path = CString::new(path).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let fd = unsafe { sys::open(c_path.as_ptr(), sys::O_RDWR | sys::O_NOCTTY, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    sys::configure_serial(fd, baud_rate)?;
    Ok(fd)
}