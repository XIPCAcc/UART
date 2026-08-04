use libc::{c_int, c_long, syscall};
use std::os::unix::io::RawFd;
use super::{
    UintrError, UintrResult,
    __NR_UINTR_REGISTER_HANDLER,
    __NR_UINTR_UNREGISTER_HANDLER,
    __NR_UINTR_CREATE_FD,
    __NR_UINTR_REGISTER_SENDER,
    __NR_UINTR_UNREGISTER_SENDER,
    __NR_UINTR_WAIT,
};

#[repr(C)]
pub struct UintrFrame {
    pub rip: u64,
    pub rflags: u64,
    pub rsp: u64,
}

pub fn uintr_register_handler(
    handler: unsafe extern "C" fn(*mut UintrFrame, u64),
    flags: c_int,
) -> UintrResult<c_int> {
    let result = unsafe { syscall(__NR_UINTR_REGISTER_HANDLER, handler, flags) as c_int };
    if result < 0 {
        Err(UintrError::RegisterHandlerError(format!(
            "uintr_register_handler failed: {}",
            std::io::Error::last_os_error()
        )))
    } else {
        Ok(result)
    }
}

pub fn uintr_create_fd(vector: c_int, flags: c_int) -> UintrResult<RawFd> {
    let result = unsafe { syscall(__NR_UINTR_CREATE_FD, vector, flags) as RawFd };
    if result < 0 {
        Err(UintrError::CreateFdError(format!(
            "uintr_create_fd failed: {}",
            std::io::Error::last_os_error()
        )))
    } else {
        Ok(result)
    }
}

pub fn uintr_register_sender(fd: RawFd, flags: c_int) -> UintrResult<c_int> {
    let result = unsafe { syscall(__NR_UINTR_REGISTER_SENDER, fd, flags) as c_int };
    if result < 0 {
        Err(UintrError::RegisterSenderError(format!(
            "uintr_register_sender failed: {}",
            std::io::Error::last_os_error()
        )))
    } else {
        Ok(result)
    }
}

pub fn uintr_unregister_handler() -> UintrResult<c_int> {
    let result = unsafe { syscall(__NR_UINTR_UNREGISTER_HANDLER) as c_int };
    if result < 0 {
        Err(UintrError::SyscallError(format!(
            "uintr_unregister_handler failed: {}",
            std::io::Error::last_os_error()
        )))
    } else {
        Ok(result)
    }
}

pub fn uintr_unregister_sender(fd: RawFd) -> UintrResult<c_int> {
    let result = unsafe { syscall(__NR_UINTR_UNREGISTER_SENDER, fd) as c_int };
    if result < 0 {
        Err(UintrError::SyscallError(format!(
            "uintr_unregister_sender failed: {}",
            std::io::Error::last_os_error()
        )))
    } else {
        Ok(result)
    }
}

pub fn uintr_wait(usec: c_long, flags: c_int) -> UintrResult<()> {
    let result = unsafe { syscall(__NR_UINTR_WAIT, usec, flags) };
    if result < 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::Interrupted {
            Ok(())
        } else {
            Err(UintrError::SyscallError(format!(
                "uintr_wait failed: {}",
                err
            )))
        }
    } else {
        Ok(())
    }
}

pub unsafe fn senduipi(index: u64) {
    unsafe {
        core::arch::asm!(
            "senduipi {0}",
            in(reg) index,
            options(nostack, nomem)
        );
    }
}

pub unsafe fn stui() {
    unsafe {
        core::arch::asm!("stui");
    }
}

pub unsafe fn clui() {
    unsafe {
        core::arch::asm!("clui");
    }
}

pub unsafe fn uiret() {
    unsafe {
        core::arch::asm!("uiret", options(noreturn));
    }
}
