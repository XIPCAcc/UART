use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::AppError;
use crate::sys;

pub(crate) static TERM: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_signal(_: std::ffi::c_int) {
    TERM.store(true, Ordering::Relaxed);
}

pub fn register_signals() -> Result<(), AppError> {
    let prev_int = unsafe { sys::signal(sys::SIGINT, handle_signal as *const () as usize) };
    if prev_int == usize::MAX {
        return Err(AppError::Signal("SIGINT".into()));
    }
    let prev_term = unsafe { sys::signal(sys::SIGTERM, handle_signal as *const () as usize) };
    if prev_term == usize::MAX {
        return Err(AppError::Signal("SIGTERM".into()));
    }
    Ok(())
}