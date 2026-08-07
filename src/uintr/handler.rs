use crate::uintr_core::{UintrToken, WAKE_FLAG};
use std::sync::atomic::Ordering;

#[no_mangle]
pub extern "C" fn rust_interrupt_callback(_handler_name: *const libc::c_char, _vector: u64) {
    unsafe {
        if let Some(ref token) = TOKEN {
            token.set_pending();
        }
    }
    // 写入唤醒标志，解除 UMONITOR/UMWAIT 休眠
    WAKE_FLAG.store(1, Ordering::Release);
}

static mut TOKEN: Option<UintrToken> = None;

pub fn set_handler_token(token: UintrToken) {
    unsafe {
        TOKEN = Some(token);
    }
}
