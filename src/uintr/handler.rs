use libc::c_char;
use crate::uintr_core::UintrToken;

#[no_mangle]
pub extern "C" fn rust_interrupt_callback(_handler_name: *const c_char, _vector: u64) {
    eprintln!("[TRACE] handler: UINTR interrupt received!");
    unsafe {
        if let Some(ref token) = TOKEN {
            token.set_pending();
            let seq = token.inner.seq.load(std::sync::atomic::Ordering::Acquire);
            eprintln!("[TRACE] handler: token.set_pending() done, seq={}", seq);
        } else {
            eprintln!("[WARN] handler: TOKEN is None, interrupt ignored");
        }
    }
}

static mut TOKEN: Option<UintrToken> = None;

pub fn set_handler_token(token: UintrToken) {
    unsafe {
        TOKEN = Some(token);
    }
}
