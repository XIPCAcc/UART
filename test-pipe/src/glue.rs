// UINTR 中断回调与 token 注册
//
// 与 handler.c 编译在同一 crate，保证链接期 rust_interrupt_callback 符号
// 与 ui_handler 同 crate 解析（避免跨 rlib 静态库链接顺序问题）。

use std::os::raw::c_char;
use std::sync::atomic::Ordering;

use uintr_runtime::uintr_core::{UintrToken, WAKE_FLAG};

static mut TOKEN: Option<UintrToken> = None;

/// handler.c 的中断处理程序 ui_handler 会回调本函数。
/// 必须在中断上下文中可安全重入：无锁、不 panic（见 UintrToken::set_pending）。
#[no_mangle]
pub extern "C" fn rust_interrupt_callback(_handler_name: *const c_char, _vector: u64) {
    unsafe {
        if let Some(ref token) = TOKEN {
            token.set_pending();
            // 写入唤醒标志，解除 UMONITOR/UMWAIT 休眠
            WAKE_FLAG.store(1, Ordering::Release);
        }
    }

}

/// 注册中断回调所用的 token（与 async_wait::init_token 返回的同一 token）
pub fn set_handler_token(token: UintrToken) {
    unsafe { TOKEN = Some(token); }
}
