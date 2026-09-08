// UINTR 用户态中断模块

pub mod syscall;
pub mod connection;
pub mod benchmark;
pub mod async_wait;

// 系统调用号
pub const __NR_UINTR_REGISTER_HANDLER: libc::c_long = 471;
pub const __NR_UINTR_UNREGISTER_HANDLER: libc::c_long = 472;
pub const __NR_UINTR_CREATE_FD: libc::c_long = 473;
pub const __NR_UINTR_REGISTER_SENDER: libc::c_long = 474;
pub const __NR_UINTR_UNREGISTER_SENDER: libc::c_long = 475;
pub const __NR_UINTR_WAIT: libc::c_long = 476;

// UINTR 常量
// 说明：uintr_register_handler 的 flags 必须为 0，保证系统调用不会被
// 用户态中断打断（否则阻塞式 syscall 会被 UINTR 返回 EINTR）。
pub const UINTR_WAIT_MAX_USEC: libc::c_long = 10_000_000;
pub const UINTR_VECTOR: u64 = 0;

// 错误类型
#[derive(Debug)]
pub enum UintrError {
    SyscallError(String),
    RegisterHandlerError(String),
    CreateFdError(String),
    RegisterSenderError(String),
    SendFdError(String),
    RecvFdError(String),
    SocketError(String),
    Timeout,
    NotInitialized,
}

impl std::fmt::Display for UintrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UintrError::SyscallError(msg) => write!(f, "System call failed: {}", msg),
            UintrError::RegisterHandlerError(msg) => write!(f, "Failed to register handler: {}", msg),
            UintrError::CreateFdError(msg) => write!(f, "Failed to create FD: {}", msg),
            UintrError::RegisterSenderError(msg) => write!(f, "Failed to register sender: {}", msg),
            UintrError::SendFdError(msg) => write!(f, "Failed to send file descriptor: {}", msg),
            UintrError::RecvFdError(msg) => write!(f, "Failed to receive file descriptor: {}", msg),
            UintrError::SocketError(msg) => write!(f, "Socket error: {}", msg),
            UintrError::Timeout => write!(f, "Timeout"),
            UintrError::NotInitialized => write!(f, "Not initialized"),
        }
    }
}

impl std::error::Error for UintrError {}

pub type UintrResult<T> = Result<T, UintrError>;
