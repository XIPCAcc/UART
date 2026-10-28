// Raw FFI bindings to libc -- no external crate needed.
// 爬虫守护进程的 TCP/epoll 系统调用通过直接调用 Linux libc 函数实现，不依赖任何第三方 crate。
use std::ffi::{c_int, c_void};
use std::io;

// ── epoll ────────────────────────────────────────────────────
pub const EPOLL_CTL_ADD: c_int = 1;
pub const EPOLL_CTL_MOD: c_int = 3;
pub const EPOLLIN: u32 = 0x001;
pub const EPOLLOUT: u32 = 0x004;
pub const EPOLLET: u32 = 1 << 31; // 边缘触发

// ── TCP socket ───────────────────────────────────────────────
// AF_INET:      IPv4 协议族
// SOCK_STREAM:  面向连接的字节流（TCP）
// SOL_SOCKET:   socket 层选项
// SO_ERROR:     取 pending 错误码（非阻塞 connect 完成后据此判成败）
// EINPROGRESS:  非阻塞 connect 正在进行中
pub const AF_INET: c_int = 2;
pub const SOCK_STREAM: c_int = 1;
// SOCK_NONBLOCK: socket()/accept4() 时直接建非阻塞 fd，省掉后续 fcntl 两次调用
pub const SOCK_NONBLOCK: c_int = 0o4000;
pub const SOL_SOCKET: c_int = 1;
pub const SO_ERROR: c_int = 4;
pub const SO_REUSEADDR: c_int = 2;
pub const EINPROGRESS: i32 = 115;

/// IPv4 socket 地址（内核 struct sockaddr_in 布局）
#[repr(C)]
pub struct SockaddrIn {
    pub sin_family: u16,
    pub sin_port: u16, // 网络字节序
    pub sin_addr: u32, // 网络字节序
    pub sin_zero: [u8; 8],
}

#[repr(C, packed)]
pub struct EpollEvent {
    pub events: u32,
    pub data: u64,
}

impl EpollEvent {
    pub unsafe fn zeroed() -> Self {
        std::mem::zeroed()
    }
}

// ── signals ─────────────────────────────────────────────────
// SIGINT:  中断信号（Ctrl+C 产生），用于优雅退出
// SIGTERM: 终止信号（systemctl stop 发送），用于优雅退出
pub const SIGINT: c_int = 2;
pub const SIGTERM: c_int = 15;

// ── extern "C" declarations ─────────────────────────────────
// 直接声明 libc 函数，链接器会自动链接 libc.so
extern "C" {
    // close: 关闭文件描述符，成功返回 0，失败返回 -1
    pub fn close(fd: c_int) -> c_int;
    // read: 从文件描述符读取数据，返回实际读取字节数，失败返回 -1
    pub fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    // write: 向文件描述符写入数据，返回实际写入字节数，失败返回 -1
    pub fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    // signal: 注册信号处理函数，返回之前的处理函数地址，失败返回 SIG_ERR (usize::MAX)
    pub fn signal(signum: c_int, handler: usize) -> usize;
    // epoll_create1: 创建 epoll 实例，flags 为 0 或 EPOLL_CLOEXEC，返回 epoll fd
    pub fn epoll_create1(flags: c_int) -> c_int;
    // epoll_ctl: 控制 epoll 实例，添加/修改/删除监视的 fd，成功返回 0
    pub fn epoll_ctl(epfd: c_int, op: c_int, fd: c_int, event: *const EpollEvent) -> c_int;
    // epoll_wait: 等待 epoll 事件，阻塞直到有事件或超时，返回就绪事件数
    #[link_name = "epoll_wait"]
    pub fn libc_epoll_wait(epfd: c_int, events: *mut EpollEvent, maxevents: c_int, timeout: c_int) -> c_int;
    // socket: 创建 TCP socket，返回 fd
    pub fn socket(domain: c_int, ty: c_int, protocol: c_int) -> c_int;
    // connect: 非阻塞 socket 上返回 -1/EINPROGRESS 表示连接进行中
    pub fn connect(fd: c_int, addr: *const SockaddrIn, addrlen: u32) -> c_int;
    // getsockopt: 非阻塞 connect 完成后取 SO_ERROR 判定成败（0 = 成功）
    pub fn getsockopt(fd: c_int, level: c_int, optname: c_int, optval: *mut c_int, optlen: *mut u32) -> c_int;
    // setsockopt: 设置 socket 选项（SO_REUSEADDR 等）
    pub fn setsockopt(fd: c_int, level: c_int, optname: c_int, optval: *const c_int, optlen: u32) -> c_int;
    // bind: 将 socket 绑定到本地地址
    pub fn bind(fd: c_int, addr: *const SockaddrIn, addrlen: u32) -> c_int;
    // listen: 将 socket 置为监听态，backlog 为全连接队列长度
    pub fn listen(fd: c_int, backlog: c_int) -> c_int;
    // accept4: 同 accept，但可用 flags 直接建非阻塞 fd（省每连接两次 fcntl）
    pub fn accept4(fd: c_int, addr: *mut SockaddrIn, addrlen: *mut u32, flags: c_int) -> c_int;

}

// ── safe wrappers ───────────────────────────────────────────
// 将原始 libc 返回值转换为 Rust 的 io::Result

pub fn raw_read(fd: c_int, buf: &mut [u8]) -> io::Result<usize> {
    let n = unsafe { read(fd, buf.as_mut_ptr() as *mut c_void, buf.len()) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

pub fn raw_write(fd: c_int, buf: &[u8]) -> io::Result<usize> {
    let n = unsafe { write(fd, buf.as_ptr() as *const c_void, buf.len()) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

pub fn epoll_create() -> io::Result<c_int> {
    let fd = unsafe { epoll_create1(0) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(fd)
    }
}

pub fn epoll_mod(epfd: c_int, fd: c_int, events: u32) -> io::Result<()> {
    let mut ev = unsafe { EpollEvent::zeroed() };
    ev.events = events | EPOLLET;
    ev.data = fd as u64;
    let ret = unsafe { epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}


/// epoll_wait: 阻塞等待事件，返回就绪事件数（0 表示超时）
pub fn epoll_wait(epfd: c_int, events: &mut [EpollEvent], timeout_ms: c_int) -> io::Result<usize> {
    let n = unsafe {
        libc_epoll_wait(epfd, events.as_mut_ptr(), events.len() as c_int, timeout_ms)
    };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

/// 关闭 fd
pub fn close_fd(fd: c_int) {
    unsafe { close(fd) };
}

// ── TCP socket ───────────────────────────────────────────────

/// 创建非阻塞 TCP socket 并发起连接。
/// 返回 Ok(fd)：连接可能已完成或仍在进行（EINPROGRESS），
/// 调用方需等 EPOLLOUT 就绪后用 socket_error() 判定最终成败。
pub fn tcp_connect_v4(addr: std::net::Ipv4Addr, port: u16) -> io::Result<c_int> {
    // SOCK_NONBLOCK 在创建时就置好非阻塞，省掉 fcntl(F_GETFL)+fcntl(F_SETFL)
    let fd = unsafe { socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let sa = SockaddrIn {
        sin_family: AF_INET as u16,
        sin_port: port.to_be(),
        sin_addr: u32::from(addr).to_be(),
        sin_zero: [0; 8],
    };
    let r = unsafe { connect(fd, &sa, std::mem::size_of::<SockaddrIn>() as u32) };
    if r == 0 {
        return Ok(fd); // 立即连接成功（本机回环常见）
    }
    let e = io::Error::last_os_error();
    if e.raw_os_error() == Some(EINPROGRESS) {
        return Ok(fd); // 连接进行中
    }
    close_fd(fd);
    Err(e)
}

/// 创建非阻塞 TCP 监听 socket（SO_REUSEADDR + bind + listen），返回监听 fd
pub fn tcp_listen_v4(addr: std::net::Ipv4Addr, port: u16, backlog: c_int) -> io::Result<c_int> {
    let fd = unsafe { socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let one: c_int = 1;
    unsafe {
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, std::mem::size_of::<c_int>() as u32)
    };
    let sa = SockaddrIn {
        sin_family: AF_INET as u16,
        sin_port: port.to_be(),
        sin_addr: u32::from(addr).to_be(),
        sin_zero: [0; 8],
    };
    if unsafe { bind(fd, &sa, std::mem::size_of::<SockaddrIn>() as u32) } < 0 {
        let e = io::Error::last_os_error();
        close_fd(fd);
        return Err(e);
    }
    if unsafe { listen(fd, backlog) } < 0 {
        let e = io::Error::last_os_error();
        close_fd(fd);
        return Err(e);
    }
    Ok(fd)
}

/// 从监听 fd 接受一个连接，返回 (连接 fd, 对端地址)
pub fn accept_v4(lfd: c_int) -> io::Result<(c_int, std::net::SocketAddr)> {
    let mut sa = SockaddrIn {
        sin_family: 0,
        sin_port: 0,
        sin_addr: 0,
        sin_zero: [0; 8],
    };
    let mut len = std::mem::size_of::<SockaddrIn>() as u32;
    // SOCK_NONBLOCK 直接建非阻塞连接 fd，省掉 fcntl(F_GETFL)+fcntl(F_SETFL)
    let fd = unsafe { accept4(lfd, &mut sa, &mut len, SOCK_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let ip = std::net::Ipv4Addr::from(u32::from_be(sa.sin_addr));
    let port = u16::from_be(sa.sin_port);
    Ok((fd, std::net::SocketAddr::new(ip.into(), port)))
}

/// 非阻塞 connect 完成后取 SO_ERROR：0 表示连接成功
pub fn socket_error(fd: c_int) -> io::Result<i32> {
    let mut val: c_int = 0;
    let mut len: u32 = std::mem::size_of::<c_int>() as u32;
    let r = unsafe { getsockopt(fd, SOL_SOCKET, SO_ERROR, &mut val, &mut len) };
    if r < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(val)
    }
}


