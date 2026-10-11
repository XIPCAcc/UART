// Raw FFI bindings to libc -- no external crate needed.
// 所有串口操作通过直接调用 Linux libc 函数实现，不依赖任何第三方 crate。
use std::ffi::{c_char, c_int, c_void};
use std::io;

// ── open flags ──────────────────────────────────────────────
// O_RDWR:   读写模式打开
// O_NOCTTY: 不将此终端设为进程的控制终端（守护进程必须设置）
pub const O_RDWR: c_int = 0o2;
pub const O_NOCTTY: c_int = 0o400;

// ── termios action ──────────────────────────────────────────
// TCSANOW: 立即生效，不等待数据发送完毕
pub const TCSANOW: c_int = 0;

// ── c_cflag bits ────────────────────────────────────────────
// CS8:    8 位数据位
// CREAD:  允许接收
// CLOCAL: 忽略调制解调器控制线（无载波检测），串口直连必须设置
// BOTHER: 使用 c_ispeed/c_ospeed 字段指定波特率（而非 CBAUD 位）
// CSTOPB: 2 位停止位（清除 = 1 位停止位）
// PARENB: 启用校验位（清除 = 无校验）
pub const CS8: u32 = 0o60;
pub const CREAD: u32 = 0o200;
pub const CLOCAL: u32 = 0o4000;
pub const BOTHER: u32 = 0o100000;
pub const CBAUD: u32 = 0o0010017;
pub const CSTOPB: u32 = 0o100;
pub const PARENB: u32 = 0o400;

// ── c_iflag bits (for cfmakeraw) ───────────────────────────
// cfmakeraw 会清除这些标志，使串口工作在原始二进制模式：
// IGNBRK: 忽略中断条件
// BRKINT: 中断条件产生 SIGINT（原始模式下需要关闭）
// PARMRK: 标记奇偶校验错误（原始模式下不需要）
// ISTRIP: 去除第 8 位（原始模式需要完整 8 位）
// INLCR:  将 NL 转为 CR（原始模式不做转换）
// IGNCR:  忽略 CR（原始模式不做转换）
// ICRNL:  将 CR 转为 NL（原始模式不做转换）
// IXON:   启用软件流控 XON/XOFF（我们不需要流控）
const IGNBRK: u32 = 0o1;
const BRKINT: u32 = 0o2;
const PARMRK: u32 = 0o10;
const ISTRIP: u32 = 0o40;
const INLCR: u32 = 0o100;
const IGNCR: u32 = 0o200;
const ICRNL: u32 = 0o400;
const IXON: u32 = 0o2000;

// ── c_oflag bits ────────────────────────────────────────────
// OPOST: 启用输出处理（原始模式需要关闭，否则 \n 会被转为 \r\n）
const OPOST: u32 = 0o1;

// ── c_lflag bits ────────────────────────────────────────────
// ECHO:   回显输入字符（串口二进制通信必须关闭）
// ECHONL: 回显 NL（原始模式不需要）
// ICANON: 规范模式（行缓冲，按行读取；原始模式需要关闭以逐字节读取）
// ISIG:   识别信号字符（Ctrl+C 等；原始模式不识别）
// IEXTEN: 扩展输入处理（原始模式不需要）
const ECHO: u32 = 0o10;
const ECHONL: u32 = 0o100;
const ICANON: u32 = 0o2;
const ISIG: u32 = 0o1;
const IEXTEN: u32 = 0o100000;

// ── c_cc indexes ────────────────────────────────────────────
// VMIN:  read() 返回的最小字节数
// VTIME: read() 的超时时间，单位为 0.1 秒
const VMIN: usize = 6;
const VTIME: usize = 5;

// ── baud rate ───────────────────────────────────────────────

// ── fcntl ────────────────────────────────────────────────────
pub const F_GETFL: c_int = 3;
pub const F_SETFL: c_int = 4;
pub const O_NONBLOCK: c_int = 0o4000;

// ── epoll ────────────────────────────────────────────────────
pub const EPOLL_CTL_ADD: c_int = 1;
pub const EPOLL_CTL_MOD: c_int = 3;
pub const EPOLL_CTL_DEL: c_int = 2;
pub const EPOLLIN: u32 = 0x001;
pub const EPOLLOUT: u32 = 0x004;
pub const EPOLLET: u32 = 1 << 31; // 边缘触发，避免 CH340 虚假唤醒

// ── TCP socket ───────────────────────────────────────────────
// AF_INET:      IPv4 协议族
// SOCK_STREAM:  面向连接的字节流（TCP）
// SOL_SOCKET:   socket 层选项
// SO_ERROR:     取 pending 错误码（非阻塞 connect 完成后据此判成败）
// EINPROGRESS:  非阻塞 connect 正在进行中
pub const AF_INET: c_int = 2;
pub const SOCK_STREAM: c_int = 1;
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

// ── termios struct (glibc layout, x86_64) ───────────────────
// 这是 Linux 内核用来配置终端/串口参数的核心结构体。
// 对应 glibc 的 <termbits.h> 定义，x86_64 平台布局。
// 注意：此布局是平台相关的，ARM 等其他架构的 c_cc 大小和对齐可能不同。
//
// c_iflag:  输入模式标志 — 控制输入处理（换行转换、软件流控等）
// c_oflag:  输出模式标志 — 控制输出处理（换行转换等）
// c_cflag:  控制模式标志 — 控制波特率、数据位、校验位、停止位等
// c_lflag:  本地模式标志 — 控制回显、规范模式、信号识别等
// c_line:   线路规程（N_TTY = 0，默认值）
// c_cc:     特殊字符数组 — 定义 VMIN/VTIME 等控制字符的值
// c_ispeed: 输入波特率
// c_ospeed: 输出波特率
#[repr(C)]
pub struct Termios {
    pub c_iflag: u32,
    pub c_oflag: u32,
    pub c_cflag: u32,
    pub c_lflag: u32,
    pub c_line: u8,
    pub c_cc: [u8; 32],
    pub c_ispeed: u32,
    pub c_ospeed: u32,
}

impl Termios {
    pub unsafe fn zeroed() -> Self {
        std::mem::zeroed()
    }
}

// ── extern "C" declarations ─────────────────────────────────
// 直接声明 libc 函数，链接器会自动链接 libc.so
extern "C" {
    // open: 打开文件/设备，返回文件描述符，失败返回 -1
    pub fn open(pathname: *const c_char, flags: c_int, mode: c_int) -> c_int;
    // close: 关闭文件描述符，成功返回 0，失败返回 -1
    pub fn close(fd: c_int) -> c_int;
    // read: 从文件描述符读取数据，返回实际读取字节数，失败返回 -1
    pub fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    // write: 向文件描述符写入数据，返回实际写入字节数，失败返回 -1
    pub fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    // fcntl: 文件描述符控制操作
    pub fn fcntl(fd: c_int, cmd: c_int, arg: c_int) -> c_int;
    // tcgetattr: 获取终端属性到 termios 结构体，成功返回 0
    pub fn tcgetattr(fd: c_int, termios_p: *mut Termios) -> c_int;
    // tcsetattr: 将 termios 结构体的设置应用到终端，成功返回 0
    pub fn tcsetattr(fd: c_int, optional_actions: c_int, termios_p: *const Termios) -> c_int;
    // signal: 注册信号处理函数，返回之前的处理函数地址，失败返回 SIG_ERR (usize::MAX)
    pub fn signal(signum: c_int, handler: usize) -> usize;
    // epoll_create1: 创建 epoll 实例，flags 为 0 或 EPOLL_CLOEXEC，返回 epoll fd
    pub fn epoll_create1(flags: c_int) -> c_int;
    // epoll_ctl: 控制 epoll 实例，添加/修改/删除监视的 fd，成功返回 0
    pub fn epoll_ctl(epfd: c_int, op: c_int, fd: c_int, event: *const EpollEvent) -> c_int;
    // epoll_wait: 等待 epoll 事件，阻塞直到有事件或超时，返回就绪事件数
    #[link_name = "epoll_wait"]
    pub fn libc_epoll_wait(epfd: c_int, events: *mut EpollEvent, maxevents: c_int, timeout: c_int) -> c_int;
    // eventfd: 创建事件计数 fd，用于从任意线程唤醒 epoll_wait
    pub fn eventfd(initval: u32, flags: c_int) -> c_int;
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
    // accept: 从监听 socket 取出一个已建立连接，返回新 fd（沿用监听 fd 的非阻塞标志）
    pub fn accept(fd: c_int, addr: *mut SockaddrIn, addrlen: *mut u32) -> c_int;
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

pub fn epoll_add(epfd: c_int, fd: c_int) -> io::Result<()> {
    let mut ev = unsafe { EpollEvent::zeroed() };
    ev.events = EPOLLIN | EPOLLET;
    ev.data = fd as u64;
    let ret = unsafe { epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
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

pub fn epoll_del(epfd: c_int, fd: c_int) -> io::Result<()> {
    let ret = unsafe { epoll_ctl(epfd, EPOLL_CTL_DEL, fd, std::ptr::null()) };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn set_nonblocking(fd: c_int) -> io::Result<()> {
    let flags = unsafe { fcntl(fd, F_GETFL, 0) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    let ret = unsafe { fcntl(fd, F_SETFL, flags | O_NONBLOCK) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
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

// ── eventfd ──────────────────────────────────────────────────

/// 创建 eventfd，用于 poke 阻塞在 epoll_wait 的 IO 线程
pub fn event_fd() -> io::Result<c_int> {
    let fd = unsafe { eventfd(0, 0) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(fd)
    }
}

/// 向 eventfd 写入 1（累加语义，多次 poke 只唤醒一次也够）
pub fn eventfd_poke(fd: c_int) {
    let v: u64 = 1;
    let _ = raw_write(fd, &v.to_ne_bytes());
}

/// 读出 eventfd 计数并清零（IO 线程被唤醒后必须读，否则水平触发会一直报就绪）
pub fn eventfd_drain(fd: c_int) {
    let mut buf = [0u8; 8];
    let _ = raw_read(fd, &mut buf);
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
    let fd = unsafe { socket(AF_INET, SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    if let Err(e) = set_nonblocking(fd) {
        close_fd(fd);
        return Err(e);
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
    let fd = unsafe { socket(AF_INET, SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let one: c_int = 1;
    unsafe {
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, std::mem::size_of::<c_int>() as u32)
    };
    if let Err(e) = set_nonblocking(fd) {
        close_fd(fd);
        return Err(e);
    }
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
    let fd = unsafe { accept(lfd, &mut sa, &mut len) };
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

// ── serial port configuration ───────────────────────────────
// 配置串口为 8N1 原始模式，无流控，非阻塞读取。
// VMIN=1, VTIME=0 配合 O_NONBLOCK：无数据时 read() 返回 EAGAIN，
// epoll 在有数据到达前不会报告 EPOLLIN，保证 epoll_wait 正确阻塞。
// 等价于: stty -F /dev/ttyS0 raw 115200 cs8 -cstopb -parenb -echo min 1 time 0

pub fn configure_serial(fd: c_int, baud_rate: u32) -> io::Result<()> {
    // 先读取当前串口配置
    let mut tios: Termios = unsafe { Termios::zeroed() };
    if unsafe { tcgetattr(fd, &mut tios) } != 0 {
        return Err(io::Error::last_os_error());
    }

    // ── cfmakeraw: 将串口设为原始二进制模式 ──
    // 清除所有输入处理标志，不做任何字符转换
    tios.c_iflag &= !(IGNBRK | BRKINT | PARMRK | ISTRIP | INLCR | IGNCR | ICRNL | IXON);
    // 关闭输出处理，直接发送原始字节
    tios.c_oflag &= !OPOST;
    // 关闭规范模式、回显、信号识别等
    tios.c_lflag &= !(ECHO | ECHONL | ICANON | ISIG | IEXTEN);
    // 清除 CSIZE（数据位）、CSTOPB（停止位）、PARENB（校验位），然后设为 8N1
    tios.c_cflag &= !(CS8 | CSTOPB | PARENB);
    tios.c_cflag |= CS8;

    // 启用接收器，忽略调制解调器控制线
    tios.c_cflag |= CREAD | CLOCAL;

    // ── 波特率 ──
    // 清除旧的 CBAUD 位，设置 BOTHER 以使用 c_ispeed/c_ospeed 指定任意波特率
    tios.c_cflag &= !CBAUD;
    tios.c_cflag |= BOTHER;
    tios.c_ispeed = baud_rate;
    tios.c_ospeed = baud_rate;

    // ── 读取模式（VMIN + VTIME 组合语义）──
    //   VMIN=0, VTIME=0 → 非阻塞：read() 立即返回，有数据返回数据，无数据返回 0
    //   VMIN=0, VTIME=N → 带超时：read() 最多阻塞 N×100ms，超时返回 0
    //   VMIN=1, VTIME=0 → 阻塞等待：read() 永远阻塞，直到收到至少 1 字节
    // VMIN=1, VTIME=0: read() 阻塞直到至少 1 字节可用
    // 配合 O_NONBLOCK: 无数据时返回 EAGAIN，epoll 正确报告 EPOLLIN
    tios.c_cc[VMIN] = 1;
    tios.c_cc[VTIME] = 0;

    if unsafe { tcsetattr(fd, TCSANOW, &tios) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}