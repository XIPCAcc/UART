// Raw FFI bindings to libc -- no external crate needed.
// 爬虫守护进程的 TCP/epoll 系统调用通过直接调用 Linux libc 函数实现，不依赖任何第三方 crate。
use std::ffi::{c_char, c_int, c_void};
use std::io;

// ── raw clone3 线程 ────────────────────────────────────────────
// 动态 worker 不用 std::thread（每线程约 17 次 glibc NPTL/Rust std 自举
// 系统调用），改用裸 clone3 + 手写 trampoline。配合 ld.so 的
// _dl_allocate_tls 为子线程建立完整 TLS（glibc malloc 的 tcache、errno、
// 栈金丝雀等都依赖它），线程生命周期系统调用降到 mmap+mprotect+clone3+exit
// 共 4 次。

pub const CLONE_VM: u64 = 0x0000_0100;
pub const CLONE_FS: u64 = 0x0000_0200;
pub const CLONE_FILES: u64 = 0x0000_0400;
pub const CLONE_SIGHAND: u64 = 0x0000_0800;
pub const CLONE_THREAD: u64 = 0x0001_0000;
pub const CLONE_SYSVSEM: u64 = 0x0004_0000;
pub const CLONE_SETTLS: u64 = 0x0008_0000;
pub const CLONE_CHILD_SETTID: u64 = 0x0100_0000; // 创建时把 tid 写入 child_tid
pub const CLONE_CHILD_CLEARTID: u64 = 0x0020_0000; // 退出时清零 child_tid（回收信号）

/// 裸 worker 线程的 clone3 标志组合：与 pthread 语义一致（共享地址空间/
/// 文件表/信号处理，同线程组），外加自管理 TLS 与退出自清 tid
pub const THREAD_CLONE_FLAGS: u64 = CLONE_VM
    | CLONE_FS
    | CLONE_FILES
    | CLONE_SIGHAND
    | CLONE_THREAD
    | CLONE_SYSVSEM
    | CLONE_SETTLS
    | CLONE_CHILD_SETTID
    | CLONE_CHILD_CLEARTID;

pub const PROT_NONE: c_int = 0;
pub const PROT_READ: c_int = 1;
pub const PROT_WRITE: c_int = 2;
pub const MAP_PRIVATE: c_int = 0x02;
pub const MAP_ANONYMOUS: c_int = 0x20;
pub const MAP_STACK: c_int = 0x20000;

/// kernel struct clone_args（88 字节，CLONE_ARGS_SIZE_VER2）
#[repr(C)]
pub struct CloneArgs {
    pub flags: u64,
    pub pidfd: u64,
    pub child_tid: u64,
    pub parent_tid: u64,
    pub exit_signal: u64, // CLONE_THREAD 下必须为 0
    pub stack: u64,       // 栈区最低地址
    pub stack_size: u64,  // 内核置子线程 rsp = stack + stack_size
    pub tls: u64,         // CLONE_SETTLS：子线程 fs base（TCB 指针）
    pub set_tid: u64,
    pub set_tid_size: u64,
    pub cgroup: u64,
}

/// clone3 + 子线程 trampoline（x86_64，SysV：rdi=args, rsi=entry, rdx=arg）。
/// 父路径：返回子 tid（>0）或 -errno（<0）。
/// 子路径：内核把全部通用寄存器复制给子线程，r12/r13 在 syscall 前装入
/// entry/arg 并被继承；子线程以新栈顶为 rsp 调 entry(arg)，返回后用
/// exit(60) 只终止本线程（不能 return——栈上没有返回地址；不能用
/// exit_group——那会杀掉整个进程）。
#[unsafe(naked)]
pub unsafe extern "C" fn raw_clone3(args: *const CloneArgs, entry: usize, arg: usize) -> i64 {
    core::arch::naked_asm!(
        "push r12",
        "push r13",
        "mov r12, rsi", // entry（跨 clone3 继承给子线程）
        "mov r13, rdx", // arg
        "mov eax, 435", // SYS_clone3
        "mov esi, 88",  // sizeof(CloneArgs)，rdi 已是 args
        "syscall",
        "test rax, rax",
        "jz 2f",
        // ── 父路径 ──
        "pop r13",
        "pop r12",
        "ret",
        // ── 子路径（rax=0，rsp=新栈顶=stack+stack_size，16 字节对齐）──
        "2:",
        "mov rdi, r13",
        "call r12",
        "mov eax, 60", // SYS_exit：仅终止调用线程
        "xor edi, edi",
        "syscall",
        "ud2",
    );
}

// ── epoll ────────────────────────────────────────────────────
pub const EPOLL_CTL_ADD: c_int = 1;
pub const EPOLL_CTL_MOD: c_int = 3;
pub const EPOLL_CTL_DEL: c_int = 2;
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
    // mmap: 分配匿名映射（worker 栈）
    pub fn mmap(addr: *mut c_void, len: usize, prot: c_int, flags: c_int, fd: c_int, offset: i64) -> *mut c_void;
    // munmap: 释放映射
    pub fn munmap(addr: *mut c_void, len: usize) -> c_int;
    // mprotect: 改页保护（栈保护页）
    pub fn mprotect(addr: *mut c_void, len: usize, prot: c_int) -> c_int;
    // dlvsym: 按版本号取动态符号（GLIBC_PRIVATE 的 ld.so 内部符号只能这样拿）
    pub fn dlvsym(handle: *mut c_void, symbol: *const c_char, version: *const c_char) -> *mut c_void;
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

pub fn epoll_del(epfd: c_int, fd: c_int) -> io::Result<()> {
    let ret = unsafe { epoll_ctl(epfd, EPOLL_CTL_DEL, fd, std::ptr::null()) };
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

// ── 裸线程栈与 TLS ─────────────────────────────────────────────

/// 分配 worker 栈：低地址一页 PROT_NONE 保护页 + stack_size 可读写。
/// 返回 (映射基址, 总映射长度, 栈区最低地址)；栈顶 = 基址 + 总长度。
pub fn alloc_stack(stack_size: usize) -> io::Result<(*mut u8, usize, *mut u8)> {
    let guard = 4096usize;
    let total = guard + stack_size;
    let base = unsafe {
        mmap(
            std::ptr::null_mut(),
            total,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_STACK,
            -1,
            0,
        )
    };
    if base as isize == -1 {
        return Err(io::Error::last_os_error());
    }
    // 栈向下生长：最低页置 PROT_NONE，溢出即 SIGSEGV 而非静默踩内存
    if unsafe { mprotect(base, guard, PROT_NONE) } < 0 {
        let e = io::Error::last_os_error();
        unsafe { munmap(base, total) };
        return Err(e);
    }
    Ok((base as *mut u8, total, unsafe { base.add(guard) } as *mut u8))
}

/// 释放 alloc_stack 分配的映射
pub fn free_stack(base: *mut u8, total: usize) {
    unsafe { munmap(base as *mut c_void, total) };
}

/// ld.so/glibc 的线程 TLS 支持函数（GLIBC_PRIVATE，仅 dlvsym 可取）。
/// allocate/deallocate 缺一不可；thread_freeres 可选（回收每线程 malloc
/// 缓存，缺失时每个 worker 生命周期泄漏约 1KB，可接受）。
pub struct TlsFns {
    /// void *_dl_allocate_tls(void *mem)：mem=NULL 时自行分配，
    /// 返回子线程 TCB（即 fs base / clone3 的 tls 参数）
    pub allocate: usize,
    /// void _dl_deallocate_tls(void *tcb, bool dealloc_tcb)
    pub deallocate: usize,
    /// void __libc_thread_freeres(void)：线程退出前回收 tcache 等
    pub thread_freeres: usize, // 0 = 不可用
    /// __libc_single_threaded 全局变量地址（0 = 不可用）：
    /// 裸 clone3 不经 pthread_create，glibc 不知道进程已多线程，
    /// malloc 等路径会走无锁单线程快道 → 启动时即置 0
    pub single_threaded: usize,
}

/// 取当前线程的 TCB 指针（x86_64 上 fs:0 即 TCB 自指 = fs base）
#[inline]
pub fn current_tcb() -> *mut u64 {
    let p: *mut u64;
    unsafe { core::arch::asm!("mov {}, fs:0", out(reg) p, options(nostack, pure, readonly)) };
    p
}

/// 解析 ld.so/glibc 私有 TLS 符号；找不到 allocate/deallocate 返回 None
pub fn resolve_tls_fns() -> Option<TlsFns> {
    unsafe {
        // RTLD_DEFAULT = NULL：在全局符号 scope（含 ld.so 自身）中查找
        let alloc = dlvsym(
            std::ptr::null_mut(),
            c"_dl_allocate_tls".as_ptr(),
            c"GLIBC_PRIVATE".as_ptr(),
        );
        let dealloc = dlvsym(
            std::ptr::null_mut(),
            c"_dl_deallocate_tls".as_ptr(),
            c"GLIBC_PRIVATE".as_ptr(),
        );
        if alloc.is_null() || dealloc.is_null() {
            return None;
        }
        let freeres = dlvsym(
            std::ptr::null_mut(),
            c"__libc_thread_freeres".as_ptr(),
            c"GLIBC_PRIVATE".as_ptr(),
        );
        let single = dlvsym(
            std::ptr::null_mut(),
            c"__libc_single_threaded".as_ptr(),
            c"GLIBC_2.32".as_ptr(),
        );
        Some(TlsFns {
            allocate: alloc as usize,
            deallocate: dealloc as usize,
            thread_freeres: freeres as usize,
            single_threaded: single as usize,
        })
    }
}
