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
pub const CS8: u32 = 0o60;
pub const CREAD: u32 = 0o200;
pub const CLOCAL: u32 = 0o4000;

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
pub const B115200: u32 = 0x1002;

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
    // tcgetattr: 获取终端属性到 termios 结构体，成功返回 0
    pub fn tcgetattr(fd: c_int, termios_p: *mut Termios) -> c_int;
    // tcsetattr: 将 termios 结构体的设置应用到终端，成功返回 0
    pub fn tcsetattr(fd: c_int, optional_actions: c_int, termios_p: *const Termios) -> c_int;
    // signal: 注册信号处理函数，返回之前的处理函数地址，失败返回 SIG_ERR (usize::MAX)
    pub fn signal(signum: c_int, handler: usize) -> usize;
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

// ── serial port configuration ───────────────────────────────
// 配置串口为 8N1 原始模式，无流控，带超时读取。
// 等价于: stty -F /dev/ttyS0 raw 115200 cs8 -cstopb -parenb -echo -echoe -echok -echoctl -echoke -ixon -ixoff clocal cread min 0 time 5

pub fn configure_serial(fd: c_int, baud_rate: u32, timeout_ms: u64) -> io::Result<()> {
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
    // 清除 CSIZE（数据位掩码）和 PARENB（校验位），然后设为 8 位数据位
    tios.c_cflag &= !(CS8 | 0o100); // clear CSIZE and PARENB
    tios.c_cflag |= CS8;

    // 启用接收器，忽略调制解调器控制线
    tios.c_cflag |= CREAD | CLOCAL;

    // ── 波特率 ──
    tios.c_ispeed = baud_rate;
    tios.c_ospeed = baud_rate;

    // ── 读取超时 ──
    // VMIN=0, VTIME=N 的语义：
    //   - 缓冲区有数据：立即返回，返回实际读取的字节数
    //   - 缓冲区无数据：等待最多 VTIME×0.1 秒，超时返回 0
    //   - 这与 serialport crate 的 Timeout 行为一致
    let vtime = ((timeout_ms + 99) / 100).min(255) as u8;
    tios.c_cc[VMIN] = 0;
    tios.c_cc[VTIME] = vtime;

    if unsafe { tcsetattr(fd, TCSANOW, &tios) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}