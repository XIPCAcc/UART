// 无缓冲、无 Mutex 的 stderr 写入工具。
//
// 为什么不能用 eprintln!：
//   Rust std 的 stderr 内部持有一把 pthread_mutex_t (PTHREAD_MUTEX_DEFAULT，不可重入)。
//   若用户态中断 UINTR 恰好发生在 eprintln! 持锁期间（格式化/write 任意一步），
//   而中断 handler 路径上的任何代码最终再调用 eprintln!（比如 panic hook、
//   alloc::boxed::from_raw 触发的 abort 信息等），就会发生单线程死锁：
//   mutex.owner == current_thread，lock() 永远阻塞等待，unlock() 永远不被执行。
//
//   表现就是用户看到的 "打印到一半卡死，Ctrl+C 也无法结束"。
//
//   （Ctrl+C 本身也可能走 eprintln! 输出 "^C"，同样死锁在同一把锁上。）
//
// 这里直接使用 libc::write(STDERR_FILENO, ...)，走 syscall，完全不碰 std 的 Stderr Mutex。
// 代价是每个 [TRACE] 行都会产生一次 syscall（频率高时开销略大），但调试安全第一。

use libc::{c_void, size_t, ssize_t, STDERR_FILENO, STDOUT_FILENO};

unsafe fn write_raw(fd: libc::c_int, bytes: &[u8]) {
    if bytes.is_empty() { return; }
    let mut remaining: &[u8] = bytes;
    while !remaining.is_empty() {
        let n = libc::write(
            fd,
            remaining.as_ptr() as *const c_void,
            remaining.len() as size_t,
        );
        if n <= 0 {
            // 写失败（比如 EPIPE / EINTR）：直接放弃，绝不 panic
            return;
        }
        // 切片向前推进 n 字节（partial write 处理）
        remaining = remaining.split_at(n as usize).1;
    }
}

/// 写 &str 到 stderr，无缓冲、无锁、不可 panics
pub fn eprint(msg: &str) {
    unsafe { write_raw(STDERR_FILENO, msg.as_bytes()); }
}

/// 写 &str + 换行到 stderr
pub fn eprintln(msg: &str) {
    unsafe {
        write_raw(STDERR_FILENO, msg.as_bytes());
        write_raw(STDERR_FILENO, b"\n");
    }
}

/// 写 &str + 换行到 stdout（用于 benchmark 结果这种用户必看输出）
pub fn println(msg: &str) {
    unsafe {
        write_raw(STDOUT_FILENO, msg.as_bytes());
        write_raw(STDOUT_FILENO, b"\n");
    }
}

// ── 小工具：把整数 / &[u8] 快速转成十进制/十六进制 ASCII 写到字节缓冲 ──

/// 把 u32 十进制写到 dst 开头，返回写入的字节数
pub fn fmt_u32(mut n: u32, dst: &mut [u8]) -> usize {
    if n == 0 {
        if !dst.is_empty() { dst[0] = b'0'; }
        return 1;
    }
    // 先算位数，再从后往前写到正确位置（不需要移动，不会重叠）
    let mut digits = 0u32;
    let mut tmp = n;
    while tmp > 0 { digits += 1; tmp /= 10; }
    let digits = (digits as usize).min(dst.len());
    for i in (0..digits).rev() {
        dst[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    digits
}

/// 把 usize 十进制写到 dst 开头，返回字节数
pub fn fmt_usize(mut n: usize, dst: &mut [u8]) -> usize {
    if n == 0 {
        if !dst.is_empty() { dst[0] = b'0'; }
        return 1;
    }
    let mut digits = 0usize;
    let mut tmp = n;
    while tmp > 0 { digits += 1; tmp /= 10; }
    let digits = digits.min(dst.len());
    for i in (0..digits).rev() {
        dst[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    digits
}

/// 把 u64 十进制写到 dst 开头，返回字节数
pub fn fmt_u64(mut n: u64, dst: &mut [u8]) -> usize {
    if n == 0 {
        if !dst.is_empty() { dst[0] = b'0'; }
        return 1;
    }
    let mut digits = 0u32;
    let mut tmp = n;
    while tmp > 0 { digits += 1; tmp /= 10; }
    let digits = (digits as usize).min(dst.len());
    for i in (0..digits).rev() {
        dst[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    digits
}

/// 把 f64 以 "整数部分.小数部分(6位)" 写到 dst，返回字节数
pub fn fmt_f64(x: f64, dst: &mut [u8]) -> usize {
    // 只处理非负数（benchmark 都是正数）
    if x.is_nan() || x.is_infinite() {
        let s = if x.is_nan() { b"nan" } else { b"inf" };
        let n = s.len().min(dst.len());
        dst[..n].copy_from_slice(&s[..n]);
        return n;
    }
    let negative = x < 0.0;
    let mut val = if negative { -x } else { x };
    // 先 + 0.5e-6 四舍五入到 6 位小数
    val += 0.0000005;
    let int_part = val as u64;
    let frac_part = ((val - int_part as f64) * 1_000_000.0) as u64;
    let mut off = 0;
    if negative && off < dst.len() { dst[off] = b'-'; off += 1; }
    let mut int_buf = [0u8; 32];
    let n = fmt_u64(int_part, &mut int_buf);
    let copy = n.min(dst.len() - off);
    dst[off..off + copy].copy_from_slice(&int_buf[..copy]);
    off += copy;
    if off < dst.len() { dst[off] = b'.'; off += 1; }
    let mut frac_buf = [0u8; 32];
    let mut fn_ = fmt_u64(frac_part, &mut frac_buf);
    // 左补 0 到 6 位
    if fn_ < 6 {
        let pad = 6 - fn_;
        frac_buf.copy_within(0..fn_, pad);
        for p in &mut frac_buf[..pad] { *p = b'0'; }
        fn_ = 6;
    } else if fn_ > 6 {
        fn_ = 6;
    }
    let copy = fn_.min(dst.len() - off);
    dst[off..off + copy].copy_from_slice(&frac_buf[..copy]);
    off += copy;
    off
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_u32_zero() {
        let mut buf = [0u8; 16];
        assert_eq!(fmt_u32(0, &mut buf), 1);
        assert_eq!(&buf[..1], b"0");
    }
    #[test]
    fn fmt_u32_12345() {
        let mut buf = [0u8; 16];
        let n = fmt_u32(12345, &mut buf);
        assert_eq!(&buf[..n], b"12345");
    }
    #[test]
    fn fmt_usize_big() {
        let mut buf = [0u8; 32];
        let n = fmt_usize(390627, &mut buf);
        assert_eq!(&buf[..n], b"390627");
    }
    #[test]
    fn fmt_f64_basic() {
        let mut buf = [0u8; 32];
        let n = fmt_f64(123.456789123, &mut buf);
        assert_eq!(&buf[..n], b"123.456789"); // 6 位四舍五入
    }
}
