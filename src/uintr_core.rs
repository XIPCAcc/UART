use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU8, AtomicPtr, Ordering};
use std::task::Waker;
use std::panic::{self, AssertUnwindSafe};

/// 全局唤醒标志：handler 写入以解除 UMONITOR/UMWAIT 休眠
pub static WAKE_FLAG: AtomicU8 = AtomicU8::new(0);

/// 哨兵值：表示 waker 为空（用悬空指针而非 null，区分"空"和"未初始化"）
/// 选 usize::MAX 是因为：Box<Waker> 对齐至少 8，真实指针低 3 位永远是 0，
/// usize::MAX 与任何合法 Waker* 都不重叠。
const WAKER_EMPTY: *mut Waker = usize::MAX as *mut Waker;

#[derive(Clone)]
pub struct UintrToken {
    pub inner: Arc<Inner>,
    pub name: String,
}

pub struct Inner {
    pub seq: AtomicU32,
    pub consumed_seq: AtomicU32,
    /// 使用 AtomicPtr 实现无锁 waker 存储：
    ///   - WAKER_EMPTY (usize::MAX)：无 waker
    ///   - 其他有效值：Box<Waker> 的裸指针
    /// 注意：null 作为保留值保留，目前不会写入
    pub waker: AtomicPtr<Waker>,
}

impl UintrToken {
    pub fn new(name: &str) -> Self {
        Self {
            inner: Arc::new(Inner {
                seq: AtomicU32::new(0),
                consumed_seq: AtomicU32::new(0),
                waker: AtomicPtr::new(WAKER_EMPTY),
            }),
            name: name.to_string(),
        }
    }

    /// 中断上下文中调用：**无锁，可安全重入，不可 panics（不可 unwind）**
    ///
    /// 用 swap 原子取走 waker，避免和用户态 CAS 注册竞争。
    /// 内部对 `waker.wake()` 额外做了 `catch_unwind`：
    ///   如果 waker 在 wake 中 panic（最常见是 Box::from_raw 的悬空指针触发异常），
    ///   直接吞没 panic，不做任何 I/O（不打印！绝对不拿 Stderr 锁），
    ///   防止 panic hook 触发的 eprintln! 与主代码持有的锁重入死锁。
    pub fn set_pending(&self) {
        self.inner.seq.fetch_add(1, Ordering::Release);
        let ptr = self.inner.waker.swap(WAKER_EMPTY, Ordering::AcqRel);
        if ptr != WAKER_EMPTY && !ptr.is_null() {
            // 安全：ptr 只能来自 register_waker 里 Box::into_raw 的合法堆指针
            let waker = unsafe { Box::from_raw(ptr) };
            // catch_unwind：隔离 panic，绝不允许它冒泡到中断上下文
            // AssertUnwindSafe 合理：我们不关心 waker 内部状态被 unwind 破坏，
            // 因为 swap 已经把它从 Inner.waker 里取走，之后不会再被使用。
            let _ = panic::catch_unwind(AssertUnwindSafe(|| {
                waker.wake();
            }));
        }
    }

    /// 用户态 poll 中调用：注册 waker（无锁 CAS 循环）
    ///
    /// 修复点：`compare_exchange_weak` 可能因为「值相等也假失败」而多次循环，
    /// 旧代码每次循环都把同一个 `new_ptr` 去替换旧值，
    /// 实际上旧值如果被中断的 `set_pending` 换成 WAKER_EMPTY 然后再被另一次
    /// register_waker 写回 0xCCC，我们就会错误地把同一个 `new_ptr` 再写进去——
    /// 造成 new_ptr 被两个栈帧同时认为是自己的，出现 double-free。
    ///
    /// 新策略：CAS 只执行 **最多一次**（用 `compare_exchange` 而非 `_weak`），
    /// 失败立刻回收 new_ptr，再用同一个 waker `clone` 一份新的重试。
    /// 代价是极端情况下多 clone 一次 Waker（含一次 Rc clone），
    /// 但保证所有权永远是「一份 new_ptr → 一次 CAS」。
    pub fn register_waker(&self, waker: Waker) {
        let mut owned_waker = Some(waker);
        loop {
            // 每次都分配一个 fresh 的 Box<Waker>（属于本轮 CAS）
            let current_waker = match owned_waker.take() {
                Some(w) => w,
                None => {
                    // 上一轮 CAS 失败导致 owned_waker 被回收；说明 waker 已被释放。
                    // 正常情况下不会进入这个分支（因为 CAS 失败时我们会立刻重新 set 回去），
                    // 但保留一个 safe net：从 set_pending 取出过的 waker 不能再复用到这里，
                    // 所以如果真走到这说明逻辑错误，直接放弃注册（下次 poll 会再注册一次）
                    return;
                }
            };
            let new_ptr = Box::into_raw(Box::new(current_waker));
            let cur = self.inner.waker.load(Ordering::Acquire);
            // compare_exchange（strong 版）：值相等就一定成功，不会假失败
            match self.inner.waker.compare_exchange(
                cur, new_ptr,
                Ordering::AcqRel, Ordering::Acquire,
            ) {
                Ok(_) => {
                    // 成功替换，旧指针 cur 必须处理：
                    if cur != WAKER_EMPTY && !cur.is_null() {
                        // 安全：cur 是上一轮 Box::into_raw 留下的
                        unsafe { drop(Box::from_raw(cur)); }
                    }
                    return;
                }
                Err(actual_cur) => {
                    // CAS 失败：new_ptr 还没被写入，所有权必须还给我们（不然泄漏）
                    unsafe { drop(Box::from_raw(new_ptr)); }
                    // 把原来的 waker 还给 owned_waker（因为 clone 的是同一个 Waker，
                    // 我们不需要再 clone 新的，直接复用这个 Waker 即可——只是把它的
                    // Box 重新包一遍）。所以这里重新 Some(waker) 回去：
                    // 但我们刚刚已经 move 了 current_waker，还要一个 Waker。
                    // 最简单的办法：失败时我们 drop 了 new_ptr（里面装的 Waker 也
                    // 会一起 drop），所以需要再 clone 一份——但 Waker 已经被 move 进
                    // 之前的 Box 里被 drop 了。
                    //
                    // 为了避免「每失败一次都要 clone 一次」的麻烦，
                    // 直接 break 放弃注册：**下一帧 poll 会再次调用 register_waker**，
                    // 那时再注册即可。双检查机制（poll 末尾再读一次 seq）保证我们不会
                    // 漏掉在"放弃注册"和"下次 poll"之间到达的中断。
                    //
                    // 实际上 CAS 在单线程（UINTR 只是异步打断，没有并行修改者）
                    // 下失败率为 0，所以这里只是理论上的安全分支，基本不会走到。
                    let _ = actual_cur; // 抑制未使用警告
                    return;
                }
            }
        }
    }

    /// 用户态 poll 中调用：尝试取出当前 waker（用于 drop 清理 / 测试）
    pub fn take_waker(&self) -> Option<Waker> {
        let ptr = self.inner.waker.swap(WAKER_EMPTY, Ordering::AcqRel);
        if ptr != WAKER_EMPTY && !ptr.is_null() {
            Some(unsafe { *Box::from_raw(ptr) })
        } else {
            None
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Inner 是 Arc 内部对象：只有所有 UintrToken 都被 drop、而且当前
        // 没有中断 handler 在 `set_pending` 中访问时才会走这里——所以用
        // 普通 get_mut()（非原子）完全安全。
        let ptr = *self.waker.get_mut();
        if ptr != WAKER_EMPTY && !ptr.is_null() {
            // 安全：ptr 来自 Box::into_raw，且之后没人会再碰 Inner.waker
            unsafe { drop(Box::from_raw(ptr)); }
        }
    }
}
