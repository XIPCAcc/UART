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
    /// 复用 waker：不取走、不 drop，用 `wake_by_ref` 唤醒。
    /// waker 一经 register_waker 注册，在 clear_waker 或 token drop 前保持有效，
    /// 因此这里的 `&*ptr` 不会悬空。相比旧的「swap 取走 + wake 消费」方案，
    /// 省去每次唤醒的 swap + Box::from_raw + Rc drop，减轻高频唤醒路径。
    pub fn set_pending(&self) {
        self.inner.seq.fetch_add(1, Ordering::Release);
        let ptr = self.inner.waker.load(Ordering::Acquire);
        if ptr != WAKER_EMPTY && !ptr.is_null() {
            // 安全：ptr 来自 register_waker 的 Box::into_raw，且在 clear_waker 前有效
            let waker = unsafe { &*ptr };
            // catch_unwind：隔离 panic，绝不允许它冒泡到中断上下文
            let _ = panic::catch_unwind(AssertUnwindSafe(|| {
                waker.wake_by_ref();
            }));
        }
    }

    /// 用户态 poll 中调用：注册 waker（仅空位注册一次，之后复用）
    ///
    /// 单 task 场景下，同一 token 只被同一 task 反复注册，waker 不变。
    /// 因此采用「惰性一次性注册」：首次把 waker 写入空位，之后直接复用，
    /// 不再做任何 Box 分配或 CAS。配合 [set_pending] 的 `wake_by_ref`，
    /// waker 一经注册在 clear_waker 之前保持有效，杜绝 use-after-free。
    pub fn register_waker(&self, waker: Waker) {
        // 快速路径：已有 waker，直接复用（传入的 clone 在返回时 drop）
        if self.inner.waker.load(Ordering::Acquire) != WAKER_EMPTY {
            return;
        }
        let new_ptr = Box::into_raw(Box::new(waker));
        // 仅在空位写入一次；若期间被并发写入（理论上单 task 不会发生），
        // 回收刚分配的 Box，继续复用已有 waker。
        if self
            .inner
            .waker
            .compare_exchange(WAKER_EMPTY, new_ptr, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // 安全：new_ptr 是本函数刚 Box::into_raw 的，尚未被任何一方取走
            unsafe { drop(Box::from_raw(new_ptr)); }
        }
    }

    /// 用户态 poll 返回 Ready 前调用：清理 waker。
    ///
    /// 任务完成（future Ready）后，token 里若残留 waker，对端后续再发中断会
    /// 唤醒已结束的 task 导致「async fn resumed after completion」panic。
    /// 因此在每次 Ready 前把 waker 从 token 取走并释放。
    pub fn clear_waker(&self) {
        let ptr = self.inner.waker.swap(WAKER_EMPTY, Ordering::AcqRel);
        if ptr != WAKER_EMPTY && !ptr.is_null() {
            // 安全：ptr 来自 register_waker 的 Box::into_raw
            unsafe { drop(Box::from_raw(ptr)); }
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
