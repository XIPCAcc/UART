use std::task::{Context, Poll};
use std::pin::Pin;
use std::future::Future;
use std::sync::atomic::Ordering;

use super::{UintrError, UintrResult};
use crate::uintr_core::UintrToken;

pub struct UintrFuture {
    token: UintrToken,
}

impl Future for UintrFuture {
    type Output = UintrResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let seq = self.token.inner.seq.load(Ordering::Acquire);
        let consumed = self.token.inner.consumed_seq.load(Ordering::Acquire);

        if seq != consumed {
            self.token.inner.consumed_seq.store(seq, Ordering::Release);
            return Poll::Ready(Ok(()));
        }

        // 无锁注册 waker：使用 AtomicPtr CAS 替换原 Mutex<Option<Waker>>
        // 关键：用户态中断可以在任何指令边界打断，所以这里绝对不能用锁
        let waker = cx.waker().clone();
        self.token.register_waker(waker);

        // 双重检查：注册 waker 后，中断可能在中间到达导致 seq 已变化
        // 此时 set_pending 已经取走旧 waker 并 wake 过了，
        // 我们刚注册的新 waker 不会被旧的 set_pending 调用，所以必须手动再检查一次
        let seq = self.token.inner.seq.load(Ordering::Acquire);
        let consumed = self.token.inner.consumed_seq.load(Ordering::Acquire);
        if seq != consumed {
            self.token.inner.consumed_seq.store(seq, Ordering::Release);
            return Poll::Ready(Ok(()));
        }

        Poll::Pending
    }
}

pub async fn uintr(token: UintrToken) -> UintrResult<()> {
    UintrFuture { token }.await
}

static mut TOKEN: Option<UintrToken> = None;

pub fn get_token() -> UintrResult<UintrToken> {
    unsafe {
        (&raw const TOKEN).as_ref().unwrap().clone().ok_or(UintrError::NotInitialized)
    }
}

pub fn init_token(name: &str) -> UintrToken {
    let token = UintrToken::new(name);
    unsafe {
        TOKEN = Some(token.clone());
    }
    token
}

pub async fn uintr_wait() -> UintrResult<()> {
    let token = get_token()?;
    uintr(token).await
}
