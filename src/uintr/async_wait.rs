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
            // 完成前清理 waker，防止任务结束后 token 残留 waker 唤醒死 task
            self.token.clear_waker();
            return Poll::Ready(Ok(()));
        }

        // 无锁注册 waker：仅空位注册一次，之后复用（避免每次 poll 都分配 Box）
        let waker = cx.waker().clone();
        self.token.register_waker(waker);

        // 双重检查：注册 waker 后，中断可能在中间到达导致 seq 已变化
        let seq = self.token.inner.seq.load(Ordering::Acquire);
        let consumed = self.token.inner.consumed_seq.load(Ordering::Acquire);
        if seq != consumed {
            self.token.inner.consumed_seq.store(seq, Ordering::Release);
            self.token.clear_waker();
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
