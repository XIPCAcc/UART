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
            let name = &self.token.name;
            eprintln!("[TRACE] async_wait: {} already pending (seq={} consumed={}), Ready", name, seq, consumed);
            return Poll::Ready(Ok(()));
        }

        let waker = cx.waker().clone();
        *self.token.inner.waker.lock().unwrap() = Some(waker);
        let name = &self.token.name;
        eprintln!("[TRACE] async_wait: {} registered waker, sleeping until UINTR...", name);

        let seq = self.token.inner.seq.load(Ordering::Acquire);
        let consumed = self.token.inner.consumed_seq.load(Ordering::Acquire);
        if seq != consumed {
            self.token.inner.consumed_seq.store(seq, Ordering::Release);
            eprintln!("[TRACE] async_wait: {} race: interrupt arrived before sleep (seq={}), Ready", name, seq);
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
