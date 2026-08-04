use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::Waker;

#[derive(Clone)]
pub struct UintrToken {
    pub inner: Arc<Inner>,
    pub name: String,
}

pub struct Inner {
    pub seq: AtomicU32,
    pub consumed_seq: AtomicU32,
    pub waker: Mutex<Option<Waker>>,
}

impl UintrToken {
    pub fn new(name: &str) -> Self {
        Self {
            inner: Arc::new(Inner {
                seq: AtomicU32::new(0),
                consumed_seq: AtomicU32::new(0),
                waker: Mutex::new(None),
            }),
            name: name.to_string(),
        }
    }

    pub fn set_pending(&self) {
        self.inner.seq.fetch_add(1, Ordering::Release);
        if let Some(waker) = self.inner.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}
