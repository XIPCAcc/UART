// 单线程本地通道 — 支持 async recv，自动关闭
//
// 用于协程间传递数据。Sender 在发送时唤醒 Receiver 的 Waker，
// Receiver 实现 Future<Output = Option<T>>，channel 关闭时返回 None。

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    queue: RefCell<VecDeque<T>>,
    waker: RefCell<Option<Waker>>,
    sender_count: Cell<usize>,
}

pub struct LocalSender<T> {
    inner: Rc<Inner<T>>,
}

pub struct LocalReceiver<T> {
    inner: Rc<Inner<T>>,
}

impl<T> Clone for LocalReceiver<T> {
    fn clone(&self) -> Self {
        LocalReceiver {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Clone for LocalSender<T> {
    fn clone(&self) -> Self {
        self.inner
            .sender_count
            .set(self.inner.sender_count.get() + 1);
        LocalSender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for LocalSender<T> {
    fn drop(&mut self) {
        let count = self.inner.sender_count.get();
        if count <= 1 {
            self.inner.sender_count.set(0);
            if let Some(waker) = self.inner.waker.borrow_mut().take() {
                waker.wake();
            }
        } else {
            self.inner.sender_count.set(count - 1);
        }
    }
}

pub fn local_channel<T>() -> (LocalSender<T>, LocalReceiver<T>) {
    let inner = Rc::new(Inner {
        queue: RefCell::new(VecDeque::new()),
        waker: RefCell::new(None),
        sender_count: Cell::new(1),
    });
    (
        LocalSender {
            inner: inner.clone(),
        },
        LocalReceiver { inner },
    )
}

impl<T> LocalSender<T> {
    pub fn send(&self, item: T) {
        self.inner.queue.borrow_mut().push_back(item);
        if let Some(waker) = self.inner.waker.borrow_mut().take() {
            eprintln!("[DEBUG] channel: send waking receiver");
            waker.wake();
        }
    }
}

impl<T> Future for LocalReceiver<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let Some(item) = self.inner.queue.borrow_mut().pop_front() {
            eprintln!("[DEBUG] channel: recv poll -> Ready(Some)");
            Poll::Ready(Some(item))
        } else if self.inner.sender_count.get() == 0 {
            eprintln!("[DEBUG] channel: recv poll -> Ready(None) (all senders dropped)");
            Poll::Ready(None)
        } else {
            eprintln!("[DEBUG] channel: recv poll -> Pending (no data, {} senders alive)", self.inner.sender_count.get());
            *self.inner.waker.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}