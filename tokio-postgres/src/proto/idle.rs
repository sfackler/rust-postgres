use futures::task::AtomicTask;
use futures::{Async, Poll};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::Error;

struct Inner {
    active: AtomicUsize,
    task: AtomicTask,
}

pub struct IdleState(Arc<Inner>);

impl IdleState {
    pub fn new() -> IdleState {
        IdleState(Arc::new(Inner {
            active: AtomicUsize::new(0),
            task: AtomicTask::new(),
        }))
    }

    pub fn guard(&self) -> IdleGuard {
        self.0.active.fetch_add(1, Ordering::SeqCst);
        IdleGuard(self.0.clone())
    }

    pub fn poll_idle(&self) -> Poll<(), Error> {
        self.0.task.register();

        if self.0.active.load(Ordering::SeqCst) == 0 {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

pub struct IdleGuard(Arc<Inner>);

impl Drop for IdleGuard {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.task.notify();
        }
    }
}
