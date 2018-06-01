use std::sync::{Arc, Mutex, MutexGuard};

use batch::Batch;
use journal::block_wrapper::BlockWrapper;
use journal::publisher::BlockPublisher;

/// Abstracts acquiring the lock used by the BlockPublisher without exposing access to the
/// publisher itself.
pub struct ChainHeadLock {
    publisher: Arc<Mutex<BlockPublisher>>,
}

impl ChainHeadLock {
    pub fn new(publisher: Arc<Mutex<BlockPublisher>>) -> Self {
        ChainHeadLock { publisher }
    }

    pub fn lock(&self) -> ChainHeadGuard  {
        eprintln!("locking chain head");
        ChainHeadGuard { guard: self.publisher.lock().unwrap() }
    }
}

/// RAII type that represents having acquired the lock used by the BlockPublisher
pub struct ChainHeadGuard<'a> {
    guard: MutexGuard<'a, BlockPublisher>,
}

impl<'a> Drop for ChainHeadGuard<'a> {
    fn drop(&mut self) {
        eprintln!("releasing chain head");
    }
}

impl<'a> ChainHeadGuard<'a> {
    pub fn on_chain_updated(
        &mut self,
        chain_head: Option<BlockWrapper>,
        committed_batches: Vec<Batch>,
        uncommitted_batches: Vec<Batch>,
    ) {
        (*self.guard).on_chain_updated(chain_head, committed_batches, uncommitted_batches)
    }
}
