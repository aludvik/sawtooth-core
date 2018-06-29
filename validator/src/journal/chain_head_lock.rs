use std::sync::RwLockWriteGuard;

use batch::Batch;
use journal::block_wrapper::BlockWrapper;
use journal::publisher::{BlockPublisher, BlockPublisherState};

/// Abstracts acquiring the lock used by the BlockPublisher without exposing access to the
/// publisher itself.
#[derive(Clone)]
pub struct ChainHeadLock {
    publisher: BlockPublisher,
}

impl ChainHeadLock {
    pub fn new(publisher: BlockPublisher) -> Self {
        ChainHeadLock { publisher }
    }

    pub fn acquire(&self) -> ChainHeadGuard {
        ChainHeadGuard {
            state: self.publisher.state.write().expect("Lock is not poisoned"),
            publisher: self.publisher.clone(),
        }
    }
}

/// RAII type that represents having acquired the lock used by the BlockPublisher
pub struct ChainHeadGuard<'a> {
    state: RwLockWriteGuard<'a, BlockPublisherState>,
    publisher: BlockPublisher,
}

impl<'a> ChainHeadGuard<'a> {
    pub fn notify_on_chain_updated(
        &mut self,
        chain_head: BlockWrapper,
        committed_batches: Vec<Batch>,
        uncommitted_batches: Vec<Batch>,
    ) {
        self.publisher.on_chain_updated_with_state(
            &mut self.state,
            chain_head,
            committed_batches,
            uncommitted_batches,
        )
    }
}
