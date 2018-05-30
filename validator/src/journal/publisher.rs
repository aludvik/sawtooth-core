/*
 * Copyright 2018 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ------------------------------------------------------------------------------
 */

use batch::Batch;
use block::Block;

use cpython;
use cpython::ObjectProtocol;
use cpython::PyObject;
use std::collections::{HashSet, VecDeque};
use std::mem;
use std::slice::Iter;
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, SendError, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};

use execution::execution_platform::ExecutionPlatform;
use execution::py_executor::PyExecutor;
use journal::candidate_block::CandidateBlock;

const NUM_PUBLISH_COUNT_SAMPLES: usize = 5;
const INITIAL_PUBLISH_COUNT: usize = 30;

/// Collects and tracks the changes in various states of the
/// Publisher. For example it tracks `consensus_ready`,
/// which denotes entering or exiting this state.
struct PublisherLoggingStates {
    consensus_ready: bool,
}

impl PublisherLoggingStates {
    fn new() -> Self {
        PublisherLoggingStates {
            consensus_ready: true,
        }
    }
}

pub struct BlockPublisher {
    transaction_executor: Box<ExecutionPlatform>,
    block_cache: PyObject,
    state_view_factory: PyObject,
    settings_cache: PyObject,
    block_sender: PyObject,
    batch_sender: PyObject,
    chain_head: PyObject,
    chain_head_lock: PyObject,
    identity_signer: PyObject,
    data_dir: PyObject,
    config_dir: PyObject,
    permission_verifier: PyObject,
    check_publish_block_frequency: u64,
    batch_observers: Vec<PyObject>,
    batch_injector_factory: PyObject,
    batch_tx: IncomingBatchSender,
    batch_rx: IncomingBatchReceiver,

    consensus_factory: PyObject,
    block_wrapper_class: PyObject,
    block_header_class: PyObject,
    block_builder_class: PyObject,
    settings_view_class: PyObject,

    candidate_block: Arc<Mutex<Option<CandidateBlock>>>,
    publisher_chain_head: Arc<Mutex<Option<Block>>>,
    pending_batches: PendingBatchesPool,
    publisher_logging_states: PublisherLoggingStates,
}

impl BlockPublisher {
    pub fn new(
        transaction_executor: PyObject,
        block_cache: PyObject,
        state_view_factory: PyObject,
        settings_cache: PyObject,
        block_sender: PyObject,
        batch_sender: PyObject,
        chain_head: PyObject,
        chain_head_lock: PyObject,
        identity_signer: PyObject,
        data_dir: PyObject,
        config_dir: PyObject,
        permission_verifier: PyObject,
        check_publish_block_frequency: u64,
        batch_observers: Vec<PyObject>,
        batch_injector_factory: PyObject,
        consensus_factory: PyObject,
        block_wrapper_class: PyObject,
        block_header_class: PyObject,
        block_builder_class: PyObject,
        settings_view_class: PyObject,
    ) -> Self {
        let (batch_tx, batch_rx) = make_batch_queue();
        let tep = Box::new(PyExecutor::new(transaction_executor).unwrap());

        BlockPublisher {
            transaction_executor: tep,
            block_cache,
            state_view_factory,
            settings_cache,
            block_sender,
            batch_sender,
            chain_head,
            chain_head_lock,
            identity_signer,
            data_dir,
            config_dir,
            permission_verifier,
            check_publish_block_frequency,
            batch_observers,
            batch_injector_factory,
            batch_tx,
            batch_rx,
            consensus_factory,
            block_wrapper_class,
            block_header_class,
            block_builder_class,
            settings_view_class,
            candidate_block: Arc::new(Mutex::new(None)),
            publisher_chain_head: Arc::new(Mutex::new(None)),
            pending_batches: PendingBatchesPool::new(
                NUM_PUBLISH_COUNT_SAMPLES,
                INITIAL_PUBLISH_COUNT,
            ),
            publisher_logging_states: PublisherLoggingStates::new(),
        }
    }

    pub fn start(&self) {
        unimplemented!()
    }

    pub fn stop(&self) {
        unimplemented!();
    }

    pub fn batch_sender(&self) -> IncomingBatchSender {
        self.batch_tx.clone()
    }

    pub fn pending_batch_info(&self) -> (usize, usize) {
        unimplemented!();
    }

    fn get_state_view(&self, py: Python, previous_block: &BlockWrapper) -> PyObject {
        self.block_wrapper_class
            .call_method(
                py,
                "state_view_for_block",
                (previous_block, &self.state_view_factory),
                None,
            )
            .expect("BlockWrapper, unable to call state_view_for_block")
    }

    fn load_injectors(&self, py: cpython::Python, block_id: &str) -> Vec<PyObject> {
        self.batch_injector_factory
            .call_method(py, "create_injectors", (block_id,), None)
            .expect("BatchInjectorFactory has no method 'create_injectors'")
            .extract::<cpython::PyList>(py)
            .unwrap()
            .iter(py)
            .collect()
    }

    fn load_consensus(
        &self,
        py: cpython::Python,
        block: Block,
        state_view: cpython::PyObject,
        public_key: String,
    ) -> cpython::PyObject {
        self.consensus_factory
            .call_method(py, "get_configured_consensus_module", NoArgs, None)
            .expect("ConsensusFactory has no method get_configured_consensus_module")
    }

    fn get_public_key(&self, py: cpython::Python) -> String {
        self.identity_signer
            .call_method(py, "get_public_key", cpython::NoArgs, None)
            .expect("IdentitySigner has no method get_public_key")
            .call_method(py, "as_hex", cpython::NoArgs, None)
            .expect("PublicKey object as no method as_hex")
            .extract::<String>(py)
            .unwrap()
    }

    fn is_building_block(&self) -> bool {
        if let Ok(candidate_block) = self.candidate_block.lock() {
            (*candidate_block).is_some()
        } else {
            warn!("BlockPublisher Candidate block lock is poisoned");
            false
        }
    }

    fn can_build_block(&self) -> bool {
        if let Ok(chain_head) = self.publisher_chain_head.lock() {
            (*chain_head).is_some() && self.pending_batches.len() > 0
        } else {
            warn!("BlockPublisher chain_head lock is poisoned!");
            false
        }
    }

    fn log_consensus_state(&mut self, ready: bool) {
        if ready && !self.publisher_logging_states.consensus_ready {
            self.publisher_logging_states.consensus_ready = true;
            debug!("Consensus is ready to build candidate block");
        } else {
            self.publisher_logging_states.consensus_ready = false;
            debug!("Consensus not ready to build candidate block");
        }
    }

    fn cancel_block(&self) {
        if let Ok(mut candidate_block) = self.candidate_block.lock() {
            if let Some(ref mut candidate_block) = *candidate_block {
                candidate_block.cancel();
            }
        } else {
            warn!("Block Publisher, candidate block lock is poisoned");
        }
    }

    pub fn on_chain_updated(
        &self,
        chain_head: PyObject,
        committed_batches: Vec<Batch>,
        uncommitted_batches: Vec<Batch>,
    ) {
        unimplemented!()
    }

    pub fn has_batch(&self, batch_id: &str) -> bool {
        if let Ok(_) = self.publisher_chain_head.lock() {
            if self.pending_batches.contains(batch_id) {
                return true;
            }
            self.batch_tx.has_batch(batch_id).unwrap_or_else(|_| {
                warn!("In BlockPublisher.has_batch, batchsender.has_batch errored");
                false
            })
        } else {
            warn!("BlockPublisher, chain_head_lock is poisoned");
            false
        }
    }
}

/// This queue keeps track of the batch ids so that components on the edge
/// can filter out duplicates early. However, there is still an opportunity for
/// duplicates to make it into this queue, which is intentional to avoid
/// blocking threads trying to put/get from the queue. Any duplicates
/// introduced by this must be filtered out later.
pub fn make_batch_queue() -> (IncomingBatchSender, IncomingBatchReceiver) {
    let (sender, reciever) = channel();
    let ids = Arc::new(Mutex::new(HashSet::new()));
    (
        IncomingBatchSender::new(ids.clone(), sender),
        IncomingBatchReceiver::new(ids, reciever),
    )
}

pub struct IncomingBatchReceiver {
    ids: Arc<Mutex<HashSet<String>>>,
    receiver: Receiver<Batch>,
}

impl IncomingBatchReceiver {
    pub fn new(
        ids: Arc<Mutex<HashSet<String>>>,
        receiver: Receiver<Batch>,
    ) -> IncomingBatchReceiver {
        IncomingBatchReceiver { ids, receiver }
    }

    pub fn get(&mut self, timeout: Duration) -> Result<Batch, BatchQueueError> {
        let batch = self.receiver.recv_timeout(timeout)?;
        self.ids.lock()?.remove(&batch.header_signature);
        Ok(batch)
    }
}

#[derive(Clone)]
pub struct IncomingBatchSender {
    ids: Arc<Mutex<HashSet<String>>>,
    sender: Sender<Batch>,
}

impl IncomingBatchSender {
    pub fn new(ids: Arc<Mutex<HashSet<String>>>, sender: Sender<Batch>) -> IncomingBatchSender {
        IncomingBatchSender { ids, sender }
    }
    pub fn put(&mut self, batch: Batch) -> Result<(), BatchQueueError> {
        if !self.ids.lock()?.contains(&batch.header_signature) {
            self.ids.lock()?.insert(batch.header_signature.clone());
            self.sender.send(batch).map_err(BatchQueueError::from)
        } else {
            Ok(())
        }
    }

    pub fn has_batch(&self, batch_id: &str) -> Result<bool, BatchQueueError> {
        Ok(self.ids.lock()?.contains(batch_id))
    }
}

#[derive(Debug)]
pub enum BatchQueueError {
    SenderError(SendError<Batch>),
    Timeout,
    MutexPoisonError(String),
}

impl From<SendError<Batch>> for BatchQueueError {
    fn from(e: SendError<Batch>) -> Self {
        BatchQueueError::SenderError(e)
    }
}

impl From<RecvTimeoutError> for BatchQueueError {
    fn from(_: RecvTimeoutError) -> Self {
        BatchQueueError::Timeout
    }
}

impl<'a> From<PoisonError<MutexGuard<'a, HashSet<String>>>> for BatchQueueError {
    fn from(_e: PoisonError<MutexGuard<HashSet<String>>>) -> Self {
        BatchQueueError::MutexPoisonError("Muxtex Poisoned".into())
    }
}

/// Ordered batches waiting to be processed
pub struct PendingBatchesPool {
    batches: Vec<Batch>,
    ids: HashSet<String>,
    limit: QueueLimit,
}

impl PendingBatchesPool {
    pub fn new(sample_size: usize, initial_value: usize) -> PendingBatchesPool {
        PendingBatchesPool {
            batches: Vec::new(),
            ids: HashSet::new(),
            limit: QueueLimit::new(sample_size, initial_value),
        }
    }

    pub fn len(&self) -> usize {
        self.batches.len()
    }

    pub fn iter(&self) -> Iter<Batch> {
        self.batches.iter()
    }

    fn contains(&self, id: &str) -> bool {
        self.ids.contains(id)
    }

    fn reset(&mut self) {
        self.batches = Vec::new();
        self.ids = HashSet::new();
    }

    pub fn append(&mut self, batch: Batch) {
        if !self.contains(&batch.header_signature) {
            self.ids.insert(batch.header_signature.clone());
            self.batches.push(batch);
        }
    }

    /// Recomputes the list of pending batches
    ///
    /// Args:
    ///   committed (List<Batches>): Batches committed in the current chain
    ///   since the root of the fork switching from.
    ///   uncommitted (List<Batches): Batches that were committed in the old
    ///   fork since the common root.
    pub fn rebuild(&mut self, committed: Option<Vec<Batch>>, uncommitted: Option<Vec<Batch>>) {
        let committed_set = if let Some(committed) = committed {
            committed
                .iter()
                .map(|i| i.header_signature.clone())
                .collect::<HashSet<String>>()
        } else {
            HashSet::new()
        };

        let previous_batches = self.batches.clone();

        self.reset();

        // Uncommitted and pending are disjoint sets since batches can only be
        // committed to a chain once.

        if let Some(batch_list) = uncommitted {
            for batch in batch_list {
                if !committed_set.contains(&batch.header_signature) {
                    self.append(batch);
                }
            }
        }

        for batch in previous_batches {
            if !committed_set.contains(&batch.header_signature) {
                self.append(batch);
            }
        }
    }

    pub fn update(&mut self, mut still_pending: Vec<Batch>, last_sent: Batch) {
        let last_index = self.batches
            .iter()
            .position(|i| i.header_signature == last_sent.header_signature);

        let unsent = if let Some(idx) = last_index {
            let mut unsent = vec![];
            mem::swap(&mut unsent, &mut self.batches);
            still_pending.extend_from_slice(unsent.split_off(idx + 1).as_slice());
            still_pending
        } else {
            let mut unsent = vec![];
            mem::swap(&mut unsent, &mut self.batches);
            unsent
        };

        self.reset();

        for batch in unsent {
            self.append(batch);
        }
    }

    pub fn update_limit(&mut self, consumed: usize) {
        self.limit.update(self.batches.len(), consumed);
    }

    pub fn limit(&self) -> usize {
        self.limit.get()
    }
}

struct RollingAverage {
    samples: VecDeque<usize>,
    current_average: usize,
}

impl RollingAverage {
    pub fn new(sample_size: usize, initial_value: usize) -> RollingAverage {
        let mut samples = VecDeque::with_capacity(sample_size);
        samples.push_back(initial_value);

        RollingAverage {
            samples,
            current_average: initial_value,
        }
    }

    pub fn value(&self) -> usize {
        self.current_average
    }

    /// Add the sample and return the updated average.
    pub fn update(&mut self, sample: usize) -> usize {
        self.samples.push_back(sample);
        self.current_average = self.samples.iter().sum::<usize>() / self.samples.len();
        self.current_average
    }
}

struct QueueLimit {
    avg: RollingAverage,
}

impl QueueLimit {
    pub fn new(sample_size: usize, initial_value: usize) -> QueueLimit {
        QueueLimit {
            avg: RollingAverage::new(sample_size, initial_value),
        }
    }

    /// Use the current queue size and the number of items consumed to
    /// update the queue limit, if there was a significant enough change.
    /// Args:
    ///     queue_length (int): the current size of the queue
    ///     consumed (int): the number items consumed
    pub fn update(&mut self, queue_length: usize, consumed: usize) {
        if consumed > 0 {
            // Only update the average if either:
            // a. Not drained below the current average
            // b. Drained the queue, but the queue was not bigger than the
            //    current running average

            let remainder = queue_length - consumed;

            if remainder > self.avg.value() || consumed > self.avg.value() {
                self.avg.update(consumed);
            }
        }
    }

    pub fn get(&self) -> usize {
        // Limit the number of items to 2 times the publishing average.  This
        // allows the queue to grow geometrically, if the queue is drained.
        2 * self.avg.value()
    }
}

/// Utility class for signaling that a background thread should shutdown
#[derive(Default)]
pub struct Exit {
    flag: Mutex<AtomicBool>,
}

impl Exit {
    pub fn new() -> Self {
        Exit {
            flag: Mutex::new(AtomicBool::new(false)),
        }
    }

    pub fn get(&self) -> bool {
        self.flag.lock().unwrap().load(Ordering::Relaxed)
    }

    pub fn set(&self) {
        self.flag.lock().unwrap().store(true, Ordering::Relaxed);
    }
}
