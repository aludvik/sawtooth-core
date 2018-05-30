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

use cpython::{ObjectProtocol, PyObject, Python, NoArgs, PyList, PyDict, PyClone};
use std::collections::{HashSet, VecDeque};
use std::mem;
use std::slice::Iter;
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, SendError, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Instant, Duration};

use execution::execution_platform::ExecutionPlatform;
use execution::py_executor::PyExecutor;
use journal::block_wrapper::BlockWrapper;
use journal::candidate_block::{FinalizeBlockResult, CandidateBlock, CandidateBlockError};
use journal::chain_commit_state::TransactionCommitCache;
use journal::pylock::PyLock;

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

pub enum InitializeBlockError {
    ConsensusNotReady,
    InvalidState,
}

pub enum FinalizeBlockError {
    ConsensusNotReady,
    NoPendingBatchesRemaining,
    InvalidState,
}

pub enum StartError {
    Disconnected,
}

pub struct BlockPublisher {
    transaction_executor: Box<ExecutionPlatform>,
    block_cache: PyObject,
    state_view_factory: PyObject,
    settings_cache: PyObject,
    block_sender: PyObject,
    batch_publisher: PyObject,
    chain_head: Option<BlockWrapper>,
    chain_head_lock: PyLock,
    identity_signer: PyObject,
    data_dir: PyObject,
    config_dir: PyObject,
    permission_verifier: PyObject,
    pub check_publish_block_frequency: Duration,
    batch_observers: Vec<PyObject>,
    batch_injector_factory: PyObject,
    batch_tx: IncomingBatchSender,
    pub batch_rx: IncomingBatchReceiver,

    consensus_factory: PyObject,
    block_wrapper_class: PyObject,
    block_header_class: PyObject,
    block_builder_class: PyObject,
    settings_view_class: PyObject,

    candidate_block: Option<CandidateBlock>,
    pending_batches: PendingBatchesPool,
    publisher_logging_states: PublisherLoggingStates,
    pub exit: Exit,
}

impl BlockPublisher {
    pub fn new(
        transaction_executor: PyObject,
        block_cache: PyObject,
        state_view_factory: PyObject,
        settings_cache: PyObject,
        block_sender: PyObject,
        batch_publisher: PyObject,
        chain_head: Option<BlockWrapper>,
        chain_head_lock: PyObject,
        identity_signer: PyObject,
        data_dir: PyObject,
        config_dir: PyObject,
        permission_verifier: PyObject,
        check_publish_block_frequency: Duration,
        batch_observers: Vec<PyObject>,
        batch_injector_factory: PyObject,
        consensus_factory: PyObject,
        block_wrapper_class: PyObject,
        block_header_class: PyObject,
        block_builder_class: PyObject,
        settings_view_class: PyObject,
    ) -> Arc<Mutex<Self>> {
        let (batch_tx, batch_rx) = make_batch_queue();
        let tep = Box::new(PyExecutor::new(transaction_executor).unwrap());

        Arc::new(Mutex::new(BlockPublisher {
            transaction_executor: tep,
            block_cache,
            state_view_factory,
            settings_cache,
            block_sender,
            batch_publisher,
            chain_head,
            chain_head_lock: PyLock::new(chain_head_lock),
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
            candidate_block: None,
            pending_batches: PendingBatchesPool::new(
                NUM_PUBLISH_COUNT_SAMPLES,
                INITIAL_PUBLISH_COUNT,
            ),
            publisher_logging_states: PublisherLoggingStates::new(),
            exit: Exit::new(),
        }))
    }

    pub fn start(publisher: Arc<Mutex<Self>>) {
        let builder = thread::Builder::new().name("PublisherThread".into());
        builder.spawn(move || {
            let mut now = Instant::now();
            let check_period = {
                publisher.lock().unwrap().check_publish_block_frequency
            };
            loop {
                debug!("loopy");
                { // Receive and process a batch
                    let mut unlocked = publisher.lock().unwrap();
                    match unlocked.batch_rx.get(check_period) {
                        Err(err) => match err {
                            BatchQueueError::Timeout => {
                                if unlocked.exit.get() {
                                    break;
                                } else {
                                    continue;
                                }
                            },
                            err => panic!("Unhandled error: {:?}", err),
                        },
                        Ok(batch) => unlocked.on_batch_received(batch),
                    }
                }
                if now.elapsed() >= check_period {
                    let mut unlocked = publisher.lock().unwrap();
                    unlocked.on_check_publish_block(false);
                    now = Instant::now();
                    if unlocked.exit.get() {
                        break
                    }
                }
            }
        }).unwrap();
    }

    pub fn stop(&self) {
        self.exit.set();
    }

    pub fn batch_sender(&self) -> IncomingBatchSender {
        self.chain_head_lock.lock();
        self.batch_tx.clone()
    }

    pub fn pending_batch_info(&self) -> (i32, i32) {
        self.chain_head_lock.lock();
        (self.pending_batches.len() as i32, self.pending_batches.limit() as i32)
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

    fn load_injectors(&self, py: Python, block_id: &str) -> Vec<PyObject> {
        self.batch_injector_factory
            .call_method(py, "create_injectors", (block_id,), None)
            .expect("BatchInjectorFactory has no method 'create_injectors'")
            .extract::<PyList>(py)
            .unwrap()
            .iter(py)
            .collect()
    }

    fn initialize_block(&mut self, previous_block: &BlockWrapper) -> Result<(), InitializeBlockError> {
        if self.candidate_block.is_some() { return Err(InitializeBlockError::InvalidState); }

        let gil = Python::acquire_gil();
        let py = gil.python();

        let kwargs = PyDict::new(py);
        kwargs.set_item(py, "default_value", 0).unwrap();
        let max_batches = self.settings_cache
            .call_method(
                py,
                "get_setting",
                (
                    "sawtooth_publisher.max_batches_per_block",
                    previous_block.block.state_root_hash.clone(),
                ),
                Some(&kwargs),
            )
            .expect("settings_cache has no method get_setting")
            .extract::<usize>(py)
            .unwrap();

        let state_view = self.get_state_view(py, previous_block);
        let public_key = self.get_public_key(py);
        let consensus = self.load_consensus(
            py,
            previous_block,
            state_view.clone_ref(py),
            public_key.clone());
        let batch_injectors = self.load_injectors(py, &previous_block.block.header_signature);

        let kwargs = PyDict::new(py);
        kwargs.set_item(py, "block_num", previous_block.block.block_num + 1).unwrap();
        kwargs.set_item(py, "previous_block_id", &previous_block.block.header_signature).unwrap();
        kwargs.set_item(py, "signer_public_key", &public_key).unwrap();
        let block_header = self.block_header_class
            .call(py, NoArgs, Some(&kwargs))
            .expect("BlockHeader could not be constructed");

        let block_builder = self.block_builder_class
            .call(py, (block_header,), None)
            .expect("BlockBuilder could not be constructed");

        let consensus_check: bool = consensus
            .call_method(
                py,
                "initialize_block",
                (block_builder.getattr(py, "block_header").unwrap(),),
                None)
            .expect("Call to consensus.initialize_block failed")
            .extract(py)
            .unwrap();

        if !consensus_check {
            return Err(InitializeBlockError::ConsensusNotReady);
        }

        let scheduler = self.transaction_executor
            .create_scheduler(&previous_block.block.state_root_hash)
            .expect("Failed to create new scheduler");

        let block_store = self.block_cache
            .getattr(py, "_block_store")
            .expect("BlockCache has not field _block_store");

        let committed_txn_cache = TransactionCommitCache::new(block_store.clone_ref(py));

        let settings_view = self.settings_view_class
            .call(py, (state_view,), None)
            .expect("SettingsView could not be constructed");

        let mut candidate_block = CandidateBlock::new(
            block_store,
            consensus,
            scheduler,
            committed_txn_cache,
            block_builder,
            max_batches,
            batch_injectors,
            self.identity_signer.clone_ref(py),
            settings_view,
        );

        for batch in self.pending_batches.iter() {
            if candidate_block.can_add_batch() {
                candidate_block.add_batch(batch.clone());
            } else {
                break;
            }
        }
        self.candidate_block = Some(candidate_block);
        Ok(())
    }

    fn finalize_block(&mut self, force: bool) -> Result<FinalizeBlockResult, FinalizeBlockError> {
        let mut option_result = None;
        if let Some(ref mut candidate_block) = &mut self.candidate_block {
            option_result = Some(candidate_block.finalize(force));
        }

        self.candidate_block = None;

        if let Some(result) = option_result {
            match result {
                Ok(finalize_result) => {
                    self.pending_batches.update(
                        finalize_result.remaining_batches.clone(),
                        finalize_result.last_batch.clone());
                    Ok(finalize_result)
                },
                Err(err) => Err(match err {
                    CandidateBlockError::ConsensusNotReady =>
                        FinalizeBlockError::ConsensusNotReady,
                    CandidateBlockError::NoPendingBatchesRemaining =>
                        FinalizeBlockError::NoPendingBatchesRemaining,
                }),
            }
        } else {
            Err(FinalizeBlockError::InvalidState)
        }
    }

    fn publish_block(&mut self, block: PyObject, injected_batches: Vec<String>) {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let block: BlockWrapper = block.extract(py)
            .expect("Got block to publish that wasn't a BlockWrapper");

        let kwargs = PyDict::new(py);
        kwargs.set_item(py, "keep_batches", injected_batches).unwrap();
        self.block_sender.call_method(py, "send", (block,), Some(&kwargs))
            .expect("BlockSender has no method send");

        self.on_chain_updated(None, Vec::new(), Vec::new());
    }

    fn load_consensus(
        &self,
        py: Python,
        block: &BlockWrapper,
        state_view: PyObject,
        public_key: String,
    ) -> PyObject {
        let consensus_block_publisher = self.consensus_factory
            .call_method(py, "get_configured_consensus_module", (block.header_signature(), state_view), None)
            .expect("ConsensusFactory has no method get_configured_consensus_module")
            .call_method(py, "BlockPublisher", (self.block_cache.clone_ref(py), self.state_view_factory.clone_ref(py), self.batch_publisher.clone_ref(py), self.data_dir.clone_ref(py), self.config_dir.clone_ref(py), public_key.clone()), None);
        consensus_block_publisher.unwrap()
    }

    fn get_public_key(&self, py: Python) -> String {
        self.identity_signer
            .call_method(py, "get_public_key", NoArgs, None)
            .expect("IdentitySigner has no method get_public_key")
            .call_method(py, "as_hex", NoArgs, None)
            .expect("PublicKey object as no method as_hex")
            .extract::<String>(py)
            .unwrap()
    }

    fn is_building_block(&self) -> bool {
        self.candidate_block.is_some()
    }

    fn can_build_block(&self) -> bool {
        self.chain_head.is_some() && self.pending_batches.len() > 0
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

    fn cancel_block(&mut self) {
        let mut candidate_block = None;
        mem::swap(&mut self.candidate_block, &mut candidate_block);
        if let Some(mut candidate_block) = candidate_block {
            candidate_block.cancel();
        }
    }

    pub fn on_batch_received(
        &mut self,
        batch: Batch
    ) {
        self.chain_head_lock.lock();
        let gil = Python::acquire_gil();
        let py = gil.python();

        for observer in &self.batch_observers {
            observer
                .call_method(py, "notify_batch_pending", (batch.clone(),), None)
                .expect("BatchObserver has no method notify_batch_pending");
        }
        let permission_check = self.permission_verifier
            .call_method(py, "is_batch_signer_authorized", (batch.clone(),), None)
            .expect("PermissionVerifier has no method is_batch_signer_authorized")
            .extract(py)
            .expect("PermissionVerifier.is_batch_signer_authorized did not return bool");

        if permission_check {
            self.pending_batches.append(batch.clone());
            if let Some(ref mut candidate_block) = self.candidate_block {
                if candidate_block.can_add_batch() {
                    candidate_block.add_batch(batch);
                }
            }
        }
    }

    pub fn on_check_publish_block(&mut self, force: bool) {
        self.chain_head_lock.lock();
        if !self.is_building_block() && self.can_build_block() {
            let chain_head = self.chain_head.clone().unwrap();
            match self.initialize_block(&chain_head) {
                Ok(_) => self.log_consensus_state(true),
                Err(_) => self.log_consensus_state(false),
            }
        }

        if self.is_building_block() {
            if let Ok(result) = self.finalize_block(force) {
                if result.block.is_some() {
                    self.publish_block(result.block.unwrap(), result.injected_batch_ids);
                }
            }
        }
    }

    pub fn on_chain_updated(
        &mut self,
        chain_head: Option<BlockWrapper>,
        committed_batches: Vec<Batch>,
        uncommitted_batches: Vec<Batch>,
    ) {
        self.chain_head_lock.lock();
        if let Some(chain_head) = chain_head {
            info!("Now building on top of block, {}", chain_head);
            self.cancel_block();
            self.pending_batches.update_limit(chain_head.block.batches.len());
            self.pending_batches.rebuild(Some(committed_batches), Some(uncommitted_batches));
            self.chain_head = Some(chain_head);
        } else {
            info!("Block publishing is suspended until new chain head arrives");
            self.cancel_block();
            self.chain_head = None;
        }
    }

    pub fn has_batch(&self, batch_id: &str) -> bool {
        self.chain_head_lock.lock();
        if self.pending_batches.contains(batch_id) {
            return true;
        }
        self.batch_tx.has_batch(batch_id).unwrap_or_else(|_| {
            warn!("In BlockPublisher.has_batch, batchsender.has_batch errored");
            false
        })
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
