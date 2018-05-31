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
use py_ffi;
use std::os::raw::{c_char, c_void};
use std::ffi::CStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cpython::{PyObject, PyList, Python, PyClone};

use batch::Batch;
use journal::publisher::{BlockPublisher, IncomingBatchSender};

#[repr(u32)]
#[derive(Debug)]
pub enum ErrorCode {
    Success = 0,
    NullPointerProvided = 0x01,
    InvalidInput = 0x02,
}

macro_rules! check_null {
    ($($arg:expr) , *) => {
        $(if $arg.is_null() { return ErrorCode::NullPointerProvided; })*
    }
}

#[no_mangle]
pub extern "C" fn block_publisher_new(
    transaction_executor_ptr: *mut py_ffi::PyObject,
    block_cache_ptr: *mut py_ffi::PyObject,
    state_view_factory_ptr: *mut py_ffi::PyObject,
    settings_cache_ptr: *mut py_ffi::PyObject,
    block_sender_ptr: *mut py_ffi::PyObject,
    batch_sender_ptr: *mut py_ffi::PyObject,
    chain_head_ptr: *mut py_ffi::PyObject,
    chain_head_lock_ptr: *mut py_ffi::PyObject,
    identity_signer_ptr: *mut py_ffi::PyObject,
    data_dir_ptr: *mut py_ffi::PyObject,
    config_dir_ptr: *mut py_ffi::PyObject,
    permission_verifier_ptr: *mut py_ffi::PyObject,
    check_publish_block_frequency_ptr: *mut py_ffi::PyObject,
    batch_observers_ptr: *mut py_ffi::PyObject,
    batch_injector_factory_ptr: *mut py_ffi::PyObject,
    block_publisher_ptr: *mut *const c_void,
) -> ErrorCode {
    check_null!(
        transaction_executor_ptr,
        block_cache_ptr,
        state_view_factory_ptr,
        settings_cache_ptr,
        block_sender_ptr,
        batch_sender_ptr,
        chain_head_ptr,
        chain_head_lock_ptr,
        identity_signer_ptr,
        data_dir_ptr,
        config_dir_ptr,
        permission_verifier_ptr,
        check_publish_block_frequency_ptr,
        batch_observers_ptr,
        batch_injector_factory_ptr
    );

    let py = unsafe { Python::assume_gil_acquired() };

    let transaction_executor = unsafe { PyObject::from_borrowed_ptr(py, transaction_executor_ptr) };
    let block_cache = unsafe { PyObject::from_borrowed_ptr(py, block_cache_ptr) };
    let state_view_factory = unsafe { PyObject::from_borrowed_ptr(py, state_view_factory_ptr) };
    let settings_cache = unsafe { PyObject::from_borrowed_ptr(py, settings_cache_ptr) };
    let block_sender = unsafe { PyObject::from_borrowed_ptr(py, block_sender_ptr) };
    let batch_sender = unsafe { PyObject::from_borrowed_ptr(py, batch_sender_ptr) };
    let chain_head = unsafe { PyObject::from_borrowed_ptr(py, chain_head_ptr) };
    let chain_head_lock = unsafe { PyObject::from_borrowed_ptr(py, chain_head_lock_ptr) };
    let identity_signer = unsafe { PyObject::from_borrowed_ptr(py, identity_signer_ptr) };
    let data_dir = unsafe { PyObject::from_borrowed_ptr(py, data_dir_ptr) };
    let config_dir = unsafe { PyObject::from_borrowed_ptr(py, config_dir_ptr) };
    let permission_verifier = unsafe { PyObject::from_borrowed_ptr(py, permission_verifier_ptr) };
    let check_publish_block_frequency = unsafe { PyObject::from_borrowed_ptr(py, check_publish_block_frequency_ptr) };
    let batch_observers = unsafe { PyObject::from_borrowed_ptr(py, batch_observers_ptr) };
    let batch_injector_factory = unsafe { PyObject::from_borrowed_ptr(py, batch_injector_factory_ptr) };

    let chain_head = if chain_head == Python::None(py) {
        None
    } else {
        chain_head.extract(py).expect("Got chain head that wasn't a BlockWrapper")
    };
    let check_publish_block_frequency: Duration =
        Duration::from_millis(check_publish_block_frequency.extract(py).unwrap());
    let batch_observers: Vec<PyObject> = batch_observers.extract::<PyList>(py).unwrap().iter(py).collect();

    let batch_publisher_mod = py.import("sawtooth_validator.journal.consensus.batch_publisher")
        .expect("Unable to import 'sawtooth_validator.journal.consensus.batch_publisher'");
    let batch_publisher = batch_publisher_mod
        .call(py, "BatchPublisher", (identity_signer.clone_ref(py), batch_sender), None)
        .expect("Unable to create BatchPublisher");

    let consensus_factory_mod = py.import(
        "sawtooth_validator.journal.consensus.consensus_factory",
    ).expect("Unable to import 'sawtooth_validator.journal.consensus.consensus_factory'");
    let consensus_factory = consensus_factory_mod.get(py, "ConsensusFactory").unwrap();

    let block_wrapper_mod = py.import("sawtooth_validator.journal.block_wrapper")
        .expect("Unable to import 'sawtooth_validator.journal.block_wrapper'");

    let block_wrapper_class = block_wrapper_mod
        .get(py, "BlockWrapper")
        .expect("Unable to import BlockWrapper from 'sawtooth_validator.journal.block_wrapper'");

    let block_header_class = py.import("sawtooth_validator.protobuf.block_pb2")
        .expect("Unable to import 'sawtooth_validator.protobuf.block_pb2'")
        .get(py, "BlockHeader")
        .expect("Unable to import BlockHeader from 'sawtooth_validator.protobuf.block_pb2'");

    let block_builder_class = py.import("sawtooth_validator.journal.block_builder")
        .expect("Unable to import 'sawtooth_validator.journal.block_builder'")
        .get(py, "BlockBuilder")
        .expect("Unable to import BlockBuilder from 'sawtooth_validator.journal.block_builder'");

    let settings_view_class = py.import("sawtooth_validator.state.settings_view")
        .expect("Unable to import 'sawtooth_validator.state.settings_view'")
        .get(py, "SettingsView")
        .expect("Unable to import SettingsView from 'sawtooth_validator.state.settings_view'");

    let publisher = BlockPublisher::new(
        transaction_executor,
        block_cache,
        state_view_factory,
        settings_cache,
        block_sender,
        batch_publisher,
        chain_head,
        chain_head_lock,
        identity_signer,
        data_dir,
        config_dir,
        permission_verifier,
        check_publish_block_frequency,
        batch_observers,
        batch_injector_factory,
        consensus_factory,
        block_wrapper_class,
        block_header_class,
        block_builder_class,
        settings_view_class);

    unsafe {
        let ptr: *const Mutex<BlockPublisher> = Arc::into_raw(publisher);
        *block_publisher_ptr = ptr as *const c_void;
    }

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_drop(publisher: *mut c_void) -> ErrorCode {
    check_null!(publisher);
    unsafe { Arc::from_raw(publisher as *mut Mutex<BlockPublisher>) };
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_start(publisher: *mut c_void) -> ErrorCode {
    check_null!(publisher);
    let publisher: Arc<Mutex<BlockPublisher>> = unsafe {
         Arc::from_raw(publisher as *mut Mutex<BlockPublisher>)
    };
    let publisher_clone = Arc::clone(&publisher);
    BlockPublisher::start(publisher_clone);
    Arc::into_raw(publisher);
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_stop(publisher: *mut c_void) -> ErrorCode {
    check_null!(publisher);
    unsafe {
        (*(publisher as *mut Mutex<BlockPublisher>)).lock().unwrap().stop()
    }
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_pending_batch_info(
    publisher: *mut c_void,
    length: *mut i32,
    limit: *mut i32
) -> ErrorCode {
    check_null!(publisher);
     unsafe {
        let info = (*(publisher as *mut Mutex<BlockPublisher>)).lock().unwrap().pending_batch_info();
        *length = info.0;
        *limit = info.1;
    }
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_batch_sender(
    publisher: *mut c_void,
    incoming_batch_sender: *mut *const c_void,
) -> ErrorCode {
    check_null!(publisher);
    let batch_tx = unsafe { (*(publisher as *mut Mutex<BlockPublisher>)).lock().unwrap().batch_sender() };
    let batch_tx_ptr: *mut IncomingBatchSender = Box::into_raw(Box::new(batch_tx));
    unsafe {
        *incoming_batch_sender = batch_tx_ptr as *const c_void;
    }

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_on_chain_updated(
    publisher: *mut c_void,
    chain_head_ptr: *mut py_ffi::PyObject,
    committed_batches_ptr: *mut py_ffi::PyObject,
    uncommitted_batches_ptr: *mut py_ffi::PyObject
) -> ErrorCode {
    check_null!(publisher);
    let (chain_head, committed_batches, uncommitted_batches) = {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let chain_head = unsafe { PyObject::from_borrowed_ptr(py, chain_head_ptr) };
        let py_committed_batches = unsafe { PyObject::from_borrowed_ptr(py, committed_batches_ptr) };
        let committed_batches: Vec<Batch> = if py_committed_batches == Python::None(py) {
            Vec::new()
        } else {
            py_committed_batches
                .extract::<PyList>(py)
                .expect("Failed to extract PyList from committed_batches")
                .iter(py)
                .map(|pyobj| pyobj.extract::<Batch>(py).unwrap())
                .collect()
        };
        let py_uncommitted_batches = unsafe { PyObject::from_borrowed_ptr(py, uncommitted_batches_ptr) };
        let uncommitted_batches: Vec<Batch> = if py_uncommitted_batches == Python::None(py) {
            Vec::new()
        } else {
            py_uncommitted_batches
                .extract::<PyList>(py)
                .expect("Failed to extract PyList from uncommitted_batches")
                .iter(py)
                .map(|pyobj| pyobj.extract::<Batch>(py).unwrap())
                .collect()
        };
        let chain_head = if chain_head == Python::None(py) {
            None
        } else {
            Some(chain_head.extract(py)
                .expect("Got a new chain head that wasn't a BlockWrapper"))
        };
        (chain_head, committed_batches, uncommitted_batches)
    };

    unsafe {
        (*(publisher as *mut Mutex<BlockPublisher>)).lock().unwrap().on_chain_updated(
            chain_head,
            committed_batches,
            uncommitted_batches,
        )
    }
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_has_batch(
    publisher: *mut c_void,
    batch_id: *const c_char,
    has: *mut bool
) -> ErrorCode {
    check_null!(publisher);
    let batch_id = match unsafe { CStr::from_ptr(batch_id).to_str() } {
        Ok(s) => s,
        Err(_) => return ErrorCode::InvalidInput,
    };
    unsafe {
        *has = (*(publisher as *mut Mutex<BlockPublisher>)).lock().unwrap().has_batch(batch_id);
    }
    ErrorCode::Success
}