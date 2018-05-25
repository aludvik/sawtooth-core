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

use cpython::{PyObject, PyList, Python};

use batch::Batch;
use journal::publisher::BlockPublisher;

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
    let identity_signer = unsafe { PyObject::from_borrowed_ptr(py, identity_signer_ptr) };
    let data_dir = unsafe { PyObject::from_borrowed_ptr(py, data_dir_ptr) };
    let config_dir = unsafe { PyObject::from_borrowed_ptr(py, config_dir_ptr) };
    let permission_verifier = unsafe { PyObject::from_borrowed_ptr(py, permission_verifier_ptr) };
    let check_publish_block_frequency = unsafe { PyObject::from_borrowed_ptr(py, check_publish_block_frequency_ptr) };
    let batch_observers = unsafe { PyObject::from_borrowed_ptr(py, batch_observers_ptr) };
    let batch_injector_factory = unsafe { PyObject::from_borrowed_ptr(py, batch_injector_factory_ptr) };

    let check_publish_block_frequency: u64 = check_publish_block_frequency.extract(py).unwrap();
    let batch_observers: Vec<PyObject> = batch_observers.extract::<PyList>(py).unwrap().iter(py).collect();

    let publisher = BlockPublisher::new(
        transaction_executor,
        block_cache,
        state_view_factory,
        settings_cache,
        block_sender,
        batch_sender,
        chain_head,
        identity_signer,
        data_dir,
        config_dir,
        permission_verifier,
        check_publish_block_frequency, // Extract int
        batch_observers, // Extract PyList
        batch_injector_factory);

    unsafe {
        *block_publisher_ptr = Box::into_raw(Box::new(publisher)) as *const c_void;
    }

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_drop(publisher: *mut c_void) -> ErrorCode {
    check_null!(publisher);
    unsafe { Box::from_raw(publisher as *mut BlockPublisher) };
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_start(publisher: *mut c_void) -> ErrorCode {
    check_null!(publisher);
    unsafe {
        (*(publisher as *mut BlockPublisher)).start()
    }
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_stop(publisher: *mut c_void) -> ErrorCode {
    check_null!(publisher);
    unsafe {
        (*(publisher as *mut BlockPublisher)).stop()
    }
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn block_publisher_pending_batch_info(
    publisher: *mut c_void,
    length: *mut usize,
    limit: *mut usize
) -> ErrorCode {
    check_null!(publisher);
     unsafe {
        let info = (*(publisher as *mut BlockPublisher)).pending_batch_info();
        *length = info.0;
        *limit = info.1;
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
    let py = unsafe { Python::assume_gil_acquired() };
    let chain_head = unsafe { PyObject::from_borrowed_ptr(py, chain_head_ptr) };
    let committed_batches: Vec<Batch> = unsafe { PyObject::from_borrowed_ptr(py, committed_batches_ptr) }
        .extract::<PyList>(py)
        .unwrap()
        .iter(py)
        .map(|pyobj| pyobj.extract::<Batch>(py).unwrap())
        .collect();
    let uncommitted_batches: Vec<Batch> = unsafe { PyObject::from_borrowed_ptr(py, uncommitted_batches_ptr) }
        .extract::<PyList>(py)
        .unwrap()
        .iter(py)
        .map(|pyobj| pyobj.extract::<Batch>(py).unwrap())
        .collect();
    unsafe {
        (*(publisher as *mut BlockPublisher)).on_chain_updated(
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
        *has = (*(publisher as *mut BlockPublisher)).has_batch(batch_id);
    }
    ErrorCode::Success
}