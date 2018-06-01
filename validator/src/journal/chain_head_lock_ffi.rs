use std::os::raw::c_void;

use py_ffi;
use cpython::Python;

use journal::publisher_ffi::convert_on_chain_updated_args;
use journal::chain_head_lock::{ChainHeadLock, ChainHeadGuard};

#[repr(u32)]
#[derive(Debug)]
pub enum ErrorCode {
    Success = 0,
    NullPointerProvided = 0x01,
}

#[no_mangle]
pub extern "C" fn chain_head_lock_lock(
    chain_head_lock_ptr: *mut c_void,
    chain_head_guard_ptr: *mut *const c_void
) -> ErrorCode {
    if chain_head_lock_ptr.is_null() { return ErrorCode::NullPointerProvided; }
    let guard = unsafe {
        (*(chain_head_lock_ptr as *mut ChainHeadLock)).lock()
    };
    unsafe {
        *chain_head_guard_ptr = Box::into_raw(Box::new(guard)) as *const c_void;
    };
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn chain_head_lock_drop(
    chain_head_lock_ptr: *mut c_void,
) -> ErrorCode {
    if chain_head_lock_ptr.is_null() { return ErrorCode::NullPointerProvided; }
    unsafe {
        Box::from_raw(chain_head_lock_ptr as *mut ChainHeadLock);
    }
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn chain_head_guard_drop(
    chain_head_guard_ptr: *mut c_void,
) -> ErrorCode {
    if chain_head_guard_ptr.is_null() { return ErrorCode::NullPointerProvided; }
    unsafe {
        Box::from_raw(chain_head_guard_ptr as *mut ChainHeadGuard);
    }
    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn chain_head_guard_on_chain_updated(
    chain_head_guard_ptr: *mut c_void,
    chain_head_ptr: *mut py_ffi::PyObject,
    committed_batches_ptr: *mut py_ffi::PyObject,
    uncommitted_batches_ptr: *mut py_ffi::PyObject,
) -> ErrorCode {
    if chain_head_guard_ptr.is_null() { return ErrorCode::NullPointerProvided; }
    if chain_head_ptr.is_null() { return ErrorCode::NullPointerProvided; }
    if committed_batches_ptr.is_null() { return ErrorCode::NullPointerProvided; }
    if uncommitted_batches_ptr.is_null() { return ErrorCode::NullPointerProvided; }

    let gil = Python::acquire_gil();
    let py = gil.python();

    let (chain_head, committed_batches, uncommitted_batches) = convert_on_chain_updated_args(
        py,
        chain_head_ptr,
        committed_batches_ptr,
        uncommitted_batches_ptr,
    );

    unsafe {
        (*(chain_head_guard_ptr as *mut ChainHeadGuard)).on_chain_updated(
            chain_head,
            committed_batches,
            uncommitted_batches)
    };
    ErrorCode::Success
}
