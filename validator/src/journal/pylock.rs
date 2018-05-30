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

use cpython::{ObjectProtocol, PyObject, Python, NoArgs};

pub struct PyLock {
    py_lock: PyObject,
}

impl PyLock {
    pub fn new(py_lock: PyObject) -> Self {
        PyLock { py_lock }
    }

    pub fn lock(&self) -> PyExternalLockGuard {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();
        self.py_lock
            .call_method(py, "acquire", NoArgs, None)
            .expect("Unable to call release on python lock");

        let py_release_fn = self.py_lock
            .getattr(py, "release")
            .expect("unable to get release function");
        PyExternalLockGuard { py_release_fn }
    }
}

pub struct PyExternalLockGuard {
    py_release_fn: PyObject,
}

impl PyExternalLockGuard {
    pub fn release(&self) {
        let gil_guard = Python::acquire_gil();
        let py = gil_guard.python();
        self.py_release_fn
            .call(py, NoArgs, None)
            .expect("Unable to call release on python lock");
    }
}

impl Drop for PyExternalLockGuard {
    fn drop(&mut self) {
        self.release();
    }
}
