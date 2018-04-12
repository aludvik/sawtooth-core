extern crate cpython;
#[macro_use]
extern crate log;

use cpython::{PyString, PyTuple, Python};

mod logger;

use logger::PyLogger;

fn main() {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let mut args: Vec<PyString> = std::env::args()
        .skip(1)
        .map(|s| PyString::new(py, &s))
        .collect();

    args.insert(0, PyString::new(py, env!("CARGO_PKG_NAME")));

    let server_log = py.import("sawtooth_validator.server.log")
        .map_err(|err| err.print(py))
        .unwrap();

    server_log.call(py, "init_console_logging", PyTuple::new(py, &[]), None)
        .map_err(|err| err.print(py))
        .unwrap();

    PyLogger::init().expect("Failed to set logger");
    debug!("Running sawtooth-validator from Rust!");

    let cli = py.import("sawtooth_validator.server.cli")
        .map_err(|err| err.print(py))
        .unwrap();

    cli.call(py, "main", (args,), None)
        .map_err(|err| err.print(py))
        .expect("sawtooth_validator.server.main returned an error");
}
