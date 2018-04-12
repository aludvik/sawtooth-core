use cpython::{ObjectProtocol, PyTuple, PyResult, PyModule, PyObject, Python, ToPyObject, PythonObject};
use log;
use log::{Log, Level, Record, Metadata, SetLoggerError};

pub struct PyLogger {
    logger: PyObject,
    logging: PyModule,
}

impl PyLogger {
    fn new() -> PyResult<Self>  {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let logging = py.import("logging")?;
        let logger = logging.call(py, "getLogger", PyTuple::new(py, &[]), None)?;
        Ok(PyLogger { logger, logging })
    }

    pub fn init() -> Result<(), SetLoggerError> {
        let logger = PyLogger::new().unwrap();
        log::set_boxed_logger(Box::new(logger))?;
        log::set_max_level(Level::Trace.to_level_filter());
        Ok(())
    }
}

fn into_level_string(level: Level) -> &'static str {
    match level {
        Level::Error => "ERROR",
        Level::Warn => "WARN",
        Level::Info => "INFO",
        Level::Debug => "DEBUG",
        Level::Trace => "DEBUG",
    }
}

fn into_level_method(level: Level) -> &'static str {
    match level {
        Level::Error => "error",
        Level::Warn => "warn",
        Level::Info => "info",
        Level::Debug => "debug",
        Level::Trace => "debug",
    }
}

impl Log for PyLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        let gil = Python::acquire_gil();
        let py = gil.python();

        let level = into_level_string(metadata.level());
        let pylevel = self.logging.get(py, level).unwrap();
        self.logger
            .call_method(
                py,
                "isEnabledFor",
                PyTuple::new(py, &[pylevel]),
                None
            )
            .unwrap()
            .extract(py)
            .unwrap()
    }

    fn log(&self, record: &Record) {
        let gil = Python::acquire_gil();
        let py = gil.python();

        if !self.enabled(record.metadata()) {
           return;
        }

        let method = into_level_method(record.level());
        let record = format!(
            "[{}: {}] {}",
            record.file().unwrap_or("unknown file"),
            record.line().unwrap_or(0),
            record.args());
        self.logger
            .call_method(
                py,
                method,
                PyTuple::new(py, &[record.to_py_object(py).into_object()]),
                None
            )
            .unwrap();
    }

    fn flush(&self) {
        let gil = Python::acquire_gil();
        let py = gil.python();
        self.logger.call_method(py, "flush", PyTuple::new(py, &[]), None).unwrap();
    }
}
