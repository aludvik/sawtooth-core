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

#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate log4rs;
extern crate sawtooth_sdk;

use std::sync::mpsc::{RecvTimeoutError, Receiver};
use std::time;
use std::process;

use log::LogLevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;

use sawtooth_sdk::consensus::{
    service::Service,
    engine::*,
    driver::Driver,
    zmq_driver::ZmqDriver,
};

pub struct Devmode {
    period: time::Duration,
    exit: Exit,
}

impl Devmode {
    fn new(period: time::Duration) -> Self {
        Devmode {
            period: period,
            exit: Exit::new(),
        }
    }
}

impl Engine for Devmode {
    fn start(&self, updates: Receiver<Update>, mut service: Box<Service>) {
        let mut start = time::Instant::now();
        service.initialize_block(None).expect("Failed to initialize");
        loop {
            if time::Instant::now().duration_since(start) > self.period {
                service.finalize_block(Vec::from(&b"Devmode"[..])).expect("Failed to finalize");
                start = time::Instant::now();
                service.initialize_block(None).expect("Failed to initialize");
            }

            match updates.recv_timeout(time::Duration::from_millis(10)) {
                Ok(update) => {
                    if self.exit.get() {
                        break;
                    }
                    info!("Got update {:?}", update);
                }
                Err(RecvTimeoutError::Disconnected) => {
                    println!("disconnected");
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {
                    if self.exit.get() {
                        break;
                    }
                }
            }
        }
    }
    fn stop(&self) {
        self.exit.set();
    }
    fn version(&self) -> String {
        "0.1".into()
    }
    fn name(&self) -> String {
        "Devmode".into()
    }
}


fn main() {
    let matches = clap_app!(intkey =>
        (version: crate_version!())
        (about: "Devmode Consensus Engine (Rust)")
        (@arg connect: -C --connect +takes_value
         "connection endpoint for validator")
        (@arg period: -P --period +takes_value
         "period in seconds between publishing blocks")
        (@arg verbose: -v --verbose +multiple
         "increase output verbosity"))
        .get_matches();

    let endpoint = matches
        .value_of("connect")
        .unwrap_or("tcp://localhost:5005");

    let period: u64 = matches
        .value_of("period")
        .unwrap_or("3")
        .parse()
        .unwrap_or_else(|err| {
            error!("{}", err);
            process::exit(1);
        });

    let console_log_level;
    match matches.occurrences_of("verbose") {
        0 => console_log_level = LogLevelFilter::Warn,
        1 => console_log_level = LogLevelFilter::Info,
        2 => console_log_level = LogLevelFilter::Debug,
        3 | _ => console_log_level = LogLevelFilter::Trace,
    }

    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "{h({l:5.5})} | {({M}:{L}):20.20} | {m}{n}",
        )))
        .build();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(console_log_level))
        .unwrap_or_else(|err| {
            error!("{}", err);
            process::exit(1);
        });

    log4rs::init_config(config)
        .unwrap_or_else(|err| {
            error!("{}", err);
            process::exit(1);
        });

    let driver = ZmqDriver::new(Box::new(Devmode::new(time::Duration::from_secs(period))));
    driver.start(&endpoint)
        .unwrap_or_else(|err| {
            error!("{}", err);
            process::exit(1);
        });
}
