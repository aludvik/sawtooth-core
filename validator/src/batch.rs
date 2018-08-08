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

use proto;
use protobuf::{self, Message};

use transaction::Transaction;

use metrics;
lazy_static! {
    static ref COLLECTOR: metrics::MetricsCollectorHandle =
        metrics::get_collector("sawtooth_validator.batch");
}

#[derive(Debug, PartialEq)]
pub struct Batch {
    pub header_signature: String,
    pub transactions: Vec<Transaction>,
    pub signer_public_key: String,
    pub transaction_ids: Vec<String>,
    pub trace: bool,

    pub header_bytes: Vec<u8>,
}

impl Batch {
    pub fn new(
        header_signature: String,
        transactions: Vec<Transaction>,
        signer_public_key: String,
        transaction_ids: Vec<String>,
        trace: bool,
        header_bytes: Vec<u8>,
    ) -> Self {
        COLLECTOR.counter("Batch.new", None, None).inc();
        COLLECTOR.counter("Batch.net", None, None).inc();
        Batch {
            header_signature,
            transactions,
            signer_public_key,
            transaction_ids,
            trace,
            header_bytes,
        }
    }
}

impl Clone for Batch {
    fn clone(&self) -> Self {
        Batch::new(
            self.header_signature.clone(),
            self.transactions.clone(),
            self.signer_public_key.clone(),
            self.transaction_ids.clone(),
            self.trace.clone(),
            self.header_bytes.clone(),
        )
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        COLLECTOR.counter("Batch.drop", None, None).inc();
        COLLECTOR.counter("Batch.net", None, None).dec();
    }
}

impl From<Batch> for proto::batch::Batch {
    fn from(batch: Batch) -> Self {
        let mut proto_batch = proto::batch::Batch::new();
        proto_batch.set_transactions(protobuf::RepeatedField::from_vec(
            batch
                .transactions
                .iter()
                .cloned()
                .map(proto::transaction::Transaction::from)
                .collect(),
        ));
        proto_batch.set_header_signature(batch.header_signature.clone());
        proto_batch.set_header(batch.header_bytes.clone());
        proto_batch
    }
}

impl From<proto::batch::Batch> for Batch {
    fn from(mut proto_batch: proto::batch::Batch) -> Batch {
        let mut batch_header: proto::batch::BatchHeader =
            protobuf::parse_from_bytes(proto_batch.get_header())
                .expect("Unable to parse BatchHeader bytes");

        Batch::new(
            proto_batch.take_header_signature(),
            proto_batch
                .take_transactions()
                .into_iter()
                .map(Transaction::from)
                .collect(),
            batch_header.take_signer_public_key(),
            batch_header.take_transaction_ids().into_vec(),
            proto_batch.get_trace(),
            proto_batch.take_header(),
        )
    }
}
