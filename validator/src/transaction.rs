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

use metrics;
lazy_static! {
    static ref COLLECTOR: metrics::MetricsCollectorHandle =
        metrics::get_collector("sawtooth_validator.transaction");
}

#[derive(Debug, PartialEq)]
pub struct Transaction {
    pub header_signature: String,
    pub payload: Vec<u8>,
    pub batcher_public_key: String,
    pub dependencies: Vec<String>,
    pub family_name: String,
    pub family_version: String,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub nonce: String,
    pub payload_sha512: String,
    pub signer_public_key: String,

    pub header_bytes: Vec<u8>,
}

impl Transaction {
    pub fn new(
        header_signature: String,
        payload: Vec<u8>,
        batcher_public_key: String,
        dependencies: Vec<String>,
        family_name: String,
        family_version: String,
        inputs: Vec<String>,
        outputs: Vec<String>,
        nonce: String,
        payload_sha512: String,
        signer_public_key: String,
        header_bytes: Vec<u8>,
    ) -> Self {
        COLLECTOR.counter("Transaction.new", None, None).inc();
        COLLECTOR.counter("Transaction.net", None, None).inc();
        Transaction {
            header_signature,
            payload,
            batcher_public_key,
            dependencies,
            family_name,
            family_version,
            inputs,
            outputs,
            nonce,
            payload_sha512,
            signer_public_key,
            header_bytes,
        }
    }
}

impl Clone for Transaction {
    fn clone(&self) -> Self {
        Transaction::new(
            self.header_signature.clone(),
            self.payload.clone(),
            self.batcher_public_key.clone(),
            self.dependencies.clone(),
            self.family_name.clone(),
            self.family_version.clone(),
            self.inputs.clone(),
            self.outputs.clone(),
            self.nonce.clone(),
            self.payload_sha512.clone(),
            self.signer_public_key.clone(),
            self.header_bytes.clone(),
        )
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        COLLECTOR.counter("Transaction.drop", None, None).inc();
        COLLECTOR.counter("Transaction.net", None, None).dec();
    }
}


impl From<Transaction> for proto::transaction::Transaction {
    fn from(other: Transaction) -> Self {
        let mut proto_transaction = proto::transaction::Transaction::new();
        proto_transaction.set_payload(other.payload.clone());
        proto_transaction.set_header_signature(other.header_signature.clone());
        proto_transaction.set_header(other.header_bytes.clone());
        proto_transaction
    }
}

impl From<proto::transaction::Transaction> for Transaction {
    fn from(mut proto_txn: proto::transaction::Transaction) -> Self {
        let mut txn_header: proto::transaction::TransactionHeader =
            protobuf::parse_from_bytes(proto_txn.get_header())
                .expect("Unable to parse TransactionHeader bytes");

        Transaction::new(
            proto_txn.take_header_signature(),
            proto_txn.take_payload(),
            txn_header.take_batcher_public_key(),
            txn_header.take_dependencies().into_vec(),
            txn_header.take_family_name(),
            txn_header.take_family_version(),
            txn_header.take_inputs().into_vec(),
            txn_header.take_outputs().into_vec(),
            txn_header.take_nonce(),
            txn_header.take_payload_sha512(),
            txn_header.take_signer_public_key(),
            proto_txn.take_header(),
        )
    }
}
