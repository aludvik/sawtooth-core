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
use proto;
use protobuf::{self, Message};
use std::fmt;

use metrics;
lazy_static! {
    static ref COLLECTOR: metrics::MetricsCollectorHandle =
        metrics::get_collector("sawtooth_validator.block");
}

#[derive(Debug, PartialEq, Default)]
pub struct Block {
    pub header_signature: String,
    pub batches: Vec<Batch>,
    pub state_root_hash: String,
    pub consensus: Vec<u8>,
    pub batch_ids: Vec<String>,
    pub signer_public_key: String,
    pub previous_block_id: String,
    pub block_num: u64,

    pub header_bytes: Vec<u8>,
}

impl Block {
    pub fn new(
        header_signature: String,
        batches: Vec<Batch>,
        state_root_hash: String,
        consensus: Vec<u8>,
        batch_ids: Vec<String>,
        signer_public_key: String,
        previous_block_id: String,
        block_num: u64,
        header_bytes: Vec<u8>,
    ) -> Self {
        COLLECTOR.counter("Block.new", None, None).inc();
        COLLECTOR.counter("Block.net", None, None).inc();
        Block {
            header_signature,
            batches,
            state_root_hash,
            consensus,
            batch_ids,
            signer_public_key,
            previous_block_id,
            block_num,
            header_bytes,
        }
    }
}

impl Clone for Block {
    fn clone(&self) -> Self {
        Block::new(
            self.header_signature.clone(),
            self.batches.clone(),
            self.state_root_hash.clone(),
            self.consensus.clone(),
            self.batch_ids.clone(),
            self.signer_public_key.clone(),
            self.previous_block_id.clone(),
            self.block_num.clone(),
            self.header_bytes.clone(),
        )
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        COLLECTOR.counter("Block.drop", None, None).inc();
        COLLECTOR.counter("Block.net", None, None).dec();
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Block(id: {}, block_num: {}, state_root_hash: {}, previous_block_id: {})",
            self.header_signature, self.block_num, self.state_root_hash, self.previous_block_id
        )
    }
}

impl From<Block> for proto::block::Block {
    fn from(other: Block) -> Self {
        let mut proto_block = proto::block::Block::new();
        proto_block.set_batches(protobuf::RepeatedField::from_vec(
            other
                .batches
                .iter()
                .cloned()
                .map(proto::batch::Batch::from)
                .collect(),
        ));
        proto_block.set_header_signature(other.header_signature.clone());
        proto_block.set_header(other.header_bytes.clone());
        proto_block
    }
}

impl From<proto::block::Block> for Block {
    fn from(mut proto_block: proto::block::Block) -> Self {
        let mut block_header: proto::block::BlockHeader =
            protobuf::parse_from_bytes(proto_block.get_header())
                .expect("Unable to parse BlockHeader bytes");

        Block::new(
            proto_block.take_header_signature(),
            proto_block
                .take_batches()
                .into_iter()
                .map(Batch::from)
                .collect(),
            block_header.take_state_root_hash(),
            block_header.take_consensus(),
            block_header.take_batch_ids().into_vec(),
            block_header.take_signer_public_key(),
            block_header.take_previous_block_id(),
            block_header.get_block_num(),
            proto_block.take_header(),
        )
    }
}
