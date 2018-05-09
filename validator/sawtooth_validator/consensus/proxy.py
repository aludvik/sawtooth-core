# Copyright 2018 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------

import logging

from sawtooth_validator.journal.publisher import EmptyBlock

LOGGER = logging.getLogger(__name__)


class ConsensusProxy:
    """Receives requests from the consensus engine handlers and delegates them
    to the appropriate components."""

    def __init__(self, network_service, chain_controller, block_publisher):
        self._network_service = network_service
        self._chain_controller = chain_controller
        self._block_publisher = block_publisher

    # Using network service
    def send_to(self, peer_id, message):
        raise NotImplementedError()

    def broadcast(self, message):
        raise NotImplementedError()


    # Using block publisher
    def initialize_block(self, previous_id):
        LOGGER.info("ConsensusProxy.initialize_block")
        if previous_id:
            self._block_publisher.initialize_block(
                hex(previous_id))
        else:
            self._block_publisher.initialize_block(
                self._chain_controller.chain_head)

    def finalize_block(self, consensus_data):
        LOGGER.info("ConsensusProxy.finalize_block")
        try:
            result = self._block_publisher.finalize_block(
                consensus=consensus_data)
        except EmptyBlock:
            LOGGER.warn("Tried to finalize an empty block")
        else:
            self._block_publisher.publish_block(result.block, result.injected_batches)

    def cancel_block(self):
        LOGGER.info("ConsensusProxy.cancel_block")
        self._block_publisher.cancel_block()


    # Using chain controller
    def check_block(self, block_ids):
        raise NotImplementedError()

    def commit_block(self, block_id):
        raise NotImplementedError()

    def ignore_block(self, block_id):
        raise NotImplementedError()

    def fail_block(self, block_id):
        raise NotImplementedError()


    # Using blockstore and state database
    def block_get(self, block_ids):
        raise NotImplementedError()

    def setting_get(self, block_id, settings):
        raise NotImplementedError()

    def state_get(self, block_id, addresses):
        raise NotImplementedError()
