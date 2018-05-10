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

LOGGER = logging.getLogger(__name__)


class ConsensusNotifier:
    """Handles sending notifications to the consensus engine using the provided
    interconnect service."""

    def __init__(self, consensus_service):
        self._service = consensus_service

    def _notify(self, message, message_type):
        futures = self._service.send_all(
            message_type,
            message.SerializeToString())
        [future.result() for future in futures]

    def notify_peer_connected(self, peer_id):
        """A new peer was added"""
        self._notify(
            validator_pb2.Message.CONSENSUS_NOTIFY_PEER_CONNECTED,
            ConsensusNotifyPeerConnected(ConsensusPeerInfo(peer_id=peer_id)))

    def notify_peer_disconnected(self, peer_id):
        """An existing peer was dropped"""
        self._notify(
            validator_pb2.Message.CONSENSUS_NOTIFY_PEER_DISCONNECTED,
            ConsensusNotifyPeerDisconnected(peer_id=peer_id))

    def notify_peer_message(self, message):
        """A new message was received from a peer"""
        self._notify(
            validator_pb2.Message.CONSENSUS_NOTIFY_PEER_MESSAGE,
            ConsensusNotifyPeerMessage(message=message))

    def notify_block_new(self, block):
        """A new block was received and passed initial consensus validation"""
        self._notify(
            validator_pb2.Message.CONSENSUS_NOTIFY_BLOCK_NEW,
            ConsensusNotifyBlockNew(ConsensusBlock(
                block_id=block.identifier,
                previous_id=block.previous_block_id,
                signer_id=block.signer_public_key,
                block_num=block.block_num,
                consensus=block.consensus)))

    def notify_block_valid(self, block_id):
        """This block can be committed successfully"""
        self._notify(
            validator_pb2.Message.CONSENSUS_NOTIFY_BLOCK_VALID,
            ConsensusNotifyBlockValid(block_id=block_id))

    def notify_block_invalid(self, block_id):
        """This block cannot be committed successfully"""
        self._notify(
            validator_pb2.Message.CONSENSUS_NOTIFY_BLOCK_INVALID,
            ConsensusNotifyBlockInvalid(block_id=block_id))

    def notify_block_commit(self, block_id):
        """This block has been committed"""
        self._notify(
            validator_pb2.Message.CONSENSUS_NOTIFY_BLOCK_COMMIT,
            ConsensusNotifyBlockCommit(block_id=block_id))
