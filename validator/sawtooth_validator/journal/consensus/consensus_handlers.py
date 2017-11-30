# Copyright 2017 Intel Corporation
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

from sawtooth_validator.protobuf import consensus_pb2
from sawtooth_validator.protobuf import validator_pb2

from sawtooth_validator.networking.dispatch import Handler
from sawtooth_validator.networking.dispatch import HandlerResult
from sawtooth_validator.networking.dispatch import HandlerStatus


LOGGER = logging.getLogger(__name__)

# TODO: Register, Notifications


class ConsensusSendToHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_SEND_TO_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusSendToResponse(
            status=consensus_pb2.ConsensusSendToResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_SEND_TO_RESPONSE)


class ConsensusBroadcastHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_BROADCAST_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusBroadcastResponse(
            status=consensus_pb2.ConsensusBroadcastResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_BROADCAST_RESPONSE)


class ConsensusInitializeBlockHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_INITIALIZE_BLOCK_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusInitializeBlockResponse(
            status=consensus_pb2.ConsensusInitializeBlockResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_INITIALIZE_BLOCK_RESPONSE)


class ConsensusFinalizeBlockHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_FINALIZE_BLOCK_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusFinalizeBlockResponse(
            status=consensus_pb2.ConsensusFinalizeBlockResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_FINALIZE_BLOCK_RESPONSE)


class ConsensusCancelBlockRHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_CANCEL_BLOCK_R_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusCancelBlockResponse(
            status=consensus_pb2.ConsensusCancelBlockResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_CANCEL_BLOCK_RESPONSE)


class ConsensusCheckBlockHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_CHECK_BLOCK_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusCheckBlockResponse(
            status=consensus_pb2.ConsensusCheckBlockResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_CHECK_BLOCK_RESPONSE)


class ConsensusCommitBlockHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_COMMIT_BLOCK_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusCommitBlockResponse(
            status=consensus_pb2.ConsensusCommitBlockResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_COMMIT_BLOCK_RESPONSE)


class ConsensusIgnoreBlockHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_IGNORE_BLOCK_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusIgnoreBlockResponse(
            status=consensus_pb2.ConsensusIgnoreBlockResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_IGNORE_BLOCK_RESPONSE)


class ConsensusFailBlockHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_FAIL_BLOCK_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusFailBlockResponse(
            status=consensus_pb2.ConsensusFailBlockResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_FAIL_BLOCK_RESPONSE)


class ConsensusBlockGetHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_BLOCK_GET_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusBlockGetResponse(
            status=consensus_pb2.ConsensusBlockGetResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_BLOCK_GET_RESPONSE)


class ConsensusSettingGetHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_SETTING_GET_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusSettingGetResponse(
            status=consensus_pb2.ConsensusSettingGetResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_SETTING_GET_RESPONSE)


class ConsensusStateGetHandler(Handler):
    REQUEST = valdiator_pb2.Message.CONSENSUS_STATE_GET_REQUEST

    def __init__(self, proxy):
        self._proxy = proxy

    def handle(self, connection_id, message_content):

        ok = consensus_pb2.ConsensusStateGetResponse(
            status=consensus_pb2.ConsensusStateGetResponse.OK)
        return HandlerResult(
            status=HandlerStatus.RETURN,
            message_out=ok,
            message_type=validator_pb2.Message.CONSENSUS_STATE_GET_RESPONSE)
