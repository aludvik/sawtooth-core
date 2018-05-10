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

from sawtooth_validator.consensus import handlers

def add(
        dispatcher,
        thread_pool,
        consensus_proxy,
):

    dispatcher.add_handler(
        handlers.ConsensusRegisterHandler.request_type(),
        handlers.ConsensusRegisterHandler(),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusSendToHandler.request_type(),
        handlers.ConsensusSendToHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusBroadcastHandler.request_type(),
        handlers.ConsensusBroadcastHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusInitializeBlockHandler.request_type(),
        handlers.ConsensusInitializeBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusFinalizeBlockHandler.request_type(),
        handlers.ConsensusFinalizeBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusCancelBlockHandler.request_type(),
        handlers.ConsensusCancelBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusCheckBlockHandler.request_type(),
        handlers.ConsensusCheckBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusCommitBlockHandler.request_type(),
        handlers.ConsensusCommitBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusIgnoreBlockHandler.request_type(),
        handlers.ConsensusIgnoreBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusFailBlockHandler.request_type(),
        handlers.ConsensusFailBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusBlocksGetHandler.request_type(),
        handlers.ConsensusBlocksGetHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusSettingsGetHandler.request_type(),
        handlers.ConsensusSettingsGetHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        handlers.ConsensusStateGetHandler.request_type(),
        handlers.ConsensusStateGetHandler(consensus_proxy),
        thread_pool)
