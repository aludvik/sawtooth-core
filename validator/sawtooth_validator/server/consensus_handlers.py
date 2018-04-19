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

from sawtooth_validator.journal.consensus import consensus_handlers

def add(
        dispatcher,
        thread_pool,
        consensus_proxy,
):

    dispatcher.add_handler(
        consensus_handlers.ConsensusSendToHandler.REQUEST,
        consensus_handlers.ConsensusSendToHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusBroadcastHandler.REQUEST,
        consensus_handlers.ConsensusBroadcastHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusInitializeBlockHandler.REQUEST,
        consensus_handlers.ConsensusInitializeBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusFinalizeBlockHandler.REQUEST,
        consensus_handlers.ConsensusFinalizeBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusCancelBlockRHandler.REQUEST,
        consensus_handlers.ConsensusCancelBlockRHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusCheckBlockHandler.REQUEST,
        consensus_handlers.ConsensusCheckBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusCommitBlockHandler.REQUEST,
        consensus_handlers.ConsensusCommitBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusIgnoreBlockHandler.REQUEST,
        consensus_handlers.ConsensusIgnoreBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusFailBlockHandler.REQUEST,
        consensus_handlers.ConsensusFailBlockHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusBlockGetHandler.REQUEST,
        consensus_handlers.ConsensusBlockGetHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusSettingGetHandler.REQUEST,
        consensus_handlers.ConsensusSettingGetHandler(consensus_proxy),
        thread_pool)

    dispatcher.add_handler(
        consensus_handlers.ConsensusStateGetHandler.REQUEST,
        consensus_handlers.ConsensusStateGetHandler(consensus_proxy),
        thread_pool)
