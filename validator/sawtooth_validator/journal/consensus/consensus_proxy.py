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


class ConsensusProxy:
    def __init__(self,
        network_service,
        consensus_service,
        block_publisher,
        chain_controller
    ):
        self._network_service = network_service
        self._consensus_service = consensus_service
        self._block_publisher = block_publisher
        self._chain_controlle = chain_controlle

    def start(self):
        pass

    def stop(self):
        pass

    # CE -> proxy -> network

    def send_to(self, message, peer_id):
        # self._connect.send_to(message, peer_id)
        pass

    def broadcast(self, message):
        # self._connect.broadcast(message)
        pass

    # CE -> proxy -> journal

    def initialize_block(self):
        pass

    def finalize_block(self, data):
        pass

    def cancel_block(self):
        pass

    def commit_block(self, block_id):
        pass

    def drop_block(self, block_id):
        pass

    # CE -> proxy -> CE

    def setting_get(self, setting, block_id):
        pass

    def state_get(self, address, block_id):
        pass

    def block_get(self, block_id):
        pass

    # Network -> proxy -> CE

    def on_message_received(self, message):
        pass

    def on_new_block_received(self, block):
        pass

    def on_add_peer(self, peer_id):
        pass

    def on_drop_peer(self, peer_id):
        pass
