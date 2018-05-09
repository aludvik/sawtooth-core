import os
import sys
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
    'sdk', 'python'))

from sawtooth_sdk.protobuf import validator_pb2
from sawtooth_sdk.protobuf import consensus_pb2
from sawtooth_sdk.messaging.stream import Stream

import time

def main():
    stream = Stream("tcp://127.0.0.1:5005")
    period = int(sys.argv[1])

    try:
        while True:
            initialize(stream)
            time.sleep(period)
            finalize(stream)

    finally:
        stream.close()

def initialize(stream):
    future = stream.send(
        validator_pb2.Message.CONSENSUS_INITIALIZE_BLOCK_REQUEST,
        consensus_pb2.ConsensusInitializeBlockRequest().SerializeToString())
    result = future.result()
    assert(result.message_type == validator_pb2.Message.CONSENSUS_INITIALIZE_BLOCK_RESPONSE)
    response = consensus_pb2.ConsensusInitializeBlockResponse()
    response.ParseFromString(result.content)
    assert(response.status == consensus_pb2.ConsensusInitializeBlockResponse.OK)

def finalize(stream):
    future = stream.send(
        validator_pb2.Message.CONSENSUS_FINALIZE_BLOCK_REQUEST,
        consensus_pb2.ConsensusFinalizeBlockRequest(data=b"Devmode").SerializeToString())
    result = future.result()
    assert(result.message_type == validator_pb2.Message.CONSENSUS_FINALIZE_BLOCK_RESPONSE)
    response = consensus_pb2.ConsensusFinalizeBlockResponse()
    response.ParseFromString(result.content)
    if response.status == consensus_pb2.ConsensusFinalizeBlockResponse.EMPTY_BLOCK:
        print("Empty block")
    else:
        assert(response.status == consensus_pb2.ConsensusFinalizeBlockResponse.OK)

if __name__ == "__main__":
    main()
