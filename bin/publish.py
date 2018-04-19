from sawtooth_sdk.protobuf import validator_pb2
from sawtooth_sdk.protobuf import consensus_pb2
from sawtooth_sdk.messaging.stream import Stream

import time

while True:
    time.sleep(1)
    stream = Stream("tcp://127.0.0.1:5005")
    future = stream.send(validator_pb2.Message.CONSENSUS_NOTIFY_BLOCK_VALID, consensus_pb2.ConsensusNotifyBlockValid().SerializeToString())
    result = future.result()
    print(result.message_type, result.content)
stream.close()
