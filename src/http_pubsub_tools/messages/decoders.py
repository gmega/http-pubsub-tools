import json
from abc import ABC, abstractmethod
from io import BytesIO
from typing import Dict

import avro
from avro.io import DatumReader, BinaryDecoder
from avro.name import Names
from google.pubsub_v1 import PubsubMessage


class DecodedMessage:
    def __init__(self, payload: Dict, message_id: str, ack_id: str):
        self.payload = payload
        self.message_id = message_id
        self.ack_id = ack_id


class MessageDecoder(ABC):

    def decode(self, message: PubsubMessage) -> DecodedMessage:
        return DecodedMessage(
            message_id=message.message.message_id,
            payload=self.decode_payload(message.message.data),
            ack_id=message.ack_id
        )

    @abstractmethod
    def decode_payload(self, payload: bytes):
        pass


class AvroDecoder(MessageDecoder):
    def __init__(self, json_schema: Dict):
        self.reader = DatumReader(avro.schema.make_avsc_object(json_schema, Names()))

    def decode_payload(self, payload: bytes) -> object:
        return self.reader.read(BinaryDecoder(BytesIO(payload)))


class JSONDecoder(MessageDecoder):

    def decode_payload(self, payload: bytes) -> DecodedMessage:
        return json.loads(payload.decode(encoding='utf-8'))
