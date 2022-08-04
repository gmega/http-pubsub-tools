import json
from typing import Dict, List

from google.cloud.pubsub_v1 import PublisherClient
from google.pubsub_v1 import Topic


def encode(json_payload: Dict) -> bytes:
    return json.dumps(json_payload).encode('utf-8')


def publish_messages(json_messages: List[Dict], topic: Topic, client: PublisherClient):
    for message in json_messages:
        client.publish(topic=topic.name, data=encode(message)).result()
