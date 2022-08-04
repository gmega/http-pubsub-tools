import time

from http_pubsub_tools.http_pubsub import batch_handler, single_message_handler
from http_pubsub_tools.messages.decoders import DecodedMessage, JSONDecoder
from tests.message_utils import publish_messages


def test_processes_single(publisher_client, pubsub_subscription, pubsub_topic):
    messages = [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 4}]
    processed = []

    @batch_handler(subscription=pubsub_subscription.name, decoder=JSONDecoder())
    @single_message_handler
    def process_single(message: DecodedMessage):
        if message.payload['a'] % 2 == 0:
            raise Exception('ooops!')

        processed.append(message.payload)

    publish_messages(messages, pubsub_topic, publisher_client)

    stats = process_single(None)

    assert processed == [{'a': 1}, {'a': 3}]

    assert stats == {'received': 4, 'processed': 2, 'errors': 2}


def test_single_respects_timeout(publisher_client, pubsub_subscription, pubsub_topic):
    messages = [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 4}]
    processed = []

    # We'll kind of count on the fact that processing the first two elements will happen before the timeout.
    # This means this test is a possible source of problems if by chance the machine running the tests is
    # loaded. This is not a great way to write tests.

    @batch_handler(subscription=pubsub_subscription.name, decoder=JSONDecoder(), batch_timeout=0.2)
    @single_message_handler
    def process_single(message: DecodedMessage):
        if message.payload['a'] == 2:
            # This will ensure we timed out.
            time.sleep(0.2)
        processed.append(message.payload)

    publish_messages(messages, pubsub_topic, publisher_client)

    stats = process_single(None)

    assert processed == [{'a': 1}, {'a': 2}]

    assert stats == {'received': 4, 'processed': 2, 'errors': 0}
