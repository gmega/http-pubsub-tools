import logging

import pytest
from google.cloud import pubsub_v1

TEST_PROJECT = 'test-project'
TEST_TOPIC = 'test-topic'
TEST_SUBSCRIPTION = 'test-subscription'

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def publisher_client():
    yield pubsub_v1.PublisherClient()


@pytest.fixture
def subscriber_client():
    with pubsub_v1.SubscriberClient() as subscriber:
        yield subscriber


@pytest.fixture
def pubsub_topic(publisher_client):
    topic_path = publisher_client.topic_path(TEST_PROJECT, TEST_TOPIC)

    yield publisher_client.create_topic(request={'name': topic_path})

    publisher_client.delete_topic(request={'topic': topic_path})


@pytest.fixture
def pubsub_subscription(subscriber_client, pubsub_topic):
    subscription_path = subscriber_client.subscription_path(TEST_PROJECT, TEST_SUBSCRIPTION)

    yield subscriber_client.create_subscription({
        'name': subscription_path,
        'topic': pubsub_topic.name
    })

    subscriber_client.delete_subscription({
        'subscription': subscription_path
    })
