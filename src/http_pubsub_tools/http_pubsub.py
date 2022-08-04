import functools
import logging
import time
from typing import Callable, Dict

from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1 import SubscriberClient

from http_pubsub_tools.defaults import DEFAULT_BATCH_TIMEOUT, DEFAULT_PUBSUB_RETRY_DEADLINE, DEFAULT_BATCH_SIZE
from http_pubsub_tools.messages.decoders import MessageDecoder, DecodedMessage
from http_pubsub_tools.messages.message_batch import MessageBatch, BatchStats

BatchHandler = Callable[[MessageBatch], None]
SingleMessageHandler = Callable[[DecodedMessage], None]


def batch_handler(subscription: str,
                  decoder: MessageDecoder,
                  batch_size: int = DEFAULT_BATCH_SIZE,
                  batch_timeout: float = DEFAULT_BATCH_TIMEOUT,
                  pubsub_retry_deadline: float = DEFAULT_PUBSUB_RETRY_DEADLINE,
                  logger=logging.getLogger(__name__)):
    """
    Transforms an HTTP handler into a function that processes PubSub messages from a given topic in batches while
    respecting a predefined timeout. The typical use case is processing batches of PubSub from within Google Cloud
    Functions or Cloud Run Django/Flask/whatnot handlers.

    Decorated handlers return a JSON-serialized :class:`BatchStats` object with statistics on messages received,
    processed, and errored out.

    :param subscription: the PubSub subscription where to pull messages from.
    :param decoder: a message decoder which knows how to decode the payload of incoming messages.
    :param batch_size: the size of the PubSub batches we want to process.
    :param batch_timeout: the total time allowed for processing.
    :param pubsub_retry_deadline: the deadline for retries when connecting to PubSub fails.
    :param logger: a logger for logging messages. Any logger with an interface compatible with the Python logging
        (PEP-282) will do.
    """

    def decorator(handler: BatchHandler):
        @functools.wraps(handler)
        def process_request(_) -> Dict:
            with pubsub_v1.SubscriberClient() as subscriber:
                # This is the time at which we should stop processing.
                timeout = time.time() + batch_timeout
                stats = BatchStats()
                while True:
                    # Processes a PubSub batch.
                    partial = process_batch(subscriber, timeout)
                    stats += partial
                    # If the current batch had zero messages or
                    # none could be processed, we're done.
                    if partial.processed == 0:
                        return stats.to_json()

        def process_batch(subscriber: SubscriberClient, timeout: float):
            messages = subscriber.pull(
                subscription=subscription,
                max_messages=batch_size,
                # FIXME return_immediately has apparently been deprecated, so we have to figure out a way to
                #   stop using it.
                return_immediately=True,
                retry=retry.Retry(deadline=pubsub_retry_deadline)
            ).received_messages

            n_received = len(messages)
            if n_received == 0:
                logger.info('There are no more pubsub messages to process.')
                return BatchStats()

            logger.info(f'Processing {n_received} PubSub messages.')

            batch = MessageBatch(
                batch=[decoder.decode(message) for message in messages],
                timeout=timeout,
                logger=logger
            )

            # Actually processes the batch.
            handler(batch)

            # Acks successful messages.
            if len(batch.successful) > 0:
                logger.info(
                    f'Acknowledging {len(batch.successful)} processed messages of {n_received}.')
                subscriber.acknowledge(request={"subscription": subscription, "ack_ids": batch.successful})
            else:
                logger.info(f"Got {n_received} messages, but were not able to process any.")

            return batch.stats()

        return process_request

    return decorator


def single_message_handler(handler: SingleMessageHandler) -> BatchHandler:
    """
    Utility decorator for cases in which what we want is a message handler that processes one message at a time
    instead of :class:`MessageBatch`es. Just apply this on top of your processing function before handing it over
    to :func:`batch_handler`.

    :param handler: a :class:`SingleMessageHandler` function.
    """

    def batch_wrapper(messages: MessageBatch):
        for message in messages:
            # noinspection PyBroadException
            try:
                handler(message)
                messages.success(message)
            except Exception as ex:
                messages.logger.exception('Failed to process message')
                messages.failure(message, ex)

    return batch_wrapper
