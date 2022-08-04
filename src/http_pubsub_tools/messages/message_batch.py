import time
from logging import Logger
from typing import Iterable, List, Iterator, Dict, Set, Optional

from http_pubsub_tools.messages.decoders import DecodedMessage


class BatchStats:
    def __init__(self, received: int = 0, processed: int = 0, errors: int = 0):
        self.received = received
        self.processed = processed
        self.errors = errors

    def __add__(self, other):
        if not isinstance(other, BatchStats):
            raise TypeError(f'cannot sum {type(other)}')

        return BatchStats(
            self.received + other.received,
            self.processed + other.processed,
            self.errors + other.errors
        )

    def to_json(self):
        return dict(self.__dict__)


class MessageBatch(Iterable[DecodedMessage]):
    def __init__(self, batch: List[DecodedMessage], timeout: float, logger: Logger):
        self.batch = batch
        self.message_ids = {message.ack_id for message in batch}

        self.successful: Set[str] = set()
        self.failed: Dict[str, Optional[Exception]] = {}

        self.timeout = timeout
        self.logger = logger

    def __iter__(self) -> Iterator[DecodedMessage]:
        for message in self.batch:
            if self.is_timed_out():
                self.logger.info('Timeout reached. Stopping batch.')
                return 'Timeout reached'

            yield message

    def is_timed_out(self):
        return time.time() > self.timeout

    def success(self, message: DecodedMessage):
        self.successful.add(message.ack_id)

    def failure(self, message: DecodedMessage, ex: Exception):
        # TODO we should probably capture more info (e.g. stacktrace) for this to be useful
        self.failed[message.ack_id] = ex

    def stats(self) -> BatchStats:
        return BatchStats(
            received=len(self.batch),
            processed=len(self.successful),
            errors=len(self.failed)
        )
