from abc import ABC, abstractmethod
from typing import Any, Iterable, Iterator


class ExecutionStream(ABC):
    """
    Abstract streaming input.

    Provides execution data incrementally instead of
    requiring the complete dataset in memory.
    """

    @abstractmethod
    def read(self) -> Iterator[Any]:
        """
        Yield records incrementally.
        """
        raise NotImplementedError


class IterableStream(ExecutionStream):
    """
    Adapter that converts any iterable into an execution stream.
    """

    def __init__(
        self,
        source: Iterable[Any],
    ):
        self.source = source

    def read(self) -> Iterator[Any]:
        for item in self.source:
            yield item


class StreamBatcher:
    """
    Converts a stream into bounded batches.

    The batch size limits how much data is held in memory
    at one time.
    """

    def __init__(
        self,
        stream: ExecutionStream,
        batch_size: int,
    ):
        if batch_size <= 0:
            raise ValueError(
                "batch_size must be greater than zero."
            )

        self.stream = stream
        self.batch_size = batch_size

    def batches(self) -> Iterator[list[Any]]:
        batch: list[Any] = []

        for item in self.stream.read():

            batch.append(item)

            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch