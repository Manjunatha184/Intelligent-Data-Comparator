from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterator


@dataclass(frozen=True)
class ColumnMetadata:
    name: str
    data_type: str
    nullable: bool = True
    ordinal_position: int | None = None
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    metadata: dict[str, Any] = field(
        default_factory=dict
    )


@dataclass(frozen=True)
class DatasetSchema:
    columns: tuple[ColumnMetadata, ...]
    metadata: dict[str, Any] = field(
        default_factory=dict
    )


class MetadataProvider(ABC):

    @abstractmethod
    def get_schema(
        self,
        dataset: dict[str, Any],
    ) -> DatasetSchema:
        raise NotImplementedError

    def list_catalogs(
        self,
        properties: dict[str, Any],
    ) -> list[str]:
        raise NotImplementedError

    def list_schemas(
        self,
        properties: dict[str, Any],
        catalog: str | None = None,
    ) -> list[str]:
        raise NotImplementedError

    def list_tables(
        self,
        properties: dict[str, Any],
        schema: str | None = None,
        catalog: str | None = None,
    ) -> list[str]:
        raise NotImplementedError


class DataProvider(ABC):
    """
    Connector-neutral contract for retrieving comparison data.

    Connectors return normalized records. Comparators never know
    whether the records came from CSV, SQL, lakehouse, API, etc.
    """

    @abstractmethod
    def get_records(
        self,
        dataset: dict[str, Any],
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def iter_records(
        self,
        dataset: dict[str, Any],
    ) -> Iterator[dict[str, Any]]:
        raise NotImplementedError

    def iter_chunks(
        self,
        dataset: dict[str, Any],
        chunk_size: int = 1000,
    ) -> Iterator[list[dict[str, Any]]]:

        if chunk_size <= 0:
            raise ValueError(
                "chunk_size must be greater than zero"
            )

        chunk: list[dict[str, Any]] = []

        for record in self.iter_records(dataset):
            chunk.append(record)

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk


    def get_volume_statistics(
        self,
        dataset: dict[str, Any],
        business_keys: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:

        raise NotImplementedError(
            "Volume statistics pushdown is not supported."
        )

class ConnectionProvider(ABC):
    """
    Connector-specific connection validation.

    Used by the Connection Manager to verify
    that a configured connection is reachable.
    """

    @abstractmethod
    def test_connection(
        self,
        properties: dict[str, Any],
    ) -> dict[str, Any]:
        raise NotImplementedError
