from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import hashlib
import json
from typing import Any

from app.comparators.record import RecordComparator


class ExecutionPartitioner(ABC):
    """
    Splits execution input into independently executable
    partitions.

    The partitioner does not execute the comparison.
    It only determines how the input is divided.
    """

    @abstractmethod
    def partition(
        self,
        data: Any,
        partition_count: int,
    ) -> list[Any]:
        raise NotImplementedError


class ListPartitioner(ExecutionPartitioner):
    """
    Simple partitioner for list-like data.

    Used as the foundation for later database,
    file, Spark and distributed partitioning.
    """

    def partition(
        self,
        data: list[Any],
        partition_count: int,
    ) -> list[list[Any]]:

        if partition_count <= 0:
            raise ValueError(
                "partition_count must be greater than zero."
            )

        if not data:
            return []

        partition_count = min(
            partition_count,
            len(data),
        )

        base_size = len(data) // partition_count
        remainder = len(data) % partition_count

        partitions = []

        start = 0

        for index in range(partition_count):

            size = (
                base_size
                + (1 if index < remainder else 0)
            )

            end = start + size

            partitions.append(
                data[start:end]
            )

            start = end

        return partitions


@dataclass
class IdentityPartitions:
    """
    Records routed by deterministic business identity.

    Records without a complete business key are kept out of
    normal keyed partitions so fallback matching can process
    them together later.
    """

    partitions: dict[int, list[dict[str, Any]]] = field(
        default_factory=dict
    )
    unkeyed_records: list[dict[str, Any]] = field(
        default_factory=list
    )


def partition_for_key(
    key_values: tuple[Any, ...] | list[Any],
    partition_count: int,
) -> int:
    """
    Return the deterministic partition for a business key.

    Python's built-in hash() is intentionally avoided because
    it is randomized between interpreter processes.
    """

    if partition_count <= 0:
        raise ValueError(
            "partition_count must be greater than zero."
        )

    canonical_key = canonical_key_for_partition(
        key_values
    )

    digest = hashlib.sha256(
        canonical_key.encode("utf-8")
    ).hexdigest()

    return int(digest, 16) % partition_count


def business_key_for_record(
    record: dict[str, Any],
    key_columns: list[str] | tuple[str, ...],
) -> tuple[Any, ...] | None:
    """
    Extract the L3 business key for a record.

    Returns None when any required key component is missing,
    None, or blank.
    """

    if not key_columns:
        return None

    key = tuple(
        record.get(column)
        for column in key_columns
    )

    if not RecordComparator._has_valid_key(key):
        return None

    return key


class IdentityPartitioner:
    """
    Deterministic business-key partitioner for L3 foundation work.

    This class only routes records. It does not execute L3 and
    does not modify fallback matching behavior.
    """

    def partition_records(
        self,
        records: list[dict[str, Any]],
        key_columns: list[str] | tuple[str, ...],
        partition_count: int,
    ) -> IdentityPartitions:

        if partition_count <= 0:
            raise ValueError(
                "partition_count must be greater than zero."
            )

        partitions = {
            partition_id: []
            for partition_id in range(partition_count)
        }

        unkeyed_records: list[dict[str, Any]] = []

        for record in records:
            key = business_key_for_record(
                record,
                key_columns,
            )

            if key is None:
                unkeyed_records.append(record)
                continue

            partition_id = partition_for_key(
                key,
                partition_count,
            )

            partitions[
                partition_id
            ].append(record)

        return IdentityPartitions(
            partitions=partitions,
            unkeyed_records=unkeyed_records,
        )


def canonical_key_for_partition(
    key_values: tuple[Any, ...] | list[Any],
) -> str:
    """
    Serialize key components with explicit tuple boundaries.
    """

    if not isinstance(
        key_values,
        (list, tuple),
    ):
        raise ValueError(
            "key_values must be a list or tuple."
        )

    payload = {
        "kind": "business_key",
        "components": [
            _canonical_component(value)
            for value in key_values
        ],
    }

    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _canonical_component(
    value: Any,
) -> dict[str, Any]:
    if value is None:
        return {
            "type": "none",
            "value": None,
        }

    if isinstance(value, bool):
        return {
            "type": "bool",
            "value": value,
        }

    if isinstance(value, int):
        return {
            "type": "int",
            "value": value,
        }

    if isinstance(value, float):
        return {
            "type": "float",
            "value": repr(value),
        }

    if isinstance(value, str):
        return {
            "type": "str",
            "value": value,
        }

    if isinstance(value, (list, tuple)):
        return {
            "type": type(value).__name__,
            "value": [
                _canonical_component(item)
                for item in value
            ],
        }

    if isinstance(value, dict):
        return {
            "type": "dict",
            "value": [
                [
                    _canonical_component(key),
                    _canonical_component(
                        nested_value
                    ),
                ]
                for key, nested_value
                in sorted(
                    value.items(),
                    key=lambda item: str(item[0]),
                )
            ],
        }

    return {
        "type": type(value).__name__,
        "value": str(value),
    }
