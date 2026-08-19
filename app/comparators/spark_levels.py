"""Spark comparison level adapters.

L1 and L2 are physically extracted into dedicated comparator modules. L3-L6
remain thin adapters to the existing SparkExecutor methods until their shared
reconciliation and cache dependencies are extracted in the next refactor step.
"""

from __future__ import annotations

from typing import Any, Protocol

from app.comparators.schema import SparkSchemaComparator
from app.comparators.volume import SparkVolumeComparator


class SparkComparatorHost(Protocol):
    """Methods still exposed by SparkExecutor to legacy level adapters."""

    def _l3(self, source: Any, target: Any, configuration: dict[str, Any]) -> dict[str, Any]: ...
    def _l4(self, source: Any, target: Any, configuration: dict[str, Any]) -> dict[str, Any]: ...
    def _l5(self, source: Any, target: Any, configuration: dict[str, Any]) -> dict[str, Any]: ...
    def _l6(self, source: Any, target: Any, configuration: dict[str, Any]) -> dict[str, Any]: ...


class _LegacySparkLevelComparator:
    method_name: str

    def execute(
        self,
        host: SparkComparatorHost,
        source: Any,
        target: Any,
        configuration: dict[str, Any],
    ) -> dict[str, Any]:
        return getattr(host, self.method_name)(source, target, configuration)


class SparkRecordComparator(_LegacySparkLevelComparator):
    """L3 record and group reconciliation adapter."""

    method_name = "_l3"


class SparkFieldComparator(_LegacySparkLevelComparator):
    """L4 hash-gated field comparison adapter."""

    method_name = "_l4"


class SparkAggregateComparator(_LegacySparkLevelComparator):
    """L5 Spark aggregate comparison adapter."""

    method_name = "_l5"


class SparkDQComparator(_LegacySparkLevelComparator):
    """L6 Spark data-quality comparison adapter."""

    method_name = "_l6"


SPARK_COMPARATORS = {
    "L1": SparkSchemaComparator(),
    "L2": SparkVolumeComparator(),
    "L3": SparkRecordComparator(),
    "L4": SparkFieldComparator(),
    "L5": SparkAggregateComparator(),
    "L6": SparkDQComparator(),
}
