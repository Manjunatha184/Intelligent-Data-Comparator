"""Spark comparison level registry.

L1-L4 are physically extracted into dedicated Spark comparator modules.
L5-L6 remain thin adapters to the proven executor methods until their own
extraction step.
"""

from __future__ import annotations

from typing import Any, Protocol

from app.comparators.schema import SparkSchemaComparator
from app.comparators.volume import SparkVolumeComparator
from app.comparators.record import SparkRecordComparator
from app.comparators.field import SparkFieldComparator


class SparkComparatorHost(Protocol):
    """Executor services used by extracted and legacy Spark comparators."""

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
