"""Spark comparison level adapters.

This module gives each comparison level a small, explicit class without
changing the existing Spark comparison algorithms. The algorithms still live
on SparkExecutor during this first extraction step, so shared reconciliation,
statistics, dataset caches, and L3 -> L4 behavior remain exactly the same.

A later mechanical move can relocate each level's private implementation into
its own module once regression tests cover the current contracts.
"""

from __future__ import annotations

from typing import Any, Protocol


class SparkComparatorHost(Protocol):
    """Methods exposed by SparkExecutor to level adapters."""

    def _l1(self, source: Any, target: Any, configuration: dict[str, Any]) -> dict[str, Any]: ...
    def _l2(self, source: Any, target: Any, configuration: dict[str, Any]) -> dict[str, Any]: ...
    def _l3(self, source: Any, target: Any, configuration: dict[str, Any]) -> dict[str, Any]: ...
    def _l4(self, source: Any, target: Any, configuration: dict[str, Any]) -> dict[str, Any]: ...
    def _l5(self, source: Any, target: Any, configuration: dict[str, Any]) -> dict[str, Any]: ...
    def _l6(self, source: Any, target: Any, configuration: dict[str, Any]) -> dict[str, Any]: ...


class _SparkLevelComparator:
    method_name: str

    def execute(
        self,
        host: SparkComparatorHost,
        source: Any,
        target: Any,
        configuration: dict[str, Any],
    ) -> dict[str, Any]:
        return getattr(host, self.method_name)(source, target, configuration)


class SparkSchemaComparator(_SparkLevelComparator):
    """L1 schema comparison."""

    method_name = "_l1"


class SparkVolumeComparator(_SparkLevelComparator):
    """L2 volume/statistics comparison."""

    method_name = "_l2"


class SparkRecordComparator(_SparkLevelComparator):
    """L3 record and group reconciliation."""

    method_name = "_l3"


class SparkFieldComparator(_SparkLevelComparator):
    """L4 hash-gated field comparison."""

    method_name = "_l4"


class SparkAggregateComparator(_SparkLevelComparator):
    """L5 Spark aggregate comparison."""

    method_name = "_l5"


class SparkDQComparator(_SparkLevelComparator):
    """L6 Spark data-quality comparison."""

    method_name = "_l6"
