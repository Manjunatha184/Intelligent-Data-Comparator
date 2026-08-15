from __future__ import annotations

from typing import Any

from app.metrics import (
    safe_percent_change,
    safe_rate_pct,
)


class VolumeComparator:
    """
    L2 - Volume Comparison.

    Compares connector-neutral volume statistics supplied
    through the execution task.

    The comparator does not access files, databases, APIs,
    warehouses, or lakehouses directly.
    """

    def execute(
        self,
        task: Any,
    ) -> dict[str, Any]:

        configuration = task.configuration

        source = configuration.get("source_statistics")
        target = configuration.get("target_statistics")

        if source is None:
            raise ValueError(
                "L2 requires source_statistics"
            )

        if target is None:
            raise ValueError(
                "L2 requires target_statistics"
            )

        comparison = self.compare(
            source=source,
            target=target,
            configuration=configuration,
        )

        return comparison

    def compare(
        self,
        source: dict[str, Any],
        target: dict[str, Any],
        configuration: dict[str, Any] | None = None,
    ) -> dict[str, Any]:

        configuration = configuration or {}

        total_rows = self._compare_metric(
            "total_rows",
            source,
            target,
        )

        filtered_rows = self._compare_metric(
            "filtered_rows",
            source,
            target,
        )

        partition_rows = self._compare_metric(
            "partition_rows",
            source,
            target,
        )

        distinct_keys = self._compare_metric(
            "distinct_key_count",
            source,
            target,
        )

        duplicate_keys = self._compare_metric(
            "duplicate_key_count",
            source,
            target,
        )

        null_counts = self._compare_null_counts(
            source.get("null_counts", {}),
            target.get("null_counts", {}),
            configuration,
        )

        checks = {
            "total_rows": total_rows,
            "filtered_rows": filtered_rows,
            "partition_rows": partition_rows,
            "distinct_key_count": distinct_keys,
            "duplicate_key_count": duplicate_keys,
            "null_counts": null_counts,
        }

        failed_checks = [
            name
            for name, result in checks.items()
            if not result["matched"]
        ]

        source_rows = source.get("total_rows")
        target_rows = target.get("total_rows")
        source_distinct = source.get(
            "distinct_key_count"
        )
        target_distinct = target.get(
            "distinct_key_count"
        )
        source_duplicates = source.get(
            "duplicate_key_count"
        )
        target_duplicates = target.get(
            "duplicate_key_count"
        )

        return {
            "metrics": {
                "status": (
                    "PASS"
                    if not failed_checks
                    else "FAIL"
                ),
                "checks_total": len(checks),
                "checks_failed": len(
                    failed_checks
                ),
                "checks_passed": (
                    len(checks)
                    - len(failed_checks)
                ),
                "total_rows_source": (
                    source_rows
                ),
                "total_rows_target": (
                    target_rows
                ),
                "row_count_percent_change": (
                    safe_percent_change(
                        source_rows,
                        target_rows,
                    )
                ),
                "volume_coverage_pct": (
                    safe_rate_pct(
                        target_rows,
                        source_rows,
                        zero_value=(
                            100.0
                            if target_rows == 0
                            else None
                        ),
                    )
                ),
                "distinct_key_count_source": (
                    source_distinct
                ),
                "distinct_key_count_target": (
                    target_distinct
                ),
                "distinct_key_percent_change": (
                    safe_percent_change(
                        source_distinct,
                        target_distinct,
                    )
                ),
                "duplicate_key_count_source": (
                    source_duplicates
                ),
                "duplicate_key_count_target": (
                    target_duplicates
                ),
                "source_duplicate_key_rate_pct": (
                    safe_rate_pct(
                        source_duplicates,
                        source_rows,
                    )
                ),
                "target_duplicate_key_rate_pct": (
                    safe_rate_pct(
                        target_duplicates,
                        target_rows,
                    )
                ),
            },
            "evidence": {
                "checks": checks,
                "failed_checks": failed_checks,
                "source": source,
                "target": target,
            },
        }

    # ========================================================
    # NUMERIC METRIC COMPARISON
    # ========================================================

    @staticmethod
    def _compare_metric(
        metric_name: str,
        source: dict[str, Any],
        target: dict[str, Any],
    ) -> dict[str, Any]:

        source_value = source.get(metric_name)
        target_value = target.get(metric_name)

        if (
            source_value is None
            and target_value is None
        ):
            return {
                "available": False,
                "matched": True,
                "metric": metric_name,
                "source": None,
                "target": None,
                "difference": None,
                "percentage_difference": None,
            }

        if (
            source_value is None
            or target_value is None
        ):
            return {
                "available": False,
                "matched": False,
                "metric": metric_name,
                "source": source_value,
                "target": target_value,
                "difference": None,
                "percentage_difference": None,
            }

        difference = (
            target_value - source_value
        )

        percentage_difference = (
            abs(difference)
            / abs(source_value)
            * 100
            if source_value != 0
            else (
                0.0
                if target_value == 0
                else None
            )
        )

        tolerance = 0

        matched = (
            abs(difference)
            <= tolerance
        )

        return {
            "available": True,
            "matched": matched,
            "metric": metric_name,
            "source": source_value,
            "target": target_value,
            "difference": difference,
            "percentage_difference": (
                percentage_difference
            ),
            "tolerance": tolerance,
        }

    # ========================================================
    # NULL COUNT COMPARISON
    # ========================================================

    @staticmethod
    def _compare_null_counts(
        source: dict[str, Any],
        target: dict[str, Any],
        configuration: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        configuration = configuration or {}
        ignored = set(configuration.get("ignored_columns", []))
        mappings = {
            item.get("source_column"): item.get("target_column")
            for item in configuration.get("column_mappings", [])
            if isinstance(item, dict)
            and item.get("source_column")
            and item.get("target_column")
        }

        results: dict[str, Any] = {}

        for column in sorted(source):
            target_column = mappings.get(column, column)
            if column in ignored or target_column in ignored:
                continue
            # L1 owns missing/unexpected columns. L2 only compares null
            # statistics for logical columns present on both sides.
            if target_column not in target:
                continue

            source_value = source.get(
                column,
                0,
            )

            target_value = target.get(
                target_column,
                0,
            )

            difference = (
                target_value - source_value
            )

            results[column] = {
                "matched": difference == 0,
                "source_column": column,
                "target_column": target_column,
                "source": source_value,
                "target": target_value,
                "difference": difference,
            }

        matched = all(
            result["matched"]
            for result in results.values()
        )

        return {
            "matched": matched,
            "columns": results,
        }
