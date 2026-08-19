from __future__ import annotations

from typing import Any

from app.metrics import safe_percent_change, safe_rate_pct


class SparkVolumeComparator:
    """L2 volume/statistics comparison on Spark DataFrames."""

    def execute(
        self,
        host: Any,
        source: Any,
        target: Any,
        configuration: dict[str, Any],
    ) -> dict[str, Any]:
        source_stats = host._stats(source, configuration, "source")
        target_stats = host._stats(target, configuration, "target")
        checks: dict[str, Any] = {}

        for name in (
            "total_rows",
            "filtered_rows",
            "partition_rows",
            "distinct_key_count",
            "duplicate_key_count",
        ):
            available = (
                source_stats[name] is not None or target_stats[name] is not None
            )
            difference = (
                None
                if not available
                or source_stats[name] is None
                or target_stats[name] is None
                else target_stats[name] - source_stats[name]
            )
            checks[name] = {
                "source": source_stats[name],
                "target": target_stats[name],
                "difference": difference,
                "percentage_difference": safe_percent_change(
                    source_stats[name], target_stats[name]
                ),
                "tolerance": None,
                "matched": source_stats[name] == target_stats[name],
                "available": available,
            }

        # Null counts use the same source -> target column mapping as L1/L4.
        # This avoids false failures for renamed fields such as
        # Email -> Target_Email.
        source_nulls = source_stats["null_counts"]
        target_nulls = target_stats["null_counts"]
        column_mappings = host._maps(configuration)
        ignored = set(configuration.get("ignored_columns", []))

        null_count_differences = []
        mapped_null_counts = []

        for source_column, source_count in source_nulls.items():
            if source_column in ignored:
                continue

            target_column = column_mappings.get(source_column, source_column)
            if target_column in ignored:
                continue

            # Schema-only missing/unexpected columns belong to L1.
            if target_column not in target_nulls:
                continue

            target_count = target_nulls[target_column]
            matched = source_count == target_count
            item = {
                "source_column": source_column,
                "target_column": target_column,
                "source": source_count,
                "target": target_count,
                "difference": target_count - source_count,
                "matched": matched,
            }
            mapped_null_counts.append(item)
            if not matched:
                null_count_differences.append(item)

        checks["null_counts"] = {
            "source": source_nulls,
            "target": target_nulls,
            "mapped_columns": mapped_null_counts,
            "differences": null_count_differences,
            "matched": len(null_count_differences) == 0,
            "available": True,
        }

        # filtered_rows and partition_rows are execution diagnostics, not
        # independent business validations.
        validation_names = [
            "total_rows",
            "distinct_key_count",
            "duplicate_key_count",
            "null_counts",
        ]
        applicable = [
            name
            for name in validation_names
            if checks[name].get("available", True)
        ]
        failed = [name for name in applicable if not checks[name]["matched"]]

        return {
            "metrics": {
                "status": "PASS" if not failed else "FAIL",
                "checks_total": len(applicable),
                "checks_failed": len(failed),
                "checks_passed": len(applicable) - len(failed),
                "total_rows_source": source_stats["total_rows"],
                "total_rows_target": target_stats["total_rows"],
                "distinct_key_count_source": source_stats["distinct_key_count"],
                "distinct_key_count_target": target_stats["distinct_key_count"],
                "duplicate_key_count_source": source_stats["duplicate_key_count"],
                "duplicate_key_count_target": target_stats["duplicate_key_count"],
                "row_count_percent_change": safe_percent_change(
                    source_stats["total_rows"], target_stats["total_rows"]
                ),
                "volume_coverage_pct": safe_rate_pct(
                    target_stats["total_rows"],
                    source_stats["total_rows"],
                    zero_value=(
                        100.0 if target_stats["total_rows"] == 0 else None
                    ),
                ),
                "distinct_key_percent_change": safe_percent_change(
                    source_stats["distinct_key_count"],
                    target_stats["distinct_key_count"],
                ),
                "source_duplicate_key_rate_pct": safe_rate_pct(
                    source_stats["duplicate_key_count"],
                    source_stats["total_rows"],
                ),
                "target_duplicate_key_rate_pct": safe_rate_pct(
                    target_stats["duplicate_key_count"],
                    target_stats["total_rows"],
                ),
            },
            "evidence": {
                "checks": checks,
                "failed_checks": failed,
                "source": source_stats,
                "target": target_stats,
            },
        }
