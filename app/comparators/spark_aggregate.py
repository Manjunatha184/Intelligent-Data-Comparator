from __future__ import annotations

from typing import Any

from app.comparators.aggregate import AggregateComparator
from app.metrics import safe_rate_pct


class SparkAggregateComparator:
    """L5 aggregate comparison on Spark DataFrames."""

    def execute(
        self,
        host: Any,
        source: Any,
        target: Any,
        configuration: dict[str, Any],
    ) -> dict[str, Any]:
        from pyspark.sql import functions as F

        rules = []
        ignored = set(configuration.get("ignored_columns", []))
        for rule in configuration.get("aggregate_rules", []):
            if AggregateComparator._uses_ignored_column(rule, ignored):
                continue
            operation = str(rule.get("function", rule.get("operation", ""))).upper()
            source_column = rule.get("source_column")
            target_column = rule.get("target_column") or source_column
            if operation not in {"SUM", "AVG", "MIN", "MAX", "COUNT"}:
                continue
            source_group_by = rule.get("source_group_by") or rule.get("group_by_columns") or []
            target_group_by = rule.get("target_group_by") or rule.get("group_by_columns") or []
            rules.append(
                (
                    rule,
                    operation,
                    source_column,
                    target_column,
                    source_group_by,
                    target_group_by,
                )
            )

        result_by_index: dict[int, dict[str, Any]] = {}

        ungrouped = [
            (index, rule)
            for index, rule in enumerate(rules)
            if not rule[4] and not rule[5]
        ]
        if ungrouped:
            source_values = source.agg(
                *[
                    host._agg_expr(operation, source_column).alias(f"value_{index}")
                    for index, (_, operation, source_column, _, _, _) in ungrouped
                ]
            ).first().asDict()
            target_values = target.agg(
                *[
                    host._agg_expr(operation, target_column).alias(f"value_{index}")
                    for index, (_, operation, _, target_column, _, _) in ungrouped
                ]
            ).first().asDict()

            for index, (rule, operation, source_column, target_column, _, _) in ungrouped:
                source_value = source_values[f"value_{index}"]
                target_value = target_values[f"value_{index}"]
                difference = (
                    None
                    if source_value is None or target_value is None
                    else float(target_value) - float(source_value)
                )
                tolerance = rule.get("tolerance")
                tolerance_pct = rule.get("tolerance_pct")

                if difference is None:
                    matched = source_value == target_value
                elif tolerance_pct is not None and source_value is not None:
                    matched = abs(difference) <= abs(float(source_value)) * (
                        float(tolerance_pct) / 100.0
                    )
                elif tolerance is not None:
                    matched = abs(difference) <= float(tolerance)
                else:
                    matched = source_value == target_value

                tolerance_evidence = (
                    {"percentage": float(tolerance_pct)}
                    if tolerance_pct is not None
                    else tolerance
                )
                result_by_index[index] = {
                    "rule_name": rule.get("name"),
                    "operation": operation,
                    "source_column": source_column,
                    "target_column": target_column,
                    "group": None,
                    "source": source_value,
                    "target": target_value,
                    "difference": difference,
                    "matched": matched,
                    "tolerance": tolerance_evidence,
                    "tolerance_pct": tolerance_pct,
                }

        for index, (
            rule,
            operation,
            source_column,
            target_column,
            source_group_by,
            target_group_by,
        ) in enumerate(rules):
            if not source_group_by and not target_group_by:
                continue

            source_agg = source.groupBy(*source_group_by).agg(
                host._agg_expr(operation, source_column).alias("sv")
            )
            target_agg = target.groupBy(*target_group_by).agg(
                host._agg_expr(operation, target_column).alias("tv")
            )

            condition = None
            for source_group, target_group in zip(source_group_by, target_group_by):
                pair = F.col(f"s.`{source_group}`").eqNullSafe(
                    F.col(f"t.`{target_group}`")
                )
                condition = pair if condition is None else condition & pair

            grouped = source_agg.alias("s").join(
                target_agg.alias("t"), condition, "full_outer"
            )
            difference_expr = F.col("tv").cast("double") - F.col("sv").cast("double")

            if rule.get("tolerance_pct") is not None:
                allowed = F.abs(F.col("sv").cast("double")) * (
                    F.lit(float(rule["tolerance_pct"])) / F.lit(100.0)
                )
                matched_expr = F.col("sv").eqNullSafe(F.col("tv")) | (
                    F.col("sv").isNotNull()
                    & F.col("tv").isNotNull()
                    & (F.abs(difference_expr) <= allowed)
                )
            elif rule.get("tolerance") is not None:
                matched_expr = F.col("sv").eqNullSafe(F.col("tv")) | (
                    F.col("sv").isNotNull()
                    & F.col("tv").isNotNull()
                    & (
                        F.abs(difference_expr)
                        <= F.lit(float(rule["tolerance"]))
                    )
                )
            else:
                matched_expr = F.col("sv").eqNullSafe(F.col("tv"))

            grouped = grouped.withColumn("matched", matched_expr)
            summary = grouped.agg(
                F.count(F.lit(1)).alias("total"),
                F.sum(F.when(~F.col("matched"), 1).otherwise(0)).alias("failed"),
            ).first()
            total = int(summary["total"] or 0)
            failed = int(summary["failed"] or 0)

            group_columns = [
                F.coalesce(
                    F.col(f"s.`{source_group}`"),
                    F.col(f"t.`{target_group}`"),
                ).alias(f"group_{group_index}")
                for group_index, (source_group, target_group) in enumerate(
                    zip(source_group_by, target_group_by)
                )
            ]
            failed_group_rows = (
                grouped.filter(~F.col("matched"))
                .select(
                    *group_columns,
                    F.col("sv").alias("source"),
                    F.col("tv").alias("target"),
                    difference_expr.alias("difference"),
                )
                .limit(host.evidence_limit)
                .collect()
            )

            tolerance = (
                {"percentage": float(rule["tolerance_pct"])}
                if rule.get("tolerance_pct") is not None
                else rule.get("tolerance")
            )
            group_results = []
            for failed_row in failed_group_rows:
                group_values = [
                    failed_row[f"group_{group_index}"]
                    for group_index in range(len(source_group_by))
                ]
                group_results.append(
                    {
                        "rule_name": rule.get("name"),
                        "operation": operation,
                        "source_column": source_column,
                        "target_column": target_column,
                        "group": (
                            group_values[0]
                            if len(group_values) == 1
                            else group_values
                        ),
                        "source": failed_row["source"],
                        "target": failed_row["target"],
                        "difference": failed_row["difference"],
                        "tolerance": tolerance,
                        "tolerance_pct": rule.get("tolerance_pct"),
                        "matched": False,
                    }
                )

            result_by_index[index] = {
                "rule_name": rule.get("name"),
                "operation": operation,
                "source_column": source_column,
                "target_column": target_column,
                "grouped": True,
                "checks": total,
                "failed": failed,
                "matched": failed == 0,
                "group_results": group_results,
            }

        results = [result_by_index[index] for index in range(len(rules))]
        failed_rules = sum(1 for item in results if not item["matched"])
        checks_total = sum(item.get("checks", 1) for item in results)
        checks_failed = sum(
            item.get("failed", 0)
            if item.get("grouped")
            else int(not item["matched"])
            for item in results
        )

        return {
            "metrics": {
                "status": "PASS" if failed_rules == 0 else "FAIL",
                "rules_total": len(rules),
                "checks_total": checks_total,
                "checks_passed": checks_total - checks_failed,
                "checks_failed": checks_failed,
                "aggregate_check_pass_rate_pct": safe_rate_pct(
                    checks_total - checks_failed,
                    checks_total,
                    zero_value=100.0,
                ),
                "aggregate_check_failure_rate_pct": safe_rate_pct(
                    checks_failed, checks_total
                ),
            },
            "evidence": {"aggregate_results": results},
        }
