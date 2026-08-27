from __future__ import annotations

from typing import Any

from app.comparators.dq import DQComparator
from app.metrics import safe_rate_pct


class SparkDQComparator:
    """L6 data-quality comparison on Spark DataFrames."""

    def execute(
        self,
        host: Any,
        source: Any,
        target: Any,
        configuration: dict[str, Any],
    ) -> dict[str, Any]:
        from pyspark.sql import functions as F

        output = []
        rules_by_side = {"SOURCE": [], "TARGET": []}
        ignored = set(configuration.get("ignored_columns", []))

        comparison_keys = configuration.get("comparison_keys", []) or []

        def business_key_for_record(record: dict[str, Any], side: str) -> Any:
            key_values = []
            key_names = []
            key_field = "source_column" if side == "SOURCE" else "target_column"
            for mapping in comparison_keys:
                column = mapping.get(key_field)
                if not column:
                    continue
                key_names.append(column)
                key_values.append(record.get(column))

            if not key_values:
                return None
            if len(key_values) == 1:
                return key_values[0]
            return " | ".join(
                f"{name}={value if value not in (None, '') else '[NULL]'}"
                for name, value in zip(key_names, key_values)
            )

        for rule in configuration.get("dq_rules", []):
            if DQComparator._uses_ignored_column(rule, ignored):
                continue
            if not rule.get("enabled", True):
                continue

            rule_type = str(rule.get("rule_type", "")).upper()
            apply_to = str(rule.get("apply_to", "BOTH")).upper()

            for side, dataframe in (("SOURCE", source), ("TARGET", target)):
                if apply_to not in ("BOTH", side):
                    continue

                column = (
                    rule.get("source_column" if side == "SOURCE" else "target_column")
                    or rule.get("column")
                )
                if not column or column not in dataframe.columns:
                    continue

                value = F.col(column)
                invalid = None

                if rule_type == "PATTERN":
                    invalid = value.isNotNull() & ~value.cast("string").rlike(
                        rule.get("regex", "")
                    )
                elif rule_type == "COMPLETENESS":
                    invalid = value.isNull() | (F.trim(value.cast("string")) == "")
                elif rule_type == "VALIDITY":
                    allowed = rule.get("allowed_values") or (
                        rule.get("value")
                        if isinstance(rule.get("value"), list)
                        else None
                    )
                    if allowed is not None:
                        invalid = ~value.isin(allowed)
                    elif rule.get("min") is not None or rule.get("max") is not None:
                        numeric = value.cast("double")
                        invalid = F.lit(False) | numeric.isNull()
                        if rule.get("min") is not None:
                            invalid = invalid | (numeric < float(rule["min"]))
                        if rule.get("max") is not None:
                            invalid = invalid | (numeric > float(rule["max"]))

                if invalid is not None:
                    rules_by_side[side].append((rule, rule_type, column, invalid))

        for side, dataframe in (("SOURCE", source), ("TARGET", target)):
            side_rules = rules_by_side[side]
            if not side_rules:
                continue

            summary = dataframe.agg(
                F.count(F.lit(1)).alias("total"),
                *[
                    F.sum(F.when(invalid, 1).otherwise(0)).alias(f"failed_{index}")
                    for index, (_, _, _, invalid) in enumerate(side_rules)
                ],
            ).first()
            total = int(summary["total"] or 0)

            for index, (rule, rule_type, column, invalid) in enumerate(side_rules):
                failed = int(summary[f"failed_{index}"] or 0)
                item = {
                    "rule_id": rule.get("rule_id"),
                    "rule_name": rule.get("name"),
                    "rule_type": rule_type,
                    "side": side,
                    "column": column,
                    "total_count": total,
                    "failed_count": failed,
                    "passed_count": total - failed,
                    "status": "PASS" if failed == 0 else "FAIL",
                }

                if failed:
                    failed_rows = (
                        dataframe.filter(invalid)
                        .limit(host.evidence_limit)
                        .collect()
                    )
                    records = []
                    for row in failed_rows:
                        record = row.asDict(recursive=True)
                        business_key = business_key_for_record(record, side)
                        display_record = dict(record)
                        # Results.jsx currently prefers `id` when choosing the
                        # first-column key. Put the configured business key there
                        # until all result consumers use the explicit field below.
                        if business_key is not None:
                            display_record = {"id": business_key, **display_record}
                        records.append(
                            {
                                "business_key": business_key,
                                "record": display_record,
                                "column": column,
                                "value": record.get(column),
                                "rule": {
                                    "rule_id": rule.get("rule_id"),
                                    "name": rule.get("name"),
                                    "rule_type": rule_type,
                                },
                                "reason": f"{rule_type} validation failed",
                                "status": "FAIL",
                            }
                        )
                    item[
                        "source_failed_records"
                        if side == "SOURCE"
                        else "target_failed_records"
                    ] = records

                output.append(item)

        failed_rules = sum(1 for item in output if item["status"] == "FAIL")
        checks_total = sum(item["total_count"] for item in output)
        checks_failed = sum(item["failed_count"] for item in output)
        dq_results = [
            {**item, "matched": item["status"] == "PASS"}
            for item in output
        ]

        return {
            "metrics": {
                "status": "PASS" if checks_failed == 0 else "FAIL",
                "rules_total": len(output),
                "rules_failed": failed_rules,
                "rules_passed": len(output) - failed_rules,
                "checks_total": checks_total,
                "checks_passed": checks_total - checks_failed,
                "checks_failed": checks_failed,
                "pass_percentage": safe_rate_pct(
                    checks_total - checks_failed,
                    checks_total,
                    zero_value=100.0,
                ),
                "failure_percentage": safe_rate_pct(checks_failed, checks_total),
            },
            "evidence": {
                "dq_results": dq_results,
                "rule_results": dq_results,
            },
        }
