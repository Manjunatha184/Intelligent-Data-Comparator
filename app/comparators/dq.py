from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Callable
import re

from app.execution.models import ExecutionTask
from app.metrics import safe_rate_pct


class DQComparator:
   
    name = "DQComparator"

    def execute(self, task: ExecutionTask) -> dict[str, Any]:
        configuration = task.configuration

        source = configuration.get(
            "source_data",
            configuration.get("source"),
        )

        target = configuration.get(
            "target_data",
            configuration.get("target"),
        )
        rules = configuration.get("dq_rules", [])
        ignored_columns = set(configuration.get("ignored_columns", []))
        rules = [
            rule for rule in rules
            if not self._uses_ignored_column(rule, ignored_columns)
        ]


        if not isinstance(rules, list):
            raise ValueError("dq_rules must be a list")

        source_data = self._extract_data(source, "source")
        target_data = self._extract_data(target, "target")

        results: list[dict[str, Any]] = []

        for index, rule in enumerate(rules):
            rule_type = str(rule.get("type", rule.get("rule_type", ""))).upper()
            try:
                rule_results = self._evaluate_rule(
                    rule=rule,
                    source=source_data,
                    target=target_data,
                )
                results.extend(rule_results)
            except Exception as exc:
                raise

        passed = sum(
            1 for result in results
            if result.get("matched") is True
        )

        failed = sum(
            1 for result in results
            if result.get("matched") is False
        )

        total = len(results)

        status = "PASS" if failed == 0 else "FAIL"

        output = {
            "metrics": {
                "status": status,
                "rules_total": len(rules),
                "checks_total": total,
                "checks_passed": passed,
                "checks_failed": failed,
                "pass_percentage": (
                    (passed / total) * 100
                    if total
                    else 100.0
                ),
                "failure_percentage": (
                    safe_rate_pct(
                        failed,
                        total,
                    )
                ),
            },
            "evidence": {
                "dq_results": results,
            },
        }
        return output

    @staticmethod
    def _uses_ignored_column(rule: dict[str, Any], ignored: set[str]) -> bool:
        if not ignored:
            return False
        scalar_fields = (
            "column", "source_column", "target_column", "reference_column"
        )
        collection_fields = ("columns", "source_columns", "target_columns")
        if any(rule.get(field) in ignored for field in scalar_fields):
            return True
        return any(
            any(column in ignored for column in (rule.get(field) or []))
            for field in collection_fields
        )

    # ============================================================
    # DATA ACCESS
    # ============================================================

    def _extract_data(
        self,
        dataset: Any,
        side: str,
    ) -> list[dict[str, Any]]:
        
        if dataset is None:
            raise ValueError(
                f"{side} dataset is required for DQ comparison"
            )

        if isinstance(dataset, list):
            return [
                row for row in dataset
                if isinstance(row, dict)
            ]

        if isinstance(dataset, dict):

            if isinstance(
                dataset.get("data"),
                list,
            ):
                return [
                    row
                    for row in dataset["data"]
                    if isinstance(row, dict)
                ]

            if isinstance(
                dataset.get("records"),
                list,
            ):
                return [
                    row
                    for row in dataset["records"]
                    if isinstance(row, dict)
                ]

        raise ValueError(
            f"Unsupported {side} dataset representation"
        )

    # ============================================================
    # RULE DISPATCH
    # ============================================================

    def _evaluate_rule(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        rule_id = rule.get("rule_id")

        if not rule_id:
            raise ValueError(
                "Every DQ rule must contain rule_id"
            )

        # --------------------------------------------------------
        # Normalize domain DQRule -> comparator rule format
        # --------------------------------------------------------

        normalized_rule = dict(rule)

        if not normalized_rule.get("type"):
            normalized_rule["type"] = normalized_rule.get(
                "rule_type",
                "",
            )

        normalized_rule["apply_to"] = str(
            normalized_rule.get(
                "apply_to",
                normalized_rule.get("scope", "BOTH"),
            )
            or "BOTH"
        ).upper()

        if (
            normalized_rule.get("column")
            and not normalized_rule.get("source_column")
        ):
            normalized_rule["source_column"] = (
                normalized_rule["column"]
            )

        if (
            normalized_rule.get("column")
            and not normalized_rule.get("target_column")
        ):
            normalized_rule["target_column"] = (
                normalized_rule["column"]
            )

        rule_type = str(
            normalized_rule.get("type", "")
        ).upper()

        handlers: dict[
            str,
            Callable[..., list[dict[str, Any]]]
        ] = {
            "COMPLETENESS": self._check_completeness,
            "VALIDITY": self._check_validity,
            "CONSISTENCY": self._check_consistency,
            "TIMELINESS": self._check_timeliness,
            "CUSTOM": self._check_custom,
            "REFERENTIAL_INTEGRITY": (
                self._check_referential_integrity
            ),
            "PATTERN": self._check_pattern,
            "DISTRIBUTION": self._check_distribution,
            "CONDITIONAL": self._check_conditional,
            "TRANSFORMATION": self._check_transformation,
        }

        handler = handlers.get(rule_type)

        if handler is None:
            raise ValueError(
                f"Unsupported DQ rule type: {rule_type}"
            )

        if rule_type in {
            "COMPLETENESS",
            "VALIDITY",
            "TIMELINESS",
            "PATTERN",
            "DISTRIBUTION",
            "REFERENTIAL_INTEGRITY",
            "TRANSFORMATION",
        }:
            self._validate_scoped_columns(
                normalized_rule,
                source,
                target,
            )

        return handler(
            normalized_rule,
            source,
            target,
        )

    # ============================================================
    # SCOPE / COLUMN RESOLUTION
    # ============================================================

    @staticmethod
    def _active_sides(
        rule: dict[str, Any],
    ) -> set[str]:

        apply_to = str(
            rule.get("apply_to", "BOTH")
            or "BOTH"
        ).upper()

        if apply_to == "SOURCE":
            return {"SOURCE"}

        if apply_to == "TARGET":
            return {"TARGET"}

        if apply_to == "BOTH":
            return {
                "SOURCE",
                "TARGET",
            }

        raise ValueError(
            "apply_to must be SOURCE, TARGET, or BOTH"
        )

    @staticmethod
    def _column_for_side(
        rule: dict[str, Any],
        side: str,
    ) -> str | None:

        if side == "SOURCE":
            return (
                rule.get("source_column")
                or rule.get("column")
            )

        return (
            rule.get("target_column")
            or rule.get("column")
        )

    @classmethod
    def _validate_scoped_columns(
        cls,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> None:

        for side in cls._active_sides(rule):
            rows = source if side == "SOURCE" else target
            column = cls._column_for_side(
                rule,
                side,
            )

            if not column:
                raise ValueError(
                    f"{side} DQ rule requires a "
                    f"{side.lower()}_column"
                )

            cls._validate_column_exists(
                rows,
                column,
                side,
            )

    @staticmethod
    def _validate_column_exists(
        rows: list[dict[str, Any]],
        column: str,
        side: str,
    ) -> None:

        if not rows:
            return

        if any(
            column in row
            for row in rows
        ):
            return

        dataset_name = (
            "source"
            if side == "SOURCE"
            else "target"
        )

        raise ValueError(
            f"Column '{column}' does not exist in "
            f"{dataset_name} dataset"
        )

    # ============================================================
    # COMPLETENESS
    # ============================================================

    def _check_completeness(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        active_sides = self._active_sides(rule)
        source_column = self._column_for_side(
            rule,
            "SOURCE",
        )
        target_column = self._column_for_side(
            rule,
            "TARGET",
        )

        source_null_records = []
        source_nulls = 0
        if "SOURCE" in active_sides:
            for row in source:
                if self._is_null(row.get(source_column)):
                    source_nulls += 1
                    if len(source_null_records) < 100:
                        source_null_records.append({"record": row, "column": source_column, "value": row.get(source_column), "reason": "Value is missing"})

        target_null_records = []
        target_nulls = 0
        if "TARGET" in active_sides:
            for row in target:
                if self._is_null(row.get(target_column)):
                    target_nulls += 1
                    if len(target_null_records) < 100:
                        target_null_records.append({"record": row, "column": target_column, "value": row.get(target_column), "reason": "Value is missing"})

        source_rate = (
            self._rate(
                source_nulls,
                len(source),
            )
            if "SOURCE" in active_sides
            else None
        )

        target_rate = (
            self._rate(
                target_nulls,
                len(target),
            )
            if "TARGET" in active_sides
            else None
        )

        tolerance = self._tolerance(
            rule,
            "percentage",
            0.0,
        )

        if active_sides == {"SOURCE", "TARGET"}:
            matched = abs(
                source_rate - target_rate
            ) <= tolerance
        elif "SOURCE" in active_sides:
            matched = source_rate <= tolerance
        else:
            matched = target_rate <= tolerance

        return [
            self._result(
                rule,
                matched,
                source=source_rate,
                target=target_rate,
                details={
                    "source_null_count": source_nulls,
                    "target_null_count": target_nulls,
                    "source_null_rate": source_rate,
                    "target_null_rate": target_rate,
                    "source_failed_records": source_null_records,
                    "target_failed_records": target_null_records,
                },
            )
        ]

    # ============================================================
    # VALIDITY
    # ============================================================

    def _check_validity(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        active_sides = self._active_sides(rule)
        if "SOURCE" in active_sides:
            source_invalid, source_failed_records = self._get_invalid_records(
                source,
                rule,
                self._column_for_side(rule, "SOURCE"),
            )
        else:
            source_invalid = 0
            source_failed_records = []

        if "TARGET" in active_sides:
            target_invalid, target_failed_records = self._get_invalid_records(
                target,
                rule,
                self._column_for_side(rule, "TARGET"),
            )
        else:
            target_invalid = 0
            target_failed_records = []

        source_rate = (
            self._rate(
                source_invalid,
                len(source),
            )
            if "SOURCE" in active_sides
            else None
        )

        target_rate = (
            self._rate(
                target_invalid,
                len(target),
            )
            if "TARGET" in active_sides
            else None
        )

        rate_difference = (
            abs(source_rate - target_rate)
            if active_sides == {"SOURCE", "TARGET"}
            else None
        )

        # --------------------------------------------------------
        # TOLERANCE
        # --------------------------------------------------------

        tolerance_config = rule.get(
            "tolerance"
        )

        if isinstance(
            tolerance_config,
            dict,
        ):

            has_percentage = (
                "percentage"
                in tolerance_config
            )

            has_absolute = (
                "absolute"
                in tolerance_config
            )

        else:

            has_percentage = False
            has_absolute = (
                tolerance_config is not None
            )

        percentage_tolerance = (
            self._tolerance(
                rule,
                "percentage",
                0,
            )
            if has_percentage
            else 0.0
        )

        absolute_tolerance = (
            self._tolerance(
                rule,
                "absolute",
                0,
            )
            if has_absolute
            else 0.0
        )

        # --------------------------------------------------------
        # MATCH
        # --------------------------------------------------------

        if active_sides == {"SOURCE", "TARGET"} and has_percentage:

            matched = (
                rate_difference
                <= percentage_tolerance
            )

        elif active_sides == {"SOURCE", "TARGET"} and has_absolute:

            matched = (
                abs(
                    source_invalid
                    - target_invalid
                )
                <= absolute_tolerance
            )

        else:
            invalid_count = (
                source_invalid
                if "SOURCE" in active_sides
                else target_invalid
            )
            invalid_rate = (
                source_rate
                if "SOURCE" in active_sides
                else target_rate
            )

            if has_percentage:
                matched = invalid_rate <= percentage_tolerance
            elif has_absolute:
                matched = invalid_count <= absolute_tolerance
            elif active_sides == {"SOURCE", "TARGET"}:
                matched = rate_difference == 0
            else:
                matched = invalid_count == 0

        return [
            self._result(
                rule,
                matched,
                source=source_invalid,
                target=target_invalid,
                details={
                    "source_invalid_count": (
                        source_invalid
                    ),
                    "target_invalid_count": (
                        target_invalid
                    ),
                    "source_invalid_rate": (
                        source_rate
                    ),
                    "target_invalid_rate": (
                        target_rate
                    ),
                    "invalid_rate_difference": (
                        rate_difference
                    ),
                    "absolute_tolerance": (
                        absolute_tolerance
                        if has_absolute
                        else None
                    ),
                    "percentage_tolerance": (
                        percentage_tolerance
                        if has_percentage
                        else None
                    ),
                    "source_failed_records": source_failed_records[:100],
                    "target_failed_records": target_failed_records[:100],
                },
            )
        ]

    def _get_invalid_records(
        self,
        rows: list[dict[str, Any]],
        rule: dict[str, Any],
        column: str | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:

        column = column or rule.get("column")

        if not column:
            raise ValueError(
                "VALIDITY rule requires column"
            )

        allowed_values = rule.get("allowed_values") or None

        min_value = rule.get("min")
        max_value = rule.get("max")

        regex = rule.get("regex")
        if not isinstance(regex, str) or not regex.strip():
            regex = None

        invalid = 0
        failed_records = []

        for row in rows:

            value = row.get(column)

            valid = True
            reason = ""

            if allowed_values is not None:
                if value not in allowed_values:
                    valid = False
                    reason = f"value not in allowed_values: {allowed_values}"

            if min_value is not None and valid:
                try:
                    numeric_value = float(value)
                    numeric_min = float(min_value)
                except (TypeError, ValueError):
                    valid = False
                    reason = f"value is not numeric for min/max range: {value}"
                if valid and numeric_value < numeric_min:
                    valid = False
                    reason = f"value < min_value: {min_value}"

            if max_value is not None and valid:
                try:
                    numeric_value = float(value)
                    numeric_max = float(max_value)
                except (TypeError, ValueError):
                    valid = False
                    reason = f"value is not numeric for min/max range: {value}"
                if valid and numeric_value > numeric_max:
                    valid = False
                    reason = f"value > max_value: {max_value}"

            if regex is not None and valid:
                if not bool(re.fullmatch(regex, str(value))):
                    valid = False
                    reason = f"value does not match regex: {regex}"

            if not valid:
                invalid += 1
                if len(failed_records) < 100:
                    failed_records.append({
                        "record": row,
                        "column": column,
                        "value": value,
                        "rule": rule,
                        "reason": reason,
                        "status": "FAIL"
                    })

        return invalid, failed_records

    # ============================================================
    # CONSISTENCY
    # ============================================================

    def _check_consistency(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        source_failures, source_failed_records = self._get_consistency_failures(
            source,
            rule,
        )

        target_failures, target_failed_records = self._get_consistency_failures(
            target,
            rule,
        )

        tolerance = self._tolerance(
            rule,
            "absolute",
            0,
        )

        matched = abs(
            source_failures - target_failures
        ) <= tolerance

        return [
            self._result(
                rule,
                matched,
                source=source_failures,
                target=target_failures,
                details={
                    "source_failure_count": source_failures,
                    "target_failure_count": target_failures,
                    "source_failed_records": source_failed_records[:100],
                    "target_failed_records": target_failed_records[:100],
                },
            )
        ]

    def _get_consistency_failures(
        self,
        rows: list[dict[str, Any]],
        rule: dict[str, Any],
    ) -> tuple[int, list[dict[str, Any]]]:

        columns = rule.get("columns", [])

        if not columns:
            raise ValueError(
                "CONSISTENCY rule requires columns"
            )

        failures = 0
        failed_records = []

        for row in rows:

            values = [
                row.get(column)
                for column in columns
            ]

            if len(set(
                self._normalize(value)
                for value in values
            )) > 1:
                failures += 1
                if len(failed_records) < 100:
                    failed_records.append({
                        "record": row,
                        "column": ", ".join(columns),
                        "value": str(values),
                        "rule": rule,
                        "reason": "Inconsistent values across columns",
                        "status": "FAIL"
                    })

        return failures, failed_records

    # ============================================================
    # TIMELINESS
    # ============================================================

    def _check_timeliness(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        active_sides = self._active_sides(rule)
        source_column = self._column_for_side(
            rule,
            "SOURCE",
        )
        target_column = self._column_for_side(
            rule,
            "TARGET",
        )

        source_latest = (
            self._latest_timestamp(
                source,
                source_column,
            )
            if "SOURCE" in active_sides
            else None
        )

        target_latest = (
            self._latest_timestamp(
                target,
                target_column,
            )
            if "TARGET" in active_sides
            else None
        )

        if active_sides == {"SOURCE", "TARGET"}:
            if source_latest is None or target_latest is None:
                matched = False
                difference_seconds = None
            else:
                difference_seconds = abs(
                    (
                        target_latest
                        - source_latest
                    ).total_seconds()
                )

                tolerance = self._tolerance(
                    rule,
                    "seconds",
                    0,
                )

                matched = (
                    difference_seconds <= tolerance
                )
        elif "SOURCE" in active_sides:
            matched = source_latest is not None
            difference_seconds = None
        else:
            matched = target_latest is not None
            difference_seconds = None

        return [
            self._result(
                rule,
                matched,
                source=(
                    source_latest.isoformat()
                    if source_latest
                    else None
                ),
                target=(
                    target_latest.isoformat()
                    if target_latest
                    else None
                ),
                details={
                    "latency_seconds": difference_seconds,
                },
            )
        ]

    # ============================================================
    # REFERENTIAL INTEGRITY
    # ============================================================

    def _check_referential_integrity(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        source_column = rule.get("source_column")
        target_column = rule.get("target_column")

        if not source_column or not target_column:
            raise ValueError(
                "REFERENTIAL_INTEGRITY requires "
                "source_column and target_column"
            )

        source_values = {
            row.get(source_column)
            for row in source
        }

        target_invalid = 0
        target_failed_records = []
        for row in target:
            if row.get(target_column) not in source_values:
                target_invalid += 1
                if len(target_failed_records) < 100:
                    target_failed_records.append({
                        "record": row,
                        "column": target_column,
                        "value": row.get(target_column),
                        "rule": rule,
                        "reason": f"Value not found in source column '{source_column}'",
                        "status": "FAIL"
                    })

        tolerance = self._tolerance(
            rule,
            "absolute",
            0,
        )

        matched = target_invalid <= tolerance

        return [
            self._result(
                rule,
                matched,
                source=0,
                target=target_invalid,
                details={
                    "invalid_reference_count": target_invalid,
                    "target_failed_records": target_failed_records[:100],
                },
            )
        ]

    # ============================================================
    # PATTERN
    # ============================================================

    def _check_pattern(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        active_sides = self._active_sides(rule)
        source_column = self._column_for_side(
            rule,
            "SOURCE",
        )
        target_column = self._column_for_side(
            rule,
            "TARGET",
        )
        regex = rule.get("regex")

        if not regex:
            raise ValueError(
                "PATTERN requires regex"
            )

        if "SOURCE" in active_sides:
            source_matches, source_failed_records = self._pattern_rate(
                source,
                source_column,
                regex,
                rule,
            )
        else:
            source_matches = None
            source_failed_records = []

        if "TARGET" in active_sides:
            target_matches, target_failed_records = self._pattern_rate(
                target,
                target_column,
                regex,
                rule,
            )
        else:
            target_matches = None
            target_failed_records = []

        tolerance = self._tolerance(
            rule,
            "percentage",
            0,
        )

        if active_sides == {"SOURCE", "TARGET"}:
            matched = abs(
                source_matches - target_matches
            ) <= tolerance
        elif "SOURCE" in active_sides:
            matched = source_matches >= 100.0 - tolerance
        else:
            matched = target_matches >= 100.0 - tolerance

        return [
            self._result(
                rule,
                matched,
                source=source_matches,
                target=target_matches,
                details={
                    "source_match_rate": source_matches,
                    "target_match_rate": target_matches,
                    "source_failed_records": source_failed_records[:100],
                    "target_failed_records": target_failed_records[:100],
                },
            )
        ]

    # ============================================================
    # DISTRIBUTION
    # ============================================================

    def _check_distribution(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        active_sides = self._active_sides(rule)
        source_column = self._column_for_side(
            rule,
            "SOURCE",
        )
        target_column = self._column_for_side(
            rule,
            "TARGET",
        )

        source_distribution = (
            self._distribution(
                source,
                source_column,
            )
            if "SOURCE" in active_sides
            else {}
        )

        target_distribution = (
            self._distribution(
                target,
                target_column,
            )
            if "TARGET" in active_sides
            else {}
        )

        categories = (
            set(source_distribution)
            | set(target_distribution)
        )

        tolerance = self._tolerance(
            rule,
            "percentage",
            0,
        )

        mismatches = []

        for category in categories:

            source_rate = source_distribution.get(
                category,
                0.0,
            )

            target_rate = target_distribution.get(
                category,
                0.0,
            )

            if abs(
                source_rate - target_rate
            ) > tolerance:
                mismatches.append(
                    {
                        "value": category,
                        "source": source_rate,
                        "target": target_rate,
                        "difference": (
                            target_rate
                            - source_rate
                        ),
                    }
                )

        matched = (
            not mismatches
            if active_sides == {"SOURCE", "TARGET"}
            else True
        )

        return [
            self._result(
                rule,
                matched,
                source=source_distribution,
                target=target_distribution,
                details={
                    "distribution_mismatches": mismatches,
                },
            )
        ]

    # ============================================================
    # CONDITIONAL
    # ============================================================

    def _check_conditional(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        condition = rule.get("condition")
        check = rule.get("check")

        if not isinstance(condition, dict):
            raise ValueError(
                "CONDITIONAL requires condition"
            )

        if not isinstance(check, dict):
            raise ValueError(
                "CONDITIONAL requires check"
            )

        source_failures, source_failed_records = self._conditional_failures(
            source,
            condition,
            check,
            rule,
        )

        target_failures, target_failed_records = self._conditional_failures(
            target,
            condition,
            check,
            rule,
        )

        tolerance = self._tolerance(
            rule,
            "absolute",
            0,
        )

        matched = abs(
            source_failures - target_failures
        ) <= tolerance

        return [
            self._result(
                rule,
                matched,
                source=source_failures,
                target=target_failures,
                details={
                    "source_failure_count": source_failures,
                    "target_failure_count": target_failures,
                    "source_failed_records": source_failed_records[:100],
                    "target_failed_records": target_failed_records[:100],
                },
            )
        ]

    # ============================================================
    # TRANSFORMATION
    # ============================================================

    def _check_transformation(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        source_column = rule.get("source_column")
        target_column = rule.get("target_column")

        if not source_column or not target_column:
            raise ValueError(
                "TRANSFORMATION requires source_column "
                "and target_column"
            )

        transformation = rule.get(
            "transformation",
            "IDENTITY",
        )

        mismatches = 0

        for index in range(
            min(len(source), len(target))
        ):

            source_value = source[index].get(
                source_column
            )

            target_value = target[index].get(
                target_column
            )

            transformed = self._transform(
                source_value,
                transformation,
                rule,
            )

            if not self._values_equal(
                transformed,
                target_value,
                rule,
            ):
                mismatches += 1

        tolerance = self._tolerance(
            rule,
            "absolute",
            0,
        )

        return [
            self._result(
                rule,
                mismatches <= tolerance,
                source=0,
                target=mismatches,
                details={
                    "transformation_mismatch_count": mismatches,
                    "transformation": transformation,
                },
            )
        ]

    # ============================================================
    # CUSTOM
    # ============================================================

    def _check_custom(
        self,
        rule: dict[str, Any],
        source: list[dict[str, Any]],
        target: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:

        source_value = rule.get(
            "source_value",
            0,
        )

        target_value = rule.get(
            "target_value",
            0,
        )

        tolerance = self._tolerance(
            rule,
            "absolute",
            0,
        )

        matched = abs(
            source_value - target_value
        ) <= tolerance

        return [
            self._result(
                rule,
                matched,
                source=source_value,
                target=target_value,
            )
        ]

    # ============================================================
    # HELPERS
    # ============================================================

    def _result(
        self,
        rule: dict[str, Any],
        matched: bool,
        source: Any,
        target: Any,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:

        result = {
            "rule_id": rule["rule_id"],
            "type": rule.get("type"),
            "source": source,
            "target": target,
            "matched": matched,
            "rule": rule,
        }

        if details:
            result.update(details)

        return result

    def _tolerance(
        self,
        rule: dict[str, Any],
        key: str,
        default: float,
    ) -> float:

        tolerance = rule.get(
            "tolerance"
        )

        if tolerance is None:
            return float(default)

        if isinstance(tolerance, (int, float)):
            return float(tolerance)

        if isinstance(tolerance, dict):
            return float(
                tolerance.get(
                    key,
                    default,
                )
            )

        raise ValueError(
            f"Invalid tolerance configuration: {tolerance!r}"
        )

    @staticmethod
    def _is_null(value: Any) -> bool:
        return value is None or (
            isinstance(value, str)
            and value.strip() == ""
        )

    @staticmethod
    def _normalize(value: Any) -> Any:

        if isinstance(value, str):
            return value.strip().lower()

        return value

    @staticmethod
    def _rate(
        numerator: int,
        denominator: int,
    ) -> float:

        if denominator == 0:
            return 0.0

        return (
            numerator
            / denominator
            * 100
        )

    @staticmethod
    def _latest_timestamp(
        rows: list[dict[str, Any]],
        column: str,
    ) -> datetime | None:

        values = []

        for row in rows:

            value = row.get(column)

            if value is None:
                continue

            if isinstance(value, datetime):
                values.append(value)
                continue

            try:
                values.append(
                    datetime.fromisoformat(
                        str(value)
                    )
                )
            except ValueError:
                continue
        return max(values) if values else None

    @staticmethod
    def _pattern_rate(
        rows: list[dict[str, Any]],
        column: str,
        regex: str,
        rule: dict[str, Any],
    ) -> tuple[float, list[dict[str, Any]]]:

        if not rows:
            return 100.0, []

        matched = 0
        failed_records = []
        
        for row in rows:
            is_match = bool(re.fullmatch(regex, str(row.get(column))))
            if is_match:
                matched += 1
            else:
                if len(failed_records) < 100:
                    failed_records.append({
                        "record": row,
                        "column": column,
                        "value": row.get(column),
                        "rule": rule,
                        "reason": f"Value does not match regex '{regex}'",
                        "status": "FAIL"
                    })

        return (
            matched
            / len(rows)
            * 100
        ), failed_records

    @staticmethod
    def _distribution(
        rows: list[dict[str, Any]],
        column: str,
    ) -> dict[Any, float]:

        if not rows:
            return {}

        counts = Counter(
            row.get(column)
            for row in rows
        )

        total = len(rows)

        return {
            value: (
                count
                / total
                * 100
            )
            for value, count in counts.items()
        }

    @staticmethod
    def _conditional_failures(
        rows: list[dict[str, Any]],
        condition: dict[str, Any],
        check: dict[str, Any],
        rule: dict[str, Any],
    ) -> tuple[int, list[dict[str, Any]]]:

        condition_column = condition.get("column")
        condition_value = condition.get("equals")

        check_column = check.get("column")
        check_type = check.get(
            "type",
            "NOT_NULL",
        )

        failures = 0
        failed_records = []

        for row in rows:

            if row.get(condition_column) != condition_value:
                continue

            value = row.get(check_column)

            valid = True

            if check_type == "NOT_NULL":
                valid = value is not None

            elif check_type == "POSITIVE":
                valid = (
                    value is not None
                    and value > 0
                )

            elif check_type == "NOT_EMPTY":
                valid = (
                    value is not None
                    and str(value).strip() != ""
                )

            if not valid:
                failures += 1
                if len(failed_records) < 100:
                    failed_records.append({
                        "record": row,
                        "column": check_column,
                        "value": value,
                        "rule": rule,
                        "reason": f"Condition ({condition_column}={condition_value}) met, but check {check_type} failed",
                        "status": "FAIL"
                    })

        return failures, failed_records

    @staticmethod
    def _transform(
        value: Any,
        transformation: str,
        rule: dict[str, Any],
    ) -> Any:

        transformation = transformation.upper()

        if transformation == "IDENTITY":
            return value

        if transformation == "LOWER":
            return str(value).lower()

        if transformation == "UPPER":
            return str(value).upper()

        if transformation == "STRIP":
            return str(value).strip()

        if transformation == "ROUND":
            precision = int(
                rule.get("precision", 2)
            )
            return round(
                float(value),
                precision,
            )

        raise ValueError(
            f"Unsupported transformation: "
            f"{transformation}"
        )

    @staticmethod
    def _values_equal(
        left: Any,
        right: Any,
        rule: dict[str, Any],
    ) -> bool:

        if isinstance(left, (int, float)) and isinstance(
            right,
            (int, float),
        ):

            tolerance = DQComparator._tolerance(
                rule,
                "absolute",
                0,
            )

            return abs(
                float(left)
                - float(right)
            ) <= tolerance

        return left == right
