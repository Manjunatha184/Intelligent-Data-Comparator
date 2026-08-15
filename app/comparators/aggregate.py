from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from app.metrics import safe_rate_pct


class AggregateComparator:

    SUPPORTED_OPERATIONS = {
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "COUNT",
    }

    def execute(self, task: Any) -> dict[str, Any]:

        configuration = task.configuration

        source_records = configuration.get(
            "source_records"
        )

        target_records = configuration.get(
            "target_records"
        )

        aggregate_rules = configuration.get(
            "aggregate_rules",
            [],
        )
        ignored_columns = set(configuration.get("ignored_columns", []))
        aggregate_rules = [
            rule for rule in aggregate_rules
            if not self._uses_ignored_column(rule, ignored_columns)
        ]

        if source_records is None:
            raise ValueError(
                "L5 requires source_records"
            )

        if target_records is None:
            raise ValueError(
                "L5 requires target_records"
            )

        if not aggregate_rules:
            return {
                "metrics": {
                    "status": "NOT_APPLICABLE",
                    "rules_total": 0,
                    "checks_total": 0,
                    "checks_passed": 0,
                    "checks_failed": 0,
                },
                "evidence": {"aggregate_results": []},
            }

        return self.compare(
            source_records=source_records,
            target_records=target_records,
            aggregate_rules=aggregate_rules,
        )

    @staticmethod
    def _uses_ignored_column(rule: dict[str, Any], ignored: set[str]) -> bool:
        if not ignored:
            return False
        scalar_fields = ("source_column", "target_column", "column")
        collection_fields = (
            "group_by", "group_by_columns", "source_group_by", "target_group_by"
        )
        if any(rule.get(field) in ignored for field in scalar_fields):
            return True
        return any(
            any(column in ignored for column in (rule.get(field) or []))
            for field in collection_fields
        )

    # ============================================================
    # PUBLIC COMPARISON
    # ============================================================

    def compare(
        self,
        source_records: list[dict[str, Any]],
        target_records: list[dict[str, Any]],
        aggregate_rules: list[dict[str, Any]],
    ) -> dict[str, Any]:

        rule_results = []

        total_checks = 0
        passed_checks = 0
        failed_checks = 0

        for rule in aggregate_rules:
            try:
                results = self._execute_rule(
                    source_records=source_records,
                    target_records=target_records,
                    rule=rule,
                )
                rule_results.extend(results)
            except Exception as e:
                rule_results.append({
                    "rule_id": rule.get("rule_id") or rule.get("name", "unknown"),
                    "rule_name": rule.get("name", "unknown"),
                    "operation": str(rule.get("function", rule.get("operation", ""))).upper(),
                    "source_column": rule.get("source_column"),
                    "target_column": rule.get("target_column") or rule.get("source_column"),
                    "group": None,
                    "source": None,
                    "target": None,
                    "difference": None,
                    "percentage_difference": None,
                    "matched": False,
                    "error": str(e),
                    "tolerance": None
                })

        for result in rule_results:

            total_checks += 1

            if result["matched"]:
                passed_checks += 1
            else:
                failed_checks += 1

        return {
            "metrics": {
                "status": (
                    "PASS"
                    if failed_checks == 0
                    else "FAIL"
                ),
                "rules_total": len(
                    aggregate_rules
                ),
                "checks_total": total_checks,
                "checks_passed": passed_checks,
                "checks_failed": failed_checks,
                "aggregate_check_pass_rate_pct": (
                    safe_rate_pct(
                        passed_checks,
                        total_checks,
                        zero_value=100.0,
                    )
                ),
                "aggregate_check_failure_rate_pct": (
                    safe_rate_pct(
                        failed_checks,
                        total_checks,
                    )
                ),
            },
            "evidence": {
                "aggregate_results": rule_results,
            },
        }

    # ============================================================
    # RULE EXECUTION
    # ============================================================

    def _execute_rule(
        self,
        source_records: list[dict[str, Any]],
        target_records: list[dict[str, Any]],
        rule: dict[str, Any],
    ) -> list[dict[str, Any]]:

        operation = str(
            rule.get(
                "function",
                rule.get("operation", ""),
            )
        ).upper()

        if operation not in self.SUPPORTED_OPERATIONS:
            raise ValueError(
                "Unsupported aggregate operation "
                f"'{operation}'. Supported operations: "
                f"{sorted(self.SUPPORTED_OPERATIONS)}"
            )

        source_column = rule.get(
            "source_column"
        )

        target_column = rule.get(
            "target_column"
        )

        if operation != "COUNT":

            if not source_column:
                raise ValueError(
                    "Aggregate rule requires "
                    "source_column"
                )

            if not target_column:
                target_column = source_column

        group_by = rule.get(
            "group_by_columns",
            rule.get("group_by", []),
        )

        if isinstance(group_by, str):
            group_by = [group_by]

        source_group_by = rule.get(
            "source_group_by",
            group_by,
        )

        target_group_by = rule.get(
            "target_group_by",
            group_by,
        )

        if isinstance(
            source_group_by,
            str,
        ):
            source_group_by = [
                source_group_by
            ]

        if isinstance(
            target_group_by,
            str,
        ):
            target_group_by = [
                target_group_by
            ]

        # --------------------------------------------------------
        # Ungrouped aggregation
        # --------------------------------------------------------

        if not source_group_by and not target_group_by:

            source_value, source_null_count = self._aggregate(
                source_records,
                source_column,
                operation,
            )

            target_value, target_null_count = self._aggregate(
                target_records,
                target_column,
                operation,
            )

            return [
                self._build_result(
                    rule=rule,
                    operation=operation,
                    source_value=source_value,
                    target_value=target_value,
                    group_key=None,
                    source_null_count=source_null_count,
                    target_null_count=target_null_count,
                )
            ]

        # --------------------------------------------------------
        # Grouped aggregation
        # --------------------------------------------------------

        source_groups = self._group_records(
            source_records,
            source_group_by,
        )

        target_groups = self._group_records(
            target_records,
            target_group_by,
        )

        source_keys = set(
            source_groups.keys()
        )

        target_keys = set(
            target_groups.keys()
        )

        all_groups = (
            source_keys
            | target_keys
        )

        results = []

        for group_key in sorted(
            all_groups,
            key=str,
        ):

            source_group = source_groups.get(
                group_key,
                [],
            )

            target_group = target_groups.get(
                group_key,
                [],
            )

            source_value, source_null_count = self._aggregate(
                source_group,
                source_column,
                operation,
            )

            target_value, target_null_count = self._aggregate(
                target_group,
                target_column,
                operation,
            )

            result = self._build_result(
                rule=rule,
                operation=operation,
                source_value=source_value,
                target_value=target_value,
                group_key=self._serialize_group_key(
                    group_key
                ),
                source_null_count=source_null_count,
                target_null_count=target_null_count,
            )

            result["source_group_exists"] = (
                group_key in source_groups
            )

            result["target_group_exists"] = (
                group_key in target_groups
            )

            results.append(result)

        return results

    # ============================================================
    # AGGREGATION
    # ============================================================

    def _aggregate(
        self,
        records: list[dict[str, Any]],
        column: str | None,
        operation: str,
    ) -> tuple[Any, int]:

        if operation == "COUNT":

            if column is None:
                return len(records), 0

            null_count = sum(
                1
                for record in records
                if record.get(column) is None
                or (
                    isinstance(record.get(column), str)
                    and not record.get(column).strip()
                )
            )

            count = len(records) - null_count
            return count, null_count

        values = []
        null_count = 0

        for record in records:

            value = record.get(column)

            if value is None or (
                isinstance(value, str)
                and not value.strip()
            ):
                null_count += 1
                continue

            numeric_value = self._to_decimal(
                value
            )

            if numeric_value is None:
                raise ValueError(
                    f"Non-numeric value encountered "
                    f"for aggregate column '{column}': "
                    f"{value!r}"
                )

            values.append(
                numeric_value
            )

        if not values:
            return None, null_count

        if operation == "SUM":
            return sum(
                values,
                Decimal("0"),
            ), null_count

        if operation == "AVG":
            return (
                sum(
                    values,
                    Decimal("0"),
                )
                / Decimal(len(values))
            ), null_count

        if operation == "MIN":
            return min(values), null_count

        if operation == "MAX":
            return max(values), null_count

        raise ValueError(
            f"Unsupported operation: {operation}"
        )

    # ============================================================
    # GROUPING
    # ============================================================

    @staticmethod
    def _group_records(
        records: list[dict[str, Any]],
        group_by: list[str],
    ) -> dict[
        tuple[Any, ...],
        list[dict[str, Any]],
    ]:

        groups = {}

        for record in records:

            key = tuple(
                record.get(column)
                for column in group_by
            )

            groups.setdefault(
                key,
                [],
            ).append(record)

        return groups

    # ============================================================
    # RESULT
    # ============================================================

    def _build_result(
        self,
        rule: dict[str, Any],
        operation: str,
        source_value: Any,
        target_value: Any,
        group_key: Any,
        source_null_count: int = 0,
        target_null_count: int = 0,
    ) -> dict[str, Any]:

        difference = self._difference(
            source_value,
            target_value,
        )

        percentage_difference = (
            self._percentage_difference(
                source_value,
                target_value,
            )
        )

        matched = self._within_tolerance(
            source_value=source_value,
            target_value=target_value,
            difference=difference,
            percentage_difference=percentage_difference,
            rule=rule,
        )

        result = {
            "rule_id": rule.get("rule_id") or rule.get("name"),
            "rule_name": rule.get("name"),
            "operation": operation,
            "source_column": rule.get(
                "source_column"
            ),
            "target_column": rule.get(
                "target_column"
            ) or rule.get(
                "source_column"
            ),
            "group": group_key,
            "source": self._serialize_value(
                source_value
            ),
            "target": self._serialize_value(
                target_value
            ),
            "source_null_count": source_null_count,
            "target_null_count": target_null_count,
            "difference": self._serialize_value(
                difference
            ),
            "percentage_difference": (
                percentage_difference
            ),
            "matched": matched,
            "tolerance": (
                {
                    "absolute": rule.get(
                        "tolerance"
                    ).get("absolute") if isinstance(rule.get("tolerance"), dict) else rule.get("tolerance"),
                    "percentage": rule.get("tolerance_pct") if rule.get("tolerance_pct") is not None else (
                        rule.get("tolerance").get("percentage") if isinstance(rule.get("tolerance"), dict) else None
                    ),
                }
            ),
        }

        # Format tolerance specifically for the UI as requested if percentage tolerance is configured
        if rule.get("tolerance_pct") is not None:
            result["tolerance"] = f"{float(rule.get('tolerance_pct'))}%"
        elif isinstance(rule.get("tolerance"), dict) and rule.get("tolerance").get("percentage") is not None:
            result["tolerance"] = f"{float(rule.get('tolerance').get('percentage'))}%"
        elif not isinstance(rule.get("tolerance"), dict) and rule.get("tolerance") is not None:
            result["tolerance"] = float(rule.get("tolerance"))
        else:
            result["tolerance"] = None

        return result

    # ============================================================
    # TOLERANCE
    # ============================================================

    @staticmethod
    def _within_tolerance(
        source_value: Any,
        target_value: Any,
        difference: Any,
        percentage_difference: float | None,
        rule: dict[str, Any],
    ) -> bool:

        if source_value is None and target_value is None:
            return True

        if source_value is None or target_value is None:
            return False

        tolerance = rule.get("tolerance")
        tolerance_pct = rule.get("tolerance_pct")

        # --------------------------------------------------------
        # PRECEDENCE
        # --------------------------------------------------------

        if tolerance_pct is not None:
            absolute_tolerance = None
            percentage_tolerance = tolerance_pct
        else:
            # Fallback to legacy tolerance
            if isinstance(tolerance, dict):
                absolute_tolerance = tolerance.get("absolute")
                percentage_tolerance = tolerance.get("percentage")
            else:
                absolute_tolerance = tolerance
                percentage_tolerance = None

        # --------------------------------------------------------
        # NO TOLERANCE
        # --------------------------------------------------------

        if absolute_tolerance is None and percentage_tolerance is None:
            return difference == 0

        # --------------------------------------------------------
        # ABSOLUTE TOLERANCE
        # --------------------------------------------------------

        if absolute_tolerance is not None:

            try:

                absolute_value = Decimal(
                    str(absolute_tolerance)
                )

            except (
                InvalidOperation,
                ValueError,
                TypeError,
            ):

                raise ValueError(
                    "Invalid aggregate absolute "
                    f"tolerance: {absolute_tolerance!r}"
                )

            if difference is not None:

                if abs(
                    Decimal(str(difference))
                ) <= absolute_value:

                    return True

        # --------------------------------------------------------
        # PERCENTAGE TOLERANCE
        # --------------------------------------------------------

        if percentage_tolerance is not None:

            try:

                percentage_value = Decimal(
                    str(percentage_tolerance)
                )

            except (
                InvalidOperation,
                ValueError,
                TypeError,
            ):

                raise ValueError(
                    "Invalid aggregate percentage "
                    f"tolerance: {percentage_tolerance!r}"
                )

            if percentage_difference is not None:

                if (
                    Decimal(
                        str(percentage_difference)
                    )
                    <= percentage_value
                ):

                    return True

        # --------------------------------------------------------
        # NO VALID TOLERANCE MATCHED
        # --------------------------------------------------------

        return False

    # ============================================================
    # DIFFERENCE
    # ============================================================

    @staticmethod
    def _difference(
        source_value: Any,
        target_value: Any,
    ) -> Any:

        if source_value is None:
            return None

        if target_value is None:
            return None

        try:
            return (
                target_value
                - source_value
            )

        except TypeError:
            return None

    @staticmethod
    def _percentage_difference(
        source_value: Any,
        target_value: Any,
    ) -> float | None:

        if source_value is None:
            return None

        if target_value is None:
            return None

        try:

            source = Decimal(
                str(source_value)
            )

            target = Decimal(
                str(target_value)
            )

        except (
            InvalidOperation,
            ValueError,
            TypeError,
        ):

            return None

        if source == 0:

            if target == 0:
                return 0.0

            return None

        difference = (
            target - source
        )

        return float(
            (
                abs(difference)
                / abs(source)
            )
            * Decimal("100")
        )

    # ============================================================
    # VALUE CONVERSION
    # ============================================================

    @staticmethod
    def _to_decimal(
        value: Any,
    ) -> Decimal | None:

        if isinstance(
            value,
            bool,
        ):
            return None

        try:

            return Decimal(
                str(value)
            )

        except (
            InvalidOperation,
            ValueError,
            TypeError,
        ):

            return None

    # ============================================================
    # SERIALIZATION
    # ============================================================

    @staticmethod
    def _serialize_value(
        value: Any,
    ) -> Any:

        if isinstance(
            value,
            Decimal,
        ):
            return float(value)

        return value

    @staticmethod
    def _serialize_group_key(
        key: tuple[Any, ...],
    ) -> Any:

        if len(key) == 1:
            return key[0]

        return list(key)
