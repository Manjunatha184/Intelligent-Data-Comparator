from __future__ import annotations

from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any


class GroupReconciliationComparator:
    """Compare independently aggregated logical groups for L3."""

    def compare(
        self,
        source_records: list[dict[str, Any]],
        target_records: list[dict[str, Any]],
        grouping_attributes: list[dict[str, str]],
        aggregation_columns: list[dict[str, str]],
    ) -> dict[str, Any]:
        if not grouping_attributes:
            raise ValueError("Group reconciliation requires grouping_attributes")
        if not aggregation_columns:
            raise ValueError("Group reconciliation requires aggregation_columns")

        self._validate_mappings(
            source_records,
            target_records,
            grouping_attributes,
            "grouping",
        )
        self._validate_mappings(
            source_records,
            target_records,
            aggregation_columns,
            "aggregation",
        )

        resolved_columns = []
        for mapping in aggregation_columns:
            # Resolve operation on this explicit pair only. Never classify
            # source and target columns in separate collections.
            resolved_columns.append({
                **mapping,
                "operation": self._infer_operation(
                    source_records, target_records, mapping
                ),
            })
        self._validate_resolved_mappings(aggregation_columns, resolved_columns)
        source_groups = self._aggregate(
            source_records,
            [{**item, "_side": "source"} for item in grouping_attributes],
            [{**item, "_side": "source"} for item in resolved_columns],
        )
        target_groups = self._aggregate(
            target_records,
            [{**item, "_side": "target"} for item in grouping_attributes],
            [{**item, "_side": "target"} for item in resolved_columns],
        )
        return self.compare_groups(source_groups, target_groups, grouping_attributes, resolved_columns)

    def compare_groups(self, source_groups, target_groups, grouping_attributes, aggregation_columns):
        keys = sorted(set(source_groups) | set(target_groups), key=repr)
        results = []

        for key in keys:
            source = source_groups.get(key)
            target = target_groups.get(key)
            if source is None:
                results.append(self._missing_result(key, target, "EXTRA_GROUP_IN_TARGET", aggregation_columns))
                continue
            if target is None:
                results.append(self._missing_result(key, source, "MISSING_GROUP_IN_TARGET", aggregation_columns))
                continue

            for column in aggregation_columns:
                source_value = source["values"].get(column["source_column"])
                target_value = target["values"].get(column["target_column"])
                operation = self._operation(column)
                difference = self._difference(source_value, target_value)
                if source_value is None and target_value is None:
                    row_status = "NOT_APPLICABLE"
                    matched = True
                else:
                    row_status = "PASS" if self._equal(source_value, target_value) else "GROUP_VALUE_MISMATCH"
                    matched = self._equal(source_value, target_value)
                results.append({
                    "group_key": list(key),
                    "source_aggregate": source_value,
                    "target_aggregate": target_value,
                    "source_column": column["source_column"],
                    "target_column": column["target_column"],
                    "operation": operation,
                    "difference": difference,
                    "matched": matched,
                    "status": row_status,
                })

        failed = sum(not item["matched"] for item in results)
        common_keys = set(source_groups) & set(target_groups)
        missing_keys = set(source_groups) - set(target_groups)
        extra_keys = set(target_groups) - set(source_groups)
        mismatch_keys = {
            tuple(item["group_key"])
            for item in results
            if item["status"] == "GROUP_VALUE_MISMATCH"
        }
        common_group_count = len(common_keys)
        group_count = len(keys)
        aggregate_results = [
            item for item in results
            if item["status"] not in {"MISSING_GROUP_IN_TARGET", "EXTRA_GROUP_IN_TARGET", "NOT_APPLICABLE"}
        ]
        aggregate_failed = sum(not item["matched"] for item in aggregate_results)
        return {
            "metrics": {
                "status": "PASS" if failed == 0 else "FAIL",
                "matching_mode": "GROUP_RECONCILIATION",
                "comparison_mode": "GROUP_RECONCILIATION",
                "source_group_count": len(source_groups),
                "target_group_count": len(target_groups),
                "group_count": group_count,
                "common_group_count": common_group_count,
                "matched_group_count": common_group_count,
                "missing_group_count": len(missing_keys),
                "extra_group_count": len(extra_keys),
                "groups_with_aggregate_mismatch": len(mismatch_keys),
                "groups_with_mismatch": len(mismatch_keys),
                "group_mismatch_count": len(mismatch_keys),
                "group_difference_count": len(missing_keys) + len(extra_keys) + len(mismatch_keys),
                "source_group_coverage_pct": (common_group_count / len(source_groups) * 100) if source_groups else None,
                "target_group_coverage_pct": (common_group_count / len(target_groups) * 100) if target_groups else None,
                "source_group_coverage": (common_group_count / len(source_groups) * 100) if source_groups else None,
                "target_group_coverage": (common_group_count / len(target_groups) * 100) if target_groups else None,
                "aggregate_checks_total": len(aggregate_results),
                "aggregate_check_count": len(aggregate_results),
                "aggregate_checks_passed": len(aggregate_results) - aggregate_failed,
                "aggregate_checks_failed": aggregate_failed,
                "checks_total": len(results),
                "checks_passed": len(results) - failed,
                "checks_failed": failed,
            },
            # Persist only actionable evidence.  Metrics retain the complete
            # result population so pass/fail counts remain accurate.
            "evidence": {
                "group_reconciliation": [
                    item for item in results
                    if item["status"] not in {"PASS", "NOT_APPLICABLE"}
                ]
            },
        }

    @staticmethod
    def _operation(column: dict[str, str]) -> str:
        return str(column.get("operation", "AVG")).upper()

    @staticmethod
    def _validate_resolved_mappings(original, resolved):
        expected = [
            (item.get("source_column"), item.get("target_column"))
            for item in original
        ]
        actual = [
            (item.get("source_column"), item.get("target_column"))
            for item in resolved
        ]
        if actual != expected:
            raise ValueError("Aggregation mapping order changed during operation inference")

    @staticmethod
    def _validate_mappings(source_records, target_records, mappings, kind):
        source_columns = set().union(*(record.keys() for record in source_records)) if source_records else set()
        target_columns = set().union(*(record.keys() for record in target_records)) if target_records else set()
        seen_source = set()
        seen_target = set()
        for mapping in mappings:
            source = mapping.get("source_column")
            target = mapping.get("target_column")
            if not source or not target:
                raise ValueError(f"Incomplete {kind} field mapping")
            if source in seen_source:
                raise ValueError(f"Duplicate source {kind} field: {source}")
            if target in seen_target:
                raise ValueError(f"Duplicate target {kind} field: {target}")
            if source not in source_columns:
                raise ValueError(f"Unknown source {kind} field: {source}")
            if target not in target_columns:
                raise ValueError(f"Unknown target {kind} field: {target}")
            seen_source.add(source)
            seen_target.add(target)
            if kind == "aggregation":
                source_numeric = GroupReconciliationComparator._has_numeric_value(source_records, source)
                target_numeric = GroupReconciliationComparator._has_numeric_value(target_records, target)
                if source_numeric != target_numeric:
                    raise ValueError(f"Incompatible aggregation types: {source} -> {target}")

    @staticmethod
    def _has_numeric_value(records, column):
        values = [record.get(column) for record in records if record.get(column) is not None and str(record.get(column)).strip()]
        if not values:
            return False
        try:
            for value in values:
                Decimal(str(value))
            return True
        except (InvalidOperation, ValueError):
            return False

    @staticmethod
    def _infer_operation(source_records, target_records, column):
        values = [
            record.get(name)
            for record in source_records + target_records
            for name in (column.get("source_column"), column.get("target_column"))
            if record.get(name) is not None and str(record.get(name)).strip()
        ]
        if values:
            try:
                for value in values:
                    Decimal(str(value))
                return "AVG"
            except (InvalidOperation, ValueError):
                pass
        return "MODE"

    def _aggregate(self, records, grouping, columns):
        groups = {}
        for record in records:
            key = tuple(record.get(item["source_column"]) for item in grouping)
            # Target records use the configured target-side grouping columns.
            if grouping and grouping[0].get("_side") == "target":
                key = tuple(record.get(item["target_column"]) for item in grouping)
            state = groups.setdefault(key, {"values": {}, "counts": defaultdict(Counter), "sums": defaultdict(Decimal)})
            for column in columns:
                name = column["source_column"] if column.get("_side") != "target" else column["target_column"]
                value = record.get(name)
                operation = self._operation(column)
                if value is None or (isinstance(value, str) and not value.strip()):
                    continue
                if operation == "MODE":
                    state["counts"][name][self._stable_value(value)] += 1
                elif operation == "AVG":
                    try:
                        state["sums"][name] += Decimal(str(value))
                        state.setdefault("numeric_counts", defaultdict(int))[name] += 1
                    except (InvalidOperation, ValueError):
                        continue
            for column in columns:
                name = column["source_column"] if column.get("_side") != "target" else column["target_column"]
                operation = self._operation(column)
                if operation == "MODE":
                    counts = state["counts"][name]
                    state["values"][name] = min(counts, key=lambda value: (-counts[value], repr(value))) if counts else None
                elif operation == "AVG":
                    count = state.get("numeric_counts", {}).get(name, 0)
                    state["values"][name] = float(state["sums"][name] / count) if count else None
        return groups

    @staticmethod
    def _stable_value(value):
        return value.strip() if isinstance(value, str) else value

    @staticmethod
    def _equal(left, right):
        return left == right

    @staticmethod
    def _difference(left, right):
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return right - left
        return "MATCH" if left == right else "VALUE_CHANGED"

    @staticmethod
    def _missing_result(key, group, status, columns):
        return {
            "group_key": list(key),
            "source_aggregate": None if status == "EXTRA_GROUP_IN_TARGET" else group["values"],
            "target_aggregate": group["values"] if status == "EXTRA_GROUP_IN_TARGET" else None,
            "source_column": None,
            "target_column": None,
            "operation": None,
            "difference": None,
            "matched": False,
            "status": status,
        }
