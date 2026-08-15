from __future__ import annotations

import hashlib
import json
from collections import Counter
from typing import Any

from app.metrics import safe_rate_pct


class RecordComparator:
    """L3 row reconciliation using configured primary keys only.

    A row is eligible for matching only when every key component is populated
    and the key occurs exactly once on both sides. Group reconciliation is a
    separate L3 capability and never creates row-level matched pairs.
    """

    def execute(self, task: Any) -> dict[str, Any]:
        configuration = task.configuration
        source_records = configuration.get("source_records")
        target_records = configuration.get("target_records")
        if source_records is None or target_records is None:
            raise ValueError("L3 requires source_records and target_records")

        comparison_keys = configuration.get("comparison_keys") or []
        is_group_mode = configuration.get("matching_mode") == "GROUP_RECONCILIATION"

        if is_group_mode:
            from app.comparators.group_reconciliation import GroupReconciliationComparator

            group_result = GroupReconciliationComparator().compare(
                source_records,
                target_records,
                configuration.get("grouping_attributes", []),
                configuration.get("aggregation_columns", []),
            )
            if not comparison_keys:
                return group_result

            row_result = self.compare(source_records, target_records, comparison_keys, configuration)
            row_metrics = dict(row_result["metrics"])
            group_metrics = dict(group_result.get("metrics", {}))
            return {
                "metrics": {
                    **row_metrics,
                    **group_metrics,
                    "matching_mode": "GROUP_RECONCILIATION",
                    "comparison_mode": "GROUP_RECONCILIATION",
                    "row_reconciliation": row_metrics,
                    "group_reconciliation": group_metrics,
                    "status": "FAIL" if row_metrics.get("status") == "FAIL" or group_metrics.get("status") == "FAIL" else "PASS",
                },
                "evidence": {
                    **row_result["evidence"],
                    "row_reconciliation": row_result["evidence"],
                    "group_reconciliation": group_result.get("evidence", {}).get("group_reconciliation", []),
                },
            }

        if not comparison_keys:
            raise ValueError("L3 row reconciliation requires comparison_keys")
        return self.compare(source_records, target_records, comparison_keys, configuration)

    def compare(
        self,
        source_records: list[dict[str, Any]],
        target_records: list[dict[str, Any]],
        comparison_keys: list[Any] | tuple[Any, ...],
        configuration: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        configuration = configuration or {}
        source_keys, target_keys = self._normalize_keys(comparison_keys)
        self._validate_keys(source_records, target_records, source_keys, target_keys)

        source_index = self._build_key_index(source_records, source_keys)
        target_index = self._build_key_index(target_records, target_keys)
        source_duplicates = self._find_duplicate_keys(source_records, source_keys)
        target_duplicates = self._find_duplicate_keys(target_records, target_keys)

        matched_keys = sorted(
            key for key in set(source_index) & set(target_index)
            if len(source_index[key]) == 1 and len(target_index[key]) == 1
        )
        matched_pairs = [
            {
                "signature": self._serialize_key(key),
                "source_record": source_index[key][0],
                "target_record": target_index[key][0],
                "match_type": "PRIMARY_KEY",
            }
            for key in matched_keys
        ]
        matched_source_ids = {id(pair["source_record"]) for pair in matched_pairs}
        matched_target_ids = {id(pair["target_record"]) for pair in matched_pairs}
        missing_records = [
            {"key": self._serialize_record_key(record, source_keys), "reason": "MISSING_IN_TARGET", "source_record": record, "target_record": None}
            for record in source_records if id(record) not in matched_source_ids
        ]
        extra_records = [
            {"key": self._serialize_record_key(record, target_keys), "reason": "MISSING_IN_SOURCE", "source_record": None, "target_record": record}
            for record in target_records if id(record) not in matched_target_ids
        ]

        hash_mismatches, exact_mismatches = self._compare_pairs(matched_pairs, configuration)
        mismatch_count = len(missing_records) + len(extra_records) + len(hash_mismatches) + len(exact_mismatches)
        source_count, target_count, matched_count = len(source_records), len(target_records), len(matched_pairs)
        return {
            "metrics": {
                "status": "PASS" if mismatch_count == 0 else "FAIL",
                "source_record_count": source_count,
                "target_record_count": target_count,
                "source_unique_key_count": len(source_index),
                "target_unique_key_count": len(target_index),
                "matched_key_count": matched_count,
                "primary_matched_count": matched_count,
                "missing_key_count": len(missing_records),
                "extra_key_count": len(extra_records),
                "source_duplicate_key_count": len(source_duplicates),
                "target_duplicate_key_count": len(target_duplicates),
                "duplicate_record_mismatch_count": 0,
                "full_row_hash_mismatch_count": len(hash_mismatches),
                "selected_column_hash_mismatch_count": 0,
                "hash_mismatch_count": len(hash_mismatches),
                "mismatch_count": mismatch_count,
                "ambiguous_record_count": 0,
                "source_record_coverage_pct": safe_rate_pct(matched_count, source_count, zero_value=100.0),
                "target_record_coverage_pct": safe_rate_pct(matched_count, target_count, zero_value=100.0),
                "missing_record_rate_pct": safe_rate_pct(len(missing_records), source_count),
                "extra_record_rate_pct": safe_rate_pct(len(extra_records), target_count),
                "ambiguous_record_rate_pct": 0.0,
                "matching_mode": configuration.get("matching_mode", "ROW_LEVEL"),
            },
            "evidence": {
                "matched_pairs": matched_pairs,
                "comparison_keys": [
                    {"source_column": source_key, "target_column": target_key}
                    for source_key, target_key in zip(source_keys, target_keys)
                ],
                "missing_keys": [item["key"] for item in missing_records],
                "extra_keys": [item["key"] for item in extra_records],
                "missing_records": missing_records[:100],
                "extra_records": extra_records[:100],
                "source_duplicate_keys": [self._serialize_key(key) for key in source_duplicates],
                "target_duplicate_keys": [self._serialize_key(key) for key in target_duplicates],
                "full_row_hash_mismatches": hash_mismatches,
                "selected_column_hash_mismatches": [],
                "duplicate_record_mismatches": [],
                "comparison_strategy": str(configuration.get("execution_mode", "HASH")).upper(),
                "exact_mismatch_count": len(exact_mismatches),
                "exact_mismatches": exact_mismatches,
            },
        }

    def _compare_pairs(self, pairs: list[dict[str, Any]], configuration: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        mappings = {item["source_column"]: item["target_column"] for item in configuration.get("column_mappings", []) if isinstance(item, dict) and item.get("source_column") and item.get("target_column")}
        ignored = set(configuration.get("ignored_columns", []))
        ignored_source = ignored | {
            source_column
            for source_column, target_column in mappings.items()
            if target_column in ignored
        }
        ignored_target = ignored | {
            target_column
            for source_column, target_column in mappings.items()
            if source_column in ignored
        }
        execution_mode = str(configuration.get("execution_mode", "HASH")).upper()
        hash_mismatches: list[dict[str, Any]] = []
        exact_mismatches: list[dict[str, Any]] = []
        for pair in pairs:
            source, target = pair["source_record"], pair["target_record"]
            if execution_mode == "EXACT":
                source_value = self._canonical_record(source, ignored_source, mappings)
                target_value = self._canonical_record(target, ignored_target, {})
                if source_value != target_value:
                    exact_mismatches.append({"key": pair["signature"], "source_record": source_value, "target_record": target_value})
            elif configuration.get("full_row_hash", True):
                source_value = self._hash_record(source, ignored_source, mappings)
                target_value = self._hash_record(target, ignored_target, {})
                if source_value != target_value:
                    hash_mismatches.append({"key": pair["signature"], "source_hash": source_value, "target_hash": target_value})
            else:
                raise ValueError(f"Unsupported L3 execution mode: {execution_mode}")
        return hash_mismatches, exact_mismatches

    @staticmethod
    def _normalize_keys(comparison_keys: list[Any] | tuple[Any, ...]) -> tuple[tuple[str, ...], tuple[str, ...]]:
        source, target = [], []
        for key in comparison_keys:
            if isinstance(key, str):
                source.append(key); target.append(key)
            elif isinstance(key, dict) and key.get("source_column") and key.get("target_column"):
                source.append(key["source_column"]); target.append(key["target_column"])
            else:
                raise ValueError("Comparison key must provide source_column and target_column")
        if not source:
            raise ValueError("L3 requires comparison_keys")
        return tuple(source), tuple(target)

    @staticmethod
    def _has_valid_key(key: tuple[Any, ...]) -> bool:
        return all(value is not None and not (isinstance(value, str) and not value.strip()) for value in key)

    @classmethod
    def _build_key_index(cls, records: list[dict[str, Any]], keys: tuple[str, ...]) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
        index: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for record in records:
            key = tuple(record.get(column) for column in keys)
            if cls._has_valid_key(key):
                index.setdefault(key, []).append(record)
        return index

    @classmethod
    def _find_duplicate_keys(cls, records: list[dict[str, Any]], keys: tuple[str, ...]) -> list[tuple[Any, ...]]:
        counts = Counter(tuple(record.get(column) for column in keys) for record in records)
        return sorted((key for key, count in counts.items() if cls._has_valid_key(key) and count > 1), key=str)

    @staticmethod
    def _validate_keys(source_records: list[dict[str, Any]], target_records: list[dict[str, Any]], source_keys: tuple[str, ...], target_keys: tuple[str, ...]) -> None:
        missing_source = [key for key in source_keys if any(key not in record for record in source_records)]
        missing_target = [key for key in target_keys if any(key not in record for record in target_records)]
        if missing_source or missing_target:
            raise ValueError(f"Comparison key missing from records: source={missing_source}, target={missing_target}")

    @staticmethod
    def _serialize_key(key: tuple[Any, ...]) -> str:
        return " | ".join("" if value is None else str(value) for value in key)

    @classmethod
    def _serialize_record_key(cls, record: dict[str, Any], keys: tuple[str, ...]) -> str:
        key = tuple(record.get(column) for column in keys)
        return cls._serialize_key(key) if cls._has_valid_key(key) else "Missing primary key"

    @classmethod
    def _hash_record(cls, record: dict[str, Any], ignored: set[str], mappings: dict[str, str]) -> str:
        return hashlib.sha256(json.dumps(cls._canonical_record(record, ignored, mappings), sort_keys=True, default=str).encode()).hexdigest()

    @staticmethod
    def _canonical_record(record: dict[str, Any], ignored: set[str], mappings: dict[str, str]) -> dict[str, Any]:
        return {mappings.get(key, key): value for key, value in record.items() if key not in ignored}
