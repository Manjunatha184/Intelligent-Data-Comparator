from __future__ import annotations

from collections import defaultdict
from typing import Any


class L7EvidenceBuilder:
    """
    Privacy boundary for L7.

    This class consumes internal L1-L6 results but emits only
    structural/statistical evidence. Raw records, matched_pairs,
    keys, source_record/target_record and source_value/target_value
    are NEVER included in the AI payload.
    """

    _RAW_KEYS = {
        "matched_pairs", "missing_records", "extra_records",
        "source_record", "target_record", "source_value",
        "target_value", "record", "records", "row", "rows",
        "key", "keys", "values", "sample", "data", "comparison_keys",
    }

    _L2_CHECK_NAMES = {
        "total_rows",
        "filtered_rows",
        "partition_rows",
        "distinct_key_count",
        "duplicate_key_count",
        "null_counts",
    }

    def build(self, level_results: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize(level_results)
        levels: dict[str, Any] = {}

        for level in ("L1", "L2", "L3", "L4", "L5", "L6"):
            result = normalized.get(level)
            if result:
                levels[level] = self._build_level(level, result)

        correlations = self._correlations(levels)

        payload = {
            "privacy_policy": {
                "raw_client_records_included": False,
                "matched_pairs_included": False,
                "record_keys_included": False,
                "raw_field_values_included": False,
                "only_derived_structural_and_statistical_evidence": True,
            },
            "levels": levels,
            "cross_level_correlations": correlations,
        }
        self.assert_privacy_safe(payload)
        return payload

    @classmethod
    def assert_privacy_safe(cls, payload: dict[str, Any]) -> None:
        """Fail closed if a raw-evidence key reaches the provider payload."""
        def walk(value: Any, path: str = "payload") -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    if key.lower() in cls._RAW_KEYS:
                        raise ValueError(f"Unsafe L7 evidence key at {path}.{key}")
                    walk(child, f"{path}.{key}")
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    walk(child, f"{path}[{index}]")

        walk(payload)

    def _build_level(self, level: str, result: dict[str, Any]) -> dict[str, Any]:
        metrics = dict(result.get("metrics") or {})
        evidence = dict(result.get("evidence") or {})

        if level == "L1":
            return self._l1(metrics, evidence)
        if level == "L2":
            return self._l2(metrics, evidence)
        if level == "L3":
            return self._l3(metrics)
        if level == "L4":
            return self._l4(metrics, evidence)
        if level == "L5":
            return self._l5(metrics, evidence)
        if level == "L6":
            return self._l6(metrics, evidence)
        return {"status": metrics.get("status", "UNKNOWN"), "metrics": self._safe_metrics(metrics)}

    def _l1(self, metrics: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": metrics.get("status", "UNKNOWN"),
            "metrics": self._safe_metrics(metrics),
            "structural_differences": {
                "missing_columns": metrics.get("missing_column_count", 0),
                "unexpected_columns": metrics.get("unexpected_column_count", 0),
                "data_type_mismatches": metrics.get("data_type_mismatch_count", 0),
                "nullable_mismatches": metrics.get("nullable_mismatch_count", 0),
                "length_mismatches": metrics.get("length_mismatch_count", 0),
                "precision_scale_mismatches": metrics.get("precision_scale_mismatch_count", 0),
                "column_order_mismatches": metrics.get("order_mismatch_count", 0),
            },
        }

    def _l2(self, metrics: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
        source = self._num(metrics.get("total_rows_source"))
        target = self._num(metrics.get("total_rows_target"))
        result = {
            "status": metrics.get("status", "UNKNOWN"),
            "metrics": self._safe_metrics(metrics),
            "statistics": {},
            "failed_checks": self._safe_l2_failed_checks(evidence.get("failed_checks")),
        }
        if source is not None and target is not None:
            result["statistics"]["record_count"] = self._delta(source, target)
        return result

    def _l3(self, metrics: dict[str, Any]) -> dict[str, Any]:
        source = self._num(metrics.get("source_record_count"))
        target = self._num(metrics.get("target_record_count"))
        matched = self._num(metrics.get("matched_key_count"))
        missing = self._num(metrics.get("missing_key_count")) or 0
        extra = self._num(metrics.get("extra_key_count")) or 0
        out = {
            "status": metrics.get("status", "UNKNOWN"),
            "metrics": self._safe_metrics(metrics),
            "record_population": {
                "matched": matched,
                "missing": missing,
                "extra": extra,
                "ambiguous": self._num(metrics.get("ambiguous_record_count")) or 0,
                "unmatchable_source": self._num(metrics.get("unmatchable_source_count")) or 0,
                "unmatchable_target": self._num(metrics.get("unmatchable_target_count")) or 0,
            },
        }
        if source is not None and target is not None:
            out["record_population"]["volume_delta"] = target - source
            out["record_population"]["target_change_percent"] = self._pct(source, target)
        if source and matched is not None:
            out["record_population"]["match_rate_percent"] = (matched / source) * 100
        return out

    def _l4(self, metrics: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
        items = self._evidence_items(evidence.get("field_mismatches"))
        by_field: dict[str, dict[str, Any]] = defaultdict(lambda: {"mismatches": 0, "numeric_delta_sum": 0.0, "numeric_delta_count": 0, "types": defaultdict(int)})
        for item in items:
            if not isinstance(item, dict):
                continue
            field = str(item.get("source_column") or item.get("target_column") or "UNKNOWN_FIELD")
            bucket = by_field[field]
            bucket["mismatches"] += 1
            ctype = item.get("comparison_type")
            if ctype:
                bucket["types"][str(ctype)] += 1
            delta = self._num(item.get("difference"))
            if delta is not None:
                bucket["numeric_delta_sum"] += delta
                bucket["numeric_delta_count"] += 1
        fields = []
        for field, bucket in by_field.items():
            fields.append({
                "field": field,
                "mismatches": bucket["mismatches"],
                "comparison_types": dict(bucket["types"]),
                "numeric_delta_sum": bucket["numeric_delta_sum"] if bucket["numeric_delta_count"] else None,
                "numeric_delta_count": bucket["numeric_delta_count"],
            })
        return {
            "status": metrics.get("status", "UNKNOWN"),
            "metrics": self._safe_metrics(metrics),
            "field_statistics": fields,
        }

    def _l5(self, metrics: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
        items = self._evidence_items(evidence.get("aggregate_results"))
        aggregates = []
        for item in items:
            if not isinstance(item, dict):
                continue
            aggregates.append({
                "operation": item.get("operation"),
                "source_column": item.get("source_column"),
                "target_column": item.get("target_column"),
                "matched": item.get("matched"),
                "source": self._num(item.get("source")),
                "target": self._num(item.get("target")),
                "difference": self._num(item.get("difference")),
                "percentage_difference": self._num(item.get("percentage_difference")),
                "tolerance": self._num(item.get("tolerance")),
            })
        return {
            "status": metrics.get("status", "UNKNOWN"),
            "metrics": self._safe_metrics(metrics),
            "aggregate_statistics": aggregates,
        }

    def _l6(self, metrics: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
        items = self._evidence_items(evidence.get("dq_results"))
        rules = []
        for item in items:
            if not isinstance(item, dict):
                continue
            rule = {
                "rule_id": item.get("rule_id"),
                "rule_type": item.get("rule_type", item.get("type")),
                "column": item.get("column", item.get("source_column")),
                "matched": item.get("matched"),
            }
            for key in ("source_count", "target_count", "source_null_count", "target_null_count", "difference", "percentage_difference", "pass_percentage", "failure_count"):
                value = self._num(item.get(key))
                if value is not None:
                    rule[key] = value
            rules.append(rule)
        return {
            "status": metrics.get("status", "UNKNOWN"),
            "metrics": self._safe_metrics(metrics),
            "quality_rule_statistics": rules,
        }

    def _correlations(self, levels: dict[str, Any]) -> list[dict[str, Any]]:
        out = []
        l2, l3 = levels.get("L2"), levels.get("L3")
        if l2 and l3 and "record_count" in l2.get("statistics", {}):
            rc = l2["statistics"]["record_count"]
            rp = l3.get("record_population", {})
            if rc.get("difference") is not None:
                vol_delta = rc["difference"]
                missing = rp.get("missing", 0)
                extra = rp.get("extra", 0)
                reconciles = vol_delta == -missing + extra
                out.append({
                    "type": "VOLUME_RECORD_RECONCILIATION",
                    "levels": ["L2", "L3"],
                    "volume_difference": vol_delta,
                    "missing_record_count": missing,
                    "extra_record_count": extra,
                    "reconciles": reconciles,
                    "interpretation": "L2 volume change is exactly reconciled by L3 missing/extra population." if reconciles else "L2 volume change is not fully reconciled by L3 missing/extra population.",
                })
        l1, l4 = levels.get("L1"), levels.get("L4")
        if l1 and l4:
            # We intentionally use counts here; no raw schema items cross the boundary.
            if l1.get("structural_differences", {}).get("length_mismatches", 0) or l1.get("structural_differences", {}).get("data_type_mismatches", 0):
                out.append({
                    "type": "SCHEMA_FIELD_INTERACTION",
                    "levels": ["L1", "L4"],
                    "schema_difference_count": l1.get("metrics", {}).get("mismatch_count", 0),
                    "field_mismatch_count": l4.get("metrics", {}).get("mismatch_count", 0),
                    "interpretation": "Schema differences coexist with field-level differences; the available evidence does not prove causality.",
                })
        l4, l5 = levels.get("L4"), levels.get("L5")
        if l4 and l5:
            field_delta = defaultdict(float)
            for item in l4.get("field_statistics", []):
                if item.get("numeric_delta_sum") is not None:
                    field_delta[item["field"]] += item["numeric_delta_sum"]
            for agg in l5.get("aggregate_statistics", []):
                col = agg.get("source_column") or agg.get("target_column")
                ad = agg.get("difference")
                if col and ad is not None and col in field_delta:
                    fd = field_delta[col]
                    out.append({
                        "type": "FIELD_AGGREGATE_RECONCILIATION",
                        "levels": ["L4", "L5"],
                        "field": col,
                        "field_numeric_delta": fd,
                        "aggregate_difference": ad,
                        "reconciles": abs(fd - ad) < 1e-9,
                        "interpretation": "Observed numeric field deltas fully reconcile the aggregate difference." if abs(fd - ad) < 1e-9 else "Observed numeric field deltas only partially or do not reconcile the aggregate difference.",
                    })
        return out

    @staticmethod
    def _normalize(level_results: dict[str, Any]) -> dict[str, dict[str, Any]]:
        out = {}
        for key, value in (level_results or {}).items():
            if hasattr(value, "model_dump"):
                value = value.model_dump(mode="json")
            if isinstance(value, dict):
                out[str(key)] = value
        return out

    @staticmethod
    def _safe_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
        safe = {}
        for key, value in metrics.items():
            if isinstance(value, (int, float, bool)) or value is None:
                safe[key] = value
            elif isinstance(value, str) and key in {"status", "operation", "mode", "execution_mode", "execution_location"}:
                safe[key] = value
        return safe

    @staticmethod
    def _evidence_items(value: Any) -> list[dict[str, Any]]:
        """Read only the bounded item collection, never forward it wholesale."""
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            for key in ("items", "sample"):
                items = value.get(key)
                if isinstance(items, list):
                    return [item for item in items if isinstance(item, dict)]
        return []

    def _safe_l2_failed_checks(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [
            item
            for item in value
            if isinstance(item, str) and item in self._L2_CHECK_NAMES
        ]

    @staticmethod
    def _num(value: Any) -> float | None:
        if isinstance(value, bool) or value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _pct(self, source: float, target: float) -> float | None:
        if source == 0:
            return 0.0 if target == 0 else None
        return ((target - source) / abs(source)) * 100

    def _delta(self, source: float, target: float) -> dict[str, float | None]:
        return {
            "baseline": source,
            "comparison": target,
            "difference": target - source,
            "difference_percent": self._pct(source, target),
        }
