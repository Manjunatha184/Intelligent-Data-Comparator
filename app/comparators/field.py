from __future__ import annotations

from typing import Any

from app.metrics import safe_rate_pct


class SparkFieldComparator:
    """L4 hash-gated field comparison on Spark DataFrames."""

    def execute(
        self,
        host: Any,
        source: Any,
        target: Any,
        configuration: dict[str, Any],
    ) -> dict[str, Any]:
        from pyspark.sql import functions as F
        from pyspark.sql.types import NumericType

        cfg = configuration
        s = source
        t = target
        keys = cfg.get("comparison_keys", [])
        if not keys:
            return {
                "metrics": {
                    "status": "NOT_APPLICABLE",
                    "comparison_mode": "GROUP_RECONCILIATION",
                    "reason": "Row-level field comparison is not applicable without row matches.",
                },
                "evidence": {},
            }

        reconciliation, match_counts, _ = host._matched_pairs(s, t, cfg)
        matched_reconciliation = reconciliation.filter(F.col("reconciliation_status") == "MATCHED")
        pairs = matched_reconciliation.select("_s", "_t", "match_type", "match_key")
        duplicate_pairs = matched_reconciliation.filter(
            (F.col("_source_key_count") > 1) | (F.col("_target_key_count") > 1)
        ).groupBy("normalized_primary_key").agg(
            F.first("match_key", ignorenulls=True).alias("key"),
            F.max("_source_key_count").alias("source_occurrences"),
            F.max("_target_key_count").alias("target_occurrences"),
            F.count(F.lit(1)).alias("compared_pairs"),
            F.first("_s", ignorenulls=True).alias("source_record"),
            F.first("_t", ignorenulls=True).alias("target_record"),
        ).select("key", "source_occurrences", "target_occurrences", "compared_pairs", "source_record", "target_record")

        resolved_pairs = host._resolve_l4_column_pairs(s.columns, t.columns, cfg)
        hashed_pairs = host._matched_row_hashes(pairs, resolved_pairs).persist()
        hash_summary = hashed_pairs.agg(
            F.sum(F.when(F.col("__source_row_hash") == F.col("__target_row_hash"), 1).otherwise(0)).alias("hash_equal"),
            F.sum(F.when(~F.col("__source_row_hash").eqNullSafe(F.col("__target_row_hash")), 1).otherwise(0)).alias("hash_changed"),
        ).first()
        hash_equal = int(hash_summary["hash_equal"] or 0)
        hash_changed = int(hash_summary["hash_changed"] or 0)
        comparison_pairs = hashed_pairs.filter(~F.col("__source_row_hash").eqNullSafe(F.col("__target_row_hash")))

        source_types = {field.name: field.dataType for field in s.schema.fields}
        target_types = {field.name: field.dataType for field in t.schema.fields}
        comps = []
        for source_column, target_column, mapping in resolved_pairs:
            source_value = F.col(f"_s.`{source_column}`")
            target_value = F.col(f"_t.`{target_column}`")
            source_compare = host._apply_mapping_normalization(source_value, mapping)
            target_compare = host._apply_mapping_normalization(target_value, mapping)
            exact_match = source_compare.eqNullSafe(target_compare)
            tolerance_pct = mapping.get("tolerance_pct")
            tolerance = mapping.get("tolerance")
            comparison_type = "EXACT"
            tolerance_type = None
            tolerance_value = None
            is_numeric_field = isinstance(source_types.get(source_column), NumericType) and isinstance(target_types.get(target_column), NumericType)

            if is_numeric_field:
                source_number = source_compare.cast("double")
                target_number = target_compare.cast("double")
                numeric_difference = target_number - source_number
                numeric_values = source_number.isNotNull() & target_number.isNotNull()
                difference = F.when(numeric_values, numeric_difference).otherwise(F.lit(None).cast("double"))
            else:
                source_number = None
                numeric_difference = None
                numeric_values = None
                difference = F.lit(None).cast("double")

            if tolerance_pct is not None:
                if is_numeric_field:
                    allowed = F.abs(source_number) * (F.lit(float(tolerance_pct)) / F.lit(100.0))
                    tolerance_match = numeric_values & (F.abs(numeric_difference) <= allowed)
                else:
                    tolerance_match = F.lit(False)
                comparison_type = "PERCENTAGE_TOLERANCE"
                tolerance_value = float(tolerance_pct)
                tolerance_type = "PERCENTAGE"
                matched_expr = exact_match | tolerance_match
            elif tolerance is not None:
                if is_numeric_field:
                    tolerance_match = numeric_values & (F.abs(numeric_difference) <= F.lit(float(tolerance)))
                else:
                    tolerance_match = F.lit(False)
                comparison_type = "NUMERIC_TOLERANCE"
                tolerance_value = float(tolerance)
                tolerance_type = "ABSOLUTE"
                matched_expr = exact_match | tolerance_match
            else:
                matched_expr = exact_match

            comps.append((source_column, target_column, ~matched_expr, difference, comparison_type, tolerance_value, tolerance_type))

        count_row = comparison_pairs.agg(
            F.count(F.lit(1)).alias("matched_record_count"),
            *[
                F.sum(F.when(bad, 1).otherwise(0)).cast("long").alias(f"mismatch_{index}")
                for index, (_, _, bad, _, _, _, _) in enumerate(comps)
            ],
        ).first() if comps else None

        field_statistics = []
        total_mismatch = 0
        for index, (source_column, target_column, _, _, comparison_type, _, _) in enumerate(comps):
            mismatch_count = int(count_row[f"mismatch_{index}"] or 0)
            total_mismatch += mismatch_count
            field_statistics.append({"field": source_column, "target_field": target_column, "mismatches": mismatch_count, "comparison_type": comparison_type})

        matched_records = int(match_counts.get("primary_matched_count") or 0)
        compared = matched_records * len(comps)
        mismatch_rows = F.array(*[
            F.when(bad, F.struct(
                F.col("match_key").alias("key"),
                F.col("match_type"),
                F.lit(source_column).alias("source_column"),
                F.lit(target_column).alias("target_column"),
                F.col(f"_s.`{source_column}`").alias("source_value"),
                F.col(f"_t.`{target_column}`").alias("target_value"),
                F.col("_s").alias("source_record"),
                F.col("_t").alias("target_record"),
                F.lit(False).alias("matched"),
                F.lit(comparison_type).alias("comparison_type"),
                difference.alias("difference"),
                F.lit(tolerance_value).cast("double").alias("tolerance"),
                F.lit(tolerance_type).cast("string").alias("tolerance_type"),
            ))
            for source_column, target_column, bad, difference, comparison_type, tolerance_value, tolerance_type in comps
        ])
        field_mismatches = comparison_pairs.select(F.explode(mismatch_rows).alias("mismatch")).filter(F.col("mismatch").isNotNull()).select("mismatch.*") if comps else None
        evidence_summary = field_mismatches.agg(F.countDistinct("key").alias("records_with_mismatch")).first() if field_mismatches is not None else None
        records_with_mismatch = int(evidence_summary["records_with_mismatch"] or 0) if evidence_summary is not None else 0
        source_stats = host._stats(s, cfg, "source")
        target_stats = host._stats(t, cfg, "target")

        return {
            "metrics": {
                "status": "PASS" if total_mismatch == 0 else "FAIL",
                "source_record_count": source_stats["total_rows"],
                "target_record_count": target_stats["total_rows"],
                "matched_record_count": matched_records,
                "compared_field_count": compared,
                "matched_field_count": compared - total_mismatch,
                "mismatch_count": total_mismatch,
                "field_conformity_pct": safe_rate_pct(compared - total_mismatch, compared, zero_value=100.0),
                "field_mismatch_rate_pct": safe_rate_pct(total_mismatch, compared),
                "records_with_mismatch": records_with_mismatch,
                "affected_record_rate_pct": safe_rate_pct(records_with_mismatch, matched_records),
                "hash_equal_record_count": hash_equal,
                "hash_changed_candidate_count": hash_changed,
                "hash_algorithm": "SHA-256",
                "source_duplicate_key_count": source_stats["duplicate_key_count"],
                "target_duplicate_key_count": target_stats["duplicate_key_count"],
                "missing_record_count": match_counts["missing_count"],
                "extra_record_count": match_counts["extra_count"],
                "ambiguous_record_count": 0,
            },
            "evidence": {
                "field_statistics": field_statistics,
                "comparison_keys": keys,
                "effective_column_mappings": [
                    {
                        "source_column": source_column,
                        "target_column": target_column,
                        "normalization": dict((mapping or {}).get("normalization") or {}),
                        "tolerance": (mapping or {}).get("tolerance"),
                        "tolerance_pct": (mapping or {}).get("tolerance_pct"),
                        "comparison_type": (
                            "PERCENTAGE_TOLERANCE" if (mapping or {}).get("tolerance_pct") is not None
                            else "NUMERIC_TOLERANCE" if (mapping or {}).get("tolerance") is not None
                            else "EXACT"
                        ),
                    }
                    for source_column, target_column, mapping in resolved_pairs
                ],
                "field_mismatches": host._bounded(field_mismatches, total_mismatch),
                "duplicate_matched_pairs": host._bounded(
                    duplicate_pairs,
                    match_counts["source_duplicate_key_count"] + match_counts["target_duplicate_key_count"],
                ),
            },
        }
