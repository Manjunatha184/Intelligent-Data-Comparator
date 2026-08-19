from __future__ import annotations

import logging
from time import perf_counter
from typing import Any

from app.metrics import safe_rate_pct

logger = logging.getLogger(__name__)


class SparkRecordComparator:
    """L3 record reconciliation on Spark DataFrames."""

    def execute(
        self,
        host: Any,
        source: Any,
        target: Any,
        configuration: dict[str, Any],
    ) -> dict[str, Any]:
        from pyspark.sql import functions as F

        cfg = configuration
        s = source
        t = target

        if not cfg.get("comparison_keys"):
            if cfg.get("matching_mode") == "GROUP_RECONCILIATION":
                return host._group(s, t, cfg)
            raise ValueError("Spark L3 requires comparison_keys")

        reconciliation, counts, pk_timing = host._matched_pairs(s, t, cfg)
        pairs = reconciliation.filter(F.col("reconciliation_status") == "MATCHED").select("_s", "_t", "match_type", "match_key")
        missing = reconciliation.filter(F.col("reconciliation_status") == "MISSING_IN_TARGET").select(F.col("match_key").alias("key"), F.col("_s").alias("record"))
        extra = reconciliation.filter(F.col("reconciliation_status") == "EXTRA_IN_TARGET").select(F.col("match_key").alias("key"), F.col("_t").alias("record"))
        source_duplicates = reconciliation.filter(F.col("reconciliation_status") == "DUPLICATE_IN_SOURCE").groupBy("match_key").agg(
            F.count(F.lit(1)).alias("duplicate_count"), F.first("_s", ignorenulls=True).alias("record")
        ).select(F.col("match_key").alias("key"), "duplicate_count", "record")
        target_duplicates = reconciliation.filter(F.col("reconciliation_status") == "DUPLICATE_IN_TARGET").groupBy("match_key").agg(
            F.count(F.lit(1)).alias("duplicate_count"), F.first("_t", ignorenulls=True).alias("record")
        ).select(F.col("match_key").alias("key"), "duplicate_count", "record")
        duplicate_key_reconciliation = reconciliation.filter(
            F.col("normalized_primary_key").isNotNull()
        ).groupBy("normalized_primary_key").agg(
            F.first("match_key", ignorenulls=True).alias("key"),
            F.max(F.coalesce(F.col("_source_key_count"), F.lit(0))).alias("source_occurrences"),
            F.max(F.coalesce(F.col("_target_key_count"), F.lit(0))).alias("target_occurrences"),
            F.sum(F.when(
                F.col("_source_key_count").isNotNull() & F.col("_target_key_count").isNotNull(), 1
            ).otherwise(0)).alias("compared_pairs"),
            F.first(F.when(F.col("_source_key_count").isNotNull(), F.col("_s")), ignorenulls=True).alias("source_record"),
            F.first(F.when(F.col("_target_key_count").isNotNull(), F.col("_t")), ignorenulls=True).alias("target_record"),
        ).filter(
            (F.col("source_occurrences") > 1) | (F.col("target_occurrences") > 1)
        ).select("key", "source_occurrences", "target_occurrences", "compared_pairs", "source_record", "target_record")
        unmatchable_source = reconciliation.filter(F.col("reconciliation_status") == "UNMATCHABLE_SOURCE").select(F.col("_s").alias("record"))
        unmatchable_target = reconciliation.filter(F.col("reconciliation_status") == "UNMATCHABLE_TARGET").select(F.col("_t").alias("record"))

        source_stats = host._stats(s, cfg, "source")
        target_stats = host._stats(t, cfg, "target")
        sc = source_stats["total_rows"]
        tc = target_stats["total_rows"]
        mic = counts["missing_count"]
        ec = counts["extra_count"]
        mc = counts["matched_key_count"]
        pmc = counts["primary_matched_count"]
        sdk = int(source_stats.get("duplicate_key_count") or 0)
        tdk = int(target_stats.get("duplicate_key_count") or 0)
        sdg = counts["source_duplicate_key_count"]
        tdg = counts["target_duplicate_key_count"]
        sdr = sdk + sdg
        tdr = tdk + tdg
        usc = counts["unmatchable_source_count"]
        utc = counts["unmatchable_target_count"]
        all_rows_have_usable_keys = usc == 0 and utc == 0
        needs_secondary_reconciliation = (usc + utc + mic + ec) > 0
        effective_matching_mode = cfg.get("matching_mode", "ROW_LEVEL") if needs_secondary_reconciliation else "ROW_LEVEL"

        metrics = {
            "status": "PASS" if mic + ec + usc + utc == 0 else "FAIL",
            "source_record_count": sc,
            "target_record_count": tc,
            "source_unique_key_count": source_stats["distinct_key_count"],
            "target_unique_key_count": target_stats["distinct_key_count"],
            "matched_key_count": mc,
            "primary_matched_count": pmc,
            "missing_key_count": mic,
            "extra_key_count": ec,
            "source_duplicate_key_count": sdk,
            "target_duplicate_key_count": tdk,
            "source_duplicated_key_value_count": sdg,
            "target_duplicated_key_value_count": tdg,
            "source_duplicate_record_count": sdr,
            "target_duplicate_record_count": tdr,
            "unmatchable_source_count": usc,
            "unmatchable_target_count": utc,
            "ambiguous_record_count": usc + utc,
            "mismatch_count": mic + ec + usc + utc,
            "source_record_coverage_pct": safe_rate_pct(counts["matched_source_record_count"], sc, zero_value=100.0),
            "target_record_coverage_pct": safe_rate_pct(counts["matched_target_record_count"], tc, zero_value=100.0),
            "missing_record_rate_pct": safe_rate_pct(mic, sc),
            "extra_record_rate_pct": safe_rate_pct(ec, tc),
            "ambiguous_record_rate_pct": safe_rate_pct(usc + utc, sc + tc),
            "matching_mode": effective_matching_mode,
            "all_rows_have_usable_keys": all_rows_have_usable_keys,
            "needs_secondary_reconciliation": needs_secondary_reconciliation,
        }

        if cfg.get("matching_mode") == "GROUP_RECONCILIATION" and needs_secondary_reconciliation:
            group_started = perf_counter()
            groups = cfg.get("grouping_attributes", []) or []
            mapping_lookup = host._mapping_lookup(cfg)

            def with_secondary_group_key(df, side):
                expressions = []
                for item in groups:
                    source_column = item["source_column"]
                    target_column = item["target_column"]
                    column = source_column if side == "source" else target_column
                    mapping = mapping_lookup.get(source_column, {})
                    if mapping.get("target_column") != target_column:
                        mapping = {}
                    expressions.append(host._apply_mapping_normalization(F.col(column), mapping).cast("string"))
                return df.withColumn("__secondary_group_key", F.to_json(F.array(*expressions)))

            null_source = reconciliation.filter(F.col("reconciliation_status") == "UNMATCHABLE_SOURCE").select("_s.*")
            null_target = reconciliation.filter(F.col("reconciliation_status") == "UNMATCHABLE_TARGET").select("_t.*")
            missing_source = reconciliation.filter(F.col("reconciliation_status") == "MISSING_IN_TARGET").select("_s.*")
            extra_target = reconciliation.filter(F.col("reconciliation_status") == "EXTRA_IN_TARGET").select("_t.*")

            keyed_null_source = with_secondary_group_key(null_source, "source")
            keyed_null_target = with_secondary_group_key(null_target, "target")
            target_null_groups = keyed_null_target.select("__secondary_group_key").distinct()
            source_null_groups = keyed_null_source.select("__secondary_group_key").distinct()
            related_missing_source = with_secondary_group_key(missing_source, "source").join(target_null_groups, "__secondary_group_key", "left_semi")
            related_extra_target = with_secondary_group_key(extra_target, "target").join(source_null_groups, "__secondary_group_key", "left_semi")

            fallback_source = keyed_null_source.unionByName(related_missing_source).drop("__secondary_group_key").persist()
            fallback_target = keyed_null_target.unionByName(related_extra_target).drop("__secondary_group_key").persist()
            secondary_matches = host._possible_key_changes(fallback_source, fallback_target, cfg).persist()
            possible_key_changes = secondary_matches.filter(F.col("status") == "POSSIBLE_KEY_CHANGE")
            missing_business_keys = secondary_matches.filter(F.col("status") == "MISSING_BUSINESS_KEY")
            secondary_summary = secondary_matches.agg(
                F.count(F.lit(1)).alias("total"),
                F.sum(F.when(F.col("status") == "POSSIBLE_KEY_CHANGE", 1).otherwise(0)).alias("possible_key_changes"),
                F.sum(F.when(F.col("status") == "MISSING_BUSINESS_KEY", 1).otherwise(0)).alias("missing_business_keys"),
            ).first()
            secondary_match_count = int(secondary_summary["total"] or 0)
            possible_key_change_count = int(secondary_summary["possible_key_changes"] or 0)
            missing_business_key_count = int(secondary_summary["missing_business_keys"] or 0)
            metrics["secondary_match_count"] = secondary_match_count
            metrics["possible_key_change_count"] = possible_key_change_count
            metrics["missing_business_key_count"] = missing_business_key_count
            group_result = None

            if (usc + utc) > 0:
                group_result = host._group(fallback_source, fallback_target, cfg)
                row_metrics = dict(metrics)
                unresolved_populated_keys = max(0, mic + ec - missing_business_key_count)
                final_status = "FAIL" if (
                    unresolved_populated_keys > 0
                    or missing_business_key_count > 0
                    or group_result["metrics"]["status"] == "FAIL"
                ) else "PASS"
                metrics = {
                    **metrics,
                    **group_result["metrics"],
                    "row_reconciliation": row_metrics,
                    "group_reconciliation": group_result["metrics"],
                    "status": final_status,
                }
                metrics["secondary_match_count"] = secondary_match_count
                metrics["possible_key_change_count"] = possible_key_change_count
                metrics["missing_business_key_count"] = missing_business_key_count
            else:
                metrics["matching_mode"] = "ROW_LEVEL"

            try:
                fallback_source.unpersist(blocking=False)
                fallback_target.unpersist(blocking=False)
            except Exception:
                logger.debug("Unable to unpersist L3 fallback datasets", exc_info=True)

            group_ms = (perf_counter() - group_started) * 1000
            evidence_started = perf_counter()
            row_evidence = {
                "matched_pairs": host._bounded(pairs, pmc),
                "missing_records": host._bounded(missing, mic),
                "extra_records": host._bounded(extra, ec),
                "duplicate_source_records": host._bounded(source_duplicates, sdk),
                "duplicate_target_records": host._bounded(target_duplicates, tdk),
                "duplicate_key_reconciliation": host._bounded(duplicate_key_reconciliation, sdg + tdg),
                "unmatchable_source_records": host._bounded(unmatchable_source, usc),
                "unmatchable_target_records": host._bounded(unmatchable_target, utc),
            }
            evidence = {
                **row_evidence,
                "row_reconciliation": row_evidence,
                "secondary_matches": host._bounded(secondary_matches, secondary_match_count),
                "missing_business_keys": host._bounded(missing_business_keys, missing_business_key_count),
                "possible_key_changes": host._bounded(possible_key_changes, possible_key_change_count),
            }
            if group_result is not None:
                evidence["group_reconciliation"] = group_result["evidence"].get("group_reconciliation", [])
            evidence_ms = (perf_counter() - evidence_started) * 1000
            logger.info("SPARK_L3_TIMING pk_build_ms=%.1f pk_summary_ms=%.1f pk_evidence_ms=%.1f group_ms=%.1f", pk_timing["pk_build_ms"], pk_timing["pk_summary_ms"], evidence_ms, group_ms)
            print(f"SPARK_L3_TIMING pk_path={pk_timing.get('pk_path','UNKNOWN')} pk_build_ms={pk_timing['pk_build_ms']:.1f} pk_summary_ms={pk_timing['pk_summary_ms']:.1f} pk_evidence_ms={evidence_ms:.1f} group_ms={group_ms:.1f}")
            return {"metrics": metrics, "evidence": evidence}

        evidence_started = perf_counter()
        evidence = {
            "matched_pairs": host._bounded(pairs, pmc),
            "missing_records": host._bounded(missing, mic),
            "extra_records": host._bounded(extra, ec),
            "duplicate_source_records": host._bounded(source_duplicates, sdk),
            "duplicate_target_records": host._bounded(target_duplicates, tdk),
            "duplicate_key_reconciliation": host._bounded(duplicate_key_reconciliation, sdg + tdg),
            "unmatchable_source_records": host._bounded(unmatchable_source, usc),
            "unmatchable_target_records": host._bounded(unmatchable_target, utc),
        }
        evidence_ms = (perf_counter() - evidence_started) * 1000
        logger.info("SPARK_L3_TIMING pk_build_ms=%.1f pk_summary_ms=%.1f pk_evidence_ms=%.1f group_ms=0.0", pk_timing["pk_build_ms"], pk_timing["pk_summary_ms"], evidence_ms)
        print(f"SPARK_L3_TIMING pk_path={pk_timing.get('pk_path','UNKNOWN')} pk_build_ms={pk_timing['pk_build_ms']:.1f} pk_summary_ms={pk_timing['pk_summary_ms']:.1f} pk_evidence_ms={evidence_ms:.1f} group_ms=0.0")
        return {"metrics": metrics, "evidence": evidence}
