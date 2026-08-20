from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

from app.comparators.aggregate import AggregateComparator
from app.comparators.dq import DQComparator
from app.metrics import safe_percent_change, safe_rate_pct


class DuckDBSchemaComparator:
    def execute(self, host, source, target, configuration):
        mappings = {
            item["source_column"]: item["target_column"]
            for item in configuration.get("column_mappings", [])
        }
        ignored = set(configuration.get("ignored_columns", []))
        source_fields = {
            column: source.type_names[column]
            for column in source.columns
            if column not in ignored
            and mappings.get(column, column) not in ignored
        }
        target_fields = {
            column: target.type_names[column]
            for column in target.columns
            if column not in ignored
        }
        missing = []
        unexpected = []
        type_mismatches = []
        nullable_mismatches = []
        matched = []
        mapped_targets = set()

        for source_column, source_type in source_fields.items():
            target_column = mappings.get(source_column, source_column)
            mapped_targets.add(target_column)
            if target_column not in target_fields:
                missing.append(
                    {
                        "source_column": source_column,
                        "expected_target_column": target_column,
                    }
                )
                continue
            matched.append(
                {
                    "source_column": source_column,
                    "target_column": target_column,
                }
            )
            if source_type != target_fields[target_column]:
                type_mismatches.append(
                    {
                        "source_column": source_column,
                        "target_column": target_column,
                        "source_type": source_type,
                        "target_type": target_fields[target_column],
                    }
                )
            if source.nullable[source_column] != target.nullable[target_column]:
                nullable_mismatches.append(
                    {
                        "source_column": source_column,
                        "target_column": target_column,
                        "source_nullable": source.nullable[source_column],
                        "target_nullable": target.nullable[target_column],
                    }
                )

        for target_column in target_fields:
            if target_column not in mapped_targets and target_column not in source_fields:
                unexpected.append({"target_column": target_column})

        mismatch_count = (
            len(missing)
            + len(unexpected)
            + len(type_mismatches)
            + len(nullable_mismatches)
        )
        schema_drift = (
            [{"type": "MISSING_COLUMN", **item} for item in missing]
            + [{"type": "UNEXPECTED_COLUMN", **item} for item in unexpected]
            + [{"type": "DATA_TYPE_CHANGED", **item} for item in type_mismatches]
            + [
                {"type": "NULLABILITY_CHANGED", **item}
                for item in nullable_mismatches
            ]
        )
        return {
            "metrics": {
                "status": "PASS" if mismatch_count == 0 else "FAIL",
                "source_column_count": len(source_fields),
                "target_column_count": len(target_fields),
                "matched_column_count": len(matched),
                "missing_column_count": len(missing),
                "unexpected_column_count": len(unexpected),
                "data_type_mismatch_count": len(type_mismatches),
                "schema_drift_count": mismatch_count,
                "nullable_mismatch_count": len(nullable_mismatches),
                "length_mismatch_count": 0,
                "precision_scale_mismatch_count": 0,
                "order_mismatch_count": 0,
                "mismatch_count": mismatch_count,
                "source_column_coverage_pct": safe_rate_pct(
                    len(matched), len(source_fields), zero_value=100.0
                ),
                "target_column_coverage_pct": safe_rate_pct(
                    len(matched), len(target_fields), zero_value=100.0
                ),
            },
            "evidence": {
                "matched_columns": matched,
                "missing_columns": missing,
                "unexpected_columns": unexpected,
                "type_mismatches": type_mismatches,
                "data_type_mismatches": type_mismatches,
                "nullable_mismatches": nullable_mismatches,
                "length_mismatches": [],
                "precision_scale_mismatches": [],
                "order_mismatch": {},
                "schema_drift": schema_drift,
            },
        }


class DuckDBVolumeComparator:
    def execute(self, host, source, target, configuration):
        source_stats = host.stats(source, configuration, "source")
        target_stats = host.stats(target, configuration, "target")
        checks = {}
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

        mappings = {
            item.get("source_column"): item.get("target_column")
            for item in configuration.get("column_mappings", [])
        }
        ignored = set(configuration.get("ignored_columns", []))
        mapped_null_counts = []
        null_count_differences = []
        for source_column, source_count in source_stats["null_counts"].items():
            target_column = mappings.get(source_column, source_column)
            if source_column in ignored or target_column in ignored:
                continue
            if target_column not in target_stats["null_counts"]:
                continue
            target_count = target_stats["null_counts"][target_column]
            item = {
                "source_column": source_column,
                "target_column": target_column,
                "source": source_count,
                "target": target_count,
                "difference": target_count - source_count,
                "matched": source_count == target_count,
            }
            mapped_null_counts.append(item)
            if not item["matched"]:
                null_count_differences.append(item)
        checks["null_counts"] = {
            "source": source_stats["null_counts"],
            "target": target_stats["null_counts"],
            "mapped_columns": mapped_null_counts,
            "differences": null_count_differences,
            "matched": not null_count_differences,
            "available": True,
        }
        validation_names = (
            "total_rows",
            "distinct_key_count",
            "duplicate_key_count",
            "null_counts",
        )
        applicable = [
            name for name in validation_names if checks[name].get("available", True)
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
                    zero_value=100.0 if target_stats["total_rows"] == 0 else None,
                ),
                "distinct_key_percent_change": safe_percent_change(
                    source_stats["distinct_key_count"],
                    target_stats["distinct_key_count"],
                ),
                "source_duplicate_key_rate_pct": safe_rate_pct(
                    source_stats["duplicate_key_count"], source_stats["total_rows"]
                ),
                "target_duplicate_key_rate_pct": safe_rate_pct(
                    target_stats["duplicate_key_count"], target_stats["total_rows"]
                ),
            },
            "evidence": {
                "checks": checks,
                "failed_checks": failed,
                "source": source_stats,
                "target": target_stats,
            },
        }


class DuckDBRecordComparator:
    def execute(self, host, source, target, configuration):
        if not configuration.get("comparison_keys"):
            if configuration.get("matching_mode") == "GROUP_RECONCILIATION":
                return DuckDBGroupComparator().execute(
                    host, source, target, configuration
                )
            raise ValueError("DuckDB L3 requires comparison_keys")

        reconciliation = host.reconciliation(source, target, configuration)
        counts = reconciliation["counts"]
        source_stats = host.stats(source, configuration, "source")
        target_stats = host.stats(target, configuration, "target")
        missing_count = counts["missing"]
        extra_count = counts["extra"]
        unmatchable_source = counts["unmatchable_source"]
        unmatchable_target = counts["unmatchable_target"]
        needs_secondary = (
            missing_count + extra_count + unmatchable_source + unmatchable_target
        ) > 0
        source_duplicate_values = counts["source_duplicate_keys"]
        target_duplicate_values = counts["target_duplicate_keys"]
        source_duplicate_records = (
            int(source_stats.get("duplicate_key_count") or 0)
            + source_duplicate_values
        )
        target_duplicate_records = (
            int(target_stats.get("duplicate_key_count") or 0)
            + target_duplicate_values
        )
        metrics = {
            "status": (
                "PASS"
                if missing_count + extra_count + unmatchable_source + unmatchable_target == 0
                else "FAIL"
            ),
            "source_record_count": source_stats["total_rows"],
            "target_record_count": target_stats["total_rows"],
            "source_unique_key_count": source_stats["distinct_key_count"],
            "target_unique_key_count": target_stats["distinct_key_count"],
            "matched_key_count": counts["matched_keys"],
            "primary_matched_count": counts["matched"],
            "missing_key_count": missing_count,
            "extra_key_count": extra_count,
            "source_duplicate_key_count": source_stats["duplicate_key_count"],
            "target_duplicate_key_count": target_stats["duplicate_key_count"],
            "source_duplicated_key_value_count": source_duplicate_values,
            "target_duplicated_key_value_count": target_duplicate_values,
            "source_duplicate_record_count": source_duplicate_records,
            "target_duplicate_record_count": target_duplicate_records,
            "unmatchable_source_count": unmatchable_source,
            "unmatchable_target_count": unmatchable_target,
            "ambiguous_record_count": unmatchable_source + unmatchable_target,
            "mismatch_count": (
                missing_count + extra_count + unmatchable_source + unmatchable_target
            ),
            "source_record_coverage_pct": safe_rate_pct(
                counts["matched_source_records"],
                source_stats["total_rows"],
                zero_value=100.0,
            ),
            "target_record_coverage_pct": safe_rate_pct(
                counts["matched_target_records"],
                target_stats["total_rows"],
                zero_value=100.0,
            ),
            "missing_record_rate_pct": safe_rate_pct(
                missing_count, source_stats["total_rows"]
            ),
            "extra_record_rate_pct": safe_rate_pct(
                extra_count, target_stats["total_rows"]
            ),
            "ambiguous_record_rate_pct": safe_rate_pct(
                unmatchable_source + unmatchable_target,
                source_stats["total_rows"] + target_stats["total_rows"],
            ),
            "matching_mode": (
                configuration.get("matching_mode", "ROW_LEVEL")
                if needs_secondary
                else "ROW_LEVEL"
            ),
            "all_rows_have_usable_keys": (
                unmatchable_source == 0 and unmatchable_target == 0
            ),
            "needs_secondary_reconciliation": needs_secondary,
        }
        evidence = self._evidence(host, reconciliation, metrics)

        # Spark only enters group fallback when at least one business key is
        # unusable. Populated missing/extra keys remain normal row-level L3.
        if (
            configuration.get("matching_mode") == "GROUP_RECONCILIATION"
            and needs_secondary
        ):
            secondary = self._secondary_reconciliation(
                host,
                reconciliation,
                configuration,
            )
            secondary_match_count = secondary["count"]
            possible_key_change_count = secondary["possible_key_change_count"]
            missing_business_key_count = secondary["missing_business_key_count"]
            metrics.update(
                {
                    "secondary_match_count": secondary_match_count,
                    "possible_key_change_count": possible_key_change_count,
                    "missing_business_key_count": missing_business_key_count,
                }
            )
            evidence.update(
                {
                    "row_reconciliation": dict(evidence),
                    "secondary_matches": secondary["evidence"],
                    "possible_key_changes": secondary["possible_key_changes"],
                    "missing_business_keys": secondary["missing_business_keys"],
                }
            )
            if unmatchable_source + unmatchable_target == 0:
                metrics["matching_mode"] = "ROW_LEVEL"
            else:
                group_result = _execute_group_reconciliation(
                    host,
                    secondary["fallback_source"],
                    secondary["fallback_target"],
                    configuration,
                )
                row_metrics = dict(metrics)
                unresolved_populated_keys = max(
                    0,
                    missing_count + extra_count - missing_business_key_count,
                )
                final_status = (
                    "FAIL"
                    if (
                        unresolved_populated_keys > 0
                        or missing_business_key_count > 0
                        or group_result["metrics"]["status"] == "FAIL"
                    )
                    else "PASS"
                )
                metrics = {
                    **metrics,
                    **group_result["metrics"],
                    "row_reconciliation": row_metrics,
                    "group_reconciliation": group_result["metrics"],
                    "status": final_status,
                    "secondary_match_count": secondary_match_count,
                    "possible_key_change_count": possible_key_change_count,
                    "missing_business_key_count": missing_business_key_count,
                }
                evidence["group_reconciliation"] = group_result["evidence"].get(
                    "group_reconciliation",
                    {"count": 0, "sample": [], "truncated": False},
                )

        return {"metrics": metrics, "evidence": evidence}

    def _secondary_reconciliation(self, host, reconciliation, configuration):
        source = reconciliation["source"]
        target = reconciliation["target"]
        table = host.identifier(reconciliation["table_name"])
        groups = configuration.get("grouping_attributes", []) or []
        keys = configuration.get("comparison_keys", []) or []
        mappings = host.mapping_lookup(configuration)

        def normalized_group(item, side, prefix, relation):
            source_column = item["source_column"]
            target_column = item["target_column"]
            column = source_column if side == "source" else target_column
            mapping = mappings.get(source_column, {})
            if mapping.get("target_column") != target_column:
                mapping = {}
            return host.normalized_value_expression(
                f"{host.identifier(relation)}.{host.identifier(prefix + column)}",
                mapping,
            )

        missing_to_null_group_match = " AND ".join(
            f"{normalized_group(item, 'source', 'source__', 'missing_source')} "
            f"IS NOT DISTINCT FROM "
            f"{normalized_group(item, 'target', 'target__', 'target_null')}"
            for item in groups
        )
        extra_to_null_group_match = " AND ".join(
            f"{normalized_group(item, 'source', 'source__', 'source_null')} "
            f"IS NOT DISTINCT FROM "
            f"{normalized_group(item, 'target', 'target__', 'extra_target')}"
            for item in groups
        )
        source_projection = ", ".join(
            f"reconciliation.{host.identifier('source__' + column)} AS "
            f"{host.identifier(column)}"
            for column in source.columns
        )
        related_source_projection = ", ".join(
            f"missing_source.{host.identifier('source__' + column)} AS "
            f"{host.identifier(column)}"
            for column in source.columns
        )
        target_projection = ", ".join(
            f"reconciliation.{host.identifier('target__' + column)} AS "
            f"{host.identifier(column)}"
            for column in target.columns
        )
        related_target_projection = ", ".join(
            f"extra_target.{host.identifier('target__' + column)} AS "
            f"{host.identifier(column)}"
            for column in target.columns
        )
        fallback_source_name = f"fallback_source_{len(host._reconciliation_cache)}"
        fallback_target_name = f"fallback_target_{len(host._reconciliation_cache)}"
        host.connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {host.identifier(fallback_source_name)} AS
            SELECT {source_projection}
            FROM {table} reconciliation
            WHERE reconciliation.reconciliation_status = 'UNMATCHABLE_SOURCE'
            UNION ALL
            SELECT {related_source_projection}
            FROM {table} missing_source
            WHERE reconciliation_status = 'MISSING_IN_TARGET'
              AND EXISTS (
                  SELECT 1 FROM {table} target_null
                  WHERE target_null.reconciliation_status = 'UNMATCHABLE_TARGET'
                    AND {missing_to_null_group_match}
              )
            """
        )
        host.connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {host.identifier(fallback_target_name)} AS
            SELECT {target_projection}
            FROM {table} reconciliation
            WHERE reconciliation.reconciliation_status = 'UNMATCHABLE_TARGET'
            UNION ALL
            SELECT {related_target_projection}
            FROM {table} extra_target
            WHERE reconciliation_status = 'EXTRA_IN_TARGET'
              AND EXISTS (
                  SELECT 1 FROM {table} source_null
                  WHERE source_null.reconciliation_status = 'UNMATCHABLE_SOURCE'
                    AND {extra_to_null_group_match}
              )
            """
        )
        fallback_source = replace(source, table_name=fallback_source_name)
        fallback_target = replace(target, table_name=fallback_target_name)

        group_aliases = [f"group_{index}" for index in range(len(groups))]

        def build_group_table(side, dataset, prefix):
            table_name = f"secondary_{side}_{len(host._reconciliation_cache)}"
            group_expressions = []
            for alias, item in zip(group_aliases, groups):
                source_column = item["source_column"]
                target_column = item["target_column"]
                column = source_column if side == "source" else target_column
                mapping = mappings.get(source_column, {})
                if mapping.get("target_column") != target_column:
                    mapping = {}
                value = host.normalized_value_expression(
                    host.identifier(column),
                    mapping,
                )
                group_expressions.append(
                    f"{value} AS {host.identifier(alias)}"
                )
            record_expressions = [
                f"first({host.identifier(column)}) AS "
                f"{host.identifier(prefix + column)}"
                for column in dataset.columns
            ]
            key_name = "source_column" if side == "source" else "target_column"
            key_columns = [item[key_name] for item in keys]
            key_available = host.populated_key_expression(key_columns)
            host.connection.execute(
                f"CREATE OR REPLACE TEMP TABLE {host.identifier(table_name)} AS "
                f"SELECT {', '.join(group_expressions)}, count(*) AS candidates, "
                f"max(CASE WHEN {key_available} THEN 1 ELSE 0 END) AS key_available, "
                f"{', '.join(record_expressions)} "
                f"FROM {host.identifier(dataset.table_name)} GROUP BY "
                f"{', '.join(str(index + 1) for index in range(len(groups)))}"
            )
            return table_name

        source_groups = build_group_table(
            "source", fallback_source, "source__"
        )
        target_groups = build_group_table(
            "target", fallback_target, "target__"
        )
        condition = " AND ".join(
            f"source.{host.identifier(alias)} IS NOT DISTINCT FROM "
            f"target.{host.identifier(alias)}"
            for alias in group_aliases
        )
        group_projection = ", ".join(
            f"coalesce(source.{host.identifier(alias)}, target.{host.identifier(alias)}) "
            f"AS {host.identifier(alias)}"
            for alias in group_aliases
        )
        source_records = ", ".join(
            f"source.{host.identifier('source__' + column)} "
            f"AS {host.identifier('source__' + column)}"
            for column in source.columns
        )
        target_records = ", ".join(
            f"target.{host.identifier('target__' + column)} "
            f"AS {host.identifier('target__' + column)}"
            for column in target.columns
        )
        secondary_table = f"secondary_matches_{len(host._reconciliation_cache)}"
        host.connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {host.identifier(secondary_table)} AS
            SELECT {group_projection}, {source_records}, {target_records},
                source.key_available AS source_key_available,
                target.key_available AS target_key_available,
                CASE
                    WHEN source.key_available <> target.key_available
                    THEN 'MISSING_BUSINESS_KEY'
                    WHEN source.key_available = 1 AND target.key_available = 1
                    THEN 'POSSIBLE_KEY_CHANGE'
                    ELSE 'MATCHED_BY_ATTRIBUTES'
                END AS status
            FROM {host.identifier(source_groups)} source
            INNER JOIN {host.identifier(target_groups)} target ON {condition}
            WHERE (source.candidates = 1 AND target.candidates = 1)
               OR source.key_available <> target.key_available
            """
        )
        summary = host.fetch_one(
            f"""
            SELECT count(*) AS total,
                count(*) FILTER (WHERE status = 'POSSIBLE_KEY_CHANGE')
                    AS possible_key_changes,
                count(*) FILTER (WHERE status = 'MISSING_BUSINESS_KEY')
                    AS missing_business_keys
            FROM {host.identifier(secondary_table)}
            """
        )
        total = int(summary["total"] or 0)
        possible_count = int(summary["possible_key_changes"] or 0)
        missing_count = int(summary["missing_business_keys"] or 0)
        rows = host.fetch_all(
            f"SELECT * FROM {host.identifier(secondary_table)} "
            f"ORDER BY {', '.join(host.identifier(alias) for alias in group_aliases)} "
            f"LIMIT {host.evidence_limit}"
        )
        samples = [
            self._secondary_sample(
                host,
                row,
                source,
                target,
                keys,
                group_aliases,
            )
            for row in rows
        ]
        possible_samples = [
            item for item in samples if item["status"] == "POSSIBLE_KEY_CHANGE"
        ]
        missing_samples = [
            item for item in samples if item["status"] == "MISSING_BUSINESS_KEY"
        ]
        return {
            "count": total,
            "possible_key_change_count": possible_count,
            "missing_business_key_count": missing_count,
            "fallback_source": fallback_source,
            "fallback_target": fallback_target,
            "evidence": {
                "count": total,
                "sample": samples,
                "truncated": total > host.evidence_limit,
            },
            "possible_key_changes": {
                "count": possible_count,
                "sample": possible_samples,
                "truncated": possible_count > host.evidence_limit,
            },
            "missing_business_keys": {
                "count": missing_count,
                "sample": missing_samples,
                "truncated": missing_count > host.evidence_limit,
            },
        }

    @staticmethod
    def _secondary_sample(host, row, source, target, keys, group_aliases):
        source_record = host.record_from_row(row, source, "source__")
        target_record = host.record_from_row(row, target, "target__")
        status = row["status"]
        reasons = {
            "MISSING_BUSINESS_KEY": (
                "The configured matching attributes identify the same record, "
                "but its business key is missing on one side"
            ),
            "POSSIBLE_KEY_CHANGE": (
                "Records share the configured matching attributes but use "
                "different business keys"
            ),
            "MATCHED_BY_ATTRIBUTES": (
                "Records have no usable business key and were paired by the "
                "configured matching attributes"
            ),
        }
        return {
            "group_key": [row[alias] for alias in group_aliases],
            "source_key": json.dumps(
                {
                    item["source_column"]: source_record.get(item["source_column"])
                    for item in keys
                    if source_record.get(item["source_column"]) is not None
                },
                separators=(",", ":"),
                default=str,
            ),
            "target_key": json.dumps(
                {
                    item["target_column"]: target_record.get(item["target_column"])
                    for item in keys
                    if target_record.get(item["target_column"]) is not None
                },
                separators=(",", ":"),
                default=str,
            ),
            "source_record": source_record,
            "target_record": target_record,
            "reason": reasons[status],
            "status": status,
        }

    def _evidence(self, host, reconciliation, metrics):
        table = host.identifier(reconciliation["table_name"])
        source = reconciliation["source"]
        target = reconciliation["target"]

        def rows_for(status, count):
            rows = host.fetch_all(
                f"SELECT * FROM {table} WHERE reconciliation_status = "
                f"{host.literal(status)} ORDER BY normalized_primary_key, "
                f"source_row_id, target_row_id LIMIT {host.evidence_limit}"
            )
            sample = []
            for row in rows:
                source_record = host.record_from_row(row, source, "source__")
                target_record = host.record_from_row(row, target, "target__")
                if status == "MATCHED":
                    sample.append(
                        {
                            "_s": source_record,
                            "_t": target_record,
                            "match_type": row["match_type"],
                            "match_key": host.match_key(row, reconciliation, "source"),
                        }
                    )
                elif status == "MISSING_IN_TARGET":
                    sample.append(
                        {
                            "key": host.match_key(row, reconciliation, "source"),
                            "record": source_record,
                        }
                    )
                elif status == "EXTRA_IN_TARGET":
                    sample.append(
                        {
                            "key": host.match_key(row, reconciliation, "target"),
                            "record": target_record,
                        }
                    )
                elif status == "UNMATCHABLE_SOURCE":
                    sample.append({"record": source_record})
                else:
                    sample.append({"record": target_record})
            return {
                "count": int(count),
                "sample": sample,
                "truncated": int(count) > host.evidence_limit,
            }

        duplicate_count = (
            metrics["source_duplicated_key_value_count"]
            + metrics["target_duplicated_key_value_count"]
        )
        duplicate_rows = host.fetch_all(
            f"""
            SELECT * FROM {table}
            WHERE normalized_primary_key IS NOT NULL
              AND (source_key_count > 1 OR target_key_count > 1)
            ORDER BY normalized_primary_key, source_row_id, target_row_id
            LIMIT {host.evidence_limit}
            """
        )
        duplicate_sample = []
        seen = set()
        for row in duplicate_rows:
            key = row["normalized_primary_key"]
            if key in seen:
                continue
            seen.add(key)
            duplicate_sample.append(
                {
                    "key": host.match_key(
                        row,
                        reconciliation,
                        "source" if row.get("source_row_id") else "target",
                    ),
                    "source_occurrences": int(row.get("source_key_count") or 0),
                    "target_occurrences": int(row.get("target_key_count") or 0),
                    "compared_pairs": int(
                        host.fetch_one(
                            f"SELECT count(*) AS total FROM {table} "
                            f"WHERE normalized_primary_key = {host.literal(key)} "
                            "AND source_key_count IS NOT NULL "
                            "AND target_key_count IS NOT NULL"
                        )["total"]
                        or 0
                    ),
                    "source_record": host.record_from_row(row, source, "source__"),
                    "target_record": host.record_from_row(row, target, "target__"),
                }
            )
        return {
            "matched_pairs": rows_for("MATCHED", metrics["primary_matched_count"]),
            "missing_records": rows_for("MISSING_IN_TARGET", metrics["missing_key_count"]),
            "extra_records": rows_for("EXTRA_IN_TARGET", metrics["extra_key_count"]),
            "duplicate_source_records": {
                "count": int(metrics["source_duplicate_key_count"] or 0),
                "sample": [],
                "truncated": False,
            },
            "duplicate_target_records": {
                "count": int(metrics["target_duplicate_key_count"] or 0),
                "sample": [],
                "truncated": False,
            },
            "duplicate_key_reconciliation": {
                "count": duplicate_count,
                "sample": duplicate_sample[: host.evidence_limit],
                "truncated": duplicate_count > host.evidence_limit,
            },
            "unmatchable_source_records": rows_for(
                "UNMATCHABLE_SOURCE", metrics["unmatchable_source_count"]
            ),
            "unmatchable_target_records": rows_for(
                "UNMATCHABLE_TARGET", metrics["unmatchable_target_count"]
            ),
        }


class DuckDBFieldComparator:
    def execute(self, host, source, target, configuration):
        if not configuration.get("comparison_keys"):
            return {
                "metrics": {
                    "status": "NOT_APPLICABLE",
                    "comparison_mode": "GROUP_RECONCILIATION",
                    "reason": "Row-level field comparison is not applicable without row matches.",
                },
                "evidence": {},
            }
        reconciliation = host.reconciliation(source, target, configuration)
        table = host.identifier(reconciliation["table_name"])
        columns = host.resolve_l4_columns(source, target, configuration)
        matched_records = reconciliation["counts"]["matched"]
        source_stats = host.stats(source, configuration, "source")
        target_stats = host.stats(target, configuration, "target")
        if not columns:
            return self._empty_result(
                source_stats,
                target_stats,
                matched_records,
                reconciliation,
            )

        source_hash_parts = []
        target_hash_parts = []
        comparisons = []
        for index, (source_column, target_column, mapping) in enumerate(columns):
            source_raw = host.identifier("source__" + source_column)
            target_raw = host.identifier("target__" + target_column)
            source_value = host.normalized_value_expression(source_raw, mapping)
            target_value = host.normalized_value_expression(target_raw, mapping)
            source_hash_parts.append(
                f"{host.literal(source_column + '=')} || "
                f"coalesce(CAST({source_value} AS VARCHAR), '<NULL>')"
            )
            target_hash_parts.append(
                f"{host.literal(source_column + '=')} || "
                f"coalesce(CAST({target_value} AS VARCHAR), '<NULL>')"
            )
            numeric = self._numeric_type(source.type_names[source_column]) and self._numeric_type(
                target.type_names[target_column]
            )
            exact = f"({source_value} IS NOT DISTINCT FROM {target_value})"
            difference = (
                f"CAST({target_value} AS DOUBLE) - CAST({source_value} AS DOUBLE)"
                if numeric
                else "CAST(NULL AS DOUBLE)"
            )
            comparison_type = "EXACT"
            tolerance_value = None
            tolerance_type = None
            matched = exact
            if mapping.get("tolerance_pct") is not None:
                tolerance_value = float(mapping["tolerance_pct"])
                tolerance_type = "PERCENTAGE"
                comparison_type = "PERCENTAGE_TOLERANCE"
                if numeric:
                    allowed = (
                        f"abs(CAST({source_value} AS DOUBLE)) * "
                        f"({tolerance_value} / 100.0)"
                    )
                    matched = (
                        f"({exact} OR (try_cast({source_value} AS DOUBLE) IS NOT NULL "
                        f"AND try_cast({target_value} AS DOUBLE) IS NOT NULL "
                        f"AND abs({difference}) <= {allowed}))"
                    )
            elif mapping.get("tolerance") is not None:
                tolerance_value = float(mapping["tolerance"])
                tolerance_type = "ABSOLUTE"
                comparison_type = "NUMERIC_TOLERANCE"
                if numeric:
                    matched = (
                        f"({exact} OR (try_cast({source_value} AS DOUBLE) IS NOT NULL "
                        f"AND try_cast({target_value} AS DOUBLE) IS NOT NULL "
                        f"AND abs({difference}) <= {tolerance_value}))"
                    )
            comparisons.append(
                {
                    "index": index,
                    "source_column": source_column,
                    "target_column": target_column,
                    "source_raw": source_raw,
                    "target_raw": target_raw,
                    "matched": matched,
                    "difference": difference,
                    "comparison_type": comparison_type,
                    "tolerance": tolerance_value,
                    "tolerance_type": tolerance_type,
                }
            )

        source_hash = "sha256(concat_ws(chr(30), " + ", ".join(source_hash_parts) + "))"
        target_hash = "sha256(concat_ws(chr(30), " + ", ".join(target_hash_parts) + "))"
        candidate_table = f"field_candidates_{len(host._reconciliation_cache)}"
        host.connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {host.identifier(candidate_table)} AS
            SELECT *, {source_hash} AS source_hash, {target_hash} AS target_hash
            FROM {table}
            WHERE reconciliation_status = 'MATCHED'
            """
        )
        candidate = host.identifier(candidate_table)
        summary_expressions = [
            "count(*) FILTER (WHERE source_hash = target_hash) AS hash_equal",
            "count(*) FILTER (WHERE source_hash IS DISTINCT FROM target_hash) AS hash_changed",
        ]
        summary_expressions.extend(
            f"count(*) FILTER (WHERE source_hash IS DISTINCT FROM target_hash "
            f"AND NOT {item['matched']}) AS mismatch_{item['index']}"
            for item in comparisons
        )
        summary = host.fetch_one(
            f"SELECT {', '.join(summary_expressions)} FROM {candidate}"
        )
        field_statistics = []
        mismatch_count = 0
        for item in comparisons:
            count = int(summary[f"mismatch_{item['index']}"] or 0)
            mismatch_count += count
            field_statistics.append(
                {
                    "field": item["source_column"],
                    "target_field": item["target_column"],
                    "mismatches": count,
                    "comparison_type": item["comparison_type"],
                }
            )

        mismatch_queries = []
        source_record_columns = ", ".join(
            host.identifier("source__" + column) for column in source.columns
        )
        target_record_columns = ", ".join(
            host.identifier("target__" + column) for column in target.columns
        )
        for item in comparisons:
            mismatch_queries.append(
                f"""
                SELECT normalized_primary_key, match_type,
                    {host.literal(item['source_column'])} AS source_column,
                    {host.literal(item['target_column'])} AS target_column,
                    {item['source_raw']} AS source_value,
                    {item['target_raw']} AS target_value,
                    {item['difference']} AS difference,
                    {host.literal(item['comparison_type'])} AS comparison_type,
                    {host.literal(item['tolerance'])}::DOUBLE AS tolerance,
                    {host.literal(item['tolerance_type'])} AS tolerance_type,
                    {source_record_columns}, {target_record_columns}
                FROM {candidate}
                WHERE source_hash IS DISTINCT FROM target_hash
                  AND NOT {item['matched']}
                """
            )
        mismatch_union = " UNION ALL ".join(mismatch_queries)
        mismatch_rows = host.fetch_all(
            f"SELECT * FROM ({mismatch_union}) mismatches "
            f"ORDER BY normalized_primary_key, source_column "
            f"LIMIT {host.evidence_limit}"
        ) if mismatch_count else []
        sample = []
        for row in mismatch_rows:
            source_record = host.record_from_row(row, source, "source__")
            target_record = host.record_from_row(row, target, "target__")
            sample.append(
                {
                    "key": self._key_from_records(
                        source_record,
                        target_record,
                        configuration.get("comparison_keys", []),
                    ),
                    "match_type": row["match_type"],
                    "source_column": row["source_column"],
                    "target_column": row["target_column"],
                    "source_value": row["source_value"],
                    "target_value": row["target_value"],
                    "source_record": source_record,
                    "target_record": target_record,
                    "matched": False,
                    "comparison_type": row["comparison_type"],
                    "difference": row["difference"],
                    "tolerance": row["tolerance"],
                    "tolerance_type": row["tolerance_type"],
                }
            )
        records_with_mismatch = (
            int(
                host.fetch_one(
                    f"SELECT count(DISTINCT normalized_primary_key) AS total "
                    f"FROM ({mismatch_union}) mismatches"
                )["total"]
                or 0
            )
            if mismatch_count
            else 0
        )
        duplicate_pair_count = (
            reconciliation["counts"]["source_duplicate_keys"]
            + reconciliation["counts"]["target_duplicate_keys"]
        )
        duplicate_rows = host.fetch_all(
            f"""
            SELECT normalized_primary_key,
                first(normalized_primary_key) AS key,
                max(source_key_count) AS source_occurrences,
                max(target_key_count) AS target_occurrences,
                count(*) AS compared_pairs,
                first(struct_pack({', '.join(
                    f'{host.identifier(column)} := {host.identifier("source__" + column)}'
                    for column in source.columns
                )})) AS source_record,
                first(struct_pack({', '.join(
                    f'{host.identifier(column)} := {host.identifier("target__" + column)}'
                    for column in target.columns
                )})) AS target_record
            FROM {candidate}
            WHERE source_key_count > 1 OR target_key_count > 1
            GROUP BY normalized_primary_key
            ORDER BY normalized_primary_key
            LIMIT {host.evidence_limit}
            """
        ) if duplicate_pair_count else []
        duplicate_sample = []
        for row in duplicate_rows:
            row["key"] = self._key_from_records(
                row["source_record"],
                row["target_record"],
                configuration.get("comparison_keys", []),
            )
            row.pop("normalized_primary_key", None)
            duplicate_sample.append(row)
        compared_fields = matched_records * len(comparisons)
        return {
            "metrics": {
                "status": "PASS" if mismatch_count == 0 else "FAIL",
                "source_record_count": source_stats["total_rows"],
                "target_record_count": target_stats["total_rows"],
                "matched_record_count": matched_records,
                "compared_field_count": compared_fields,
                "matched_field_count": compared_fields - mismatch_count,
                "mismatch_count": mismatch_count,
                "field_conformity_pct": safe_rate_pct(
                    compared_fields - mismatch_count,
                    compared_fields,
                    zero_value=100.0,
                ),
                "field_mismatch_rate_pct": safe_rate_pct(
                    mismatch_count, compared_fields
                ),
                "records_with_mismatch": records_with_mismatch,
                "affected_record_rate_pct": safe_rate_pct(
                    records_with_mismatch, matched_records
                ),
                "missing_record_count": reconciliation["counts"]["missing"],
                "extra_record_count": reconciliation["counts"]["extra"],
                # L4 only evaluates authoritative matched pairs. Spark's L4
                # contract reports no ambiguous pairs; L3 owns null-key counts.
                "ambiguous_record_count": 0,
                "source_duplicate_key_count": source_stats["duplicate_key_count"],
                "target_duplicate_key_count": target_stats["duplicate_key_count"],
                "hash_algorithm": "SHA-256",
                "hash_equal_record_count": int(summary["hash_equal"] or 0),
                "hash_changed_candidate_count": int(summary["hash_changed"] or 0),
            },
            "evidence": {
                "field_statistics": field_statistics,
                "comparison_keys": configuration.get("comparison_keys", []),
                "effective_column_mappings": [
                    {
                        "source_column": item["source_column"],
                        "target_column": item["target_column"],
                        "normalization": dict(
                            (
                                host.mapping_lookup(configuration).get(
                                    item["source_column"], {}
                                )
                                or {}
                            ).get("normalization")
                            or {}
                        ),
                        "tolerance": item["tolerance"],
                        "tolerance_pct": (
                            host.mapping_lookup(configuration)
                            .get(item["source_column"], {})
                            .get("tolerance_pct")
                        ),
                        "comparison_type": item["comparison_type"],
                    }
                    for item in comparisons
                ],
                "field_mismatches": {
                    "count": mismatch_count,
                    "sample": sample,
                    "truncated": mismatch_count > host.evidence_limit,
                },
                "duplicate_matched_pairs": {
                    "count": duplicate_pair_count,
                    "sample": duplicate_sample,
                    "truncated": duplicate_pair_count > host.evidence_limit,
                },
            },
        }

    @staticmethod
    def _numeric_type(type_name):
        return (
            type_name in {"bigint", "double"}
            or type_name.startswith("decimal(")
        )

    @staticmethod
    def _key_from_records(source_record, target_record, keys):
        import json

        return json.dumps(
            {
                item["source_column"]: (
                    source_record.get(item["source_column"])
                    if source_record
                    else target_record.get(item["target_column"])
                )
                for item in keys
            },
            separators=(",", ":"),
            default=str,
        )

    @staticmethod
    def _empty_result(source_stats, target_stats, matched_records, reconciliation):
        return {
            "metrics": {
                "status": "PASS",
                "source_record_count": source_stats["total_rows"],
                "target_record_count": target_stats["total_rows"],
                "matched_record_count": matched_records,
                "compared_field_count": 0,
                "matched_field_count": 0,
                "mismatch_count": 0,
                "field_conformity_pct": 100.0,
                "field_mismatch_rate_pct": 0.0,
                "records_with_mismatch": 0,
                "affected_record_rate_pct": 0.0,
                "missing_record_count": reconciliation["counts"]["missing"],
                "extra_record_count": reconciliation["counts"]["extra"],
                "ambiguous_record_count": 0,
                "source_duplicate_key_count": source_stats["duplicate_key_count"],
                "target_duplicate_key_count": target_stats["duplicate_key_count"],
                "hash_algorithm": "SHA-256",
                "hash_equal_record_count": matched_records,
                "hash_changed_candidate_count": 0,
            },
            "evidence": {
                "field_statistics": [],
                "compared_fields": [],
                "field_mismatches": {"count": 0, "sample": [], "truncated": False},
                "duplicate_matched_pairs": {"count": 0, "sample": [], "truncated": False},
            },
        }


class DuckDBAggregateComparator:
    SUPPORTED = {"SUM", "AVG", "MIN", "MAX", "COUNT"}

    def execute(self, host, source, target, configuration):
        ignored = set(configuration.get("ignored_columns", []))
        rules = []
        for rule in configuration.get("aggregate_rules", []):
            if AggregateComparator._uses_ignored_column(rule, ignored):
                continue
            operation = str(rule.get("function", rule.get("operation", ""))).upper()
            if operation not in self.SUPPORTED:
                continue
            source_column = rule.get("source_column")
            target_column = rule.get("target_column") or source_column
            source_groups = rule.get("source_group_by") or rule.get("group_by_columns") or []
            target_groups = rule.get("target_group_by") or rule.get("group_by_columns") or []
            rules.append(
                (rule, operation, source_column, target_column, source_groups, target_groups)
            )

        results = []
        for rule, operation, source_column, target_column, source_groups, target_groups in rules:
            if source_groups or target_groups:
                results.append(
                    self._grouped_rule(
                        host,
                        source,
                        target,
                        rule,
                        operation,
                        source_column,
                        target_column,
                        source_groups,
                        target_groups,
                    )
                )
            else:
                source_value = host.fetch_one(
                    f"SELECT {self._aggregate(host, operation, source_column)} AS value "
                    f"FROM {host.identifier(source.table_name)}"
                )["value"]
                target_value = host.fetch_one(
                    f"SELECT {self._aggregate(host, operation, target_column)} AS value "
                    f"FROM {host.identifier(target.table_name)}"
                )["value"]
                difference = (
                    None
                    if source_value is None or target_value is None
                    else float(target_value) - float(source_value)
                )
                matched = self._matched(rule, source_value, target_value, difference)
                tolerance_pct = rule.get("tolerance_pct")
                results.append(
                    {
                        "rule_name": rule.get("name"),
                        "operation": operation,
                        "source_column": source_column,
                        "target_column": target_column,
                        "group": None,
                        "source": source_value,
                        "target": target_value,
                        "difference": difference,
                        "matched": matched,
                        "tolerance": (
                            {"percentage": float(tolerance_pct)}
                            if tolerance_pct is not None
                            else rule.get("tolerance")
                        ),
                        "tolerance_pct": tolerance_pct,
                    }
                )

        failed_rules = sum(not item["matched"] for item in results)
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

    def _grouped_rule(
        self,
        host,
        source,
        target,
        rule,
        operation,
        source_column,
        target_column,
        source_groups,
        target_groups,
    ):
        source_group_select = ", ".join(
            f"{host.identifier(column)} AS {host.identifier('group_' + str(index))}"
            for index, column in enumerate(source_groups)
        )
        target_group_select = ", ".join(
            f"{host.identifier(column)} AS {host.identifier('group_' + str(index))}"
            for index, column in enumerate(target_groups)
        )
        group_aliases = [f"group_{index}" for index in range(len(source_groups))]
        source_table = f"aggregate_source_{abs(hash((rule.get('name'), source_column))) % 1000000}"
        target_table = f"aggregate_target_{abs(hash((rule.get('name'), target_column))) % 1000000}"
        host.connection.execute(
            f"CREATE OR REPLACE TEMP TABLE {host.identifier(source_table)} AS "
            f"SELECT {source_group_select}, "
            f"{self._aggregate(host, operation, source_column)} AS value "
            f"FROM {host.identifier(source.table_name)} GROUP BY ALL"
        )
        host.connection.execute(
            f"CREATE OR REPLACE TEMP TABLE {host.identifier(target_table)} AS "
            f"SELECT {target_group_select}, "
            f"{self._aggregate(host, operation, target_column)} AS value "
            f"FROM {host.identifier(target.table_name)} GROUP BY ALL"
        )
        condition = " AND ".join(
            f"source.{host.identifier(alias)} IS NOT DISTINCT FROM "
            f"target.{host.identifier(alias)}"
            for alias in group_aliases
        )
        difference = "CAST(target.value AS DOUBLE) - CAST(source.value AS DOUBLE)"
        matched = self._matched_sql(host, rule, "source.value", "target.value", difference)
        groups = ", ".join(
            f"coalesce(source.{host.identifier(alias)}, target.{host.identifier(alias)}) "
            f"AS {host.identifier(alias)}"
            for alias in group_aliases
        )
        joined = (
            f"SELECT {groups}, source.value AS source, target.value AS target, "
            f"{difference} AS difference, {matched} AS matched "
            f"FROM {host.identifier(source_table)} source FULL OUTER JOIN "
            f"{host.identifier(target_table)} target ON {condition}"
        )
        summary = host.fetch_one(
            f"SELECT count(*) AS total, count(*) FILTER (WHERE NOT matched) AS failed "
            f"FROM ({joined}) grouped"
        )
        failed_rows = host.fetch_all(
            f"SELECT * FROM ({joined}) grouped WHERE NOT matched "
            f"ORDER BY {', '.join(host.identifier(alias) for alias in group_aliases)} "
            f"LIMIT {host.evidence_limit}"
        )
        group_results = []
        for row in failed_rows:
            values = [row[alias] for alias in group_aliases]
            group_results.append(
                {
                    "rule_name": rule.get("name"),
                    "operation": operation,
                    "source_column": source_column,
                    "target_column": target_column,
                    "group": values[0] if len(values) == 1 else values,
                    "source": row["source"],
                    "target": row["target"],
                    "difference": row["difference"],
                    "tolerance": (
                        {"percentage": float(rule["tolerance_pct"])}
                        if rule.get("tolerance_pct") is not None
                        else rule.get("tolerance")
                    ),
                    "tolerance_pct": rule.get("tolerance_pct"),
                    "matched": False,
                }
            )
        return {
            "rule_name": rule.get("name"),
            "operation": operation,
            "source_column": source_column,
            "target_column": target_column,
            "grouped": True,
            "checks": int(summary["total"] or 0),
            "failed": int(summary["failed"] or 0),
            "matched": int(summary["failed"] or 0) == 0,
            "group_results": group_results,
        }

    @staticmethod
    def _aggregate(host, operation, column):
        value = host.identifier(column) if column else "1"
        return f"{operation.lower()}({value})"

    @staticmethod
    def _matched(rule, source, target, difference):
        if difference is None:
            return source == target
        if rule.get("tolerance_pct") is not None and source is not None:
            return abs(difference) <= abs(float(source)) * (
                float(rule["tolerance_pct"]) / 100.0
            )
        if rule.get("tolerance") is not None:
            return abs(difference) <= float(rule["tolerance"])
        return source == target

    @staticmethod
    def _matched_sql(host, rule, source, target, difference):
        exact = f"({source} IS NOT DISTINCT FROM {target})"
        if rule.get("tolerance_pct") is not None:
            allowed = (
                f"abs(CAST({source} AS DOUBLE)) * "
                f"({float(rule['tolerance_pct'])} / 100.0)"
            )
            return (
                f"({exact} OR ({source} IS NOT NULL AND {target} IS NOT NULL "
                f"AND abs({difference}) <= {allowed}))"
            )
        if rule.get("tolerance") is not None:
            return (
                f"({exact} OR ({source} IS NOT NULL AND {target} IS NOT NULL "
                f"AND abs({difference}) <= {float(rule['tolerance'])}))"
            )
        return exact


class DuckDBDQComparator:
    def execute(self, host, source, target, configuration):
        ignored = set(configuration.get("ignored_columns", []))
        output = []
        for side, dataset in (("SOURCE", source), ("TARGET", target)):
            side_rules = []
            for rule in configuration.get("dq_rules", []):
                if DQComparator._uses_ignored_column(rule, ignored):
                    continue
                if not rule.get("enabled", True):
                    continue
                apply_to = str(rule.get("apply_to", "BOTH")).upper()
                if apply_to not in {"BOTH", side}:
                    continue
                column = (
                    rule.get("source_column" if side == "SOURCE" else "target_column")
                    or rule.get("column")
                )
                if not column or column not in dataset.columns:
                    continue
                invalid = self._invalid_expression(host, rule, column)
                if invalid:
                    side_rules.append((rule, column, invalid))
            if not side_rules:
                continue

            expressions = ["count(*) AS total"]
            expressions.extend(
                f"count(*) FILTER (WHERE {invalid}) AS failed_{index}"
                for index, (_, _, invalid) in enumerate(side_rules)
            )
            summary = host.fetch_one(
                f"SELECT {', '.join(expressions)} "
                f"FROM {host.identifier(dataset.table_name)}"
            )
            total = int(summary["total"] or 0)
            for index, (rule, column, invalid) in enumerate(side_rules):
                failed = int(summary[f"failed_{index}"] or 0)
                rule_type = str(rule.get("rule_type", "")).upper()
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
                    failed_rows = host.fetch_all(
                        f"SELECT * FROM {host.identifier(dataset.table_name)} "
                        f"WHERE {invalid} LIMIT {host.evidence_limit}"
                    )
                    records = [
                        {
                            "record": row,
                            "column": column,
                            "value": row.get(column),
                            "rule": {
                                "rule_id": rule.get("rule_id"),
                                "name": rule.get("name"),
                                "rule_type": rule_type,
                            },
                            "reason": f"{rule_type} validation failed",
                            "status": "FAIL",
                        }
                        for row in failed_rows
                    ]
                    item[
                        "source_failed_records"
                        if side == "SOURCE"
                        else "target_failed_records"
                    ] = records
                output.append(item)

        failed_rules = sum(item["status"] == "FAIL" for item in output)
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

    @staticmethod
    def _invalid_expression(host, rule, column):
        value = host.identifier(column)
        rule_type = str(rule.get("rule_type", "")).upper()
        if rule_type == "PATTERN":
            return (
                f"{value} IS NOT NULL AND NOT regexp_matches("
                f"CAST({value} AS VARCHAR), {host.literal(rule.get('regex', ''))})"
            )
        if rule_type == "COMPLETENESS":
            return f"{value} IS NULL OR trim(CAST({value} AS VARCHAR)) = ''"
        if rule_type == "VALIDITY":
            allowed = rule.get("allowed_values") or (
                rule.get("value") if isinstance(rule.get("value"), list) else None
            )
            if allowed is not None:
                return (
                    f"{value} NOT IN ("
                    + ", ".join(host.literal(item) for item in allowed)
                    + ")"
                )
            minimum = rule.get("min")
            maximum = rule.get("max")
            if minimum is not None or maximum is not None:
                numeric = f"try_cast({value} AS DOUBLE)"
                parts = [f"{numeric} IS NULL"]
                if minimum is not None:
                    parts.append(f"{numeric} < {float(minimum)}")
                if maximum is not None:
                    parts.append(f"{numeric} > {float(maximum)}")
                return " OR ".join(parts)
        return None


class DuckDBGroupComparator:
    """Group-only L3 entry point; full fallback is added by record matching."""

    def execute(self, host, source, target, configuration):
        # Group reconciliation is intentionally implemented through aggregate
        # rules in a follow-up helper. Refuse incomplete configuration rather
        # than returning engine-dependent results.
        groups = configuration.get("grouping_attributes", []) or []
        aggregates = configuration.get("aggregation_columns", []) or []
        if not groups:
            raise ValueError("Group reconciliation requires grouping_attributes")
        if not aggregates:
            raise ValueError("Group reconciliation requires aggregation_columns")
        return _execute_group_reconciliation(
            host,
            source,
            target,
            configuration,
        )


def _execute_group_reconciliation(host, source, target, configuration):
    """Execute canonical grouped comparison entirely inside DuckDB."""
    groups = configuration.get("grouping_attributes", []) or []
    aggregates = configuration.get("aggregation_columns", []) or []
    mappings = host.mapping_lookup(configuration)
    aliases = [f"group_{index}" for index in range(len(groups))]

    def group_expression(item, side):
        source_column = item["source_column"]
        target_column = item["target_column"]
        column = source_column if side == "source" else target_column
        mapping = mappings.get(source_column, {})
        if mapping.get("target_column") != target_column:
            mapping = {}
        return host.normalized_value_expression(host.identifier(column), mapping)

    def aggregate_expression(item, side, index):
        source_column = item["source_column"]
        target_column = item["target_column"]
        column = source_column if side == "source" else target_column
        mapping = mappings.get(source_column, {})
        if mapping.get("target_column") != target_column:
            mapping = {}
        value = host.normalized_value_expression(host.identifier(column), mapping)
        operation = str(item.get("operation", "AVG")).upper()
        if operation in {"SUM", "AVG"}:
            value = f"try_cast({value} AS DOUBLE)"
        return (
            f"{operation.lower()}({value}) "
            f"AS {host.identifier('aggregate_' + str(index))}"
        )

    table_names = {}
    for side, dataset in (("source", source), ("target", target)):
        group_select = [
            f"{group_expression(item, side)} AS {host.identifier(alias)}"
            for item, alias in zip(groups, aliases)
        ]
        regular_aggregates = [
            aggregate_expression(item, side, index)
            for index, item in enumerate(aggregates)
            if str(item.get("operation", "AVG")).upper() != "MODE"
        ]
        table_name = f"group_{side}_{len(host._reconciliation_cache)}"
        table_names[side] = table_name
        normalized_table = table_name + "_normalized"
        host.connection.execute(
            f"CREATE OR REPLACE TEMP TABLE {host.identifier(normalized_table)} AS "
            f"SELECT *, {', '.join(group_select)} "
            f"FROM {host.identifier(dataset.table_name)}"
        )
        select_parts = [
            *[host.identifier(alias) for alias in aliases],
            "count(*) AS present",
            "first(struct_pack("
            + ", ".join(
                f"{host.identifier(column)} := {host.identifier(column)}"
                for column in dataset.columns
            )
            + ")) AS representative_record",
            *regular_aggregates,
        ]
        host.connection.execute(
            f"CREATE OR REPLACE TEMP TABLE {host.identifier(table_name)} AS "
            f"SELECT {', '.join(select_parts)} "
            f"FROM {host.identifier(normalized_table)} GROUP BY "
            f"{', '.join(str(index + 1) for index in range(len(groups)))}"
        )

        for index, item in enumerate(aggregates):
            if str(item.get("operation", "AVG")).upper() != "MODE":
                continue
            source_column = item["source_column"]
            target_column = item["target_column"]
            column = source_column if side == "source" else target_column
            mapping = mappings.get(source_column, {})
            if mapping.get("target_column") != target_column:
                mapping = {}
            value = host.normalized_value_expression(
                host.identifier(column), mapping
            )
            mode_table = f"{table_name}_mode_{index}"
            group_columns = ", ".join(host.identifier(alias) for alias in aliases)
            host.connection.execute(
                f"CREATE OR REPLACE TEMP TABLE {host.identifier(mode_table)} AS "
                f"SELECT {group_columns}, mode_value AS "
                f"{host.identifier('aggregate_' + str(index))} FROM ("
                f"SELECT {group_columns}, {value} AS mode_value, "
                f"row_number() OVER (PARTITION BY {group_columns} "
                f"ORDER BY count(*) DESC, CAST({value} AS VARCHAR) ASC) AS mode_rank "
                f"FROM {host.identifier(normalized_table)} "
                f"WHERE {value} IS NOT NULL GROUP BY {group_columns}, {value}"
                f") ranked_modes WHERE mode_rank = 1"
            )
            join_condition = " AND ".join(
                f"grouped.{host.identifier(alias)} IS NOT DISTINCT FROM "
                f"modes.{host.identifier(alias)}"
                for alias in aliases
            )
            host.connection.execute(
                f"CREATE OR REPLACE TEMP TABLE {host.identifier(table_name)} AS "
                f"SELECT grouped.*, modes.{host.identifier('aggregate_' + str(index))} "
                f"FROM {host.identifier(table_name)} grouped LEFT JOIN "
                f"{host.identifier(mode_table)} modes ON {join_condition}"
            )

    condition = " AND ".join(
        f"source.{host.identifier(alias)} IS NOT DISTINCT FROM "
        f"target.{host.identifier(alias)}"
        for alias in aliases
    )
    joined = f"group_joined_{len(host._reconciliation_cache)}"
    projections = []
    for alias in aliases:
        projections.extend(
            [
                f"source.{host.identifier(alias)} AS {host.identifier('source_' + alias)}",
                f"target.{host.identifier(alias)} AS {host.identifier('target_' + alias)}",
            ]
        )
    projections.extend(
        [
            "source.present AS source_present",
            "target.present AS target_present",
            "source.representative_record AS source_record",
            "target.representative_record AS target_record",
        ]
    )
    for index in range(len(aggregates)):
        projections.extend(
            [
                f"source.{host.identifier('aggregate_' + str(index))} "
                f"AS {host.identifier('source_aggregate_' + str(index))}",
                f"target.{host.identifier('aggregate_' + str(index))} "
                f"AS {host.identifier('target_aggregate_' + str(index))}",
            ]
        )
    host.connection.execute(
        f"CREATE OR REPLACE TEMP TABLE {host.identifier(joined)} AS "
        f"SELECT {', '.join(projections)} FROM "
        f"{host.identifier(table_names['source'])} source FULL OUTER JOIN "
        f"{host.identifier(table_names['target'])} target ON {condition}"
    )
    joined_table = host.identifier(joined)
    source_present = "source_present IS NOT NULL"
    target_present = "target_present IS NOT NULL"
    common = f"({source_present} AND {target_present})"
    row_mismatch = f"({common} AND source_present <> target_present)"
    duplicate = "(coalesce(source_present, 0) > 1 OR coalesce(target_present, 0) > 1)"
    applicable = []
    failed = []
    for index in range(len(aggregates)):
        source_value = host.identifier("source_aggregate_" + str(index))
        target_value = host.identifier("target_aggregate_" + str(index))
        applicable.append(
            f"({common} AND NOT ({source_value} IS NULL AND {target_value} IS NULL))"
        )
        failed.append(
            f"({applicable[-1]} AND {source_value} IS DISTINCT FROM {target_value})"
        )
    any_failure = " OR ".join([row_mismatch, duplicate, *failed])
    expressions = [
        f"count(*) FILTER (WHERE {source_present}) AS source_groups",
        f"count(*) FILTER (WHERE {target_present}) AS target_groups",
        f"count(*) FILTER (WHERE {common}) AS common_groups",
        f"count(*) FILTER (WHERE {row_mismatch}) AS row_failed",
        f"count(*) FILTER (WHERE {duplicate}) AS duplicate_failed",
        f"count(*) FILTER (WHERE {any_failure}) AS mismatch_groups",
    ]
    expressions.extend(
        f"count(*) FILTER (WHERE {value}) AS applicable_{index}"
        for index, value in enumerate(applicable)
    )
    expressions.extend(
        f"count(*) FILTER (WHERE {value}) AS failed_{index}"
        for index, value in enumerate(failed)
    )
    summary = host.fetch_one(
        f"SELECT {', '.join(expressions)} FROM {joined_table}"
    )
    source_group_count = int(summary["source_groups"] or 0)
    target_group_count = int(summary["target_groups"] or 0)
    common_count = int(summary["common_groups"] or 0)
    missing = source_group_count - common_count
    extra = target_group_count - common_count
    aggregate_total = (
        int(summary["row_failed"] or 0)
        + int(summary["duplicate_failed"] or 0)
        + sum(int(summary[f"applicable_{index}"] or 0) for index in range(len(aggregates)))
    )
    aggregate_failed = (
        int(summary["row_failed"] or 0)
        + int(summary["duplicate_failed"] or 0)
        + sum(int(summary[f"failed_{index}"] or 0) for index in range(len(aggregates)))
    )
    mismatch_groups = int(summary["mismatch_groups"] or 0)
    difference_count = missing + extra + mismatch_groups
    metrics = {
        "status": "PASS" if difference_count == 0 else "FAIL",
        "matching_mode": "GROUP_RECONCILIATION",
        "comparison_mode": "GROUP_RECONCILIATION",
        "source_group_count": source_group_count,
        "target_group_count": target_group_count,
        "group_count": source_group_count + extra,
        "common_group_count": common_count,
        "matched_group_count": common_count,
        "missing_group_count": missing,
        "extra_group_count": extra,
        "groups_with_aggregate_mismatch": mismatch_groups,
        "groups_with_mismatch": mismatch_groups,
        "group_mismatch_count": mismatch_groups,
        "group_difference_count": difference_count,
        "mismatch_group_count": mismatch_groups,
        "mismatch_count": difference_count,
        "source_group_coverage_pct": safe_rate_pct(
            common_count, source_group_count, zero_value=100.0
        ),
        "target_group_coverage_pct": safe_rate_pct(
            common_count, target_group_count, zero_value=100.0
        ),
        "source_group_coverage": safe_rate_pct(
            common_count, source_group_count, zero_value=100.0
        ),
        "target_group_coverage": safe_rate_pct(
            common_count, target_group_count, zero_value=100.0
        ),
        "aggregate_checks_total": aggregate_total,
        "aggregate_check_count": aggregate_total,
        "aggregate_checks_passed": aggregate_total - aggregate_failed,
        "aggregate_checks_failed": aggregate_failed,
        "checks_total": aggregate_total,
        "checks_passed": aggregate_total - aggregate_failed,
        "checks_failed": aggregate_failed,
    }
    failing_groups = host.fetch_all(
        f"SELECT * FROM {joined_table} WHERE NOT ({source_present} AND {target_present}) "
        f"OR {any_failure} LIMIT {host.evidence_limit}"
    ) if difference_count else []
    evidence_rows = []

    def aggregate_text(value):
        return None if value is None else str(value)

    def numeric_difference(source_value, target_value):
        try:
            return float(target_value) - float(source_value)
        except (TypeError, ValueError):
            return None

    for row in failing_groups:
        group_key = [
            aggregate_text(
                row.get("source_" + alias)
                if row.get("source_" + alias) is not None
                else row.get("target_" + alias)
            )
            for alias in aliases
        ]
        source_count = row.get("source_present")
        target_count = row.get("target_present")
        common_group = source_count is not None and target_count is not None
        common_fields = {
            "group_key": group_key,
            "source_record": row.get("source_record"),
            "target_record": row.get("target_record"),
        }

        if not common_group:
            evidence_rows.append(
                {
                    **common_fields,
                    "source_aggregate": None,
                    "target_aggregate": None,
                    "source_column": None,
                    "target_column": None,
                    "operation": None,
                    "difference": None,
                    "status": (
                        "EXTRA_GROUP_IN_TARGET"
                        if source_count is None
                        else "MISSING_GROUP_IN_TARGET"
                    ),
                    "matched": False,
                }
            )
            continue

        if source_count != target_count:
            evidence_rows.append(
                {
                    **common_fields,
                    "source_aggregate": str(source_count),
                    "target_aggregate": str(target_count),
                    "source_column": "Rows",
                    "target_column": "Rows",
                    "operation": "COUNT",
                    "difference": float(target_count - source_count),
                    "status": "GROUP_ROW_COUNT_MISMATCH",
                    "matched": False,
                }
            )

        if source_count > 1 or target_count > 1:
            evidence_rows.append(
                {
                    **common_fields,
                    "source_aggregate": str(source_count or 0),
                    "target_aggregate": str(target_count or 0),
                    "source_column": "Rows",
                    "target_column": "Rows",
                    "operation": "DUPLICATE COUNT",
                    "difference": float((target_count or 0) - (source_count or 0)),
                    "status": "GROUP_DUPLICATE_ROWS",
                    "matched": False,
                }
            )

        for index, item in enumerate(aggregates):
            source_value = row.get("source_aggregate_" + str(index))
            target_value = row.get("target_aggregate_" + str(index))
            if source_value is None and target_value is None:
                continue
            if source_value == target_value:
                continue
            evidence_rows.append(
                {
                    **common_fields,
                    "source_aggregate": aggregate_text(source_value),
                    "target_aggregate": aggregate_text(target_value),
                    "source_column": item["source_column"],
                    "target_column": item["target_column"],
                    "operation": str(item.get("operation", "AVG")).upper(),
                    "difference": numeric_difference(source_value, target_value),
                    "status": "GROUP_VALUE_MISMATCH",
                    "matched": False,
                }
            )

    exception_count = missing + extra + aggregate_failed
    evidence_rows = evidence_rows[: host.evidence_limit]
    return {
        "metrics": metrics,
        "evidence": {
            "group_reconciliation": {
                "count": exception_count,
                "sample": evidence_rows,
                "truncated": exception_count > host.evidence_limit,
            }
        },
    }


DUCKDB_COMPARATORS = {
    "L1": DuckDBSchemaComparator(),
    "L2": DuckDBVolumeComparator(),
    "L3": DuckDBRecordComparator(),
    "L4": DuckDBFieldComparator(),
    "L5": DuckDBAggregateComparator(),
    "L6": DuckDBDQComparator(),
}
