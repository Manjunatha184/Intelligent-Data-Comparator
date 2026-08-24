from __future__ import annotations

from app.comparators.duckdb_levels import DuckDBFieldComparator as _BaseDuckDBFieldComparator
from app.metrics import safe_rate_pct


class DuckDBFieldComparator(_BaseDuckDBFieldComparator):
    """L4 hash-gated field comparison for unambiguous one-to-one keys only."""

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
        primary_matched_records = int(reconciliation["counts"]["matched"] or 0)
        source_stats = host.stats(source, configuration, "source")
        target_stats = host.stats(target, configuration, "target")

        ambiguity_summary = host.fetch_one(
            f"""
            SELECT
                count(*) FILTER (
                    WHERE reconciliation_status = 'MATCHED'
                      AND coalesce(source_key_count, 0) = 1
                      AND coalesce(target_key_count, 0) = 1
                ) AS eligible_records,
                count(DISTINCT normalized_primary_key) FILTER (
                    WHERE reconciliation_status = 'MATCHED'
                      AND (coalesce(source_key_count, 0) > 1
                           OR coalesce(target_key_count, 0) > 1)
                ) AS ambiguous_duplicate_keys
            FROM {table}
            """
        )
        eligible_records = int(ambiguity_summary["eligible_records"] or 0)
        ambiguous_duplicate_key_count = int(
            ambiguity_summary["ambiguous_duplicate_keys"] or 0
        )

        if not columns:
            result = self._empty_result(
                source_stats,
                target_stats,
                eligible_records,
                reconciliation,
            )
            result["metrics"].update(
                {
                    "primary_matched_record_count": primary_matched_records,
                    "field_comparison_eligible_count": eligible_records,
                    "ambiguous_duplicate_key_count": ambiguous_duplicate_key_count,
                    "ambiguous_record_count": ambiguous_duplicate_key_count,
                }
            )
            return result

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
            numeric = self._numeric_type(
                source.type_names[source_column]
            ) and self._numeric_type(target.type_names[target_column])
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
              AND coalesce(source_key_count, 0) = 1
              AND coalesce(target_key_count, 0) = 1
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

        source_record_columns = ", ".join(
            host.identifier("source__" + column) for column in source.columns
        )
        target_record_columns = ", ".join(
            host.identifier("target__" + column) for column in target.columns
        )
        mismatch_queries = []
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
        mismatch_rows = (
            host.fetch_all(
                f"SELECT * FROM ({mismatch_union}) mismatches "
                f"ORDER BY normalized_primary_key, source_column "
                f"LIMIT {host.evidence_limit}"
            )
            if mismatch_count
            else []
        )
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

        duplicate_rows = host.fetch_all(
            f"""
            SELECT normalized_primary_key,
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
            FROM {table}
            WHERE reconciliation_status = 'MATCHED'
              AND (coalesce(source_key_count, 0) > 1
                   OR coalesce(target_key_count, 0) > 1)
            GROUP BY normalized_primary_key
            ORDER BY normalized_primary_key
            LIMIT {host.evidence_limit}
            """
        ) if ambiguous_duplicate_key_count else []
        duplicate_sample = []
        for row in duplicate_rows:
            source_record = row["source_record"]
            target_record = row["target_record"]
            duplicate_sample.append(
                {
                    "key": self._key_from_records(
                        source_record,
                        target_record,
                        configuration.get("comparison_keys", []),
                    ),
                    "source_occurrences": int(row["source_occurrences"] or 0),
                    "target_occurrences": int(row["target_occurrences"] or 0),
                    "compared_pairs": int(row["compared_pairs"] or 0),
                    "source_record": source_record,
                    "target_record": target_record,
                    "status": "AMBIGUOUS_DUPLICATE_KEY",
                    "field_comparison_performed": False,
                    "reason": "Business key does not uniquely identify one row on both sides",
                }
            )

        compared_fields = eligible_records * len(comparisons)
        return {
            "metrics": {
                "status": "PASS" if mismatch_count == 0 else "FAIL",
                "source_record_count": source_stats["total_rows"],
                "target_record_count": target_stats["total_rows"],
                "primary_matched_record_count": primary_matched_records,
                "matched_record_count": eligible_records,
                "field_comparison_eligible_count": eligible_records,
                "ambiguous_duplicate_key_count": ambiguous_duplicate_key_count,
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
                    records_with_mismatch, eligible_records
                ),
                "missing_record_count": reconciliation["counts"]["missing"],
                "extra_record_count": reconciliation["counts"]["extra"],
                "ambiguous_record_count": ambiguous_duplicate_key_count,
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
                    "count": ambiguous_duplicate_key_count,
                    "sample": duplicate_sample,
                    "truncated": ambiguous_duplicate_key_count > host.evidence_limit,
                },
            },
        }
