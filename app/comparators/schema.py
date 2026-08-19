from __future__ import annotations

from typing import Any

from app.metrics import safe_rate_pct


class SparkSchemaComparator:
    """L1 schema comparison on Spark DataFrames."""

    def execute(
        self,
        host: Any,
        source: Any,
        target: Any,
        configuration: dict[str, Any],
    ) -> dict[str, Any]:
        maps = host._maps(configuration)
        ignored = set(configuration.get("ignored_columns", []))

        source_fields = {
            field.name: field.dataType.simpleString()
            for field in source.schema.fields
            if field.name not in ignored
            and maps.get(field.name, field.name) not in ignored
        }
        target_fields = {
            field.name: field.dataType.simpleString()
            for field in target.schema.fields
            if field.name not in ignored
        }
        source_nullable = {
            field.name: field.nullable
            for field in source.schema.fields
            if field.name not in ignored
        }
        target_nullable = {
            field.name: field.nullable
            for field in target.schema.fields
            if field.name not in ignored
        }

        missing = []
        unexpected = []
        type_mismatches = []
        matched = []
        nullable_mismatches = []
        length_mismatches = []
        mapped_targets = set()

        for source_column, source_type in source_fields.items():
            target_column = maps.get(source_column, source_column)
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

            if source_nullable[source_column] != target_nullable[target_column]:
                nullable_mismatches.append(
                    {
                        "source_column": source_column,
                        "target_column": target_column,
                        "source_nullable": source_nullable[source_column],
                        "target_nullable": target_nullable[target_column],
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
                "length_mismatch_count": len(length_mismatches),
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
                "length_mismatches": length_mismatches,
                "precision_scale_mismatches": [],
                "order_mismatch": {},
                "schema_drift": (
                    [{"type": "MISSING_COLUMN", **item} for item in missing]
                    + [{"type": "UNEXPECTED_COLUMN", **item} for item in unexpected]
                    + [{"type": "DATA_TYPE_CHANGED", **item} for item in type_mismatches]
                    + [
                        {"type": "NULLABILITY_CHANGED", **item}
                        for item in nullable_mismatches
                    ]
                ),
            },
        }
