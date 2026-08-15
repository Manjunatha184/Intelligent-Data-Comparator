from __future__ import annotations

from typing import Any

from app.connectors.base import (
    ColumnMetadata,
    DatasetSchema,
    MetadataProvider,
)
from app.metrics import safe_rate_pct

# Backward-compatible alias.
#
# Existing L1 tests and internal code can continue using
# SchemaColumn while the canonical connector-neutral model
# lives in app.connectors.base.
SchemaColumn = ColumnMetadata

# Backward-compatible alias.
SchemaProvider = MetadataProvider


# ============================================================
# SCHEMA COMPARATOR
# ============================================================

class SchemaComparator:
    """
    L1 - Structural Comparison.

    Responsibilities:

        - column presence
        - unexpected columns
        - data type compatibility
        - nullable changes
        - column order
        - length
        - precision
        - scale
        - schema drift
        - mappings
        - ignored columns

    The comparator does NOT:

        - open files
        - connect to databases
        - execute SQL
        - read CSV
        - use pandas
        - know connector-specific APIs

    Connector-specific metadata access is delegated to
    MetadataProvider implementations.
    """

    def __init__(
        self,
        schema_providers: dict[
            str,
            SchemaProvider,
        ] | None = None,
    ) -> None:

        self._schema_providers = (
            schema_providers or {}
        )

    # ========================================================
    # PROVIDER REGISTRATION
    # ========================================================

    def register_provider(
        self,
        connector_type: str,
        provider: SchemaProvider,
    ) -> None:

        connector_type = (
            connector_type.strip().lower()
        )

        if not connector_type:
            raise ValueError(
                "connector_type cannot be empty"
            )

        if connector_type in self._schema_providers:
            raise ValueError(
                "Schema provider already registered: "
                f"{connector_type}"
            )

        self._schema_providers[
            connector_type
        ] = provider

    # ========================================================
    # PUBLIC EXECUTION
    # ========================================================

    def execute(
        self,
        task: Any,
    ) -> dict[str, Any]:

        configuration = task.configuration

        source = configuration.get("source")
        target = configuration.get("target")

        if not source:
            raise ValueError(
                "L1 requires source configuration"
            )

        if not target:
            raise ValueError(
                "L1 requires target configuration"
            )

        source_schema = self._get_schema(
            source
        )

        target_schema = self._get_schema(
            target
        )

        mappings = self._build_mappings(
            configuration.get(
                "column_mappings",
                [],
            )
        )

        ignored_columns = set(
            configuration.get(
                "ignored_columns",
                [],
            )
        )

        return self.compare(
            source_schema=source_schema,
            target_schema=target_schema,
            mappings=mappings,
            ignored_columns=ignored_columns,
        )

    # ========================================================
    # SCHEMA RESOLUTION
    # ========================================================

    def _get_schema(
        self,
        dataset: dict[str, Any],
    ) -> DatasetSchema:

        connector_type = dataset.get(
            "connector_type"
        )

        if not connector_type:
            raise ValueError(
                "Dataset connector_type is required"
            )

        connector_type = (
            connector_type.strip().lower()
        )

        provider = self._schema_providers.get(
            connector_type
        )

        if provider is None:
            raise ValueError(
                "No schema provider registered for "
                f"connector type: {connector_type}"
            )

        return provider.get_schema(
            dataset
        )

    # ========================================================
    # MAPPINGS
    # ========================================================

    @staticmethod
    def _build_mappings(
        mappings: list[dict[str, Any]],
    ) -> dict[str, str]:

        result: dict[str, str] = {}

        for mapping in mappings:

            source_column = mapping.get(
                "source_column"
            )

            target_column = mapping.get(
                "target_column"
            )

            if not source_column:
                raise ValueError(
                    "Column mapping is missing "
                    "source_column"
                )

            if not target_column:
                raise ValueError(
                    "Column mapping is missing "
                    "target_column"
                )

            if source_column in result:
                raise ValueError(
                    "Duplicate source column mapping: "
                    f"{source_column}"
                )

            result[source_column] = target_column

        return result

    # ========================================================
    # CORE COMPARISON
    # ========================================================

    def compare(
        self,
        source_schema: DatasetSchema,
        target_schema: DatasetSchema,
        mappings: dict[str, str] | None = None,
        ignored_columns: set[str] | None = None,
    ) -> dict[str, Any]:

        mappings = mappings or {}
        ignored_columns = ignored_columns or set()

        source_columns = self._filter_columns(
            source_schema.columns,
            ignored_columns,
        )

        target_columns = self._filter_columns(
            target_schema.columns,
            ignored_columns,
        )

        source_by_name = {
            column.name: column
            for column in source_columns
        }

        target_by_name = {
            column.name: column
            for column in target_columns
        }

        missing_columns: list[dict[str, Any]] = []
        unexpected_columns: list[dict[str, Any]] = []

        type_mismatches: list[dict[str, Any]] = []
        nullable_mismatches: list[dict[str, Any]] = []

        length_mismatches: list[dict[str, Any]] = []

        precision_scale_mismatches: list[
            dict[str, Any]
        ] = []

        matched_columns: list[dict[str, Any]] = []

        # ----------------------------------------------------
        # SOURCE -> TARGET
        # ----------------------------------------------------

        for source_column in source_columns:

            source_name = source_column.name

            target_name = mappings.get(
                source_name,
                source_name,
            )

            target_column = target_by_name.get(
                target_name
            )

            if target_column is None:

                missing_columns.append(
                    {
                        "source_column": source_name,
                        "expected_target_column": (
                            target_name
                        ),
                    }
                )

                continue

            matched_columns.append(
                {
                    "source_column": source_name,
                    "target_column": target_name,
                }
            )

            # ----------------------------------------------
            # DATA TYPE
            # ----------------------------------------------

            if not self._types_compatible(
                source_column,
                target_column,
            ):

                type_mismatches.append(
                    {
                        "source_column": source_name,
                        "target_column": target_name,
                        "source_type": (
                            source_column.data_type
                        ),
                        "target_type": (
                            target_column.data_type
                        ),
                    }
                )

            # ----------------------------------------------
            # NULLABILITY
            # ----------------------------------------------

            if (
                source_column.nullable
                != target_column.nullable
            ):

                nullable_mismatches.append(
                    {
                        "source_column": source_name,
                        "target_column": target_name,
                        "source_nullable": (
                            source_column.nullable
                        ),
                        "target_nullable": (
                            target_column.nullable
                        ),
                    }
                )

            # ----------------------------------------------
            # LENGTH
            # ----------------------------------------------

            if self._property_mismatch(
                source_column.length,
                target_column.length,
            ):

                length_mismatches.append(
                    {
                        "source_column": source_name,
                        "target_column": target_name,
                        "source_length": (
                            source_column.length
                        ),
                        "target_length": (
                            target_column.length
                        ),
                    }
                )

            # ----------------------------------------------
            # PRECISION / SCALE
            # ----------------------------------------------

            if (
                self._property_mismatch(
                    source_column.precision,
                    target_column.precision,
                )
                or self._property_mismatch(
                    source_column.scale,
                    target_column.scale,
                )
            ):

                precision_scale_mismatches.append(
                    {
                        "source_column": source_name,
                        "target_column": target_name,
                        "source_precision": (
                            source_column.precision
                        ),
                        "target_precision": (
                            target_column.precision
                        ),
                        "source_scale": (
                            source_column.scale
                        ),
                        "target_scale": (
                            target_column.scale
                        ),
                    }
                )

        # ----------------------------------------------------
        # TARGET-ONLY COLUMNS
        # ----------------------------------------------------

        mapped_target_columns = set(
            mappings.values()
        )

        for target_column in target_columns:

            target_name = target_column.name

            if target_name in mapped_target_columns:
                continue

            if target_name not in source_by_name:

                unexpected_columns.append(
                    {
                        "target_column": target_name,
                    }
                )

        # ----------------------------------------------------
        # COLUMN ORDER
        # ----------------------------------------------------

        expected_target_order = [
            mappings.get(
                column.name,
                column.name,
            )
            for column in source_columns
        ]

        actual_target_order = [
            column.name
            for column in target_columns
            if column.name in expected_target_order
        ]

        order_mismatch = (
            expected_target_order
            != actual_target_order
        )

        order_evidence: dict[str, Any] = {}

        if order_mismatch:

            order_evidence = {
                "expected_order": (
                    expected_target_order
                ),
                "actual_order": (
                    actual_target_order
                ),
            }

        # ----------------------------------------------------
        # SCHEMA DRIFT
        # ----------------------------------------------------

        schema_drift: list[dict[str, Any]] = []

        for item in missing_columns:

            schema_drift.append(
                {
                    "type": "MISSING_COLUMN",
                    **item,
                }
            )

        for item in unexpected_columns:

            schema_drift.append(
                {
                    "type": "UNEXPECTED_COLUMN",
                    **item,
                }
            )

        for item in type_mismatches:

            schema_drift.append(
                {
                    "type": "DATA_TYPE_CHANGED",
                    **item,
                }
            )

        for item in nullable_mismatches:

            schema_drift.append(
                {
                    "type": "NULLABILITY_CHANGED",
                    **item,
                }
            )

        for item in length_mismatches:

            schema_drift.append(
                {
                    "type": "LENGTH_CHANGED",
                    **item,
                }
            )

        for item in precision_scale_mismatches:

            schema_drift.append(
                {
                    "type": "PRECISION_SCALE_CHANGED",
                    **item,
                }
            )

        if order_mismatch:

            schema_drift.append(
                {
                    "type": "COLUMN_ORDER_CHANGED",
                    **order_evidence,
                }
            )

        # ----------------------------------------------------
        # RESULT
        # ----------------------------------------------------

        mismatch_count = (
            len(missing_columns)
            + len(unexpected_columns)
            + len(type_mismatches)
            + len(nullable_mismatches)
            + len(length_mismatches)
            + len(precision_scale_mismatches)
            + int(order_mismatch)
        )

        status = (
            "PASS"
            if mismatch_count == 0
            else "FAIL"
        )

        source_column_count = len(source_columns)
        target_column_count = len(target_columns)
        matched_column_count = len(matched_columns)

        return {
            "metrics": {
                "status": status,
                "source_column_count": (
                    source_column_count
                ),
                "target_column_count": (
                    target_column_count
                ),
                "matched_column_count": (
                    matched_column_count
                ),
                "source_column_coverage_pct": (
                    safe_rate_pct(
                        matched_column_count,
                        source_column_count,
                        zero_value=(
                            100.0
                            if matched_column_count == 0
                            else None
                        ),
                    )
                ),
                "target_column_coverage_pct": (
                    safe_rate_pct(
                        matched_column_count,
                        target_column_count,
                        zero_value=(
                            100.0
                            if matched_column_count == 0
                            else None
                        ),
                    )
                ),
                "missing_column_count": len(
                    missing_columns
                ),
                "unexpected_column_count": len(
                    unexpected_columns
                ),
                "data_type_mismatch_count": len(
                    type_mismatches
                ),
                "nullable_mismatch_count": len(
                    nullable_mismatches
                ),
                "length_mismatch_count": len(
                    length_mismatches
                ),
                "precision_scale_mismatch_count": len(
                    precision_scale_mismatches
                ),
                "order_mismatch_count": int(
                    order_mismatch
                ),
                "schema_drift_count": len(
                    schema_drift
                ),
                "mismatch_count": mismatch_count,
            },
            "evidence": {
                "matched_columns": matched_columns,
                "missing_columns": (
                    missing_columns
                ),
                "unexpected_columns": (
                    unexpected_columns
                ),
                "type_mismatches": (
                    type_mismatches
                ),
                "nullable_mismatches": (
                    nullable_mismatches
                ),
                "length_mismatches": (
                    length_mismatches
                ),
                "precision_scale_mismatches": (
                    precision_scale_mismatches
                ),
                "order_mismatch": order_evidence,
                "schema_drift": schema_drift,
            },
        }

    # ========================================================
    # HELPERS
    # ========================================================

    @staticmethod
    def _filter_columns(
        columns: tuple[SchemaColumn, ...],
        ignored_columns: set[str],
    ) -> list[SchemaColumn]:

        return [
            column
            for column in columns
            if column.name not in ignored_columns
        ]

    @staticmethod
    def _property_mismatch(
        source_value: Any,
        target_value: Any,
    ) -> bool:

        # If neither connector exposes the property,
        # there is nothing to compare.

        if (
            source_value is None
            and target_value is None
        ):
            return False

        return source_value != target_value

    @staticmethod
    def _types_compatible(
        source: SchemaColumn,
        target: SchemaColumn,
    ) -> bool:

        source_type = (
            source.data_type
            .strip()
            .upper()
        )

        target_type = (
            target.data_type
            .strip()
            .upper()
        )

        return source_type == target_type
