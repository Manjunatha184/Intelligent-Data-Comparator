from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any

from app.metrics import safe_rate_pct


class FieldComparator:
    """
    L4 - Field-level comparison.

    Compares corresponding source and target fields using
    configuration supplied through ExecutionTask.

    The comparator is connector-neutral and contains no
    file/database/platform-specific logic.
    """

    def execute(self, task: Any) -> dict[str, Any]:

        configuration = task.configuration

        if (
            configuration.get("matching_mode") == "GROUP_RECONCILIATION"
            and not configuration.get("comparison_keys")
            and not any(key.startswith("dependency_") for key in configuration)
        ):
            return {
                "metrics": {
                    "status": "NOT_APPLICABLE",
                    "comparison_mode": "GROUP_RECONCILIATION",
                    "reason": "Row-level field comparison is not applicable without row matches.",
                },
                "evidence": {
                    "not_applicable": "Row-level field comparison is not applicable without row matches."
                },
            }

        execution_mode = getattr(
            task,
            "execution_mode",
            None,
        )

        if execution_mode is not None:
            configuration = dict(configuration)

            if hasattr(execution_mode, "value"):
                configuration["execution_mode"] = (
                    execution_mode.value
                )
            else:
                configuration["execution_mode"] = str(
                    execution_mode
                )

        source_records = configuration.get(
            "source_records"
        )

        target_records = configuration.get(
            "target_records"
        )

        if target_records is None:
            raise ValueError(
                "L4 requires target_records"
            )

        comparison_keys = configuration.get(
            "comparison_keys",
            []
        )

        # --------------------------------------------------------
        # PREFERRED PATH: consume L3 matching result
        # --------------------------------------------------------

        l3_result = self._get_l3_result(
            configuration
        )

        if l3_result is not None:
            if not l3_result["matched_pairs"] and not comparison_keys:
                return {
                    "metrics": {
                        "status": "NOT_APPLICABLE",
                        "comparison_mode": "GROUP_RECONCILIATION",
                        "reason": "Row-level field comparison is not applicable without row matches.",
                    },
                    "evidence": {
                        "not_applicable": "Row-level field comparison is not applicable without row matches."
                    },
                }
            return self.compare_matched_pairs(
                matched_pairs=l3_result["matched_pairs"],
                configuration=configuration,
                source_record_count=len(source_records),
                target_record_count=len(target_records),
                missing=l3_result.get("missing", []),
                extra=l3_result.get("extra", []),
                ambiguous=l3_result.get("ambiguous", []),
            )

        if configuration.get("matching_mode") == "GROUP_RECONCILIATION" and not comparison_keys:
            return {
                "metrics": {
                    "status": "NOT_APPLICABLE",
                    "comparison_mode": "GROUP_RECONCILIATION",
                    "reason": "Row-level field comparison is not applicable without row matches.",
                },
                "evidence": {
                    "not_applicable": "Row-level field comparison is not applicable without row matches."
                },
            }

        # --------------------------------------------------------
        # DIRECT EXECUTION
        #
        # Used by standalone L4 tests / direct comparator calls.
        # In the real execution pipeline L3 result is preferred.
        # --------------------------------------------------------

        if comparison_keys:
            return self.compare(
                source_records=source_records,
                target_records=target_records,
                comparison_keys=comparison_keys,
                configuration=configuration,
            )

        raise RuntimeError(
            "L4 requires the matching result from L3 "
            "or comparison_keys for standalone execution."
        )
    # ========================================================
    # STATE INJECTION HANDLER
    # ========================================================

    def _get_l3_result(
        self,
        configuration: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Retrieve the L3 record-comparison result injected by the
        Execution Engine.

        L3 is the single source of truth for record matching.

        L4 consumes ONLY:

            evidence["matched_pairs"]

        L3's matched_pairs contains only configured primary-key matches.
        """

        for key, value in configuration.items():

            if not key.startswith("dependency_"):
                continue

            # ------------------------------------------------
            # Dependency result can be either:
            #   1. a dictionary
            #   2. a result/model object
            # ------------------------------------------------

            if isinstance(value, dict):

                evidence = value.get(
                    "evidence",
                    {},
                )

            else:

                evidence = getattr(
                    value,
                    "evidence",
                    {},
                )

            if not isinstance(evidence, dict):
                continue

            # L3 supplies the authoritative primary-key match stream.

            matched_pairs = evidence.get(
                "matched_pairs",
                [],
            )

            # ------------------------------------------------
            # Support wrapped result format:
            #
            # {
            #     "count": 10,
            #     "items": [...]
            # }
            # ------------------------------------------------

            if isinstance(
                matched_pairs,
                dict,
            ):

                matched_pairs = matched_pairs.get(
                    "items",
                    [],
                )

            if not isinstance(
                matched_pairs,
                list,
            ):

                matched_pairs = []

            # ------------------------------------------------
            # Missing records
            # ------------------------------------------------

            missing = evidence.get(
                "missing_records",
                [],
            )

            if isinstance(
                missing,
                dict,
            ):

                missing = missing.get(
                    "items",
                    [],
                )

            if not isinstance(
                missing,
                list,
            ):

                missing = []

            # ------------------------------------------------
            # Extra records
            # ------------------------------------------------

            extra = evidence.get(
                "extra_records",
                [],
            )

            if isinstance(
                extra,
                dict,
            ):

                extra = extra.get(
                    "items",
                    [],
                )

            if not isinstance(
                extra,
                list,
            ):

                extra = []

            return {
                "matched_pairs": matched_pairs,
                "missing": missing,
                "extra": extra,
                "ambiguous": [],
            }

        return None

    def compare(
        self,
        source_records: list[dict[str, Any]],
        target_records: list[dict[str, Any]],
        comparison_keys: list[Any] | tuple[Any, ...],
        configuration: dict[str, Any] | None = None,
    ) -> dict[str, Any]:

        configuration = configuration or {}

        mappings = self._normalize_mappings(
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

        selected_columns = configuration.get(
            "selected_columns"
        )

        normalization = configuration.get(
            "normalization",
            {},
        )

        tolerances = configuration.get(
            "tolerances",
            {},
        )

        source_index = self._build_index(
            source_records,
            comparison_keys,
            "source",
        )

        target_index = self._build_index(
            target_records,
            comparison_keys,
            "target",
        )

        common_keys = (
            set(source_index)
            & set(target_index)
        )

        field_mismatches: list[dict[str, Any]] = []

        compared_field_count = 0
        matched_field_count = 0

        for record_key in sorted(
            common_keys,
            key=str,
        ):

            source_record = source_index[
                record_key
            ]

            target_record = target_index[
                record_key
            ]

            columns = self._resolve_columns(
                source_record=source_record,
                target_record=target_record,
                mappings=mappings,
                selected_columns=selected_columns,
                ignored_columns=ignored_columns,
            )

            for source_column, target_column in columns:

                compared_field_count += 1

                source_value = source_record.get(
                    source_column
                )

                target_value = target_record.get(
                    target_column
                )
                mapping = mappings.get(
                    source_column,
                    {},
                )

                comparison = self._compare_value(
                    source_value=source_value,
                    target_value=target_value,
                    source_column=source_column,
                    target_column=target_column,
                    normalization=normalization,
                    tolerances=tolerances,
                    mapping=mapping,
                )

                if comparison["matched"]:

                    matched_field_count += 1

                else:

                    field_mismatches.append(
                        {
                            "key": self._serialize_key(
                                record_key
                            ),
                            "source_column": source_column,
                            "target_column": target_column,
                            "source_value": source_value,
                            "target_value": target_value,
                            "source_record": source_record,
                            "target_record": target_record,
                            **comparison,
                        }
                    )

        mismatch_count = len(
            field_mismatches
        )
        records_with_mismatch = len(
            {
                mismatch["key"]
                for mismatch in field_mismatches
            }
        )

        return {
            "metrics": {
                "status": (
                    "PASS"
                    if mismatch_count == 0
                    else "FAIL"
                ),
                "source_record_count": len(
                    source_records
                ),
                "target_record_count": len(
                    target_records
                ),
                "matched_record_count": len(
                    common_keys
                ),
                "compared_field_count": (
                    compared_field_count
                ),
                "matched_field_count": (
                    matched_field_count
                ),
                "mismatch_count": mismatch_count,
                "field_conformity_pct": (
                    safe_rate_pct(
                        matched_field_count,
                        compared_field_count,
                        zero_value=100.0,
                    )
                ),
                "field_mismatch_rate_pct": (
                    safe_rate_pct(
                        mismatch_count,
                        compared_field_count,
                    )
                ),
                "records_with_mismatch": (
                    records_with_mismatch
                ),
                "affected_record_rate_pct": (
                    safe_rate_pct(
                        records_with_mismatch,
                        len(common_keys),
                    )
                ),
            },
            "evidence": {
                "comparison_keys": list(
                    comparison_keys
                ),
                "field_mismatches": (
                    field_mismatches[:100]
                ),
            },
        }

    def compare_matched_pairs(
        self,
        matched_pairs: list[dict[str, Any]],
        configuration: dict[str, Any],
        source_record_count: int,
        target_record_count: int,
        missing: list[dict[str, Any]],
        extra: list[dict[str, Any]],
        ambiguous: list[dict[str, Any]],
    ) -> dict[str, Any]:

        mappings = self._normalize_mappings(
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

        selected_columns = configuration.get(
            "selected_columns"
        )

        normalization = configuration.get(
            "normalization",
            {},
        )

        tolerances = configuration.get(
            "tolerances",
            {},
        )

        field_mismatch_sample_limit = configuration.get(
            "field_mismatch_sample_limit",
            100,
        )

        field_mismatches = []

        compared_field_count = 0
        matched_field_count = 0

        for pair in matched_pairs:

            source_record = pair[
                "source_record"
            ]

            target_record = pair[
                "target_record"
            ]

            signature = pair.get(
                "signature",
                "Missing primary key"
            )

            match_type = pair.get(
                "match_type",
                pair.get("match_method", "BUSINESS_KEY"),
            )
            
            columns = self._resolve_columns(
                source_record=source_record,
                target_record=target_record,
                mappings=mappings,
                selected_columns=selected_columns,
                ignored_columns=ignored_columns,
            )

            for source_column, target_column in columns:


                compared_field_count += 1

                source_value = source_record.get(
                    source_column
                )

                target_value = target_record.get(
                    target_column
                )

                mapping = mappings.get(
                    source_column,
                    {},
                )

                comparison = self._compare_value(
                    source_value=source_value,
                    target_value=target_value,
                    source_column=source_column,
                    target_column=target_column,
                    normalization=normalization,
                    tolerances=tolerances,
                    mapping=mapping,
                )

                if comparison["matched"]:

                    matched_field_count += 1

                else:

                    field_mismatches.append(
                        {
                            "key": signature,
                            "match_type": match_type,
                            "source_column": source_column,
                            "target_column": target_column,
                            "source_value": source_value,
                            "target_value": target_value,
                            "source_record": source_record,
                            "target_record": target_record,
                            **comparison,
                        }
                    )

        mismatch_count = len(
            field_mismatches
        )
        records_with_mismatch = len(
            {
                mismatch["key"]
                for mismatch in field_mismatches
            }
        )

        return {
            "metrics": {
                "status": (
                    "PASS"
                    if mismatch_count == 0
                    and not missing
                    and not extra
                    and not ambiguous
                    else "FAIL"
                ),

                "source_record_count": (
                    source_record_count
                ),

                "target_record_count": (
                    target_record_count
                ),

                "matched_record_count": (
                    len(matched_pairs)
                ),

                "compared_field_count": (
                    compared_field_count
                ),

                "matched_field_count": (
                    matched_field_count
                ),

                "mismatch_count": (
                    mismatch_count
                ),

                "field_conformity_pct": (
                    safe_rate_pct(
                        matched_field_count,
                        compared_field_count,
                        zero_value=100.0,
                    )
                ),

                "field_mismatch_rate_pct": (
                    safe_rate_pct(
                        mismatch_count,
                        compared_field_count,
                    )
                ),

                "records_with_mismatch": (
                    records_with_mismatch
                ),

                "affected_record_rate_pct": (
                    safe_rate_pct(
                        records_with_mismatch,
                        len(matched_pairs),
                    )
                ),

                "missing_record_count": (
                    len(missing)
                ),

                "extra_record_count": (
                    len(extra)
                ),

                "ambiguous_record_count": (
                    len(ambiguous)
                ),
            },

            "evidence": {
                "matching_mode": "PRIMARY_KEY",

                "field_mismatches": (
                    list(field_mismatches)
                    if field_mismatch_sample_limit is None
                    else field_mismatches[
                        :field_mismatch_sample_limit
                    ]
                ),

                "missing": missing[:100],

                "extra": extra[:100],

                "ambiguous": ambiguous[:100],
            },
        }

    # ============================================================
    # RECORD INDEX
    # ============================================================

    @staticmethod
    def _build_index(
        records: list[dict[str, Any]],
        comparison_keys: list[Any] | tuple[Any, ...],
        side: str,
    ) -> dict[
        tuple[Any, ...],
        dict[str, Any],
    ]:

        index = {}

        for record in records:

            key = tuple(
                FieldComparator._get_key_value(
                    record,
                    comparison_key,
                    side,
                )
                for comparison_key in comparison_keys
            )

            index[key] = record

        return index


    @staticmethod
    def _get_key_value(
        record: dict[str, Any],
        key: Any,
        side: str,
    ) -> Any:

        if isinstance(key, str):
            return record.get(key)

        if isinstance(key, dict):

            if side == "source":
                column = key.get("source_column")
            elif side == "target":
                column = key.get("target_column")
            else:
                raise ValueError(
                    f"Unsupported comparison side: {side}"
                )

            if not column:
                raise ValueError(
                    f"Comparison key missing {side}_column: {key}"
                )

            return record.get(column)

        raise ValueError(
            f"Unsupported comparison key: {key}"
        )

    # ============================================================
    # COLUMN RESOLUTION
    # ============================================================

    @staticmethod
    def _normalize_mappings(
        mappings: Any,
    ) -> dict[str, dict[str, Any]]:

        if isinstance(mappings, dict):

            normalized = {}

            for source_column, mapping in mappings.items():

                if isinstance(mapping, str):

                    normalized[source_column] = {
                        "target_column": mapping,
                    }

                elif isinstance(mapping, dict):

                    normalized[source_column] = dict(
                        mapping
                    )

            return normalized

        result = {}

        for mapping in mappings or []:

            if not isinstance(mapping, dict):
                continue

            source_column = mapping.get(
                "source_column"
            )

            target_column = mapping.get(
                "target_column"
            )

            if not source_column or not target_column:
                continue

            result[source_column] = {
                "target_column": target_column,
                "normalize": mapping.get(
                    "normalize",
                    False,
                ),
                "normalization": dict(mapping.get("normalization") or {}),
                "comparison_type": mapping.get(
                    "comparison_type"
                ),
                "tolerance": mapping.get(
                    "tolerance"
                ),
                "tolerance_pct": mapping.get(
                    "tolerance_pct"
                ),
                "regex": mapping.get(
                    "regex"
                ),
            }

        return result

    @staticmethod
    def _resolve_columns(
        source_record: dict[str, Any],
        target_record: dict[str, Any],
        mappings: dict[str, str],
        selected_columns: list[str] | None,
        ignored_columns: set[str],
    ) -> list[tuple[str, str]]:

        if selected_columns is None:

            source_columns = list(
                source_record.keys()
            )

        else:

            source_columns = list(
                selected_columns
            )

        columns = []

        for source_column in source_columns:

            if source_column in ignored_columns:
                continue

            mapping = mappings.get(
                source_column,
                {},
            )

            target_column = mapping.get(
                "target_column",
                source_column,
            )

            if target_column in ignored_columns:
                continue

            if target_column not in target_record:
                continue

            columns.append(
                (
                    source_column,
                    target_column,
                )
            )

        return columns

    # ============================================================
    # VALUE COMPARISON
    # ============================================================

    @classmethod
    def _compare_value(
        cls,
        source_value: Any,
        target_value: Any,
        source_column: str,
        target_column: str,
        normalization: dict[str, Any],
        tolerances: dict[str, Any],
        mapping: dict[str, Any] | None = None,
    ) -> dict[str, Any]:

        source_normalized = cls._normalize_value(
            source_value,
            source_column,
            normalization,
            (mapping or {}).get("normalization"),
        )

        target_normalized = cls._normalize_value(
            target_value,
            target_column,
            normalization,
            (mapping or {}).get("normalization"),
        )

        comparison_type = str(
            (mapping or {}).get(
                "comparison_type",
                "",
            )
            or ""
        ).upper()

        if comparison_type == "REGEX":
            return cls._compare_regex(
                target_normalized,
                (mapping or {}).get("regex"),
            )

        # Exact equality after normalization.
        if source_normalized == target_normalized:

            return {
                "matched": True,
                "comparison_type": "EXACT",
                "difference": None,
                "tolerance": None,
            }

        if comparison_type == "EXACT":
            return cls._exact_mismatch(
                source_normalized,
                target_normalized,
            )

        # Numeric tolerance.
        numeric_tolerance = cls._resolve_tolerance(
            tolerances,
            source_column,
            target_column,
            "numeric",
            mapping=mapping,
        )

        numeric_tolerance_pct = (
            (mapping or {}).get("tolerance_pct")
        )

        if numeric_tolerance is not None or numeric_tolerance_pct is not None:

            numeric_result = cls._compare_numeric(
                source_normalized,
                target_normalized,
                tolerance=numeric_tolerance,
                tolerance_pct=numeric_tolerance_pct,
            )

            if numeric_result is not None:
                return numeric_result

        # Time tolerance.
        time_tolerance = (
            None
            if comparison_type == "NUMERIC"
            else cls._resolve_tolerance(
                tolerances,
                source_column,
                target_column,
                "time",
            )
        )

        if time_tolerance is not None:

            time_result = cls._compare_datetime(
                source_normalized,
                target_normalized,
                time_tolerance,
            )

            if time_result is not None:
                return time_result

        return cls._exact_mismatch(
            source_normalized,
            target_normalized,
        )

    @staticmethod
    def _exact_mismatch(
        source_value: Any,
        target_value: Any,
    ) -> dict[str, Any]:

        if source_value is None and target_value is not None:
            difference: float | str = "NULL_TO_VALUE"
        elif source_value is not None and target_value is None:
            difference = "VALUE_TO_NULL"
        else:
            numeric_difference = (
                FieldComparator._numeric_difference(
                    source_value,
                    target_value,
                )
            )
            difference = (
                numeric_difference
                if numeric_difference is not None
                else "VALUE_CHANGED"
            )

        return {
            "matched": False,
            "comparison_type": "EXACT",
            "difference": difference,
            "tolerance": None,
        }

    @staticmethod
    def _numeric_difference(
        source_value: Any,
        target_value: Any,
    ) -> float | None:

        try:
            source_number = Decimal(
                str(source_value)
            )
            target_number = Decimal(
                str(target_value)
            )
        except (
            InvalidOperation,
            ValueError,
            TypeError,
        ):
            return None

        return float(
            target_number
            - source_number
        )

    @staticmethod
    def _compare_regex(
        target_value: Any,
        pattern: Any,
    ) -> dict[str, Any]:

        if not pattern:
            raise ValueError(
                "REGEX field comparison requires a regex pattern"
            )

        try:
            compiled = re.compile(str(pattern))
        except re.error as exc:
            raise ValueError(
                "Invalid REGEX field comparison pattern: "
                f"{exc}"
            ) from exc

        matched = bool(
            compiled.fullmatch(
                "" if target_value is None else str(target_value)
            )
        )

        return {
            "matched": matched,
            "comparison_type": "REGEX",
            "difference": (
                None
                if matched
                else "REGEX_MISMATCH"
            ),
            "tolerance": None,
            "regex": str(pattern),
        }

    # ============================================================
    # NORMALIZATION
    # ============================================================

    @classmethod
    def _normalize_value(
        cls,
        value: Any,
        column: str,
        normalization: dict[str, Any],
        mapping_rules: dict[str, Any] | None = None,
    ) -> Any:

        if value is None:
            return None

        rules = normalization.get(
            column,
            {},
        )

        if isinstance(
            rules,
            list,
        ):
            rules = {
                rule: True
                for rule in rules
            }

        if not isinstance(rules, dict):
            rules = {}

        if isinstance(mapping_rules, dict):
            rules = {**rules, **mapping_rules}

        result = value

        if rules.get("trim") and isinstance(
            result,
            str,
        ):
            result = result.strip()

        if rules.get("case_insensitive") and isinstance(
            result,
            str,
        ):
            result = result.casefold()

        if rules.get("empty_as_null") and result == "":
            result = None

        if rules.get("round") is not None:

            digits = int(
                rules["round"]
            )

            if isinstance(result, (int, float, Decimal)) and not isinstance(result, bool):
                result = round(result, digits)

        return result

    # ============================================================
    # NUMERIC COMPARISON
    # ============================================================

    @staticmethod
    def _compare_numeric(
        source_value: Any,
        target_value: Any,
        tolerance: Any,
        tolerance_pct: Any = None,
    ) -> dict[str, Any] | None:

        try:
            source_number = Decimal(str(source_value))
            target_number = Decimal(str(target_value))
        except (InvalidOperation, ValueError, TypeError):
            return None

        difference = target_number - source_number

        if tolerance_pct is not None:
            try:
                tolerance_pct_num = Decimal(str(tolerance_pct))
            except (InvalidOperation, ValueError, TypeError):
                return None

            if source_number == 0:
                allowed_difference = Decimal("0")
            else:
                allowed_difference = abs(source_number) * (tolerance_pct_num / Decimal("100"))

            return {
                "matched": abs(difference) <= allowed_difference,
                "comparison_type": "PERCENTAGE_TOLERANCE",
                "difference": float(difference),
                "tolerance": float(tolerance_pct_num),
                "tolerance_type": "PERCENTAGE",
            }

        try:
            tolerance_number = Decimal(str(tolerance))
        except (InvalidOperation, ValueError, TypeError):
            return None

        return {
            "matched": abs(difference) <= tolerance_number,
            "comparison_type": "NUMERIC_TOLERANCE",
            "difference": float(difference),
            "tolerance": float(tolerance_number),
        }

    # ============================================================
    # DATETIME COMPARISON
    # ============================================================

    @staticmethod
    def _compare_datetime(
        source_value: Any,
        target_value: Any,
        tolerance: Any,
    ) -> dict[str, Any] | None:

        source_dt = FieldComparator._to_datetime(
            source_value
        )

        target_dt = FieldComparator._to_datetime(
            target_value
        )

        if source_dt is None or target_dt is None:
            return None

        try:
            tolerance_seconds = float(
                tolerance
            )
        except (
            ValueError,
            TypeError,
        ):
            return None

        difference_seconds = (
            target_dt - source_dt
        ).total_seconds()

        return {
            "matched": abs(
                difference_seconds
            ) <= tolerance_seconds,
            "comparison_type": "TIME_TOLERANCE",
            "difference_seconds": (
                difference_seconds
            ),
            "tolerance_seconds": (
                tolerance_seconds
            ),
            "difference": difference_seconds,
            "tolerance": tolerance_seconds,
        }

    @staticmethod
    def _to_datetime(
        value: Any,
    ) -> datetime | None:

        if isinstance(
            value,
            datetime,
        ):
            return value

        if isinstance(
            value,
            date,
        ):
            return datetime.combine(
                value,
                datetime.min.time(),
            )

        if not isinstance(value, str):
            return None

        try:

            return datetime.fromisoformat(
                value
            )

        except ValueError:

            return None

    # ============================================================
    # TOLERANCE RESOLUTION
    # ============================================================

    @staticmethod
    def _resolve_tolerance(
        tolerances: dict[str, Any],
        source_column: str,
        target_column: str,
        tolerance_type: str,
        mapping: dict[str, Any] | None = None,
    ) -> Any:

        # --------------------------------------------------------
        # COLUMN MAPPING TOLERANCE
        # --------------------------------------------------------

        if isinstance(mapping, dict):

            mapping_tolerance = mapping.get(
                "tolerance"
            )

            if mapping_tolerance is not None:

                if isinstance(
                    mapping_tolerance,
                    dict,
                ):

                    value = mapping_tolerance.get(
                        tolerance_type
                    )

                    if value is not None:
                        return value

                    # Allow a generic numeric tolerance
                    # inside a mapping dictionary.
                    if tolerance_type == "numeric":

                        value = mapping_tolerance.get(
                            "value"
                        )

                        if value is not None:
                            return value

                elif tolerance_type == "numeric":

                    return mapping_tolerance

        # --------------------------------------------------------
        # GLOBAL TOLERANCE
        # --------------------------------------------------------

        column_config = tolerances.get(
            source_column
        )

        if column_config is None:

            column_config = tolerances.get(
                target_column
            )

        if isinstance(
            column_config,
            dict,
        ):

            return column_config.get(
                tolerance_type
            )

        return None

    # ============================================================
    # KEY SERIALIZATION
    # ============================================================

    @staticmethod
    def _serialize_key(
        key: tuple[Any, ...],
    ) -> Any:

        if len(key) == 1:
            return key[0]

        return list(key)
