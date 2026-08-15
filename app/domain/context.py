from typing import Any
import re

from pydantic import BaseModel, Field, model_validator

from app.execution.models import (
    ComparisonLevel,
    ExecutionMode,
    ExecutionLocation,
    DataAccessMode,
)
from app.connectors.filters import RowFilter

class DatasetConfiguration(BaseModel):
    connector_type: str
    properties: dict = Field(default_factory=dict)


class ComparisonKey(BaseModel):
    source_column: str
    target_column: str


class ColumnMapping(BaseModel):
    source_column: str
    target_column: str
    normalize: bool = False
    normalization: dict[str, Any] = Field(default_factory=dict)
    comparison_type: str | None = None
    tolerance: float | None = None
    tolerance_pct: float | None = None
    regex: str | None = None

    @model_validator(mode="after")
    def validate_mapping(self):
        allowed_normalization = {"trim", "case_insensitive", "empty_as_null", "round"}
        unknown = set(self.normalization) - allowed_normalization
        if unknown:
            raise ValueError(f"Unsupported normalization option(s): {', '.join(sorted(unknown))}")
        if "round" in self.normalization:
            try:
                digits = int(self.normalization["round"])
            except (TypeError, ValueError) as exc:
                raise ValueError("normalization.round must be an integer") from exc
            if digits < 0:
                raise ValueError("normalization.round must be non-negative")
            self.normalization["round"] = digits
        if self.comparison_type is not None:
            self.comparison_type = (
                self.comparison_type.upper()
            )

        if self.comparison_type == "REGEX":
            if not self.regex:
                raise ValueError(
                    "REGEX column mapping requires 'regex'"
                )

            try:
                re.compile(self.regex)
            except re.error as exc:
                raise ValueError(
                    "Invalid REGEX column mapping pattern: "
                    f"{exc}"
                ) from exc

        if (
            self.comparison_type == "NUMERIC"
            and self.tolerance is not None
            and self.tolerance < 0
        ):
            raise ValueError(
                "NUMERIC column mapping tolerance must be "
                "greater than or equal to zero"
            )
            
        if self.tolerance_pct is not None and (self.tolerance_pct < 0 or self.tolerance_pct > 100):
            raise ValueError("tolerance_pct must be between 0 and 100")

        # Preserve an explicit comparison type for downstream executors and
        # persisted plans. The Spark executor still keys off the tolerance
        # fields themselves, so this is metadata rather than changed semantics.
        if self.comparison_type is None:
            if self.tolerance_pct is not None:
                self.comparison_type = "PERCENTAGE_TOLERANCE"
            elif self.tolerance is not None:
                self.comparison_type = "NUMERIC_TOLERANCE"

        # Remove false/undefined normalization entries so the persisted
        # mapping is stable and easy to inspect end-to-end.
        self.normalization = {
            key: value for key, value in self.normalization.items()
            if value is not None and value is not False
        }

        return self


class AggregateRule(BaseModel):
    name: str
    source_column: str | None = None
    target_column: str | None = None
    function: str
    group_by_columns: list[str] = Field(default_factory=list)
    source_group_by: list[str] = Field(default_factory=list)
    target_group_by: list[str] = Field(default_factory=list)
    tolerance: float | None = None
    tolerance_pct: float | None = None
    enabled: bool = True

    @model_validator(mode="after")
    def validate_rule(self):
        if self.tolerance_pct is not None and (self.tolerance_pct < 0 or self.tolerance_pct > 100):
            raise ValueError("tolerance_pct must be between 0 and 100")
        return self


class DQRule(BaseModel):
    rule_id: str
    name: str
    column: str | None = None
    rule_type: str
    apply_to: str = "BOTH"

    value: str | list[str] | int | float | None = None

    source_column: str | None = None
    target_column: str | None = None

    allowed_values: list[Any] | None = None

    min: float | None = None
    max: float | None = None

    regex: str | None = None

    columns: list[str] = Field(default_factory=list)

    tolerance: float | dict[str, float] | None = None

    condition: dict[str, Any] | None = None
    check: dict[str, Any] | None = None

    transformation: str | None = None

    source_value: float | int | None = None
    target_value: float | int | None = None

    enabled: bool = True

    @model_validator(mode="after")
    def validate_rule(self):
        self.apply_to = self.apply_to.upper()

        if self.apply_to not in {
            "SOURCE",
            "TARGET",
            "BOTH",
        }:
            raise ValueError(
                "apply_to must be SOURCE, TARGET, or BOTH"
            )

        rule_type = self.rule_type.upper()

        if rule_type == "PATTERN":
            if not self._has_scoped_columns():
                raise ValueError(
                    "PATTERN rule requires configured column(s)"
                )

            if not self.regex:
                raise ValueError(
                    "PATTERN rule requires 'regex'"
                )

        elif rule_type == "COMPLETENESS":
            if not self._has_scoped_columns():
                raise ValueError(
                    "COMPLETENESS rule requires configured column(s)"
                )

        elif rule_type == "VALIDITY":
            if not self._has_scoped_columns(): 
                raise ValueError(
                    "VALIDITY rule requires configured column(s)"
                )
        
        elif rule_type == "CONSISTENCY":
            if not self.columns:
                raise ValueError(
                    "CONSISTENCY rule requires 'columns'"
                )

        elif rule_type == "TIMELINESS":
            if not self._has_scoped_columns():
                raise ValueError(
                    "TIMELINESS rule requires configured column(s)"
                )

        elif rule_type == "REFERENTIAL_INTEGRITY":
            if not self.source_column:
                raise ValueError(
                    "REFERENTIAL_INTEGRITY rule requires "
                    "'source_column'"
                )

            if not self.target_column:
                raise ValueError(
                    "REFERENTIAL_INTEGRITY rule requires "
                    "'target_column'"
                )

        elif rule_type == "DISTRIBUTION":
            if not self._has_scoped_columns():
                raise ValueError(
                    "DISTRIBUTION rule requires configured column(s)"
                )

        elif rule_type == "CONDITIONAL":
            if not isinstance(self.condition, dict):
                raise ValueError(
                    "CONDITIONAL rule requires 'condition'"
                )

            if not isinstance(self.check, dict):
                raise ValueError(
                    "CONDITIONAL rule requires 'check'"
                )

        elif rule_type == "TRANSFORMATION":
            if not self.source_column:
                raise ValueError(
                    "TRANSFORMATION rule requires "
                    "'source_column'"
                )

            if not self.target_column:
                raise ValueError(
                    "TRANSFORMATION rule requires "
                    "'target_column'"
                )

        return self

    def _has_scoped_columns(self) -> bool:
        if self.column:
            return True

        if self.apply_to == "SOURCE":
            return bool(self.source_column)

        if self.apply_to == "TARGET":
            return bool(self.target_column)

        return bool(self.source_column and self.target_column)

class StrategyPolicy(BaseModel):
    max_exact_rows: int
    max_exact_bytes: int
    sampling_min_rows: int
    allow_sampling: bool = False
    prefer_pushdown: bool = True

class RuntimeConfiguration(BaseModel):
    configuration_id: int

    source: DatasetConfiguration
    target: DatasetConfiguration

    comparison_levels: list[ComparisonLevel]

    l7_enabled: bool = False

    comparison_keys: list[ComparisonKey]

    column_mappings: list[ColumnMapping]

    ignored_columns: list[str] = Field(default_factory=list)

    aggregate_rules: list[AggregateRule] = Field(default_factory=list)

    dq_rules: list[DQRule] = Field(default_factory=list)

    source_filters: list[RowFilter] = Field(default_factory=list)
    target_filters: list[RowFilter] = Field(default_factory=list)

    matching_mode: str = "ROW_LEVEL"
    grouping_attributes: list[ComparisonKey] = Field(default_factory=list)
    aggregation_columns: list[dict[str, str]] = Field(default_factory=list)

    @model_validator(mode="after")
    def expand_ignored_mapped_columns(self):
        """Treat either side of a mapping as one logical ignored column."""
        ignored = set(self.ignored_columns)
        changed = True
        while changed:
            changed = False
            for mapping in self.column_mappings:
                source_column = mapping.source_column
                target_column = mapping.target_column
                if source_column in ignored or target_column in ignored:
                    before = len(ignored)
                    ignored.update((source_column, target_column))
                    changed = changed or len(ignored) != before
        self.ignored_columns = sorted(ignored)

        for key in self.comparison_keys:
            if key.source_column in ignored or key.target_column in ignored:
                raise ValueError(
                    "Comparison keys cannot be ignored columns"
                )

        for pair in self.grouping_attributes:
            if pair.source_column in ignored or pair.target_column in ignored:
                raise ValueError(
                    "Grouping fields cannot be ignored columns"
                )

        for pair in self.aggregation_columns:
            if (
                pair.get("source_column") in ignored
                or pair.get("target_column") in ignored
            ):
                raise ValueError(
                    "Aggregation fields cannot be ignored columns"
                )
        return self

    @model_validator(mode="after")
    def validate_group_reconciliation(self):
        if self.matching_mode != "GROUP_RECONCILIATION":
            return self
        if not self.grouping_attributes or any(not item.source_column or not item.target_column for item in self.grouping_attributes):
            raise ValueError("Group reconciliation requires grouping field pairs")
        if not self.aggregation_columns or any(not item.get("source_column") or not item.get("target_column") for item in self.aggregation_columns):
            raise ValueError("Group reconciliation requires aggregation field pairs")
        return self

    strategy_policy: StrategyPolicy

class InputAnalysis(BaseModel):
    source_metadata: dict = Field(default_factory=dict)
    target_metadata: dict = Field(default_factory=dict)

    source_row_count: int | None = None
    target_row_count: int | None = None

    platform_capabilities: dict = Field(default_factory=dict)

    mappings: list[ColumnMapping] = Field(default_factory=list)

    dq_rules: list[DQRule] = Field(default_factory=list)

class LevelStrategy(BaseModel):
    comparison_level: ComparisonLevel
    comparison_mode: ExecutionMode
    execution_location: ExecutionLocation
    data_access_mode: DataAccessMode | None = None


class StrategyDecision(BaseModel):
    strategies: list[LevelStrategy] = Field(default_factory=list)
