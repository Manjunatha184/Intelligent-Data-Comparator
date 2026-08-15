from typing import Any

from pydantic import BaseModel, Field

from app.domain.context import (
    DatasetConfiguration,
    ComparisonKey,
    ColumnMapping,
    AggregateRule,
    DQRule,
    StrategyPolicy,
)
from app.connectors.filters import RowFilter


class ComparisonRequest(BaseModel):
    """
    Complete comparison configuration accepted by the API.

    The API uses the same domain models as the planner so that
    configuration validation happens before execution.
    """

    configuration_id: int

    source: DatasetConfiguration

    target: DatasetConfiguration

    comparison_levels: list[str] = Field(min_length=1)

    l7_enabled: bool = False

    comparison_keys: list[ComparisonKey] = Field(
        default_factory=list
    )

    column_mappings: list[ColumnMapping] = Field(
        default_factory=list
    )

    ignored_columns: list[str] = Field(
        default_factory=list
    )

    aggregate_rules: list[AggregateRule] = Field(
        default_factory=list
    )

    dq_rules: list[DQRule] = Field(
        default_factory=list
    )

    source_filters: list[RowFilter] = Field(default_factory=list)
    target_filters: list[RowFilter] = Field(default_factory=list)

    matching_mode: str = "ROW_LEVEL"
    grouping_attributes: list[ComparisonKey] = Field(default_factory=list)
    aggregation_columns: list[dict[str, str]] = Field(default_factory=list)

    strategy_policy: StrategyPolicy


class ComparisonStartResponse(BaseModel):
    run_id: str
    plan_id: str
    status: str
    total_tasks: int
    task_ids: list[str]


class ComparisonStatusResponse(BaseModel):
    run_id: str
    plan_id: str
    status: str

    completed_tasks: list[str] = Field(
        default_factory=list
    )

    failed_tasks: list[str] = Field(
        default_factory=list
    )

    task_errors: dict[str, str] = Field(
        default_factory=dict
    )

    running_tasks: list[str] = Field(
        default_factory=list
    )

    waiting_tasks: list[str] = Field(
        default_factory=list
    )

    progress: float = 0.0


class ComparisonCancelResponse(BaseModel):
    run_id: str
    status: str

class ComparisonResultResponse(BaseModel):
    run_id: str
    plan_id: str
    status: str
    comparison_status: str

    datasets: dict[str, Any] = Field(
        default_factory=dict
    )

    levels: list[dict[str, Any]] = Field(
        default_factory=list
    )

    analysis: dict[str, Any] | None = None
