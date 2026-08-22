from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    event,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
)


class Base(DeclarativeBase):
    """Base class for all persistence models."""

    pass


class ConfigurationModel(Base):
    """
    Persisted comparison configuration.
    """

    __tablename__ = "configurations"

    configuration_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    configuration: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


@event.listens_for(
    ConfigurationModel.configuration,
    "set",
    retval=True,
    active_history=True,
)
def preserve_configuration_lifecycle_metadata(
    target,
    value: Any,
    oldvalue: Any,
    initiator,
):
    """Keep UI lifecycle metadata when execution refreshes runtime JSON.

    Comparison execution intentionally persists the validated runtime
    configuration again before running.  That runtime model does not contain
    the UI-only ``_meta`` and ``_workspace`` keys.  Without this guard the
    second save replaces the JSONB document and silently destroys the saved
    comparison name and editable workspace snapshot.

    Explicit configuration API updates still win: if the incoming value
    contains either reserved key, that incoming value is kept unchanged.
    """
    if not isinstance(value, dict) or not isinstance(oldvalue, dict):
        return value

    merged = dict(value)

    for reserved_key in ("_meta", "_workspace"):
        if reserved_key not in merged and reserved_key in oldvalue:
            merged[reserved_key] = oldvalue[reserved_key]

    return merged


class ConnectionModel(Base):
    """
    Persisted connector connection.

    Stores connection configuration separately from
    comparison configurations.
    """

    __tablename__ = "connections"

    connection_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    name: Mapped[str] = mapped_column(
        String(150),
        nullable=False,
    )

    connector_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    properties: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default="UNKNOWN",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index(
            "ix_connections_connector_type",
            "connector_type",
        ),
        Index(
            "ix_connections_status",
            "status",
        ),
    )


class ExecutionPlanModel(Base):
    """
    Persisted planner decision.
    """

    __tablename__ = "execution_plans"

    plan_id: Mapped[str] = mapped_column(
        String(100),
        primary_key=True,
    )

    configuration_id: Mapped[int] = mapped_column(
        ForeignKey(
            "configurations.configuration_id"
        ),
        nullable=False,
    )

    planner_version: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    plan: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index(
            "ix_execution_plans_configuration_id",
            "configuration_id",
        ),
    )


class ComparisonRunModel(Base):
    """
    One complete comparison execution.
    """

    __tablename__ = "comparison_runs"

    run_id: Mapped[str] = mapped_column(
        String(100),
        primary_key=True,
    )

    plan_id: Mapped[str] = mapped_column(
        ForeignKey(
            "execution_plans.plan_id"
        ),
        nullable=False,
    )

    configuration_id: Mapped[int] = mapped_column(
        ForeignKey(
            "configurations.configuration_id"
        ),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
    )

    comparison_status: Mapped[str | None] = mapped_column(
        String(30),
        nullable=True,
    )

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index(
            "ix_comparison_runs_plan_id",
            "plan_id",
        ),
        Index(
            "ix_comparison_runs_configuration_id",
            "configuration_id",
        ),
    )


class L7AnalysisReportModel(Base):
    """Persisted optional L7 analysis for a completed comparison run."""

    __tablename__ = "l7_analysis_reports"

    run_id: Mapped[str] = mapped_column(
        ForeignKey("comparison_runs.run_id"),
        primary_key=True,
    )

    report: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )

    error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class TaskExecutionModel(Base):
    """
    Persisted execution state for one comparison task.
    """

    __tablename__ = "task_executions"

    task_execution_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    run_id: Mapped[str] = mapped_column(
        ForeignKey(
            "comparison_runs.run_id"
        ),
        nullable=False,
    )

    task_id: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    comparison_level: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    comparator_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    execution_mode: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
    )

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    __table_args__ = (
        Index(
            "ix_task_executions_run_id",
            "run_id",
        ),
        Index(
            "ix_task_executions_task_id",
            "task_id",
        ),
    )


class ComparisonResultModel(Base):
    """
    Persisted comparator metrics and evidence.
    """

    __tablename__ = "comparison_results"

    result_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    run_id: Mapped[str] = mapped_column(
        ForeignKey(
            "comparison_runs.run_id"
        ),
        nullable=False,
    )

    task_id: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    metrics: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    evidence: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index(
            "ix_comparison_results_run_id",
            "run_id",
        ),
        Index(
            "ix_comparison_results_task_id",
            "task_id",
        ),
    )


class ComparisonEvidenceItemModel(Base):
    """
    Permanent row-wise exception evidence.

    Detailed evidence lives here for pagination/filtering. The
    comparison_results table remains the summary/result envelope.
    """

    __tablename__ = "comparison_evidence_items"

    evidence_item_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    result_id: Mapped[int] = mapped_column(
        ForeignKey(
            "comparison_results.result_id"
        ),
        nullable=False,
    )

    run_id: Mapped[str] = mapped_column(
        ForeignKey(
            "comparison_runs.run_id"
        ),
        nullable=False,
    )

    task_id: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    comparison_level: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )

    evidence_type: Mapped[str] = mapped_column(
        String(80),
        nullable=False,
    )

    entity_key: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    source_field: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
    )

    target_field: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
    )

    ordinal: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index(
            "ix_comparison_evidence_run_task_type_ord",
            "run_id",
            "task_id",
            "evidence_type",
            "ordinal",
        ),
        Index(
            "ix_comparison_evidence_run_level_type",
            "run_id",
            "comparison_level",
            "evidence_type",
        ),
        Index(
            "ix_comparison_evidence_result_type_ord",
            "result_id",
            "evidence_type",
            "ordinal",
        ),
    )


class RuleModel(Base):
    """
    Persisted reusable rule (Aggregate or DQ).
    """

    __tablename__ = "rules"

    rule_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    name: Mapped[str] = mapped_column(
        String(150),
        nullable=False,
    )

    rule_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
