from enum import Enum
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, ConfigDict


# ============================================================
# ENUMS
# ============================================================

class ComparisonLevel(str, Enum):
    L1 = "L1"
    L2 = "L2"
    L3 = "L3"
    L4 = "L4"
    L5 = "L5"
    L6 = "L6"


class ExecutionMode(str, Enum):
    EXACT = "EXACT"
    HASH = "HASH"
    AGGREGATE = "AGGREGATE"
    SAMPLED = "SAMPLED"


class ExecutionStatus(str, Enum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


class Priority(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ExecutionLocation(str, Enum):
    SPARK = "SPARK"
    DUCKDB = "DUCKDB"


class ExecutionResultStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"


# ============================================================
# IMMUTABLE EXECUTION TASK
# ============================================================

class ExecutionTask(BaseModel):
    """
    Immutable task definition.

    Runtime information does NOT belong here.
    """

    model_config = ConfigDict(
        frozen=True
    )

    task_id: str

    comparison_level: ComparisonLevel

    comparator_name: str

    execution_mode: ExecutionMode

    priority: Priority

    dependencies: tuple[str, ...] = ()

    parallel_eligible: bool = True

    enabled: bool = True

    configuration: dict[str, Any] = Field(
        default_factory=dict
    )

    estimated_duration_ms: int | None = None


# ============================================================
# EXECUTION RESULT
# ============================================================

class ExecutionResult(BaseModel):
    """
    Result of one execution attempt.
    """

    task_id: str

    comparator_name: str

    status: ExecutionResultStatus

    attempt_number: int = 1

    started_at: datetime | None = None

    finished_at: datetime | None = None

    duration_ms: int | None = None

    execution_mode: ExecutionMode | None = None

    execution_location: ExecutionLocation | None = None

    metrics: dict[str, Any] = Field(
        default_factory=dict
    )

    evidence: dict[str, Any] = Field(
        default_factory=dict
    )

    runtime_context: dict[str, Any] = Field(
        default_factory=dict
    )

    error: str | None = None


# ============================================================
# RUNTIME TASK STATE
# ============================================================

class RuntimeTaskState(BaseModel):
    """
    Mutable state owned by the Execution Engine.
    """

    task_id: str

    status: ExecutionStatus = ExecutionStatus.PENDING

    attempt_count: int = 0

    max_attempts: int = 1

    started_at: datetime | None = None

    finished_at: datetime | None = None

    last_error: str | None = None

    result: ExecutionResult | None = None

    results: list[ExecutionResult] = Field(
        default_factory=list
    )


# ============================================================
# DEPENDENCY GRAPH
# ============================================================

class DependencyGraph(BaseModel):
    """
    Immutable dependency definition.

    task_id -> dependencies
    """

    model_config = ConfigDict(
        frozen=True
    )

    dependencies: dict[str, tuple[str, ...]] = Field(
        default_factory=dict
    )


# ============================================================
# EXECUTION GROUP
# ============================================================

class ExecutionGroup(BaseModel):
    """
    Group of tasks that are eligible to execute together.
    """

    model_config = ConfigDict(
        frozen=True
    )

    group_id: str

    tasks: tuple[ExecutionTask, ...]

    parallel: bool = True


# ============================================================
# EXECUTION METADATA
# ============================================================

class ExecutionMetadata(BaseModel):
    plan_id: str

    run_id: str

    configuration_id: int

    planner_version: str

    platform: str

    execution_strategy: ExecutionMode

    created_at: datetime


# ============================================================
# EXECUTION RULES
# ============================================================

class ExecutionRules(BaseModel):
    max_parallel_workers: int = 4

    retry_enabled: bool = True

    retry_count: int = 2

    task_timeout_seconds: int = 1800

    continue_on_error: bool = True

    fail_fast: bool = False

    cancellation_enabled: bool = True


# ============================================================
# EXECUTION ESTIMATE
# ============================================================

class ExecutionEstimate(BaseModel):
    total_tasks: int

    parallel_groups: int

    estimated_duration_seconds: int | None = None

    estimated_memory_mb: int | None = None

    estimated_compute_cost: str | None = None


# ============================================================
# IMMUTABLE EXECUTION PLAN
# ============================================================

class ExecutionPlan(BaseModel):
    """
    Immutable execution blueprint.

    The Execution Engine must never mutate this object.
    """

    model_config = ConfigDict(
        frozen=True
    )

    metadata: ExecutionMetadata

    tasks: tuple[ExecutionTask, ...]

    groups: tuple[ExecutionGroup, ...]

    rules: ExecutionRules

    estimate: ExecutionEstimate

    dependency_graph: DependencyGraph = Field(
        default_factory=DependencyGraph
    )


# ============================================================
# MUTABLE RUNTIME QUEUE
# ============================================================

class RuntimeQueue(BaseModel):
    """
    Mutable runtime state.
    """

    ready_tasks: list[ExecutionTask] = Field(
        default_factory=list
    )

    waiting_tasks: list[ExecutionTask] = Field(
        default_factory=list
    )

    running_tasks: list[ExecutionTask] = Field(
        default_factory=list
    )

    completed_tasks: list[ExecutionTask] = Field(
        default_factory=list
    )

    failed_tasks: list[ExecutionTask] = Field(
        default_factory=list
    )

    task_states: dict[str, RuntimeTaskState] = Field(
        default_factory=dict
    )


# ============================================================
# EXECUTION BATCH
# ============================================================

class ExecutionBatch(BaseModel):
    """
    A scheduling batch.

    parallel=True means the tasks are eligible to execute
    concurrently. Actual concurrency is implemented by
    the Phase-B worker pool.
    """

    batch_id: str

    tasks: tuple[ExecutionTask, ...]

    parallel: bool = True
