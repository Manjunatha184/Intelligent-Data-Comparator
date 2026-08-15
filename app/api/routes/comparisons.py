import json
import logging
from time import perf_counter

from app.persistence.repository import PostgresRepository
from app.persistence.config import get_database_url

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.api.schemas.comparison import (
    ComparisonRequest,
    ComparisonStartResponse,
    ComparisonStatusResponse,
    ComparisonCancelResponse,
    ComparisonResultResponse,
)

from io import BytesIO

from fastapi.responses import StreamingResponse
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

from app.domain.context import RuntimeConfiguration
from app.strategy.planner import StrategyPlanner

from app.execution.engine import ExecutionEngine
from app.execution.dispatcher import ExecutionDispatcher
from app.execution.collector import ExecutionCollector
from app.execution.models import ExecutionPlan

from app.connectors.manager import ConnectorManager
from app.connectors.csv import CSVMetadataProvider
from app.connectors.databricks import DatabricksConnector

from app.comparators.registry import ComparatorRegistry
from app.analysis.engine import L7AnalysisEngine


logger = logging.getLogger(__name__)


# ============================================================
# ROUTER
# ============================================================

router = APIRouter(
    prefix="/comparisons",
    tags=["Comparisons"],
)

MAX_EVIDENCE_PAGE_SIZE = 200
VALID_COMPARISON_LEVELS = {
    "L1",
    "L2",
    "L3",
    "L4",
    "L5",
    "L6",
}


# ============================================================
# CONNECTOR REGISTRY
# ============================================================

csv_provider = CSVMetadataProvider()
databricks_provider = DatabricksConnector()

connector_manager = ConnectorManager()

# ------------------------------------------------------------
# CSV
# ------------------------------------------------------------

connector_manager.register(
    "csv",
    csv_provider,
)

connector_manager.register_data_provider(
    "csv",
    csv_provider,
)

connector_manager.register_connection_provider(
    "csv",
    csv_provider,
)

# ------------------------------------------------------------
# DATABRICKS
# ------------------------------------------------------------

connector_manager.register(
    "databricks",
    databricks_provider,
)

connector_manager.register_data_provider(
    "databricks",
    databricks_provider,
)

connector_manager.register_connection_provider(
    "databricks",
    databricks_provider,
)

# ============================================================
# COMPARATOR REGISTRY
# ============================================================

comparator_registry = ComparatorRegistry(
    schema_providers={
        "csv": csv_provider,
        "databricks": databricks_provider,
    }
)


# ============================================================
# COMPARATOR REGISTRY
# ============================================================

comparator_registry = ComparatorRegistry(
    schema_providers={
        "csv": csv_provider,
    }
)


# ============================================================
# IN-MEMORY RUN REGISTRY
# ============================================================

RUNS: dict[str, dict[str, Any]] = {}

# ============================================================
# PERSISTENCE
# ============================================================

persistence_repository = PostgresRepository(
    get_database_url()
)

persistence_repository.create_tables()


# ============================================================
# EXECUTION-ONLY CONNECTION HYDRATION
# ============================================================

_MASKED_SECRET_VALUES = {
    "********",
    "[REDACTED]",
    "REDACTED",
}


def _is_masked_secret(value: Any) -> bool:
    return (
        isinstance(value, str)
        and value.strip() in _MASKED_SECRET_VALUES
    )


def _hydrate_dataset_for_execution(
    dataset: dict[str, Any],
) -> dict[str, Any]:
    """
    Restore server-side Databricks credentials using connection_id.

    The browser correctly receives masked secrets from GET /connections.
    Those masked values must never be used for SQL execution.  Hydration is
    performed only on the in-memory execution plan, after the persisted plan
    has already been saved, so real credentials are not written into the
    persisted comparison configuration/plan.
    """
    resolved_dataset = dict(dataset or {})

    connector_type = str(
        resolved_dataset.get("connector_type") or ""
    ).strip().lower()

    if connector_type != "databricks":
        return resolved_dataset

    client_properties = dict(
        resolved_dataset.get("properties") or {}
    )

    connection_id = (
        resolved_dataset.get("connection_id")
        or client_properties.get("connection_id")
    )

    saved = None

    if connection_id is not None:
        saved = persistence_repository.get_connection(
            int(connection_id)
        )

        if saved is None:
            raise ValueError(
                f"Databricks connection not found: {connection_id}"
            )

    else:
        # Older/stale frontend bundles may submit the Databricks dataset
        # without connection_id.  Resolve the saved connection safely using
        # non-secret connection identity fields rather than trusting a masked
        # access token from the browser.
        nested = client_properties.get("connection")
        nested = nested if isinstance(nested, dict) else {}

        request_host = (
            client_properties.get("server_hostname")
            or nested.get("server_hostname")
        )
        request_http_path = (
            client_properties.get("http_path")
            or nested.get("http_path")
        )

        candidates = []

        for connection in persistence_repository.get_connections():
            if (
                str(connection.get("connector_type") or "")
                .strip()
                .lower()
                != "databricks"
            ):
                continue

            properties = connection.get("properties") or {}

            if (
                request_host
                and request_http_path
                and properties.get("server_hostname") == request_host
                and properties.get("http_path") == request_http_path
            ):
                candidates.append(connection)

        if len(candidates) == 1:
            saved = candidates[0]
            connection_id = saved.get("connection_id")
            logger.info(
                "Resolved Databricks comparison connection without "
                "connection_id using server_hostname/http_path: %s",
                connection_id,
            )

        elif len(candidates) > 1:
            raise ValueError(
                "Multiple saved Databricks connections match this "
                "server hostname and HTTP path; connection_id is required"
            )

        else:
            raise ValueError(
                "Unable to resolve the saved Databricks connection. "
                "Re-select the Databricks source/target connection and retry."
            )

    if str(saved.get("connector_type") or "").strip().lower() != "databricks":
        raise ValueError(
            f"Connection {connection_id} is not a Databricks connection"
        )

    saved_properties = dict(
        saved.get("properties") or {}
    )

    # Start with authoritative server-side properties, which contain the real
    # credentials.  Only non-secret dataset-specific values from the request
    # are allowed to override them.
    hydrated_properties = dict(saved_properties)

    for key, value in client_properties.items():
        lowered = str(key).lower()

        if lowered in {
            "access_token",
            "token",
            "password",
            "secret",
            "api_key",
            "client_secret",
        }:
            # Never let a browser-provided secret (masked or otherwise)
            # replace the saved server-side credential.
            continue

        if lowered == "connection":
            # The frontend may echo a nested masked connection object.
            # Ignore it; top-level saved credentials are authoritative.
            continue

        hydrated_properties[key] = value

    hydrated_properties["connection_id"] = int(
        connection_id
    )

    resolved_dataset["connection_id"] = int(
        connection_id
    )
    resolved_dataset["properties"] = hydrated_properties

    logger.info(
        "Hydrated Databricks dataset for execution "
        "connection_id=%s catalog=%s schema=%s table=%s "
        "credential_present=%s",
        connection_id,
        hydrated_properties.get("catalog"),
        hydrated_properties.get("schema"),
        hydrated_properties.get("table"),
        bool(hydrated_properties.get("access_token"))
        and not _is_masked_secret(
            hydrated_properties.get("access_token")
        ),
    )

    return resolved_dataset


def _hydrate_plan_connections_for_execution(plan) -> None:
    """
    Hydrate source/target datasets on execution tasks only.

    IMPORTANT: call this only after save_execution_plan(), otherwise real
    credentials could be persisted as part of task.configuration.
    """
    for task in plan.tasks:
        configuration = task.configuration

        if not isinstance(configuration, dict):
            continue

        for side in ("source", "target"):
            dataset = configuration.get(side)

            if isinstance(dataset, dict):
                configuration[side] = (
                    _hydrate_dataset_for_execution(
                        dataset
                    )
                )


# L7 is a post-L1-L6 analysis stage. It is intentionally not part
# of the execution plan and never performs matching/comparison.
l7_analysis_engine = L7AnalysisEngine()


# ============================================================
# BUILD RUNTIME CONFIGURATION
# ============================================================

def build_runtime_configuration(
    request: ComparisonRequest,
) -> RuntimeConfiguration:

    # Dump the validated API models explicitly in Python mode.  In
    # particular, keep the complete ColumnMapping object (normalization,
    # tolerance/tolerance_pct and comparison_type) intact all the way to the
    # planner/Spark task configuration.
    payload = request.model_dump(mode="python")
    payload["column_mappings"] = [
        mapping.model_dump(mode="python", exclude_none=True)
        for mapping in request.column_mappings
    ]

    runtime = RuntimeConfiguration.model_validate(payload)
    return runtime


# ============================================================
# BUILD EXECUTION PLAN
# ============================================================

def build_execution_plan(
    configuration: RuntimeConfiguration,
):

    planner = StrategyPlanner()

    analysis = planner.analyze_inputs(
        configuration
    )

    strategy = planner.choose_strategy(
        analysis,
        configuration.comparison_levels,
        configuration.strategy_policy,
    )

    tasks = planner.build_execution_tasks(
        configuration,
        strategy,
    )

    groups = planner.build_execution_groups(
        tasks
    )

    return planner.build_execution_plan(
        configuration,
        tasks,
        groups,
    )


# ============================================================
# EXECUTE PLAN
# ============================================================

def execute_plan(
    run_id: str,
    plan,
):
    run_started = perf_counter()
    # --------------------------------------------------------
    # Persist configuration BEFORE execution plan/run
    # --------------------------------------------------------

    configuration = RUNS[run_id]["configuration"]

    persistence_repository.save_configuration(
        configuration_id=(
            configuration.configuration_id
        ),
        configuration=configuration.model_dump(
            mode="python"
        ),
    )

    # --------------------------------------------------------
    # Persist execution plan BEFORE creating run
    # --------------------------------------------------------

    persistence_repository.save_execution_plan(
        plan_id=plan.metadata.plan_id,
        configuration_id=(
            plan.metadata.configuration_id
        ),
        planner_version=(
            plan.metadata.planner_version
        ),
        plan=plan.model_dump(
            mode="json"
        ),
    )

    # --------------------------------------------------------
    # Restore Databricks secrets for EXECUTION ONLY.
    #
    # This intentionally happens after the persisted plan is saved so masked
    # browser values remain in persisted configuration while Spark receives
    # the real server-side credential from connection_id.
    # --------------------------------------------------------

    _hydrate_plan_connections_for_execution(
        plan
    )

    # --------------------------------------------------------
    # Create execution engine
    # --------------------------------------------------------

    engine = ExecutionEngine(
        persistence_repository=(
            persistence_repository
        )
    )

    dispatcher = ExecutionDispatcher(
        comparator_registry=comparator_registry,
        connector_manager=connector_manager,
    )

    collector = ExecutionCollector()

    # --------------------------------------------------------
    # Initialize execution
    #
    # At this point:
    #
    # configuration EXISTS
    # execution_plan EXISTS
    #
    # therefore create_run() can safely execute.
    # --------------------------------------------------------

    runtime_queue = engine.initialize(
        plan
    )

    RUNS[run_id]["engine"] = engine
    RUNS[run_id]["plan"] = plan

    # --------------------------------------------------------
    # Initial scheduling
    # --------------------------------------------------------

    engine.schedule_ready_tasks()

    # --------------------------------------------------------
    # Execute all scheduled work
    # --------------------------------------------------------

    execution_started = perf_counter()
    engine.execute_remaining_batches(
        dispatcher=dispatcher,
        collector=collector,
    )
    logger.info("COMPARISON_TIMING run_id=%s execution_and_persistence_ms=%.1f", run_id, (perf_counter() - execution_started) * 1000)

    # --------------------------------------------------------
    # Finalize
    # --------------------------------------------------------

    result = engine.finalize_execution()

    RUNS[run_id]["result"] = result

    RUNS[run_id]["runtime_queue"] = (
        engine.runtime_queue
    )

    # ========================================================
    # L7 — OPTIONAL AI ANALYSIS
    #
    # Build the input from authoritative L1-L6 ExecutionResult
    # objects. L7EvidenceBuilder removes raw records, keys,
    # matched_pairs and raw field values before anything reaches
    # Groq Cloud.
    # ========================================================

    if configuration.l7_enabled:
        level_results: dict[str, Any] = {}

        for task in plan.tasks:
            state = engine.runtime_queue.task_states.get(task.task_id)
            if state is None or state.result is None:
                continue
            level_results[task.comparison_level.value] = state.result.model_dump(mode="json")

        l7_started = perf_counter()
        try:
            l7_report = l7_analysis_engine.analyze(
                run_id=run_id,
                level_results=level_results,
            )
            report_payload = l7_report.model_dump(mode="json")
            RUNS[run_id]["l7_report"] = l7_report
            RUNS[run_id]["l7_error"] = None
            persistence_repository.save_l7_analysis_report(run_id, report_payload)
        except Exception as exc:
            logger.exception("L7 analysis failed for run %s", run_id)
            RUNS[run_id]["l7_report"] = None
            RUNS[run_id]["l7_error"] = str(exc)
            persistence_repository.save_l7_analysis_report(
                run_id,
                None,
                error=str(exc),
            )

        logger.info("COMPARISON_TIMING run_id=%s l7_ms=%.1f", run_id, (perf_counter() - l7_started) * 1000)
    else:
        RUNS[run_id]["l7_report"] = None
        RUNS[run_id]["l7_error"] = None
    logger.info("COMPARISON_TIMING run_id=%s total_ms=%.1f", run_id, (perf_counter() - run_started) * 1000)
    print(f"COMPARISON_TIMING run_id={run_id} total_ms={(perf_counter() - run_started) * 1000:.1f}")

    return result


# ============================================================
# LIMIT RESULT EVIDENCE
# ============================================================

def _limit_result_evidence(
    result: dict[str, Any],
    limit: int = 50,
) -> dict[str, Any]:
    """
    Create an API-safe representation of a comparison result.

    Metrics remain complete.

    Large evidence collections are represented
    by their total count and a bounded sample.
    """

    result = dict(result)

    evidence = result.get("evidence")

    if not isinstance(evidence, dict):
        return result

    bounded_evidence = {}

    for key, value in evidence.items():

        if isinstance(value, list):

            bounded_evidence[key] = {
                "count": len(value),
                "sample": value[:limit],
                "truncated": len(value) > limit,
            }

        else:
            bounded_evidence[key] = value

    result["evidence"] = bounded_evidence

    return result


# ============================================================
# BUILD DIFFERENCE VIEW
# ============================================================

def _build_difference_view(
    evidence: dict[str, Any],
    limit: int | None = None,
) -> dict[str, Any]:

    differences: dict[str, Any] = {}

    if not isinstance(evidence, dict):
        return differences

    for key, value in evidence.items():

        # ----------------------------------------------------
        # Comparison keys are configuration information,
        # not actual differences.
        # ----------------------------------------------------

        if key == "comparison_keys":
            continue

        # ----------------------------------------------------
        # Standard bounded evidence
        # ----------------------------------------------------

        if isinstance(value, dict):

            if "sample" in value:

                sample = value.get(
                    "sample",
                    [],
                )

                count = value.get(
                    "count",
                    len(sample),
                )

                if count > 0:

                    items = (
                        sample
                        if limit is None
                        else sample[:limit]
                    )

                    truncated = (
                        value.get(
                            "truncated",
                            False,
                        )
                        if limit is None
                        else (
                            value.get(
                                "truncated",
                                False,
                            )
                            or count > limit
                        )
                    )

                    differences[key] = {
                        "count": count,
                        "items": items,
                        "truncated": truncated,
                    }

                continue

            # ------------------------------------------------
            # L2 checks
            # ------------------------------------------------

            if key == "checks":

                checks = []

                for check_name, check in value.items():

                    if not isinstance(
                        check,
                        dict,
                    ):
                        continue

                    checks.append(
                        {
                            "check": check_name,
                            **check,
                        }
                    )

                if checks:

                    differences["checks"] = (
                        checks
                        if limit is None
                        else checks[:limit]
                    )

                continue

            if key == "group_reconciliation" and isinstance(value, list):
                differences[key] = {
                    "count": len(value),
                    "items": value if limit is None else value[:limit],
                    "truncated": limit is not None and len(value) > limit,
                }
                continue

            # ------------------------------------------------
            # Other nested evidence dictionaries
            # ------------------------------------------------

            differences[key] = value

            continue

        # ----------------------------------------------------
        # Raw list evidence
        # ----------------------------------------------------

        if isinstance(value, list):

            if value:

                items = (
                    value
                    if limit is None
                    else value[:limit]
                )

                differences[key] = {
                    "count": len(value),
                    "items": items,
                    "truncated": (
                        False
                        if limit is None
                        else len(value) > limit
                    ),
                }

            continue

    return differences


# ============================================================
# LEVEL DISPLAY NAMES
# ============================================================

LEVEL_NAMES = {
    "L1": "Schema",
    "L2": "Volume",
    "L3": "Record",
    "L4": "Field",
    "L5": "Aggregate",
    "L6": "Data Quality",
}


# ============================================================
# BUILD LEVEL RESULT
# ============================================================

def _build_level_result(
    task,
    raw_result: dict[str, Any],
    limit: int | None = None,
) -> dict[str, Any]:

    # Some execution adapters wrap comparator output under `result`.
    # Normalize that wrapper before exposing the public level contract.
    if (
        not raw_result.get("metrics")
        and isinstance(raw_result.get("result"), dict)
        and isinstance(raw_result["result"].get("metrics"), dict)
    ):
        raw_result = raw_result["result"]

    comparison_level = task.comparison_level

    if hasattr(
        comparison_level,
        "value",
    ):
        level = comparison_level.value
    else:
        level = str(
            comparison_level
        )

    metrics = raw_result.get(
        "metrics",
        {},
    )

    evidence = raw_result.get(
        "evidence",
        {},
    )

    return {
        "level": level,

        "name": LEVEL_NAMES.get(
            level,
            level,
        ),

        "status": (
            raw_result.get("status")
            if raw_result.get("status") in {"FAILED", "SKIPPED", "CANCELLED"}
            else metrics.get("status", "PASS")
        ),

        "metrics": metrics,

        "differences": _build_difference_view(
            evidence,
            limit=limit,
        ),
    }


def _state_status_value(state) -> str:
    status = getattr(state, "status", None)
    if hasattr(status, "value"):
        status = status.value
    return str(status or "").upper()


def _terminal_level_without_result(
    task,
    status: str,
    reason: str | None = None,
) -> dict[str, Any]:
    level = task.comparison_level
    if hasattr(level, "value"):
        level = level.value
    else:
        level = str(level)

    if not reason:
        if level == "L4" and status == "SKIPPED":
            reason = (
                "L4 requires successful L3 record reconciliation; "
                "L3 did not complete successfully."
            )
        elif status == "SKIPPED":
            reason = (
                "Task was skipped because an upstream dependency "
                "did not complete successfully."
            )
        elif status == "CANCELLED":
            reason = "Task was cancelled before producing a result."
        else:
            reason = "Task failed before producing comparison evidence."

    return {
        "level": level,
        "name": LEVEL_NAMES.get(level, level),
        "status": status,
        "metrics": {
            "status": status,
            "reason": reason,
        },
        "differences": {},
    }


# ============================================================
# CREATE COMPARISON
# ============================================================

@router.post(
    "",
    response_model=ComparisonStartResponse,
)
def create_comparison(
    request: ComparisonRequest,
):

    try:

        print("COMPARISON_REQUEST_SUMMARY=", {
            "comparison_keys": [key.model_dump(mode="python") for key in request.comparison_keys],
            "group_reconciliation_enabled": request.matching_mode == "GROUP_RECONCILIATION",
            "grouping_mappings": [item.model_dump(mode="python") for item in request.grouping_attributes],
            "aggregation_mappings": request.aggregation_columns,
            "column_mappings": [mapping.model_dump(mode="python", exclude_none=True) for mapping in request.column_mappings],
            "dq_rule_count": len(request.dq_rules),
            "dq_rule_types": [rule.rule_type for rule in request.dq_rules],
            "aggregate_rule_count": len(request.aggregate_rules),
        })

        configuration = (
            build_runtime_configuration(
                request
            )
        )

        plan = build_execution_plan(
            configuration
        )

        run_id = plan.metadata.run_id

        RUNS[run_id] = {
            "plan": plan,
            "configuration": configuration,
            "engine": None,
            "runtime_queue": None,
            "result": None,
        }

        execute_plan(
            run_id,
            plan,
        )

        runtime_queue = (
            RUNS[run_id]["runtime_queue"]
        )

        completed = (
            runtime_queue.completed_tasks
            if runtime_queue
            else []
        )

        failed = (
            runtime_queue.failed_tasks
            if runtime_queue
            else []
        )

        status = (
            "FAILED"
            if failed
            else "COMPLETED"
        )

        return ComparisonStartResponse(
            run_id=run_id,
            plan_id=plan.metadata.plan_id,
            status=status,
            total_tasks=len(plan.tasks),
            task_ids=[
                task.task_id
                for task in plan.tasks
            ],
        )

    except Exception as exc:

        print(f"COMPARISON_CREATE_400 {type(exc).__name__}: {exc}")

        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc


# ============================================================
# GET COMPARISON STATUS
# ============================================================

@router.get(
    "/{run_id}",
    response_model=ComparisonStatusResponse,
)
def get_comparison_status(
    run_id: str,
):

    run = RUNS.get(run_id)

    if run is None:

        raise HTTPException(
            status_code=404,
            detail=f"Run not found: {run_id}",
        )

    plan = run["plan"]
    runtime_queue = run["runtime_queue"]

    # --------------------------------------------------------
    # Execution has not started yet
    # --------------------------------------------------------

    if runtime_queue is None:

        return ComparisonStatusResponse(
            run_id=run_id,
            plan_id=plan.metadata.plan_id,
            status="PENDING",
        )

    # --------------------------------------------------------
    # Runtime task collections
    # --------------------------------------------------------

    completed = (
        runtime_queue.completed_tasks
    )

    failed = (
        runtime_queue.failed_tasks
    )

    running = (
        runtime_queue.running_tasks
    )

    waiting = (
        runtime_queue.waiting_tasks
    )

    # --------------------------------------------------------
    # Task errors
    # --------------------------------------------------------

    task_errors = {
        task.task_id: (
            runtime_queue.task_states[
                task.task_id
            ].last_error
            or "Unknown execution error"
        )
        for task in failed
        if task.task_id
        in runtime_queue.task_states
    }

    # --------------------------------------------------------
    # Progress
    # --------------------------------------------------------

    total_tasks = len(plan.tasks)

    finished_tasks = (
        len(completed)
        + len(failed)
    )

    progress = (
        (
            finished_tasks
            / total_tasks
        )
        * 100
        if total_tasks
        else 100
    )

    # --------------------------------------------------------
    # Overall status
    # --------------------------------------------------------

    status = (
        "FAILED"
        if failed
        else (
            "COMPLETED"
            if finished_tasks == total_tasks
            else "RUNNING"
        )
    )

    # --------------------------------------------------------
    # Response
    # --------------------------------------------------------

    return ComparisonStatusResponse(
        run_id=run_id,
        plan_id=plan.metadata.plan_id,
        status=status,

        completed_tasks=[
            task.task_id
            for task in completed
        ],

        failed_tasks=[
            task.task_id
            for task in failed
        ],

        task_errors=task_errors,

        running_tasks=[
            task.task_id
            for task in running
        ],

        waiting_tasks=[
            task.task_id
            for task in waiting
        ],

        progress=progress,
    )


# ============================================================
# CANCEL COMPARISON
# ============================================================

@router.post(
    "/{run_id}/cancel",
    response_model=ComparisonCancelResponse,
)
def cancel_comparison(
    run_id: str,
):

    run = RUNS.get(run_id)

    if run is None:

        raise HTTPException(
            status_code=404,
            detail=f"Run not found: {run_id}",
        )

    engine = run.get("engine")

    if engine is None:

        raise HTTPException(
            status_code=400,
            detail="Execution has not started.",
        )

    engine.request_cancel()

    return ComparisonCancelResponse(
        run_id=run_id,
        status="CANCEL_REQUESTED",
    )


# ============================================================
# GET COMPARISON RESULTS
# ============================================================

@router.get(
    "/{run_id}/results",
    response_model=ComparisonResultResponse,
)
def get_comparison_results(
    run_id: str,
):

    run = RUNS.get(run_id)

    if run is None:
        persisted_run = persistence_repository.get_run(run_id)

        if persisted_run is None:
            raise HTTPException(
                status_code=404,
                detail=f"Run not found: {run_id}",
            )

        persisted_plan = persisted_run.get("plan")
        if not persisted_plan:
            raise HTTPException(
                status_code=409,
                detail="Comparison plan is not available.",
            )

        plan = ExecutionPlan.model_validate(persisted_plan)
        persisted_tasks = {
            task["task_id"]: task
            for task in persisted_run.get("tasks", [])
        }
        persisted_results = {
            result["task_id"]: result
            for result in persisted_run.get("results", [])
        }

        levels = []
        for task in plan.tasks:
            result = persisted_results.get(task.task_id)
            task_state = persisted_tasks.get(task.task_id, {})
            task_status = str(
                task_state.get("status") or ""
            ).upper()

            if result is None:
                if task_status in {
                    "FAILED",
                    "SKIPPED",
                    "CANCELLED",
                }:
                    levels.append(
                        _terminal_level_without_result(
                            task=task,
                            status=task_status,
                            reason=task_state.get("last_error"),
                        )
                    )
                continue

            raw_result = {
                "status": (
                    task_status
                    if task_status in {
                        "FAILED",
                        "SKIPPED",
                        "CANCELLED",
                    }
                    else None
                ),
                "metrics": result.get("metrics") or {},
                "evidence": result.get("evidence") or {},
            }
            level_result = _build_level_result(task=task, raw_result=raw_result, limit=None)
            levels.append(level_result)

        if not levels and persisted_run.get("status") in {
            "RUNNING",
            "INCOMPLETE",
        }:
            raise HTTPException(
                status_code=409,
                detail="Comparison has not completed execution.",
            )

        configuration_record = persistence_repository.get_configuration(
            persisted_run["configuration_id"]
        ) or {}
        configuration_data = (
            configuration_record.get("configuration") or {}
        )
        source_config = configuration_data.get("source") or {}
        target_config = configuration_data.get("target") or {}
        source_properties = source_config.get("properties") or {}
        target_properties = target_config.get("properties") or {}

        comparison_status = persisted_run.get("comparison_status")
        if (
            comparison_status is None
            or any(
                level.get("status") in {
                    "FAIL",
                    "FAILED",
                    "SKIPPED",
                    "CANCELLED",
                }
                for level in levels
            )
        ):
            comparison_status = (
                "FAIL"
                if any(
                    level.get("status") in {
                        "FAIL",
                        "FAILED",
                        "SKIPPED",
                        "CANCELLED",
                    }
                    for level in levels
                )
                else "PASS"
            )

        return ComparisonResultResponse(
            run_id=run_id,
            plan_id=persisted_run["plan_id"],
            status=persisted_run.get("status") or "COMPLETED",
            comparison_status=comparison_status,
            datasets={
                "source": {
                    "type": source_config.get("connector_type"),
                    "path": source_properties.get("path"),
                    "records": 0,
                },
                "target": {
                    "type": target_config.get("connector_type"),
                    "path": target_properties.get("path"),
                    "records": 0,
                },
            },
            levels=levels,
            analysis=persisted_run.get("analysis"),
        )

    plan = run["plan"]

    configuration = run.get(
        "configuration"
    )

    runtime_queue = run.get(
        "runtime_queue"
    )

    if runtime_queue is None:

        raise HTTPException(
            status_code=400,
            detail="Comparison has not completed execution.",
        )

    # ========================================================
    # BUILD PRESENTATION LEVELS
    # ========================================================

    levels: list[dict[str, Any]] = []

    for task in plan.tasks:

        state = runtime_queue.task_states.get(
            task.task_id
        )

        if state is None:
            continue

        if state.result is None:
            task_status = _state_status_value(
                state
            )

            if task_status in {
                "FAILED",
                "SKIPPED",
                "CANCELLED",
            }:
                levels.append(
                    _terminal_level_without_result(
                        task=task,
                        status=task_status,
                        reason=getattr(
                            state,
                            "last_error",
                            None,
                        ),
                    )
                )

            continue

        raw_result = state.result.model_dump(
            mode="json"
        )

        level_result = _build_level_result(
            task=task,
            raw_result=raw_result,
            limit=None,
        )

        levels.append(
            level_result
        )

    # ========================================================
    # DATASET INFORMATION
    # ========================================================

    configuration_data = {}

    if configuration is not None:

        if hasattr(
            configuration,
            "model_dump",
        ):
            configuration_data = (
                configuration.model_dump(
                    mode="json"
                )
            )
        elif isinstance(
            configuration,
            dict,
        ):
            configuration_data = (
                configuration
            )

    source_config = (
        configuration_data.get(
            "source",
            {},
        )
        or {}
    )

    target_config = (
        configuration_data.get(
            "target",
            {},
        )
        or {}
    )

    source_properties = (
        source_config.get(
            "properties",
            {},
        )
        or {}
    )

    target_properties = (
        target_config.get(
            "properties",
            {},
        )
        or {}
    )

    source_records = 0
    target_records = 0

    # ========================================================
    # GET RECORD COUNTS FROM RUNTIME CONTEXT
    # ========================================================

    for task in plan.tasks:

        state = runtime_queue.task_states.get(
            task.task_id
        )

        if state is None:
            continue

        if state.result is None:
            continue

        runtime_context = getattr(
            state.result,
            "runtime_context",
            {},
        ) or {}

        source_records = max(
            source_records,
            runtime_context.get(
                "source_records_loaded",
                0,
            ),
        )

        target_records = max(
            target_records,
            runtime_context.get(
                "target_records_loaded",
                0,
            ),
        )

    datasets = {
        "source": {
            "type": source_config.get(
                "connector_type"
            ),

            "path": source_properties.get(
                "path"
            ),

            "records": source_records,
        },

        "target": {
            "type": target_config.get(
                "connector_type"
            ),

            "path": target_properties.get(
                "path"
            ),

            "records": target_records,
        },
    }

    # ========================================================
    # EXECUTION STATUS
    # ========================================================

    failed_tasks = (
        runtime_queue.failed_tasks
    )

    status = (
        "FAILED"
        if failed_tasks
        else "COMPLETED"
    )

    # ========================================================
    # COMPARISON STATUS
    # ========================================================

    comparison_status = "PASS"

    for level in levels:

        if level.get("status") in {
            "FAIL",
            "FAILED",
            "SKIPPED",
            "CANCELLED",
        }:

            comparison_status = "FAIL"

            break

    # ========================================================
    # RESPONSE
    # ========================================================

    return ComparisonResultResponse(
        run_id=run_id,

        plan_id=plan.metadata.plan_id,

        status=status,

        comparison_status=comparison_status,

        datasets=datasets,

        levels=levels,

        analysis=(
            run.get("l7_report").model_dump(mode="json")
            if run.get("l7_report") is not None
            else {"error": run.get("l7_error")} if run.get("l7_error") else None
        ),
    )


@router.get(
    "/{run_id}/evidence/{comparison_level}",
)
def get_comparison_evidence(
    run_id: str,
    comparison_level: str,
    page: int = Query(
        1,
        ge=1,
    ),
    page_size: int = Query(
        50,
        ge=1,
        le=MAX_EVIDENCE_PAGE_SIZE,
    ),
    evidence_type: str | None = None,
    source_field: str | None = None,
    target_field: str | None = None,
    task_id: str | None = None,
):

    comparison_level = comparison_level.upper()

    if page < 1:
        raise HTTPException(
            status_code=400,
            detail="page must be greater than or equal to 1",
        )

    if page_size < 1:
        raise HTTPException(
            status_code=400,
            detail=(
                "page_size must be greater than or equal "
                "to 1"
            ),
        )

    if page_size > MAX_EVIDENCE_PAGE_SIZE:
        raise HTTPException(
            status_code=400,
            detail=(
                "page_size must be less than or equal to "
                f"{MAX_EVIDENCE_PAGE_SIZE}"
            ),
        )

    if comparison_level not in VALID_COMPARISON_LEVELS:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid comparison_level: "
                f"{comparison_level}"
            ),
        )

    try:

        if not persistence_repository.run_exists(
            run_id
        ):
            raise HTTPException(
                status_code=404,
                detail=f"Run not found: {run_id}",
            )

        result = (
            persistence_repository
            .get_latest_successful_result_for_level(
                run_id=run_id,
                comparison_level=comparison_level,
                task_id=task_id,
            )
        )

        if result is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No successful persisted result found "
                    "for requested comparison evidence."
                ),
            )

        result_id = int(
            result["result_id"]
        )

        resolved_task_id = result[
            "task_id"
        ]

        offset = (
            page - 1
        ) * page_size

        filters = {
            "evidence_type": evidence_type,
            "source_field": source_field,
            "target_field": target_field,
        }

        total_count = (
            persistence_repository
            .count_evidence_items(
                run_id=run_id,
                result_id=result_id,
                task_id=resolved_task_id,
                comparison_level=comparison_level,
                evidence_type=evidence_type,
                source_field=source_field,
                target_field=target_field,
            )
        )

        items = (
            persistence_repository
            .list_evidence_items(
                run_id=run_id,
                result_id=result_id,
                task_id=resolved_task_id,
                comparison_level=comparison_level,
                evidence_type=evidence_type,
                source_field=source_field,
                target_field=target_field,
                limit=page_size,
                offset=offset,
            )
        )

        return {
            "run_id": run_id,
            "comparison_level": comparison_level,
            "result_id": result_id,
            "task_id": resolved_task_id,
            "page": page,
            "page_size": page_size,
            "returned_count": len(items),
            "total_count": total_count,
            "has_next": (
                offset + len(items)
                < total_count
            ),
            "filters": {
                key: value
                for key, value in filters.items()
                if value is not None
            },
            "items": [
                {
                    "evidence_item_id": item.get(
                        "evidence_item_id"
                    ),
                    "evidence_type": item.get(
                        "evidence_type"
                    ),
                    "entity_key": item.get(
                        "entity_key"
                    ),
                    "source_field": item.get(
                        "source_field"
                    ),
                    "target_field": item.get(
                        "target_field"
                    ),
                    "ordinal": item.get(
                        "ordinal"
                    ),
                    "payload": item.get(
                        "payload"
                    ),
                    "created_at": item.get(
                        "created_at"
                    ),
                }
                for item in items
            ],
        }

    except HTTPException:
        raise

    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve comparison evidence.",
        ) from exc


@router.get("/{run_id}/analysis/pdf")
def download_analysis_pdf(run_id: str):
    run = RUNS.get(run_id)
    report = run.get("l7_report") if run else persistence_repository.get_l7_analysis_report(run_id)

    if hasattr(report, "model_dump"):
        report = report.model_dump()

    if not report or report.get("error"):
        raise HTTPException(
            status_code=404,
            detail="L7 analysis report is not available",
        )

    buffer = BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=18 * mm,
        leftMargin=18 * mm,
        topMargin=18 * mm,
        bottomMargin=18 * mm,
        title="Comparison Analysis Report",
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontSize=20,
        leading=24,
        alignment=TA_CENTER,
        spaceAfter=10,
    )

    heading_style = ParagraphStyle(
        "ReportHeading",
        parent=styles["Heading2"],
        fontSize=13,
        leading=16,
        spaceBefore=12,
        spaceAfter=6,
    )

    body_style = ParagraphStyle(
        "ReportBody",
        parent=styles["BodyText"],
        fontSize=9,
        leading=13,
        spaceAfter=5,
    )

    small_style = ParagraphStyle(
        "ReportSmall",
        parent=styles["BodyText"],
        fontSize=8,
        leading=11,
    )

    label_style = ParagraphStyle(
        "ReportLabel",
        parent=small_style,
        textColor=colors.HexColor("#667085"),
        fontSize=7,
        leading=9,
    )

    story = []

    # --------------------------------------------------
    # HEADER
    # --------------------------------------------------

    story.append(
        Paragraph(
            "Data Comparison Analysis",
            title_style,
        )
    )

    header_table = Table(
        [
            [Paragraph("RUN ID", label_style), Paragraph("OVERALL STATUS", label_style), Paragraph("SEVERITY", label_style), Paragraph("GENERATED", label_style)],
            [Paragraph(str(run_id), small_style), Paragraph(str(report.get("overall_status", "UNKNOWN")), body_style), Paragraph(str(report.get("severity", "UNKNOWN")), body_style), Paragraph(str(report.get("generated_at", "Not available")), small_style)],
        ],
        colWidths=[75 * mm, 30 * mm, 25 * mm, 44 * mm],
    )
    header_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F8FAFC")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#D0D5DD")),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#EAECF0")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("PADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(header_table)

    story.append(Spacer(1, 6))

    # --------------------------------------------------
    # EXECUTIVE SUMMARY
    # --------------------------------------------------

    story.append(
        Paragraph(
            "Executive Summary",
            heading_style,
        )
    )

    story.append(
        Paragraph(
            str(
                report.get(
                    "executive_summary",
                    "No executive summary available.",
                )
            ),
            body_style,
        )
    )

    # --------------------------------------------------
    # OVERALL ASSESSMENT
    # --------------------------------------------------

    story.append(
        Paragraph(
            "Overall Assessment",
            heading_style,
        )
    )

    story.append(
        Paragraph(
            str(
                report.get(
                    "overall_assessment",
                    "No assessment available.",
                )
            ),
            body_style,
        )
    )

    sanitized = report.get("technical_evidence", {}).get("sanitized_evidence", {})
    levels = sanitized.get("levels", {})
    correlations = report.get("cross_level_analysis") or sanitized.get("cross_level_correlations", [])
    validation_summary = {
        str(item.get("level")): item
        for item in report.get("validation_summary", [])
        if isinstance(item, dict) and item.get("level")
    }

    # --------------------------------------------------
    # VALIDATION SUMMARY
    # --------------------------------------------------

    story.append(
        Paragraph(
            "Validation Summary",
            heading_style,
        )
    )

    summary_rows = [
        ["Level", "Validation", "Summary", "Status"]
    ]

    level_names = {
        "L1": "Schema",
        "L2": "Volume",
        "L3": "Record Matching",
        "L4": "Field Comparison",
        "L5": "Aggregation",
        "L6": "Data Quality"
    }

    for level_key in ["L1", "L2", "L3", "L4", "L5", "L6"]:
        level_data = levels.get(level_key, {})
        if level_data:
            summary = validation_summary.get(level_key, {})
            summary_rows.append(
                [
                    Paragraph(level_key, small_style),
                    Paragraph(level_names.get(level_key, level_key), small_style),
                    Paragraph(str(summary.get("summary") or "No summary was recorded."), small_style),
                    Paragraph(str(summary.get("status") or level_data.get("status", "UNKNOWN")), small_style),
                ]
            )

    if len(summary_rows) > 1:
        table = Table(
            summary_rows,
            colWidths=[15 * mm, 38 * mm, 92 * mm, 25 * mm],
            repeatRows=1,
        )

        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E5E7EB")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D1D5DB")),
                    ("PADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        story.append(table)
    else:
        story.append(Paragraph("No validation levels executed.", body_style))

    # --------------------------------------------------
    # KEY EVIDENCE
    # --------------------------------------------------

    story.append(
        Paragraph(
            "Key Findings",
            heading_style,
        )
    )

    findings = report.get("key_findings", [])
    if not findings:
        story.append(Paragraph("No key findings reported.", body_style))
        story.append(Spacer(1, 6))
    else:
        for finding in findings:
            f_dict = finding if isinstance(finding, dict) else (finding.model_dump() if hasattr(finding, "model_dump") else getattr(finding, "__dict__", {}))
            title = f_dict.get("title", "Finding")
            severity = f_dict.get("severity", "MEDIUM")
            
            story.append(Paragraph(f"<b>{title}</b>", ParagraphStyle('FTitle', parent=styles["Heading3"], fontSize=10, leading=13)))
            story.append(Paragraph(f"<b>Severity:</b> {severity}", body_style))
            
            observed = f_dict.get("observed_evidence", [])
            if observed:
                story.append(Paragraph("<b>Observed Evidence:</b>", body_style))
                for obs in observed:
                    story.append(Paragraph(f"• {obs}", small_style))
            
            derived = f_dict.get("derived_statistics", [])
            if derived:
                story.append(Paragraph("<b>Derived Metrics:</b>", body_style))
                for dm in derived:
                    story.append(Paragraph(f"• {dm}", small_style))
            
            interpretation = f_dict.get("likely_explanation")
            if interpretation:
                story.append(Paragraph("<b>What this means:</b>", body_style))
                story.append(Paragraph(f"• {interpretation}", small_style))
            
            impact = f_dict.get("impact")
            if impact:
                story.append(Paragraph("<b>Why this matters:</b>", body_style))
                story.append(Paragraph(f"- {impact}", small_style))

            story.append(Spacer(1, 8))

    # --------------------------------------------------
    # CROSS LEVEL ANALYSIS
    # --------------------------------------------------

    story.append(
        Paragraph(
            "How the Validation Levels Relate",
            heading_style,
        )
    )

    if correlations:
        for corr in correlations:
            story.append(Paragraph(f"<b>{corr.get('title') or corr.get('type', 'Cross-level comparison')}</b>", body_style))
            story.append(Paragraph(str(corr.get("conclusion") or corr.get("interpretation", "")), body_style))
            for evidence_item in corr.get("evidence", []):
                if isinstance(evidence_item, dict):
                    evidence_item = evidence_item.get("statement") or json.dumps(evidence_item, default=str)
                story.append(Paragraph(f"- {evidence_item}", small_style))
            for k, v in corr.items():
                if k not in ("correlation_id", "title", "type", "conclusion", "interpretation", "evidence", "levels"):
                    story.append(Paragraph(f"• {str(k).replace('_', ' ').capitalize()}: {str(v)}", small_style))
            story.append(Paragraph(f"<b>Levels:</b> {', '.join(corr.get('levels', []))}", body_style))
            story.append(Spacer(1, 6))
    else:
        story.append(Paragraph("No cross-level correlations were established.", body_style))
        story.append(Spacer(1, 6))

    # --------------------------------------------------
    # PRIVACY
    # --------------------------------------------------

    story.append(
        Paragraph(
            "Privacy",
            heading_style,
        )
    )

    story.append(
        Paragraph(
            "Privacy-safe analysis: raw client records, matched pairs, record keys and raw field values were not provided to the LLM. "
            "Analysis uses only derived structural and statistical evidence.",
            body_style,
        )
    )

    doc.build(story)

    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={
            "Content-Disposition": (
                f'attachment; filename="'
                f'comparison-analysis-{run_id}.pdf"'
            )
        },
    )


@router.get(
    "",
)
def list_comparisons():
    try:
        return persistence_repository.list_runs()
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )

@router.delete(
    "/{run_id}",
)
def delete_comparison(run_id: str):
    try:
        persistence_repository.delete_run(run_id)
        return {"status": "DELETED"}
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )
