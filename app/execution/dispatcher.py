from datetime import datetime, timezone
from typing import Any

from app.connectors.manager import ConnectorManager

from app.execution.models import (
    ExecutionTask,
    ExecutionResult,
    ExecutionResultStatus,
    ExecutionLocation,
)

from app.execution.spark_executor import SparkExecutor
from app.execution.duckdb_executor import DuckDBExecutor


# ============================================================
# EXECUTION DISPATCHER
# ============================================================


class ExecutionDispatcher:

    def __init__(
        self,
        connector_manager=None,
        persistence_repository=None,
    ) -> None:

        self.connector_manager = connector_manager
        self.persistence_repository = persistence_repository

        # One SparkExecutor spans the execution plan so the Spark
        # session, datasets, statistics and PK reconciliation can
        # be reused across L1-L6.
        self.spark_executor = SparkExecutor(
            connector_manager=self.connector_manager
        )
        self.duckdb_executor = DuckDBExecutor(
            connector_manager=self.connector_manager
        )

    def close(self) -> None:
        """Release resources owned by this plan-scoped dispatcher."""
        self.spark_executor.close()
        self.duckdb_executor.close()

    # ========================================================
    # PUBLIC DISPATCH
    # ========================================================

    def dispatch(
        self,
        task: ExecutionTask,
        attempt_number: int = 1,
    ) -> ExecutionResult:

        started_at = datetime.now(
            timezone.utc
        )

        execution_location = self._resolve_execution_location(task)

        try:

            raw_result = self._execute(
                task=task,
                execution_location=execution_location,
            )

            finished_at = datetime.now(
                timezone.utc
            )

            return self._build_success_result(
                task=task,
                attempt_number=attempt_number,
                started_at=started_at,
                finished_at=finished_at,
                execution_location=execution_location,
                raw_result=raw_result,
            )

        except Exception as exc:

            finished_at = datetime.now(
                timezone.utc
            )

            return self._build_failure_result(
                task=task,
                attempt_number=attempt_number,
                started_at=started_at,
                finished_at=finished_at,
                execution_location=execution_location,
                error=str(exc),
            )

    # ========================================================
    # EXECUTION ROUTING
    # ========================================================

    def _execute(
        self,
        task: ExecutionTask,
        execution_location: ExecutionLocation,
    ) -> Any:

        if execution_location == ExecutionLocation.SPARK:
            return self._execute_spark(task)
        if execution_location == ExecutionLocation.DUCKDB:
            return self._execute_duckdb(task)
        raise RuntimeError(
            f"Unsupported execution location: {execution_location.value}"
        )

    # ========================================================
    # SPARK EXECUTION
    # ========================================================

    def _execute_spark(
        self,
        task: ExecutionTask,
    ) -> Any:

        return self.spark_executor.execute(task)

    def _execute_duckdb(
        self,
        task: ExecutionTask,
    ) -> Any:
        return self.duckdb_executor.execute(task)

    @staticmethod
    def _resolve_execution_location(
        task: ExecutionTask,
    ) -> ExecutionLocation:
        value = task.configuration.get(
            "execution_location",
            ExecutionLocation.SPARK.value,
        )
        if isinstance(value, ExecutionLocation):
            return value
        try:
            return ExecutionLocation(str(value).upper())
        except ValueError as error:
            raise RuntimeError(
                f"Invalid execution location '{value}' for {task.task_id}"
            ) from error

    # ========================================================
    # SUCCESS RESULT
    # ========================================================

    def _build_success_result(
        self,
        task: ExecutionTask,
        attempt_number: int,
        started_at: datetime,
        finished_at: datetime,
        execution_location: ExecutionLocation,
        raw_result: Any,
    ) -> ExecutionResult:

        metrics: dict[str, Any] = {}
        evidence: dict[str, Any] = {}
        runtime_context: dict[str, Any] = {}

        if isinstance(raw_result, dict):

            metrics = raw_result.get(
                "metrics",
                {},
            )

            evidence = raw_result.get(
                "evidence",
                {},
            )

            runtime_context = raw_result.get(
                "runtime_context",
                {},
            )

            if (
                raw_result.get(
                    "execution_location"
                )
                and not metrics.get(
                    "execution_location"
                )
            ):

                metrics = {
                    **metrics,
                    "execution_location": raw_result[
                        "execution_location"
                    ],
                }

            # ------------------------------------------------
            # Preserve executor metadata separately.
            # ------------------------------------------------

            raw_execution_location = (
                raw_result.get(
                    "execution_location"
                )
            )

            if (
                raw_execution_location
                and not metrics.get(
                    "execution_location"
                )
            ):

                metrics = {
                    **metrics,
                    "execution_location": (
                        raw_execution_location
                    ),
                }

        duration_ms = int(
            (
                finished_at
                - started_at
            ).total_seconds()
            * 1000
        )

        return ExecutionResult(
            task_id=task.task_id,
            comparator_name=(
                task.comparator_name
            ),
            status=(
                ExecutionResultStatus.SUCCESS
            ),
            attempt_number=attempt_number,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
            execution_mode=(
                task.execution_mode
            ),
            execution_location=(
                execution_location
            ),
            metrics=metrics,
            evidence=evidence,
            runtime_context=runtime_context,
        )

    # ========================================================
    # FAILURE RESULT
    # ========================================================

    def _build_failure_result(
        self,
        task: ExecutionTask,
        attempt_number: int,
        started_at: datetime,
        finished_at: datetime,
        execution_location: ExecutionLocation,
        error: str,
    ) -> ExecutionResult:

        duration_ms = int(
            (
                finished_at
                - started_at
            ).total_seconds()
            * 1000
        )

        return ExecutionResult(
            task_id=task.task_id,
            comparator_name=(
                task.comparator_name
            ),
            status=(
                ExecutionResultStatus.FAILED
            ),
            attempt_number=attempt_number,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
            execution_mode=(
                task.execution_mode
            ),
            execution_location=(
                execution_location
            ),
            error=error,
            runtime_context={}
        )
