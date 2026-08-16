from datetime import datetime, timezone
from typing import Any

from app.connectors.manager import ConnectorManager

from app.execution.models import (
    ExecutionTask,
    ExecutionResult,
    ExecutionResultStatus,
    ExecutionLocation,
)

from app.execution.executor import (
    ConnectorPushdownAdapter,
    LocalExecutor,
    PushdownExecutor,
    RecordingPushdownAdapter,
)
from app.execution.spark_executor import SparkExecutor


# ============================================================
# COMPATIBILITY REGISTRY
# ============================================================


class _DictComparatorRegistry:
    """
    Compatibility adapter for callers that still provide
    a plain comparator dictionary.

    The dispatcher depends only on the registry contract.
    """

    def __init__(
        self,
        comparators: dict[str, Any],
    ) -> None:

        self._comparators = comparators

    def get(
        self,
        name: str,
    ) -> Any:

        comparator = self._comparators.get(name)

        if comparator is None:
            raise RuntimeError(
                f"Comparator not registered: {name}"
            )

        return comparator


# ============================================================
# EXECUTION DISPATCHER
# ============================================================


class ExecutionDispatcher:
    """
    Resolves the correct execution backend for an ExecutionTask.

    The dispatcher does NOT decide comparison strategy.

    Strategy has already been decided by the planner and stored
    in the ExecutionTask configuration.

    The dispatcher only routes:

        LOCAL
            -> LocalExecutor
            -> Comparator

        PUSHDOWN
            -> PushdownExecutor
            -> PushdownAdapter

    Connector access is provided through ConnectorManager.

    The dispatcher does not know whether the connector is:

        CSV
        PostgreSQL
        MySQL
        Snowflake
        Databricks
        API
        etc.

    It also converts raw execution output into the standardized
    ExecutionResult model.
    """

    def __init__(
        self,
        comparator_registry=None,
        comparators=None,
        connector_manager=None,
        pushdown_adapter=None,
        persistence_repository=None,
    ) -> None:

        if comparator_registry is not None:
            self.comparator_registry = comparator_registry

        elif comparators is not None:
            self.comparator_registry = (
                _DictComparatorRegistry(comparators)
            )

        else:
            self.comparator_registry = (
                _DictComparatorRegistry({})
            )

        self.connector_manager = connector_manager
        self.persistence_repository = persistence_repository
        # One executor spans a plan so Spark can reuse its session, filtered
        # datasets, and authoritative primary-key match stream across levels.
        self.spark_executor = SparkExecutor(
            connector_manager=self.connector_manager
        )

        if pushdown_adapter is not None:
            self.pushdown_adapter = pushdown_adapter
        elif connector_manager is not None:
            self.pushdown_adapter = ConnectorPushdownAdapter(
                connector_manager=self.connector_manager,
                comparator_registry=self.comparator_registry,
            )
        else:
            self.pushdown_adapter = (
                RecordingPushdownAdapter()
            )

    def close(self) -> None:
        """Release resources owned by this plan-scoped dispatcher."""
        self.spark_executor.close()

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

        execution_location = (
            self._resolve_execution_location(
                task
            )
        )

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

        if (
            execution_location
            == ExecutionLocation.LOCAL
        ):

            return self._execute_local(
                task
            )

        if execution_location == ExecutionLocation.SPARK:
            return self._execute_spark(task)

        if (
            execution_location
            == ExecutionLocation.PUSHDOWN
        ):

            return self._execute_pushdown(
                task
            )

        raise RuntimeError(
            "Unsupported execution location: "
            f"{execution_location}"
        )

    # ========================================================
    # LOCAL EXECUTION
    # ========================================================

    def _execute_local(
        self,
        task: ExecutionTask,
    ) -> Any:

        comparator = (
            self.comparator_registry.get(
                task.comparator_name
            )
        )

        executor = LocalExecutor(
            comparator=comparator,
            connector_manager=self.connector_manager,
            persistence_repository=(
                self.persistence_repository
            ),
        )

        raw_result = executor.execute(task)

        runtime_context = {}

        if isinstance(raw_result, dict):
            runtime_context.update(
                raw_result.get(
                    "runtime_context",
                    {},
                )
                or {}
            )

        runtime_task = (
            executor.last_runtime_task
        )

        if runtime_task is not None:

            configuration = (
                runtime_task.configuration
            )

            source_records = configuration.get(
                "source_records",
                [],
            )

            target_records = configuration.get(
                "target_records",
                [],
            )

            runtime_context.update(
                {
                "source_records_loaded": len(
                    source_records
                ),
                "target_records_loaded": len(
                    target_records
                ),

                "source_statistics": (
                    configuration.get(
                        "source_statistics"
                    )
                ),

                "target_statistics": (
                    configuration.get(
                        "target_statistics"
                    )
                ),

                "comparison_keys": (
                    configuration.get(
                        "comparison_keys",
                        [],
                    )
                ),

                "source_data_loaded": (
                    "source_data"
                    in configuration
                ),

                "target_data_loaded": (
                    "target_data"
                    in configuration
                ),
                }
            )

        if isinstance(raw_result, dict):

            return {
                **raw_result,
                "runtime_context": runtime_context,
            }

        return {
            "result": raw_result,
            "runtime_context": runtime_context,
        }

    def _execute_spark(self, task: ExecutionTask) -> Any:
        return self.spark_executor.execute(task)

    # ========================================================
    # PUSHDOWN EXECUTION
    # ========================================================

    def _execute_pushdown(
        self,
        task: ExecutionTask,
    ) -> Any:

        executor = PushdownExecutor(
            adapter=self.pushdown_adapter
        )

        return executor.execute(
            task
        )

    # ========================================================
    # LOCATION RESOLUTION
    # ========================================================

    def _resolve_execution_location(
        self,
        task: ExecutionTask,
    ) -> ExecutionLocation:

        value = task.configuration.get(
            "execution_location"
        )

        if value is None:

            return ExecutionLocation.LOCAL

        if isinstance(
            value,
            ExecutionLocation,
        ):

            return value

        try:

            return ExecutionLocation(
                value
            )

        except ValueError as exc:

            raise RuntimeError(
                "Invalid execution location "
                f"'{value}' for task "
                f"{task.task_id}"
            ) from exc

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
