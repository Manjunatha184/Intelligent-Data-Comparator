from datetime import datetime, timezone
import logging
import os
from time import perf_counter

from app.execution.models import (
    ComparisonLevel,
    ExecutionPlan,
    ExecutionTask,
    RuntimeQueue,
    RuntimeTaskState,
    ExecutionBatch,
    ExecutionStatus,
    ExecutionResultStatus,
)

from app.execution.resource_manager import (
    ResourceCapacity,
    ResourceRequirement,
    ResourceManager,
)

from app.execution.dispatcher import ExecutionDispatcher
from app.execution.collector import ExecutionCollector
from app.execution.worker_pool import ExecutionWorkerPool
from app.execution.cancellation import CancellationController
from app.persistence.repository import PostgresRepository


logger = logging.getLogger(__name__)

EXECUTION_MEMORY_CAPACITY_MB = int(os.getenv("EXECUTION_MEMORY_CAPACITY_MB", "4096"))
EXECUTION_DEFAULT_CPU_UNITS = int(os.getenv("EXECUTION_DEFAULT_CPU_UNITS", "1"))
EXECUTION_DEFAULT_MEMORY_MB = int(os.getenv("EXECUTION_DEFAULT_MEMORY_MB", "256"))
EXECUTION_DEFAULT_WORKERS = int(os.getenv("EXECUTION_DEFAULT_WORKERS", "1"))


class ExecutionEngine:
    """
    Executes an immutable ExecutionPlan.

    The ExecutionPlan is immutable.

    The ExecutionEngine owns all mutable runtime state.

    Responsibilities:

        Stage 1 - Initialize execution
        Stage 2 - Build runtime queue
        Stage 3 - Schedule ready tasks
        Stage 4 - Execute batches
        Stage 5 - Update runtime state
        Stage 6 - Manage remaining batches
        Stage 7 - Finalize execution

    Execution capabilities:

        - Immutable execution plan
        - Runtime task state
        - Dependency-aware scheduling
        - Parallel worker pool
        - Retry support
        - Timeout support
        - Cancellation
        - Error policies
        - Batch lifecycle
        - Pushdown executor abstraction
        - Partition execution
        - Streaming support
        - Resource-aware scheduling
    """

    # ========================================================
    # INITIALIZATION
    # ========================================================

    def __init__(
        self,
        persistence_repository: PostgresRepository | None = None,
    ):

        self.plan: ExecutionPlan | None = None

        self.persistence_repository = (
            persistence_repository
        )

        self.runtime_queue: RuntimeQueue | None = None

        self.started_at: datetime | None = None

        self.pending_batches: list[
            ExecutionBatch
        ] = []

        self.cancellation = (
            CancellationController()
        )

        # ----------------------------------------------------
        # Resource management
        # ----------------------------------------------------

        self.resource_manager: ResourceManager | None = None

        # Keeps track of resources allocated to each batch.
        #
        # This is required because resources must be released
        # when a batch finishes, is skipped, or is cancelled.
        self.batch_resource_requirements: dict[
            str,
            list[ResourceRequirement],
        ] = {}

    # ========================================================
    # STAGE 1 — INITIALIZE EXECUTION
    # ========================================================

    def initialize(
        self,
        plan: ExecutionPlan,
    ) -> RuntimeQueue:
        """
        Initialize runtime state for an execution plan.
        """

        if not plan.tasks:
            raise ValueError(
                "Execution plan contains no tasks."
            )

        self._validate_dependency_graph(plan)

        # Reset cancellation state for every new run.
        self.cancellation.reset()

        runtime_queue = RuntimeQueue()

        # ----------------------------------------------------
        # Immutable plan -> mutable runtime queue
        # ----------------------------------------------------

        runtime_queue.waiting_tasks = list(
            plan.tasks
        )

        # ----------------------------------------------------
        # Create mutable runtime state for every task
        # ----------------------------------------------------

        runtime_queue.task_states = {
            task.task_id: RuntimeTaskState(
                task_id=task.task_id,
                status=ExecutionStatus.PENDING,
                attempt_count=0,
                max_attempts=(
                    plan.rules.retry_count + 1
                    if plan.rules.retry_enabled
                    else 1
                ),
            )
            for task in plan.tasks
        }

        self.plan = plan

        self.runtime_queue = runtime_queue

        self.started_at = datetime.now(
            timezone.utc
        )

        # ----------------------------------------------------
        # Persist execution run
        # ----------------------------------------------------

        if self.persistence_repository is not None:

            self.persistence_repository.create_run(
                run_id=plan.metadata.run_id,
                plan_id=plan.metadata.plan_id,
                configuration_id=(
                    plan.metadata.configuration_id
                ),
                status="RUNNING",
                started_at=(
                    self.started_at.isoformat()
                ),
            )

        self.pending_batches = []

        self.batch_resource_requirements = {}

        # ----------------------------------------------------
        # Initialize resource manager
        # ----------------------------------------------------

        max_workers = (
            plan.rules.max_parallel_workers
        )

        if max_workers <= 0:
            raise ValueError(
                "max_parallel_workers must be greater than zero."
            )

        self.resource_manager = ResourceManager(
            ResourceCapacity(
                cpu_units=max_workers,
                memory_mb=EXECUTION_MEMORY_CAPACITY_MB,
                workers=max_workers,
            )
        )

        return runtime_queue

    # ========================================================
    # CANCELLATION
    # ========================================================

    def request_cancel(self) -> None:
        """
        Request cancellation of the current execution.

        Running tasks are not forcefully terminated.
        The engine stops scheduling new work.
        """

        if self.runtime_queue is None:
            raise RuntimeError(
                "Execution Engine has not been initialized."
            )

        self.cancellation.request_cancel()

    def is_cancelled(self) -> bool:
        """
        Return True when cancellation has been requested.
        """

        return (
            self.cancellation.is_cancel_requested
        )

    def cancel_pending_tasks(self) -> list[str]:
        """
        Cancel all WAITING and READY tasks.

        RUNNING tasks are intentionally left untouched.
        """

        if self.runtime_queue is None:
            raise RuntimeError(
                "Runtime queue has not been initialized."
            )

        return (
            self.cancellation.cancel_pending_tasks(
                self.runtime_queue
            )
        )

    # ========================================================
    # SKIP PENDING TASKS
    # ========================================================

    def skip_pending_tasks(self) -> list[str]:
        """
        Skip all work that has not started because an
        execution error policy stopped further processing.

        RUNNING tasks are never skipped here.
        """

        if self.runtime_queue is None:
            raise RuntimeError(
                "Runtime queue has not been initialized."
            )

        skipped_ids = set()

        # ====================================================
        # 1. WAITING TASKS
        # ====================================================

        for task in self.runtime_queue.waiting_tasks:

            state = self.runtime_queue.task_states[
                task.task_id
            ]

            if state.status in (
                ExecutionStatus.PENDING,
                ExecutionStatus.READY,
            ):

                state.status = (
                    ExecutionStatus.SKIPPED
                )

                skipped_ids.add(
                    task.task_id
                )

        # ====================================================
        # 2. READY TASKS
        # ====================================================

        for task in self.runtime_queue.ready_tasks:

            state = self.runtime_queue.task_states[
                task.task_id
            ]

            if state.status in (
                ExecutionStatus.PENDING,
                ExecutionStatus.READY,
            ):

                state.status = (
                    ExecutionStatus.SKIPPED
                )

                skipped_ids.add(
                    task.task_id
                )

        # ====================================================
        # 3. FUTURE BATCHES
        # ====================================================

        for batch in self.pending_batches:

            for task in batch.tasks:

                state = self.runtime_queue.task_states[
                    task.task_id
                ]

                if state.status == ExecutionStatus.RUNNING:
                    continue

                if state.status not in (
                    ExecutionStatus.COMPLETED,
                    ExecutionStatus.FAILED,
                    ExecutionStatus.SKIPPED,
                    ExecutionStatus.CANCELLED,
                ):

                    state.status = (
                        ExecutionStatus.SKIPPED
                    )

                    skipped_ids.add(
                        task.task_id
                    )

        # ====================================================
        # 4. Release resources belonging to future batches
        # ====================================================

        for batch in list(
            self.pending_batches
        ):
            self._release_batch_resources(
                batch
            )

        # ====================================================
        # 5. Remove skipped tasks from queues
        # ====================================================

        skipped_set = set(
            skipped_ids
        )

        self.runtime_queue.waiting_tasks = [
            task
            for task
            in self.runtime_queue.waiting_tasks
            if task.task_id
            not in skipped_set
        ]

        self.runtime_queue.ready_tasks = [
            task
            for task
            in self.runtime_queue.ready_tasks
            if task.task_id
            not in skipped_set
        ]

        # ====================================================
        # 6. Remove future batches
        # ====================================================

        self.pending_batches = []

        return sorted(
            skipped_ids
        )

    # ========================================================
    # STAGE 2 — BUILD RUNTIME QUEUE
    # ========================================================

    def build_runtime_queue(
        self,
    ) -> RuntimeQueue:
        """
        Move dependency-ready tasks from WAITING to READY.

        A task becomes READY only when every dependency has
        completed successfully.
        """

        if self.runtime_queue is None:
            raise RuntimeError(
                "Execution Engine has not been initialized."
            )

        # ----------------------------------------------------
        # Cancellation
        # ----------------------------------------------------

        if self.cancellation.is_cancel_requested:

            self.cancel_pending_tasks()

            return self.runtime_queue

        # ----------------------------------------------------
        # Completed dependency IDs
        # ----------------------------------------------------

        completed_ids = {
            task_id
            for task_id, state
            in self.runtime_queue.task_states.items()
            if state.status
            == ExecutionStatus.COMPLETED
        }

        # A task whose dependency has already reached a terminal
        # non-success state can never become READY.  Leaving such
        # tasks in PENDING causes the engine's deadlock guard to
        # report "unresolved dependencies", hiding the real failure.
        blocked_dependency_statuses = {
            ExecutionStatus.FAILED,
            ExecutionStatus.SKIPPED,
            ExecutionStatus.CANCELLED,
        }

        remaining_tasks = []

        ready_tasks = []

        # ----------------------------------------------------
        # Evaluate waiting tasks
        # ----------------------------------------------------

        for task in self.runtime_queue.waiting_tasks:

            state = self.runtime_queue.task_states[
                task.task_id
            ]

            # ------------------------------------------------
            # Disabled task
            # ------------------------------------------------

            if not task.enabled:

                state.status = (
                    ExecutionStatus.SKIPPED
                )

                continue

            # ------------------------------------------------
            # Dependencies
            # ------------------------------------------------

            blocking_dependencies = [
                dependency
                for dependency in task.dependencies
                if (
                    dependency in self.runtime_queue.task_states
                    and self.runtime_queue.task_states[
                        dependency
                    ].status in blocked_dependency_statuses
                )
            ]

            if blocking_dependencies:
                # Do not manufacture a second execution failure for a
                # task that could never run.  The original dependency
                # remains FAILED and this downstream task is SKIPPED.
                state.status = ExecutionStatus.SKIPPED
                logger.warning(
                    "Skipping task %s because dependencies did not "
                    "complete successfully: %s",
                    task.task_id,
                    ", ".join(blocking_dependencies),
                )
                continue

            dependencies_met = all(
                dependency in completed_ids
                for dependency in task.dependencies
            )

            if dependencies_met:

                state.status = (
                    ExecutionStatus.READY
                )

                ready_tasks.append(task)

            else:

                remaining_tasks.append(task)

        self.runtime_queue.ready_tasks = (
            ready_tasks
        )

        self.runtime_queue.waiting_tasks = (
            remaining_tasks
        )

        # ----------------------------------------------------
        # Cancellation race protection
        # ----------------------------------------------------

        if self.cancellation.is_cancel_requested:

            self.cancel_pending_tasks()

        return self.runtime_queue

    # ========================================================
    # RESOURCE REQUIREMENT
    # ========================================================

    def _get_resource_requirement(
        self,
        task,
    ) -> ResourceRequirement:
        """
        Resolve resource requirements for a task.

        Tasks may optionally define:

            configuration["resource_requirements"]

        Example:

            {
                "cpu_units": 2,
                "memory_mb": 1024,
                "workers": 1
            }

        If no requirement is supplied, a safe default is used.
        """

        configuration = (
            task.configuration
            if task.configuration
            else {}
        )

        raw_requirement = configuration.get(
            "resource_requirements"
        )

        if not raw_requirement:
            return ResourceRequirement(
                cpu_units=EXECUTION_DEFAULT_CPU_UNITS,
                memory_mb=EXECUTION_DEFAULT_MEMORY_MB,
                workers=EXECUTION_DEFAULT_WORKERS,
            )

        return ResourceRequirement(
            cpu_units=int(
                raw_requirement.get(
                    "cpu_units",
                    EXECUTION_DEFAULT_CPU_UNITS,
                )
            ),
            memory_mb=int(
                raw_requirement.get(
                    "memory_mb",
                    EXECUTION_DEFAULT_MEMORY_MB,
                )
            ),
            workers=int(
                raw_requirement.get(
                    "workers",
                    EXECUTION_DEFAULT_WORKERS,
                )
            ),
        )

    # ========================================================
    # RESOURCE ALLOCATION
    # ========================================================

    def _allocate_batch_resources(
        self,
        batch: ExecutionBatch,
    ) -> None:
        """
        Allocate resources for every task in a batch.
        """

        if self.resource_manager is None:
            raise RuntimeError(
                "Resource manager has not been initialized."
            )

        requirements = []

        try:

            for task in batch.tasks:

                requirement = (
                    self._get_resource_requirement(
                        task
                    )
                )

                self.resource_manager.allocate(
                    requirement
                )

                requirements.append(
                    requirement
                )

        except Exception:

            # Roll back allocations already made
            # if the complete batch cannot be allocated.

            for requirement in requirements:
                self.resource_manager.release(
                    requirement
                )

            raise

        self.batch_resource_requirements[
            batch.batch_id
        ] = requirements

    # ========================================================
    # RESOURCE RELEASE
    # ========================================================

    def _release_batch_resources(
        self,
        batch: ExecutionBatch,
    ) -> None:
        """
        Release resources allocated to a batch.

        Safe to call multiple times.
        """

        if self.resource_manager is None:
            return

        requirements = (
            self.batch_resource_requirements.pop(
                batch.batch_id,
                [],
            )
        )

        for requirement in requirements:

            self.resource_manager.release(
                requirement
            )

    # ========================================================
    # STAGE 3 — SCHEDULE READY TASKS
    # ========================================================

    def schedule_ready_tasks(
        self,
    ) -> list[ExecutionBatch]:
        """
        Build execution batches from READY tasks.

        Scheduling considers:

            - max_parallel_workers
            - task parallel eligibility
            - CPU requirements
            - memory requirements
            - worker requirements

        Resource allocation happens before a batch is exposed
        as pending work.
        """

        if self.plan is None:
            raise RuntimeError(
                "Execution Engine has not been initialized."
            )

        if self.runtime_queue is None:
            raise RuntimeError(
                "Runtime queue has not been initialized."
            )

        if self.resource_manager is None:
            raise RuntimeError(
                "Resource manager has not been initialized."
            )

        # ----------------------------------------------------
        # Cancellation
        # ----------------------------------------------------

        if self.cancellation.is_cancel_requested:

            self.cancel_pending_tasks()

            return []

        max_workers = (
            self.plan.rules.max_parallel_workers
        )

        if max_workers <= 0:
            raise ValueError(
                "max_parallel_workers must be greater than zero."
            )

        ready_tasks = list(
            self.runtime_queue.ready_tasks
        )

        if not ready_tasks:
            return []

        batches = []

        remaining_tasks = list(
            ready_tasks
        )

        batch_number = 1

        while remaining_tasks:

            batch_tasks = []

            selected_indexes = []

            used_workers = 0

            # ------------------------------------------------
            # Select tasks that fit current resources
            # ------------------------------------------------

            for index, task in enumerate(
                remaining_tasks
            ):

                requirement = (
                    self._get_resource_requirement(
                        task
                    )
                )

                # Respect worker limit.

                if (
                    used_workers
                    + requirement.workers
                    > max_workers
                ):
                    continue

                # Check global currently available resources.

                if not self.resource_manager.can_allocate(
                    requirement
                ):
                    continue

                batch_tasks.append(task)

                selected_indexes.append(index)

                used_workers += (
                    requirement.workers
                )

                # Stop once worker capacity is reached.

                if used_workers >= max_workers:
                    break

            # ------------------------------------------------
            # Nothing can fit
            # ------------------------------------------------

            if not batch_tasks:

                blocked_task = remaining_tasks[0]

                requirement = (
                    self._get_resource_requirement(
                        blocked_task
                    )
                )

                raise RuntimeError(
                    "Task cannot be scheduled because "
                    "its resource requirement exceeds "
                    "available execution capacity. "
                    f"Task={blocked_task.task_id}, "
                    f"CPU={requirement.cpu_units}, "
                    f"Memory={requirement.memory_mb}MB, "
                    f"Workers={requirement.workers}"
                )

            # ------------------------------------------------
            # Build batch
            # ------------------------------------------------

            parallel_eligible = (
                len(batch_tasks) > 1
                and all(
                    task.parallel_eligible
                    for task in batch_tasks
                )
            )

            batch = ExecutionBatch(
                batch_id=f"BATCH-{batch_number}",
                tasks=tuple(batch_tasks),
                parallel=parallel_eligible,
            )

            batches.append(batch)

            batch_number += 1


            # ------------------------------------------------
            # Remove selected tasks
            # ------------------------------------------------

            selected_set = set(
                selected_indexes
            )

            remaining_tasks = [
                task
                for index, task
                in enumerate(
                    remaining_tasks
                )
                if index
                not in selected_set
            ]

        # ----------------------------------------------------
        # READY tasks have now been scheduled.
        # ----------------------------------------------------

        self.runtime_queue.ready_tasks = []

        self.pending_batches = batches

        return batches

    # ========================================================
    # STAGE 4 — EXECUTE ONE BATCH
    # ========================================================

    def execute_batch(
        self,
        batch: ExecutionBatch,
        dispatcher: ExecutionDispatcher,
        collector: ExecutionCollector,
    ) -> None:

        if self.runtime_queue is None:
            raise RuntimeError(
                "Runtime queue has not been initialized."
            )

        if self.plan is None:
            raise RuntimeError(
                "Execution Engine has not been initialized."
            )

        if self.resource_manager is None:
            raise RuntimeError(
                "Resource manager has not been initialized."
            )

        # Allocate resources only when this batch starts.
        self._allocate_batch_resources(batch)

        try:

            attempt_numbers = {}

            runtime_tasks = []
            precreated_result_ids: dict[str, int] = {}

            if (
                self.persistence_repository is not None
                and getattr(
                    dispatcher,
                    "persistence_repository",
                    None,
                )
                is None
            ):
                dispatcher.persistence_repository = (
                    self.persistence_repository
                )

            for task in batch.tasks:

                # ------------------------------------------------
                # Inject dependency results statelessly
                # ------------------------------------------------
                
                dependencies_context = {
                    "__run_id": (
                        self.plan.metadata.run_id
                    )
                }

                for dependency_id in task.dependencies:
                    dep_state = self.runtime_queue.task_states.get(
                        dependency_id
                    )
                    if dep_state and dep_state.result:
                        dependencies_context[
                            f"dependency_{dependency_id}"
                        ] = dep_state.result
                
                if dependencies_context:
                    # Create a new configuration dictionary
                    new_configuration = dict(task.configuration)
                    new_configuration.update(dependencies_context)
                    
                    # Create a cloned task for execution
                    runtime_task = task.model_copy(
                        update={"configuration": new_configuration}
                    )
                else:
                    runtime_task = task

                if (
                    self.persistence_repository is not None
                    and self._uses_stored_l3_matched_pairs(
                        runtime_task
                    )
                ):
                    result_id = (
                        self.persistence_repository
                        .save_comparison_result(
                            run_id=(
                                self.plan.metadata.run_id
                            ),
                            task_id=task.task_id,
                            metrics={
                                "status": "RUNNING",
                            },
                            evidence={
                                "status": "PENDING",
                            },
                        )
                    )

                    precreated_result_ids[
                        task.task_id
                    ] = result_id

                    new_configuration = dict(
                        runtime_task.configuration
                    )
                    new_configuration[
                        "__comparison_result_id"
                    ] = result_id
                    runtime_task = runtime_task.model_copy(
                        update={
                            "configuration": (
                                new_configuration
                            )
                        }
                    )

                runtime_tasks.append(runtime_task)

                state = self.runtime_queue.task_states[
                    task.task_id
                ]

                state.status = (
                    ExecutionStatus.RUNNING
                )

                state.attempt_count += 1

                attempt_numbers[
                    task.task_id
                ] = state.attempt_count

                if task not in (
                    self.runtime_queue.running_tasks
                ):
                    self.runtime_queue.running_tasks.append(
                        task
                    )

            worker_pool = ExecutionWorkerPool(
                max_workers=(
                    # A SparkSession is shared for dataset/match reuse. Run
                    # Spark levels serially through that session; concurrent
                    # driver jobs only duplicate work and contend for the two
                    # available executors.
                    1
                    if any(
                        task.configuration.get("execution_location")
                        in {"SPARK", "DUCKDB"}
                        for task in runtime_tasks
                    )
                    else self.plan.rules.max_parallel_workers
                ),
                dispatcher=dispatcher,
            )

            results = worker_pool.execute(
                tasks=runtime_tasks,
                attempt_numbers=attempt_numbers,
            )

            result_by_task_id = {
                result.task_id: result
                for result in results
            }

            for task in batch.tasks:

                result = result_by_task_id.get(
                    task.task_id
                )

                if result is None:
                    raise RuntimeError(
                        "WorkerPool returned no result for "
                        f"task {task.task_id}"
                    )

                collector.collect(
                    task,
                    result,
                    self.runtime_queue,
                )

                # ------------------------------------------------
                # Persist task execution and comparison result
                # ------------------------------------------------

                if self.persistence_repository is not None:

                    state = self.runtime_queue.task_states[
                        task.task_id
                    ]

                    self.persistence_repository.save_task_execution(
                        run_id=self.plan.metadata.run_id,
                        task_id=task.task_id,
                        comparison_level=(
                            task.comparison_level.value
                        ),
                        comparator_name=(
                            task.comparator_name
                        ),
                        execution_mode=(
                            task.execution_mode.value
                        ),
                        status=(
                            state.status.value
                        ),
                        started_at=(
                            result.started_at.isoformat()
                            if result.started_at
                            else None
                        ),
                        finished_at=(
                            result.finished_at.isoformat()
                            if result.finished_at
                            else None
                        ),
                        error=(
                            state.last_error
                            if state.last_error
                            else None
                        ),
                    )

                    result_id = (
                        result.runtime_context.get(
                            "comparison_result_id"
                        )
                        or precreated_result_ids.get(
                            task.task_id
                        )
                    )

                    if result_id is not None:
                        persistence_started = perf_counter()
                        metrics = result.metrics
                        evidence = result.evidence

                        if (
                            result.status
                            == ExecutionResultStatus.FAILED
                            and result_id
                            in precreated_result_ids.values()
                        ):
                            metrics = dict(metrics)
                            metrics["status"] = "FAILED"
                            metrics["error"] = (
                                result.error
                            )
                            metrics[
                                "attempt_number"
                            ] = result.attempt_number

                            evidence = dict(evidence)
                            evidence["status"] = "FAILED"
                            evidence[
                                "incomplete"
                            ] = True
                            if result.error:
                                evidence[
                                    "error"
                                ] = result.error

                        self.persistence_repository.update_comparison_result(
                            result_id=int(result_id),
                            metrics=metrics,
                            evidence=evidence,
                        )
                        logger.info("COMPARISON_TIMING run_id=%s task_id=%s level=%s result_persistence_ms=%.1f", self.plan.metadata.run_id, task.task_id, task.comparison_level.value, (perf_counter() - persistence_started) * 1000)
                    else:
                        persistence_started = perf_counter()
                        self.persistence_repository.save_comparison_result(
                            run_id=self.plan.metadata.run_id,
                            task_id=task.task_id,
                            metrics=result.metrics,
                            evidence=result.evidence,
                        )
                        logger.info("COMPARISON_TIMING run_id=%s task_id=%s level=%s result_persistence_ms=%.1f", self.plan.metadata.run_id, task.task_id, task.comparison_level.value, (perf_counter() - persistence_started) * 1000)

        except Exception:

            # Always release resources if execution itself
            # fails before complete_batch() is called.

            self._release_batch_resources(batch)

            raise

    @staticmethod
    def _uses_stored_l3_matched_pairs(
        task: ExecutionTask,
    ) -> bool:

        if task.comparison_level != ComparisonLevel.L4:
            return False

        for key, value in task.configuration.items():
            if not key.startswith("dependency_"):
                continue

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

            if (
                isinstance(evidence, dict)
                and isinstance(
                    evidence.get("matched_pairs_ref"),
                    dict,
                )
            ):
                return True

        return False

    # ========================================================
    # STAGE 4-6 — EXECUTE REMAINING BATCHES
    # ========================================================

    def execute_remaining_batches(
        self,
        dispatcher: ExecutionDispatcher,
        collector: ExecutionCollector,
    ) -> None:
        """
        Execute all remaining scheduled work.

        Lifecycle:

            pending batches
                    ↓
            execute batch
                    ↓
            collect results
                    ↓
            complete batch
                    ↓
            rebuild runtime queue
                    ↓
            schedule newly-ready tasks
                    ↓
            repeat
        """

        if self.plan is None:
            raise RuntimeError(
                "Execution Engine has not been initialized."
            )

        if self.runtime_queue is None:
            raise RuntimeError(
                "Runtime queue has not been initialized."
            )

        while True:

            # =================================================
            # 1. Execute scheduled batch
            # =================================================

            if self.pending_batches:

                batch = self.pending_batches[0]

                try:

                    self.execute_batch(
                        batch,
                        dispatcher,
                        collector,
                    )

                    self.complete_batch(
                        batch
                    )

                except Exception:

                    # Prevent resource leakage if execution
                    # fails before normal batch completion.

                    self._release_batch_resources(
                        batch
                    )

                    self.pending_batches = [
                        pending
                        for pending
                        in self.pending_batches
                        if pending.batch_id
                        != batch.batch_id
                    ]

                    raise

                # =================================================
                # ERROR POLICY
                # =================================================

                failed_tasks = (
                    self.runtime_queue.failed_tasks
                )

                if failed_tasks:

                    if self.plan.rules.fail_fast:

                        self.skip_pending_tasks()

                        self.pending_batches = []

                        break

                    elif not self.plan.rules.continue_on_error:

                        self.skip_pending_tasks()

                        self.pending_batches = []

                        break

                # ------------------------------------------------
                # Cancellation after current batch
                # ------------------------------------------------

                if (
                    self.cancellation.is_cancel_requested
                ):

                    self.cancel_pending_tasks()

                    # Release any future batches.

                    for pending_batch in list(
                        self.pending_batches
                    ):
                        self._release_batch_resources(
                            pending_batch
                        )

                    self.pending_batches = []

                    continue

                continue

            # =================================================
            # 2. Cancellation before new work
            # =================================================

            if self.cancellation.is_cancel_requested:

                self.cancel_pending_tasks()

                break

            # =================================================
            # 3. Recalculate dependency-ready tasks
            # =================================================

            self.build_runtime_queue()

            # =================================================
            # 4. Cancellation check
            # =================================================

            if self.cancellation.is_cancel_requested:

                self.cancel_pending_tasks()

                break

            # =================================================
            # 5. Schedule newly-ready tasks
            # =================================================

            batches = (
                self.schedule_ready_tasks()
            )

            if batches:
                continue

            # =================================================
            # 6. No batches and no remaining work
            # =================================================

            if not self.has_remaining_work():
                break

            # =================================================
            # 7. Prevent infinite loop
            # =================================================

            if (
                not self.runtime_queue.ready_tasks
                and not self.runtime_queue.running_tasks
                and self.runtime_queue.waiting_tasks
            ):

                raise RuntimeError(
                    "Execution cannot make progress. "
                    "Waiting tasks have unresolved dependencies."
                )

    # ========================================================
    # STAGE 5 — UPDATE RUNTIME STATE
    # ========================================================

    def update_runtime_state(
        self,
    ) -> dict:
        """
        Calculate current execution progress.
        """

        if self.plan is None:
            raise RuntimeError(
                "Execution Engine has not been initialized."
            )

        if self.runtime_queue is None:
            raise RuntimeError(
                "Runtime queue has not been initialized."
            )

        states = list(
            self.runtime_queue.task_states.values()
        )

        total = len(states)

        completed = sum(
            state.status
            == ExecutionStatus.COMPLETED
            for state in states
        )

        failed = sum(
            state.status
            == ExecutionStatus.FAILED
            for state in states
        )

        running = sum(
            state.status
            == ExecutionStatus.RUNNING
            for state in states
        )

        ready = sum(
            state.status
            == ExecutionStatus.READY
            for state in states
        )

        waiting = sum(
            state.status
            == ExecutionStatus.PENDING
            for state in states
        )

        skipped = sum(
            state.status
            == ExecutionStatus.SKIPPED
            for state in states
        )

        cancelled = sum(
            state.status
            == ExecutionStatus.CANCELLED
            for state in states
        )

        finished = (
            completed
            + failed
            + skipped
            + cancelled
        )

        progress = (
            (finished / total) * 100
            if total > 0
            else 0.0
        )

        return {
            "total_tasks": total,
            "completed_tasks": completed,
            "failed_tasks": failed,
            "running_tasks": running,
            "ready_tasks": ready,
            "waiting_tasks": waiting,
            "skipped_tasks": skipped,
            "cancelled_tasks": cancelled,
            "progress_percent": round(
                progress,
                2,
            ),
        }

    # ========================================================
    # STAGE 6 — REMAINING WORK
    # ========================================================

    def has_remaining_work(
        self,
    ) -> bool:
        """
        Return True when execution still has non-terminal work.
        """

        if self.runtime_queue is None:
            raise RuntimeError(
                "Runtime queue has not been initialized."
            )

        if self.pending_batches:
            return True

        if self.runtime_queue.running_tasks:
            return True

        if self.runtime_queue.ready_tasks:
            return True

        if self.runtime_queue.waiting_tasks:
            return True

        terminal_states = {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.SKIPPED,
            ExecutionStatus.CANCELLED,
        }

        return any(
            state.status
            not in terminal_states
            for state
            in self.runtime_queue.task_states.values()
        )

    # ========================================================
    # STAGE 7 — FINALIZE EXECUTION
    # ========================================================

    def finalize_execution(
        self,
    ) -> dict:
        """
        Finalize the execution run.

        Final status:

            FAILED
                At least one task failed.

            CANCELLED
                Execution was explicitly cancelled.

            COMPLETED
                All tasks completed/skipped normally.

            INCOMPLETE
                Execution ended without a terminal state.
        """

        if self.plan is None:
            raise RuntimeError(
                "Execution Engine has not been initialized."
            )

        if self.runtime_queue is None:
            raise RuntimeError(
                "Runtime queue has not been initialized."
            )

        state = self.update_runtime_state()

        if self.pending_batches:
            raise RuntimeError(
                "Cannot finalize execution with pending batches."
            )

        if self.runtime_queue.running_tasks:
            raise RuntimeError(
                "Cannot finalize execution with running tasks."
            )

        finished_at = datetime.now(
            timezone.utc
        )

        if self.started_at is None:
            raise RuntimeError(
                "Execution start time is not initialized."
            )

        duration = (
            finished_at - self.started_at
        ).total_seconds()

        # ----------------------------------------------------
        # Final status
        # ----------------------------------------------------

        if (
            self.cancellation.is_cancel_requested
        ):

            status = "CANCELLED"

        elif state["failed_tasks"] > 0:

            status = "FAILED"

        elif (
            state["completed_tasks"]
            + state["skipped_tasks"]
            + state["cancelled_tasks"]
            == state["total_tasks"]
        ):

            status = "COMPLETED"

        else:

            status = "INCOMPLETE"

        # ----------------------------------------------------
        # Persist final run status
        # ----------------------------------------------------

        if self.persistence_repository is not None:

            comparison_status = None

            comparison_results = []

            for task in self.plan.tasks:

                task_state = (
                    self.runtime_queue.task_states[
                        task.task_id
                    ]
                )

                if task_state.result is not None:

                    comparison_results.append(
                        task_state.result
                    )

            if comparison_results:

                if any(
                    result.metrics.get("status") == "FAIL"
                    for result in comparison_results
                ):
                    comparison_status = "FAIL"

                else:
                    comparison_status = "PASS"

            self.persistence_repository.complete_run(
                run_id=self.plan.metadata.run_id,
                status=status,
                comparison_status=comparison_status,
                finished_at=finished_at.isoformat(),
            )

        return {
            "plan_id": (
                self.plan.metadata.plan_id
            ),

            "run_id": (
                self.plan.metadata.run_id
            ),

            "status": status,

            "started_at": self.started_at,

            "finished_at": finished_at,

            "duration_seconds": duration,

            "runtime_state": state,

            "completed_tasks": [
                task.task_id
                for task
                in self.runtime_queue.completed_tasks
            ],

            "failed_tasks": [
                task.task_id
                for task
                in self.runtime_queue.failed_tasks
            ],

            "cancelled_tasks": [
                task_id
                for task_id, task_state
                in self.runtime_queue.task_states.items()
                if task_state.status
                == ExecutionStatus.CANCELLED
            ],
        }

    # ========================================================
    # DEPENDENCY VALIDATION
    # ========================================================

    def _validate_dependency_graph(
        self,
        plan: ExecutionPlan,
    ) -> None:
        """
        Validate:

            1. Every task exists.
            2. Every dependency exists.
            3. No circular dependencies exist.
        """

        task_ids = {
            task.task_id
            for task in plan.tasks
        }

        graph = (
            plan.dependency_graph.dependencies
        )

        # ----------------------------------------------------
        # Validate task IDs and dependencies
        # ----------------------------------------------------

        for task_id, dependencies in graph.items():

            if task_id not in task_ids:

                raise ValueError(
                    "Unknown task in dependency graph: "
                    f"{task_id}"
                )

            for dependency in dependencies:

                if dependency not in task_ids:

                    raise ValueError(
                        f"Task {task_id} depends on "
                        f"unknown task {dependency}"
                    )

        # ----------------------------------------------------
        # Detect cycles using DFS
        # ----------------------------------------------------

        visiting = set()

        visited = set()

        def visit(
            task_id: str,
        ) -> None:

            if task_id in visiting:

                raise ValueError(
                    "Circular dependency detected."
                )

            if task_id in visited:
                return

            visiting.add(task_id)

            for dependency in graph.get(
                task_id,
                (),
            ):

                visit(
                    dependency
                )

            visiting.remove(task_id)

            visited.add(task_id)

        for task_id in task_ids:

            visit(
                task_id
            )

    # ========================================================
    # BATCH LIFECYCLE
    # ========================================================

    def complete_batch(
        self,
        batch: ExecutionBatch,
    ) -> None:
        """
        Mark a batch as completed and remove it from
        pending work.

        Resources are released only after all tasks in
        the batch reach a terminal state.
        """

        if self.runtime_queue is None:
            raise RuntimeError(
                "Runtime queue has not been initialized."
            )

        batch_task_ids = {
            task.task_id
            for task in batch.tasks
        }

        # ----------------------------------------------------
        # Verify terminal states
        # ----------------------------------------------------

        for task_id in batch_task_ids:

            state = self.runtime_queue.task_states[
                task_id
            ]

            if state.status not in (
                ExecutionStatus.COMPLETED,
                ExecutionStatus.FAILED,
                ExecutionStatus.SKIPPED,
                ExecutionStatus.CANCELLED,
            ):

                raise RuntimeError(
                    f"Batch {batch.batch_id} cannot be "
                    f"completed because task {task_id} "
                    f"is still {state.status}."
                )

        # ----------------------------------------------------
        # Release resources
        # ----------------------------------------------------

        self._release_batch_resources(
            batch
        )

        # ----------------------------------------------------
        # Remove batch from pending work
        # ----------------------------------------------------

        self.pending_batches = [
            pending_batch
            for pending_batch
            in self.pending_batches
            if pending_batch.batch_id
            != batch.batch_id
        ]
