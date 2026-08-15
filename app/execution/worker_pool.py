from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)

from app.execution.models import (
    ExecutionTask,
    ExecutionResult,
)

from app.execution.dispatcher import ExecutionDispatcher


class ExecutionWorkerPool:
    """
    Executes independent ExecutionTasks concurrently.

    Responsibilities:
        - Create worker threads
        - Submit tasks to workers
        - Dispatch tasks
        - Collect ExecutionResult objects
        - Shut down workers cleanly

    The WorkerPool does NOT:
        - modify RuntimeQueue
        - update task state
        - collect results
        - decide execution strategy
        - retry tasks
        - handle business-level comparison logic
    """

    def __init__(
        self,
        max_workers: int,
        dispatcher: ExecutionDispatcher,
    ):

        if max_workers <= 0:
            raise ValueError(
                "max_workers must be greater than zero."
            )

        self.max_workers = max_workers
        self.dispatcher = dispatcher

    # ========================================================
    # EXECUTE ONE TASK
    # ========================================================

    def _execute_task(
        self,
        task: ExecutionTask,
        attempt_number: int = 1,
    ) -> ExecutionResult:
        """
        Execute one task through the Dispatcher.

        This method runs inside a worker.
        """

        return self.dispatcher.dispatch(
            task,
            attempt_number=attempt_number,
        )

    # ========================================================
    # EXECUTE TASK BATCH
    # ========================================================

    def execute(
        self,
        tasks: list[ExecutionTask] | tuple[ExecutionTask, ...],
        attempt_numbers: dict[str, int] | None = None,
    ) -> list[ExecutionResult]:
        """
        Execute a batch of tasks concurrently.

        Each task is dispatched independently.

        Results are returned to the caller.
        Runtime state is NOT modified here.
        """

        if not tasks:
            return []

        attempt_numbers = (
            attempt_numbers
            if attempt_numbers is not None
            else {}
        )

        worker_count = min(
            self.max_workers,
            len(tasks),
        )

        results = []

        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="execution-worker",
        ) as executor:

            future_to_task = {}

            for task in tasks:

                attempt_number = (
                    attempt_numbers.get(
                        task.task_id,
                        1,
                    )
                )

                future = executor.submit(
                    self._execute_task,
                    task,
                    attempt_number,
                )

                future_to_task[
                    future
                ] = task

            for future in as_completed(
                future_to_task
            ):

                task = future_to_task[
                    future
                ]

                try:

                    result = future.result()

                except Exception as exc:

                    # The Dispatcher normally converts
                    # comparator exceptions into ExecutionResult.
                    #
                    # This catches unexpected worker-level
                    # failures so one worker cannot crash
                    # the entire pool.

                    result = (
                        self._build_worker_failure_result(
                            task,
                            attempt_numbers.get(
                                task.task_id,
                                1,
                            ),
                            exc,
                        )
                    )

                results.append(result)

        return results

    # ========================================================
    # WORKER FAILURE RESULT
    # ========================================================

    def _build_worker_failure_result(
        self,
        task: ExecutionTask,
        attempt_number: int,
        exc: Exception,
    ) -> ExecutionResult:
        """
        Convert an unexpected worker failure into
        a structured ExecutionResult.

        This keeps the execution contract intact.
        """

        from datetime import datetime, timezone

        started_at = datetime.now(
            timezone.utc
        )

        finished_at = datetime.now(
            timezone.utc
        )

        return ExecutionResult(
            task_id=task.task_id,
            comparator_name=task.comparator_name,
            status="FAILED",
            attempt_number=attempt_number,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=0,
            execution_mode=task.execution_mode,
            execution_location=None,
            metrics={},
            evidence={},
            error=str(exc),
        )