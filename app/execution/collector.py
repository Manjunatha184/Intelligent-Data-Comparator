from datetime import datetime, timezone

from app.execution.models import (
    ExecutionTask,
    RuntimeQueue,
    ExecutionStatus,
    ExecutionResult,
    ExecutionResultStatus,
)


class ExecutionCollector:
    """
    Collects structured execution results
    and updates runtime state.
    """

    def collect(
        self,
        task: ExecutionTask,
        result: ExecutionResult,
        runtime_queue: RuntimeQueue,
    ) -> None:

        state = runtime_queue.task_states[
            task.task_id
        ]

        runtime_queue.running_tasks = [
            running_task
            for running_task
            in runtime_queue.running_tasks
            if running_task.task_id
            != task.task_id
        ]

        state.finished_at = (
            result.finished_at
            or datetime.now(timezone.utc)
        )

        state.result = result

        state.results.append(result)

        if (
            result.status
            == ExecutionResultStatus.SUCCESS
        ):

            state.status = (
                ExecutionStatus.COMPLETED
            )

            if (
                task
                not in runtime_queue.completed_tasks
            ):
                runtime_queue.completed_tasks.append(
                    task
                )

        elif (
            result.status
            == ExecutionResultStatus.SKIPPED
        ):

            state.status = (
                ExecutionStatus.SKIPPED
            )

        else:

            state.status = (
                ExecutionStatus.FAILED
            )

            state.last_error = result.error

            if (
                task
                not in runtime_queue.failed_tasks
            ):
                runtime_queue.failed_tasks.append(
                    task
                )
