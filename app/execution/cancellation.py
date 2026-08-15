from app.execution.models import (
    ExecutionTask,
    ExecutionStatus,
    RuntimeQueue,
)


class CancellationController:
    """
    Controls cancellation of an execution run.

    Responsibilities:
        - Accept a cancellation request.
        - Stop new work from being scheduled.
        - Mark waiting/ready tasks as CANCELLED.
        - Report cancellation state.

    It does NOT:
        - Force-kill running Python threads.
        - Execute tasks.
        - Retry tasks.
        - Collect execution results.
    """

    def __init__(self):
        self._cancel_requested = False

    @property
    def is_cancel_requested(self) -> bool:
        """
        Return whether cancellation has been requested.
        """

        return self._cancel_requested

    def request_cancel(self) -> None:
        """
        Request cancellation of the current execution.
        """

        self._cancel_requested = True

    def reset(self) -> None:
        """
        Reset cancellation state for a new execution.
        """

        self._cancel_requested = False

    def cancel_pending_tasks(
        self,
        runtime_queue: RuntimeQueue,
    ) -> list[str]:
        """
        Cancel tasks that have not started execution.

        RUNNING tasks are intentionally not modified here.
        """

        if not self._cancel_requested:
            return []

        cancelled_ids = []

        # ----------------------------------------------------
        # Cancel READY tasks
        # ----------------------------------------------------

        for task in list(
            runtime_queue.ready_tasks
        ):

            state = runtime_queue.task_states[
                task.task_id
            ]

            state.status = (
                ExecutionStatus.CANCELLED
            )

            cancelled_ids.append(
                task.task_id
            )

        # ----------------------------------------------------
        # Cancel WAITING tasks
        # ----------------------------------------------------

        for task in list(
            runtime_queue.waiting_tasks
        ):

            state = runtime_queue.task_states[
                task.task_id
            ]

            state.status = (
                ExecutionStatus.CANCELLED
            )

            cancelled_ids.append(
                task.task_id
            )

        # ----------------------------------------------------
        # Remove cancelled tasks from queues
        # ----------------------------------------------------

        cancelled_set = set(
            cancelled_ids
        )

        runtime_queue.ready_tasks = [
            task
            for task in runtime_queue.ready_tasks
            if task.task_id
            not in cancelled_set
        ]

        runtime_queue.waiting_tasks = [
            task
            for task in runtime_queue.waiting_tasks
            if task.task_id
            not in cancelled_set
        ]

        return cancelled_ids

    def is_running_task(
        self,
        task: ExecutionTask,
        runtime_queue: RuntimeQueue,
    ) -> bool:
        """
        Check whether a task is currently running.
        """

        return any(
            running_task.task_id
            == task.task_id
            for running_task
            in runtime_queue.running_tasks
        )