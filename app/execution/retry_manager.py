from app.execution.models import (
    ExecutionTask,
    ExecutionResult,
    ExecutionResultStatus,
    RuntimeTaskState,
)


class RetryManager:
    """
    Controls retry decisions for failed execution tasks.

    Responsibilities:
        - Determine whether a failed task can be retried.
        - Track remaining attempts.
        - Record execution results in runtime task state.

    Does NOT:
        - Execute tasks.
        - Dispatch comparators.
        - Modify RuntimeQueue collections.
        - Decide execution strategy.
    """

    def should_retry(
        self,
        task: ExecutionTask,
        result: ExecutionResult,
        state: RuntimeTaskState,
    ) -> bool:
        """
        Determine whether the task should be retried.
        """

        # Successful tasks are never retried.
        if result.status == ExecutionResultStatus.SUCCESS:
            return False

        # Skipped tasks are never retried.
        if result.status == ExecutionResultStatus.SKIPPED:
            return False

        # Only failed tasks are retry candidates.
        if result.status != ExecutionResultStatus.FAILED:
            return False

        # Maximum attempts reached.
        if state.attempt_count >= state.max_attempts:
            return False

        return True

    def attempts_remaining(
        self,
        state: RuntimeTaskState,
    ) -> int:
        """
        Return the number of attempts still available.
        """

        return max(
            state.max_attempts - state.attempt_count,
            0,
        )

    def record_result(
        self,
        state: RuntimeTaskState,
        result: ExecutionResult,
    ) -> None:
        """
        Store the latest execution result in runtime state.
        """

        state.result = result

        state.results.append(result)

        state.last_error = result.error

        if result.finished_at is not None:
            state.finished_at = result.finished_at