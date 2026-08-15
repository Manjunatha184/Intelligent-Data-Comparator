from concurrent.futures import Future, TimeoutError

from app.execution.models import (
    ExecutionTask,
    ExecutionResult,
    ExecutionResultStatus,
)


class TimeoutManager:
    """
    Controls task execution timeout decisions.

    Responsibilities:
        - Apply the configured task timeout.
        - Detect when a task exceeds its timeout.
        - Convert timeout events into ExecutionResult.

    It does NOT:
        - Retry tasks.
        - Modify RuntimeQueue.
        - Collect results.
        - Decide execution strategy.
    """

    def __init__(
        self,
        timeout_seconds: int,
    ):
        if timeout_seconds <= 0:
            raise ValueError(
                "timeout_seconds must be greater than zero."
            )

        self.timeout_seconds = timeout_seconds

    def wait_for_result(
        self,
        future: Future,
        task: ExecutionTask,
        attempt_number: int,
    ) -> ExecutionResult:
        """
        Wait for a worker Future up to the configured timeout.

        A timeout is represented as a FAILED ExecutionResult.
        RetryManager can therefore handle it using the existing
        retry contract.
        """

        try:
            result = future.result(
                timeout=self.timeout_seconds
            )

            return result

        except TimeoutError:

            future.cancel()

            return self.build_timeout_result(
                task=task,
                attempt_number=attempt_number,
            )

    def build_timeout_result(
        self,
        task: ExecutionTask,
        attempt_number: int,
    ) -> ExecutionResult:
        """
        Build a structured timeout result.
        """

        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)

        return ExecutionResult(
            task_id=task.task_id,
            comparator_name=task.comparator_name,
            status=ExecutionResultStatus.FAILED,
            attempt_number=attempt_number,
            started_at=now,
            finished_at=now,
            duration_ms=(
                self.timeout_seconds * 1000
            ),
            execution_mode=task.execution_mode,
            execution_location=None,
            metrics={
                "timeout": True,
                "timeout_seconds": (
                    self.timeout_seconds
                ),
            },
            evidence={},
            error=(
                f"Task exceeded execution timeout "
                f"of {self.timeout_seconds} seconds."
            ),
        )