from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable


class PersistenceRepository(ABC):
    """
    Abstract persistence contract.

    The execution engine depends on this interface,
    not on SQLite/PostgreSQL-specific implementation.
    """

    # ====================================================
    # CONNECTIONS
    # ====================================================

    @abstractmethod
    def save_configuration(
        self,
        configuration: dict[str, Any],
        configuration_id: int | None = None,
    ) -> int:
        pass

    @abstractmethod
    def get_configuration(
        self,
        configuration_id: int,
    ) -> dict[str, Any] | None:
        pass

    @abstractmethod
    def get_connection(
        self,
        connection_id: int,
    ) -> dict[str, Any] | None:
        pass

    @abstractmethod
    def update_connection_status(
        self,
        connection_id: int,
        status: str,
    ) -> None:
        pass

    @abstractmethod
    def delete_connection(
        self,
        connection_id: int,
    ) -> None:
        pass

    # ====================================================
    # EXECUTION PLAN
    # ====================================================

    @abstractmethod
    def save_execution_plan(
        self,
        plan_id: str,
        configuration_id: int,
        planner_version: str,
        plan: dict[str, Any],
    ) -> None:
        pass

    # ====================================================
    # COMPARISON RUN
    # ====================================================

    @abstractmethod
    def create_run(
        self,
        run_id: str,
        plan_id: str,
        configuration_id: int,
        status: str,
        started_at: str,
    ) -> None:
        pass

    @abstractmethod
    def complete_run(
        self,
        run_id: str,
        status: str,
        comparison_status: str | None,
        finished_at: str,
    ) -> None:
        pass

    # ====================================================
    # TASK EXECUTION
    # ====================================================

    @abstractmethod
    def save_task_execution(
        self,
        run_id: str,
        task_id: str,
        comparison_level: str,
        comparator_name: str,
        execution_mode: str,
        status: str,
        started_at: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
    ) -> None:
        pass

    # ====================================================
    # COMPARISON RESULT
    # ====================================================

    @abstractmethod
    def save_comparison_result(
        self,
        run_id: str,
        task_id: str,
        metrics: dict[str, Any],
        evidence: dict[str, Any],
    ) -> int:
        pass

    @abstractmethod
    def write_evidence_items(
        self,
        *,
        result_id: int,
        run_id: str,
        task_id: str,
        comparison_level: str,
        evidence_type: str,
        items: Iterable[dict[str, Any]],
        batch_size: int = 1000,
    ) -> int:
        pass

    @abstractmethod
    def list_evidence_items(
        self,
        *,
        run_id: str,
        result_id: int | None = None,
        task_id: str | None = None,
        comparison_level: str | None = None,
        evidence_type: str | None = None,
        source_field: str | None = None,
        target_field: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    def count_evidence_items(
        self,
        *,
        run_id: str,
        result_id: int | None = None,
        task_id: str | None = None,
        comparison_level: str | None = None,
        evidence_type: str | None = None,
        source_field: str | None = None,
        target_field: str | None = None,
    ) -> int:
        pass

    @abstractmethod
    def update_comparison_result(
        self,
        result_id: int,
        metrics: dict[str, Any],
        evidence: dict[str, Any],
    ) -> None:
        pass

    # ====================================================
    # READ
    # ====================================================

    @abstractmethod
    def get_run(
        self,
        run_id: str,
    ) -> dict[str, Any] | None:
        pass

    @abstractmethod
    def run_exists(
        self,
        run_id: str,
    ) -> bool:
        pass

    @abstractmethod
    def get_latest_successful_result_for_level(
        self,
        *,
        run_id: str,
        comparison_level: str,
        task_id: str | None = None,
    ) -> dict[str, Any] | None:
        pass

    @abstractmethod
    def get_task_results(
        self,
        run_id: str,
    ) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    def list_runs(
        self,
    ) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    def delete_run(
        self,
        run_id: str,
    ) -> None:
        pass

    # ====================================================
    # RULES
    # ====================================================

    @abstractmethod
    def save_rule(
        self,
        name: str,
        rule_type: str,
        payload: dict[str, Any],
    ) -> int:
        pass

    @abstractmethod
    def get_rules(
        self,
    ) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    def update_rule(
        self,
        rule_id: int,
        name: str,
        rule_type: str,
        payload: dict[str, Any],
    ) -> None:
        pass

    @abstractmethod
    def delete_rule(
        self,
        rule_id: int,
    ) -> None:
        pass
