from __future__ import annotations
from sqlalchemy import delete, func, select

from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.persistence.base import PersistenceRepository
from app.persistence.models import (
    Base,
    ComparisonEvidenceItemModel,
    ComparisonResultModel,
    ComparisonRunModel,
    L7AnalysisReportModel,
    ConfigurationModel,
    ConnectionModel,
    ExecutionPlanModel,
    TaskExecutionModel,
    RuleModel,
)


class PostgresRepository(PersistenceRepository):
    """
    PostgreSQL implementation of the persistence repository.

    The rest of the application depends on the
    PersistenceRepository interface and does not need
    to know that PostgreSQL is being used.
    """

    def __init__(
        self,
        database_url: str,
    ) -> None:

        self.engine = create_engine(
            database_url,
            pool_pre_ping=True,
        )

    # ====================================================
    # DATABASE INITIALIZATION
    # ====================================================

    def create_tables(self) -> None:
        """
        Create persistence tables if they do not exist.
        """

        Base.metadata.create_all(
            self.engine
        )

    # ====================================================
    # CONNECTIONS
    # ====================================================

    def save_connection(
        self,
        name: str,
        connector_type: str,
        properties: dict[str, Any],
        status: str,
    ) -> int:

        with Session(self.engine) as session:

            connection = ConnectionModel(
                name=name,
                connector_type=connector_type.strip().lower(),
                properties=properties,
                status=status,
            )

            session.add(connection)

            session.commit()

            session.refresh(connection)

            return connection.connection_id


    def get_connections(
        self,
    ) -> list[dict[str, Any]]:

        with Session(self.engine) as session:

            connections = session.scalars(
                select(ConnectionModel)
                .order_by(
                    ConnectionModel.created_at.desc()
                )
            ).all()

            return [
                {
                    "connection_id": (
                        connection.connection_id
                    ),
                    "name": connection.name,
                    "connector_type": (
                        connection.connector_type
                    ),
                    "properties": connection.properties,
                    "status": connection.status,
                    "created_at": (
                        connection.created_at
                    ),
                    "updated_at": (
                        connection.updated_at
                    ),
                }
                for connection in connections
            ]


    def get_connection(
        self,
        connection_id: int,
    ) -> dict[str, Any] | None:

        with Session(self.engine) as session:

            connection = session.get(
                ConnectionModel,
                connection_id,
            )

            if connection is None:
                return None

            return {
                "connection_id": (
                    connection.connection_id
                ),
                "name": connection.name,
                "connector_type": (
                    connection.connector_type
                ),
                "properties": connection.properties,
                "status": connection.status,
                "created_at": (
                    connection.created_at
                ),
                "updated_at": (
                    connection.updated_at
                ),
            }


    def update_connection_status(
        self,
        connection_id: int,
        status: str,
    ) -> None:

        with Session(self.engine) as session:

            connection = session.get(
                ConnectionModel,
                connection_id,
            )

            if connection is None:
                raise ValueError(
                    f"Connection not found: "
                    f"{connection_id}"
                )

            connection.status = status

            session.commit()


    def delete_connection(
        self,
        connection_id: int,
    ) -> None:

        with Session(self.engine) as session:

            connection = session.get(
                ConnectionModel,
                connection_id,
            )

            if connection is None:
                raise ValueError(
                    f"Connection not found: "
                    f"{connection_id}"
                )

            session.delete(connection)

            session.commit()

    # ====================================================
    # HELPERS
    # ====================================================

    @staticmethod
    def _to_datetime(
        value: str | datetime | None,
    ) -> datetime | None:

        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

    # ====================================================
    # CONFIGURATION
    # ====================================================

    def save_configuration(
        self,
        configuration: dict[str, Any],
        configuration_id: int | None = None,
    ) -> int:

        with Session(self.engine) as session:

            if configuration_id is not None:
                existing = session.get(
                    ConfigurationModel,
                    configuration_id,
                )

                if existing is None:
                    model = ConfigurationModel(
                        configuration_id=configuration_id,
                        configuration=configuration,
                    )
                    session.add(model)
                    session.commit()
                    return configuration_id
                else:
                    existing.configuration = configuration
                    session.commit()
                    return configuration_id
            else:
                model = ConfigurationModel(
                    configuration=configuration,
                )
                session.add(model)
                session.commit()
                session.refresh(model)
                return model.configuration_id

    def get_configuration(
        self,
        configuration_id: int,
    ) -> dict[str, Any] | None:

        with Session(self.engine) as session:

            model = session.get(
                ConfigurationModel,
                configuration_id,
            )

            if model is None:
                return None

            return {
                "configuration_id": (
                    model.configuration_id
                ),
                "configuration": model.configuration,
                "created_at": (
                    model.created_at.isoformat()
                    if model.created_at
                    else None
                ),
            }        

    # ====================================================
    # EXECUTION PLAN
    # ====================================================

    def save_execution_plan(
        self,
        plan_id: str,
        configuration_id: int,
        planner_version: str,
        plan: dict[str, Any],
    ) -> None:

        with Session(
            self.engine
        ) as session:

            existing = session.get(
                ExecutionPlanModel,
                plan_id,
            )

            if existing is None:

                session.add(
                    ExecutionPlanModel(
                        plan_id=plan_id,
                        configuration_id=(
                            configuration_id
                        ),
                        planner_version=(
                            planner_version
                        ),
                        plan=plan,
                    )
                )

            else:

                existing.configuration_id = (
                    configuration_id
                )

                existing.planner_version = (
                    planner_version
                )

                existing.plan = plan

            session.commit()

    # ====================================================
    # COMPARISON RUN
    # ====================================================

    def create_run(
        self,
        run_id: str,
        plan_id: str,
        configuration_id: int,
        status: str,
        started_at: str,
    ) -> None:

        with Session(
            self.engine
        ) as session:

            existing = session.get(
                ComparisonRunModel,
                run_id,
            )

            if existing is not None:
                raise ValueError(
                    f"Run already exists: {run_id}"
                )

            session.add(
                ComparisonRunModel(
                    run_id=run_id,
                    plan_id=plan_id,
                    configuration_id=(
                        configuration_id
                    ),
                    status=status,
                    started_at=self._to_datetime(
                        started_at
                    ),
                )
            )

            session.commit()

    def complete_run(
        self,
        run_id: str,
        status: str,
        comparison_status: str | None,
        finished_at: str,
    ) -> None:

        with Session(
            self.engine
        ) as session:

            run = session.get(
                ComparisonRunModel,
                run_id,
            )

            if run is None:
                raise ValueError(
                    f"Run not found: {run_id}"
                )

            run.status = status

            run.comparison_status = (
                comparison_status
            )

            run.finished_at = (
                self._to_datetime(
                    finished_at
                )
            )

            session.commit()

    # ====================================================
    # L7 ANALYSIS REPORTS
    # ====================================================

    def save_l7_analysis_report(
        self,
        run_id: str,
        report: dict[str, Any] | None,
        error: str | None = None,
    ) -> None:
        with Session(self.engine) as session:
            model = session.get(L7AnalysisReportModel, run_id)
            if model is None:
                session.add(L7AnalysisReportModel(
                    run_id=run_id,
                    report=report,
                    error=error,
                ))
            else:
                model.report = report
                model.error = error
            session.commit()

    def get_l7_analysis_report(self, run_id: str) -> dict[str, Any] | None:
        with Session(self.engine) as session:
            model = session.get(L7AnalysisReportModel, run_id)
            if model is None:
                return None
            return model.report or ({"error": model.error} if model.error else None)

    # ====================================================
    # TASK EXECUTION
    # ====================================================

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

        with Session(
            self.engine
        ) as session:

            existing = session.scalar(
                select(
                    TaskExecutionModel
                ).where(
                    TaskExecutionModel.run_id
                    == run_id,
                    TaskExecutionModel.task_id
                    == task_id,
                )
            )

            if existing is None:

                session.add(
                    TaskExecutionModel(
                        run_id=run_id,
                        task_id=task_id,
                        comparison_level=(
                            comparison_level
                        ),
                        comparator_name=(
                            comparator_name
                        ),
                        execution_mode=(
                            execution_mode
                        ),
                        status=status,
                        started_at=(
                            self._to_datetime(
                                started_at
                            )
                        ),
                        finished_at=(
                            self._to_datetime(
                                finished_at
                            )
                        ),
                        error=error,
                    )
                )

            else:

                existing.comparison_level = (
                    comparison_level
                )

                existing.comparator_name = (
                    comparator_name
                )

                existing.execution_mode = (
                    execution_mode
                )

                existing.status = status

                existing.started_at = (
                    self._to_datetime(
                        started_at
                    )
                )

                existing.finished_at = (
                    self._to_datetime(
                        finished_at
                    )
                )

                existing.error = error

            session.commit()

    # ====================================================
    # COMPARISON RESULT
    # ====================================================

    def save_comparison_result(
        self,
        run_id: str,
        task_id: str,
        metrics: dict[str, Any],
        evidence: dict[str, Any],
    ) -> int:

        with Session(
            self.engine
        ) as session:

            result = ComparisonResultModel(
                run_id=run_id,
                task_id=task_id,
                metrics=metrics,
                evidence=evidence,
            )

            session.add(result)

            session.commit()

            session.refresh(result)

            return result.result_id

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

        if batch_size <= 0:
            raise ValueError(
                "batch_size must be greater than zero"
            )

        written = 0
        batch: list[ComparisonEvidenceItemModel] = []

        with Session(
            self.engine
        ) as session:

            for index, item in enumerate(items):
                payload = item.get(
                    "payload",
                    item,
                )

                ordinal = item.get(
                    "ordinal",
                    index,
                )

                batch.append(
                    ComparisonEvidenceItemModel(
                        result_id=result_id,
                        run_id=run_id,
                        task_id=task_id,
                        comparison_level=(
                            comparison_level
                        ),
                        evidence_type=evidence_type,
                        entity_key=item.get(
                            "entity_key"
                        ),
                        source_field=item.get(
                            "source_field"
                        ),
                        target_field=item.get(
                            "target_field"
                        ),
                        ordinal=int(ordinal),
                        payload=payload,
                    )
                )

                if len(batch) >= batch_size:
                    session.add_all(batch)
                    session.commit()
                    written += len(batch)
                    batch = []

            if batch:
                session.add_all(batch)
                session.commit()
                written += len(batch)

        return written

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

        if not run_id:
            raise ValueError(
                "run_id is required"
            )

        if limit <= 0:
            raise ValueError(
                "limit must be greater than zero"
            )

        if offset < 0:
            raise ValueError(
                "offset must be greater than or equal to zero"
            )

        statement = select(
            ComparisonEvidenceItemModel
        ).where(
            ComparisonEvidenceItemModel.run_id
            == run_id
        )

        if result_id is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.result_id
                == result_id
            )

        if task_id is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.task_id
                == task_id
            )

        if comparison_level is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.comparison_level
                == comparison_level
            )

        if evidence_type is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.evidence_type
                == evidence_type
            )

        if source_field is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.source_field
                == source_field
            )

        if target_field is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.target_field
                == target_field
            )

        statement = statement.order_by(
            ComparisonEvidenceItemModel.ordinal.asc(),
            ComparisonEvidenceItemModel.evidence_item_id.asc(),
        ).limit(
            limit
        ).offset(
            offset
        )

        with Session(
            self.engine
        ) as session:

            items = session.scalars(
                statement
            ).all()

            return [
                {
                    "evidence_item_id": (
                        item.evidence_item_id
                    ),
                    "result_id": item.result_id,
                    "run_id": item.run_id,
                    "task_id": item.task_id,
                    "comparison_level": (
                        item.comparison_level
                    ),
                    "evidence_type": (
                        item.evidence_type
                    ),
                    "entity_key": item.entity_key,
                    "source_field": item.source_field,
                    "target_field": item.target_field,
                    "ordinal": item.ordinal,
                    "payload": item.payload,
                    "created_at": (
                        item.created_at.isoformat()
                        if item.created_at
                        else None
                    ),
                }
                for item in items
            ]

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

        if not run_id:
            raise ValueError(
                "run_id is required"
            )

        statement = select(
            func.count(
                ComparisonEvidenceItemModel.evidence_item_id
            )
        ).where(
            ComparisonEvidenceItemModel.run_id
            == run_id
        )

        if result_id is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.result_id
                == result_id
            )

        if task_id is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.task_id
                == task_id
            )

        if comparison_level is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.comparison_level
                == comparison_level
            )

        if evidence_type is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.evidence_type
                == evidence_type
            )

        if source_field is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.source_field
                == source_field
            )

        if target_field is not None:
            statement = statement.where(
                ComparisonEvidenceItemModel.target_field
                == target_field
            )

        with Session(
            self.engine
        ) as session:

            return int(
                session.scalar(statement) or 0
            )

    def update_comparison_result(
        self,
        result_id: int,
        metrics: dict[str, Any],
        evidence: dict[str, Any],
    ) -> None:

        with Session(
            self.engine
        ) as session:

            result = session.get(
                ComparisonResultModel,
                result_id,
            )

            if result is None:
                raise ValueError(
                    f"Comparison result not found: "
                    f"{result_id}"
                )

            result.metrics = metrics
            result.evidence = evidence

            session.commit()

    # ====================================================
    # READ RUN
    # ====================================================

    def run_exists(
        self,
        run_id: str,
    ) -> bool:

        with Session(
            self.engine
        ) as session:

            return (
                session.get(
                    ComparisonRunModel,
                    run_id,
                )
                is not None
            )

    def get_latest_successful_result_for_level(
        self,
        *,
        run_id: str,
        comparison_level: str,
        task_id: str | None = None,
    ) -> dict[str, Any] | None:

        statement = (
            select(
                ComparisonResultModel.result_id,
                ComparisonResultModel.task_id,
                ComparisonResultModel.metrics,
                ComparisonResultModel.created_at,
            )
            .join(
                TaskExecutionModel,
                (
                    TaskExecutionModel.run_id
                    == ComparisonResultModel.run_id
                )
                & (
                    TaskExecutionModel.task_id
                    == ComparisonResultModel.task_id
                ),
            )
            .where(
                ComparisonResultModel.run_id
                == run_id,
                TaskExecutionModel.comparison_level
                == comparison_level,
                TaskExecutionModel.status
                == "COMPLETED",
            )
            .order_by(
                ComparisonResultModel.result_id.desc()
            )
        )

        if task_id is not None:
            statement = statement.where(
                ComparisonResultModel.task_id
                == task_id
            )

        with Session(
            self.engine
        ) as session:

            rows = session.execute(
                statement
            ).all()

            for row in rows:
                metrics = row.metrics or {}
                status = metrics.get("status")

                if status in {
                    "FAILED",
                    "RUNNING",
                }:
                    continue

                return {
                    "result_id": row.result_id,
                    "task_id": row.task_id,
                    "created_at": (
                        row.created_at.isoformat()
                        if row.created_at
                        else None
                    ),
                }

            return None

    def get_run(
        self,
        run_id: str,
    ) -> dict[str, Any] | None:

        with Session(
            self.engine
        ) as session:

            run = session.get(
                ComparisonRunModel,
                run_id,
            )

            if run is None:
                return None

            plan = session.get(
                ExecutionPlanModel,
                run.plan_id,
            )

            tasks = session.scalars(
                select(
                    TaskExecutionModel
                ).where(
                    TaskExecutionModel.run_id
                    == run_id
                ).order_by(
                    TaskExecutionModel.task_execution_id
                )
            ).all()

            results = session.scalars(
                select(
                    ComparisonResultModel
                ).where(
                    ComparisonResultModel.run_id
                    == run_id
                ).order_by(
                    ComparisonResultModel.result_id
                )
            ).all()

            l7_analysis = session.get(L7AnalysisReportModel, run_id)

            return {
                "run_id": run.run_id,
                "plan_id": run.plan_id,
                "configuration_id": (
                    run.configuration_id
                ),
                "status": run.status,
                "comparison_status": (
                    run.comparison_status
                ),
                "analysis": (
                    l7_analysis.report
                    if l7_analysis and l7_analysis.report
                    else {"error": l7_analysis.error}
                    if l7_analysis and l7_analysis.error
                    else None
                ),
                "plan": plan.plan if plan is not None else None,
                "started_at": (
                    run.started_at.isoformat()
                    if run.started_at
                    else None
                ),
                "finished_at": (
                    run.finished_at.isoformat()
                    if run.finished_at
                    else None
                ),
                "tasks": [
                    {
                        "task_execution_id": (
                            task.task_execution_id
                        ),
                        "task_id": task.task_id,
                        "comparison_level": (
                            task.comparison_level
                        ),
                        "comparator_name": (
                            task.comparator_name
                        ),
                        "execution_mode": (
                            task.execution_mode
                        ),
                        "status": task.status,
                        "started_at": (
                            task.started_at.isoformat()
                            if task.started_at
                            else None
                        ),
                        "finished_at": (
                            task.finished_at.isoformat()
                            if task.finished_at
                            else None
                        ),
                        "error": task.error,
                    }
                    for task in tasks
                ],
                "results": [
                    {
                        "result_id": (
                            result.result_id
                        ),
                        "task_id": result.task_id,
                        "metrics": result.metrics,
                        "evidence": result.evidence,
                        "created_at": (
                            result.created_at.isoformat()
                            if result.created_at
                            else None
                        ),
                    }
                    for result in results
                ],
            }

    # ====================================================
    # READ TASK RESULTS
    # ====================================================

    def get_task_results(
        self,
        run_id: str,
    ) -> list[dict[str, Any]]:

        with Session(
            self.engine
        ) as session:

            results = session.scalars(
                select(
                    ComparisonResultModel
                ).where(
                    ComparisonResultModel.run_id
                    == run_id
                ).order_by(
                    ComparisonResultModel.result_id
                )
            ).all()

            return [
                {
                    "result_id": result.result_id,
                    "task_id": result.task_id,
                    "metrics": result.metrics,
                    "evidence": result.evidence,
                    "created_at": (
                        result.created_at.isoformat()
                        if result.created_at
                        else None
                    ),
                }
                for result in results
            ]

    # ====================================================
    # HEALTH CHECK
    # ====================================================

    def health_check(self) -> bool:

        with self.engine.connect() as connection:

            connection.exec_driver_sql(
                "SELECT 1"
            )

        return True

    # ====================================================
    # LIST AND DELETE RUNS
    # ====================================================

    def list_runs(self) -> list[dict[str, Any]]:
        with Session(self.engine) as session:
            stmt = select(ComparisonRunModel).order_by(ComparisonRunModel.created_at.desc())
            runs = session.scalars(stmt).all()

            results = []
            for r in runs:
                results.append({
                    "run_id": r.run_id,
                    "plan_id": r.plan_id,
                    "configuration_id": r.configuration_id,
                    "status": r.status,
                    "comparison_status": r.comparison_status,
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                    "created_at": r.created_at.isoformat(),
                })
            return results

    def delete_run(self, run_id: str) -> None:
        with Session(self.engine) as session:
            run = session.get(ComparisonRunModel, run_id)

            if run is None:
                raise ValueError(f"Run not found: {run_id}")

            try:
                # ------------------------------------------------
                # DELETE CHILD RECORDS FIRST
                # ------------------------------------------------

                session.execute(
                    delete(
                        ComparisonEvidenceItemModel
                    ).where(
                        ComparisonEvidenceItemModel.run_id
                        == run_id
                    )
                )

                session.execute(
                    delete(ComparisonResultModel).where(
                        ComparisonResultModel.run_id == run_id
                    )
                )

                session.execute(
                    delete(L7AnalysisReportModel).where(
                        L7AnalysisReportModel.run_id == run_id
                    )
                )

                session.execute(
                    delete(TaskExecutionModel).where(
                        TaskExecutionModel.run_id == run_id
                    )
                )

                # ------------------------------------------------
                # DELETE PARENT RUN
                # ------------------------------------------------

                session.delete(run)

                session.commit()

            except Exception:
                session.rollback()
                raise

    # ====================================================
    # RULES
    # ====================================================

    def save_rule(self, name: str, rule_type: str, payload: dict[str, Any]) -> int:
        with Session(self.engine) as session:
            model = RuleModel(
                name=name,
                rule_type=rule_type,
                payload=payload,
            )
            session.add(model)
            session.commit()
            session.refresh(model)
            return model.rule_id

    def get_rules(self) -> list[dict[str, Any]]:
        with Session(self.engine) as session:
            stmt = select(RuleModel).order_by(RuleModel.created_at.desc())
            rules = session.scalars(stmt).all()
            
            results = []
            for r in rules:
                results.append({
                    "rule_id": r.rule_id,
                    "name": r.name,
                    "rule_type": r.rule_type,
                    "payload": r.payload,
                    "created_at": r.created_at.isoformat(),
                })
            return results

    def update_rule(self, rule_id: int, name: str, rule_type: str, payload: dict[str, Any]) -> None:
        with Session(self.engine) as session:
            rule = session.get(RuleModel, rule_id)
            if rule is not None:
                rule.name = name
                rule.rule_type = rule_type
                rule.payload = payload
                session.commit()

    def delete_rule(self, rule_id: int) -> None:
        with Session(self.engine) as session:
            rule = session.get(RuleModel, rule_id)
            if rule is not None:
                session.delete(rule)
                session.commit()

    # ====================================================
    # SHUTDOWN
    # ====================================================

    def close(self) -> None:

        self.engine.dispose()
