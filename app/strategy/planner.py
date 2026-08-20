import os
import logging
from datetime import datetime, timezone

from uuid import uuid4

from app.domain.context import (
    InputAnalysis,
    RuntimeConfiguration,
    StrategyDecision,
    LevelStrategy,
    StrategyPolicy,
)

from app.execution.models import (
    ComparisonLevel,
    ExecutionLocation,
    ExecutionMode,
    ExecutionTask,
    ExecutionGroup,
    ExecutionRules,
    ExecutionMetadata,
    ExecutionEstimate,
    ExecutionPlan,
    DependencyGraph,
    Priority,
)

from app.strategy.analyzers import get_dataset_analyzer


logger = logging.getLogger(__name__)

class StrategyPlanner:
    """
    Builds the execution plan for a comparison run.
    """

    def __init__(self, connector_manager=None) -> None:
        self.connector_manager = connector_manager

    def analyze_inputs(
        self,
        configuration: RuntimeConfiguration,
    ) -> InputAnalysis:

        source_analyzer = get_dataset_analyzer(
            configuration.source.connector_type
        )

        target_analyzer = get_dataset_analyzer(
            configuration.target.connector_type
        )

        source_metadata = source_analyzer.analyze(
            configuration.source.connector_type,
            configuration.source.properties,
        )

        target_metadata = target_analyzer.analyze(
            configuration.target.connector_type,
            configuration.target.properties,
        )

        source_metadata = self._resolve_remote_row_count(
            configuration.source.connector_type,
            configuration.source.model_dump(mode="python"),
            configuration.source_filters,
            source_metadata,
        )
        target_metadata = self._resolve_remote_row_count(
            configuration.target.connector_type,
            configuration.target.model_dump(mode="python"),
            configuration.target_filters,
            target_metadata,
        )

        self._validate_filter_fields(
            configuration.source_filters,
            source_metadata.get("columns", []),
            "source",
        )
        self._validate_filter_fields(
            configuration.target_filters,
            target_metadata.get("columns", []),
            "target",
        )

        platform_capabilities = self._analyze_platform_capabilities(
            configuration.source.connector_type,
            configuration.target.connector_type,
        )

        return InputAnalysis(
            source_metadata=source_metadata,
            target_metadata=target_metadata,
            source_row_count=source_metadata["row_count"],
            target_row_count=target_metadata["row_count"],
            platform_capabilities=platform_capabilities,
            mappings=configuration.column_mappings,
            dq_rules=configuration.dq_rules,
        )

    def _resolve_remote_row_count(
        self,
        connector_type,
        dataset,
        filters,
        metadata,
    ):
        """Get an exact filtered count before routing remote data locally."""
        if str(connector_type).lower() != "databricks":
            return metadata
        if self.connector_manager is None:
            return metadata

        try:
            statistics = self.connector_manager.get_volume_statistics(
                "databricks",
                dataset,
                business_keys=[],
                filters=[item.model_dump(mode="python") for item in filters],
            )
        except Exception:
            logger.warning(
                "Unable to determine Databricks row count; routing to Spark",
                exc_info=True,
            )
            metadata["row_count"] = None
            metadata["row_count_known"] = False
            return metadata

        metadata["row_count"] = int(statistics.get("filtered_rows", 0))
        metadata["row_count_known"] = True
        return metadata

    @staticmethod
    def _validate_filter_fields(filters, columns, side):
        if not columns:
            return
        known = {column if isinstance(column, str) else column.get("name") for column in columns}
        for item in filters:
            if item.field not in known:
                raise ValueError(f"Unknown {side} filter field: {item.field}")

    def _analyze_platform_capabilities(
        self,
        source_connector: str,
        target_connector: str,
    ) -> dict:

        source_analyzer = get_dataset_analyzer(
            source_connector
        )

        target_analyzer = get_dataset_analyzer(
            target_connector
        )

        source_capabilities = getattr(
            source_analyzer,
            "capabilities",
            {},
        )

        target_capabilities = getattr(
            target_analyzer,
            "capabilities",
            {},
        )

        capability_keys = {
            "supports_hash",
            "supports_sampling",
        }

        return {
            key: (
                source_capabilities.get(key, False)
                and target_capabilities.get(key, False)
            )
            for key in capability_keys
        }


    def choose_strategy(
        self,
        analysis: InputAnalysis,
        comparison_levels: list[ComparisonLevel],
        policy: StrategyPolicy,
    ) -> StrategyDecision:

        capabilities = analysis.platform_capabilities

        total_rows = max(
            analysis.source_row_count or 0,
            analysis.target_row_count or 0,
        )

        source_size = analysis.source_metadata.get(
            "file_size_bytes", 0
        )
        target_size = analysis.target_metadata.get(
            "file_size_bytes", 0
        )

        total_bytes = source_size + target_size

        execution_location = self._choose_execution_location(
            analysis,
            total_rows=total_rows,
            total_bytes=total_bytes,
        )

        strategies = []

        for level in comparison_levels:

            if level == ComparisonLevel.L1:
                mode = ExecutionMode.EXACT

            elif level == ComparisonLevel.L2:
                mode = ExecutionMode.AGGREGATE

            elif level == ComparisonLevel.L3:
                if (
                    total_rows <= policy.max_exact_rows
                    and total_bytes <= policy.max_exact_bytes
                ):
                    mode = ExecutionMode.EXACT

                elif capabilities.get("supports_hash", False):
                    mode = ExecutionMode.HASH

                elif (
                    policy.allow_sampling
                    and total_rows >= policy.sampling_min_rows
                    and capabilities.get("supports_sampling", False)
                ):
                    mode = ExecutionMode.SAMPLED

                else:
                    raise ValueError(
                        "No suitable strategy available for L3"
                    )

            elif level == ComparisonLevel.L4:
                mode = ExecutionMode.EXACT

            elif level == ComparisonLevel.L5:
                mode = ExecutionMode.AGGREGATE

            elif level == ComparisonLevel.L6:
                mode = ExecutionMode.EXACT

            else:
                raise ValueError(
                    f"Unsupported comparison level: {level}"
                )

            strategies.append(
                LevelStrategy(
                    comparison_level=level,
                    comparison_mode=mode,
                    execution_location=execution_location,
                )   
            )

        return StrategyDecision(strategies=strategies)

    @staticmethod
    def _choose_execution_location(
        analysis: InputAnalysis,
        total_rows: int,
        total_bytes: int,
    ) -> ExecutionLocation:
        """Route one complete comparison plan to a single execution engine."""
        duckdb_enabled = os.getenv(
            "DUCKDB_EXECUTION_ENABLED",
            "true",
        ).strip().lower() in {"1", "true", "yes", "on"}
        if not duckdb_enabled:
            return ExecutionLocation.SPARK

        source_connector = str(
            analysis.source_metadata.get("connector_type") or ""
        ).lower()
        target_connector = str(
            analysis.target_metadata.get("connector_type") or ""
        ).lower()
        local_connectors = {"csv", "databricks"}
        if (
            source_connector not in local_connectors
            or target_connector not in local_connectors
        ):
            return ExecutionLocation.SPARK

        metadata_items = (
            analysis.source_metadata,
            analysis.target_metadata,
        )
        uses_databricks = "databricks" in {
            source_connector,
            target_connector,
        }
        if uses_databricks and any(
            str(item.get("connector_type", "")).lower() == "databricks"
            and not item.get("row_count_known", False)
            for item in metadata_items
        ):
            return ExecutionLocation.SPARK

        maximum_rows = int(os.getenv("DUCKDB_MAX_ROWS", "1000000"))
        if uses_databricks:
            maximum_rows = min(
                maximum_rows,
                int(os.getenv("DUCKDB_DATABRICKS_MAX_ROWS", "100000")),
            )
        maximum_bytes = int(
            os.getenv("DUCKDB_MAX_INPUT_BYTES", str(1024 * 1024 * 1024))
        )
        if total_rows <= maximum_rows and total_bytes <= maximum_bytes:
            return ExecutionLocation.DUCKDB
        return ExecutionLocation.SPARK

    def build_execution_tasks(
        self,
        configuration: RuntimeConfiguration,
        decision: StrategyDecision,
    ) -> list[ExecutionTask]:

        comparator_map = {
            ComparisonLevel.L1: "SchemaComparator",
            ComparisonLevel.L2: "VolumeComparator",
            ComparisonLevel.L3: "RecordComparator",
            ComparisonLevel.L4: "FieldComparator",
            ComparisonLevel.L5: "AggregateComparator",
            ComparisonLevel.L6: "DQComparator",
        }

        priority_map = {
            ComparisonLevel.L1: Priority.HIGH,
            ComparisonLevel.L2: Priority.HIGH,
            ComparisonLevel.L3: Priority.HIGH,
            ComparisonLevel.L4: Priority.MEDIUM,
            ComparisonLevel.L5: Priority.MEDIUM,
            ComparisonLevel.L6: Priority.MEDIUM,
        }

        tasks = []

        for strategy in decision.strategies:

            level = strategy.comparison_level

            comparator_name = comparator_map[level]

            task_configuration = {
                "configuration_id": configuration.configuration_id,

                "source": configuration.source.model_dump(
                    mode="python"
                ),

                "target": configuration.target.model_dump(
                    mode="python"
                ),

                "execution_location": (
                    strategy.execution_location.value
                ),

                "execution_mode": (
                    strategy.comparison_mode.value
                ),

                "comparison_level": level.value,

                "comparator_name": comparator_name,

                "comparison_keys": [
                    key.model_dump(mode="python")
                    for key in configuration.comparison_keys
                ],

                "matching_mode": configuration.matching_mode,

                "grouping_attributes": [
                    item.model_dump(mode="python")
                    for item in configuration.grouping_attributes
                ],

                "aggregation_columns": list(
                    configuration.aggregation_columns
                ),

                "column_mappings": [
                    mapping.model_dump(mode="python")
                    for mapping in configuration.column_mappings
                ],

                "ignored_columns": list(
                    configuration.ignored_columns
                ),

                "aggregate_rules": [
                    rule.model_dump(mode="python")
                    for rule in configuration.aggregate_rules
                ],

                "dq_rules": [
                    rule.model_dump(mode="python")
                    for rule in configuration.dq_rules
                ],
            }

            task_configuration["source"]["properties"] = {
                **task_configuration["source"].get("properties", {}),
                "_filters": [f.model_dump(mode="python") for f in configuration.source_filters],
            }
            task_configuration["target"]["properties"] = {
                **task_configuration["target"].get("properties", {}),
                "_filters": [f.model_dump(mode="python") for f in configuration.target_filters],
            }


            tasks.append(
                ExecutionTask(
                    task_id=f"TASK-{level.value}",

                    comparison_level=level,

                    comparator_name=comparator_name,

                    execution_mode=(
                        strategy.comparison_mode
                    ),

                    priority=priority_map[level],

                    dependencies=(
                        (f"TASK-{ComparisonLevel.L3.value}",)
                        if level == ComparisonLevel.L4
                        else ()
                    ),

                    parallel_eligible=True,

                    enabled=True,

                    configuration=task_configuration,
                )
            )

        return tasks

    def build_execution_groups(
        self,
        tasks: list[ExecutionTask],
    ) -> list[ExecutionGroup]:

        groups = []
        remaining = list(tasks)
        group_number = 1

        completed_task_ids = set()

        while remaining:
            ready_tasks = [
                task
                for task in remaining
                if all(
                    dependency in completed_task_ids
                    for dependency in task.dependencies
                )
            ]

            if not ready_tasks:
                raise ValueError(
                    "Unable to build execution groups. "
                    "Task dependencies contain a cycle or invalid dependency."
                )

            parallel_tasks = [
                task
                for task in ready_tasks
                if task.parallel_eligible
            ]

            if parallel_tasks:
                group_tasks = parallel_tasks
                parallel = len(group_tasks) > 1
            else:
                group_tasks = [ready_tasks[0]]
                parallel = False

            groups.append(
                ExecutionGroup(
                    group_id=f"GROUP-{group_number}",
                    tasks=group_tasks,
                    parallel=parallel,
                )
            )

            for task in group_tasks:
                remaining.remove(task)
                completed_task_ids.add(task.task_id)

            group_number += 1

        return groups

    def build_execution_plan(
        self,
        configuration: RuntimeConfiguration,
        tasks: list[ExecutionTask],
        groups: list[ExecutionGroup],
    ) -> ExecutionPlan:

        self._validate_execution_plan(tasks, groups)

        metadata = ExecutionMetadata(
            plan_id=f"PLAN-{uuid4()}",
            run_id=f"RUN-{uuid4()}",
            configuration_id=configuration.configuration_id,
            planner_version="1.0",
            platform=configuration.source.connector_type,
            execution_strategy=(
                tasks[0].execution_mode
                if tasks
                else ExecutionMode.EXACT
            ),
            created_at=datetime.now(timezone.utc),
        )

        rules = ExecutionRules()

        estimate = ExecutionEstimate(
            total_tasks=len(tasks),
            parallel_groups=sum(
                1 for group in groups if group.parallel
            ),
            estimated_duration_seconds=None,
            estimated_memory_mb=None,
            estimated_compute_cost=None,
        )

        dependency_graph = DependencyGraph(
            dependencies=self.build_dependency_graph(
                tasks
            )
        )

        return ExecutionPlan(
            metadata=metadata,
            tasks=tasks,
            groups=groups,
            rules=rules,
            estimate=estimate,
            dependency_graph=dependency_graph,
        )

    def _validate_execution_plan(
        self,
        tasks: list[ExecutionTask],
        groups: list[ExecutionGroup],
    ) -> None:
    
        task_ids = [task.task_id for task in tasks]

        if len(task_ids) != len(set(task_ids)):
            raise ValueError("Duplicate task IDs detected.")

        task_id_set = set(task_ids)

        for task in tasks:
            for dependency in task.dependencies:
                if dependency not in task_id_set:
                    raise ValueError(
                        f"Invalid dependency '{dependency}' "
                        f"for task '{task.task_id}'."
                    )

        grouped_task_ids = [
            task.task_id
            for group in groups
            for task in group.tasks
        ]

        if set(grouped_task_ids) != task_id_set:
            raise ValueError(
                "Execution groups do not contain exactly "
                "the planned tasks."
            )

        if len(grouped_task_ids) != len(set(grouped_task_ids)):
            raise ValueError(
                "A task appears in multiple execution groups."
            )

    def build_dependency_graph(
        self,
        tasks: list[ExecutionTask],
    ) -> dict[str, tuple[str, ...]]:
        """
        Build the execution dependency graph from task definitions.
        """

        task_ids = {
            task.task_id
            for task in tasks
        }

        graph = {}

        for task in tasks:

            for dependency in task.dependencies:

                if dependency not in task_ids:
                    raise ValueError(
                        f"Task {task.task_id} depends on "
                        f"unknown task {dependency}"
                    )

            graph[task.task_id] = tuple(
                task.dependencies
            )

        return graph
