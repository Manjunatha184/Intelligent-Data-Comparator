from abc import ABC, abstractmethod
from collections import Counter
import pickle
import tempfile
from typing import Any, Protocol
import copy
from uuid import uuid4

from app.execution.intermediate_store import SQLiteEvidenceStore
from app.execution.partitioner import (
    business_key_for_record,
    partition_for_key,
)
from app.execution.models import (
    ComparisonLevel,
    DataAccessMode,
    ExecutionTask,
    ExecutionLocation,
)
from app.metrics import safe_rate_pct


DEFAULT_L3_PARTITION_COUNT = 16
DEFAULT_EVIDENCE_BATCH_SIZE = 1000
DEFAULT_INLINE_MATCHED_PAIR_SAMPLE_SIZE = 10
DEFAULT_FIELD_MISMATCH_SAMPLE_SIZE = 10


# ============================================================
# PLATFORM ADAPTER
# ============================================================

class PushdownAdapter(Protocol):
    """
    Contract for a platform capable of executing comparison
    operations close to the data.
    """

    def execute_pushdown(
        self,
        task: ExecutionTask,
    ) -> Any:
        ...


# ============================================================
# EXECUTION EXECUTOR
# ============================================================

class ExecutionExecutor(ABC):

    @abstractmethod
    def execute(
        self,
        task: ExecutionTask,
    ) -> Any:
        raise NotImplementedError


# ============================================================
# LOCAL EXECUTOR
# ============================================================

class LocalExecutor(ExecutionExecutor):
    """
    Executes comparison tasks locally.

    Dataset access is handled through the connector manager.

    The ExecutionTask itself remains immutable.

    Runtime execution data such as:

        source_records
        target_records
        source_statistics
        target_statistics
        source_data
        target_data

    is created in a NEW ExecutionTask.
    """

    def __init__(
        self,
        comparator,
        connector_manager=None,
        persistence_repository=None,
    ):
        self.comparator = comparator
        self.connector_manager = connector_manager
        self.persistence_repository = persistence_repository
        self.last_runtime_task = None

    def execute(
        self,
        task: ExecutionTask,
    ) -> Any:

        # ----------------------------------------------------
        # Backward-compatible test mode
        # ----------------------------------------------------
        #
        # Some unit tests provide a comparator directly without
        # a connector manager. In that case, execute the task
        # exactly as supplied.
        #
        if self.connector_manager is None:
            return self.comparator.execute(task)

        if self._should_execute_partitioned_l3(task):
            return self._execute_partitioned_l3(task)

        if self._should_execute_streamed_l4(task):
            return self._execute_streamed_l4(task)

        # ----------------------------------------------------
        # Build runtime execution task
        # ----------------------------------------------------
        #
        # IMPORTANT:
        #
        # Do NOT modify the original ExecutionTask.
        #
        # _build_execution_task() creates a copied task with
        # all connector-resolved runtime data required by the
        # comparator.
        #
        runtime_task = self._build_execution_task(
            task
        )
        self.last_runtime_task = runtime_task
        # ----------------------------------------------------
        # Execute comparator
        # ----------------------------------------------------

        result = self.comparator.execute(
            runtime_task
        )

        return self._merge_runtime_metrics(
            result,
            runtime_task,
        )

    # ========================================================
    # BUILD EXECUTION CONTEXT
    # ========================================================

    def _build_execution_task(
        self,
        task: ExecutionTask,
    ) -> ExecutionTask:

        configuration = dict(
            task.configuration
        )

        source = configuration.get(
            "source"
        )

        target = configuration.get(
            "target"
        )

        if not isinstance(source, dict):
            raise ValueError(
                "Source dataset configuration is invalid"
            )

        if not isinstance(target, dict):
            raise ValueError(
                "Target dataset configuration is invalid"
            )

        source_connector = source.get(
            "connector_type"
        )

        target_connector = target.get(
            "connector_type"
        )

        if not source_connector:
            raise ValueError(
                "Source connector_type is required"
            )

        if not target_connector:
            raise ValueError(
                "Target connector_type is required"
            )

        # ====================================================
        # NORMALIZE COMPARISON KEYS
        # ====================================================

        comparison_keys = (
            self._normalize_comparison_keys(
                configuration.get(
                    "comparison_keys",
                    [],
                )
            )
        )

        configuration[
            "comparison_keys"
        ] = comparison_keys

        data_access_mode = (
            self._resolve_data_access_mode(
                configuration.get(
                    "data_access_mode"
                )
            )
        )

        if (
            data_access_mode == DataAccessMode.CHUNKED
            and task.comparison_level == ComparisonLevel.L2
        ):
            return self._build_chunked_l2_execution_task(
                task=task,
                configuration=configuration,
                source_connector=source_connector,
                source=source,
                target_connector=target_connector,
                target=target,
                comparison_keys=comparison_keys,
            )

        return self._build_in_memory_execution_task(
            task=task,
            configuration=configuration,
            source_connector=source_connector,
            source=source,
            target_connector=target_connector,
            target=target,
            comparison_keys=comparison_keys,
            data_access_mode=data_access_mode,
        )

    def _build_in_memory_execution_task(
        self,
        task: ExecutionTask,
        configuration: dict[str, Any],
        source_connector: str,
        source: dict[str, Any],
        target_connector: str,
        target: dict[str, Any],
        comparison_keys: list[dict[str, str]],
        data_access_mode: DataAccessMode,
    ) -> ExecutionTask:

        # ====================================================
        # LOAD NORMALIZED RECORDS
        # ====================================================

        source_records = (
            self.connector_manager.get_records(
                source_connector,
                source,
            )
        )

        target_records = (
            self.connector_manager.get_records(
                target_connector,
                target,
            )
        )

        # ====================================================
        # NORMALIZED RECORDS
        # ====================================================

        configuration[
            "source_records"
        ] = source_records

        configuration[
            "target_records"
        ] = target_records

        # ====================================================
        # L2 STATISTICS
        # ====================================================

        configuration[
            "source_statistics"
        ] = self._build_statistics(
            source_records,
            comparison_keys,
            "source_column",
        )

        configuration[
            "target_statistics"
        ] = self._build_statistics(
            target_records,
            comparison_keys,
            "target_column",
        )

        # ====================================================
        # L6 DATA
        # ====================================================

        configuration[
            "source_data"
        ] = source_records

        configuration[
            "target_data"
        ] = target_records

        configuration[
            "data_access_metrics"
        ] = self._build_in_memory_runtime_metrics(
            data_access_mode,
            source_records,
            target_records,
            task.comparison_level,
        )

        # ====================================================
        # IMPORTANT
        # ====================================================
        #
        # ExecutionTask is frozen.
        #
        # Never modify:
        #
        #     task.configuration
        #
        # Create a new task containing the runtime
        # execution configuration.
        # ====================================================

        return task.model_copy(
            update={
                "configuration": configuration
            }
        )

    def _build_chunked_l2_execution_task(
        self,
        task: ExecutionTask,
        configuration: dict[str, Any],
        source_connector: str,
        source: dict[str, Any],
        target_connector: str,
        target: dict[str, Any],
        comparison_keys: list[dict[str, str]],
    ) -> ExecutionTask:

        chunk_size = int(
            configuration.get(
                "chunk_size",
                configuration.get(
                    "data_access_chunk_size",
                    1000,
                ),
            )
        )

        if chunk_size <= 0:
            raise ValueError(
                "chunk_size must be greater than zero"
            )

        source_statistics, source_metrics = (
            self._build_chunked_statistics(
                connector_type=source_connector,
                dataset=source,
                comparison_keys=comparison_keys,
                chunk_size=chunk_size,
                key_side="source_column",
            )
        )

        target_statistics, target_metrics = (
            self._build_chunked_statistics(
                connector_type=target_connector,
                dataset=target,
                comparison_keys=comparison_keys,
                chunk_size=chunk_size,
                key_side="target_column",
            )
        )

        configuration[
            "source_statistics"
        ] = source_statistics

        configuration[
            "target_statistics"
        ] = target_statistics

        configuration[
            "data_access_metrics"
        ] = {
            "data_access_mode": DataAccessMode.CHUNKED.value,
            "source_chunks_processed": (
                source_metrics["chunks_processed"]
            ),
            "target_chunks_processed": (
                target_metrics["chunks_processed"]
            ),
            "source_records_processed": (
                source_metrics["records_processed"]
            ),
            "target_records_processed": (
                target_metrics["records_processed"]
            ),
            "chunked_l2_statistics": True,
        }

        return task.model_copy(
            update={
                "configuration": configuration
            }
        )

    # ========================================================
    # CHUNKED L3 PARTITIONED EXECUTION
    # ========================================================

    def _should_execute_partitioned_l3(
        self,
        task: ExecutionTask,
    ) -> bool:

        configuration = task.configuration

        return (
            task.comparison_level == ComparisonLevel.L3
            and task.comparator_name == "RecordComparator"
            and self._resolve_data_access_mode(
                configuration.get("data_access_mode")
            )
            == DataAccessMode.CHUNKED
        )

    def _execute_partitioned_l3(
        self,
        task: ExecutionTask,
    ) -> dict[str, Any]:

        configuration = dict(
            task.configuration
        )

        source = configuration.get("source")
        target = configuration.get("target")

        if not isinstance(source, dict):
            raise ValueError(
                "Source dataset configuration is invalid"
            )

        if not isinstance(target, dict):
            raise ValueError(
                "Target dataset configuration is invalid"
            )

        source_connector = source.get("connector_type")
        target_connector = target.get("connector_type")

        if not source_connector:
            raise ValueError(
                "Source connector_type is required"
            )

        if not target_connector:
            raise ValueError(
                "Target connector_type is required"
            )

        comparison_keys = self._normalize_comparison_keys(
            configuration.get(
                "comparison_keys",
                [],
            )
        )

        configuration[
            "comparison_keys"
        ] = comparison_keys

        partition_count = int(
            configuration.get(
                "partition_count",
                DEFAULT_L3_PARTITION_COUNT,
            )
        )

        if partition_count <= 0:
            raise ValueError(
                "partition_count must be greater than zero"
            )

        chunk_size = int(
            configuration.get(
                "chunk_size",
                configuration.get(
                    "data_access_chunk_size",
                    1000,
                ),
            )
        )

        if chunk_size <= 0:
            raise ValueError(
                "chunk_size must be greater than zero"
            )

        source_key_columns = [
            key["source_column"]
            for key in comparison_keys
        ]

        target_key_columns = [
            key["target_column"]
            for key in comparison_keys
        ]

        runtime_metrics = {
            "data_access_mode": DataAccessMode.CHUNKED.value,
            "partitioned_l3": True,
            "partition_count": partition_count,
            "partition_spill_used": True,
        }

        source_records_processed = 0
        target_records_processed = 0
        source_chunks_processed = 0
        target_chunks_processed = 0

        run_id = str(
            configuration.get(
                "__run_id",
                configuration.get(
                    "run_id",
                    f"local-{uuid4()}",
                ),
            )
        )

        evidence_directory = configuration.get(
            "evidence_store_dir"
        ) or tempfile.mkdtemp(
            prefix=(
                f"v1_comparator_evidence_{run_id}_"
                f"{task.task_id}_"
            )
        )

        evidence_store = SQLiteEvidenceStore.create(
            directory=evidence_directory,
            run_id=run_id,
            task_id=task.task_id,
        )

        merged_result = self._empty_l3_merged_result(
            configuration=configuration,
            execution_mode=task.execution_mode.value,
        )

        with tempfile.TemporaryDirectory() as temp_dir:

            source_paths = [
                f"{temp_dir}/source_partition_{index}.pkl"
                for index in range(partition_count)
            ]

            target_paths = [
                f"{temp_dir}/target_partition_{index}.pkl"
                for index in range(partition_count)
            ]

            for chunk in self.connector_manager.iter_chunks(
                source_connector,
                source,
                chunk_size=chunk_size,
            ):
                source_chunks_processed += 1
                source_records_processed += len(chunk)

                self._spill_l3_chunk(
                    chunk=chunk,
                    key_columns=source_key_columns,
                    partition_count=partition_count,
                    partition_paths=source_paths,
                )

            for chunk in self.connector_manager.iter_chunks(
                target_connector,
                target,
                chunk_size=chunk_size,
            ):
                target_chunks_processed += 1
                target_records_processed += len(chunk)

                self._spill_l3_chunk(
                    chunk=chunk,
                    key_columns=target_key_columns,
                    partition_count=partition_count,
                    partition_paths=target_paths,
                )

            partitions_processed = 0

            for partition_id in range(partition_count):
                source_records = self._load_spill_records(
                    source_paths[partition_id]
                )
                target_records = self._load_spill_records(
                    target_paths[partition_id]
                )

                if not source_records and not target_records:
                    continue

                partitions_processed += 1

                partition_result = (
                    self._execute_l3_comparator(
                        task=task,
                        configuration=configuration,
                        source_records=source_records,
                        target_records=target_records,
                    )
                )

                self._merge_l3_partition_result(
                    aggregate=merged_result,
                    result=partition_result,
                    evidence_store=evidence_store,
                )

            result = self._finalize_l3_stored_evidence(
                aggregate=merged_result,
                evidence_store=evidence_store,
            )

        runtime_metrics.update(
            {
                "partitions_processed": partitions_processed,
                "source_chunks_processed": source_chunks_processed,
                "target_chunks_processed": target_chunks_processed,
                "source_records_processed": (
                    source_records_processed
                ),
                "target_records_processed": (
                    target_records_processed
                ),
            }
        )

        metrics = dict(
            result.get(
                "metrics",
                {},
            )
        )

        metrics[
            "source_record_count"
        ] = source_records_processed

        metrics[
            "target_record_count"
        ] = target_records_processed

        metrics.update(runtime_metrics)

        return {
            **result,
            "metrics": metrics,
        }

    def _execute_l3_comparator(
        self,
        task: ExecutionTask,
        configuration: dict[str, Any],
        source_records: list[dict[str, Any]],
        target_records: list[dict[str, Any]],
    ) -> dict[str, Any]:

        partition_configuration = copy.deepcopy(
            configuration
        )

        partition_configuration[
            "source_records"
        ] = source_records

        partition_configuration[
            "target_records"
        ] = target_records

        partition_task = task.model_copy(
            update={
                "configuration": partition_configuration
            }
        )

        return self.comparator.execute(
            partition_task
        )

    def _spill_l3_chunk(
        self,
        chunk: list[dict[str, Any]],
        key_columns: list[str],
        partition_count: int,
        partition_paths: list[str],
    ) -> None:

        for record in chunk:
            key = business_key_for_record(
                record,
                key_columns,
            )

            if key is None:
                # Null/blank primary keys are deliberately unmatched. Keep
                # them in a deterministic partition so L3 records them as
                # missing/extra rather than attempting a secondary match.
                partition_id = 0
            else:
                partition_id = partition_for_key(
                    key,
                    partition_count,
                )

            with open(
                partition_paths[partition_id],
                "ab",
            ) as handle:
                pickle.dump(
                    record,
                    handle,
                )

    @staticmethod
    def _load_spill_records(
        path: str,
    ) -> list[dict[str, Any]]:

        records: list[dict[str, Any]] = []

        try:
            with open(path, "rb") as handle:
                while True:
                    try:
                        records.append(
                            pickle.load(handle)
                        )
                    except EOFError:
                        break
        except FileNotFoundError:
            return []

        return records

    def _should_execute_streamed_l4(
        self,
        task: ExecutionTask,
    ) -> bool:

        if task.comparison_level != ComparisonLevel.L4:
            return False

        return self._find_l3_matched_pairs_ref(
            task.configuration
        ) is not None

    def _execute_streamed_l4(
        self,
        task: ExecutionTask,
    ) -> dict[str, Any]:

        configuration = dict(
            task.configuration
        )

        l3_evidence = self._find_l3_evidence(
            configuration
        )

        if l3_evidence is None:
            raise ValueError(
                "L4 requires L3 dependency evidence"
            )

        matched_pairs_ref = l3_evidence.get(
            "matched_pairs_ref"
        )

        if not isinstance(matched_pairs_ref, dict):
            raise ValueError(
                "L4 matched_pairs_ref is invalid"
            )

        batch_size = int(
            configuration.get(
                "matched_pair_batch_size",
                DEFAULT_EVIDENCE_BATCH_SIZE,
            )
        )

        store = SQLiteEvidenceStore.from_ref(
            matched_pairs_ref
        )

        aggregate = self._empty_l4_merged_result(
            source_record_count=(
                self._find_l3_metric(
                    configuration,
                    "source_record_count",
                )
                or 0
            ),
            target_record_count=(
                self._find_l3_metric(
                    configuration,
                    "target_record_count",
                )
                or 0
            ),
        )

        batches_processed = 0
        evidence_batch_size = int(
            configuration.get(
                "evidence_batch_size",
                DEFAULT_EVIDENCE_BATCH_SIZE,
            )
        )

        if evidence_batch_size <= 0:
            raise ValueError(
                "evidence_batch_size must be greater than zero"
            )

        comparison_result_id = configuration.get(
            "__comparison_result_id"
        )

        evidence_batch: list[dict[str, Any]] = []
        mismatch_ordinal = 0
        records_with_mismatch: set[str] = set()

        batch_configuration = dict(configuration)
        batch_configuration[
            "field_mismatch_sample_limit"
        ] = None

        for batch in store.iter_batches(
            evidence_key=matched_pairs_ref[
                "evidence_key"
            ],
            batch_size=batch_size,
        ):
            batches_processed += 1

            batch_result = (
                self.comparator.compare_matched_pairs(
                    matched_pairs=batch,
                    configuration=batch_configuration,
                    source_record_count=aggregate[
                        "metrics"
                    ]["source_record_count"],
                    target_record_count=aggregate[
                        "metrics"
                    ]["target_record_count"],
                    missing=[],
                    extra=[],
                    ambiguous=[],
                )
            )

            mismatch_ordinal = (
                self._merge_l4_batch_result(
                    aggregate=aggregate,
                    result=batch_result,
                    evidence_batch=evidence_batch,
                    start_ordinal=mismatch_ordinal,
                    records_with_mismatch=(
                        records_with_mismatch
                    ),
                )
            )

            if (
                self.persistence_repository is not None
                and comparison_result_id is not None
            ):
                self._flush_l4_evidence_batch(
                    result_id=int(comparison_result_id),
                    run_id=str(
                        configuration.get("__run_id", "")
                    ),
                    task_id=task.task_id,
                    evidence_batch=evidence_batch,
                    batch_size=evidence_batch_size,
                )

        if (
            self.persistence_repository is not None
            and comparison_result_id is not None
            and evidence_batch
        ):
            self._write_l4_evidence_items(
                result_id=int(comparison_result_id),
                run_id=str(configuration.get("__run_id", "")),
                task_id=task.task_id,
                evidence_batch=evidence_batch,
            )
            evidence_batch.clear()

        missing_count = int(
            self._find_l3_metric(
                configuration,
                "missing_key_count",
            )
            or 0
        )

        extra_count = int(
            self._find_l3_metric(
                configuration,
                "extra_key_count",
            )
            or 0
        )

        ambiguous_count = int(
            self._find_l3_metric(
                configuration,
                "ambiguous_record_count",
            )
            or 0
        )

        metrics = aggregate["metrics"]

        metrics[
            "missing_record_count"
        ] = missing_count

        metrics[
            "extra_record_count"
        ] = extra_count

        metrics[
            "ambiguous_record_count"
        ] = ambiguous_count

        metrics[
            "status"
        ] = (
            "PASS"
            if metrics["mismatch_count"] == 0
            and missing_count == 0
            and extra_count == 0
            and ambiguous_count == 0
            else "FAIL"
        )

        metrics[
            "matched_pair_batches_processed"
        ] = batches_processed

        metrics[
            "matched_pairs_streamed"
        ] = True

        metrics[
            "evidence_storage_mode"
        ] = (
            "POSTGRES"
            if (
                self.persistence_repository is not None
                and comparison_result_id is not None
            )
            else "INLINE_SAMPLE_ONLY"
        )

        metrics[
            "field_mismatch_count"
        ] = metrics["mismatch_count"]

        metrics[
            "records_with_mismatch"
        ] = len(records_with_mismatch)

        metrics[
            "field_conformity_pct"
        ] = safe_rate_pct(
            metrics["matched_field_count"],
            metrics["compared_field_count"],
            zero_value=100.0,
        )

        metrics[
            "field_mismatch_rate_pct"
        ] = safe_rate_pct(
            metrics["mismatch_count"],
            metrics["compared_field_count"],
        )

        metrics[
            "affected_record_rate_pct"
        ] = safe_rate_pct(
            metrics["records_with_mismatch"],
            metrics["matched_record_count"],
        )

        metrics[
            "compared_pair_count"
        ] = metrics["matched_record_count"]

        metrics[
            "field_mismatches_stored"
        ] = (
            self.persistence_repository is not None
            and comparison_result_id is not None
        )

        metrics[
            "field_mismatch_evidence_batch_size"
        ] = evidence_batch_size

        aggregate[
            "runtime_context"
        ] = {
            "comparison_result_id": comparison_result_id,
            "comparison_result_precreated": (
                comparison_result_id is not None
            ),
        }

        return aggregate

    @staticmethod
    def _find_l3_evidence(
        configuration: dict[str, Any],
    ) -> dict[str, Any] | None:

        for key, value in configuration.items():
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

            if isinstance(evidence, dict):
                return evidence

        return None

    @classmethod
    def _find_l3_matched_pairs_ref(
        cls,
        configuration: dict[str, Any],
    ) -> dict[str, Any] | None:

        evidence = cls._find_l3_evidence(
            configuration
        )

        if not evidence:
            return None

        ref = evidence.get(
            "matched_pairs_ref"
        )

        return ref if isinstance(ref, dict) else None

    @staticmethod
    def _find_l3_metric(
        configuration: dict[str, Any],
        metric_name: str,
    ) -> Any:

        for key, value in configuration.items():
            if not key.startswith("dependency_"):
                continue

            if isinstance(value, dict):
                metrics = value.get(
                    "metrics",
                    {},
                )
            else:
                metrics = getattr(
                    value,
                    "metrics",
                    {},
                )

            if isinstance(metrics, dict):
                return metrics.get(metric_name)

        return None

    @staticmethod
    def _empty_l4_merged_result(
        source_record_count: int,
        target_record_count: int,
    ) -> dict[str, Any]:

        return {
            "metrics": {
                "status": "PASS",
                "source_record_count": source_record_count,
                "target_record_count": target_record_count,
                "matched_record_count": 0,
                "compared_field_count": 0,
                "matched_field_count": 0,
                "mismatch_count": 0,
                "missing_record_count": 0,
                "extra_record_count": 0,
                "ambiguous_record_count": 0,
                "field_conformity_pct": 100.0,
                "field_mismatch_rate_pct": 0.0,
                "records_with_mismatch": 0,
                "affected_record_rate_pct": 0.0,
            },
            "evidence": {
                "matching_mode": "PRIMARY_KEY",
                "field_mismatches": {
                    "count": 0,
                    "sample": [],
                    "truncated": False,
                },
                "field_mismatch_count": 0,
                "mismatch_count_by_field": {},
                "field_mismatch_evidence": {
                    "storage": "POSTGRES",
                    "evidence_type": "field_mismatch",
                },
                "missing": [],
                "extra": [],
                "ambiguous": [],
            },
        }

    @staticmethod
    def _merge_l4_batch_result(
        aggregate: dict[str, Any],
        result: dict[str, Any],
        evidence_batch: list[dict[str, Any]],
        start_ordinal: int,
        records_with_mismatch: set[str],
    ) -> int:

        aggregate_metrics = aggregate["metrics"]
        result_metrics = result.get(
            "metrics",
            {},
        )

        for key in (
            "matched_record_count",
            "compared_field_count",
            "matched_field_count",
            "mismatch_count",
        ):
            aggregate_metrics[key] += int(
                result_metrics.get(key, 0) or 0
            )

        aggregate_evidence = aggregate["evidence"]
        result_evidence = result.get(
            "evidence",
            {},
        )

        field_mismatches = result_evidence.get(
            "field_mismatches",
            [],
        )

        ordinal = start_ordinal

        for mismatch in field_mismatches:
            entity_key = str(
                mismatch.get(
                    "key",
                    "Missing primary key",
                )
            )

            records_with_mismatch.add(entity_key)

            source_field = mismatch.get(
                "source_column"
            )
            target_field = mismatch.get(
                "target_column"
            )

            by_field = aggregate_evidence[
                "mismatch_count_by_field"
            ]
            by_field[source_field] = (
                by_field.get(source_field, 0)
                + 1
            )

            mismatch_summary = aggregate_evidence[
                "field_mismatches"
            ]

            if (
                len(mismatch_summary["sample"])
                < DEFAULT_FIELD_MISMATCH_SAMPLE_SIZE
            ):
                mismatch_summary["sample"].append(
                    mismatch
                )

            evidence_batch.append(
                LocalExecutor._build_l4_field_mismatch_evidence_item(
                    mismatch=mismatch,
                    ordinal=ordinal,
                )
            )
            ordinal += 1

        aggregate_evidence[
            "field_mismatch_count"
        ] = aggregate_metrics[
            "mismatch_count"
        ]

        aggregate_evidence[
            "field_mismatches"
        ]["count"] = aggregate_metrics[
            "mismatch_count"
        ]
        aggregate_evidence[
            "field_mismatches"
        ]["truncated"] = (
            aggregate_metrics["mismatch_count"]
            > len(
                aggregate_evidence[
                    "field_mismatches"
                ]["sample"]
            )
        )

        for key in (
            "missing",
            "extra",
            "ambiguous",
        ):
            aggregate_evidence[key].extend(
                result_evidence.get(key, [])
            )

        return ordinal

    @staticmethod
    def _build_l4_field_mismatch_evidence_item(
        mismatch: dict[str, Any],
        ordinal: int,
    ) -> dict[str, Any]:

        entity_key = str(
            mismatch.get(
                "key",
                "Missing primary key",
            )
        )

        source_field = mismatch.get(
            "source_column"
        )

        target_field = mismatch.get(
            "target_column"
        )

        payload = {
            key: value
            for key, value in mismatch.items()
            if key
            not in {
                "key",
                "source_column",
                "target_column",
            }
        }

        return {
            "entity_key": entity_key,
            "source_field": source_field,
            "target_field": target_field,
            "ordinal": ordinal,
            "payload": payload,
        }

    def _flush_l4_evidence_batch(
        self,
        result_id: int,
        run_id: str,
        task_id: str,
        evidence_batch: list[dict[str, Any]],
        batch_size: int,
    ) -> None:

        while len(evidence_batch) >= batch_size:
            batch = evidence_batch[:batch_size]
            self._write_l4_evidence_items(
                result_id=result_id,
                run_id=run_id,
                task_id=task_id,
                evidence_batch=batch,
            )
            del evidence_batch[:batch_size]

    def _write_l4_evidence_items(
        self,
        result_id: int,
        run_id: str,
        task_id: str,
        evidence_batch: list[dict[str, Any]],
    ) -> None:

        if not evidence_batch:
            return

        if self.persistence_repository is None:
            return

        self.persistence_repository.write_evidence_items(
            result_id=result_id,
            run_id=run_id,
            task_id=task_id,
            comparison_level=ComparisonLevel.L4.value,
            evidence_type="field_mismatch",
            items=evidence_batch,
            batch_size=len(evidence_batch),
        )

    @staticmethod
    def _empty_l3_merged_result(
        configuration: dict[str, Any],
        execution_mode: str,
    ) -> dict[str, Any]:

        return {
            "metrics": {
                "status": "PASS",
                "source_record_count": 0,
                "target_record_count": 0,
                "source_unique_key_count": 0,
                "target_unique_key_count": 0,
                "matched_key_count": 0,
                "missing_key_count": 0,
                "extra_key_count": 0,
                "source_duplicate_key_count": 0,
                "target_duplicate_key_count": 0,
                "full_row_hash_mismatch_count": 0,
                "duplicate_record_mismatch_count": 0,
                "selected_column_hash_mismatch_count": 0,
                "hash_mismatch_count": 0,
                "mismatch_count": 0,
                "ambiguous_record_count": 0,
                "unmatchable_source_count": 0,
                "unmatchable_target_count": 0,
            },
            "evidence": {
                "matched_pairs": {
                    "count": 0,
                    "items": [],
                    "truncated": False,
                },
                "matched_pair_count": 0,
                "comparison_keys": configuration.get(
                    "comparison_keys",
                    [],
                ),
                "missing_keys": [],
                "extra_keys": [],
                "missing_records": [],
                "extra_records": [],
                "source_duplicate_keys": [],
                "target_duplicate_keys": [],
                "full_row_hash_mismatches": [],
                "selected_column_hash_mismatches": [],
                "duplicate_record_mismatches": [],
                "comparison_strategy": execution_mode,
                "exact_mismatch_count": 0,
                "exact_mismatches": [],
            },
        }

    @staticmethod
    def _merge_l3_partition_result(
        aggregate: dict[str, Any],
        result: dict[str, Any],
        evidence_store: SQLiteEvidenceStore,
    ) -> None:

        metrics = aggregate["metrics"]
        result_metrics = result.get(
            "metrics",
            {},
        )

        for key in metrics:
            if key == "status":
                continue

            metrics[key] += int(
                result_metrics.get(key, 0) or 0
            )

        evidence = aggregate["evidence"]
        result_evidence = result.get(
            "evidence",
            {},
        )

        matched_pairs = result_evidence.get(
            "matched_pairs",
            [],
        )

        if isinstance(matched_pairs, list):
            evidence_store.append_items(
                "matched_pairs",
                matched_pairs,
            )

            sample = evidence["matched_pairs"][
                "items"
            ]

            remaining_sample_slots = (
                DEFAULT_INLINE_MATCHED_PAIR_SAMPLE_SIZE
                - len(sample)
            )

            if remaining_sample_slots > 0:
                sample.extend(
                    matched_pairs[:remaining_sample_slots]
                )

        for key in (
            "missing_keys",
            "extra_keys",
            "missing_records",
            "extra_records",
            "source_duplicate_keys",
            "target_duplicate_keys",
            "full_row_hash_mismatches",
            "selected_column_hash_mismatches",
            "duplicate_record_mismatches",
            "exact_mismatches",
        ):
            evidence[key].extend(
                result_evidence.get(key, [])
            )

    @staticmethod
    def _finalize_l3_stored_evidence(
        aggregate: dict[str, Any],
        evidence_store: SQLiteEvidenceStore,
    ) -> dict[str, Any]:

        matched_pair_count = evidence_store.count_items(
            "matched_pairs"
        )

        evidence = aggregate["evidence"]
        evidence[
            "matched_pair_count"
        ] = matched_pair_count
        evidence[
            "matched_pairs_ref"
        ] = evidence_store.ref(
            "matched_pairs"
        )
        evidence["matched_pairs"][
            "count"
        ] = matched_pair_count
        evidence["matched_pairs"][
            "truncated"
        ] = (
            matched_pair_count
            > len(evidence["matched_pairs"]["items"])
        )

        evidence[
            "exact_mismatch_count"
        ] = len(
            evidence["exact_mismatches"]
        )

        metrics = aggregate["metrics"]
        metrics[
            "matched_pair_count"
        ] = matched_pair_count
        metrics[
            "matched_pairs_inline"
        ] = False
        metrics[
            "matched_pairs_stored"
        ] = True
        metrics[
            "evidence_storage_mode"
        ] = "SQLITE"
        metrics[
            "source_record_coverage_pct"
        ] = safe_rate_pct(
            metrics["matched_key_count"],
            metrics["source_record_count"],
            zero_value=(
                100.0
                if metrics["matched_key_count"] == 0
                else None
            ),
        )
        metrics[
            "target_record_coverage_pct"
        ] = safe_rate_pct(
            metrics["matched_key_count"],
            metrics["target_record_count"],
            zero_value=(
                100.0
                if metrics["matched_key_count"] == 0
                else None
            ),
        )
        metrics[
            "missing_record_rate_pct"
        ] = safe_rate_pct(
            metrics["missing_key_count"],
            metrics["source_record_count"],
        )
        metrics[
            "extra_record_rate_pct"
        ] = safe_rate_pct(
            metrics["extra_key_count"],
            metrics["target_record_count"],
        )
        metrics[
            "ambiguous_record_rate_pct"
        ] = safe_rate_pct(
            metrics["ambiguous_record_count"],
            metrics["source_record_count"],
        )
        metrics[
            "status"
        ] = (
            "PASS"
            if metrics["mismatch_count"] == 0
            else "FAIL"
        )

        return aggregate

    @staticmethod
    def _merge_l3_partition_results(
        partition_results: list[dict[str, Any]],
        configuration: dict[str, Any],
        execution_mode: str,
    ) -> dict[str, Any]:

        metrics = {
            "status": "PASS",
            "source_record_count": 0,
            "target_record_count": 0,
            "source_unique_key_count": 0,
            "target_unique_key_count": 0,
            "matched_key_count": 0,
            "missing_key_count": 0,
            "extra_key_count": 0,
            "source_duplicate_key_count": 0,
            "target_duplicate_key_count": 0,
            "full_row_hash_mismatch_count": 0,
            "duplicate_record_mismatch_count": 0,
            "selected_column_hash_mismatch_count": 0,
            "hash_mismatch_count": 0,
            "mismatch_count": 0,
            "ambiguous_record_count": 0,
        }

        evidence = {
            "matched_pairs": [],
            "comparison_keys": configuration.get(
                "comparison_keys",
                [],
            ),
            "missing_keys": [],
            "extra_keys": [],
            "missing_records": [],
            "extra_records": [],
            "source_duplicate_keys": [],
            "target_duplicate_keys": [],
            "full_row_hash_mismatches": [],
            "selected_column_hash_mismatches": [],
            "duplicate_record_mismatches": [],
            "comparison_strategy": execution_mode,
            "exact_mismatch_count": 0,
            "exact_mismatches": [],
        }

        metric_sum_keys = {
            key
            for key in metrics
            if key != "status"
        }

        evidence_list_keys = {
            "matched_pairs",
            "missing_keys",
            "extra_keys",
            "missing_records",
            "extra_records",
            "source_duplicate_keys",
            "target_duplicate_keys",
            "full_row_hash_mismatches",
            "selected_column_hash_mismatches",
            "duplicate_record_mismatches",
            "exact_mismatches",
        }

        for result in partition_results:
            result_metrics = result.get(
                "metrics",
                {},
            )

            for key in metric_sum_keys:
                metrics[key] += int(
                    result_metrics.get(key, 0) or 0
                )

            result_evidence = result.get(
                "evidence",
                {},
            )

            for key in evidence_list_keys:
                evidence[key].extend(
                    result_evidence.get(key, [])
                )

        evidence[
            "exact_mismatch_count"
        ] = len(
            evidence["exact_mismatches"]
        )

        metrics[
            "source_record_coverage_pct"
        ] = safe_rate_pct(
            metrics["matched_key_count"],
            metrics["source_record_count"],
            zero_value=(
                100.0
                if metrics["matched_key_count"] == 0
                else None
            ),
        )
        metrics[
            "target_record_coverage_pct"
        ] = safe_rate_pct(
            metrics["matched_key_count"],
            metrics["target_record_count"],
            zero_value=(
                100.0
                if metrics["matched_key_count"] == 0
                else None
            ),
        )
        metrics[
            "missing_record_rate_pct"
        ] = safe_rate_pct(
            metrics["missing_key_count"],
            metrics["source_record_count"],
        )
        metrics[
            "extra_record_rate_pct"
        ] = safe_rate_pct(
            metrics["extra_key_count"],
            metrics["target_record_count"],
        )
        metrics[
            "ambiguous_record_rate_pct"
        ] = safe_rate_pct(
            metrics["ambiguous_record_count"],
            metrics["source_record_count"],
        )

        metrics[
            "status"
        ] = (
            "PASS"
            if metrics["mismatch_count"] == 0
            else "FAIL"
        )

        return {
            "metrics": metrics,
            "evidence": evidence,
        }

    # ========================================================
    # COMPARISON KEY NORMALIZATION
    # ========================================================

    @staticmethod
    def _normalize_comparison_keys(
        keys: list[Any],
    ) -> list[dict[str, str]]:
        """
        Normalize comparison keys while preserving
        source-to-target column mappings.

        Supported forms:

            "ID"

        becomes:

            {
                "source_column": "ID",
                "target_column": "ID"
            }

        And:

            {
                "source_column": "customer_id",
                "target_column": "cust_id"
            }

        is preserved as-is.
        """

        normalized: list[dict[str, str]] = []

        for key in keys:

            if isinstance(key, str):

                normalized.append(
                    {
                        "source_column": key,
                        "target_column": key,
                    }
                )

                continue

            if isinstance(key, dict):

                source_column = key.get(
                    "source_column"
                )

                target_column = key.get(
                    "target_column"
                )

                if not source_column:
                    raise ValueError(
                        "Comparison key requires "
                        "source_column"
                    )

                if not target_column:
                    target_column = source_column

                normalized.append(
                    {
                        "source_column": source_column,
                        "target_column": target_column,
                    }
                )

                continue

            raise ValueError(
                "Unsupported comparison key "
                f"representation: "
                f"{type(key).__name__}"
            )

        return normalized

    # ========================================================
    # STATISTICS
    # ========================================================

    @staticmethod
    def _build_statistics(
        records: list[dict[str, Any]],
        comparison_keys: list[dict[str, str]],
        key_side: str = "source_column",
    ) -> dict[str, Any]:

        total_rows = len(records)

        # ----------------------------------------------------
        # Key statistics
        # ----------------------------------------------------

        key_values = []

        if comparison_keys:

            for record in records:

                key = tuple(
                    record.get(
                        column[key_side]
                    )
                    for column in comparison_keys
                )

                # Ignore records whose comparison key is
                # NULL or empty. They are counted separately
                # in null_counts and must not be treated as
                # duplicate business keys.
                key_is_null = any(
                    value is None
                    or (
                        isinstance(value, str)
                        and not value.strip()
                    )
                    for value in key
                )

                if not key_is_null:
                    key_values.append(key)

        distinct_key_count = (
            len(set(key_values))
            if comparison_keys
            else 0
        )

        duplicate_key_count = (
            len(key_values) - distinct_key_count
            if comparison_keys
            else 0
        )

        # ----------------------------------------------------
        # Null counts
        # ----------------------------------------------------

        null_counts: dict[str, int] = {}

        columns: set[str] = set()

        for record in records:
            columns.update(
                record.keys()
            )

        for column in columns:

            null_counts[column] = sum(
                1
                for record in records
                if record.get(column) is None
                or (
                    isinstance(
                        record.get(column),
                        str,
                    )
                    and not record.get(column).strip()
                )
            )

        # ----------------------------------------------------
        # Connector-neutral statistics contract
        # ----------------------------------------------------

        return {
            "total_rows": total_rows,

            "filtered_rows": total_rows,

            "partition_rows": total_rows,

            "distinct_key_count": (
                distinct_key_count
            ),

            "duplicate_key_count": (
                duplicate_key_count
            ),

            "null_counts": null_counts,
        }

    def _build_chunked_statistics(
        self,
        connector_type: str,
        dataset: dict[str, Any],
        comparison_keys: list[dict[str, str]],
        chunk_size: int,
        key_side: str,
    ) -> tuple[dict[str, Any], dict[str, int]]:

        total_rows = 0
        chunks_processed = 0
        key_values_seen: set[tuple[Any, ...]] = set()
        non_null_key_count = 0
        null_counts: dict[str, int] = {}

        for chunk in self.connector_manager.iter_chunks(
            connector_type,
            dataset,
            chunk_size=chunk_size,
        ):

            chunks_processed += 1
            total_rows += len(chunk)

            for record in chunk:

                for column, value in record.items():

                    null_counts.setdefault(
                        column,
                        0,
                    )

                    if (
                        value is None
                        or (
                            isinstance(value, str)
                            and not value.strip()
                        )
                    ):
                        null_counts[column] = (
                            null_counts.get(column, 0)
                            + 1
                        )

                if not comparison_keys:
                    continue

                key = tuple(
                    record.get(
                        column[key_side]
                    )
                    for column in comparison_keys
                )

                key_is_null = any(
                    value is None
                    or (
                        isinstance(value, str)
                        and not value.strip()
                    )
                    for value in key
                )

                if key_is_null:
                    continue

                non_null_key_count += 1
                key_values_seen.add(key)

        distinct_key_count = (
            len(key_values_seen)
            if comparison_keys
            else 0
        )

        duplicate_key_count = (
            non_null_key_count - distinct_key_count
            if comparison_keys
            else 0
        )

        return (
            {
                "total_rows": total_rows,
                "filtered_rows": total_rows,
                "partition_rows": total_rows,
                "distinct_key_count": (
                    distinct_key_count
                ),
                "duplicate_key_count": (
                    duplicate_key_count
                ),
                "null_counts": null_counts,
            },
            {
                "chunks_processed": chunks_processed,
                "records_processed": total_rows,
            },
        )

    @staticmethod
    def _resolve_data_access_mode(
        value: Any,
    ) -> DataAccessMode:

        if value is None:
            return DataAccessMode.IN_MEMORY

        if isinstance(value, DataAccessMode):
            return value

        try:
            return DataAccessMode(value)
        except ValueError:
            return DataAccessMode.IN_MEMORY

    @staticmethod
    def _build_in_memory_runtime_metrics(
        data_access_mode: DataAccessMode,
        source_records: list[dict[str, Any]],
        target_records: list[dict[str, Any]],
        comparison_level: ComparisonLevel,
    ) -> dict[str, Any]:

        metrics = {
            "data_access_mode": (
                DataAccessMode.IN_MEMORY.value
            ),
            "source_records_processed": len(
                source_records
            ),
            "target_records_processed": len(
                target_records
            ),
        }

        if (
            data_access_mode == DataAccessMode.CHUNKED
            and comparison_level in {
                ComparisonLevel.L3,
                ComparisonLevel.L4,
            }
        ):
            metrics[
                "chunked_matching_enabled"
            ] = False
            metrics[
                "chunked_fallback_reason"
            ] = (
                "Identity-aware chunked matching is "
                "not implemented for this level yet."
            )

        return metrics

    @staticmethod
    def _merge_runtime_metrics(
        result: Any,
        runtime_task: ExecutionTask,
    ) -> Any:

        runtime_metrics = runtime_task.configuration.get(
            "data_access_metrics",
            {},
        )

        if not runtime_metrics:
            return result

        if not isinstance(result, dict):
            return result

        metrics = dict(
            result.get(
                "metrics",
                {},
            )
        )

        metrics.update(runtime_metrics)

        return {
            **result,
            "metrics": metrics,
        }


# ============================================================
# PUSHDOWN EXECUTOR
# ============================================================

class PushdownExecutor(ExecutionExecutor):

    def __init__(
        self,
        adapter: PushdownAdapter,
    ):
        self.adapter = adapter

    def execute(
        self,
        task: ExecutionTask,
    ) -> Any:

        return self.adapter.execute_pushdown(
            task
        )


# ============================================================
# CONNECTOR PUSHDOWN ADAPTER
# ============================================================

class ConnectorPushdownAdapter:

    def __init__(
        self,
        connector_manager,
        comparator_registry,
    ):
        self.connector_manager = connector_manager
        self.comparator_registry = comparator_registry

    def execute_pushdown(
        self,
        task: ExecutionTask,
    ) -> dict[str, Any]:

        if task.comparison_level != ComparisonLevel.L2:
            raise RuntimeError(
                "Pushdown is only implemented for L2."
            )

        configuration = dict(
            task.configuration
        )

        source = configuration.get(
            "source"
        )

        target = configuration.get(
            "target"
        )

        if not isinstance(source, dict):
            raise ValueError(
                "Source dataset configuration is invalid"
            )

        if not isinstance(target, dict):
            raise ValueError(
                "Target dataset configuration is invalid"
            )

        source_connector = source.get(
            "connector_type"
        )

        target_connector = target.get(
            "connector_type"
        )

        if not source_connector:
            raise ValueError(
                "Source connector_type is required"
            )

        if not target_connector:
            raise ValueError(
                "Target connector_type is required"
            )

        if not self.connector_manager.supports_pushdown(
            source_connector,
            task.comparison_level.value,
        ):
            raise RuntimeError(
                "Source connector does not support "
                f"{task.comparison_level.value} pushdown: "
                f"{source_connector}"
            )

        if not self.connector_manager.supports_pushdown(
            target_connector,
            task.comparison_level.value,
        ):
            raise RuntimeError(
                "Target connector does not support "
                f"{task.comparison_level.value} pushdown: "
                f"{target_connector}"
            )

        comparison_keys = (
            LocalExecutor._normalize_comparison_keys(
                configuration.get(
                    "comparison_keys",
                    [],
                )
            )
        )

        source_keys = [
            key["source_column"]
            for key in comparison_keys
        ]

        target_keys = [
            key["target_column"]
            for key in comparison_keys
        ]

        source_statistics = (
            self.connector_manager.get_volume_statistics(
                source_connector,
                source,
                business_keys=source_keys,
                filters=source.get("properties", {}).get("_filters") or None,
            )
        )

        target_statistics = (
            self.connector_manager.get_volume_statistics(
                target_connector,
                target,
                business_keys=target_keys,
                filters=target.get("properties", {}).get("_filters") or None,
            )
        )

        runtime_configuration = {
            **configuration,
            "comparison_keys": comparison_keys,
            "source_statistics": source_statistics,
            "target_statistics": target_statistics,
        }

        runtime_task = task.model_copy(
            update={
                "configuration": runtime_configuration
            }
        )

        comparator = self.comparator_registry.get(
            task.comparator_name
        )

        result = comparator.execute(
            runtime_task
        )

        if not isinstance(result, dict):
            return result

        metrics = dict(
            result.get(
                "metrics",
                {},
            )
        )

        metrics.update(
            {
                "execution_location": (
                    ExecutionLocation.PUSHDOWN.value
                ),
                "pushdown_used": True,
                "rows_materialized": 0,
                "source_statistics_received": True,
                "target_statistics_received": True,
            }
        )

        return {
            **result,
            "metrics": metrics,
        }


# ============================================================
# TEST / DEMO PUSHDOWN ADAPTER
# ============================================================

class RecordingPushdownAdapter:

    def __init__(self):
        self.executed_tasks: list[str] = []

    def execute_pushdown(
        self,
        task: ExecutionTask,
    ) -> dict[str, Any]:

        self.executed_tasks.append(
            task.task_id
        )

        return {
            "execution_location":
                ExecutionLocation.PUSHDOWN.value,

            "task_id":
                task.task_id,

            "pushdown_executed":
                True,
        }
