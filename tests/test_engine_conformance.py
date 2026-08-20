from __future__ import annotations

import json
import os
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.connectors.base import ColumnMetadata, DatasetSchema
from app.execution.duckdb_executor import DuckDBExecutor
from app.execution.models import (
    ComparisonLevel,
    ExecutionMode,
    ExecutionTask,
    Priority,
)
from app.execution.spark_executor import SparkExecutor
from app.strategy.planner import StrategyPlanner


class EngineConformanceTest(unittest.TestCase):
    """The router may change execution engines, never comparison semantics."""

    maxDiff = None

    @classmethod
    def setUpClass(cls):
        os.environ["SPARK_MASTER_URL"] = "local[2]"
        os.environ["SPARK_DRIVER_HOST"] = "127.0.0.1"
        os.environ["SPARK_TINY_SHUFFLE_PARTITIONS"] = "1"
        cls.temporary_directory = tempfile.TemporaryDirectory()
        root = Path(cls.temporary_directory.name)
        cls.source_path = root / "source.csv"
        cls.target_path = root / "target.csv"
        cls.source_path.write_text(
            "ID,Name,Sales,Quantity,Email,Status,Region\n"
            "1,Alice,100.0,2,alice@example.com,Open,North\n"
            "2,Bob,200.0,4,bob@example.com,Closed,South\n"
            "2,Bob,210.0,4,bob@example.com,Open,South\n"
            "3,Carol,300.0,6,carol@example.com,Open,North\n"
            ",Dana,400.0,8,dana@example.com,Pending,West\n"
            "5,Eve,500.0,10,bad-email,Closed,West\n",
            encoding="utf-8",
        )
        cls.target_path.write_text(
            "ID,Name,Sales,Quantity,Target_Email,Status,Region\n"
            "1,Alice,100.0,2,ALICE@example.com,Open,North\n"
            "2,Bob,200.4,4,bob@example.com,Open,South\n"
            "2,Bob,210.0,5,bob@example.com,Closed,South\n"
            "4,Carol,300.0,6,carol@example.com,Open,North\n"
            ",Dana,400.0,8,dana@example.com,Pending,West\n"
            "5,Eve,550.0,10,bad-email,Closed,West\n"
            "6,Frank,600.0,12,frank@example.com,Open,East\n",
            encoding="utf-8",
        )
        cls.configuration = {
            "source": cls._dataset(cls.source_path),
            "target": cls._dataset(cls.target_path),
            "comparison_keys": [
                {"source_column": "ID", "target_column": "ID"}
            ],
            "matching_mode": "GROUP_RECONCILIATION",
            "grouping_attributes": [
                {"source_column": "Name", "target_column": "Name"},
                {"source_column": "Region", "target_column": "Region"},
            ],
            "aggregation_columns": [
                {
                    "source_column": "Sales",
                    "target_column": "Sales",
                    "operation": "AVG",
                },
                {
                    "source_column": "Status",
                    "target_column": "Status",
                    "operation": "MODE",
                },
            ],
            "column_mappings": [
                {
                    "source_column": "Email",
                    "target_column": "Target_Email",
                    "case_insensitive": True,
                    "trim": True,
                },
                {
                    "source_column": "Sales",
                    "target_column": "Sales",
                    "tolerance": 0.5,
                },
            ],
            "aggregate_rules": [
                {
                    "name": "sales_total",
                    "function": "SUM",
                    "source_column": "Sales",
                    "target_column": "Sales",
                    "tolerance": 1.0,
                },
                {
                    "name": "quantity_by_region",
                    "function": "SUM",
                    "source_column": "Quantity",
                    "target_column": "Quantity",
                    "source_group_by": ["Region"],
                    "target_group_by": ["Region"],
                },
            ],
            "dq_rules": [
                {
                    "rule_id": "email-pattern",
                    "name": "Email format",
                    "rule_type": "PATTERN",
                    "apply_to": "BOTH",
                    "source_column": "Email",
                    "target_column": "Target_Email",
                    "regex": r"^[^@]+@[^@]+\.[^@]+$",
                    "enabled": True,
                },
                {
                    "rule_id": "quantity-range",
                    "name": "Quantity range",
                    "rule_type": "VALIDITY",
                    "apply_to": "BOTH",
                    "source_column": "Quantity",
                    "target_column": "Quantity",
                    "min": 0,
                    "max": 10,
                    "enabled": True,
                },
            ],
            "ignored_columns": [],
        }
        cls.spark = SparkExecutor(evidence_limit=100)
        cls.duckdb = DuckDBExecutor(evidence_limit=100)

    @classmethod
    def tearDownClass(cls):
        cls.spark.close()
        cls.duckdb.close()
        cls.temporary_directory.cleanup()

    @staticmethod
    def _dataset(path):
        return {
            "connector_type": "csv",
            "properties": {
                "path": str(path),
                "delimiter": ",",
            },
        }

    @staticmethod
    def _task(level, configuration):
        return ExecutionTask(
            task_id=f"conformance-{level.value}",
            comparison_level=level,
            comparator_name=f"{level.value}Comparator",
            execution_mode=ExecutionMode.EXACT,
            priority=Priority.MEDIUM,
            configuration=configuration,
        )

    @staticmethod
    def _canonical(value, path=""):
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, dict):
            return {
                key: EngineConformanceTest._canonical(item, f"{path}.{key}")
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            items = [
                EngineConformanceTest._canonical(item, path)
                for item in value
            ]
            if path.endswith(".sample"):
                return sorted(
                    items,
                    key=lambda item: json.dumps(item, sort_keys=True, default=str),
                )
            return items
        return value

    def test_l1_to_l6_business_results_match(self):
        for level in ComparisonLevel:
            with self.subTest(level=level.value):
                task = self._task(level, self.configuration)
                spark_result = self.spark.execute(task)
                duckdb_result = self.duckdb.execute(task)
                self.assertEqual(
                    self._canonical(spark_result["metrics"]),
                    self._canonical(duckdb_result["metrics"]),
                )
                self.assertEqual(
                    self._evidence_counts(spark_result["evidence"]),
                    self._evidence_counts(duckdb_result["evidence"]),
                )
                self.assertEqual(
                    self._canonical(spark_result["evidence"]),
                    self._canonical(duckdb_result["evidence"]),
                )

    def test_router_keeps_large_and_unknown_remote_workloads_on_spark(self):
        csv_analysis = SimpleNamespace(
            source_metadata={"connector_type": "csv"},
            target_metadata={"connector_type": "csv"},
        )
        remote_analysis = SimpleNamespace(
            source_metadata={"connector_type": "databricks"},
            target_metadata={"connector_type": "csv"},
        )
        bounded_databricks_analysis = SimpleNamespace(
            source_metadata={
                "connector_type": "databricks",
                "row_count_known": True,
            },
            target_metadata={"connector_type": "csv"},
        )

        self.assertEqual(
            StrategyPlanner._choose_execution_location(
                csv_analysis,
                total_rows=20_000,
                total_bytes=2_000_000,
            ).value,
            "DUCKDB",
        )
        self.assertEqual(
            StrategyPlanner._choose_execution_location(
                csv_analysis,
                total_rows=2_000_000,
                total_bytes=2_000_000,
            ).value,
            "SPARK",
        )
        self.assertEqual(
            StrategyPlanner._choose_execution_location(
                remote_analysis,
                total_rows=20_000,
                total_bytes=2_000_000,
            ).value,
            "SPARK",
        )
        self.assertEqual(
            StrategyPlanner._choose_execution_location(
                bounded_databricks_analysis,
                total_rows=300,
                total_bytes=2_000_000,
            ).value,
            "DUCKDB",
        )
        self.assertEqual(
            StrategyPlanner._choose_execution_location(
                bounded_databricks_analysis,
                total_rows=100_001,
                total_bytes=2_000_000,
            ).value,
            "SPARK",
        )

    def test_duckdb_streams_bounded_databricks_chunks(self):
        class FakeDatabricksManager:
            def __init__(self):
                self.requested_chunk_sizes = []

            def get_data_provider(self, connector_type):
                self.assert_databricks(connector_type)
                return self

            def get_schema(self, connector_type, dataset):
                self.assert_databricks(connector_type)
                return DatasetSchema(
                    columns=(
                        ColumnMetadata("ID", "BIGINT"),
                        ColumnMetadata("Amount", "DECIMAL(10,2)"),
                        ColumnMetadata("Name", "STRING"),
                    )
                )

            def iter_chunks(self, connector_type, dataset, chunk_size):
                self.assert_databricks(connector_type)
                self.requested_chunk_sizes.append(chunk_size)
                records = {
                    "source": [
                        {"ID": 1, "Amount": Decimal("10.00"), "Name": "A"},
                        {"ID": 2, "Amount": Decimal("20.00"), "Name": "B"},
                    ],
                    "target": [
                        {"ID": 1, "Amount": Decimal("10.00"), "Name": "A"},
                        {"ID": 2, "Amount": Decimal("21.00"), "Name": "B"},
                    ],
                }
                yield records[dataset["properties"]["table"]][:1]
                yield records[dataset["properties"]["table"]][1:]

            @staticmethod
            def assert_databricks(connector_type):
                if connector_type != "databricks":
                    raise AssertionError(connector_type)

        manager = FakeDatabricksManager()
        configuration = {
            "source": {
                "connector_type": "databricks",
                "properties": {"table": "source"},
            },
            "target": {
                "connector_type": "databricks",
                "properties": {"table": "target"},
            },
            "comparison_keys": [
                {"source_column": "ID", "target_column": "ID"}
            ],
            "column_mappings": [],
            "ignored_columns": [],
        }
        executor = DuckDBExecutor(
            connector_manager=manager,
            evidence_limit=10,
        )
        try:
            with patch.dict(
                os.environ,
                {"DUCKDB_DATABRICKS_CHUNK_SIZE": "1"},
            ):
                schema_result = executor.execute(
                    self._task(ComparisonLevel.L1, configuration)
                )
                volume_result = executor.execute(
                    self._task(ComparisonLevel.L2, configuration)
                )
        finally:
            executor.close()

        self.assertEqual(schema_result["metrics"]["status"], "PASS")
        self.assertEqual(volume_result["metrics"]["total_rows_source"], 2)
        self.assertEqual(volume_result["metrics"]["total_rows_target"], 2)
        self.assertEqual(manager.requested_chunk_sizes, [1, 1])

    def test_duckdb_accepts_spark_permissive_csv_rows(self):
        root = Path(self.temporary_directory.name)
        source_path = root / "uneven-source.csv"
        target_path = root / "uneven-target.csv"
        valid_rows = "".join(
            f"{index},{index * 10},2026-08-{index:02d}\n"
            for index in range(1, 21)
        )
        source_path.write_text(
            "ID,Amount,Event_Date\n"
            + valid_rows
            + "21,210,2026-08-21,EXTRA\n\n",
            encoding="utf-8",
        )
        target_path.write_text(
            "ID,Amount,Event_Date\n"
            + valid_rows
            + "21,210\n\n",
            encoding="utf-8",
        )
        configuration = {
            "source": self._dataset(source_path),
            "target": self._dataset(target_path),
            "comparison_keys": [
                {"source_column": "ID", "target_column": "ID"}
            ],
            "column_mappings": [],
            "ignored_columns": [],
        }
        executor = DuckDBExecutor(evidence_limit=10)
        try:
            result = executor.execute(
                self._task(ComparisonLevel.L2, configuration)
            )
        finally:
            executor.close()

        self.assertEqual(result["metrics"]["total_rows_source"], 21)
        self.assertEqual(result["metrics"]["total_rows_target"], 21)
        self.assertEqual(
            result["evidence"]["target"]["null_counts"]["Event_Date"],
            1,
        )

    def test_duckdb_group_mismatches_have_ui_statuses(self):
        configuration = {
            **self.configuration,
            "comparison_keys": [],
        }
        result = self.duckdb.execute(
            self._task(ComparisonLevel.L3, configuration)
        )
        reconciliation = result["evidence"]["group_reconciliation"]
        statuses = {
            item["status"] for item in reconciliation["sample"]
        }

        self.assertGreater(result["metrics"]["aggregate_checks_failed"], 0)
        self.assertIn("GROUP_VALUE_MISMATCH", statuses)
        self.assertTrue(
            statuses.intersection(
                {"GROUP_ROW_COUNT_MISMATCH", "GROUP_DUPLICATE_ROWS"}
            )
        )

    @classmethod
    def _evidence_counts(cls, value, path=""):
        counts = {}
        if isinstance(value, dict):
            if "count" in value and "sample" in value:
                counts[path] = int(value["count"])
            for key, item in value.items():
                counts.update(cls._evidence_counts(item, f"{path}.{key}"))
        elif isinstance(value, list):
            counts[path] = len(value)
        return counts


if __name__ == "__main__":
    unittest.main()
