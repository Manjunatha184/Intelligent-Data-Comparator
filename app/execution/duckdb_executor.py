from __future__ import annotations

import json
import logging
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any

from app.comparators.duckdb_levels import DUCKDB_COMPARATORS
from app.connectors.csv import CSVMetadataProvider
from app.execution.models import ComparisonLevel, ExecutionTask
from app.metrics import safe_rate_pct


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DuckDBDataset:
    table_name: str
    columns: tuple[str, ...]
    type_names: dict[str, str]
    nullable: dict[str, bool]


class DuckDBExecutor:
    """Plan-scoped analytical executor for bounded local comparisons."""

    def __init__(self, connector_manager=None, evidence_limit: int | None = None):
        self.connector_manager = connector_manager
        self.evidence_limit = evidence_limit or int(
            os.getenv("SPARK_EVIDENCE_LIMIT", "100")
        )
        self._connection = None
        self._working_directory: str | None = None
        self._dataset_cache: dict[str, DuckDBDataset] = {}
        self._statistics_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._reconciliation_cache: dict[str, dict[str, Any]] = {}
        self._csv_provider = CSVMetadataProvider()

    @property
    def connection(self):
        if self._connection is None:
            import duckdb

            root = os.getenv("DUCKDB_TEMP_ROOT", "/tmp/duckdb-comparator")
            os.makedirs(root, exist_ok=True)
            self._working_directory = tempfile.mkdtemp(
                prefix="run-",
                dir=root,
            )
            database_path = os.path.join(
                self._working_directory,
                "comparison.duckdb",
            )
            self._connection = duckdb.connect(database_path)
            threads = max(1, int(os.getenv("DUCKDB_THREADS", "2")))
            memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "2GB")
            temporary_directory = os.path.join(
                self._working_directory,
                "spill",
            )
            os.makedirs(temporary_directory, exist_ok=True)
            self._connection.execute(f"SET threads = {threads}")
            self._connection.execute(
                f"SET memory_limit = {self.literal(memory_limit)}"
            )
            self._connection.execute(
                f"SET temp_directory = {self.literal(temporary_directory)}"
            )
            self._connection.execute("SET preserve_insertion_order = false")
        return self._connection

    def execute(self, task: ExecutionTask) -> dict[str, Any]:
        started = perf_counter()
        source = self._load(task.configuration["source"])
        target = self._load(task.configuration["target"])
        comparator = DUCKDB_COMPARATORS.get(task.comparison_level.value)
        if comparator is None:
            raise ValueError(
                f"Unsupported DuckDB comparison level: {task.comparison_level.value}"
            )

        result = comparator.execute(
            self,
            source,
            target,
            task.configuration,
        )
        result = self._normalize_contract(task.comparison_level, result)
        result.setdefault("runtime_context", {}).update(
            {
                "engine": "DUCKDB",
                "distributed": False,
                "full_collect_used": False,
                "database_backed": True,
                "execution_ms": round((perf_counter() - started) * 1000, 1),
            }
        )
        result["execution_location"] = "DUCKDB"
        logger.info(
            "DUCKDB_TIMING task_id=%s level=%s comparison_ms=%.1f",
            task.task_id,
            task.comparison_level.value,
            (perf_counter() - started) * 1000,
        )
        return result

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None
        if self._working_directory:
            shutil.rmtree(self._working_directory, ignore_errors=True)
            self._working_directory = None
        self._dataset_cache.clear()
        self._statistics_cache.clear()
        self._reconciliation_cache.clear()

    def _load(self, dataset: dict[str, Any]) -> DuckDBDataset:
        cache_key = json.dumps(dataset, sort_keys=True, default=str)
        cached = self._dataset_cache.get(cache_key)
        if cached is not None:
            return cached

        connector_type = str(dataset.get("connector_type", "")).lower()
        if connector_type == "databricks":
            loaded = self._load_databricks(dataset)
            self._dataset_cache[cache_key] = loaded
            return loaded
        if connector_type != "csv":
            raise ValueError(
                "DuckDB execution supports CSV and bounded Databricks datasets; "
                f"got '{connector_type}'"
            )

        properties = dataset.get("properties", {}) or {}
        path_value = properties.get("path")
        if not path_value:
            raise ValueError("DuckDB CSV dataset requires properties.path")
        path = self._csv_provider._resolve_csv_path(
            Path(path_value),
            properties.get("filename"),
        )
        if not path.is_file():
            raise FileNotFoundError(f"CSV dataset not found: {path}")

        metadata = self._csv_provider.get_schema(
            {
                **dataset,
                "properties": {
                    **properties,
                    "path": str(path),
                },
            }
        )
        table_name = f"dataset_{len(self._dataset_cache)}"
        raw_table_name = f"{table_name}_raw"
        typed_table_name = f"{table_name}_typed"
        columns = tuple(column.name for column in metadata.columns)
        type_names = {
            column.name: self._canonical_type_name(column)
            for column in metadata.columns
        }
        nullable = {
            # Spark's CSV reader makes every supplied schema field nullable.
            # Preserve that public L1 contract even when sampled values happen
            # to contain no nulls.
            column.name: True
            for column in metadata.columns
        }

        raw_definitions = ", ".join(
            f"{self.identifier(column.name)} VARCHAR"
            for column in metadata.columns
        )
        typed_projections = ", ".join(
            self._typed_csv_projection(column)
            for column in metadata.columns
        )
        delimiter = str(properties.get("delimiter", ","))
        copy_sql = (
            f"COPY {self.identifier(raw_table_name)} FROM {self.literal(str(path))} "
            f"(FORMAT CSV, HEADER TRUE, DELIMITER {self.literal(delimiter)}, "
            "QUOTE '\"', ESCAPE '\"', AUTO_DETECT FALSE, NULLSTR '', "
            "STRICT_MODE FALSE, NULL_PADDING TRUE)"
        )
        filter_sql = self._filter_clause(
            properties.get("_filters", []) or [],
            columns,
        )
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.execute(
                f"CREATE TABLE {self.identifier(raw_table_name)} "
                f"({raw_definitions})"
            )
            self.connection.execute(copy_sql)
            self.connection.execute(
                f"CREATE TABLE {self.identifier(typed_table_name)} AS "
                f"SELECT {typed_projections} "
                f"FROM {self.identifier(raw_table_name)}"
            )
            self.connection.execute(
                f"CREATE TABLE {self.identifier(table_name)} AS "
                f"SELECT * FROM {self.identifier(typed_table_name)}{filter_sql}"
            )
            self.connection.execute(
                f"DROP TABLE {self.identifier(raw_table_name)}"
            )
            self.connection.execute(
                f"DROP TABLE {self.identifier(typed_table_name)}"
            )
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

        loaded = DuckDBDataset(
            table_name=table_name,
            columns=columns,
            type_names=type_names,
            nullable=nullable,
        )
        self._dataset_cache[cache_key] = loaded
        return loaded

    def _load_databricks(self, dataset: dict[str, Any]) -> DuckDBDataset:
        """Stream a bounded Databricks result into a disk-backed DuckDB table."""
        from app.connectors.databricks import DatabricksConnector

        if self.connector_manager is not None:
            metadata = self.connector_manager.get_schema(
                "databricks",
                dataset,
            )
            chunks = self.connector_manager.iter_chunks(
                "databricks",
                dataset,
                chunk_size=max(
                    1,
                    int(os.getenv("DUCKDB_DATABRICKS_CHUNK_SIZE", "1000")),
                ),
            )
        else:
            provider = DatabricksConnector()
            metadata = provider.get_schema(dataset)
            chunks = provider.iter_chunks(
                dataset,
                chunk_size=max(
                    1,
                    int(os.getenv("DUCKDB_DATABRICKS_CHUNK_SIZE", "1000")),
                ),
            )

        columns = tuple(column.name for column in metadata.columns)
        if not columns:
            raise ValueError("Databricks dataset has no columns")
        if len(columns) != len(set(columns)):
            raise ValueError("Databricks dataset contains duplicate column names")

        table_name = f"dataset_{len(self._dataset_cache)}"
        column_types = {
            column.name: self._databricks_type(column.data_type)
            for column in metadata.columns
        }
        definitions = ", ".join(
            f"{self.identifier(column)} {column_types[column][0]}"
            for column in columns
        )
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = (
            f"INSERT INTO {self.identifier(table_name)} VALUES "
            f"({placeholders})"
        )
        maximum_rows = int(os.getenv("DUCKDB_DATABRICKS_MAX_ROWS", "100000"))
        loaded_rows = 0

        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.execute(
                f"CREATE TABLE {self.identifier(table_name)} ({definitions})"
            )
            for chunk in chunks:
                loaded_rows += len(chunk)
                if loaded_rows > maximum_rows:
                    raise ValueError(
                        "Databricks dataset exceeded the DuckDB routing limit "
                        f"of {maximum_rows} rows; rerun this comparison on Spark"
                    )
                values = [
                    tuple(
                        self._coerce_databricks_value(
                            record.get(column),
                            column_types[column][0],
                        )
                        for column in columns
                    )
                    for record in chunk
                ]
                if values:
                    self.connection.executemany(insert_sql, values)
            self.connection.execute("COMMIT")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise

        return DuckDBDataset(
            table_name=table_name,
            columns=columns,
            type_names={
                column: column_types[column][1]
                for column in columns
            },
            nullable={
                column.name: bool(column.nullable)
                for column in metadata.columns
            },
        )

    def stats(
        self,
        dataset: DuckDBDataset,
        configuration: dict[str, Any],
        side: str,
    ) -> dict[str, Any]:
        cache_key = (dataset.table_name, side)
        cached = self._statistics_cache.get(cache_key)
        if cached is not None:
            return cached

        key_name = "source_column" if side == "source" else "target_column"
        key_columns = [
            item[key_name]
            for item in configuration.get("comparison_keys", [])
        ]
        expressions = ["count(*) AS total_rows"]
        expressions.extend(
            f"count(*) FILTER (WHERE {self.identifier(column)} IS NULL) "
            f"AS {self.identifier('null_' + str(index))}"
            for index, column in enumerate(dataset.columns)
        )
        if key_columns:
            valid_key = self.populated_key_expression(key_columns)
            key_value = self.normalized_key_expression(key_columns)
            expressions.extend(
                [
                    f"count(*) FILTER (WHERE {valid_key}) AS keyed_rows",
                    f"count(DISTINCT CASE WHEN {valid_key} THEN {key_value} END) "
                    "AS distinct_key_count",
                ]
            )
        row = self.fetch_one(
            f"SELECT {', '.join(expressions)} "
            f"FROM {self.identifier(dataset.table_name)}"
        )
        total_rows = int(row["total_rows"] or 0)
        if key_columns:
            distinct_key_count = int(row["distinct_key_count"] or 0)
            duplicate_key_count = int(row["keyed_rows"] or 0) - distinct_key_count
        else:
            distinct_key_count = None
            duplicate_key_count = None

        result = {
            "total_rows": total_rows,
            "filtered_rows": total_rows,
            "partition_rows": total_rows,
            "distinct_key_count": distinct_key_count,
            "duplicate_key_count": duplicate_key_count,
            "null_counts": {
                column: int(row[f"null_{index}"] or 0)
                for index, column in enumerate(dataset.columns)
            },
        }
        self._statistics_cache[cache_key] = result
        return result

    def reconciliation(
        self,
        source: DuckDBDataset,
        target: DuckDBDataset,
        configuration: dict[str, Any],
    ) -> dict[str, Any]:
        keys = configuration.get("comparison_keys", []) or []
        if not keys:
            raise ValueError("DuckDB row comparison requires comparison_keys")
        cache_key = json.dumps(
            {
                "source": source.table_name,
                "target": target.table_name,
                "keys": keys,
            },
            sort_keys=True,
        )
        cached = self._reconciliation_cache.get(cache_key)
        if cached is not None:
            return cached

        source_keys = [item["source_column"] for item in keys]
        target_keys = [item["target_column"] for item in keys]
        source_valid = self.populated_key_expression(source_keys, "source_base")
        target_valid = self.populated_key_expression(target_keys, "target_base")
        source_key = self.normalized_key_expression(source_keys, "source_base")
        target_key = self.normalized_key_expression(target_keys, "target_base")
        source_order = self.row_order_expression(source.columns, "source_counted")
        target_order = self.row_order_expression(target.columns, "target_counted")
        table_name = f"reconciliation_{len(self._reconciliation_cache)}"

        source_projection = ", ".join(
            f"source_ready.{self.identifier(column)} AS "
            f"{self.identifier('source__' + column)}"
            for column in source.columns
        )
        target_projection = ", ".join(
            f"target_ready.{self.identifier(column)} AS "
            f"{self.identifier('target__' + column)}"
            for column in target.columns
        )
        sql = f"""
            CREATE TABLE {self.identifier(table_name)} AS
            WITH
            source_base AS (
                SELECT row_number() OVER () AS __source_row_id, *
                FROM {self.identifier(source.table_name)}
            ),
            target_base AS (
                SELECT row_number() OVER () AS __target_row_id, *
                FROM {self.identifier(target.table_name)}
            ),
            source_counted AS (
                SELECT *,
                    CASE WHEN {source_valid} THEN {source_key} END AS __normalized_key,
                    CASE WHEN {source_valid} THEN
                        count(*) OVER (PARTITION BY {source_key})
                    END AS __key_count
                FROM source_base
            ),
            target_counted AS (
                SELECT *,
                    CASE WHEN {target_valid} THEN {target_key} END AS __normalized_key,
                    CASE WHEN {target_valid} THEN
                        count(*) OVER (PARTITION BY {target_key})
                    END AS __key_count
                FROM target_base
            ),
            source_prepared AS (
                SELECT *,
                    CASE WHEN __normalized_key IS NOT NULL THEN
                        row_number() OVER (
                            PARTITION BY __normalized_key
                            ORDER BY {source_order}, __source_row_id
                        )
                    END AS __key_ordinal
                FROM source_counted
            ),
            target_prepared AS (
                SELECT *,
                    CASE WHEN __normalized_key IS NOT NULL THEN
                        row_number() OVER (
                            PARTITION BY __normalized_key
                            ORDER BY {target_order}, __target_row_id
                        )
                    END AS __key_ordinal
                FROM target_counted
            ),
            source_population AS (
                SELECT __normalized_key, count(*) AS __source_population
                FROM source_prepared
                WHERE __normalized_key IS NOT NULL
                GROUP BY __normalized_key
            ),
            target_population AS (
                SELECT __normalized_key, count(*) AS __target_population
                FROM target_prepared
                WHERE __normalized_key IS NOT NULL
                GROUP BY __normalized_key
            ),
            source_ready AS (
                SELECT source_prepared.*,
                    coalesce(target_population.__target_population, 0)
                        AS __target_population,
                    CASE
                        WHEN __normalized_key IS NULL THEN
                            concat('SOURCE_UNMATCHED:', __source_row_id::VARCHAR)
                        ELSE concat(
                            'KEY:', __normalized_key, ':',
                            CASE
                                WHEN coalesce(__target_population, 0) > 0
                                  AND __key_ordinal > __target_population
                                THEN 1 ELSE __key_ordinal
                            END::VARCHAR
                        )
                    END AS __join_key
                FROM source_prepared
                LEFT JOIN target_population USING (__normalized_key)
            ),
            target_ready AS (
                SELECT target_prepared.*,
                    coalesce(source_population.__source_population, 0)
                        AS __source_population,
                    CASE
                        WHEN __normalized_key IS NULL THEN
                            concat('TARGET_UNMATCHED:', __target_row_id::VARCHAR)
                        ELSE concat(
                            'KEY:', __normalized_key, ':',
                            CASE
                                WHEN coalesce(__source_population, 0) > 0
                                  AND __key_ordinal > __source_population
                                THEN 1 ELSE __key_ordinal
                            END::VARCHAR
                        )
                    END AS __join_key
                FROM target_prepared
                LEFT JOIN source_population USING (__normalized_key)
            )
            SELECT
                {source_projection},
                {target_projection},
                coalesce(
                    source_ready.__normalized_key,
                    target_ready.__normalized_key
                ) AS normalized_primary_key,
                CASE
                    WHEN source_ready.__source_row_id IS NOT NULL
                     AND target_ready.__target_row_id IS NOT NULL THEN 'MATCHED'
                    WHEN source_ready.__source_row_id IS NOT NULL
                     AND source_ready.__normalized_key IS NULL THEN 'UNMATCHABLE_SOURCE'
                    WHEN target_ready.__target_row_id IS NOT NULL
                     AND target_ready.__normalized_key IS NULL THEN 'UNMATCHABLE_TARGET'
                    WHEN source_ready.__source_row_id IS NOT NULL THEN 'MISSING_IN_TARGET'
                    ELSE 'EXTRA_IN_TARGET'
                END AS reconciliation_status,
                CASE
                    WHEN source_ready.__source_row_id IS NOT NULL
                     AND target_ready.__target_row_id IS NOT NULL
                    THEN 'PRIMARY_KEY'
                END AS match_type,
                source_ready.__key_count AS source_key_count,
                target_ready.__key_count AS target_key_count,
                source_ready.__source_row_id AS source_row_id,
                target_ready.__target_row_id AS target_row_id
            FROM source_ready
            FULL OUTER JOIN target_ready
              ON source_ready.__join_key = target_ready.__join_key
        """
        self.connection.execute(sql)
        summary = self.fetch_one(
            f"""
            SELECT
                count(*) FILTER (WHERE reconciliation_status = 'MATCHED') AS matched,
                count(DISTINCT normalized_primary_key)
                    FILTER (WHERE reconciliation_status = 'MATCHED') AS matched_keys,
                count(DISTINCT source_row_id)
                    FILTER (WHERE reconciliation_status = 'MATCHED') AS matched_source_records,
                count(DISTINCT target_row_id)
                    FILTER (WHERE reconciliation_status = 'MATCHED') AS matched_target_records,
                count(*) FILTER (WHERE reconciliation_status = 'MISSING_IN_TARGET') AS missing,
                count(*) FILTER (WHERE reconciliation_status = 'EXTRA_IN_TARGET') AS extra,
                count(DISTINCT normalized_primary_key)
                    FILTER (WHERE source_key_count > 1) AS source_duplicate_keys,
                count(DISTINCT normalized_primary_key)
                    FILTER (WHERE target_key_count > 1) AS target_duplicate_keys,
                count(*) FILTER (WHERE reconciliation_status = 'UNMATCHABLE_SOURCE')
                    AS unmatchable_source,
                count(*) FILTER (WHERE reconciliation_status = 'UNMATCHABLE_TARGET')
                    AS unmatchable_target
            FROM {self.identifier(table_name)}
            """
        )
        result = {
            "table_name": table_name,
            "source": source,
            "target": target,
            "keys": keys,
            "counts": {name: int(value or 0) for name, value in summary.items()},
        }
        self._reconciliation_cache[cache_key] = result
        return result

    def fetch_one(self, sql: str, parameters: list[Any] | None = None) -> dict[str, Any]:
        cursor = self.connection.execute(sql, parameters or [])
        row = cursor.fetchone()
        names = [column[0] for column in cursor.description]
        return dict(zip(names, row or [None] * len(names)))

    def fetch_all(self, sql: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
        cursor = self.connection.execute(sql, parameters or [])
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    def bounded_query(
        self,
        sql: str,
        total_count: int,
    ) -> dict[str, Any]:
        if total_count <= 0:
            return {"count": 0, "sample": [], "truncated": False}
        sample = self.fetch_all(f"{sql} LIMIT {self.evidence_limit}")
        return {
            "count": int(total_count),
            "sample": sample,
            "truncated": int(total_count) > self.evidence_limit,
        }

    @staticmethod
    def identifier(value: str) -> str:
        return '"' + str(value).replace('"', '""') + '"'

    @staticmethod
    def literal(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    def populated_key_expression(
        self,
        columns: list[str],
        alias: str | None = None,
    ) -> str:
        return " AND ".join(
            f"{self.column(column, alias)} IS NOT NULL AND "
            f"trim(CAST({self.column(column, alias)} AS VARCHAR)) <> ''"
            for column in columns
        )

    def normalized_key_expression(
        self,
        columns: list[str],
        alias: str | None = None,
    ) -> str:
        values = ", ".join(
            f"CAST({self.column(column, alias)} AS VARCHAR)"
            for column in columns
        )
        return f"concat_ws(chr(31), {values})"

    def row_order_expression(
        self,
        columns: tuple[str, ...],
        alias: str,
    ) -> str:
        values = ", ".join(
            f"coalesce(CAST({self.column(column, alias)} AS VARCHAR), '<NULL>')"
            for column in sorted(columns)
        )
        return f"concat_ws(chr(30), {values})"

    def column(self, name: str, alias: str | None = None) -> str:
        quoted = self.identifier(name)
        return f"{self.identifier(alias)}.{quoted}" if alias else quoted

    def normalized_value_expression(
        self,
        expression: str,
        mapping: dict[str, Any] | None,
    ) -> str:
        rules = dict((mapping or {}).get("normalization") or {})
        value = expression
        if rules.get("empty_as_null"):
            value = (
                f"CASE WHEN trim(CAST({value} AS VARCHAR)) = '' "
                f"THEN NULL ELSE {value} END"
            )
        if rules.get("trim"):
            value = f"trim(CAST({value} AS VARCHAR))"
        if rules.get("case_insensitive"):
            value = f"lower(CAST({value} AS VARCHAR))"
        if rules.get("round") is not None:
            value = f"round(CAST({value} AS DOUBLE), {int(rules['round'])})"
        return value

    @staticmethod
    def mapping_lookup(configuration: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {
            item.get("source_column"): item
            for item in configuration.get("column_mappings", [])
            if isinstance(item, dict)
            and item.get("source_column")
            and item.get("target_column")
        }

    @staticmethod
    def resolve_l4_columns(source, target, configuration):
        mappings = DuckDBExecutor.mapping_lookup(configuration)
        ignored = set(configuration.get("ignored_columns", []))
        source_keys = {
            item.get("source_column")
            for item in configuration.get("comparison_keys", [])
        }
        target_columns = set(target.columns)
        resolved = []
        for source_column in source.columns:
            if source_column in ignored or source_column in source_keys:
                continue
            mapping = mappings.get(source_column, {})
            target_column = mapping.get("target_column", source_column)
            if target_column in ignored or target_column not in target_columns:
                continue
            resolved.append((source_column, target_column, mapping))
        return resolved

    def record_from_row(
        self,
        row: dict[str, Any],
        dataset: DuckDBDataset,
        prefix: str,
    ) -> dict[str, Any] | None:
        record = {
            column: row.get(prefix + column)
            for column in dataset.columns
        }
        return record if any(value is not None for value in record.values()) else None

    def match_key(self, row: dict[str, Any], reconciliation, side="source"):
        dataset = reconciliation[side]
        prefix = "source__" if side == "source" else "target__"
        key_name = "source_column" if side == "source" else "target_column"
        return json.dumps(
            {
                item[key_name]: row.get(prefix + item[key_name])
                for item in reconciliation["keys"]
            },
            separators=(",", ":"),
            default=str,
        )

    def _filter_clause(self, filters, columns):
        if not filters:
            return ""
        known = set(columns)
        expressions = []
        aliases = {
            "EQ": "=",
            "NE": "!=",
            "GT": ">",
            "GTE": ">=",
            "LT": "<",
            "LTE": "<=",
        }
        for item in filters:
            field = item.get("field")
            if field not in known:
                raise ValueError(f"Unknown filter field: {field}")
            operator = str(item.get("operator", "=")).upper().strip()
            operator = aliases.get(operator, operator)
            column = self.identifier(field)
            value = item.get("value")
            if operator == "IN":
                values = value if isinstance(value, list) else [value]
                expressions.append(
                    f"{column} IN ({', '.join(self.literal(item) for item in values)})"
                )
            elif operator == "IS NULL":
                expressions.append(f"{column} IS NULL")
            elif operator == "IS NOT NULL":
                expressions.append(f"{column} IS NOT NULL")
            elif operator in {"=", "!=", ">", ">=", "<", "<="}:
                expressions.append(f"{column} {operator} {self.literal(value)}")
            else:
                raise ValueError(f"Unsupported DuckDB filter operator: {operator}")
        return " WHERE " + " AND ".join(expressions)

    @staticmethod
    def _duckdb_type(column) -> str:
        if column.data_type == "BOOLEAN":
            return "BOOLEAN"
        if column.data_type == "INTEGER":
            return "BIGINT"
        if column.data_type == "DATETIME":
            return "TIMESTAMP"
        if column.data_type == "DECIMAL":
            precision = min(column.precision or 38, 38)
            scale = min(column.scale or 18, precision)
            return f"DECIMAL({precision},{scale})"
        return "VARCHAR"

    def _typed_csv_projection(self, column) -> str:
        name = self.identifier(column.name)
        if column.data_type == "STRING":
            return name
        return (
            f"try_cast({name} AS {self._duckdb_type(column)}) "
            f"AS {name}"
        )

    @staticmethod
    def _databricks_type(data_type: Any) -> tuple[str, str]:
        """Return matching DuckDB storage and Spark simple-string types."""
        normalized = str(data_type or "STRING").strip().upper()
        decimal_match = re.match(
            r"^(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$",
            normalized,
        )
        if decimal_match:
            precision = min(int(decimal_match.group(1)), 38)
            scale = min(int(decimal_match.group(2)), precision)
            return (
                f"DECIMAL({precision},{scale})",
                f"decimal({precision},{scale})",
            )
        if normalized in {"DECIMAL", "NUMERIC"}:
            return "DECIMAL(38,18)", "decimal(38,18)"
        if normalized in {
            "TINYINT",
            "SMALLINT",
            "INT",
            "INTEGER",
            "BIGINT",
            "LONG",
        }:
            return "BIGINT", "bigint"
        if normalized in {"FLOAT", "REAL", "DOUBLE"}:
            return "DOUBLE", "double"
        if normalized in {"BOOLEAN", "BOOL"}:
            return "BOOLEAN", "boolean"
        if normalized == "DATE":
            return "DATE", "date"
        if normalized.startswith("TIMESTAMP"):
            return "TIMESTAMP", "timestamp"
        if normalized in {"BINARY", "VARBINARY"}:
            return "BLOB", "binary"
        return "VARCHAR", "string"

    @staticmethod
    def _coerce_databricks_value(value: Any, duckdb_type: str) -> Any:
        if value is None:
            return None
        if duckdb_type == "VARCHAR" and not isinstance(value, str):
            return str(value)
        if duckdb_type == "BLOB" and isinstance(value, memoryview):
            return bytes(value)
        return value

    @staticmethod
    def _canonical_type_name(column) -> str:
        if column.data_type == "BOOLEAN":
            return "boolean"
        if column.data_type == "INTEGER":
            return "bigint"
        if column.data_type == "DATETIME":
            return "timestamp"
        if column.data_type == "DECIMAL":
            precision = min(column.precision or 38, 38)
            scale = min(column.scale or 18, precision)
            return f"decimal({precision},{scale})"
        return "string"

    def _normalize_contract(self, level, result):
        metrics = result.setdefault("metrics", {})
        evidence = result.setdefault("evidence", {})
        metrics.setdefault("status", "PASS")
        if level in {ComparisonLevel.L5, ComparisonLevel.L6}:
            total = metrics.setdefault("checks_total", 0)
            failed = metrics.setdefault("checks_failed", 0)
            passed = metrics.setdefault("checks_passed", total - failed)
            if level == ComparisonLevel.L5:
                metrics.setdefault(
                    "aggregate_check_pass_rate_pct",
                    safe_rate_pct(passed, total, zero_value=100.0),
                )
                metrics.setdefault(
                    "aggregate_check_failure_rate_pct",
                    safe_rate_pct(failed, total),
                )
            else:
                metrics.setdefault(
                    "pass_percentage",
                    safe_rate_pct(passed, total, zero_value=100.0),
                )
                metrics.setdefault(
                    "failure_percentage",
                    safe_rate_pct(failed, total),
                )
                evidence.setdefault("dq_results", evidence.get("rule_results", []))
        return result
