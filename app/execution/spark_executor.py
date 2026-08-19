from __future__ import annotations

import os
import json
import re
import logging
import threading
from time import perf_counter
from typing import Any

from app.execution.models import ComparisonLevel, ExecutionTask
from app.comparators.spark_levels import SPARK_COMPARATORS
from app.metrics import safe_percent_change, safe_rate_pct


logger = logging.getLogger(__name__)


_SPARK_SESSION = None
_SPARK_SESSION_LOCK = threading.Lock()


class SparkExecutor:
    """Distributed Spark executor for comparison levels L1-L6.

    Data stays in Spark DataFrames. Only bounded evidence samples and scalar
    aggregate results are returned to the FastAPI process.
    """

    def __init__(self, connector_manager=None, evidence_limit: int | None = None):
        self.connector_manager = connector_manager
        self.evidence_limit = evidence_limit or int(os.getenv("SPARK_EVIDENCE_LIMIT", "100"))
        self._spark = None
        self._dataset_cache: dict[str, Any] = {}
        self._match_cache: dict[str, tuple[Any, Any, Any, dict[str, int]]] = {}
        self._stats_cache: dict[tuple[int, str], dict[str, Any]] = {}

    @property
    def spark(self):
        global _SPARK_SESSION
        if self._spark is None:
            with _SPARK_SESSION_LOCK:
                if _SPARK_SESSION is not None:
                    self._spark = _SPARK_SESSION
                    return self._spark
                started = perf_counter()
                from pyspark.sql import SparkSession
                builder = SparkSession.builder.appName(os.getenv("SPARK_APP_NAME", "V1-Comparator"))
                master = os.getenv("SPARK_MASTER_URL")
                if master:
                    builder = builder.master(master)
                builder = (
                    builder
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
                    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "67108864")
                    .config("spark.locality.wait", "0s")
                    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "16"))
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "backend"))
                )
                # CSV comparisons do not need JDBC. Only register an explicitly
                # configured, present jar so normal CSV runs stay warning-free.
                jdbc_jar = os.getenv("SPARK_JARS", "").strip()
                if jdbc_jar and os.path.exists(jdbc_jar):
                    builder = builder.config("spark.jars", jdbc_jar)
                elif jdbc_jar:
                    logger.warning("Configured SPARK_JARS path does not exist; JDBC jar was not registered")
                self._spark = builder.getOrCreate()
                self._spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
                _SPARK_SESSION = self._spark
                logger.info("SPARK_TIMING spark_session_initialization_ms=%.1f", (perf_counter() - started) * 1000)
                print(f"SPARK_TIMING spark_session_initialization_ms={(perf_counter() - started) * 1000:.1f}")
        return self._spark

    def warm_up(self) -> None:
        """Create and validate the shared Spark session once during startup."""
        started = perf_counter()
        self.spark.range(1).count()
        logger.info("SPARK_SESSION_READY startup_ms=%.1f", (perf_counter() - started) * 1000)

    def close(self) -> None:
        """Release all per-comparison Spark cache without stopping the cluster."""
        for dataframe in list(self._dataset_cache.values()):
            try:
                dataframe.unpersist(blocking=False)
            except Exception:
                logger.debug("Unable to unpersist a comparison dataset", exc_info=True)

        for cached in list(self._match_cache.values()):
            try:
                reconciliation = cached[0]
                reconciliation.unpersist(blocking=False)
            except Exception:
                logger.debug("Unable to unpersist a reconciliation stream", exc_info=True)

        self._dataset_cache.clear()
        self._match_cache.clear()
        self._stats_cache.clear()

        try:
            if self._spark is not None:
                self._spark.catalog.clearCache()
        except Exception:
            # A dead Py4J gateway cannot be repaired inside the current Python
            # process. Cleanup must not hide the original comparison failure.
            logger.warning("Spark cache cleanup skipped because the driver is unavailable")

    def execute(self, task: ExecutionTask) -> dict[str, Any]:
        """Load datasets and delegate the selected level to its comparator."""
        load_started = perf_counter()
        source = self._load(task.configuration["source"])
        target = self._load(task.configuration["target"])
        logger.info(
            "SPARK_TIMING task_id=%s level=%s dataset_loading_ms=%.1f",
            task.task_id,
            task.comparison_level.value,
            (perf_counter() - load_started) * 1000,
        )
        print(
            f"SPARK_TIMING task_id={task.task_id} "
            f"level={task.comparison_level.value} "
            f"dataset_loading_ms={(perf_counter() - load_started) * 1000:.1f}"
        )

        level = task.comparison_level
        comparator = SPARK_COMPARATORS.get(level.value)
        if comparator is None:
            raise ValueError(f"Unsupported Spark comparison level: {level}")

        level_started = perf_counter()
        result = comparator.execute(self, source, target, task.configuration)
        logger.info(
            "SPARK_TIMING task_id=%s level=%s comparison_ms=%.1f",
            task.task_id,
            level.value,
            (perf_counter() - level_started) * 1000,
        )
        print(
            f"SPARK_TIMING task_id={task.task_id} "
            f"level={level.value} "
            f"comparison_ms={(perf_counter() - level_started) * 1000:.1f}"
        )

        result = self._normalize_contract(level, result)
        result.setdefault("runtime_context", {}).update(
            {
                "engine": "SPARK",
                "spark_master": self.spark.sparkContext.master,
                "spark_app_id": self.spark.sparkContext.applicationId,
                "distributed": True,
                "full_collect_used": False,
            }
        )
        result["execution_location"] = "SPARK"
        return result

    def _load(self, dataset: dict[str, Any]):
        from pyspark.sql import functions as F
        cache_key = json.dumps(dataset, sort_keys=True, default=str)
        cached = self._dataset_cache.get(cache_key)
        if cached is not None:
            return cached

        connector = str(dataset.get("connector_type", "")).lower()
        props = dataset.get("properties", {}) or {}
        filters_already_applied = False

        if connector == "csv":
            path = props.get("path")
            if not path:
                raise ValueError("Spark CSV dataset requires properties.path")
            size_bytes = os.path.getsize(path)
            if size_bytes <= int(os.getenv("SPARK_TINY_FILE_BYTES", str(4 * 1024 * 1024))):
                self.spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_TINY_SHUFFLE_PARTITIONS", "1"))
                self.spark.conf.set("spark.sql.files.maxPartitionBytes", str(max(size_bytes, 1)))
            elif size_bytes <= int(os.getenv("SPARK_SMALL_FILE_BYTES", str(128 * 1024 * 1024))):
                self.spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SMALL_SHUFFLE_PARTITIONS", "4"))
            else:
                self.spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "16"))
            df = (self.spark.read.schema(self._csv_schema(dataset))
                  .option("header", "true")
                  .option("delimiter", props.get("delimiter", ","))
                  .csv(path))
        elif connector in {"postgres", "postgresql"}:
            conn = props.get("connection", props)
            host = conn.get("host", "postgres")
            port = conn.get("port", 5432)
            db = conn.get("database") or conn.get("dbname")
            table = props.get("table")
            schema = props.get("schema")
            if schema and table:
                table = f"{schema}.{table}"
            url = f"jdbc:postgresql://{host}:{port}/{db}"
            df = (self.spark.read.format("jdbc").option("url", url).option("dbtable", table)
                  .option("user", conn.get("user") or conn.get("username"))
                  .option("password", conn.get("password")).option("driver", "org.postgresql.Driver").load())

        elif connector == "databricks":
            df = self._load_databricks(dataset)
            # DatabricksConnector.iter_chunks() executes the configured
            # filter clause in Databricks SQL before returning rows.
            filters_already_applied = True

        else:
            raise ValueError(
                "Spark executor supports CSV, PostgreSQL, and Databricks "
                f"datasets; got '{connector}'"
            )

        for flt in ([] if filters_already_applied else (props.get("_filters", []) or [])):
            field, raw_operator, value = flt.get("field"), str(flt.get("operator", "EQ")).upper().strip(), flt.get("value")
            op = {
                "=": "EQ",
                "!=": "NE",
                ">": "GT",
                ">=": "GTE",
                "<": "LT",
                "<=": "LTE",
                "IS NULL": "IS_NULL",
                "IS NOT NULL": "IS_NOT_NULL",
            }.get(raw_operator, raw_operator.replace(" ", "_"))
            if field not in df.columns:
                raise ValueError(f"Unknown filter field: {field}")
            c = F.col(field)
            expr = {"EQ": c == value, "NE": c != value, "GT": c > value, "GTE": c >= value,
                    "LT": c < value, "LTE": c <= value}.get(op)
            if op == "IN": expr = c.isin(value if isinstance(value, list) else [value])
            if op == "IS_NULL": expr = c.isNull()
            if op == "IS_NOT_NULL": expr = c.isNotNull()
            if expr is None: raise ValueError(f"Unsupported Spark filter operator: {op}")
            df = df.filter(expr)
        # L1-L6 reuse the same filtered datasets. Persisting prevents every
        # level from rereading CSV/JDBC input while keeping data distributed.
        df = df.persist()
        # Materialize once now; subsequent L1-L6 actions reuse this cached,
        # filtered DataFrame rather than re-reading the input file.
        df.count()
        self._dataset_cache[cache_key] = df
        return df

    def _load_databricks(self, dataset: dict[str, Any]):
        """
        Read a Databricks SQL table through the existing application connector
        and promote bounded chunks into Spark DataFrames.

        This deliberately avoids requiring a separate Databricks JDBC jar in
        the Spark containers.  Authentication, catalog/schema/table quoting and
        filters stay owned by DatabricksConnector.
        """
        from app.connectors.databricks import DatabricksConnector

        connector = None
        if self.connector_manager is not None:
            try:
                connector = self.connector_manager.get_data_provider("databricks")
            except Exception:
                logger.exception(
                    "Registered Databricks provider lookup failed; "
                    "using direct connector instance"
                )

        if connector is None:
            connector = DatabricksConnector()

        chunk_size = int(
            os.getenv(
                "SPARK_DATABRICKS_CHUNK_SIZE",
                str(connector.DEFAULT_CHUNK_SIZE),
            )
        )
        if chunk_size <= 0:
            raise ValueError(
                "SPARK_DATABRICKS_CHUNK_SIZE must be greater than zero"
            )

        combined = None
        expected_columns = None

        chunk_iterator = (
            self.connector_manager.iter_chunks(
                "databricks",
                dataset,
                chunk_size=chunk_size,
            )
            if self.connector_manager is not None
            else connector.iter_chunks(
                dataset,
                chunk_size=chunk_size,
            )
        )

        for chunk in chunk_iterator:
            if not chunk:
                continue

            chunk_df = self._databricks_chunk_dataframe(
                dataset,
                chunk,
            )

            if expected_columns is None:
                expected_columns = list(chunk_df.columns)
            elif set(chunk_df.columns) != set(expected_columns):
                raise ValueError(
                    "Databricks returned inconsistent columns between chunks"
                )

            combined = (
                chunk_df
                if combined is None
                else combined.unionByName(
                    chunk_df,
                    allowMissingColumns=False,
                )
            )

        if combined is None:
            # Preserve schema even for an empty selected table so L1 and the
            # remaining levels can still run deterministically.
            combined = self.spark.createDataFrame(
                [],
                schema=self._databricks_spark_schema(dataset),
            )

        return combined

    def _databricks_chunk_dataframe(
        self,
        dataset: dict[str, Any],
        records: list[dict[str, Any]],
    ):
        """
        Prefer Spark's native Python-value inference for normal data.  If a
        chunk contains only NULL values for a column, inference can fail; in
        that case use the Databricks metadata schema.
        """
        try:
            return self.spark.createDataFrame(records)
        except Exception as inference_error:
            logger.info(
                "Databricks Spark schema inference failed; using metadata "
                "schema instead: %s",
                inference_error,
            )
            schema = self._databricks_spark_schema(dataset)
            return self.spark.createDataFrame(
                self._coerce_records_for_schema(records, schema),
                schema=schema,
            )

    @staticmethod
    def _coerce_records_for_schema(records, schema):
        """
        Unknown Databricks SQL types are represented as Spark strings by the
        metadata mapper.  Convert only those fallback fields to strings; known
        numeric/date/boolean values keep their native Python representation.
        """
        from pyspark.sql.types import StringType

        string_fields = {
            field.name
            for field in schema.fields
            if isinstance(field.dataType, StringType)
        }

        if not string_fields:
            return records

        normalized = []
        for record in records:
            row = dict(record)
            for field in string_fields:
                value = row.get(field)
                if value is not None and not isinstance(value, str):
                    row[field] = str(value)
            normalized.append(row)
        return normalized

    @staticmethod
    def _databricks_spark_schema(dataset: dict[str, Any]):
        from app.connectors.databricks import DatabricksConnector
        from pyspark.sql.types import (
            BinaryType,
            BooleanType,
            DateType,
            DecimalType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        metadata = DatabricksConnector().get_schema(dataset)

        fields = []
        decimal_pattern = re.compile(
            r"^(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$",
            re.IGNORECASE,
        )

        for column in metadata.columns:
            raw_type = str(column.data_type or "STRING").strip()
            upper_type = raw_type.upper()

            decimal_match = decimal_pattern.match(upper_type)
            if decimal_match:
                precision = min(int(decimal_match.group(1)), 38)
                scale = min(int(decimal_match.group(2)), precision)
                data_type = DecimalType(precision, scale)
            elif upper_type in {
                "TINYINT",
                "SMALLINT",
                "INT",
                "INTEGER",
                "BIGINT",
                "LONG",
            }:
                data_type = LongType()
            elif upper_type in {
                "FLOAT",
                "REAL",
                "DOUBLE",
            }:
                data_type = DoubleType()
            elif upper_type in {
                "BOOLEAN",
                "BOOL",
            }:
                data_type = BooleanType()
            elif upper_type == "DATE":
                data_type = DateType()
            elif upper_type.startswith("TIMESTAMP"):
                data_type = TimestampType()
            elif upper_type in {
                "BINARY",
                "VARBINARY",
            }:
                data_type = BinaryType()
            else:
                # STRING / VARCHAR / CHAR and complex/unknown types use a
                # stable textual representation for this comparator path.
                data_type = StringType()

            fields.append(
                StructField(
                    column.name,
                    data_type,
                    bool(column.nullable),
                )
            )

        return StructType(fields)

    @staticmethod
    def _csv_schema(dataset: dict[str, Any]):
        """Use the connector's bounded CSV inference instead of Spark's full
        inferSchema pass, preserving the application's CSV type semantics."""
        from pyspark.sql.types import BooleanType, DecimalType, LongType, StringType, StructField, StructType, TimestampType
        from app.connectors.csv import CSVMetadataProvider

        type_map = {
            "BOOLEAN": BooleanType,
            "INTEGER": LongType,
            "DATETIME": TimestampType,
            "STRING": StringType,
        }
        fields = []
        for column in CSVMetadataProvider().get_schema(dataset).columns:
            if column.data_type == "DECIMAL":
                data_type = DecimalType(column.precision or 38, column.scale or 18)
            else:
                data_type = type_map.get(column.data_type, StringType)()
            fields.append(StructField(column.name, data_type, column.nullable))
        return StructType(fields)

    @staticmethod
    def _maps(cfg):
        return {m["source_column"]: m["target_column"] for m in cfg.get("column_mappings", [])}


    def _key_exprs(self, cfg, side):
        from pyspark.sql import functions as F
        name = "source_column" if side == "source" else "target_column"
        return [F.col(k[name]).cast("string") for k in cfg.get("comparison_keys", [])]

    def _stats(self, df, cfg, side):
        from pyspark.sql import functions as F
        cache_key = (id(df), side)
        if cache_key in self._stats_cache:
            return self._stats_cache[cache_key]
        keys = self._key_exprs(cfg, side)
        key_value = F.concat_ws("\u001f", *keys) if keys else None
        valid = None
        for key in keys:
            populated = key.isNotNull() & (F.trim(key) != "")
            valid = populated if valid is None else valid & populated
        aggregates = [
            F.count(F.lit(1)).alias("total_rows"),
            *[F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns],
        ]
        if valid is not None:
            aggregates.extend([
                F.sum(F.when(valid, 1).otherwise(0)).alias("keyed_rows"),
                F.countDistinct(F.when(valid, key_value)).alias("distinct_key_count"),
            ])
        base = df.agg(*aggregates).first().asDict()
        total = int(base.pop("total_rows") or 0)
        if keys:
            distinct = base.pop("distinct_key_count") or 0
            dup = (base.pop("keyed_rows") or 0) - distinct
        else: distinct = dup = None
        null_counts = {
            column: int(count or 0)
            for column, count in base.items()
        }
        result = {"total_rows": total, "filtered_rows": total, "partition_rows": total, "distinct_key_count": distinct,
                "duplicate_key_count": dup, "null_counts": null_counts}
        self._stats_cache[cache_key] = result
        return result


    def _joined(self, s, t, cfg):
        from pyspark.sql import functions as F
        keys=cfg.get("comparison_keys",[])
        if not keys: raise ValueError("Spark row comparison requires comparison_keys")
        ss=s.alias("s"); tt=t.alias("t")
        cond=None
        for k in keys:
            x=F.col(f"s.`{k['source_column']}`").eqNullSafe(F.col(f"t.`{k['target_column']}`")); cond=x if cond is None else cond & x
        return ss.join(tt,cond,"full_outer"), cond

    def _matched_pairs(self, s, t, cfg):
        """Build the authoritative PK reconciliation using the cheapest safe Spark path.

        L2 normally computes and caches per-side key statistics before L3.  When
        those statistics prove that populated business keys are unique on both
        sides, the expensive duplicate-key Window/group/join machinery is not
        needed.  In that common case we use one direct distributed full-outer
        join on the normalized PK.  If either side contains duplicates, the
        existing deterministic occurrence-matching implementation is used
        unchanged.
        """
        from pyspark.sql import functions as F

        keys = cfg.get("comparison_keys", [])
        if not keys:
            raise ValueError("Spark row comparison requires comparison_keys")

        cache_key = json.dumps(
            {
                "source": cfg.get("source"),
                "target": cfg.get("target"),
                "comparison_keys": keys,
            },
            sort_keys=True,
            default=str,
        )
        cached = self._match_cache.get(cache_key)
        if cached is not None:
            return cached

        # These are normally cache hits because L2 runs before L3.  If L3 is
        # invoked independently they are still exact distributed Spark stats.
        source_stats = self._stats(s, cfg, "source")
        target_stats = self._stats(t, cfg, "target")
        source_duplicates = int(source_stats.get("duplicate_key_count") or 0)
        target_duplicates = int(target_stats.get("duplicate_key_count") or 0)

        # Preserve the full deterministic duplicate-occurrence algorithm for
        # the complex case.  Nothing about duplicate semantics changes.
        if source_duplicates > 0 or target_duplicates > 0:
            return self._matched_pairs_with_duplicates(s, t, cfg)

        build_started = perf_counter()

        source = s.withColumn("__source_row_id", F.monotonically_increasing_id())
        target = t.withColumn("__target_row_id", F.monotonically_increasing_id())

        source_valid = None
        target_valid = None
        for item in keys:
            source_column = item["source_column"]
            target_column = item["target_column"]
            source_populated = (
                F.col(source_column).isNotNull()
                & (F.trim(F.col(source_column).cast("string")) != "")
            )
            target_populated = (
                F.col(target_column).isNotNull()
                & (F.trim(F.col(target_column).cast("string")) != "")
            )
            source_valid = (
                source_populated
                if source_valid is None
                else source_valid & source_populated
            )
            target_valid = (
                target_populated
                if target_valid is None
                else target_valid & target_populated
            )

        source_key = F.concat_ws(
            "\u001f",
            *[F.col(item["source_column"]).cast("string") for item in keys],
        )
        target_key = F.concat_ws(
            "\u001f",
            *[F.col(item["target_column"]).cast("string") for item in keys],
        )

        source_prepared = (
            source
            .withColumn(
                "__normalized_primary_key",
                F.when(source_valid, source_key),
            )
            .withColumn(
                "__key_count",
                F.when(source_valid, F.lit(1)).otherwise(F.lit(None).cast("int")),
            )
        )
        target_prepared = (
            target
            .withColumn(
                "__normalized_primary_key",
                F.when(target_valid, target_key),
            )
            .withColumn(
                "__key_count",
                F.when(target_valid, F.lit(1)).otherwise(F.lit(None).cast("int")),
            )
        )

        # Normal equality deliberately does NOT match NULL keys.  Rows without
        # usable PKs therefore remain separate UNMATCHABLE_SOURCE/TARGET rows.
        joined = source_prepared.alias("s").join(
            target_prepared.alias("t"),
            F.col("s.__normalized_primary_key")
            == F.col("t.__normalized_primary_key"),
            "full_outer",
        )

        source_struct = F.struct(
            *[
                F.col(f"s.`{field.name}`").alias(field.name)
                for field in s.schema.fields
            ]
        )
        target_struct = F.struct(
            *[
                F.col(f"t.`{field.name}`").alias(field.name)
                for field in t.schema.fields
            ]
        )
        source_key_json = F.to_json(
            F.struct(*[F.col(f"s.`{item['source_column']}`") for item in keys])
        )
        target_key_json = F.to_json(
            F.struct(*[F.col(f"t.`{item['target_column']}`") for item in keys])
        )

        reconciliation = joined.select(
            source_struct.alias("_s"),
            target_struct.alias("_t"),
            F.coalesce(
                F.col("s.__normalized_primary_key"),
                F.col("t.__normalized_primary_key"),
            ).alias("normalized_primary_key"),
            F.when(
                F.col("s.__source_row_id").isNotNull()
                & F.col("t.__target_row_id").isNotNull(),
                F.lit("MATCHED"),
            )
            .when(
                F.col("s.__source_row_id").isNotNull()
                & F.col("s.__normalized_primary_key").isNull(),
                F.lit("UNMATCHABLE_SOURCE"),
            )
            .when(
                F.col("t.__target_row_id").isNotNull()
                & F.col("t.__normalized_primary_key").isNull(),
                F.lit("UNMATCHABLE_TARGET"),
            )
            .when(
                F.col("s.__source_row_id").isNotNull(),
                F.lit("MISSING_IN_TARGET"),
            )
            .otherwise(F.lit("EXTRA_IN_TARGET"))
            .alias("reconciliation_status"),
            F.when(
                F.col("s.__source_row_id").isNotNull()
                & F.col("t.__target_row_id").isNotNull(),
                F.lit("PRIMARY_KEY"),
            )
            .otherwise(F.lit(None).cast("string"))
            .alias("match_type"),
            F.when(
                F.col("s.__source_row_id").isNotNull(),
                source_key_json,
            )
            .otherwise(target_key_json)
            .alias("match_key"),
            F.col("s.__key_count").alias("_source_key_count"),
            F.col("t.__key_count").alias("_target_key_count"),
            F.col("s.__source_row_id").alias("_source_row_id"),
            F.col("t.__target_row_id").alias("_target_row_id"),
        ).persist()

        pk_build_ms = (perf_counter() - build_started) * 1000
        summary_started = perf_counter()

        # Unique keys make matched keys == matched records.  Avoid expensive
        # countDistinct expressions and duplicate bookkeeping on this path.
        summary = reconciliation.agg(
            F.sum(
                F.when(F.col("reconciliation_status") == "MATCHED", 1).otherwise(0)
            ).alias("matched"),
            F.sum(
                F.when(
                    F.col("reconciliation_status") == "MISSING_IN_TARGET", 1
                ).otherwise(0)
            ).alias("missing"),
            F.sum(
                F.when(
                    F.col("reconciliation_status") == "EXTRA_IN_TARGET", 1
                ).otherwise(0)
            ).alias("extra"),
            F.sum(
                F.when(
                    F.col("reconciliation_status") == "UNMATCHABLE_SOURCE", 1
                ).otherwise(0)
            ).alias("unmatchable_source"),
            F.sum(
                F.when(
                    F.col("reconciliation_status") == "UNMATCHABLE_TARGET", 1
                ).otherwise(0)
            ).alias("unmatchable_target"),
        ).first().asDict()

        pk_summary_ms = (perf_counter() - summary_started) * 1000
        matched = int(summary.get("matched") or 0)

        counts = {
            "primary_matched_count": matched,
            "matched_key_count": matched,
            "matched_source_record_count": matched,
            "matched_target_record_count": matched,
            "missing_count": int(summary.get("missing") or 0),
            "extra_count": int(summary.get("extra") or 0),
            "source_duplicate_record_count": 0,
            "target_duplicate_record_count": 0,
            "source_duplicate_key_count": 0,
            "target_duplicate_key_count": 0,
            "unmatchable_source_count": int(summary.get("unmatchable_source") or 0),
            "unmatchable_target_count": int(summary.get("unmatchable_target") or 0),
            "source_unique_key_count": int(source_stats.get("distinct_key_count") or 0),
            "target_unique_key_count": int(target_stats.get("distinct_key_count") or 0),
        }

        result = (
            reconciliation,
            counts,
            {
                "pk_build_ms": pk_build_ms,
                "pk_summary_ms": pk_summary_ms,
                "pk_path": "UNIQUE_KEY_FAST_PATH",
            },
        )
        self._match_cache[cache_key] = result
        return result

    def _matched_pairs_with_duplicates(self, s, t, cfg):
        """Build the one authoritative, full-outer PK reconciliation stream."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        keys = cfg.get("comparison_keys", [])
        if not keys:
            raise ValueError("Spark row comparison requires comparison_keys")

        cache_key = json.dumps({"source": cfg.get("source"), "target": cfg.get("target"), "comparison_keys": keys}, sort_keys=True, default=str)
        cached = self._match_cache.get(cache_key)
        if cached is not None:
            return cached

        build_started = perf_counter()
        source = s.withColumn("__source_row_id", F.monotonically_increasing_id())
        target = t.withColumn("__target_row_id", F.monotonically_increasing_id())
        source_valid = None
        target_valid = None
        for k in keys:
            source_populated = F.col(k["source_column"]).isNotNull() & (F.trim(F.col(k["source_column"]).cast("string")) != "")
            target_populated = F.col(k["target_column"]).isNotNull() & (F.trim(F.col(k["target_column"]).cast("string")) != "")
            source_valid = source_populated if source_valid is None else source_valid & source_populated
            target_valid = target_populated if target_valid is None else target_valid & target_populated

        source_key = F.concat_ws("\u001f", *[F.col(k["source_column"]).cast("string") for k in keys])
        target_key = F.concat_ws("\u001f", *[F.col(k["target_column"]).cast("string") for k in keys])
        source_prepared = source.withColumn("__normalized_primary_key", F.when(source_valid, source_key))
        target_prepared = target.withColumn("__normalized_primary_key", F.when(target_valid, target_key))
        source_prepared = source_prepared.withColumn(
            "__key_count",
            F.when(source_valid, F.count("__source_row_id").over(Window.partitionBy("__normalized_primary_key"))),
        )
        target_prepared = target_prepared.withColumn(
            "__key_count",
            F.when(target_valid, F.count("__target_row_id").over(Window.partitionBy("__normalized_primary_key"))),
        )
        # Pair repeated populated keys by a deterministic occurrence number.
        # This avoids a Cartesian join while allowing every paired occurrence
        # to continue to L4. Any excess occurrence remains missing/extra in L3.
        source_order = F.to_json(F.struct(*[F.col(column) for column in sorted(s.columns)]))
        target_order = F.to_json(F.struct(*[F.col(column) for column in sorted(t.columns)]))
        source_prepared = source_prepared.withColumn(
            "__key_ordinal",
            F.when(source_valid, F.row_number().over(Window.partitionBy("__normalized_primary_key").orderBy(source_order, "__source_row_id"))),
        )
        target_prepared = target_prepared.withColumn(
            "__key_ordinal",
            F.when(target_valid, F.row_number().over(Window.partitionBy("__normalized_primary_key").orderBy(target_order, "__target_row_id"))),
        )
        source_population = source_prepared.filter(F.col("__normalized_primary_key").isNotNull()).groupBy(
            "__normalized_primary_key"
        ).agg(F.count(F.lit(1)).alias("__source_population_count"))
        target_population = target_prepared.filter(F.col("__normalized_primary_key").isNotNull()).groupBy(
            "__normalized_primary_key"
        ).agg(F.count(F.lit(1)).alias("__target_population_count"))
        source_prepared = source_prepared.join(target_population, "__normalized_primary_key", "left").fillna(
            0, subset=["__target_population_count"]
        )
        target_prepared = target_prepared.join(source_population, "__normalized_primary_key", "left").fillna(
            0, subset=["__source_population_count"]
        )
        source_pair_ordinal = F.when(
            (F.col("__target_population_count") > 0) & (F.col("__key_ordinal") > F.col("__target_population_count")),
            F.lit(1),
        ).otherwise(F.col("__key_ordinal"))
        target_pair_ordinal = F.when(
            (F.col("__source_population_count") > 0) & (F.col("__key_ordinal") > F.col("__source_population_count")),
            F.lit(1),
        ).otherwise(F.col("__key_ordinal"))
        source_prepared = source_prepared.withColumn(
            "__join_key",
            F.when(source_valid, F.concat(F.lit("KEY:"), F.col("__normalized_primary_key"), F.lit(":"), source_pair_ordinal))
            .otherwise(F.concat(F.lit("SOURCE_UNMATCHED:"), F.col("__source_row_id").cast("string"))),
        )
        target_prepared = target_prepared.withColumn(
            "__join_key",
            F.when(target_valid, F.concat(F.lit("KEY:"), F.col("__normalized_primary_key"), F.lit(":"), target_pair_ordinal))
            .otherwise(F.concat(F.lit("TARGET_UNMATCHED:"), F.col("__target_row_id").cast("string"))),
        )

        source_struct = F.struct(*[F.col(f"s.`{field.name}`").alias(field.name) for field in s.schema.fields])
        target_struct = F.struct(*[F.col(f"t.`{field.name}`").alias(field.name) for field in t.schema.fields])
        source_key_json = F.to_json(F.struct(*[F.col(f"s.`{k['source_column']}`") for k in keys]))
        target_key_json = F.to_json(F.struct(*[F.col(f"t.`{k['target_column']}`") for k in keys]))
        joined = source_prepared.alias("s").join(
            target_prepared.alias("t"),
            F.col("s.__join_key") == F.col("t.__join_key"),
            "full_outer",
        )
        reconciliation = joined.select(
            source_struct.alias("_s"),
            target_struct.alias("_t"),
            F.coalesce(F.col("s.__normalized_primary_key"), F.col("t.__normalized_primary_key")).alias("normalized_primary_key"),
            F.when(F.col("s.__source_row_id").isNotNull() & F.col("t.__target_row_id").isNotNull(), F.lit("MATCHED"))
            .when(F.col("s.__source_row_id").isNotNull() & F.col("s.__normalized_primary_key").isNull(), F.lit("UNMATCHABLE_SOURCE"))
            .when(F.col("t.__target_row_id").isNotNull() & F.col("t.__normalized_primary_key").isNull(), F.lit("UNMATCHABLE_TARGET"))
            .when(F.col("s.__source_row_id").isNotNull(), F.lit("MISSING_IN_TARGET"))
            .otherwise(F.lit("EXTRA_IN_TARGET")).alias("reconciliation_status"),
            F.when(F.col("s.__source_row_id").isNotNull() & F.col("t.__target_row_id").isNotNull(), F.lit("PRIMARY_KEY")).otherwise(F.lit(None).cast("string")).alias("match_type"),
            F.when(F.col("s.__source_row_id").isNotNull(), source_key_json).otherwise(target_key_json).alias("match_key"),
            F.col("s.__key_count").alias("_source_key_count"),
            F.col("t.__key_count").alias("_target_key_count"),
            F.col("s.__source_row_id").alias("_source_row_id"),
            F.col("t.__target_row_id").alias("_target_row_id"),
        ).persist()
        pk_build_ms = (perf_counter() - build_started) * 1000
        summary_started = perf_counter()
        summary = reconciliation.agg(
            F.sum(F.when(F.col("reconciliation_status") == "MATCHED", 1).otherwise(0)).alias("matched"),
            F.countDistinct(F.when(F.col("reconciliation_status") == "MATCHED", F.col("normalized_primary_key"))).alias("matched_keys"),
            F.countDistinct(F.when(F.col("reconciliation_status") == "MATCHED", F.col("_source_row_id"))).alias("matched_source_records"),
            F.countDistinct(F.when(F.col("reconciliation_status") == "MATCHED", F.col("_target_row_id"))).alias("matched_target_records"),
            F.sum(F.when(F.col("reconciliation_status") == "MISSING_IN_TARGET", 1).otherwise(0)).alias("missing"),
            F.sum(F.when(F.col("reconciliation_status") == "EXTRA_IN_TARGET", 1).otherwise(0)).alias("extra"),
            F.sum(F.when(F.col("_source_key_count") > 1, 1).otherwise(0)).alias("source_duplicate_records"),
            F.sum(F.when(F.col("_target_key_count") > 1, 1).otherwise(0)).alias("target_duplicate_records"),
            F.countDistinct(F.when(F.col("_source_key_count") > 1, F.col("normalized_primary_key"))).alias("source_duplicate_keys"),
            F.countDistinct(F.when(F.col("_target_key_count") > 1, F.col("normalized_primary_key"))).alias("target_duplicate_keys"),
            F.sum(F.when(F.col("reconciliation_status") == "UNMATCHABLE_SOURCE", 1).otherwise(0)).alias("unmatchable_source"),
            F.sum(F.when(F.col("reconciliation_status") == "UNMATCHABLE_TARGET", 1).otherwise(0)).alias("unmatchable_target"),
            F.sum(F.when(F.col("_source_key_count") == 1, 1).otherwise(0)).alias("source_unique"),
            F.sum(F.when(F.col("_target_key_count") == 1, 1).otherwise(0)).alias("target_unique"),
        ).first().asDict()
        pk_summary_ms = (perf_counter() - summary_started) * 1000
        counts = {
            "primary_matched_count": int(summary.get("matched") or 0),
            "matched_key_count": int(summary.get("matched_keys") or 0),
            "matched_source_record_count": int(summary.get("matched_source_records") or 0),
            "matched_target_record_count": int(summary.get("matched_target_records") or 0),
            "missing_count": int(summary.get("missing") or 0),
            "extra_count": int(summary.get("extra") or 0),
            "source_duplicate_record_count": int(summary.get("source_duplicate_records") or 0),
            "target_duplicate_record_count": int(summary.get("target_duplicate_records") or 0),
            "source_duplicate_key_count": int(summary.get("source_duplicate_keys") or 0),
            "target_duplicate_key_count": int(summary.get("target_duplicate_keys") or 0),
            "unmatchable_source_count": int(summary.get("unmatchable_source") or 0),
            "unmatchable_target_count": int(summary.get("unmatchable_target") or 0),
            "source_unique_key_count": int(summary.get("source_unique") or 0),
            "target_unique_key_count": int(summary.get("target_unique") or 0),
        }
        result = (reconciliation, counts, {"pk_build_ms": pk_build_ms, "pk_summary_ms": pk_summary_ms, "pk_path": "DUPLICATE_KEY_PATH"})
        self._match_cache[cache_key] = result
        return result


    def _possible_key_changes(self, source, target, cfg):
        """Match unresolved rows one-to-one using configured grouping fields."""
        from pyspark.sql import functions as F

        groups = cfg.get("grouping_attributes", []) or []
        keys = cfg.get("comparison_keys", []) or []
        if not groups or not keys:
            return source.limit(0).select(F.lit(None).cast("string").alias("source_key"))

        source_group_columns = [item["source_column"] for item in groups]
        target_group_columns = [item["target_column"] for item in groups]
        source_record = F.struct(*[F.col(column) for column in source.columns])
        target_record = F.struct(*[F.col(column) for column in target.columns])
        source_key = F.to_json(F.struct(*[F.col(item["source_column"]) for item in keys]))
        target_key = F.to_json(F.struct(*[F.col(item["target_column"]) for item in keys]))
        source_key_available = None
        target_key_available = None
        for item in keys:
            source_part = F.col(item["source_column"]).isNotNull() & (F.trim(F.col(item["source_column"]).cast("string")) != "")
            target_part = F.col(item["target_column"]).isNotNull() & (F.trim(F.col(item["target_column"]).cast("string")) != "")
            source_key_available = source_part if source_key_available is None else source_key_available & source_part
            target_key_available = target_part if target_key_available is None else target_key_available & target_part

        source_groups = source.groupBy(*source_group_columns).agg(
            F.count(F.lit(1)).alias("source_candidates"),
            F.first(source_record, ignorenulls=True).alias("source_record"),
            F.first(source_key, ignorenulls=True).alias("source_key"),
            F.max(source_key_available.cast("int")).alias("source_key_available"),
        ).alias("s")
        target_groups = target.groupBy(*target_group_columns).agg(
            F.count(F.lit(1)).alias("target_candidates"),
            F.first(target_record, ignorenulls=True).alias("target_record"),
            F.first(target_key, ignorenulls=True).alias("target_key"),
            F.max(target_key_available.cast("int")).alias("target_key_available"),
        ).alias("t")

        condition = None
        for source_column, target_column in zip(source_group_columns, target_group_columns):
            pair = F.col(f"s.`{source_column}`").eqNullSafe(F.col(f"t.`{target_column}`"))
            condition = pair if condition is None else condition & pair

        group_value = F.array(*[
            F.coalesce(F.col(f"s.`{source_column}`"), F.col(f"t.`{target_column}`")).cast("string")
            for source_column, target_column in zip(source_group_columns, target_group_columns)
        ])
        return source_groups.join(target_groups, condition, "inner").filter(
            ((F.col("source_candidates") == 1) & (F.col("target_candidates") == 1))
            | (F.col("source_key_available") != F.col("target_key_available"))
        ).select(
            group_value.alias("group_key"),
            "source_key",
            "target_key",
            "source_record",
            "target_record",
            F.when(
                F.col("source_key_available") != F.col("target_key_available"),
                F.lit("The configured matching attributes identify the same record, but its business key is missing on one side"),
            ).when(
                (F.col("source_key_available") == 1) & (F.col("target_key_available") == 1),
                F.lit("Records share the configured matching attributes but use different business keys"),
            ).otherwise(
                F.lit("Records have no usable business key and were paired by the configured matching attributes")
            ).alias("reason"),
            F.when(
                F.col("source_key_available") != F.col("target_key_available"),
                F.lit("MISSING_BUSINESS_KEY"),
            ).when(
                (F.col("source_key_available") == 1) & (F.col("target_key_available") == 1),
                F.lit("POSSIBLE_KEY_CHANGE"),
            ).otherwise(F.lit("MATCHED_BY_ATTRIBUTES")).alias("status"),
        )

    @staticmethod
    def _mapping_lookup(cfg):
        return {
            item.get("source_column"): item
            for item in (cfg.get("column_mappings", []) or [])
            if isinstance(item, dict) and item.get("source_column") and item.get("target_column")
        }

    @staticmethod
    def _apply_mapping_normalization(expr, mapping):
        """Apply only value normalization; matching identity is never changed."""
        from pyspark.sql import functions as F
        normalization = dict((mapping or {}).get("normalization") or {})
        value = expr
        if normalization.get("empty_as_null"):
            value = F.when(F.trim(value.cast("string")) == "", F.lit(None)).otherwise(value)
        if normalization.get("trim"):
            value = F.trim(value.cast("string"))
        if normalization.get("case_insensitive"):
            value = F.lower(value.cast("string"))
        if normalization.get("round") is not None:
            value = F.round(value.cast("double"), int(normalization["round"]))
        return value

    def _matched_row_hashes(self, pairs, resolved_pairs):
        """Attach canonical SHA-256 content hashes to authoritative PK pairs.

        Hashing is deliberately *after* unique-PK reconciliation.  It is never
        used to manufacture row identity for duplicate or null keys.  Equal
        hashes let L4 skip rows that are provably identical after configured
        normalization; unequal hashes remain candidates for normal field/tolerance
        evaluation.
        """
        from pyspark.sql import functions as F
        if not resolved_pairs:
            return pairs.withColumn("__source_row_hash", F.lit(None).cast("string")).withColumn("__target_row_hash", F.lit(None).cast("string"))

        source_parts = []
        target_parts = []
        for sc, tc, mapping in resolved_pairs:
            sv = self._apply_mapping_normalization(F.col(f"_s.`{sc}`"), mapping)
            tv = self._apply_mapping_normalization(F.col(f"_t.`{tc}`"), mapping)
            # Length-prefixing/field separators avoid ambiguous concatenations.
            source_parts.append(F.concat(F.lit(sc + "="), F.coalesce(sv.cast("string"), F.lit("<NULL>"))))
            target_parts.append(F.concat(F.lit(sc + "="), F.coalesce(tv.cast("string"), F.lit("<NULL>"))))

        return (pairs
            .withColumn("__source_row_hash", F.sha2(F.concat_ws("\u001e", *source_parts), 256))
            .withColumn("__target_row_hash", F.sha2(F.concat_ws("\u001e", *target_parts), 256)))

    @staticmethod
    def _resolve_l4_column_pairs(source_columns, target_columns, cfg):
        """Resolve L4 fields exactly like the canonical FieldComparator.

        Explicit column mappings are overrides, not an allow-list. Any source
        column without an explicit mapping compares to the same-named target
        column when that target exists. Comparison-key source fields and
        ignored source fields are excluded from field-level checks.
        """
        explicit_mappings = {
            mapping.get("source_column"): mapping
            for mapping in cfg.get("column_mappings", [])
            if isinstance(mapping, dict)
            and mapping.get("source_column")
            and mapping.get("target_column")
        }
        ignored = set(cfg.get("ignored_columns", []))
        key_sources = {
            key.get("source_column")
            for key in cfg.get("comparison_keys", [])
            if isinstance(key, dict) and key.get("source_column")
        }
        target_set = set(target_columns)

        resolved = []
        for source_column in source_columns:
            if source_column in ignored or source_column in key_sources:
                continue
            mapping = explicit_mappings.get(source_column, {})
            target_column = mapping.get("target_column", source_column)
            if target_column in ignored:
                continue
            if target_column not in target_set:
                continue
            resolved.append((source_column, target_column, mapping))
        return resolved


    def _agg_expr(self, op, col):
        from pyspark.sql import functions as F
        return {"SUM":F.sum,"AVG":F.avg,"MIN":F.min,"MAX":F.max,"COUNT":F.count}[op](F.col(col) if col else F.lit(1))


    def _group(self,s,t,cfg):
        from pyspark.sql import functions as F
        group_started = perf_counter()
        groups=cfg.get("grouping_attributes",[]) or []
        aggs=cfg.get("aggregation_columns",[]) or []
        if not groups:
            raise ValueError("Group reconciliation requires grouping_attributes")

        mapping_lookup = self._mapping_lookup(cfg)
        group_aliases = [f"__g{i}" for i in range(len(groups))]

        def mapping_for_pair(source_column, target_column):
            mapping = mapping_lookup.get(source_column)
            if mapping and mapping.get("target_column") == target_column:
                return mapping
            return {}

        def prepare(df, side):
            prepared = df
            for i, item in enumerate(groups):
                sc=item["source_column"]; tc=item["target_column"]
                col = sc if side == "source" else tc
                mapping = mapping_for_pair(sc, tc)
                prepared = prepared.withColumn(
                    group_aliases[i],
                    self._apply_mapping_normalization(F.col(col), mapping),
                )
            return prepared

        def agg_expr(op, expr):
            op = op.upper()
            if op == "SUM": return F.sum(expr)
            if op == "AVG": return F.avg(expr)
            if op == "MIN": return F.min(expr)
            if op == "MAX": return F.max(expr)
            if op == "COUNT": return F.count(expr)
            raise ValueError(f"Unsupported group aggregation operation: {op}")

        prepared_caches = []

        def build(df, side):
            # Base aggregates and MODE calculations reuse the same normalized
            # rows. Persist once so Spark does not rebuild normalization and the
            # upstream fallback lineage for each aggregation branch.
            prepared = prepare(df, side).persist()
            prepared_caches.append(prepared)
            record_struct = F.struct(*[F.col(column) for column in df.columns])
            aggregate_exprs=[
                F.count(F.lit(1)).alias("__present"),
                F.first(record_struct, ignorenulls=True).alias("__record"),
            ]
            mode_specs=[]
            for i,item in enumerate(aggs):
                sc=item["source_column"]; tc=item["target_column"]
                col = sc if side == "source" else tc
                op=str(item.get("operation","AVG")).upper()
                mapping = mapping_for_pair(sc, tc)
                value_expr = self._apply_mapping_normalization(F.col(col), mapping)
                if op == "MODE":
                    mode_specs.append((i, value_expr))
                else:
                    aggregate_exprs.append(agg_expr(op, value_expr).alias(f"a{i}"))

            result = prepared.groupBy(*group_aliases).agg(*aggregate_exprs)

            # Deterministic MODE without a Window/sort stage. First count each
            # value inside the group, then choose the value with the smallest
            # ordering tuple (-frequency, lexical_value): highest frequency wins;
            # ties resolve to the lexically smallest normalized value, exactly as
            # before. min_by is available in Spark 3.5.3.
            for index, value_expr in mode_specs:
                mode_values = prepared.select(
                    *[F.col(alias) for alias in group_aliases],
                    value_expr.alias("__mode_value"),
                ).filter(F.col("__mode_value").isNotNull())

                counts = mode_values.groupBy(
                    *group_aliases, "__mode_value"
                ).agg(
                    F.count(F.lit(1)).alias("__mode_count")
                )

                ordering = F.struct(
                    (-F.col("__mode_count")).alias("frequency_order"),
                    F.col("__mode_value").cast("string").alias("lexical_order"),
                )

                modes = counts.groupBy(*group_aliases).agg(
                    F.min_by(F.col("__mode_value"), ordering).alias(f"a{index}")
                )

                result = result.join(modes, group_aliases, "left")

            return result

        a=build(s,"source").alias("s")
        b=build(t,"target").alias("t")
        cond=None
        for alias in group_aliases:
            q=F.col(f"s.`{alias}`").eqNullSafe(F.col(f"t.`{alias}`"))
            cond=q if cond is None else cond&q

        # Cache the expensive grouped source/target join.  The summary action
        # below materializes it once; bounded evidence reuses the cached rows.
        j=a.join(b,cond,"full_outer").persist()

        source_present = F.col("s.__present").isNotNull()
        target_present = F.col("t.__present").isNotNull()
        common_present = source_present & target_present
        row_count_mismatch = common_present & (F.col("s.__present") != F.col("t.__present"))
        duplicate_group = (
            (F.coalesce(F.col("s.__present"), F.lit(0)) > 1)
            | (F.coalesce(F.col("t.__present"), F.lit(0)) > 1)
        )

        # Build the aggregate mismatch/applicability expressions once.  These
        # are evaluated directly on one joined group row, so all metrics can be
        # computed in the same Spark aggregation that materializes `j`.
        aggregate_applicable=[]
        aggregate_failed=[]
        for index,item in enumerate(aggs):
            source_value=F.col(f"s.a{index}")
            target_value=F.col(f"t.a{index}")
            both_null=source_value.isNull() & target_value.isNull()
            applicable=common_present & ~both_null
            failed=applicable & ~source_value.eqNullSafe(target_value)
            aggregate_applicable.append(applicable)
            aggregate_failed.append(failed)

        any_group_failure = row_count_mismatch | duplicate_group
        for failed in aggregate_failed:
            any_group_failure = any_group_failure | failed

        summary_exprs=[
            F.sum(F.when(source_present,1).otherwise(0)).alias("source_groups"),
            F.sum(F.when(target_present,1).otherwise(0)).alias("target_groups"),
            F.sum(F.when(common_present,1).otherwise(0)).alias("common_groups"),
            F.sum(F.when(row_count_mismatch,1).otherwise(0)).alias("row_count_checks_failed"),
            F.sum(F.when(duplicate_group,1).otherwise(0)).alias("duplicate_checks_failed"),
            F.sum(F.when(any_group_failure,1).otherwise(0)).alias("mismatch_groups"),
        ]
        for index, applicable in enumerate(aggregate_applicable):
            summary_exprs.append(
                F.sum(F.when(applicable,1).otherwise(0)).alias(f"aggregate_applicable_{index}")
            )
        for index, failed in enumerate(aggregate_failed):
            summary_exprs.append(
                F.sum(F.when(failed,1).otherwise(0)).alias(f"aggregate_failed_{index}")
            )

        summary_started = perf_counter()
        summary=j.agg(*summary_exprs).first()
        summary_ms = (perf_counter() - summary_started) * 1000

        # `j` is now fully materialized. The normalized per-side inputs are no
        # longer needed and can be released before evidence scans the cached join.
        for prepared in prepared_caches:
            try:
                prepared.unpersist(blocking=False)
            except Exception:
                logger.debug("Unable to unpersist prepared group dataset", exc_info=True)

        source_groups=int(summary["source_groups"] or 0)
        target_groups=int(summary["target_groups"] or 0)
        common=int(summary["common_groups"] or 0)
        missing=source_groups-common
        extra=target_groups-common

        row_count_checks_failed=int(summary["row_count_checks_failed"] or 0)
        duplicate_checks_failed=int(summary["duplicate_checks_failed"] or 0)
        aggregate_field_checks=sum(int(summary[f"aggregate_applicable_{i}"] or 0) for i in range(len(aggs)))
        aggregate_field_failed=sum(int(summary[f"aggregate_failed_{i}"] or 0) for i in range(len(aggs)))
        aggregate_checks_total=row_count_checks_failed+duplicate_checks_failed+aggregate_field_checks
        aggregate_checks_failed=row_count_checks_failed+duplicate_checks_failed+aggregate_field_failed
        mismatch_groups=int(summary["mismatch_groups"] or 0)

        group_key = F.array(*[
            F.coalesce(F.col(f"s.`{alias}`").cast("string"), F.col(f"t.`{alias}`").cast("string"))
            for alias in group_aliases
        ])
        presence_rows=j.filter(F.col("s.__present").isNull() | F.col("t.__present").isNull()).select(
            group_key.alias("group_key"),
            F.col("s.__record").alias("source_record"), F.col("t.__record").alias("target_record"),
            F.lit(None).cast("string").alias("source_aggregate"), F.lit(None).cast("string").alias("target_aggregate"),
            F.lit(None).cast("string").alias("source_column"), F.lit(None).cast("string").alias("target_column"),
            F.lit(None).cast("string").alias("operation"), F.lit(None).cast("double").alias("difference"),
            F.when(F.col("s.__present").isNull(), F.lit("EXTRA_GROUP_IN_TARGET")).otherwise(F.lit("MISSING_GROUP_IN_TARGET")).alias("status"),
        ).withColumn("matched", F.lit(False))

        common_rows=j.filter(common_present)
        count_mismatch_rows=common_rows.filter(F.col("s.__present") != F.col("t.__present")).select(
            group_key.alias("group_key"),
            F.col("s.__record").alias("source_record"), F.col("t.__record").alias("target_record"),
            F.col("s.__present").cast("string").alias("source_aggregate"),
            F.col("t.__present").cast("string").alias("target_aggregate"),
            F.lit("Rows").alias("source_column"), F.lit("Rows").alias("target_column"),
            F.lit("COUNT").alias("operation"),
            (F.col("t.__present").cast("double")-F.col("s.__present").cast("double")).alias("difference"),
            F.lit("GROUP_ROW_COUNT_MISMATCH").alias("status"), F.lit(False).alias("matched"),
        )
        duplicate_group_rows=j.filter(
            (F.coalesce(F.col("s.__present"), F.lit(0)) > 1)
            | (F.coalesce(F.col("t.__present"), F.lit(0)) > 1)
        ).select(
            group_key.alias("group_key"),
            F.col("s.__record").alias("source_record"), F.col("t.__record").alias("target_record"),
            F.coalesce(F.col("s.__present"), F.lit(0)).cast("string").alias("source_aggregate"),
            F.coalesce(F.col("t.__present"), F.lit(0)).cast("string").alias("target_aggregate"),
            F.lit("Rows").alias("source_column"), F.lit("Rows").alias("target_column"),
            F.lit("DUPLICATE COUNT").alias("operation"),
            (F.coalesce(F.col("t.__present"), F.lit(0)).cast("double")
             - F.coalesce(F.col("s.__present"), F.lit(0)).cast("double")).alias("difference"),
            F.lit("GROUP_DUPLICATE_ROWS").alias("status"), F.lit(False).alias("matched"),
        )
        aggregate_structs=[]
        for index,item in enumerate(aggs):
            source_value,target_value=F.col(f"s.a{index}"),F.col(f"t.a{index}")
            status=(F.when(source_value.isNull() & target_value.isNull(), F.lit("NOT_APPLICABLE"))
                    .when(source_value.eqNullSafe(target_value), F.lit("PASS"))
                    .otherwise(F.lit("GROUP_VALUE_MISMATCH")))
            aggregate_structs.append(F.struct(
                F.col("s.__record").alias("source_record"),
                F.col("t.__record").alias("target_record"),
                source_value.cast("string").alias("source_aggregate"),
                target_value.cast("string").alias("target_aggregate"),
                F.lit(item["source_column"]).alias("source_column"),
                F.lit(item["target_column"]).alias("target_column"),
                F.lit(str(item.get("operation","AVG")).upper()).alias("operation"),
                (target_value.cast("double")-source_value.cast("double")).alias("difference"),
                status.alias("status"),
                status.isin("PASS","NOT_APPLICABLE").alias("matched"),
            ))
        aggregate_rows=(common_rows.select(group_key.alias("group_key"),F.explode(F.array(*aggregate_structs)).alias("aggregate"))
                        .select("group_key","aggregate.*")) if aggregate_structs else None
        result_rows=presence_rows.unionByName(count_mismatch_rows).unionByName(duplicate_group_rows)
        if aggregate_rows is not None:
            result_rows=result_rows.unionByName(aggregate_rows)

        metrics={
            "status":"PASS" if missing+extra+mismatch_groups==0 else "FAIL",
            "matching_mode":"GROUP_RECONCILIATION","comparison_mode":"GROUP_RECONCILIATION",
            "source_group_count":source_groups,"target_group_count":target_groups,"group_count":source_groups+extra,
            "common_group_count":common,"matched_group_count":common,"missing_group_count":missing,"extra_group_count":extra,
            "groups_with_aggregate_mismatch":mismatch_groups,"groups_with_mismatch":mismatch_groups,
            "group_mismatch_count":mismatch_groups,"group_difference_count":missing+extra+mismatch_groups,
            "mismatch_group_count":mismatch_groups,"mismatch_count":missing+extra+mismatch_groups,
            "source_group_coverage_pct":safe_rate_pct(common,source_groups,zero_value=100.0),
            "target_group_coverage_pct":safe_rate_pct(common,target_groups,zero_value=100.0),
            "source_group_coverage":safe_rate_pct(common,source_groups,zero_value=100.0),
            "target_group_coverage":safe_rate_pct(common,target_groups,zero_value=100.0),
            "aggregate_checks_total":aggregate_checks_total,"aggregate_check_count":aggregate_checks_total,
            "aggregate_checks_passed":aggregate_checks_total-aggregate_checks_failed,"aggregate_checks_failed":aggregate_checks_failed,
            "checks_total":aggregate_checks_total,"checks_passed":aggregate_checks_total-aggregate_checks_failed,"checks_failed":aggregate_checks_failed,
        }

        # Only exception evidence is collected. `j` is already materialized by
        # the single summary action above, so this bounded sample scans cached
        # group rows without recomputing source/target aggregation.
        exception_rows=result_rows.filter(~F.col("status").isin("PASS","NOT_APPLICABLE"))
        exception_count=missing+extra+aggregate_checks_failed
        evidence_started = perf_counter()
        bounded_evidence=self._bounded(exception_rows,exception_count)
        evidence_ms = (perf_counter() - evidence_started) * 1000
        total_ms = (perf_counter() - group_started) * 1000
        logger.info("SPARK_GROUP_OPT_TIMING summary_ms=%.1f evidence_ms=%.1f total_ms=%.1f", summary_ms, evidence_ms, total_ms)
        print(f"SPARK_GROUP_OPT_TIMING summary_ms={summary_ms:.1f} evidence_ms={evidence_ms:.1f} total_ms={total_ms:.1f}")
        try:
            j.unpersist(blocking=False)
        except Exception:
            logger.debug("Unable to unpersist group reconciliation join", exc_info=True)
        return {"metrics":metrics,"evidence":{"group_reconciliation":bounded_evidence}}


    def _bounded(self, df, total_count: int | None = None):
        if df is None:
            return {"count": 0, "sample": [], "truncated": False}
        # If the caller already proved that this evidence bucket is empty, do
        # not launch a Spark job merely to collect zero rows.
        if total_count is not None and int(total_count) == 0:
            return {"count": 0, "sample": [], "truncated": False}
        # The public count remains exact. Where the caller already has it,
        # avoid a full count() and collect one bounded extra row to determine
        # truncation. Otherwise retain the existing exact-count contract.
        rows = df.limit(self.evidence_limit + 1).collect()
        truncated = len(rows) > self.evidence_limit
        sample = [row.asDict(recursive=True) for row in rows[:self.evidence_limit]]
        if total_count is None:
            total_count = df.count()
        return {"count": total_count, "sample": sample, "truncated": truncated}

    def _normalize_contract(self, level, result):
        """Ensure every Spark level uses the same public result envelope as local execution."""
        metrics = result.setdefault("metrics", {})
        evidence = result.setdefault("evidence", {})
        metrics.setdefault("status", "PASS")
        if level == ComparisonLevel.L5:
            total = metrics.setdefault("checks_total", 0); failed = metrics.setdefault("checks_failed", 0)
            metrics.setdefault("checks_passed", total - failed)
            metrics.setdefault("aggregate_check_pass_rate_pct", safe_rate_pct(metrics["checks_passed"], total, zero_value=100.0))
            metrics.setdefault("aggregate_check_failure_rate_pct", safe_rate_pct(failed, total))
        elif level == ComparisonLevel.L6:
            total = metrics.setdefault("checks_total", 0); failed = metrics.setdefault("checks_failed", 0)
            metrics.setdefault("checks_passed", total - failed)
            metrics.setdefault("pass_percentage", safe_rate_pct(metrics["checks_passed"], total, zero_value=100.0))
            metrics.setdefault("failure_percentage", safe_rate_pct(failed, total))
            evidence.setdefault("dq_results", evidence.get("rule_results", []))
        return result