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
        from pyspark.sql import functions as spark_functions

        cache_key = json.dumps(dataset, sort_keys=True, default=str)
        cached = self._dataset_cache.get(cache_key)
        if cached is not None:
            return cached

        connector = str(dataset.get("connector_type", "")).lower()
        properties = dataset.get("properties", {}) or {}
        filters_already_applied = False

        if connector == "csv":
            path = properties.get("path")
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
            dataframe = (
                self.spark.read.schema(self._csv_schema(dataset))
                .option("header", "true")
                .option("delimiter", properties.get("delimiter", ","))
                .csv(path)
            )
        elif connector in {"postgres", "postgresql"}:
            connection_properties = properties.get("connection", properties)
            host = connection_properties.get("host", "postgres")
            port = connection_properties.get("port", 5432)
            database = (
                connection_properties.get("database")
                or connection_properties.get("dbname")
            )
            table = properties.get("table")
            schema = properties.get("schema")
            if schema and table:
                table = f"{schema}.{table}"
            url = f"jdbc:postgresql://{host}:{port}/{database}"
            dataframe = (
                self.spark.read.format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option(
                    "user",
                    connection_properties.get("user")
                    or connection_properties.get("username"),
                )
                .option("password", connection_properties.get("password"))
                .option("driver", "org.postgresql.Driver")
                .load()
            )

        elif connector == "databricks":
            dataframe = self._load_databricks(dataset)
            # DatabricksConnector.iter_chunks() executes the configured
            # filter clause in Databricks SQL before returning rows.
            filters_already_applied = True

        else:
            raise ValueError(
                "Spark executor supports CSV, PostgreSQL, and Databricks "
                f"datasets; got '{connector}'"
            )

        filters = (
            []
            if filters_already_applied
            else (properties.get("_filters", []) or [])
        )
        for filter_item in filters:
            field = filter_item.get("field")
            if field not in dataframe.columns:
                raise ValueError(f"Unknown filter field: {field}")

            value = filter_item.get("value")
            operator = self._normalize_filter_operator(
                filter_item.get("operator", "EQ")
            )
            expression = self._filter_expression(
                spark_functions.col(field),
                operator,
                value,
            )
            dataframe = dataframe.filter(expression)

        # L1-L6 reuse the same filtered datasets. Persisting prevents every
        # level from rereading CSV/JDBC input while keeping data distributed.
        dataframe = dataframe.persist()
        # Materialize once now; subsequent L1-L6 actions reuse this cached,
        # filtered DataFrame rather than re-reading the input file.
        dataframe.count()
        self._dataset_cache[cache_key] = dataframe
        return dataframe

    @staticmethod
    def _normalize_filter_operator(operator: Any) -> str:
        aliases = {
            "=": "EQ",
            "!=": "NE",
            ">": "GT",
            ">=": "GTE",
            "<": "LT",
            "<=": "LTE",
            "IS NULL": "IS_NULL",
            "IS NOT NULL": "IS_NOT_NULL",
        }
        raw_operator = str(operator).upper().strip()
        return aliases.get(raw_operator, raw_operator.replace(" ", "_"))

    @staticmethod
    def _filter_expression(column, operator: str, value: Any):
        comparisons = {
            "EQ": column == value,
            "NE": column != value,
            "GT": column > value,
            "GTE": column >= value,
            "LT": column < value,
            "LTE": column <= value,
        }
        expression = comparisons.get(operator)

        if operator == "IN":
            return column.isin(value if isinstance(value, list) else [value])
        if operator == "IS_NULL":
            return column.isNull()
        if operator == "IS_NOT_NULL":
            return column.isNotNull()
        if expression is None:
            raise ValueError(f"Unsupported Spark filter operator: {operator}")

        return expression

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

        metadata = (
            self.connector_manager.get_schema("databricks", dataset)
            if self.connector_manager is not None
            else connector.get_schema(dataset)
        )
        spark_schema = self._spark_schema_from_databricks_metadata(metadata)

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
                chunk,
                spark_schema,
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
                schema=spark_schema,
            )

        return combined

    def _databricks_chunk_dataframe(
        self,
        records: list[dict[str, Any]],
        schema,
    ):
        """Apply the declared Databricks schema in both execution engines."""
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
        metadata = DatabricksConnector().get_schema(dataset)
        return SparkExecutor._spark_schema_from_databricks_metadata(metadata)

    @staticmethod
    def _spark_schema_from_databricks_metadata(metadata):
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
            elif upper_type in {"DECIMAL", "NUMERIC"}:
                data_type = DecimalType(38, 18)
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
        from app.connectors.csv import CSVMetadataProvider
        from pyspark.sql.types import (
            BooleanType,
            DecimalType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

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
    def _maps(configuration):
        return {
            mapping["source_column"]: mapping["target_column"]
            for mapping in configuration.get("column_mappings", [])
        }


    def _key_exprs(self, configuration, side):
        from pyspark.sql import functions as spark_functions

        column_key = "source_column" if side == "source" else "target_column"
        return [
            spark_functions.col(key[column_key]).cast("string")
            for key in configuration.get("comparison_keys", [])
        ]

    @staticmethod
    def _keys_populated(keys):
        from pyspark.sql import functions as spark_functions

        populated_expression = None
        for key in keys:
            key_populated = key.isNotNull() & (spark_functions.trim(key) != "")
            populated_expression = (
                key_populated
                if populated_expression is None
                else populated_expression & key_populated
            )
        return populated_expression

    def _stats(self, dataframe, configuration, side):
        from pyspark.sql import functions as spark_functions

        cache_key = (id(dataframe), side)
        if cache_key in self._stats_cache:
            return self._stats_cache[cache_key]

        keys = self._key_exprs(configuration, side)
        key_value = spark_functions.concat_ws("\u001f", *keys) if keys else None
        valid = self._keys_populated(keys)

        aggregates = [
            spark_functions.count(spark_functions.lit(1)).alias("total_rows"),
            *[
                spark_functions.sum(
                    spark_functions.when(
                        spark_functions.col(column).isNull(),
                        1,
                    ).otherwise(0)
                ).alias(column)
                for column in dataframe.columns
            ],
        ]
        if valid is not None:
            aggregates.extend(
                [
                    spark_functions.sum(
                        spark_functions.when(valid, 1).otherwise(0)
                    ).alias("keyed_rows"),
                    spark_functions.countDistinct(
                        spark_functions.when(valid, key_value)
                    ).alias("distinct_key_count"),
                ]
            )

        base = dataframe.agg(*aggregates).first().asDict()
        total = int(base.pop("total_rows") or 0)
        if keys:
            distinct = base.pop("distinct_key_count") or 0
            dup = (base.pop("keyed_rows") or 0) - distinct
        else:
            distinct = dup = None

        null_counts = {
            column: int(count or 0)
            for column, count in base.items()
        }
        result = {
            "total_rows": total,
            "filtered_rows": total,
            "partition_rows": total,
            "distinct_key_count": distinct,
            "duplicate_key_count": dup,
            "null_counts": null_counts,
        }
        self._stats_cache[cache_key] = result
        return result


    def _joined(self, source_dataframe, target_dataframe, configuration):
        from pyspark.sql import functions as spark_functions

        keys = configuration.get("comparison_keys", [])
        if not keys:
            raise ValueError("Spark row comparison requires comparison_keys")

        source = source_dataframe.alias("source")
        target = target_dataframe.alias("target")
        condition = None
        for key in keys:
            pair_condition = spark_functions.col(f"source.`{key['source_column']}`").eqNullSafe(
                spark_functions.col(f"target.`{key['target_column']}`")
            )
            condition = (
                pair_condition
                if condition is None
                else condition & pair_condition
            )

        return source.join(target, condition, "full_outer"), condition

    @staticmethod
    def _match_cache_key(configuration, keys):
        return json.dumps(
            {
                "source": configuration.get("source"),
                "target": configuration.get("target"),
                "comparison_keys": keys,
            },
            sort_keys=True,
            default=str,
        )

    @staticmethod
    def _business_key_available(columns):
        from pyspark.sql import functions as spark_functions

        available = None
        for column in columns:
            part = (
                spark_functions.col(column).isNotNull()
                & (spark_functions.trim(spark_functions.col(column).cast("string")) != "")
            )
            available = part if available is None else available & part
        return available

    def _matched_pairs(self, source_dataframe, target_dataframe, configuration):
        """Build the authoritative PK reconciliation using the cheapest safe Spark path.

        L2 normally computes and caches per-side key statistics before L3.  When
        those statistics prove that populated business keys are unique on both
        sides, the expensive duplicate-key Window/group/join machinery is not
        needed.  In that common case we use one direct distributed full-outer
        join on the normalized PK.  If either side contains duplicates, the
        existing deterministic occurrence-matching implementation is used
        unchanged.
        """
        from pyspark.sql import functions as spark_functions

        keys = configuration.get("comparison_keys", [])
        if not keys:
            raise ValueError("Spark row comparison requires comparison_keys")

        cache_key = self._match_cache_key(configuration, keys)
        cached = self._match_cache.get(cache_key)
        if cached is not None:
            return cached

        # These are normally cache hits because L2 runs before L3.  If L3 is
        # invoked independently they are still exact distributed Spark stats.
        source_stats = self._stats(source_dataframe, configuration, "source")
        target_stats = self._stats(target_dataframe, configuration, "target")
        source_duplicates = int(source_stats.get("duplicate_key_count") or 0)
        target_duplicates = int(target_stats.get("duplicate_key_count") or 0)

        # Preserve the full deterministic duplicate-occurrence algorithm for
        # the complex case.  Nothing about duplicate semantics changes.
        if source_duplicates > 0 or target_duplicates > 0:
            return self._matched_pairs_with_duplicates(
                source_dataframe,
                target_dataframe,
                configuration,
            )

        build_started = perf_counter()

        source = source_dataframe.withColumn(
            "__source_row_id",
            spark_functions.monotonically_increasing_id(),
        )
        target = target_dataframe.withColumn(
            "__target_row_id",
            spark_functions.monotonically_increasing_id(),
        )

        source_valid = None
        target_valid = None
        for item in keys:
            source_column = item["source_column"]
            target_column = item["target_column"]
            source_populated = (
                spark_functions.col(source_column).isNotNull()
                & (
                    spark_functions.trim(
                        spark_functions.col(source_column).cast("string")
                    )
                    != ""
                )
            )
            target_populated = (
                spark_functions.col(target_column).isNotNull()
                & (
                    spark_functions.trim(
                        spark_functions.col(target_column).cast("string")
                    )
                    != ""
                )
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

        source_key = spark_functions.concat_ws(
            "\u001f",
            *[spark_functions.col(item["source_column"]).cast("string") for item in keys],
        )
        target_key = spark_functions.concat_ws(
            "\u001f",
            *[spark_functions.col(item["target_column"]).cast("string") for item in keys],
        )

        source_prepared = (
            source
            .withColumn(
                "__normalized_primary_key",
                spark_functions.when(source_valid, source_key),
            )
            .withColumn(
                "__key_count",
                spark_functions.when(
                    source_valid,
                    spark_functions.lit(1),
                ).otherwise(spark_functions.lit(None).cast("int")),
            )
        )
        target_prepared = (
            target
            .withColumn(
                "__normalized_primary_key",
                spark_functions.when(target_valid, target_key),
            )
            .withColumn(
                "__key_count",
                spark_functions.when(
                    target_valid,
                    spark_functions.lit(1),
                ).otherwise(spark_functions.lit(None).cast("int")),
            )
        )

        # Normal equality deliberately does NOT match NULL keys.  Rows without
        # usable PKs therefore remain separate UNMATCHABLE_SOURCE/TARGET rows.
        joined = source_prepared.alias("source").join(
            target_prepared.alias("target"),
            spark_functions.col("source.__normalized_primary_key")
            == spark_functions.col("target.__normalized_primary_key"),
            "full_outer",
        )

        source_struct = spark_functions.struct(
            *[
                spark_functions.col(f"source.`{field.name}`").alias(field.name)
                for field in source_dataframe.schema.fields
            ]
        )
        target_struct = spark_functions.struct(
            *[
                spark_functions.col(f"target.`{field.name}`").alias(field.name)
                for field in target_dataframe.schema.fields
            ]
        )
        source_key_json = spark_functions.to_json(
            spark_functions.struct(
                *[
                    spark_functions.col(f"source.`{item['source_column']}`")
                    for item in keys
                ]
            )
        )
        target_key_json = spark_functions.to_json(
            spark_functions.struct(
                *[
                    spark_functions.col(f"target.`{item['target_column']}`")
                    for item in keys
                ]
            )
        )

        reconciliation = joined.select(
            source_struct.alias("_s"),
            target_struct.alias("_t"),
            spark_functions.coalesce(
                spark_functions.col("source.__normalized_primary_key"),
                spark_functions.col("target.__normalized_primary_key"),
            ).alias("normalized_primary_key"),
            spark_functions.when(
                spark_functions.col("source.__source_row_id").isNotNull()
                & spark_functions.col("target.__target_row_id").isNotNull(),
                spark_functions.lit("MATCHED"),
            )
            .when(
                spark_functions.col("source.__source_row_id").isNotNull()
                & spark_functions.col("source.__normalized_primary_key").isNull(),
                spark_functions.lit("UNMATCHABLE_SOURCE"),
            )
            .when(
                spark_functions.col("target.__target_row_id").isNotNull()
                & spark_functions.col("target.__normalized_primary_key").isNull(),
                spark_functions.lit("UNMATCHABLE_TARGET"),
            )
            .when(
                spark_functions.col("source.__source_row_id").isNotNull(),
                spark_functions.lit("MISSING_IN_TARGET"),
            )
            .otherwise(spark_functions.lit("EXTRA_IN_TARGET"))
            .alias("reconciliation_status"),
            spark_functions.when(
                spark_functions.col("source.__source_row_id").isNotNull()
                & spark_functions.col("target.__target_row_id").isNotNull(),
                spark_functions.lit("PRIMARY_KEY"),
            )
            .otherwise(spark_functions.lit(None).cast("string"))
            .alias("match_type"),
            spark_functions.when(
                spark_functions.col("source.__source_row_id").isNotNull(),
                source_key_json,
            )
            .otherwise(target_key_json)
            .alias("match_key"),
            spark_functions.col("source.__key_count").alias("_source_key_count"),
            spark_functions.col("target.__key_count").alias("_target_key_count"),
            spark_functions.col("source.__source_row_id").alias("_source_row_id"),
            spark_functions.col("target.__target_row_id").alias("_target_row_id"),
        ).persist()

        pk_build_ms = (perf_counter() - build_started) * 1000
        summary_started = perf_counter()

        # Unique keys make matched keys == matched records.  Avoid expensive
        # countDistinct expressions and duplicate bookkeeping on this path.
        summary = reconciliation.agg(
            spark_functions.sum(
                spark_functions.when(spark_functions.col("reconciliation_status") == "MATCHED", 1).otherwise(0)
            ).alias("matched"),
            spark_functions.sum(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "MISSING_IN_TARGET", 1
                ).otherwise(0)
            ).alias("missing"),
            spark_functions.sum(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "EXTRA_IN_TARGET", 1
                ).otherwise(0)
            ).alias("extra"),
            spark_functions.sum(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "UNMATCHABLE_SOURCE", 1
                ).otherwise(0)
            ).alias("unmatchable_source"),
            spark_functions.sum(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "UNMATCHABLE_TARGET", 1
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

    def _matched_pairs_with_duplicates(
        self,
        source_dataframe,
        target_dataframe,
        configuration,
    ):
        """Build the one authoritative, full-outer PK reconciliation stream."""
        from pyspark.sql import functions as spark_functions
        from pyspark.sql.window import Window

        keys = configuration.get("comparison_keys", [])
        if not keys:
            raise ValueError("Spark row comparison requires comparison_keys")

        cache_key = self._match_cache_key(configuration, keys)
        cached = self._match_cache.get(cache_key)
        if cached is not None:
            return cached

        build_started = perf_counter()
        source = source_dataframe.withColumn(
            "__source_row_id",
            spark_functions.monotonically_increasing_id(),
        )
        target = target_dataframe.withColumn(
            "__target_row_id",
            spark_functions.monotonically_increasing_id(),
        )

        source_key_columns = [key["source_column"] for key in keys]
        target_key_columns = [key["target_column"] for key in keys]
        source_valid = self._business_key_available(source_key_columns)
        target_valid = self._business_key_available(target_key_columns)

        source_key = spark_functions.concat_ws(
            "\u001f",
            *[spark_functions.col(column).cast("string") for column in source_key_columns],
        )
        target_key = spark_functions.concat_ws(
            "\u001f",
            *[spark_functions.col(column).cast("string") for column in target_key_columns],
        )
        source_prepared = source.withColumn(
            "__normalized_primary_key",
            spark_functions.when(source_valid, source_key),
        )
        target_prepared = target.withColumn(
            "__normalized_primary_key",
            spark_functions.when(target_valid, target_key),
        )
        source_prepared = source_prepared.withColumn(
            "__key_count",
            spark_functions.when(
                source_valid,
                spark_functions.count("__source_row_id").over(
                    Window.partitionBy("__normalized_primary_key")
                ),
            ),
        )
        target_prepared = target_prepared.withColumn(
            "__key_count",
            spark_functions.when(
                target_valid,
                spark_functions.count("__target_row_id").over(
                    Window.partitionBy("__normalized_primary_key")
                ),
            ),
        )
        # Pair repeated populated keys by a deterministic occurrence number.
        # This avoids a Cartesian join while allowing every paired occurrence
        # to continue to L4. Any excess occurrence remains missing/extra in L3.
        source_order = spark_functions.to_json(
            spark_functions.struct(
                *[
                    spark_functions.col(column)
                    for column in sorted(source_dataframe.columns)
                ]
            )
        )
        target_order = spark_functions.to_json(
            spark_functions.struct(
                *[
                    spark_functions.col(column)
                    for column in sorted(target_dataframe.columns)
                ]
            )
        )
        source_prepared = source_prepared.withColumn(
            "__key_ordinal",
            spark_functions.when(
                source_valid,
                spark_functions.row_number().over(
                    Window.partitionBy("__normalized_primary_key")
                    .orderBy(source_order, "__source_row_id")
                ),
            ),
        )
        target_prepared = target_prepared.withColumn(
            "__key_ordinal",
            spark_functions.when(
                target_valid,
                spark_functions.row_number().over(
                    Window.partitionBy("__normalized_primary_key")
                    .orderBy(target_order, "__target_row_id")
                ),
            ),
        )
        source_population = (
            source_prepared
            .filter(spark_functions.col("__normalized_primary_key").isNotNull())
            .groupBy("__normalized_primary_key")
            .agg(spark_functions.count(spark_functions.lit(1)).alias("__source_population_count"))
        )
        target_population = (
            target_prepared
            .filter(spark_functions.col("__normalized_primary_key").isNotNull())
            .groupBy("__normalized_primary_key")
            .agg(spark_functions.count(spark_functions.lit(1)).alias("__target_population_count"))
        )
        source_prepared = (
            source_prepared
            .join(target_population, "__normalized_primary_key", "left")
            .fillna(0, subset=["__target_population_count"])
        )
        target_prepared = (
            target_prepared
            .join(source_population, "__normalized_primary_key", "left")
            .fillna(0, subset=["__source_population_count"])
        )
        source_pair_ordinal = spark_functions.when(
            (spark_functions.col("__target_population_count") > 0)
            & (spark_functions.col("__key_ordinal") > spark_functions.col("__target_population_count")),
            spark_functions.lit(1),
        ).otherwise(spark_functions.col("__key_ordinal"))
        target_pair_ordinal = spark_functions.when(
            (spark_functions.col("__source_population_count") > 0)
            & (spark_functions.col("__key_ordinal") > spark_functions.col("__source_population_count")),
            spark_functions.lit(1),
        ).otherwise(spark_functions.col("__key_ordinal"))
        source_prepared = source_prepared.withColumn(
            "__join_key",
            spark_functions.when(
                source_valid,
                spark_functions.concat(
                    spark_functions.lit("KEY:"),
                    spark_functions.col("__normalized_primary_key"),
                    spark_functions.lit(":"),
                    source_pair_ordinal,
                ),
            ).otherwise(
                spark_functions.concat(
                    spark_functions.lit("SOURCE_UNMATCHED:"),
                    spark_functions.col("__source_row_id").cast("string"),
                )
            ),
        )
        target_prepared = target_prepared.withColumn(
            "__join_key",
            spark_functions.when(
                target_valid,
                spark_functions.concat(
                    spark_functions.lit("KEY:"),
                    spark_functions.col("__normalized_primary_key"),
                    spark_functions.lit(":"),
                    target_pair_ordinal,
                ),
            ).otherwise(
                spark_functions.concat(
                    spark_functions.lit("TARGET_UNMATCHED:"),
                    spark_functions.col("__target_row_id").cast("string"),
                )
            ),
        )

        source_struct = spark_functions.struct(
            *[
                spark_functions.col(f"source.`{field.name}`").alias(field.name)
                for field in source_dataframe.schema.fields
            ]
        )
        target_struct = spark_functions.struct(
            *[
                spark_functions.col(f"target.`{field.name}`").alias(field.name)
                for field in target_dataframe.schema.fields
            ]
        )
        source_key_json = spark_functions.to_json(
            spark_functions.struct(*[spark_functions.col(f"source.`{key['source_column']}`") for key in keys])
        )
        target_key_json = spark_functions.to_json(
            spark_functions.struct(*[spark_functions.col(f"target.`{key['target_column']}`") for key in keys])
        )
        joined = source_prepared.alias("source").join(
            target_prepared.alias("target"),
            spark_functions.col("source.__join_key") == spark_functions.col("target.__join_key"),
            "full_outer",
        )
        reconciliation = joined.select(
            source_struct.alias("_s"),
            target_struct.alias("_t"),
            spark_functions.coalesce(
                spark_functions.col("source.__normalized_primary_key"),
                spark_functions.col("target.__normalized_primary_key"),
            ).alias("normalized_primary_key"),
            spark_functions.when(
                spark_functions.col("source.__source_row_id").isNotNull()
                & spark_functions.col("target.__target_row_id").isNotNull(),
                spark_functions.lit("MATCHED"),
            )
            .when(
                spark_functions.col("source.__source_row_id").isNotNull()
                & spark_functions.col("source.__normalized_primary_key").isNull(),
                spark_functions.lit("UNMATCHABLE_SOURCE"),
            )
            .when(
                spark_functions.col("target.__target_row_id").isNotNull()
                & spark_functions.col("target.__normalized_primary_key").isNull(),
                spark_functions.lit("UNMATCHABLE_TARGET"),
            )
            .when(
                spark_functions.col("source.__source_row_id").isNotNull(),
                spark_functions.lit("MISSING_IN_TARGET"),
            )
            .otherwise(spark_functions.lit("EXTRA_IN_TARGET"))
            .alias("reconciliation_status"),
            spark_functions.when(
                spark_functions.col("source.__source_row_id").isNotNull()
                & spark_functions.col("target.__target_row_id").isNotNull(),
                spark_functions.lit("PRIMARY_KEY"),
            )
            .otherwise(spark_functions.lit(None).cast("string"))
            .alias("match_type"),
            spark_functions.when(
                spark_functions.col("source.__source_row_id").isNotNull(),
                source_key_json,
            )
            .otherwise(target_key_json)
            .alias("match_key"),
            spark_functions.col("source.__key_count").alias("_source_key_count"),
            spark_functions.col("target.__key_count").alias("_target_key_count"),
            spark_functions.col("source.__source_row_id").alias("_source_row_id"),
            spark_functions.col("target.__target_row_id").alias("_target_row_id"),
        ).persist()
        pk_build_ms = (perf_counter() - build_started) * 1000
        summary_started = perf_counter()
        summary = reconciliation.agg(
            spark_functions.sum(
                spark_functions.when(spark_functions.col("reconciliation_status") == "MATCHED", 1)
                .otherwise(0)
            ).alias("matched"),
            spark_functions.countDistinct(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "MATCHED",
                    spark_functions.col("normalized_primary_key"),
                )
            ).alias("matched_keys"),
            spark_functions.countDistinct(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "MATCHED",
                    spark_functions.col("_source_row_id"),
                )
            ).alias("matched_source_records"),
            spark_functions.countDistinct(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "MATCHED",
                    spark_functions.col("_target_row_id"),
                )
            ).alias("matched_target_records"),
            spark_functions.sum(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "MISSING_IN_TARGET",
                    1,
                ).otherwise(0)
            ).alias("missing"),
            spark_functions.sum(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "EXTRA_IN_TARGET",
                    1,
                ).otherwise(0)
            ).alias("extra"),
            spark_functions.sum(
                spark_functions.when(spark_functions.col("_source_key_count") > 1, 1).otherwise(0)
            ).alias("source_duplicate_records"),
            spark_functions.sum(
                spark_functions.when(spark_functions.col("_target_key_count") > 1, 1).otherwise(0)
            ).alias("target_duplicate_records"),
            spark_functions.countDistinct(
                spark_functions.when(
                    spark_functions.col("_source_key_count") > 1,
                    spark_functions.col("normalized_primary_key"),
                )
            ).alias("source_duplicate_keys"),
            spark_functions.countDistinct(
                spark_functions.when(
                    spark_functions.col("_target_key_count") > 1,
                    spark_functions.col("normalized_primary_key"),
                )
            ).alias("target_duplicate_keys"),
            spark_functions.sum(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "UNMATCHABLE_SOURCE",
                    1,
                ).otherwise(0)
            ).alias("unmatchable_source"),
            spark_functions.sum(
                spark_functions.when(
                    spark_functions.col("reconciliation_status") == "UNMATCHABLE_TARGET",
                    1,
                ).otherwise(0)
            ).alias("unmatchable_target"),
            spark_functions.sum(
                spark_functions.when(spark_functions.col("_source_key_count") == 1, 1).otherwise(0)
            ).alias("source_unique"),
            spark_functions.sum(
                spark_functions.when(spark_functions.col("_target_key_count") == 1, 1).otherwise(0)
            ).alias("target_unique"),
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
        result = (
            reconciliation,
            counts,
            {
                "pk_build_ms": pk_build_ms,
                "pk_summary_ms": pk_summary_ms,
                "pk_path": "DUPLICATE_KEY_PATH",
            },
        )
        self._match_cache[cache_key] = result
        return result


    def _possible_key_changes(self, source, target, configuration):
        """Match unresolved rows one-to-one using configured grouping fields."""
        from pyspark.sql import functions as spark_functions

        groups = configuration.get("grouping_attributes", []) or []
        keys = configuration.get("comparison_keys", []) or []
        if not groups or not keys:
            return source.limit(0).select(
                spark_functions.lit(None).cast("string").alias("source_key")
            )

        source_group_columns = [item["source_column"] for item in groups]
        target_group_columns = [item["target_column"] for item in groups]
        source_record = spark_functions.struct(
            *[spark_functions.col(column) for column in source.columns]
        )
        target_record = spark_functions.struct(
            *[spark_functions.col(column) for column in target.columns]
        )
        source_key_columns = [item["source_column"] for item in keys]
        target_key_columns = [item["target_column"] for item in keys]
        source_key = spark_functions.to_json(
            spark_functions.struct(
                *[spark_functions.col(column) for column in source_key_columns]
            )
        )
        target_key = spark_functions.to_json(
            spark_functions.struct(
                *[spark_functions.col(column) for column in target_key_columns]
            )
        )
        source_key_available = self._business_key_available(source_key_columns)
        target_key_available = self._business_key_available(target_key_columns)

        source_groups = source.groupBy(*source_group_columns).agg(
            spark_functions.count(spark_functions.lit(1)).alias("source_candidates"),
            spark_functions.first(source_record, ignorenulls=True).alias("source_record"),
            spark_functions.first(source_key, ignorenulls=True).alias("source_key"),
            spark_functions.max(source_key_available.cast("int")).alias("source_key_available"),
        ).alias("source")
        target_groups = target.groupBy(*target_group_columns).agg(
            spark_functions.count(spark_functions.lit(1)).alias("target_candidates"),
            spark_functions.first(target_record, ignorenulls=True).alias("target_record"),
            spark_functions.first(target_key, ignorenulls=True).alias("target_key"),
            spark_functions.max(target_key_available.cast("int")).alias("target_key_available"),
        ).alias("target")

        condition = None
        for source_column, target_column in zip(
            source_group_columns,
            target_group_columns,
        ):
            pair = spark_functions.col(f"source.`{source_column}`").eqNullSafe(
                spark_functions.col(f"target.`{target_column}`")
            )
            condition = pair if condition is None else condition & pair

        group_value = spark_functions.array(
            *[
                spark_functions.coalesce(
                    spark_functions.col(f"source.`{source_column}`"),
                    spark_functions.col(f"target.`{target_column}`"),
                ).cast("string")
                for source_column, target_column in zip(
                    source_group_columns,
                    target_group_columns,
                )
            ]
        )
        paired_unique_group = (
            (spark_functions.col("source_candidates") == 1)
            & (spark_functions.col("target_candidates") == 1)
        )
        asymmetric_key_availability = (
            spark_functions.col("source_key_available")
            != spark_functions.col("target_key_available")
        )

        return source_groups.join(target_groups, condition, "inner").filter(
            paired_unique_group | asymmetric_key_availability
        ).select(
            group_value.alias("group_key"),
            "source_key",
            "target_key",
            "source_record",
            "target_record",
            spark_functions.when(
                spark_functions.col("source_key_available") != spark_functions.col("target_key_available"),
                spark_functions.lit(
                    "The configured matching attributes identify the same record, "
                    "but its business key is missing on one side"
                ),
            ).when(
                (spark_functions.col("source_key_available") == 1) & (spark_functions.col("target_key_available") == 1),
                spark_functions.lit(
                    "Records share the configured matching attributes but use "
                    "different business keys"
                ),
            ).otherwise(
                spark_functions.lit(
                    "Records have no usable business key and were paired by the "
                    "configured matching attributes"
                )
            ).alias("reason"),
            spark_functions.when(
                spark_functions.col("source_key_available") != spark_functions.col("target_key_available"),
                spark_functions.lit("MISSING_BUSINESS_KEY"),
            ).when(
                (spark_functions.col("source_key_available") == 1) & (spark_functions.col("target_key_available") == 1),
                spark_functions.lit("POSSIBLE_KEY_CHANGE"),
            )
            .otherwise(spark_functions.lit("MATCHED_BY_ATTRIBUTES"))
            .alias("status"),
        )

    @staticmethod
    def _mapping_lookup(configuration):
        return {
            item.get("source_column"): item
            for item in (configuration.get("column_mappings", []) or [])
            if (
                isinstance(item, dict)
                and item.get("source_column")
                and item.get("target_column")
            )
        }

    @staticmethod
    def _apply_mapping_normalization(expression, mapping):
        """Apply only value normalization; matching identity is never changed."""
        from pyspark.sql import functions as spark_functions
        normalization = dict((mapping or {}).get("normalization") or {})
        value = expression
        if normalization.get("empty_as_null"):
            value = spark_functions.when(
                spark_functions.trim(value.cast("string")) == "",
                spark_functions.lit(None),
            ).otherwise(value)
        if normalization.get("trim"):
            value = spark_functions.trim(value.cast("string"))
        if normalization.get("case_insensitive"):
            value = spark_functions.lower(value.cast("string"))
        if normalization.get("round") is not None:
            value = spark_functions.round(value.cast("double"), int(normalization["round"]))
        return value

    def _matched_row_hashes(self, pairs, resolved_pairs):
        """Attach canonical SHA-256 content hashes to authoritative PK pairs.

        Hashing is deliberately *after* unique-PK reconciliation.  It is never
        used to manufacture row identity for duplicate or null keys.  Equal
        hashes let L4 skip rows that are provably identical after configured
        normalization; unequal hashes remain candidates for normal field/tolerance
        evaluation.
        """
        from pyspark.sql import functions as spark_functions
        if not resolved_pairs:
            return (
                pairs
                .withColumn("__source_row_hash", spark_functions.lit(None).cast("string"))
                .withColumn("__target_row_hash", spark_functions.lit(None).cast("string"))
            )

        source_parts = []
        target_parts = []
        for source_column, target_column, mapping in resolved_pairs:
            source_value = self._apply_mapping_normalization(
                spark_functions.col(f"_s.`{source_column}`"),
                mapping,
            )
            target_value = self._apply_mapping_normalization(
                spark_functions.col(f"_t.`{target_column}`"),
                mapping,
            )
            # Length-prefixing/field separators avoid ambiguous concatenations.
            source_parts.append(
                spark_functions.concat(
                    spark_functions.lit(source_column + "="),
                    spark_functions.coalesce(
                        source_value.cast("string"),
                        spark_functions.lit("<NULL>"),
                    ),
                )
            )
            target_parts.append(
                spark_functions.concat(
                    spark_functions.lit(source_column + "="),
                    spark_functions.coalesce(
                        target_value.cast("string"),
                        spark_functions.lit("<NULL>"),
                    ),
                )
            )

        return (
            pairs
            .withColumn(
                "__source_row_hash",
                spark_functions.sha2(spark_functions.concat_ws("\u001e", *source_parts), 256),
            )
            .withColumn(
                "__target_row_hash",
                spark_functions.sha2(spark_functions.concat_ws("\u001e", *target_parts), 256),
            )
        )

    @staticmethod
    def _resolve_l4_column_pairs(source_columns, target_columns, configuration):
        """Resolve L4 fields exactly like the canonical FieldComparator.

        Explicit column mappings are overrides, not an allow-list. Any source
        column without an explicit mapping compares to the same-named target
        column when that target exists. Comparison-key source fields and
        ignored source fields are excluded from field-level checks.
        """
        explicit_mappings = {
            mapping.get("source_column"): mapping
            for mapping in configuration.get("column_mappings", [])
            if isinstance(mapping, dict)
            and mapping.get("source_column")
            and mapping.get("target_column")
        }
        ignored = set(configuration.get("ignored_columns", []))
        key_sources = {
            key.get("source_column")
            for key in configuration.get("comparison_keys", [])
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


    def _agg_expr(self, operation, column):
        from pyspark.sql import functions as spark_functions

        aggregate_functions = {
            "SUM": spark_functions.sum,
            "AVG": spark_functions.avg,
            "MIN": spark_functions.min,
            "MAX": spark_functions.max,
            "COUNT": spark_functions.count,
        }
        value = spark_functions.col(column) if column else spark_functions.lit(1)
        return aggregate_functions[operation](value)


    def _group(self, source_dataframe, target_dataframe, configuration):
        from pyspark.sql import functions as spark_functions

        group_started = perf_counter()
        groups = configuration.get("grouping_attributes", []) or []
        aggregation_columns = configuration.get("aggregation_columns", []) or []
        if not groups:
            raise ValueError("Group reconciliation requires grouping_attributes")

        mapping_lookup = self._mapping_lookup(configuration)
        group_aliases = [
            f"__group_{group_index}"
            for group_index in range(len(groups))
        ]

        def mapping_for_pair(source_column, target_column):
            mapping = mapping_lookup.get(source_column)
            if mapping and mapping.get("target_column") == target_column:
                return mapping
            return {}

        def prepare(dataframe, side):
            prepared = dataframe
            for group_index, item in enumerate(groups):
                source_column = item["source_column"]
                target_column = item["target_column"]
                column = source_column if side == "source" else target_column
                mapping = mapping_for_pair(source_column, target_column)
                prepared = prepared.withColumn(
                    group_aliases[group_index],
                    self._apply_mapping_normalization(spark_functions.col(column), mapping),
                )
            return prepared

        def aggregate_expression(operation, expression):
            operation = operation.upper()
            if operation == "SUM":
                return spark_functions.sum(expression)
            if operation == "AVG":
                return spark_functions.avg(expression)
            if operation == "MIN":
                return spark_functions.min(expression)
            if operation == "MAX":
                return spark_functions.max(expression)
            if operation == "COUNT":
                return spark_functions.count(expression)
            raise ValueError(
                f"Unsupported group aggregation operation: {operation}"
            )

        prepared_caches = []

        def build(dataframe, side):
            # Base aggregates and MODE calculations reuse the same normalized
            # rows. Persist once so Spark does not rebuild normalization and the
            # upstream fallback lineage for each aggregation branch.
            prepared = prepare(dataframe, side).persist()
            prepared_caches.append(prepared)
            record_struct = spark_functions.struct(
                *[spark_functions.col(column) for column in dataframe.columns]
            )
            aggregate_exprs = [
                spark_functions.count(spark_functions.lit(1)).alias("__present"),
                spark_functions.first(record_struct, ignorenulls=True).alias("__record"),
            ]
            mode_specs = []
            for aggregate_index, item in enumerate(aggregation_columns):
                source_column = item["source_column"]
                target_column = item["target_column"]
                column = source_column if side == "source" else target_column
                operation = str(item.get("operation", "AVG")).upper()
                mapping = mapping_for_pair(source_column, target_column)
                value_expr = self._apply_mapping_normalization(
                    spark_functions.col(column),
                    mapping,
                )
                if operation == "MODE":
                    mode_specs.append((aggregate_index, value_expr))
                else:
                    aggregate_exprs.append(
                        aggregate_expression(operation, value_expr).alias(
                            f"__aggregate_{aggregate_index}"
                        )
                    )

            result = prepared.groupBy(*group_aliases).agg(*aggregate_exprs)

            # Deterministic MODE without a Window/sort stage. First count each
            # value inside the group, then choose the value with the smallest
            # ordering tuple (-frequency, lexical_value): highest frequency wins;
            # ties resolve to the lexically smallest normalized value, exactly as
            # before. min_by is available in Spark 3.5.3.
            for index, value_expr in mode_specs:
                mode_values = prepared.select(
                    *[spark_functions.col(alias) for alias in group_aliases],
                    value_expr.alias("__mode_value"),
                ).filter(spark_functions.col("__mode_value").isNotNull())

                counts = mode_values.groupBy(
                    *group_aliases, "__mode_value"
                ).agg(
                    spark_functions.count(spark_functions.lit(1)).alias("__mode_count")
                )

                ordering = spark_functions.struct(
                    (-spark_functions.col("__mode_count")).alias("frequency_order"),
                    spark_functions.col("__mode_value").cast("string").alias("lexical_order"),
                )

                modes = counts.groupBy(*group_aliases).agg(
                    spark_functions.min_by(
                        spark_functions.col("__mode_value"),
                        ordering,
                    ).alias(f"__aggregate_{index}")
                )

                result = result.join(modes, group_aliases, "left")

            return result

        source_groups = build(source_dataframe, "source").alias("source")
        target_groups = build(target_dataframe, "target").alias("target")
        condition = None
        for alias in group_aliases:
            group_match = spark_functions.col(f"source.`{alias}`").eqNullSafe(
                spark_functions.col(f"target.`{alias}`")
            )
            condition = (
                group_match
                if condition is None
                else condition & group_match
            )

        # Cache the expensive grouped source/target join.  The summary action
        # below materializes it once; bounded evidence reuses the cached rows.
        joined_groups = source_groups.join(
            target_groups,
            condition,
            "full_outer",
        ).persist()

        source_present = spark_functions.col("source.__present").isNotNull()
        target_present = spark_functions.col("target.__present").isNotNull()
        common_present = source_present & target_present
        row_count_mismatch = common_present & (
            spark_functions.col("source.__present") != spark_functions.col("target.__present")
        )
        duplicate_group = (
            (spark_functions.coalesce(spark_functions.col("source.__present"), spark_functions.lit(0)) > 1)
            | (spark_functions.coalesce(spark_functions.col("target.__present"), spark_functions.lit(0)) > 1)
        )

        # Build the aggregate mismatch/applicability expressions once.  These
        # are evaluated directly on one joined group row, so all metrics can be
        # computed in the same Spark aggregation that materializes
        # `joined_groups`.
        aggregate_applicable = []
        aggregate_failed = []
        for aggregate_index, item in enumerate(aggregation_columns):
            source_value = spark_functions.col(f"source.__aggregate_{aggregate_index}")
            target_value = spark_functions.col(f"target.__aggregate_{aggregate_index}")
            both_null = source_value.isNull() & target_value.isNull()
            applicable = common_present & ~both_null
            failed = applicable & ~source_value.eqNullSafe(target_value)
            aggregate_applicable.append(applicable)
            aggregate_failed.append(failed)

        any_group_failure = row_count_mismatch | duplicate_group
        for failed in aggregate_failed:
            any_group_failure = any_group_failure | failed

        summary_exprs = [
            spark_functions.sum(spark_functions.when(source_present, 1).otherwise(0)).alias("source_groups"),
            spark_functions.sum(spark_functions.when(target_present, 1).otherwise(0)).alias("target_groups"),
            spark_functions.sum(spark_functions.when(common_present, 1).otherwise(0)).alias("common_groups"),
            spark_functions.sum(spark_functions.when(row_count_mismatch, 1).otherwise(0)).alias(
                "row_count_checks_failed"
            ),
            spark_functions.sum(spark_functions.when(duplicate_group, 1).otherwise(0)).alias(
                "duplicate_checks_failed"
            ),
            spark_functions.sum(spark_functions.when(any_group_failure, 1).otherwise(0)).alias(
                "mismatch_groups"
            ),
        ]
        for index, applicable in enumerate(aggregate_applicable):
            summary_exprs.append(
                spark_functions.sum(spark_functions.when(applicable, 1).otherwise(0)).alias(
                    f"aggregate_applicable_{index}"
                )
            )
        for index, failed in enumerate(aggregate_failed):
            summary_exprs.append(
                spark_functions.sum(spark_functions.when(failed, 1).otherwise(0)).alias(
                    f"aggregate_failed_{index}"
                )
            )

        summary_started = perf_counter()
        summary = joined_groups.agg(*summary_exprs).first()
        summary_ms = (perf_counter() - summary_started) * 1000

        # `joined_groups` is now fully materialized. The normalized per-side
        # inputs are no longer needed and can be released before evidence scans
        # the cached join.
        for prepared in prepared_caches:
            try:
                prepared.unpersist(blocking=False)
            except Exception:
                logger.debug("Unable to unpersist prepared group dataset", exc_info=True)

        source_group_count = int(summary["source_groups"] or 0)
        target_group_count = int(summary["target_groups"] or 0)
        common = int(summary["common_groups"] or 0)
        missing = source_group_count - common
        extra = target_group_count - common

        row_count_checks_failed = int(summary["row_count_checks_failed"] or 0)
        duplicate_checks_failed = int(summary["duplicate_checks_failed"] or 0)
        aggregate_field_checks = sum(
            int(summary[f"aggregate_applicable_{aggregate_index}"] or 0)
            for aggregate_index in range(len(aggregation_columns))
        )
        aggregate_field_failed = sum(
            int(summary[f"aggregate_failed_{aggregate_index}"] or 0)
            for aggregate_index in range(len(aggregation_columns))
        )
        aggregate_checks_total = (
            row_count_checks_failed
            + duplicate_checks_failed
            + aggregate_field_checks
        )
        aggregate_checks_failed = (
            row_count_checks_failed
            + duplicate_checks_failed
            + aggregate_field_failed
        )
        mismatch_groups = int(summary["mismatch_groups"] or 0)

        group_key = spark_functions.array(
            *[
                spark_functions.coalesce(
                    spark_functions.col(f"source.`{alias}`").cast("string"),
                    spark_functions.col(f"target.`{alias}`").cast("string"),
                )
                for alias in group_aliases
            ]
        )
        presence_rows = joined_groups.filter(
            spark_functions.col("source.__present").isNull() | spark_functions.col("target.__present").isNull()
        ).select(
            group_key.alias("group_key"),
            spark_functions.col("source.__record").alias("source_record"),
            spark_functions.col("target.__record").alias("target_record"),
            spark_functions.lit(None).cast("string").alias("source_aggregate"),
            spark_functions.lit(None).cast("string").alias("target_aggregate"),
            spark_functions.lit(None).cast("string").alias("source_column"),
            spark_functions.lit(None).cast("string").alias("target_column"),
            spark_functions.lit(None).cast("string").alias("operation"),
            spark_functions.lit(None).cast("double").alias("difference"),
            spark_functions.when(
                spark_functions.col("source.__present").isNull(),
                spark_functions.lit("EXTRA_GROUP_IN_TARGET"),
            )
            .otherwise(spark_functions.lit("MISSING_GROUP_IN_TARGET"))
            .alias("status"),
        ).withColumn("matched", spark_functions.lit(False))

        common_rows = joined_groups.filter(common_present)
        count_mismatch_rows = common_rows.filter(
            spark_functions.col("source.__present") != spark_functions.col("target.__present")
        ).select(
            group_key.alias("group_key"),
            spark_functions.col("source.__record").alias("source_record"),
            spark_functions.col("target.__record").alias("target_record"),
            spark_functions.col("source.__present").cast("string").alias("source_aggregate"),
            spark_functions.col("target.__present").cast("string").alias("target_aggregate"),
            spark_functions.lit("Rows").alias("source_column"),
            spark_functions.lit("Rows").alias("target_column"),
            spark_functions.lit("COUNT").alias("operation"),
            (
                spark_functions.col("target.__present").cast("double")
                - spark_functions.col("source.__present").cast("double")
            ).alias("difference"),
            spark_functions.lit("GROUP_ROW_COUNT_MISMATCH").alias("status"),
            spark_functions.lit(False).alias("matched"),
        )
        duplicate_group_rows = joined_groups.filter(
            (spark_functions.coalesce(spark_functions.col("source.__present"), spark_functions.lit(0)) > 1)
            | (spark_functions.coalesce(spark_functions.col("target.__present"), spark_functions.lit(0)) > 1)
        ).select(
            group_key.alias("group_key"),
            spark_functions.col("source.__record").alias("source_record"),
            spark_functions.col("target.__record").alias("target_record"),
            spark_functions.coalesce(spark_functions.col("source.__present"), spark_functions.lit(0)).cast("string").alias("source_aggregate"),
            spark_functions.coalesce(spark_functions.col("target.__present"), spark_functions.lit(0)).cast("string").alias("target_aggregate"),
            spark_functions.lit("Rows").alias("source_column"),
            spark_functions.lit("Rows").alias("target_column"),
            spark_functions.lit("DUPLICATE COUNT").alias("operation"),
            (
                spark_functions.coalesce(spark_functions.col("target.__present"), spark_functions.lit(0)).cast("double")
                - spark_functions.coalesce(spark_functions.col("source.__present"), spark_functions.lit(0)).cast("double")
            ).alias("difference"),
            spark_functions.lit("GROUP_DUPLICATE_ROWS").alias("status"),
            spark_functions.lit(False).alias("matched"),
        )
        aggregate_structs = []
        for aggregate_index, item in enumerate(aggregation_columns):
            source_value = spark_functions.col(f"source.__aggregate_{aggregate_index}")
            target_value = spark_functions.col(f"target.__aggregate_{aggregate_index}")
            status = (
                spark_functions.when(
                    source_value.isNull() & target_value.isNull(),
                    spark_functions.lit("NOT_APPLICABLE"),
                )
                .when(source_value.eqNullSafe(target_value), spark_functions.lit("PASS"))
                .otherwise(spark_functions.lit("GROUP_VALUE_MISMATCH"))
            )
            aggregate_structs.append(
                spark_functions.struct(
                    spark_functions.col("source.__record").alias("source_record"),
                    spark_functions.col("target.__record").alias("target_record"),
                    source_value.cast("string").alias("source_aggregate"),
                    target_value.cast("string").alias("target_aggregate"),
                    spark_functions.lit(item["source_column"]).alias("source_column"),
                    spark_functions.lit(item["target_column"]).alias("target_column"),
                    spark_functions.lit(str(item.get("operation", "AVG")).upper()).alias("operation"),
                    (
                        target_value.cast("double")
                        - source_value.cast("double")
                    ).alias("difference"),
                    status.alias("status"),
                    status.isin("PASS", "NOT_APPLICABLE").alias("matched"),
                )
            )

        aggregate_rows = None
        if aggregate_structs:
            aggregate_rows = (
                common_rows
                .select(
                    group_key.alias("group_key"),
                    spark_functions.explode(spark_functions.array(*aggregate_structs)).alias("aggregate"),
                )
                .select("group_key", "aggregate.*")
            )

        result_rows = (
            presence_rows
            .unionByName(count_mismatch_rows)
            .unionByName(duplicate_group_rows)
        )
        if aggregate_rows is not None:
            result_rows = result_rows.unionByName(aggregate_rows)

        group_difference_count = missing + extra + mismatch_groups
        aggregate_checks_passed = aggregate_checks_total - aggregate_checks_failed
        metrics = {
            "status": "PASS" if group_difference_count == 0 else "FAIL",
            "matching_mode": "GROUP_RECONCILIATION",
            "comparison_mode": "GROUP_RECONCILIATION",
            "source_group_count": source_group_count,
            "target_group_count": target_group_count,
            "group_count": source_group_count + extra,
            "common_group_count": common,
            "matched_group_count": common,
            "missing_group_count": missing,
            "extra_group_count": extra,
            "groups_with_aggregate_mismatch": mismatch_groups,
            "groups_with_mismatch": mismatch_groups,
            "group_mismatch_count": mismatch_groups,
            "group_difference_count": group_difference_count,
            "mismatch_group_count": mismatch_groups,
            "mismatch_count": group_difference_count,
            "source_group_coverage_pct": safe_rate_pct(
                common,
                source_group_count,
                zero_value=100.0,
            ),
            "target_group_coverage_pct": safe_rate_pct(
                common,
                target_group_count,
                zero_value=100.0,
            ),
            "source_group_coverage": safe_rate_pct(
                common,
                source_group_count,
                zero_value=100.0,
            ),
            "target_group_coverage": safe_rate_pct(
                common,
                target_group_count,
                zero_value=100.0,
            ),
            "aggregate_checks_total": aggregate_checks_total,
            "aggregate_check_count": aggregate_checks_total,
            "aggregate_checks_passed": aggregate_checks_passed,
            "aggregate_checks_failed": aggregate_checks_failed,
            "checks_total": aggregate_checks_total,
            "checks_passed": aggregate_checks_passed,
            "checks_failed": aggregate_checks_failed,
        }

        exception_rows = result_rows.filter(
            ~spark_functions.col("status").isin("PASS", "NOT_APPLICABLE")
        )
        exception_count = missing + extra + aggregate_checks_failed
        evidence_started = perf_counter()
        bounded_evidence = self._bounded(exception_rows, exception_count)
        evidence_ms = (perf_counter() - evidence_started) * 1000
        total_ms = (perf_counter() - group_started) * 1000
        logger.info(
            "SPARK_GROUP_OPT_TIMING summary_ms=%.1f evidence_ms=%.1f total_ms=%.1f",
            summary_ms,
            evidence_ms,
            total_ms,
        )
        print(
            f"SPARK_GROUP_OPT_TIMING summary_ms={summary_ms:.1f} "
            f"evidence_ms={evidence_ms:.1f} total_ms={total_ms:.1f}"
        )
        try:
            joined_groups.unpersist(blocking=False)
        except Exception:
            logger.debug("Unable to unpersist group reconciliation join", exc_info=True)
        return {
            "metrics": metrics,
            "evidence": {"group_reconciliation": bounded_evidence},
        }


    def _bounded(self, dataframe, total_count: int | None = None):
        if dataframe is None:
            return {"count": 0, "sample": [], "truncated": False}
        if total_count is not None and int(total_count) == 0:
            return {"count": 0, "sample": [], "truncated": False}
        rows = dataframe.limit(self.evidence_limit + 1).collect()
        truncated = len(rows) > self.evidence_limit
        sample = [row.asDict(recursive=True) for row in rows[:self.evidence_limit]]
        if total_count is None:
            total_count = dataframe.count()
        return {"count": total_count, "sample": sample, "truncated": truncated}

    def _normalize_contract(self, level, result):
        """Ensure every Spark level uses the same public result envelope as local execution."""
        metrics = result.setdefault("metrics", {})
        evidence = result.setdefault("evidence", {})
        metrics.setdefault("status", "PASS")

        if level == ComparisonLevel.L5:
            total = metrics.setdefault("checks_total", 0)
            failed = metrics.setdefault("checks_failed", 0)

            metrics.setdefault("checks_passed", total - failed)
            passed = metrics["checks_passed"]
            metrics.setdefault(
                "aggregate_check_pass_rate_pct",
                safe_rate_pct(passed, total, zero_value=100.0),
            )
            metrics.setdefault(
                "aggregate_check_failure_rate_pct",
                safe_rate_pct(failed, total),
            )
        elif level == ComparisonLevel.L6:
            total = metrics.setdefault("checks_total", 0)
            failed = metrics.setdefault("checks_failed", 0)

            metrics.setdefault("checks_passed", total - failed)
            passed = metrics["checks_passed"]
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
