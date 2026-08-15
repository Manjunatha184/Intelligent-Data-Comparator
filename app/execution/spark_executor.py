from __future__ import annotations

import os
import json
import re
import logging
import threading
from time import perf_counter
from typing import Any

from app.execution.models import ComparisonLevel, ExecutionTask
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

    def execute(self, task: ExecutionTask) -> dict[str, Any]:
        load_started = perf_counter()
        source = self._load(task.configuration["source"])
        target = self._load(task.configuration["target"])
        logger.info("SPARK_TIMING task_id=%s level=%s dataset_loading_ms=%.1f", task.task_id, task.comparison_level.value, (perf_counter() - load_started) * 1000)
        print(f"SPARK_TIMING task_id={task.task_id} level={task.comparison_level.value} dataset_loading_ms={(perf_counter() - load_started) * 1000:.1f}")
        level = task.comparison_level
        level_started = perf_counter()
        if level == ComparisonLevel.L1:
            result = self._l1(source, target, task.configuration)
        elif level == ComparisonLevel.L2:
            result = self._l2(source, target, task.configuration)
        elif level == ComparisonLevel.L3:
            result = self._l3(source, target, task.configuration)
        elif level == ComparisonLevel.L4:
            result = self._l4(source, target, task.configuration)
        elif level == ComparisonLevel.L5:
            result = self._l5(source, target, task.configuration)
        elif level == ComparisonLevel.L6:
            result = self._l6(source, target, task.configuration)
        else:
            raise ValueError(f"Unsupported Spark comparison level: {level}")
        logger.info("SPARK_TIMING task_id=%s level=%s comparison_ms=%.1f", task.task_id, level.value, (perf_counter() - level_started) * 1000)
        print(f"SPARK_TIMING task_id={task.task_id} level={level.value} comparison_ms={(perf_counter() - level_started) * 1000:.1f}")
        result = self._normalize_contract(level, result)
        result.setdefault("runtime_context", {}).update({
            "engine": "SPARK",
            "spark_master": self.spark.sparkContext.master,
            "spark_app_id": self.spark.sparkContext.applicationId,
            "distributed": True,
            "full_collect_used": False,
        })
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

    def _l1(self, s, t, cfg):
        maps, ignored = self._maps(cfg), set(cfg.get("ignored_columns", []))
        sf = {f.name: f.dataType.simpleString() for f in s.schema.fields if f.name not in ignored}
        tf = {f.name: f.dataType.simpleString() for f in t.schema.fields if f.name not in ignored}
        source_nullable = {f.name: f.nullable for f in s.schema.fields if f.name not in ignored}
        target_nullable = {f.name: f.nullable for f in t.schema.fields if f.name not in ignored}
        missing, unexpected, types, matched = [], [], [], []
        nullable_mismatches, length_mismatches = [], []
        mapped_targets = set()
        for sc, st in sf.items():
            tc = maps.get(sc, sc); mapped_targets.add(tc)
            if tc not in tf:
                missing.append({"source_column": sc, "expected_target_column": tc})
            else:
                matched.append({"source_column": sc, "target_column": tc})
                if st != tf[tc]: types.append({"source_column": sc, "target_column": tc, "source_type": st, "target_type": tf[tc]})
                if source_nullable[sc] != target_nullable[tc]:
                    nullable_mismatches.append({"source_column": sc, "target_column": tc, "source_nullable": source_nullable[sc], "target_nullable": target_nullable[tc]})
        for tc in tf:
            if tc not in mapped_targets and tc not in sf: unexpected.append({"target_column": tc})
        mismatch = len(missing)+len(unexpected)+len(types)+len(nullable_mismatches)
        return {"metrics": {"status": "PASS" if mismatch == 0 else "FAIL", "source_column_count": len(sf),
                "target_column_count": len(tf), "matched_column_count": len(matched), "missing_column_count": len(missing),
                "unexpected_column_count": len(unexpected), "data_type_mismatch_count": len(types), "schema_drift_count": mismatch,
                "nullable_mismatch_count": len(nullable_mismatches), "length_mismatch_count": len(length_mismatches),
                "precision_scale_mismatch_count": 0, "order_mismatch_count": 0, "mismatch_count": mismatch,
                "source_column_coverage_pct": safe_rate_pct(len(matched), len(sf), zero_value=100.0),
                "target_column_coverage_pct": safe_rate_pct(len(matched), len(tf), zero_value=100.0)},
                "evidence": {"matched_columns": matched, "missing_columns": missing, "unexpected_columns": unexpected,
                "type_mismatches": types, "data_type_mismatches": types, "nullable_mismatches": nullable_mismatches,
                "length_mismatches": length_mismatches, "precision_scale_mismatches": [], "order_mismatch": {},
                "schema_drift": ([{"type": "MISSING_COLUMN", **item} for item in missing] +
                                 [{"type": "UNEXPECTED_COLUMN", **item} for item in unexpected] +
                                 [{"type": "DATA_TYPE_CHANGED", **item} for item in types] +
                                 [{"type": "NULLABILITY_CHANGED", **item} for item in nullable_mismatches])}}

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

    def _l2(self, s, t, cfg):
        ss, ts = self._stats(s,cfg,"source"), self._stats(t,cfg,"target")
        checks = {}

        for k in ("total_rows","filtered_rows","partition_rows","distinct_key_count","duplicate_key_count"):
            available = ss[k] is not None or ts[k] is not None
            difference = None if not available or ss[k] is None or ts[k] is None else ts[k] - ss[k]
            checks[k] = {
                "source": ss[k],
                "target": ts[k],
                "difference": difference,
                "percentage_difference": safe_percent_change(ss[k], ts[k]),
                "tolerance": None,
                "matched": ss[k] == ts[k],
                "available": available,
            }

        # IMPORTANT:
        # Null counts must be compared using the same source -> target column
        # mapping used by L1/L4. Comparing the two raw dictionaries directly
        # produces a false failure for renamed columns such as:
        #     Email -> Target_Email
        #
        # Example:
        #     {"Email": 0} != {"Target_Email": 0}
        # even though the mapped null counts are equal.
        source_nulls = ss["null_counts"]
        target_nulls = ts["null_counts"]
        column_mappings = self._maps(cfg)
        ignored = set(cfg.get("ignored_columns", []))

        null_count_differences = []
        mapped_null_counts = []

        for source_column, source_count in source_nulls.items():
            if source_column in ignored:
                continue

            # Explicit mapping wins; otherwise same-name matching is used.
            target_column = column_mappings.get(source_column, source_column)

            if target_column in ignored:
                continue

            # Missing/unexpected schema columns belong to L1, not L2.
            # L2 compares null counts only for columns that exist on both sides.
            if target_column not in target_nulls:
                continue

            target_count = target_nulls[target_column]
            matched = source_count == target_count

            item = {
                "source_column": source_column,
                "target_column": target_column,
                "source": source_count,
                "target": target_count,
                "difference": target_count - source_count,
                "matched": matched,
            }
            mapped_null_counts.append(item)

            if not matched:
                null_count_differences.append(item)

        checks["null_counts"] = {
            "source": source_nulls,
            "target": target_nulls,
            "mapped_columns": mapped_null_counts,
            "differences": null_count_differences,
            "matched": len(null_count_differences) == 0,
            "available": True,
        }

        # filtered_rows and partition_rows are execution diagnostics, not
        # independent business validations. Counting them as failures inflated
        # L2 from one row-count problem into three identical failures.
        validation_names = ["total_rows", "distinct_key_count", "duplicate_key_count", "null_counts"]
        applicable = [name for name in validation_names if checks[name].get("available", True)]
        failed=[name for name in applicable if not checks[name]["matched"]]
        return {"metrics":{"status":"PASS" if not failed else "FAIL","checks_total":len(applicable),"checks_failed":len(failed),
                "checks_passed":len(applicable)-len(failed),"total_rows_source":ss["total_rows"],"total_rows_target":ts["total_rows"],
                "distinct_key_count_source":ss["distinct_key_count"],"distinct_key_count_target":ts["distinct_key_count"],
                "duplicate_key_count_source":ss["duplicate_key_count"],"duplicate_key_count_target":ts["duplicate_key_count"],
                "row_count_percent_change":safe_percent_change(ss["total_rows"],ts["total_rows"]),
                "volume_coverage_pct":safe_rate_pct(ts["total_rows"],ss["total_rows"],zero_value=100.0 if ts["total_rows"] == 0 else None),
                "distinct_key_percent_change":safe_percent_change(ss["distinct_key_count"],ts["distinct_key_count"]),
                "source_duplicate_key_rate_pct":safe_rate_pct(ss["duplicate_key_count"],ss["total_rows"]),
                "target_duplicate_key_rate_pct":safe_rate_pct(ts["duplicate_key_count"],ts["total_rows"])},
                "evidence":{"checks":checks,"failed_checks":failed,"source":ss,"target":ts}}

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
        source_prepared = source_prepared.withColumn(
            "__join_key",
            F.when((F.col("__key_count") == 1), F.concat(F.lit("KEY:"), F.col("__normalized_primary_key")))
            .otherwise(F.concat(F.lit("SOURCE_UNMATCHED:"), F.col("__source_row_id").cast("string"))),
        )
        target_prepared = target_prepared.withColumn(
            "__join_key",
            F.when((F.col("__key_count") == 1), F.concat(F.lit("KEY:"), F.col("__normalized_primary_key")))
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
            .when(F.col("s.__source_row_id").isNotNull(), F.lit("MISSING_IN_TARGET"))
            .otherwise(F.lit("EXTRA_IN_TARGET")).alias("reconciliation_status"),
            F.when(F.col("s.__source_row_id").isNotNull() & F.col("t.__target_row_id").isNotNull(), F.lit("PRIMARY_KEY")).otherwise(F.lit(None).cast("string")).alias("match_type"),
            F.when(F.col("s.__source_row_id").isNotNull(), source_key_json).otherwise(target_key_json).alias("match_key"),
            F.col("s.__key_count").alias("_source_key_count"),
            F.col("t.__key_count").alias("_target_key_count"),
        ).persist()
        pk_build_ms = (perf_counter() - build_started) * 1000
        summary_started = perf_counter()
        summary = reconciliation.agg(
            F.sum(F.when(F.col("reconciliation_status") == "MATCHED", 1).otherwise(0)).alias("matched"),
            F.sum(F.when(F.col("reconciliation_status") == "MISSING_IN_TARGET", 1).otherwise(0)).alias("missing"),
            F.sum(F.when(F.col("reconciliation_status") == "EXTRA_IN_TARGET", 1).otherwise(0)).alias("extra"),
            F.sum(F.when(F.col("_source_key_count") == 1, 1).otherwise(0)).alias("source_unique"),
            F.sum(F.when(F.col("_target_key_count") == 1, 1).otherwise(0)).alias("target_unique"),
        ).first().asDict()
        pk_summary_ms = (perf_counter() - summary_started) * 1000
        counts = {
            "primary_matched_count": int(summary.get("matched") or 0),
            "missing_count": int(summary.get("missing") or 0),
            "extra_count": int(summary.get("extra") or 0),
            "source_unique_key_count": int(summary.get("source_unique") or 0),
            "target_unique_key_count": int(summary.get("target_unique") or 0),
        }
        result = (reconciliation, counts, {"pk_build_ms": pk_build_ms, "pk_summary_ms": pk_summary_ms})
        self._match_cache[cache_key] = result
        return result

    def _l3(self,s,t,cfg):
        from pyspark.sql import functions as F

        if not cfg.get("comparison_keys"):
            if cfg.get("matching_mode") == "GROUP_RECONCILIATION": return self._group(s,t,cfg)
            raise ValueError("Spark L3 requires comparison_keys")
        reconciliation, counts, pk_timing = self._matched_pairs(s,t,cfg)
        pairs = reconciliation.filter(F.col("reconciliation_status") == "MATCHED").select("_s", "_t", "match_type", "match_key")
        missing = reconciliation.filter(F.col("reconciliation_status") == "MISSING_IN_TARGET").select("_s.*")
        extra = reconciliation.filter(F.col("reconciliation_status") == "EXTRA_IN_TARGET").select("_t.*")
        source_stats = self._stats(s, cfg, "source")
        target_stats = self._stats(t, cfg, "target")
        sc=source_stats["total_rows"]; tc=target_stats["total_rows"]
        mic=counts["missing_count"]; ec=counts["extra_count"]; mc=counts["primary_matched_count"]
        metrics={"status":"PASS" if mic+ec==0 else "FAIL","source_record_count":sc,"target_record_count":tc,
                 "source_unique_key_count":counts["source_unique_key_count"], "target_unique_key_count":counts["target_unique_key_count"],
                 "matched_key_count":mc,"primary_matched_count":mc,
                 "missing_key_count":mic,"extra_key_count":ec,"ambiguous_record_count":0,
                 "mismatch_count":mic+ec,
                 "source_record_coverage_pct":safe_rate_pct(mc,sc,zero_value=100.0),
                 "target_record_coverage_pct":safe_rate_pct(mc,tc,zero_value=100.0),
                 "missing_record_rate_pct":safe_rate_pct(mic,sc),"extra_record_rate_pct":safe_rate_pct(ec,tc),
                 "ambiguous_record_rate_pct":0.0,
                 "matching_mode":cfg.get("matching_mode","ROW_LEVEL")}
        if cfg.get("matching_mode") == "GROUP_RECONCILIATION":
            group_started = perf_counter()
            gr=self._group(s,t,cfg); row_metrics=dict(metrics); metrics={**metrics,**gr["metrics"],"row_reconciliation":row_metrics,"group_reconciliation":gr["metrics"],
                "status":"FAIL" if row_metrics["status"]=="FAIL" or gr["metrics"]["status"]=="FAIL" else "PASS"}
            group_ms = (perf_counter() - group_started) * 1000
            evidence_started = perf_counter()
            evidence = {"matched_pairs":self._bounded(pairs, mc),"missing_records":self._bounded(missing, mic),"extra_records":self._bounded(extra, ec),"group_reconciliation":gr["evidence"].get("group_reconciliation",[])}
            evidence_ms = (perf_counter() - evidence_started) * 1000
            logger.info("SPARK_L3_TIMING pk_build_ms=%.1f pk_summary_ms=%.1f pk_evidence_ms=%.1f group_ms=%.1f", pk_timing["pk_build_ms"], pk_timing["pk_summary_ms"], evidence_ms, group_ms)
            print(f"SPARK_L3_TIMING pk_build_ms={pk_timing['pk_build_ms']:.1f} pk_summary_ms={pk_timing['pk_summary_ms']:.1f} pk_evidence_ms={evidence_ms:.1f} group_ms={group_ms:.1f}")
            return {"metrics":metrics,"evidence":evidence}
        evidence_started = perf_counter()
        evidence = {"matched_pairs":self._bounded(pairs, mc),"missing_records":self._bounded(missing, mic),"extra_records":self._bounded(extra, ec)}
        evidence_ms = (perf_counter() - evidence_started) * 1000
        logger.info("SPARK_L3_TIMING pk_build_ms=%.1f pk_summary_ms=%.1f pk_evidence_ms=%.1f group_ms=0.0", pk_timing["pk_build_ms"], pk_timing["pk_summary_ms"], evidence_ms)
        print(f"SPARK_L3_TIMING pk_build_ms={pk_timing['pk_build_ms']:.1f} pk_summary_ms={pk_timing['pk_summary_ms']:.1f} pk_evidence_ms={evidence_ms:.1f} group_ms=0.0")
        return {"metrics":metrics,"evidence":evidence}

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
            if target_column not in target_set:
                continue
            resolved.append((source_column, target_column, mapping))
        return resolved

    def _l4(self,s,t,cfg):
        from pyspark.sql import functions as F
        from pyspark.sql.types import NumericType
        keys=cfg.get("comparison_keys",[])
        if not keys: return {"metrics":{"status":"NOT_APPLICABLE","comparison_mode":"GROUP_RECONCILIATION","reason":"Row-level field comparison is not applicable without row matches."},"evidence":{}}
        reconciliation, match_counts, _ = self._matched_pairs(s,t,cfg)
        pairs = reconciliation.filter(F.col("reconciliation_status") == "MATCHED").select("_s", "_t", "match_type", "match_key")

        resolved_pairs = self._resolve_l4_column_pairs(s.columns, t.columns, cfg)
        hashed_pairs = self._matched_row_hashes(pairs, resolved_pairs).persist()
        hash_summary = hashed_pairs.agg(
            F.sum(F.when(F.col("__source_row_hash") == F.col("__target_row_hash"), 1).otherwise(0)).alias("hash_equal"),
            F.sum(F.when(~F.col("__source_row_hash").eqNullSafe(F.col("__target_row_hash")), 1).otherwise(0)).alias("hash_changed"),
        ).first()
        hash_equal = int(hash_summary["hash_equal"] or 0)
        hash_changed = int(hash_summary["hash_changed"] or 0)
        # Only changed hash candidates need field expressions.  A tolerance can
        # still make a changed-hash field PASS, so all candidates are evaluated.
        comparison_pairs = hashed_pairs.filter(~F.col("__source_row_hash").eqNullSafe(F.col("__target_row_hash")))

        # Schema types let us distinguish a real arithmetic difference from
        # a textual/value mismatch.  Numeric fields expose target - source even
        # when no tolerance is configured; non-numeric fields expose N/A.
        source_types = {field.name: field.dataType for field in s.schema.fields}
        target_types = {field.name: field.dataType for field in t.schema.fields}

        comps=[]
        for sc,tc,mapping in resolved_pairs:
            source_value = F.col(f"_s.`{sc}`")
            target_value = F.col(f"_t.`{tc}`")
            source_compare = self._apply_mapping_normalization(source_value, mapping)
            target_compare = self._apply_mapping_normalization(target_value, mapping)

            exact_match = source_compare.eqNullSafe(target_compare)
            tolerance_pct = mapping.get("tolerance_pct")
            tolerance = mapping.get("tolerance")
            comparison_type = "EXACT"
            tolerance_type = None
            tolerance_value = None

            is_numeric_field = (
                isinstance(source_types.get(sc), NumericType)
                and isinstance(target_types.get(tc), NumericType)
            )

            # Difference is meaningful for numeric fields whether comparison is
            # exact or tolerance-based.
            if is_numeric_field:
                source_number = source_compare.cast("double")
                target_number = target_compare.cast("double")
                numeric_difference = target_number - source_number
                numeric_values = source_number.isNotNull() & target_number.isNotNull()
                difference = F.when(numeric_values, numeric_difference).otherwise(
                    F.lit(None).cast("double")
                )
            else:
                source_number = None
                target_number = None
                numeric_difference = None
                numeric_values = None
                difference = F.lit(None).cast("double")

            if tolerance_pct is not None:
                # Percentage tolerance is valid only for numeric data.
                if is_numeric_field:
                    allowed = F.abs(source_number) * (
                        F.lit(float(tolerance_pct)) / F.lit(100.0)
                    )
                    tolerance_match = numeric_values & (
                        F.abs(numeric_difference) <= allowed
                    )
                else:
                    tolerance_match = F.lit(False)

                comparison_type = "PERCENTAGE_TOLERANCE"
                tolerance_value = float(tolerance_pct)
                tolerance_type = "PERCENTAGE"
                matched_expr = exact_match | tolerance_match

            elif tolerance is not None:
                # Absolute numeric tolerance.
                if is_numeric_field:
                    tolerance_match = numeric_values & (
                        F.abs(numeric_difference) <= F.lit(float(tolerance))
                    )
                else:
                    tolerance_match = F.lit(False)

                comparison_type = "NUMERIC_TOLERANCE"
                tolerance_value = float(tolerance)
                tolerance_type = "ABSOLUTE"
                matched_expr = exact_match | tolerance_match

            else:
                matched_expr = exact_match

            bad = ~matched_expr
            comps.append(
                (
                    sc,
                    tc,
                    bad,
                    difference,
                    comparison_type,
                    tolerance_value,
                    tolerance_type,
                )
            )
        count_row = comparison_pairs.agg(
            F.count(F.lit(1)).alias("matched_record_count"),
            *[
            F.sum(F.when(bad, 1).otherwise(0)).cast("long").alias(f"mismatch_{index}")
            for index, (_, _, bad, _, _, _, _) in enumerate(comps)
            ],
        ).first() if comps else None
        field_statistics=[]; total_mismatch=0
        for index, (sc,tc,bad,_,comparison_type,_,_) in enumerate(comps):
            n = int(count_row[f"mismatch_{index}"] or 0)
            total_mismatch+=n; field_statistics.append({"field":sc,"target_field":tc,"mismatches":n,"comparison_type":comparison_type})
        mr=int(match_counts.get("primary_matched_count") or 0)
        compared=mr*len(comps)
        mismatch_rows = F.array(*[
            F.when(bad, F.struct(
                F.col("match_key").alias("key"), F.col("match_type"),
                F.lit(sc).alias("source_column"), F.lit(tc).alias("target_column"),
                F.col(f"_s.`{sc}`").alias("source_value"), F.col(f"_t.`{tc}`").alias("target_value"),
                F.col("_s").alias("source_record"), F.col("_t").alias("target_record"),
                F.lit(False).alias("matched"), F.lit(comparison_type).alias("comparison_type"),
                difference.alias("difference"), F.lit(tolerance_value).cast("double").alias("tolerance"),
                F.lit(tolerance_type).cast("string").alias("tolerance_type"),
            ))
            for sc, tc, bad, difference, comparison_type, tolerance_value, tolerance_type in comps
        ])
        field_mismatches = comparison_pairs.select(F.explode(mismatch_rows).alias("mismatch")).filter(F.col("mismatch").isNotNull()).select("mismatch.*") if comps else None
        evidence_summary = field_mismatches.agg(F.countDistinct("key").alias("records_with_mismatch")).first() if field_mismatches is not None else None
        records_with_mismatch = int(evidence_summary["records_with_mismatch"] or 0) if evidence_summary is not None else 0
        source_stats = self._stats(s, cfg, "source")
        target_stats = self._stats(t, cfg, "target")
        return {"metrics":{"status":"PASS" if total_mismatch==0 else "FAIL","source_record_count":source_stats["total_rows"],"target_record_count":target_stats["total_rows"],
                "matched_record_count":mr,"compared_field_count":compared,"matched_field_count":compared-total_mismatch,"mismatch_count":total_mismatch,
                "field_conformity_pct":safe_rate_pct(compared-total_mismatch,compared,zero_value=100.0),"field_mismatch_rate_pct":safe_rate_pct(total_mismatch,compared),
                "records_with_mismatch": records_with_mismatch,
                "affected_record_rate_pct":safe_rate_pct(records_with_mismatch,mr),
                "hash_equal_record_count": hash_equal,
                "hash_changed_candidate_count": hash_changed,
                "hash_algorithm": "SHA-256",
                "missing_record_count":match_counts["missing_count"],"extra_record_count":match_counts["extra_count"],"ambiguous_record_count":0},
                "evidence":{"field_statistics":field_statistics,"comparison_keys":keys,
                            "effective_column_mappings":[{
                                "source_column": sc, "target_column": tc,
                                "normalization": dict((mapping or {}).get("normalization") or {}),
                                "tolerance": (mapping or {}).get("tolerance"),
                                "tolerance_pct": (mapping or {}).get("tolerance_pct"),
                                "comparison_type": comparison_type,
                            } for sc, tc, mapping in resolved_pairs
                              for comparison_type in [(
                                  "PERCENTAGE_TOLERANCE" if (mapping or {}).get("tolerance_pct") is not None
                                  else "NUMERIC_TOLERANCE" if (mapping or {}).get("tolerance") is not None
                                  else "EXACT"
                              )]],
                            "field_mismatches":self._bounded(field_mismatches, total_mismatch)}}

    def _agg_expr(self, op, col):
        from pyspark.sql import functions as F
        return {"SUM":F.sum,"AVG":F.avg,"MIN":F.min,"MAX":F.max,"COUNT":F.count}[op](F.col(col) if col else F.lit(1))

    def _l5(self,s,t,cfg):
        from pyspark.sql import functions as F

        rules = []
        for r in cfg.get("aggregate_rules",[]):
            op=str(r.get("function",r.get("operation",""))).upper(); sc=r.get("source_column"); tc=r.get("target_column") or sc
            if op not in {"SUM","AVG","MIN","MAX","COUNT"}: continue
            sg=r.get("source_group_by") or r.get("group_by_columns") or []; tg=r.get("target_group_by") or r.get("group_by_columns") or []
            rules.append((r, op, sc, tc, sg, tg))

        result_by_index={}
        ungrouped = [(index, rule) for index, rule in enumerate(rules) if not rule[4] and not rule[5]]
        if ungrouped:
            source_values = s.agg(*[
                self._agg_expr(op, sc).alias(f"value_{index}")
                for index, (_, op, sc, _, _, _) in ungrouped
            ]).first().asDict()
            target_values = t.agg(*[
                self._agg_expr(op, tc).alias(f"value_{index}")
                for index, (_, op, _, tc, _, _) in ungrouped
            ]).first().asDict()
            for index, (r, op, sc, tc, _, _) in ungrouped:
                sv=source_values[f"value_{index}"]; tv=target_values[f"value_{index}"]
                diff=None if sv is None or tv is None else float(tv)-float(sv)
                tol=r.get("tolerance"); tol_pct=r.get("tolerance_pct")
                if diff is None:
                    matched = sv == tv
                elif tol_pct is not None and sv is not None:
                    matched = abs(diff) <= abs(float(sv)) * (float(tol_pct) / 100.0)
                elif tol is not None:
                    matched = abs(diff) <= float(tol)
                else:
                    matched = sv == tv
                tolerance_evidence = ({"percentage": float(tol_pct)} if tol_pct is not None else tol)
                result_by_index[index] = {"rule_name":r.get("name"),"operation":op,"source_column":sc,"target_column":tc,"group":None,"source":sv,"target":tv,"difference":diff,"matched":matched,"tolerance":tolerance_evidence,"tolerance_pct":tol_pct}
        for index, (r, op, sc, tc, sg, tg) in enumerate(rules):
            if not sg and not tg:
                continue
            sa=s.groupBy(*sg).agg(self._agg_expr(op,sc).alias("sv")); ta=t.groupBy(*tg).agg(self._agg_expr(op,tc).alias("tv"))
            cond=None
            for a,b in zip(sg,tg):
                x=F.col(f"s.`{a}`").eqNullSafe(F.col(f"t.`{b}`")); cond=x if cond is None else cond & x
            g=sa.alias("s").join(ta.alias("t"),cond,"full_outer")
            diff_expr = F.col("tv").cast("double") - F.col("sv").cast("double")
            if r.get("tolerance_pct") is not None:
                allowed = F.abs(F.col("sv").cast("double")) * (F.lit(float(r["tolerance_pct"])) / F.lit(100.0))
                matched_expr = F.col("sv").eqNullSafe(F.col("tv")) | (F.col("sv").isNotNull() & F.col("tv").isNotNull() & (F.abs(diff_expr) <= allowed))
            elif r.get("tolerance") is not None:
                matched_expr = F.col("sv").eqNullSafe(F.col("tv")) | (F.col("sv").isNotNull() & F.col("tv").isNotNull() & (F.abs(diff_expr) <= F.lit(float(r["tolerance"]))))
            else:
                matched_expr = F.col("sv").eqNullSafe(F.col("tv"))
            g=g.withColumn("matched", matched_expr)
            summary = g.agg(
                F.count(F.lit(1)).alias("total"),
                F.sum(F.when(~F.col("matched"), 1).otherwise(0)).alias("failed"),
            ).first()
            total = int(summary["total"] or 0); failed = int(summary["failed"] or 0)
            result_by_index[index] = {"rule_name":r.get("name"),"operation":op,"source_column":sc,"target_column":tc,"grouped":True,"checks":total,"failed":failed,"matched":failed==0}
        results = [result_by_index[index] for index in range(len(rules))]
        failed=sum(1 for x in results if not x["matched"])
        checks_total = sum(item.get("checks", 1) for item in results)
        checks_failed = sum(item.get("failed", 0) if item.get("grouped") else int(not item["matched"]) for item in results)
        return {"metrics":{"status":"PASS" if failed==0 else "FAIL","rules_total":len(rules),"checks_total":checks_total,"checks_passed":checks_total-checks_failed,"checks_failed":checks_failed,
                "aggregate_check_pass_rate_pct":safe_rate_pct(checks_total-checks_failed,checks_total,zero_value=100.0),"aggregate_check_failure_rate_pct":safe_rate_pct(checks_failed,checks_total)},
                "evidence":{"aggregate_results":results}}

    def _group(self,s,t,cfg):
        from pyspark.sql import functions as F
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

        def build(df, side):
            from pyspark.sql.window import Window
            prepared = prepare(df, side)
            aggregate_exprs=[F.count(F.lit(1)).alias("__present")]
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

            # Compute a real deterministic MODE instead of first().  Ties are
            # resolved by the lexically smallest normalized value so repeated
            # runs return the same result regardless of partition order.
            for index, value_expr in mode_specs:
                mode_values = prepared.select(
                    *[F.col(alias) for alias in group_aliases],
                    value_expr.alias("__mode_value"),
                ).filter(F.col("__mode_value").isNotNull())
                counts = mode_values.groupBy(*group_aliases, "__mode_value").agg(F.count(F.lit(1)).alias("__mode_count"))
                window = Window.partitionBy(*group_aliases).orderBy(F.desc("__mode_count"), F.asc(F.col("__mode_value").cast("string")))
                modes = (counts.withColumn("__mode_rank", F.row_number().over(window))
                         .filter(F.col("__mode_rank") == 1)
                         .select(*group_aliases, F.col("__mode_value").alias(f"a{index}")))
                result = result.join(modes, group_aliases, "left")

            return result

        a=build(s,"source").alias("s")
        b=build(t,"target").alias("t")
        cond=None
        for alias in group_aliases:
            q=F.col(f"s.`{alias}`").eqNullSafe(F.col(f"t.`{alias}`"))
            cond=q if cond is None else cond&q
        j=a.join(b,cond,"full_outer").persist()
        group_summary = j.agg(
            F.sum(F.when(F.col("s.__present").isNotNull(), 1).otherwise(0)).alias("source_groups"),
            F.sum(F.when(F.col("t.__present").isNotNull(), 1).otherwise(0)).alias("target_groups"),
            F.sum(F.when(F.col("s.__present").isNotNull() & F.col("t.__present").isNotNull(), 1).otherwise(0)).alias("common_groups"),
        ).first()
        source_groups=int(group_summary["source_groups"] or 0)
        target_groups=int(group_summary["target_groups"] or 0)
        common=int(group_summary["common_groups"] or 0)
        missing=source_groups-common
        extra=target_groups-common

        group_key = F.array(*[
            F.coalesce(F.col(f"s.`{alias}`").cast("string"), F.col(f"t.`{alias}`").cast("string"))
            for alias in group_aliases
        ])
        presence_rows=j.filter(F.col("s.__present").isNull() | F.col("t.__present").isNull()).select(
            group_key.alias("group_key"),
            F.lit(None).cast("string").alias("source_aggregate"), F.lit(None).cast("string").alias("target_aggregate"),
            F.lit(None).cast("string").alias("source_column"), F.lit(None).cast("string").alias("target_column"),
            F.lit(None).cast("string").alias("operation"), F.lit(None).cast("double").alias("difference"),
            F.when(F.col("s.__present").isNull(), F.lit("EXTRA_GROUP_IN_TARGET")).otherwise(F.lit("MISSING_GROUP_IN_TARGET")).alias("status"),
        ).withColumn("matched", F.lit(False))

        common_rows=j.filter(F.col("s.__present").isNotNull() & F.col("t.__present").isNotNull())
        aggregate_structs=[]
        for index,item in enumerate(aggs):
            source_value,target_value=F.col(f"s.a{index}"),F.col(f"t.a{index}")
            status=(F.when(source_value.isNull() & target_value.isNull(), F.lit("NOT_APPLICABLE"))
                    .when(source_value.eqNullSafe(target_value), F.lit("PASS"))
                    .otherwise(F.lit("GROUP_VALUE_MISMATCH")))
            aggregate_structs.append(F.struct(
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
        result_rows=presence_rows if aggregate_rows is None else presence_rows.unionByName(aggregate_rows)
        result_rows=result_rows.persist()
        result_summary=result_rows.agg(
            F.count(F.lit(1)).alias("result_row_count"),
            F.sum(F.when(~F.col("status").isin("MISSING_GROUP_IN_TARGET","EXTRA_GROUP_IN_TARGET","NOT_APPLICABLE"),1).otherwise(0)).alias("applicable"),
            F.sum(F.when(F.col("status")=="GROUP_VALUE_MISMATCH",1).otherwise(0)).alias("failed"),
            F.countDistinct(F.when(F.col("status")=="GROUP_VALUE_MISMATCH",F.to_json(F.col("group_key")))).alias("mismatch_groups"),
        ).first()
        aggregate_checks_total=int(result_summary["applicable"] or 0)
        aggregate_checks_failed=int(result_summary["failed"] or 0)
        mismatch_groups=int(result_summary["mismatch_groups"] or 0)
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
        # Evidence is an exception list.  Keeping passing aggregates here makes
        # large reconciliations slow to persist and buries the actionable rows.
        # The complete pass/fail totals remain in metrics above.
        exception_rows = result_rows.filter(~F.col("status").isin("PASS", "NOT_APPLICABLE"))
        exception_count = missing + extra + aggregate_checks_failed
        return {"metrics":metrics,"evidence":{"group_reconciliation":self._bounded(exception_rows, exception_count)}}

    def _l6(self,s,t,cfg):
        from pyspark.sql import functions as F
        out=[]
        rules_by_side={"SOURCE":[],"TARGET":[]}
        for r in cfg.get("dq_rules",[]):
            if not r.get("enabled",True): continue
            typ=str(r.get("rule_type","")).upper(); apply=str(r.get("apply_to","BOTH")).upper()
            for side,df in (("SOURCE",s),("TARGET",t)):
                if apply not in ("BOTH",side): continue
                col=r.get("source_column" if side=="SOURCE" else "target_column") or r.get("column")
                if not col or col not in df.columns: continue
                c=F.col(col); invalid=None
                if typ=="PATTERN": invalid=c.isNotNull() & ~c.cast("string").rlike(r.get("regex",""))
                elif typ=="COMPLETENESS": invalid=c.isNull() | (F.trim(c.cast("string"))=="")
                elif typ=="VALIDITY":
                    allowed=r.get("allowed_values") or (r.get("value") if isinstance(r.get("value"),list) else None)
                    if allowed is not None:
                        invalid=~c.isin(allowed)
                    elif r.get("min") is not None or r.get("max") is not None:
                        numeric = c.cast("double")
                        invalid=F.lit(False) | numeric.isNull()
                        if r.get("min") is not None: invalid=invalid | (numeric < float(r["min"]))
                        if r.get("max") is not None: invalid=invalid | (numeric > float(r["max"]))
                if invalid is not None:
                    rules_by_side[side].append((r,typ,col,invalid))
        for side, df in (("SOURCE",s),("TARGET",t)):
            side_rules=rules_by_side[side]
            if not side_rules:
                continue
            summary=df.agg(
                F.count(F.lit(1)).alias("total"),
                *[
                    F.sum(F.when(invalid,1).otherwise(0)).alias(f"failed_{index}")
                    for index, (_,_,_,invalid) in enumerate(side_rules)
                ],
            ).first()
            total=int(summary["total"] or 0)
            for index,(r,typ,col,invalid) in enumerate(side_rules):
                failed=int(summary[f"failed_{index}"] or 0)
                item={"rule_id":r.get("rule_id"),"rule_name":r.get("name"),"rule_type":typ,"side":side,"column":col,"total_count":total,"failed_count":failed,"passed_count":total-failed,"status":"PASS" if failed==0 else "FAIL"}
                if failed:
                    # Bounded evidence only; full data remains distributed.
                    failed_rows = df.filter(invalid).limit(self.evidence_limit).collect()
                    records=[]
                    for row in failed_rows:
                        record=row.asDict(recursive=True)
                        records.append({
                            "record": record,
                            "column": col,
                            "value": record.get(col),
                            "rule": {"rule_id": r.get("rule_id"), "name": r.get("name"), "rule_type": typ},
                            "reason": f"{typ} validation failed",
                            "status": "FAIL",
                        })
                    item["source_failed_records" if side=="SOURCE" else "target_failed_records"] = records
                out.append(item)
        failed=sum(1 for x in out if x["status"]=="FAIL")
        checks_total=sum(x["total_count"] for x in out); checks_failed=sum(x["failed_count"] for x in out)
        dq_results = [{**item, "matched": item["status"] == "PASS"} for item in out]
        return {"metrics":{"status":"PASS" if checks_failed==0 else "FAIL","rules_total":len(out),"rules_failed":failed,"rules_passed":len(out)-failed,"checks_total":checks_total,"checks_passed":checks_total-checks_failed,"checks_failed":checks_failed,"pass_percentage":safe_rate_pct(checks_total-checks_failed,checks_total,zero_value=100.0),"failure_percentage":safe_rate_pct(checks_failed,checks_total)},"evidence":{"dq_results":dq_results, "rule_results":dq_results}}

    def _bounded(self, df, total_count: int | None = None):
        if df is None:
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
