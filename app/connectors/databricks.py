from __future__ import annotations

from typing import Any, Iterator
from decimal import Decimal
import logging

from databricks import sql

from app.connectors.base import (
    ColumnMetadata,
    DatasetSchema,
    MetadataProvider,
    DataProvider,
    ConnectionProvider,
)
from app.connectors.filters import RowFilter


class DatabricksConnector(
    MetadataProvider,
    DataProvider,
    ConnectionProvider,
):
    """
    Databricks SQL connector.

    Supports:

        - connection testing
        - table schema discovery
        - record retrieval

    Dataset example:

        {
            "catalog": "main",
            "schema": "sales",
            "table": "customers"
        }
    """

    DEFAULT_CHUNK_SIZE = 1000
    _logger = logging.getLogger(__name__)

    @staticmethod
    def resolve_connection_properties(properties: dict[str, Any]) -> dict[str, Any]:
        nested = properties.get("connection")
        if not isinstance(nested, dict):
            return properties

        def usable(value: Any) -> bool:
            return bool(value) and str(value) not in {"********", "[REDACTED]", "REDACTED"}

        resolved = dict(properties)
        for key in ("server_hostname", "http_path", "access_token"):
            if usable(resolved.get(key)):
                continue
            if usable(nested.get(key)):
                resolved[key] = nested[key]
        return resolved

    # ====================================================
    # CAPABILITIES
    # ====================================================

    def supports_pushdown(
        self,
        comparison_level: str,
    ) -> bool:

        return comparison_level.upper() == "L2"

    # ====================================================
    # CONNECTION
    # ====================================================

    def _connect(
        self,
        properties: dict[str, Any],
    ):

        properties = self.resolve_connection_properties(properties)

        host = self._plain_credential(properties.get(
            "server_hostname"
        ))

        http_path = self._plain_credential(properties.get(
            "http_path"
        ))

        access_token = self._plain_credential(properties.get(
            "access_token"
        ))

        if not host:
            raise ValueError(
                "Databricks requires 'server_hostname'"
            )

        if not http_path:
            raise ValueError(
                "Databricks requires 'http_path'"
            )

        if not access_token:
            raise ValueError(
                "Databricks requires 'access_token'"
            )

        return sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=access_token,
            use_cloud_fetch=False,
        )

    @staticmethod
    def _plain_credential(value: Any) -> Any:
        """Return the scalar value expected by databricks-sql-connector."""
        if hasattr(value, "get_secret_value"):
            return value.get_secret_value()
        if isinstance(value, dict):
            for key in ("value", "secret", "access_token", "token"):
                if key in value:
                    return DatabricksConnector._plain_credential(value[key])
        return value

    # ====================================================
    # TEST CONNECTION
    # ====================================================

    def test_connection(
        self,
        properties: dict[str, Any],
    ) -> dict[str, Any]:

        connection = None

        try:

            connection = self._connect(
                properties
            )

            cursor = connection.cursor()

            cursor.execute(
                "SELECT 1"
            )

            result = cursor.fetchone()

            cursor.close()

            return {
                "status": "CONNECTED",
                "message": (
                    "Databricks connection "
                    "successful."
                ),
                "test_result": (
                    result[0]
                    if result
                    else None
                ),
            }

        finally:

            if connection is not None:
                connection.close()

    # ====================================================
    # SCHEMA & METADATA DISCOVERY
    # ====================================================

    def list_catalogs(self, properties: dict[str, Any]) -> list[str]:
        connection = None
        cursor = None
        try:
            connection = self._connect(properties)
            cursor = connection.cursor()
            cursor.execute("SHOW CATALOGS")
            rows = cursor.fetchall()
            return [row[0] for row in rows if row]
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    def list_schemas(
        self,
        properties: dict[str, Any],
        catalog: str | None = None,
    ) -> list[str]:
        connection = None
        cursor = None
        try:
            connection = self._connect(properties)
            cursor = connection.cursor()
            
            query = "SHOW SCHEMAS"
            if catalog:
                query += f" IN `{catalog}`"
                
            cursor.execute(query)
            rows = cursor.fetchall()
            return [row[0] for row in rows if row]
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    def list_tables(
        self,
        properties: dict[str, Any],
        schema: str | None = None,
        catalog: str | None = None,
    ) -> list[str]:
        connection = None
        cursor = None
        try:
            connection = self._connect(properties)
            cursor = connection.cursor()
            
            query = "SHOW TABLES"
            if catalog and schema:
                query += f" IN `{catalog}`.`{schema}`"
            elif schema:
                query += f" IN `{schema}`"
                
            cursor.execute(query)
            rows = cursor.fetchall()
            # SHOW TABLES returns (database, tableName, isTemporary)
            # We want the tableName, which is usually the second column,
            # but in some versions/contexts it might just be the first.
            # Databricks SHOW TABLES typically returns (database, tableName, isTemporary)
            
            tables = []
            for row in rows:
                if not row:
                    continue
                # If there are multiple columns, tableName is typically index 1
                if len(row) > 1:
                    tables.append(row[1])
                else:
                    tables.append(row[0])
            return tables
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    def get_schema(
        self,
        dataset: dict[str, Any],
    ) -> DatasetSchema:

        properties = dataset.get(
            "properties",
            {},
        )

        nested_connection = properties.get("connection")
        connection_properties = (
            nested_connection
            if isinstance(nested_connection, dict)
            and any(
                key in nested_connection
                for key in ("server_hostname", "http_path", "access_token")
            )
            else properties
        )

        catalog = properties.get(
            "catalog"
        )

        schema_name = properties.get(
            "schema"
        )

        table = properties.get(
            "table"
        )

        if not catalog:
            raise ValueError(
                "Databricks dataset requires "
                "'catalog'"
            )

        if not schema_name:
            raise ValueError(
                "Databricks dataset requires "
                "'schema'"
            )

        if not table:
            raise ValueError(
                "Databricks dataset requires "
                "'table'"
            )

        connection = None
        cursor = None

        try:

            connection = self._connect(
                self.resolve_connection_properties(properties)
            )

            cursor = connection.cursor()

            columns = []
            metadata_error = None
            information_schema = (
                f"{self._quote_identifier(catalog)}."
                f"{self._quote_identifier('INFORMATION_SCHEMA')}."
                f"{self._quote_identifier('COLUMNS')}"
            )
            query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
                FROM {information_schema}
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """

            try:
                try:
                    cursor.execute(query, parameters=[schema_name, table])
                except TypeError:
                    cursor.execute(
                        query.replace("?", "'" + str(schema_name).replace("'", "''") + "'", 1)
                            .replace("?", "'" + str(table).replace("'", "''") + "'", 1)
                    )
                rows = cursor.fetchall()
                for row in rows:
                    if not row or not row[0]:
                        continue
                    columns.append(ColumnMetadata(
                        name=row[0],
                        data_type=str(row[1] or "UNKNOWN"),
                        nullable=str(row[2]).upper() != "NO" if len(row) > 2 and row[2] is not None else True,
                        ordinal_position=int(row[3]) if len(row) > 3 and row[3] is not None else len(columns),
                    ))
            except Exception as exc:
                metadata_error = exc
                columns = []

            if not columns:
                try:
                    cursor.execute(
                        f"SHOW COLUMNS IN {self._dataset_table_identifier(properties)}"
                    )
                    rows = cursor.fetchall()
                    for ordinal, row in enumerate(rows):
                        if not row or not row[0]:
                            continue
                        columns.append(ColumnMetadata(
                            name=row[0],
                            data_type=str(row[1] or "UNKNOWN") if len(row) > 1 else "UNKNOWN",
                            ordinal_position=ordinal,
                        ))
                except Exception as fallback_error:
                    if metadata_error is not None:
                        raise RuntimeError(
                            "Unable to read Databricks schema metadata: "
                            f"{type(metadata_error).__name__}; fallback "
                            f"{type(fallback_error).__name__}"
                        ) from fallback_error
                    raise

            if not columns:
                raise RuntimeError(
                    "Schema metadata is not accessible for the selected Databricks table."
                )

            cursor.close()

            return DatasetSchema(
                columns=tuple(columns),
                metadata={
                    "connector_type": (
                        "databricks"
                    ),
                    "catalog": catalog,
                    "schema": schema_name,
                    "table": table,
                },
            )

        finally:

            if connection is not None:
                connection.close()

    # ====================================================
    # RECORDS
    # ====================================================

    def get_records(
        self,
        dataset: dict[str, Any],
    ) -> list[dict[str, Any]]:

        return list(
            self.iter_records(dataset)
        )

    def iter_records(
        self,
        dataset: dict[str, Any],
    ) -> Iterator[dict[str, Any]]:

        for chunk in self.iter_chunks(
            dataset,
            chunk_size=self.DEFAULT_CHUNK_SIZE,
        ):
            for record in chunk:
                yield record

    def iter_chunks(
        self,
        dataset: dict[str, Any],
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> Iterator[list[dict[str, Any]]]:

        if chunk_size <= 0:
            raise ValueError(
                "chunk_size must be greater than zero"
            )

        properties = dataset.get(
            "properties",
            {},
        )

        connection_properties = self.resolve_connection_properties(properties)

        catalog = properties.get(
            "catalog"
        )

        schema_name = properties.get(
            "schema"
        )

        table = properties.get(
            "table"
        )

        if not catalog:
            raise ValueError(
                "Databricks dataset requires "
                "'catalog'"
            )

        if not schema_name:
            raise ValueError(
                "Databricks dataset requires "
                "'schema'"
            )

        if not table:
            raise ValueError(
                "Databricks dataset requires "
                "'table'"
            )

        connection = None
        cursor = None

        try:

            connection = self._connect(
                connection_properties
            )

            cursor = connection.cursor()

            table_identifier = self._dataset_table_identifier(properties)
            query = f"SELECT * FROM {table_identifier}"
            query += self._build_filter_clause(properties.get("_filters", []))

            try:
                cursor.execute(query)

                columns = [
                    description[0]
                    for description
                    in cursor.description
                ]

                while True:

                    rows = cursor.fetchmany(
                        chunk_size
                    )

                    if not rows:
                        break

                    yield self._rows_to_records(
                        columns,
                        rows,
                    )

            except Exception as exc:
                self._logger.exception(
                    "Databricks row retrieval failed for %s: %s: %r",
                    table_identifier,
                    type(exc).__name__,
                    exc,
                )
                raise

        finally:

            if cursor is not None:
                cursor.close()

            if connection is not None:
                connection.close()

    @staticmethod
    def _rows_to_records(
        columns: list[str],
        rows,
    ) -> list[dict[str, Any]]:

        return [
            dict(
                zip(
                    columns,
                    row,
                )
            )
            for row in rows
        ]

    # ====================================================
    # L2 PUSHDOWN STATISTICS
    # ====================================================

    def get_volume_statistics(
        self,
        dataset: dict[str, Any],
        business_keys: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:

        properties = dataset.get(
            "properties",
            {},
        )

        connection_properties = self.resolve_connection_properties(properties)

        table_identifier = self._dataset_table_identifier(
            properties
        )

        normalized_keys = [
            self._quote_identifier(key)
            for key in business_keys or []
        ]

        query = self._build_volume_statistics_query(
            table_identifier,
            normalized_keys,
            filters or properties.get("_filters", []),
        )

        connection = None
        cursor = None

        try:

            connection = self._connect(
                connection_properties
            )

            cursor = connection.cursor()

            cursor.execute(query)

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError(
                    "Databricks volume statistics query "
                    "returned no result."
                )

            return {
                "total_rows": int(row[0] or 0),
                "filtered_rows": int(row[1] or 0),
                "partition_rows": int(row[2] or 0),
                "distinct_key_count": int(row[3] or 0),
                "duplicate_key_count": int(row[4] or 0),
                "null_key_count": int(row[5] or 0),
                "null_counts": {},
            }

        finally:

            if cursor is not None:
                cursor.close()

            if connection is not None:
                connection.close()

    @classmethod
    def _build_volume_statistics_query(
        cls,
        table_identifier: str,
        quoted_keys: list[str],
        filters: list[dict[str, Any]] | None = None,
    ) -> str:

        filter_clause = cls._build_filter_clause(filters or [])

        if not quoted_keys:
            return f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) AS filtered_rows,
                COUNT(*) AS partition_rows,
                0 AS distinct_key_count,
                0 AS duplicate_key_count,
                0 AS null_key_count
            FROM {table_identifier}{filter_clause}
            """

        key_list = ", ".join(
            quoted_keys
        )

        null_condition = " OR ".join(
            f"{key} IS NULL"
            for key in quoted_keys
        )

        return f"""
        WITH base AS (
            SELECT {key_list}
            FROM {table_identifier}{filter_clause}
        ),
        key_groups AS (
            SELECT
                {key_list},
                COUNT(*) AS key_count
            FROM base
            WHERE NOT ({null_condition})
            GROUP BY {key_list}
        )
        SELECT
            (SELECT COUNT(*) FROM base) AS total_rows,
            (SELECT COUNT(*) FROM base) AS filtered_rows,
            (SELECT COUNT(*) FROM base) AS partition_rows,
            (SELECT COUNT(*) FROM key_groups)
                AS distinct_key_count,
            COALESCE(
                (
                    SELECT SUM(key_count - 1)
                    FROM key_groups
                    WHERE key_count > 1
                ),
                0
            ) AS duplicate_key_count,
            (
                SELECT COUNT(*)
                FROM base
                WHERE {null_condition}
            ) AS null_key_count
        """

    @classmethod
    def _build_filter_clause(cls, filters: list[dict[str, Any]]) -> str:
        clauses = []
        for raw in filters:
            item = RowFilter.model_validate(raw)
            field = cls._quote_identifier(item.field)
            if item.operator in {"IS NULL", "IS NOT NULL"}:
                clauses.append(f"{field} {item.operator}")
                continue
            values = item.value if item.operator == "IN" else [item.value]
            rendered = ", ".join(cls._quote_literal(value) for value in values)
            clauses.append(f"{field} {item.operator} ({rendered})" if item.operator == "IN" else f"{field} {item.operator} {rendered}")
        return f" WHERE {' AND '.join(clauses)}" if clauses else ""

    @staticmethod
    def _quote_literal(value: Any) -> str:
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float, Decimal)):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    @classmethod
    def _dataset_table_identifier(
        cls,
        properties: dict[str, Any],
    ) -> str:

        catalog = properties.get(
            "catalog"
        )

        schema_name = properties.get(
            "schema"
        )

        table = properties.get(
            "table"
        )

        if not catalog:
            raise ValueError(
                "Databricks dataset requires 'catalog'"
            )

        if not schema_name:
            raise ValueError(
                "Databricks dataset requires 'schema'"
            )

        if not table:
            raise ValueError(
                "Databricks dataset requires 'table'"
            )

        return ".".join(
            [
                cls._quote_identifier(catalog),
                cls._quote_identifier(schema_name),
                cls._quote_identifier(table),
            ]
        )

    @staticmethod
    def _quote_identifier(
        value: str,
    ) -> str:

        if not isinstance(value, str):
            raise ValueError(
                "SQL identifier must be a string"
            )

        normalized = value.strip()

        if not normalized:
            raise ValueError(
                "SQL identifier cannot be empty"
            )

        if "`" in normalized:
            raise ValueError(
                "SQL identifier cannot contain backticks"
            )

        return f"`{normalized}`"