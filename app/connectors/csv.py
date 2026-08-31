from __future__ import annotations

import csv
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

from app.connectors.base import (
    ColumnMetadata,
    DatasetSchema,
    MetadataProvider,
    DataProvider,
    ConnectionProvider,
)
from app.connectors.filters import filter_records


class CSVMetadataProvider(
    MetadataProvider,
    DataProvider,
    ConnectionProvider,
):
    """
    Connector-specific metadata provider for CSV datasets.

    This class is responsible only for converting CSV metadata
    into the connector-neutral DatasetSchema model.

    Comparators never depend on this class directly.
    """

    SCHEMA_SAMPLE_SIZE = 1000
    UPLOAD_ROOT = Path("/app/data/uploads")

    def get_schema(
        self,
        dataset: dict[str, Any],
    ) -> DatasetSchema:

        properties = dataset.get(
            "properties",
            {},
        )

        path_value = properties.get("path")

        if not path_value:
            raise ValueError(
                "CSV dataset requires 'properties.path'"
            )

        filename = properties.get("filename")
        path = self._resolve_csv_path(
            Path(path_value),
            str(filename) if filename else None,
        )

        if not path.exists():
            raise FileNotFoundError(
                f"CSV dataset not found: {path}"
            )

        if not path.is_file():
            raise ValueError(
                f"CSV dataset path is not a file: {path}"
            )

        delimiter = properties.get(
            "delimiter",
            ",",
        )

        encoding = properties.get(
            "encoding",
            "utf-8",
        )

        with path.open(
            mode="r",
            encoding=encoding,
            newline="",
        ) as file:

            reader = csv.reader(
                file,
                delimiter=delimiter,
            )

            headers = next(
                reader,
                [],
            )

            if not headers:
                raise ValueError(
                    f"CSV dataset has no columns: {path}"
                )

            row_count = 0

            sample_values: list[list[str]] = [
                []
                for _ in headers
            ]

            for row in reader:

                row_count += 1

                if row_count > self.SCHEMA_SAMPLE_SIZE:
                    continue

                for index in range(len(headers)):
                    if len(row) > index:
                        sample_values[index].append(
                            row[index]
                        )

        if len(headers) != len(set(headers)):
            raise ValueError(
                "CSV dataset contains duplicate column names"
            )

        columns: list[ColumnMetadata] = []

        for index, name in enumerate(headers):

            metadata = self._infer_column_metadata(
                name=name,
                values=sample_values[index],
                ordinal_position=index,
            )

            columns.append(metadata)

        return DatasetSchema(
            columns=tuple(columns),
            metadata={
                "connector_type": "csv",
                "path": str(path),
                "file_size_bytes": path.stat().st_size,
                "row_count": row_count,
            },
        )

    # ========================================================
    # CONNECTION
    # ========================================================

    def _resolve_csv_path(
        self,
        path: Path,
        filename: str | None = None,
    ) -> Path:
        if path.exists():
            return path

        names = self._fallback_filenames(
            filename or path.name,
        )
        candidates = [
            candidate
            for name in names
            for candidate in self.UPLOAD_ROOT.glob(f"*/{name}")
            if candidate.is_file()
        ]

        if not candidates:
            return path

        return max(
            candidates,
            key=lambda candidate: candidate.stat().st_mtime,
        )

    @staticmethod
    def _fallback_filenames(
        filename: str,
    ) -> list[str]:
        names = [Path(filename).name]
        lowered = names[0].lower()

        if lowered not in names:
            names.append(lowered)

        source_match = re.fullmatch(
            r"source_data_s(\d+)\.csv",
            lowered,
        )
        if source_match:
            number = source_match.group(1)
            names.extend(
                [
                    f"source_t{number}.csv",
                    f"source_csv_t{number}.csv",
                ]
            )

        target_match = re.fullmatch(
            r"target_data_t(\d+)\.csv",
            lowered,
        )
        if target_match:
            number = target_match.group(1)
            names.extend(
                [
                    f"target_t{number}.csv",
                    f"target_csv_t{number}.csv",
                ]
            )

        return list(dict.fromkeys(names))

    def test_connection(
        self,
        properties: dict[str, Any],
    ) -> dict[str, Any]:

        path_value = properties.get("path")

        if not path_value:
            raise ValueError(
                "CSV connection requires 'path'"
            )

        path = Path(path_value)

        if not path.exists():
            raise FileNotFoundError(
                f"CSV file not found: {path}"
            )

        if not path.is_file():
            raise ValueError(
                f"CSV path is not a file: {path}"
            )

        delimiter = properties.get(
            "delimiter",
            ",",
        )

        encoding = properties.get(
            "encoding",
            "utf-8",
        )

        with path.open(
            mode="r",
            encoding=encoding,
            newline="",
        ) as file:

            reader = csv.reader(
                file,
                delimiter=delimiter,
            )

            headers = next(
                reader,
                [],
            )

        if not headers:
            raise ValueError(
                f"CSV file has no header: {path}"
            )

        if len(headers) != len(set(headers)):
            raise ValueError(
                "CSV dataset contains duplicate column names"
            )

        return {
            "status": "CONNECTED",
            "message": "CSV connection successful.",
            "path": str(path),
            "column_count": len(headers),
            "columns": headers,
        }

    # ========================================================
    # COLUMN METADATA INFERENCE
    # ========================================================

    def _infer_column_metadata(
        self,
        name: str,
        values: list[str],
        ordinal_position: int,
    ) -> ColumnMetadata:

        normalized_values = [
            value.strip()
            for value in values
        ]

        non_null_values = [
            value
            for value in normalized_values
            if value != ""
        ]

        nullable = (
            len(non_null_values)
            != len(normalized_values)
        )

        data_type = self._infer_data_type(
            non_null_values
        )

        length = None
        precision = None
        scale = None

        if data_type == "STRING":

            if non_null_values:
                length = max(
                    len(value)
                    for value in non_null_values
                )

        elif data_type == "DECIMAL":

            precision, scale = (
                self._infer_decimal_metadata(
                    non_null_values
                )
            )

        return ColumnMetadata(
            name=name,
            data_type=data_type,
            nullable=nullable,
            ordinal_position=ordinal_position,
            length=length,
            precision=precision,
            scale=scale,
            metadata={
                "connector_type": "csv",
            },
        )

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

        properties = dataset.get(
            "properties",
            {},
        )

        path_value = properties.get("path")

        if not path_value:
            raise ValueError(
                "CSV dataset requires 'properties.path'"
            )

        path = Path(path_value)

        if not path.exists():
            raise FileNotFoundError(
                f"CSV dataset not found: {path}"
            )

        delimiter = properties.get(
            "delimiter",
            ",",
        )

        encoding = properties.get(
            "encoding",
            "utf-8",
        )

        with path.open(
            mode="r",
            encoding=encoding,
            newline="",
        ) as file:

            reader = csv.DictReader(
                file,
                delimiter=delimiter,
            )

            if reader.fieldnames is None:
                raise ValueError(
                    f"CSV dataset has no header: {path}"
                )

            filters = properties.get("_filters", [])
            for row in filter_records(reader, filters):
                yield dict(row)

    # ========================================================
    # TYPE INFERENCE
    # ========================================================

    @staticmethod
    def _infer_data_type(
        values: list[str],
    ) -> str:

        if not values:
            return "STRING"

        if all(
            CSVMetadataProvider._is_boolean(value)
            for value in values
        ):
            return "BOOLEAN"

        if all(
            CSVMetadataProvider._is_integer(value)
            for value in values
        ):
            return "INTEGER"

        if all(
            CSVMetadataProvider._is_decimal(value)
            for value in values
        ):
            return "DECIMAL"

        if all(
            CSVMetadataProvider._is_datetime(value)
            for value in values
        ):
            return "DATETIME"

        return "STRING"

    # ========================================================
    # VALUE TYPE HELPERS
    # ========================================================

    @staticmethod
    def _is_boolean(
        value: str,
    ) -> bool:

        return value.lower() in {
            "true",
            "false",
            "yes",
            "no",
        }

    @staticmethod
    def _is_integer(
        value: str,
    ) -> bool:

        try:
            int(value.strip())
            return True
        except (
            TypeError,
            ValueError,
        ):
            return False

    @staticmethod
    def _is_decimal(
        value: str,
    ) -> bool:

        try:
            Decimal(value.strip())
            return True
        except (
            TypeError,
            ValueError,
            InvalidOperation,
        ):
            return False

    @staticmethod
    def _is_datetime(
        value: str,
    ) -> bool:

        try:
            datetime.fromisoformat(
                value.strip()
            )
            return True
        except (
            TypeError,
            ValueError,
        ):
            return False
    # ========================================================
    # DECIMAL METADATA
    # ========================================================

    @staticmethod
    def _infer_decimal_metadata(
        values: list[str],
    ) -> tuple[int | None, int | None]:

        if not values:
            return None, None

        max_integer_digits = 0
        max_scale = 0

        for value in values:
            decimal_value = Decimal(value)

            sign, digits, exponent = decimal_value.as_tuple()

            if exponent >= 0:
                integer_digits = len(digits) + exponent
                scale = 0
            else:
                scale = -exponent
                integer_digits = max(len(digits) - scale, 0)

            max_integer_digits = max(
                max_integer_digits,
                integer_digits,
            )

            max_scale = max(
                max_scale,
                scale,
            )

        precision = max_integer_digits + max_scale

        return precision, max_scale