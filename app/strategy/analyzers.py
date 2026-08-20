from abc import ABC, abstractmethod
import csv
from pathlib import Path
from typing import Any


class DatasetAnalyzer(ABC):
    """
    Contract for analyzing a source or target dataset.

    The StrategyPlanner depends only on this contract.
    Connector-specific implementations live behind it.
    """

    @abstractmethod
    def analyze(
        self,
        connector_type: str,
        properties: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Analyze a dataset and return connector-neutral metadata.
        """
        raise NotImplementedError


class CSVDatasetAnalyzer(DatasetAnalyzer):
    """
    Dataset analyzer for CSV sources/targets.

    CSV-specific logic is isolated here.
    """
    capabilities = {
        "supports_hash": True,
        "supports_sampling": True,
    }

    def analyze(
        self,
        connector_type: str,
        properties: dict[str, Any],
    ) -> dict[str, Any]:

        path_value = properties.get("path")

        if not path_value:
            raise ValueError(
                "CSV dataset requires 'path' property."
            )

        path = Path(path_value)

        if not path.exists():
            raise FileNotFoundError(
                f"Dataset not found: {path}"
            )

        if not path.is_file():
            raise ValueError(
                f"Dataset path is not a file: {path}"
            )

        with path.open(
            mode="r",
            encoding=properties.get(
                "encoding",
                "utf-8",
            ),
            newline="",
        ) as file:

            reader = csv.reader(file)

            columns = next(
                reader,
                [],
            )

            row_count = sum(
                1
                for _ in reader
            )

        return {
            "connector_type": connector_type,
            "path": str(path),
            "file_size_bytes": path.stat().st_size,
            "columns": columns,
            "column_count": len(columns),
            "row_count": row_count,
        }


class DatabricksDatasetAnalyzer(DatasetAnalyzer):
    """
    Lightweight Databricks planning metadata.

    The connector remains responsible for executing Databricks
    queries. The planner only needs declared capabilities and any
    supplied estimates.
    """

    capabilities = {
        "supports_hash": True,
        "supports_sampling": True,
    }

    def analyze(
        self,
        connector_type: str,
        properties: dict[str, Any],
    ) -> dict[str, Any]:

        declared_row_count = properties.get("row_count")
        return {
            "connector_type": connector_type,
            "catalog": properties.get("catalog"),
            "schema": properties.get("schema"),
            "table": properties.get("table"),
            "file_size_bytes": properties.get(
                "file_size_bytes",
                0,
            ),
            "columns": properties.get(
                "columns",
                [],
            ),
            "column_count": len(
                properties.get(
                    "columns",
                    [],
                )
            ),
            "row_count": declared_row_count,
            "row_count_known": declared_row_count is not None,
        }


class DatasetAnalyzerRegistry:
    """
    Registry for connector-specific dataset analyzers.

    The planner does not know which connector implementations
    exist. New connectors are registered here during application
    bootstrap.
    """

    def __init__(
        self,
        analyzers: dict[
            str,
            DatasetAnalyzer,
        ] | None = None,
    ) -> None:

        self._analyzers = {}

        if analyzers:
            for connector_type, analyzer in analyzers.items():
                self.register(
                    connector_type,
                    analyzer,
                )

    def register(
        self,
        connector_type: str,
        analyzer: DatasetAnalyzer,
    ) -> None:

        key = connector_type.strip().lower()

        if not key:
            raise ValueError(
                "connector_type cannot be empty."
            )

        if key in self._analyzers:
            raise ValueError(
                f"Dataset analyzer already registered: "
                f"{connector_type}"
            )

        self._analyzers[key] = analyzer

    def get(
        self,
        connector_type: str,
    ) -> DatasetAnalyzer:

        key = connector_type.strip().lower()

        analyzer = self._analyzers.get(key)

        if analyzer is None:
            raise ValueError(
                f"Unsupported connector type: "
                f"{connector_type}"
            )

        return analyzer


def create_default_analyzer_registry() -> DatasetAnalyzerRegistry:
    """
    Create the application's default analyzer registry.

    Connector implementations are registered during bootstrap,
    not selected by the planner.
    """

    registry = DatasetAnalyzerRegistry()

    registry.register(
        "csv",
        CSVDatasetAnalyzer(),
    )

    registry.register(
        "databricks",
        DatabricksDatasetAnalyzer(),
    )

    return registry


_DEFAULT_REGISTRY = (
    create_default_analyzer_registry()
)


def get_dataset_analyzer(
    connector_type: str,
) -> DatasetAnalyzer:

    return _DEFAULT_REGISTRY.get(
        connector_type
    )
