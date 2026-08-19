from __future__ import annotations

from time import perf_counter
from typing import Any

from app.comparators.spark_levels import SPARK_COMPARATORS
from app.execution.spark_executor import SparkExecutor as LegacySparkExecutor


class SparkExecutor(LegacySparkExecutor):
    """Comparator-dispatching Spark executor.

    Dataset loading, Spark session ownership, caches, reconciliation helpers,
    and the public result contract remain on the proven executor while level
    algorithms are extracted incrementally into app.comparators.
    """

    def execute(self, task) -> dict[str, Any]:
        load_started = perf_counter()
        source = self._load(task.configuration["source"])
        target = self._load(task.configuration["target"])

        level = task.comparison_level
        level_started = perf_counter()
        comparator = SPARK_COMPARATORS.get(level.value)
        if comparator is None:
            raise ValueError(f"Unsupported Spark comparison level: {level}")

        result = comparator.execute(self, source, target, task.configuration)

        # Keep the existing public result envelope and runtime metadata exactly
        # as before; only the level dispatch boundary changes.
        result = self._normalize_contract(level, result)
        result.setdefault("runtime_context", {}).update(
            {
                "engine": "SPARK",
                "spark_master": self.spark.sparkContext.master,
                "spark_app_id": self.spark.sparkContext.applicationId,
                "distributed": True,
                "full_collect_used": False,
                "dataset_loading_ms": (perf_counter() - load_started) * 1000,
                "comparison_ms": (perf_counter() - level_started) * 1000,
            }
        )
        result["execution_location"] = "SPARK"
        return result
