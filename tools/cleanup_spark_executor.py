from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
executor_path = ROOT / "app/execution/spark_executor.py"
dispatcher_path = ROOT / "app/execution/dispatcher.py"
refactored_path = ROOT / "app/execution/spark_executor_refactored.py"

text = executor_path.read_text(encoding="utf-8")

if "from app.comparators.spark_levels import SPARK_COMPARATORS" not in text:
    marker = "from app.execution.models import ComparisonLevel, ExecutionTask\n"
    if marker not in text:
        raise SystemExit("executor import marker not found")
    text = text.replace(
        marker,
        marker + "from app.comparators.spark_levels import SPARK_COMPARATORS\n",
        1,
    )

# Parse the current source and remove only the duplicated level algorithms.
tree = ast.parse(text)
executor_class = next(
    node for node in tree.body
    if isinstance(node, ast.ClassDef) and node.name == "SparkExecutor"
)

remove_names = {"_l1", "_l2", "_l3", "_l4", "_l5", "_l6"}
remove_nodes = [
    node for node in executor_class.body
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    and node.name in remove_names
]
found = {node.name for node in remove_nodes}
if found != remove_names:
    raise SystemExit(f"Expected L1-L6 methods; found {sorted(found)}")

execute_node = next(
    node for node in executor_class.body
    if isinstance(node, ast.FunctionDef) and node.name == "execute"
)

lines = text.splitlines(keepends=True)

new_execute = '''    def execute(self, task: ExecutionTask) -> dict[str, Any]:
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
'''.splitlines(keepends=True)

# Replace execute and delete L1-L6 from bottom to top so line numbers remain valid.
operations = [(execute_node.lineno, execute_node.end_lineno, new_execute)]
operations += [(node.lineno, node.end_lineno, []) for node in remove_nodes]
for start, end, replacement in sorted(operations, reverse=True):
    lines[start - 1:end] = replacement

text = "".join(lines)

# Remove imports that existed only for the old embedded L5/L6 algorithms.
text = text.replace("from app.comparators.aggregate import AggregateComparator\n", "")
text = text.replace("from app.comparators.dq import DQComparator\n", "")

# Safety checks: no duplicate level implementation may remain.
parsed = ast.parse(text)
executor_class = next(
    node for node in parsed.body
    if isinstance(node, ast.ClassDef) and node.name == "SparkExecutor"
)
remaining_methods = {
    node.name for node in executor_class.body
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
}
if remove_names & remaining_methods:
    raise SystemExit("Duplicated L1-L6 methods still remain")
if "execute" not in remaining_methods:
    raise SystemExit("SparkExecutor.execute was lost")

executor_path.write_text(text, encoding="utf-8")

# Dispatcher now imports the one real executor directly.
dispatcher = dispatcher_path.read_text(encoding="utf-8")
old = "from app.execution.spark_executor_refactored import SparkExecutor"
new = "from app.execution.spark_executor import SparkExecutor"
if old not in dispatcher:
    raise SystemExit("dispatcher refactored executor import not found")
dispatcher_path.write_text(dispatcher.replace(old, new, 1), encoding="utf-8")

# The wrapper is no longer part of the architecture.
if refactored_path.exists():
    refactored_path.unlink()

print("Spark executor cleanup complete")
print("Removed embedded methods:", ", ".join(sorted(remove_names)))
print("Dispatcher now uses app.execution.spark_executor.SparkExecutor")
