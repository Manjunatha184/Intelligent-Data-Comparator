"""Spark comparison level registry.

L1-L6 are physically extracted into dedicated Spark comparator modules.
SparkExecutor remains the shared host for loading, reconciliation caches,
normalization helpers, bounded evidence, and result-contract normalization.
"""

from app.comparators.field import SparkFieldComparator
from app.comparators.record import SparkRecordComparator
from app.comparators.schema import SparkSchemaComparator
from app.comparators.spark_aggregate import SparkAggregateComparator
from app.comparators.spark_dq import SparkDQComparator
from app.comparators.volume import SparkVolumeComparator


SPARK_COMPARATORS = {
    "L1": SparkSchemaComparator(),
    "L2": SparkVolumeComparator(),
    "L3": SparkRecordComparator(),
    "L4": SparkFieldComparator(),
    "L5": SparkAggregateComparator(),
    "L6": SparkDQComparator(),
}
