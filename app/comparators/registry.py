from typing import Any

from app.comparators.aggregate import AggregateComparator
from app.comparators.dq import DQComparator
from app.comparators.field import FieldComparator
from app.comparators.record import RecordComparator
from app.comparators.schema import SchemaComparator
from app.comparators.volume import VolumeComparator

from app.connectors.csv import CSVMetadataProvider


class ComparatorRegistry:
    """
    Central registry for all comparison implementations.

    The registry owns comparator construction.

    The dispatcher only asks the registry for a comparator
    by name and does not know how comparators are constructed.
    """

    def __init__(
        self,
        schema_providers: dict[str, Any] | None = None,
    ) -> None:

        providers = (
            dict(schema_providers)
            if schema_providers is not None
            else {}
        )

        # Register the currently supported connector.
        #
        # Future connectors can be added here through their
        # own MetadataProvider implementations or injected
        # through schema_providers.
        if "csv" not in providers:
            providers["csv"] = (
                CSVMetadataProvider()
            )

        self._comparators: dict[
            str,
            Any,
        ] = {}

        self.register(
            "SchemaComparator",
            SchemaComparator(
                schema_providers=providers
            ),
        )

        self.register(
            "VolumeComparator",
            VolumeComparator(),
        )

        self.register(
            "RecordComparator",
            RecordComparator(),
        )

        self.register(
            "FieldComparator",
            FieldComparator(),
        )

        self.register(
            "AggregateComparator",
            AggregateComparator(),
        )

        self.register(
            "DQComparator",
            DQComparator(),
        )

    # ========================================================
    # REGISTRATION
    # ========================================================

    def register(
        self,
        name: str,
        comparator: Any,
    ) -> None:

        key = name.strip()

        if not key:
            raise ValueError(
                "Comparator name cannot be empty."
            )

        if key in self._comparators:
            raise ValueError(
                f"Comparator already registered: {key}"
            )

        self._comparators[key] = comparator

    # ========================================================
    # RESOLUTION
    # ========================================================

    def get(
        self,
        name: str,
    ) -> Any:

        comparator = self._comparators.get(
            name
        )

        if comparator is None:
            raise ValueError(
                f"Comparator not registered: {name}"
            )

        return comparator

    # ========================================================
    # INSPECTION
    # ========================================================

    def as_dict(
        self,
    ) -> dict[str, Any]:

        return dict(
            self._comparators
        )
