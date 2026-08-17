from typing import Any
from typing import Iterator

from app.connectors.base import (
    DatasetSchema,
    MetadataProvider,
    DataProvider,
    ConnectionProvider,
)


class ConnectorManager:

    def __init__(
        self,
        providers: dict[str, MetadataProvider] | None = None,
        data_providers: dict[str, DataProvider] | None = None,
        connection_providers: (
            dict[str, ConnectionProvider] | None
        ) = None,
    ) -> None:

        self._providers = providers or {}

        self._data_providers = (
            data_providers or {}
        )

        self._connection_providers = (
            connection_providers or {}
        )


    # ========================================================
    # CONNECTION PROVIDERS
    # ========================================================

    def register_connection_provider(
        self,
        connector_type: str,
        provider: ConnectionProvider,
    ) -> None:

        key = connector_type.strip().lower()

        if not key:
            raise ValueError(
                "connector_type cannot be empty"
            )

        if key in self._connection_providers:
            raise ValueError(
                f"Connection provider already "
                f"registered: {connector_type}"
            )

        self._connection_providers[key] = provider


    def get_connection_provider(
        self,
        connector_type: str,
    ) -> ConnectionProvider:

        key = connector_type.strip().lower()

        provider = (
            self._connection_providers.get(key)
        )

        if provider is None:
            raise ValueError(
                f"No connection provider registered "
                f"for connector type: "
                f"{connector_type}"
            )

        return provider


    def test_connection(
        self,
        connector_type: str,
        properties: dict[str, Any],
    ) -> dict[str, Any]:

        provider = (
            self.get_connection_provider(
                connector_type
            )
        )

        return provider.test_connection(
            properties
        )

    # ========================================================
    # METADATA PROVIDERS
    # ========================================================

    def register(
        self,
        connector_type: str,
        provider: MetadataProvider,
    ) -> None:

        key = connector_type.strip().lower()

        if not key:
            raise ValueError(
                "connector_type cannot be empty"
            )

        if key in self._providers:
            raise ValueError(
                f"Connector already registered: "
                f"{connector_type}"
            )

        self._providers[key] = provider

    def get_provider(
        self,
        connector_type: str,
    ) -> MetadataProvider:

        key = connector_type.strip().lower()

        provider = self._providers.get(key)

        if provider is None:
            raise ValueError(
                f"No metadata provider registered for "
                f"connector type: {connector_type}"
            )

        return provider

    def get_schema(
        self,
        connector_type: str,
        dataset: dict[str, Any],
    ) -> DatasetSchema:

        provider = self.get_provider(
            connector_type
        )

        return provider.get_schema(dataset)

    def list_catalogs(
        self,
        connector_type: str,
        properties: dict[str, Any],
    ) -> list[str]:
        provider = self.get_provider(connector_type)
        return provider.list_catalogs(properties)

    def list_schemas(
        self,
        connector_type: str,
        properties: dict[str, Any],
        catalog: str | None = None,
    ) -> list[str]:
        provider = self.get_provider(connector_type)
        return provider.list_schemas(properties, catalog=catalog)

    def list_tables(
        self,
        connector_type: str,
        properties: dict[str, Any],
        schema: str | None = None,
        catalog: str | None = None,
    ) -> list[str]:
        provider = self.get_provider(connector_type)
        return provider.list_tables(properties, schema=schema, catalog=catalog)

    # ========================================================
    # DATA PROVIDERS
    # ========================================================

    def register_data_provider(
        self,
        connector_type: str,
        provider: DataProvider,
    ) -> None:

        key = connector_type.strip().lower()

        if not key:
            raise ValueError(
                "connector_type cannot be empty"
            )

        if key in self._data_providers:
            raise ValueError(
                f"Data connector already registered: "
                f"{connector_type}"
            )

        self._data_providers[key] = provider

    def get_data_provider(
        self,
        connector_type: str,
    ) -> DataProvider:

        key = connector_type.strip().lower()

        provider = self._data_providers.get(key)

        if provider is None:
            raise ValueError(
                f"No data provider registered for "
                f"connector type: {connector_type}"
            )

        return provider

    def get_records(
        self,
        connector_type: str,
        dataset: dict[str, Any],
    ) -> list[dict[str, Any]]:

        provider = self.get_data_provider(
            connector_type
        )

        return provider.get_records(dataset)

    def iter_chunks(
        self,
        connector_type: str,
        dataset: dict[str, Any],
        chunk_size: int = 1000,
    ) -> Iterator[list[dict[str, Any]]]:

        provider = self.get_data_provider(
            connector_type
        )

        return provider.iter_chunks(
            dataset,
            chunk_size=chunk_size,
        )

 
    def get_volume_statistics(
        self,
        connector_type: str,
        dataset: dict[str, Any],
        business_keys: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:

        provider = self.get_data_provider(
            connector_type
        )

        if filters is None:
            return provider.get_volume_statistics(
                dataset,
                business_keys=business_keys,
            )
        return provider.get_volume_statistics(
            dataset,
            business_keys=business_keys,
            filters=filters,
        )
