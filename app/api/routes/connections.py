from pathlib import Path
from uuid import uuid4
import shutil
from typing import Any
import logging
import re

from fastapi import APIRouter, HTTPException, UploadFile, File, UploadFile, File
from pydantic import BaseModel, Field

from app.api.schemas.connection import (
    ConnectionCreateRequest,
    ConnectionResponse,
    ConnectionSummaryResponse,
    ConnectionTestResponse,
)

class SchemaRequest(BaseModel):
    connector_type: str = Field(min_length=1, max_length=50)
    properties: dict[str, Any] = Field(default_factory=dict)


logger = logging.getLogger(__name__)


def _safe_databricks_error(error: Exception) -> str:
    message = str(error)
    message = re.sub(r"(?i)(access[_ -]?token|password|secret)\s*[:=]\s*[^,\s]+", r"\1=[REDACTED]", message)
    return message[:500]


def _sanitize_databricks_value(value: Any, key: str = "") -> Any:
    if isinstance(value, dict):
        return {
            child_key: (
                "[REDACTED]"
                if re.search(r"token|secret|password|credential", child_key, re.I)
                else _sanitize_databricks_value(child_value, child_key)
            )
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_databricks_value(item, key) for item in value]
    return value

from app.connectors.manager import ConnectorManager
from app.connectors.csv import CSVMetadataProvider
from app.connectors.databricks import DatabricksConnector

from app.persistence.repository import PostgresRepository
from app.persistence.config import get_database_url


router = APIRouter(
    prefix="/connections",
    tags=["Connections"],
)


# ============================================================
# CONNECTOR MANAGER
# ============================================================

csv_provider = CSVMetadataProvider()
databricks_provider = DatabricksConnector()

connector_manager = ConnectorManager()


connector_manager.register(
    "csv",
    csv_provider,
)

connector_manager.register_data_provider(
    "csv",
    csv_provider,
)

connector_manager.register_connection_provider(
    "csv",
    csv_provider,
)


connector_manager.register(
    "databricks",
    databricks_provider,
)

connector_manager.register_data_provider(
    "databricks",
    databricks_provider,
)

connector_manager.register_connection_provider(
    "databricks",
    databricks_provider,
)



# ============================================================
# PERSISTENCE
# ============================================================

repository = PostgresRepository(
    get_database_url()
)

repository.create_tables()


# ============================================================
# SECRET MASKING
# ============================================================

SECRET_FIELDS = {
    "password",
    "access_token",
    "token",
    "api_key",
    "secret",
    "client_secret",
}


def _sanitize_properties(
    properties: dict[str, Any],
) -> dict[str, Any]:

    sanitized = {}

    for key, value in properties.items():

        if key.lower() in SECRET_FIELDS:

            sanitized[key] = "********"

        else:

            sanitized[key] = value

    return sanitized


def _resolve_databricks_request_properties(
    connector_type: str,
    properties: dict[str, Any],
) -> dict[str, Any]:
    resolved = dict(properties)
    if connector_type.strip().lower() != "databricks":
        return resolved

    connection_id = resolved.get("connection_id")
    if connection_id is None:
        return resolved

    saved = repository.get_connection(int(connection_id))
    if saved is None:
        raise ValueError("Databricks connection was not found")

    dataset_identity = {
        key: resolved[key]
        for key in ("connection_id", "catalog", "schema", "table")
        if key in resolved
    }
    return {
        **saved.get("properties", {}),
        **dataset_identity,
    }

# ============================================================
# CSV FILE UPLOAD
# ============================================================

UPLOAD_ROOT = Path("/app/data/uploads")


@router.post("/upload-csv")
def upload_csv(
    file: UploadFile = File(...),
):
    filename = Path(
        file.filename or ""
    ).name

    if not filename:
        raise HTTPException(
            status_code=400,
            detail="CSV filename is required",
        )

    if not filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=400,
            detail="Only CSV files are supported",
        )

    upload_id = str(uuid4())

    destination_dir = (
        UPLOAD_ROOT / upload_id
    )

    destination_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    destination = (
        destination_dir / filename
    )

    try:

        with destination.open("wb") as output:
            shutil.copyfileobj(
                file.file,
                output,
            )

        size = destination.stat().st_size

        if size == 0:
            destination.unlink(
                missing_ok=True
            )

            raise HTTPException(
                status_code=400,
                detail="Uploaded CSV is empty",
            )

        return {
            "upload_id": upload_id,
            "filename": filename,
            "path": str(destination),
            "size": size,
        }

    except HTTPException:
        raise

    except Exception as exc:

        if destination.exists():
            destination.unlink(
                missing_ok=True
            )

        raise HTTPException(
            status_code=500,
            detail=(
                "Unable to store uploaded CSV"
            ),
        ) from exc

    finally:

        try:
            file.file.close()
        except Exception:
            pass


# ============================================================
# CREATE CONNECTION
# ============================================================

@router.post(
    "",
    response_model=ConnectionResponse,
)
def create_connection(
    request: ConnectionCreateRequest,
):

    connector_type = (
        request.connector_type
        .strip()
        .lower()
    )

    try:

        # ----------------------------------------------------
        # Verify that the connector exists
        # ----------------------------------------------------

        connector_manager.get_connection_provider(
            connector_type
        )

        # ----------------------------------------------------
        # Test before saving
        # ----------------------------------------------------

        test_result = (
            connector_manager.test_connection(
                connector_type,
                request.properties,
            )
        )

        connection_id = (
            repository.save_connection(
                name=request.name,
                connector_type=connector_type,
                properties=request.properties,
                status="CONNECTED",
            )
        )

        connection = repository.get_connection(
            connection_id
        )

        if connection is None:
            raise RuntimeError(
                "Connection was created but could "
                "not be retrieved."
            )

        connection["properties"] = (
            _sanitize_properties(
                connection["properties"]
            )
        )

        return connection

    except Exception as exc:

        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc


# ============================================================
# GET SCHEMA
# ============================================================

@router.post(
    "/schema",
)
def get_dataset_schema(
    request: SchemaRequest,
):
    try:
        properties = _resolve_databricks_request_properties(
            request.connector_type,
            request.properties,
        )
        dataset = {"properties": properties}

        schema = connector_manager.get_schema(
            request.connector_type,
            dataset,
        )
        
        import dataclasses
        return dataclasses.asdict(schema)
        
    except Exception as exc:
        if request.connector_type.strip().lower() == "databricks":
            properties = request.properties
            logger.error(
                "Databricks schema lookup failed for %s.%s.%s: %s: %s",
                properties.get("catalog", "<missing>"),
                properties.get("schema", "<missing>"),
                properties.get("table", "<missing>"),
                type(exc).__name__,
                _safe_databricks_error(exc),
            )
            raise HTTPException(
                status_code=400,
                detail="Unable to read schema for selected Databricks table",
            ) from exc
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc


# ============================================================
# METADATA DISCOVERY
# ============================================================

class DiscoveryRequest(BaseModel):
    connector_type: str = Field(min_length=1, max_length=50)
    properties: dict[str, Any] = Field(default_factory=dict)
    catalog: str | None = None
    schema_name: str | None = None

@router.post(
    "/discover/catalogs",
)
def discover_catalogs(
    request: DiscoveryRequest,
):
    try:
        properties = _resolve_databricks_request_properties(
            request.connector_type,
            request.properties,
        )
        return connector_manager.list_catalogs(
            connector_type=request.connector_type,
            properties=properties,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

@router.post(
    "/discover/schemas",
)
def discover_schemas(
    request: DiscoveryRequest,
):
    try:
        properties = _resolve_databricks_request_properties(
            request.connector_type,
            request.properties,
        )
        return connector_manager.list_schemas(
            connector_type=request.connector_type,
            properties=properties,
            catalog=request.catalog,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

@router.post(
    "/discover/tables",
)
def discover_tables(
    request: DiscoveryRequest,
):
    try:
        properties = _resolve_databricks_request_properties(
            request.connector_type,
            request.properties,
        )
        return connector_manager.list_tables(
            connector_type=request.connector_type,
            properties=properties,
            catalog=request.catalog,
            schema=request.schema_name,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ============================================================
# LIST CONNECTIONS
# ============================================================

@router.get(
    "",
    response_model=list[
        ConnectionSummaryResponse
    ],
)
def list_connections():

    connections = (
        repository.get_connections()
    )

    return [
        {
            "connection_id": (
                connection["connection_id"]
            ),
            "name": connection["name"],
            "connector_type": (
                connection["connector_type"]
            ),
            "status": connection["status"],
            "properties": _sanitize_properties(
                connection.get("properties", {})
            ),
        }
        for connection in connections
    ]


# ============================================================
# GET CONNECTION
# ============================================================

@router.get(
    "/{connection_id}",
    response_model=ConnectionResponse,
)
def get_connection(
    connection_id: int,
):

    connection = repository.get_connection(
        connection_id
    )

    if connection is None:

        raise HTTPException(
            status_code=404,
            detail=(
                f"Connection not found: "
                f"{connection_id}"
            ),
        )

    connection["properties"] = (
        _sanitize_properties(
            connection["properties"]
        )
    )

    return connection


# ============================================================
# TEST CONNECTION
# ============================================================

@router.post(
    "/{connection_id}/test",
    response_model=ConnectionTestResponse,
)
def test_connection(
    connection_id: int,
):

    connection = repository.get_connection(
        connection_id
    )

    if connection is None:

        raise HTTPException(
            status_code=404,
            detail=(
                f"Connection not found: "
                f"{connection_id}"
            ),
        )

    try:

        result = (
            connector_manager.test_connection(
                connection[
                    "connector_type"
                ],
                connection[
                    "properties"
                ],
            )
        )

        repository.update_connection_status(
            connection_id,
            "CONNECTED",
        )

        return {
            "connection_id": connection_id,
            "name": connection["name"],
            "connector_type": (
                connection[
                    "connector_type"
                ]
            ),
            "status": "CONNECTED",
            "message": result.get(
                "message",
                "Connection successful.",
            ),
            "details": {
                key: value
                for key, value
                in result.items()
                if key not in {
                    "status",
                    "message",
                }
            },
        }

    except Exception as exc:

        repository.update_connection_status(
            connection_id,
            "FAILED",
        )

        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc


# ============================================================
# DELETE CONNECTION
# ============================================================

@router.delete(
    "/{connection_id}",
)
def delete_connection(
    connection_id: int,
):

    try:

        repository.delete_connection(
            connection_id
        )

        return {
            "connection_id": connection_id,
            "status": "DELETED",
        }

    except ValueError as exc:

        raise HTTPException(
            status_code=404,
            detail=str(exc),
        ) from exc
