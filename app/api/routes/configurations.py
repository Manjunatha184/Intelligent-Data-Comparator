from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.persistence.config import get_database_url
from app.persistence.models import ConfigurationModel
from app.persistence.repository import PostgresRepository


router = APIRouter(
    prefix="/configurations",
    tags=["Configurations"],
)


repository = PostgresRepository(
    get_database_url()
)

repository.create_tables()


class ConfigurationCreateRequest(BaseModel):
    configuration: dict[str, Any] = Field(
        default_factory=dict
    )
    name: str | None = None
    status: str = "SAVED"


class ConfigurationUpdateRequest(BaseModel):
    configuration: dict[str, Any] | None = None
    name: str | None = None
    status: str | None = None


class ConfigurationResponse(BaseModel):
    configuration_id: int
    name: str
    status: str
    configuration: dict[str, Any]
    created_at: str | None = None
    updated_at: str | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_status(value: str | None) -> str:
    status = str(value or "SAVED").strip().upper()
    if status not in {"DRAFT", "SAVED"}:
        raise ValueError("Configuration status must be DRAFT or SAVED.")
    return status


def _decorate_configuration(
    configuration: dict[str, Any],
    *,
    name: str | None,
    status: str | None,
    preserve_created_at: str | None = None,
) -> dict[str, Any]:
    """Persist lifecycle metadata inside the existing JSONB document.

    Keeping metadata in JSONB makes this backward compatible with existing
    installations because no PostgreSQL ALTER TABLE migration is required.
    """
    payload = dict(configuration or {})
    existing_meta = payload.get("_meta") if isinstance(payload.get("_meta"), dict) else {}

    resolved_name = (name if name is not None else existing_meta.get("name")) or "Untitled comparison"
    resolved_name = str(resolved_name).strip() or "Untitled comparison"

    resolved_status = _normalize_status(
        status if status is not None else existing_meta.get("status", "SAVED")
    )

    created_at = preserve_created_at or existing_meta.get("created_at") or _now_iso()

    payload["_meta"] = {
        **existing_meta,
        "name": resolved_name,
        "status": resolved_status,
        "created_at": created_at,
        "updated_at": _now_iso(),
    }
    return payload


def _response_from_record(record: dict[str, Any]) -> dict[str, Any]:
    configuration = dict(record.get("configuration") or {})
    meta = configuration.get("_meta") if isinstance(configuration.get("_meta"), dict) else {}
    return {
        "configuration_id": record["configuration_id"],
        "name": meta.get("name") or f"Comparison {record['configuration_id']}",
        "status": str(meta.get("status") or "SAVED").upper(),
        "configuration": configuration,
        "created_at": meta.get("created_at") or record.get("created_at"),
        "updated_at": meta.get("updated_at") or record.get("created_at"),
    }


@router.post(
    "",
    response_model=ConfigurationResponse,
)
def create_configuration(
    request: ConfigurationCreateRequest,
):
    try:
        configuration = _decorate_configuration(
            request.configuration,
            name=request.name,
            status=request.status,
        )

        configuration_id = repository.save_configuration(
            configuration
        )

        record = repository.get_configuration(configuration_id)
        if record is None:
            raise ValueError("Configuration was saved but could not be reloaded.")

        return _response_from_record(record)

    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc


@router.get(
    "",
    response_model=list[ConfigurationResponse],
)
def list_configurations(
    status: str | None = Query(default=None),
    search: str | None = Query(default=None),
):
    """List reusable configurations and drafts, newest first."""
    try:
        requested_status = str(status).strip().upper() if status else None
        if requested_status and requested_status not in {"DRAFT", "SAVED"}:
            raise ValueError("Configuration status must be DRAFT or SAVED.")

        needle = str(search or "").strip().lower()

        with Session(repository.engine) as session:
            models = session.scalars(
                select(ConfigurationModel).order_by(ConfigurationModel.created_at.desc())
            ).all()

            records = []
            for model in models:
                configuration = dict(model.configuration or {})
                meta = configuration.get("_meta") if isinstance(configuration.get("_meta"), dict) else {}
                item_status = str(meta.get("status") or "SAVED").upper()
                item_name = meta.get("name") or f"Comparison {model.configuration_id}"

                if requested_status and item_status != requested_status:
                    continue
                if needle and needle not in str(item_name).lower() and needle not in str(model.configuration_id).lower():
                    continue

                records.append(
                    _response_from_record(
                        {
                            "configuration_id": model.configuration_id,
                            "configuration": configuration,
                            "created_at": model.created_at.isoformat() if model.created_at else None,
                        }
                    )
                )

            return records

    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc


@router.get(
    "/{configuration_id}",
    response_model=ConfigurationResponse,
)
def get_configuration(configuration_id: int):
    record = repository.get_configuration(configuration_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Configuration not found.")
    return _response_from_record(record)


@router.put(
    "/{configuration_id}",
    response_model=ConfigurationResponse,
)
def update_configuration(
    configuration_id: int,
    request: ConfigurationUpdateRequest,
):
    try:
        existing = repository.get_configuration(configuration_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Configuration not found.")

        current_configuration = dict(existing.get("configuration") or {})
        current_meta = current_configuration.get("_meta") if isinstance(current_configuration.get("_meta"), dict) else {}

        incoming = request.configuration if request.configuration is not None else current_configuration
        configuration = _decorate_configuration(
            incoming,
            name=request.name if request.name is not None else current_meta.get("name"),
            status=request.status if request.status is not None else current_meta.get("status", "SAVED"),
            preserve_created_at=current_meta.get("created_at") or existing.get("created_at"),
        )

        repository.save_configuration(
            configuration,
            configuration_id=configuration_id,
        )

        updated = repository.get_configuration(configuration_id)
        if updated is None:
            raise ValueError("Configuration was updated but could not be reloaded.")
        return _response_from_record(updated)

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc


@router.delete(
    "/{configuration_id}",
)
def delete_configuration(configuration_id: int):
    """Delete a configuration only when no historical run references it."""
    try:
        with Session(repository.engine) as session:
            model = session.get(ConfigurationModel, configuration_id)
            if model is None:
                raise HTTPException(status_code=404, detail="Configuration not found.")

            try:
                session.delete(model)
                session.commit()
            except Exception as exc:
                session.rollback()
                raise ValueError(
                    "This configuration is referenced by comparison history and cannot be deleted. "
                    "Keep the historical configuration or create a new editable copy."
                ) from exc

        return {"deleted": True, "configuration_id": configuration_id}

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc
