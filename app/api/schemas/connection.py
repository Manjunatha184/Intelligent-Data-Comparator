from typing import Any

from pydantic import BaseModel, Field


class ConnectionCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=150)

    connector_type: str = Field(
        min_length=1,
        max_length=50,
    )

    properties: dict[str, Any] = Field(
        default_factory=dict
    )


class ConnectionResponse(BaseModel):
    connection_id: int

    name: str

    connector_type: str

    status: str

    properties: dict[str, Any] = Field(
        default_factory=dict
    )

    created_at: Any | None = None

    updated_at: Any | None = None


class ConnectionSummaryResponse(BaseModel):
    connection_id: int

    name: str

    connector_type: str

    status: str

    properties: dict[str, Any] = Field(
        default_factory=dict
    )


class ConnectionTestResponse(BaseModel):
    connection_id: int

    name: str

    connector_type: str

    status: str

    message: str

    details: dict[str, Any] = Field(
        default_factory=dict
    )
