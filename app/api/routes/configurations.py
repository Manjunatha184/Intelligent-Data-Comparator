from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel, Field

from app.persistence.repository import PostgresRepository
from app.persistence.config import get_database_url


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


class ConfigurationResponse(BaseModel):
    configuration_id: int
    configuration: dict[str, Any]


@router.post(
    "",
    response_model=ConfigurationResponse,
)
def create_configuration(
    request: ConfigurationCreateRequest,
):

    try:

        configuration_id = (
            repository.save_configuration(
                request.configuration
            )
        )

        return {
            "configuration_id": configuration_id,
            "configuration": request.configuration,
        }

    except Exception as exc:

        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc
