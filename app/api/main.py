from dotenv import load_dotenv

load_dotenv()

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes.comparisons import router as comparison_router
from app.api.routes.connections import router as connection_router 
from app.api.routes.configurations import (
    router as configuration_router,
) 
from app.api.routes.rules import router as rule_router

@asynccontextmanager
async def lifespan(_: FastAPI):
    from app.execution.spark_executor import SparkExecutor

    SparkExecutor().warm_up()
    yield


app = FastAPI(
    title="Enterprise Data Comparator",
    description=(
        "Configuration-driven source-to-target "
        "data comparison framework."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get(
    "/health",
    tags=["System"],
)
def health():
    return {
        "status": "healthy",
        "service": "data-comparator",
    }


app.include_router(
    comparison_router,
    prefix="/api/v1",
)

app.include_router(
    connection_router,
    prefix="/api/v1",
)

app.include_router(
    configuration_router,
    prefix="/api/v1",
)

app.include_router(
    rule_router,
    prefix="/api/v1",
)
