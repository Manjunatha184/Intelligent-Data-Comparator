from __future__ import annotations

import os

from dotenv import load_dotenv


load_dotenv()


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be configured in the environment.")
    return database_url
