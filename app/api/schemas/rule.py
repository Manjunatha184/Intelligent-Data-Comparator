from typing import Any
from pydantic import BaseModel, Field

class RuleCreate(BaseModel):
    name: str
    rule_type: str
    payload: dict[str, Any]

class RuleResponse(BaseModel):
    rule_id: int
    name: str
    rule_type: str
    payload: dict[str, Any]
    created_at: str

class RuleUpdate(BaseModel):
    name: str
    rule_type: str
    payload: dict[str, Any]
