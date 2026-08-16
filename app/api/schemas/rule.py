from typing import Any
from pydantic import BaseModel, model_validator

from app.domain.context import DQRule

class RuleWrite(BaseModel):
    name: str
    rule_type: str
    payload: dict[str, Any]

    @model_validator(mode="after")
    def validate_payload(self):
        self.rule_type = self.rule_type.upper()
        if self.rule_type == "DQ":
            self.payload = DQRule.model_validate(self.payload).model_dump()
        return self


class RuleCreate(RuleWrite):
    pass

class RuleResponse(BaseModel):
    rule_id: int
    name: str
    rule_type: str
    payload: dict[str, Any]
    created_at: str

class RuleUpdate(RuleWrite):
    pass
