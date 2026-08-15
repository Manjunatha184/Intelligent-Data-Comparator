from fastapi import APIRouter, HTTPException

from app.api.schemas.rule import RuleCreate, RuleResponse, RuleUpdate
from app.persistence.repository import PostgresRepository
from app.persistence.config import get_database_url

repository = PostgresRepository(get_database_url())
repository.create_tables()

router = APIRouter(tags=["Rules"])

@router.post("/rules", response_model=RuleResponse)
def create_rule(request: RuleCreate):
    try:
        rule_id = repository.save_rule(
            name=request.name,
            rule_type=request.rule_type,
            payload=request.payload,
        )
        # Fetch the created rule to return the full response
        rules = repository.get_rules()
        for r in rules:
            if r["rule_id"] == rule_id:
                return r
        raise HTTPException(status_code=500, detail="Failed to retrieve created rule")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@router.get("/rules", response_model=list[RuleResponse])
def get_rules():
    try:
        rules = repository.get_rules()
        return rules
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@router.put("/rules/{rule_id}", response_model=RuleResponse)
def update_rule(rule_id: int, request: RuleUpdate):
    try:
        repository.update_rule(
            rule_id=rule_id,
            name=request.name,
            rule_type=request.rule_type,
            payload=request.payload,
        )
        rules = repository.get_rules()
        for r in rules:
            if r["rule_id"] == rule_id:
                return r
        raise HTTPException(status_code=404, detail="Rule not found")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@router.delete("/rules/{rule_id}")
def delete_rule(rule_id: int):
    try:
        repository.delete_rule(rule_id)
        return {"status": "DELETED"}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
