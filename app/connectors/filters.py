from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, model_validator


FILTER_OPERATORS = {"=", "!=", ">", ">=", "<", "<=", "IN", "IS NULL", "IS NOT NULL"}


class RowFilter(BaseModel):
    field: str
    operator: str
    value: Any = None

    @model_validator(mode="after")
    def validate_filter(self):
        self.operator = self.operator.upper().strip()
        if not self.field.strip():
            raise ValueError("Filter field is required")
        if self.operator not in FILTER_OPERATORS:
            raise ValueError(f"Unsupported filter operator: {self.operator}")
        if self.operator in {"IS NULL", "IS NOT NULL"}:
            if self.value is not None:
                raise ValueError(f"{self.operator} does not accept a value")
        elif self.value is None:
            raise ValueError(f"{self.operator} requires a value")
        elif self.operator == "IN" and (not isinstance(self.value, list) or not self.value):
            raise ValueError("IN requires a non-empty list of values")
        return self


def filter_records(records, filters: list[dict[str, Any]]):
    parsed = [RowFilter.model_validate(item) for item in filters]
    for record in records:
        if all(matches(record, item) for item in parsed):
            yield record


def matches(record: dict[str, Any], item: RowFilter) -> bool:
    if item.field not in record:
        raise ValueError(f"Unknown filter field: {item.field}")
    actual = _coerce_actual(record[item.field])
    if item.operator == "IS NULL":
        return actual is None
    if item.operator == "IS NOT NULL":
        return actual is not None
    expected_values = item.value if item.operator == "IN" else [item.value]
    coerced = [_coerce_expected(value, actual) for value in expected_values]
    if item.operator == "IN":
        return actual in coerced
    expected = coerced[0]
    if item.operator == "=": return actual == expected
    if item.operator == "!=": return actual != expected
    if actual is None: return False
    if item.operator == ">": return actual > expected
    if item.operator == ">=": return actual >= expected
    if item.operator == "<": return actual < expected
    return actual <= expected


def _coerce_actual(value: Any) -> Any:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, str):
        text = value.strip()
        try: return int(text)
        except ValueError: pass
        try: return Decimal(text)
        except InvalidOperation: pass
        try: return datetime.fromisoformat(text)
        except ValueError: return text
    return value


def _coerce_expected(value: Any, actual: Any) -> Any:
    if actual is None: return value
    if isinstance(actual, (int, float, Decimal)) and not isinstance(actual, bool):
        try: return type(actual)(value)
        except (TypeError, ValueError, InvalidOperation) as exc:
            raise ValueError(f"Invalid numeric filter value: {value}") from exc
    if isinstance(actual, datetime):
        try: return datetime.fromisoformat(str(value))
        except ValueError as exc: raise ValueError(f"Invalid datetime filter value: {value}") from exc
    if isinstance(actual, date):
        try: return date.fromisoformat(str(value))
        except ValueError as exc: raise ValueError(f"Invalid date filter value: {value}") from exc
    if isinstance(actual, bool):
        if isinstance(value, bool): return value
        if str(value).lower() in {"true", "1"}: return True
        if str(value).lower() in {"false", "0"}: return False
        raise ValueError(f"Invalid boolean filter value: {value}")
    return str(value)
