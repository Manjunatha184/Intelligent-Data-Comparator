from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def safe_rate_pct(
    numerator: Any,
    denominator: Any,
    zero_value: float | None = 0.0,
) -> float | None:
    numerator_value = _to_decimal(numerator)
    denominator_value = _to_decimal(denominator)

    if (
        numerator_value is None
        or denominator_value is None
    ):
        return None

    if denominator_value == 0:
        return zero_value

    return _round_pct(
        numerator_value
        / denominator_value
        * Decimal("100")
    )


def safe_percent_change(
    source: Any,
    target: Any,
) -> float | None:
    source_value = _to_decimal(source)
    target_value = _to_decimal(target)

    if (
        source_value is None
        or target_value is None
    ):
        return None

    if source_value == 0:
        if target_value == 0:
            return 0.0
        return None

    return _round_pct(
        (target_value - source_value)
        / source_value
        * Decimal("100")
    )


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _round_pct(value: Decimal) -> float:
    return round(float(value), 2)
