from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_PROMPT_DIRECTORY = Path(__file__).with_name("prompts")
SYSTEM_PROMPT_REFERENCE = "app/analysis/prompts/l7_system.txt"
REPORT_REQUEST_REFERENCE = "app/analysis/prompts/l7_report_request.txt"


def load_system_prompt() -> str:
    return _load("l7_system.txt")


def render_report_request(evidence: dict[str, Any], run_id: str) -> str:
    template = _load("l7_report_request.txt")
    return (
        template.replace("{{RUN_ID}}", run_id)
        .replace(
            "{{SANITIZED_EVIDENCE_JSON}}",
            json.dumps(evidence, indent=2, default=str),
        )
    )


def prompt_references() -> dict[str, str]:
    """Stable source references exposed with each generated L7 report."""
    return {
        "system": SYSTEM_PROMPT_REFERENCE,
        "report_request": REPORT_REQUEST_REFERENCE,
    }


def _load(filename: str) -> str:
    content = (_PROMPT_DIRECTORY / filename).read_text(encoding="utf-8").strip()
    if not content:
        raise RuntimeError(f"L7 prompt template is empty: {filename}")
    return content
