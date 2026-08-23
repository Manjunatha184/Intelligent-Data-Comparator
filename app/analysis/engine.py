from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.analysis.evidence_builder import L7EvidenceBuilder
from app.analysis.groq_analyzer import GroqL7Analyzer
from app.analysis.models import L7Report
from app.analysis.prompts import prompt_references


# L7 is explanatory, while L1-L6 are deterministic validations. Re-running the
# same comparison should therefore not produce a differently worded report just
# because the LLM was called again. Cache the normalized narrative by the exact
# sanitized evidence payload for the lifetime of the backend process.
_L7_REPORT_CACHE: dict[str, L7Report] = {}


def _evidence_fingerprint(evidence: dict[str, Any]) -> str:
    payload = json.dumps(
        evidence,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class L7AnalysisEngine:
    """Build sanitized evidence locally, then use Groq for reasoning."""

    def __init__(self, groq_analyzer: GroqL7Analyzer | None = None) -> None:
        self.evidence_builder = L7EvidenceBuilder()
        self.groq_analyzer = groq_analyzer

    def analyze(self, run_id: str, level_results: dict[str, Any]) -> L7Report:
        evidence = self.evidence_builder.build(level_results)
        self.evidence_builder.assert_privacy_safe(evidence)

        fingerprint = _evidence_fingerprint(evidence)
        cached = _L7_REPORT_CACHE.get(fingerprint)

        if cached is not None:
            report = cached.model_copy(deep=True)
            report.run_id = run_id
            report.generated_at = datetime.now(timezone.utc).isoformat()
        else:
            analyzer = self.groq_analyzer or GroqL7Analyzer()
            report = analyzer.analyze(
                evidence=evidence,
                run_id=run_id,
            )
            # Cache only the normalized report narrative. A deep copy prevents a
            # later run from mutating the cached version when run metadata changes.
            _L7_REPORT_CACHE[fingerprint] = report.model_copy(deep=True)

        report.technical_evidence = {
            "sanitized_evidence": evidence,
            "privacy_boundary": evidence["privacy_policy"],
            "prompt_templates": prompt_references(),
        }
        report.generated_at = report.generated_at or datetime.now(timezone.utc).isoformat()
        return report
