from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.analysis.evidence_builder import L7EvidenceBuilder
from app.analysis.groq_analyzer import GroqL7Analyzer
from app.analysis.models import L7Report
from app.analysis.prompts import prompt_references


class L7AnalysisEngine:
    """Build sanitized evidence locally, then use Groq for reasoning."""

    def __init__(self, groq_analyzer: GroqL7Analyzer | None = None) -> None:
        self.evidence_builder = L7EvidenceBuilder()
        self.groq_analyzer = groq_analyzer

    def analyze(self, run_id: str, level_results: dict[str, Any]) -> L7Report:
        evidence = self.evidence_builder.build(level_results)
        self.evidence_builder.assert_privacy_safe(evidence)
        analyzer = self.groq_analyzer or GroqL7Analyzer()
        report = analyzer.analyze(
            evidence=evidence,
            run_id=run_id,
        )
        report.technical_evidence = {
            "sanitized_evidence": evidence,
            "privacy_boundary": evidence["privacy_policy"],
            "prompt_templates": prompt_references(),
        }
        report.generated_at = report.generated_at or datetime.now(timezone.utc).isoformat()
        return report
