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


def _number(*values: Any) -> float | None:
    for value in values:
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return float(value)
        try:
            if value is not None and value != "":
                return float(value)
        except (TypeError, ValueError):
            pass
    return None


def _comparison_percentages(evidence: dict[str, Any]) -> tuple[float | None, float | None]:
    """Return validation-level score and a data-population match percentage.

    The validation score is intentionally level based: passed executed L1-L6
    validations divided by executed validations.

    Data Match is intentionally NOT level based. It measures the actual data:
    record coverage from L3 and, where raw comparison counts are available,
    field-value conformity from L4. Independent record and field defects are
    combined multiplicatively so either kind of data difference lowers the
    score. A tiny mismatch in a large dataset therefore remains very close to
    100%, but is not represented as a perfect match.
    """
    levels = evidence.get("levels", {})
    statuses = [
        str(value.get("status", "")).upper()
        for value in levels.values()
        if isinstance(value, dict)
        and str(value.get("status", "")).upper()
        not in {"", "NOT_APPLICABLE", "NOT RUN", "UNKNOWN"}
    ]
    validation_percentage = None
    if statuses:
        validation_percentage = round(
            (sum(status == "PASS" for status in statuses) / len(statuses)) * 100,
            2,
        )

    l3_metrics = (
        levels.get("L3", {}).get("metrics", {})
        if isinstance(levels.get("L3"), dict)
        else {}
    )
    source_records = _number(
        l3_metrics.get("source_record_count"),
        l3_metrics.get("total_rows_source"),
    )
    target_records = _number(
        l3_metrics.get("target_record_count"),
        l3_metrics.get("total_rows_target"),
    )
    matched_records = _number(
        l3_metrics.get("matched_record_count"),
        l3_metrics.get("primary_matched_record_count"),
        l3_metrics.get("matched_key_count"),
        l3_metrics.get("primary_matched_count"),
    )

    record_ratio = None
    if (
        source_records is not None
        and source_records >= 0
        and target_records is not None
        and target_records >= 0
        and matched_records is not None
        and matched_records >= 0
    ):
        population = max(source_records, target_records)
        if population > 0:
            record_ratio = max(0.0, min(1.0, matched_records / population))

    l4_metrics = (
        levels.get("L4", {}).get("metrics", {})
        if isinstance(levels.get("L4"), dict)
        else {}
    )
    matched_fields = _number(l4_metrics.get("matched_field_count"))
    mismatched_fields = _number(
        l4_metrics.get("mismatch_count"),
        l4_metrics.get("field_mismatch_count"),
    )
    compared_fields = _number(
        l4_metrics.get("compared_field_values"),
        l4_metrics.get("compared_field_count"),
        l4_metrics.get("total_field_comparisons"),
    )

    field_ratio = None
    if matched_fields is not None and matched_fields >= 0 and mismatched_fields is not None and mismatched_fields >= 0:
        total = matched_fields + mismatched_fields
        if total > 0:
            field_ratio = max(0.0, min(1.0, matched_fields / total))
    elif compared_fields is not None and compared_fields > 0 and mismatched_fields is not None and mismatched_fields >= 0:
        field_ratio = max(0.0, min(1.0, (compared_fields - mismatched_fields) / compared_fields))

    if record_ratio is not None and field_ratio is not None:
        data_match_percentage = record_ratio * field_ratio * 100
    elif field_ratio is not None:
        data_match_percentage = field_ratio * 100
    elif record_ratio is not None:
        data_match_percentage = record_ratio * 100
    else:
        data_match_percentage = None

    return (
        validation_percentage,
        round(data_match_percentage, 6) if data_match_percentage is not None else None,
    )


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

        validation_percentage, data_match_percentage = _comparison_percentages(evidence)
        report.overall_validation_percentage = validation_percentage
        report.overall_data_match_percentage = data_match_percentage
        report.technical_evidence = {
            "sanitized_evidence": evidence,
            "privacy_boundary": evidence["privacy_policy"],
            "prompt_templates": prompt_references(),
        }
        report.generated_at = report.generated_at or datetime.now(timezone.utc).isoformat()
        return report
