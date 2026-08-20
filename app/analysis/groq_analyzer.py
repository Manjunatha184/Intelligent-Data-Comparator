from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from groq import Groq

from app.analysis.evidence_builder import L7EvidenceBuilder
from app.analysis.models import L7Report
from app.analysis.prompts import (
    load_system_prompt,
    prompt_references,
    render_report_request,
)


class GroqL7Analyzer:

    def __init__(self) -> None:
        api_key = os.getenv("GROQ_API_KEY")

        if not api_key:
            raise RuntimeError(
                "GROQ_API_KEY is not configured"
            )

        self.client = Groq(
            api_key=api_key
        )

        self.model = os.getenv(
            "GROQ_MODEL",
            "openai/gpt-oss-120b",
        )

    # ==========================================================
    # MAIN
    # ==========================================================

    def analyze(
        self,
        evidence: dict[str, Any],
        run_id: str,
    ) -> L7Report:
        L7EvidenceBuilder.assert_privacy_safe(evidence)

        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0.1,
            response_format={
                "type": "json_object"
            },
            messages=[
                {
                    "role": "system",
                    "content": self._system_prompt(),
                },
                {
                    "role": "user",
                    "content": self._user_prompt(
                        evidence,
                        run_id,
                    ),
                },
            ],
        )

        content = (
            response.choices[0]
            .message
            .content
        )

        if not content:
            raise RuntimeError(
                "Groq returned an empty response"
            )

        try:
            raw = json.loads(content)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Groq returned invalid JSON: {exc}"
            ) from exc

        # ------------------------------------------------------
        # IMPORTANT:
        #
        # Never trust the LLM's exact JSON structure.
        # Normalize it before Pydantic validation.
        # ------------------------------------------------------

        normalized = self._normalize_report(
            raw,
            run_id,
            evidence,
        )

        return L7Report.model_validate(
            normalized
        )

    # ==========================================================
    # PROMPTS
    # ==========================================================

    def _system_prompt(self) -> str:
        return load_system_prompt()

    def _user_prompt(
        self,
        evidence: dict[str, Any],
        run_id: str,
    ) -> str:

        return render_report_request(evidence, run_id)

    # ==========================================================
    # NORMALIZATION
    # ==========================================================

    def _normalize_report(
        self,
        raw: dict[str, Any],
        run_id: str,
        evidence: dict[str, Any],
    ) -> dict[str, Any]:

        normalized = {}

        normalized["report_version"] = "2.0"

        normalized["run_id"] = run_id

        normalized["generated_at"] = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        normalized["overall_status"] = (
            self._overall_status(
                raw,
                evidence,
            )
        )

        normalized["severity"] = self._severity_from_evidence(evidence)

        normalized[
            "executive_summary"
        ] = self._string_value(
            raw.get(
                "executive_summary"
            ),
            "Comparison analysis completed.",
        )

        normalized[
            "overall_assessment"
        ] = self._string_value(
            raw.get(
                "overall_assessment"
            ),
            "Analysis is based on available validation evidence.",
        )

        normalized["validation_summary"] = self._build_validation_from_evidence(evidence)

        normalized[
            "key_findings"
        ] = self._normalize_findings(
            raw.get(
                "key_findings"
            ),
            evidence,
        )

        normalized[
            "cross_level_analysis"
        ] = self._normalize_correlations(
            raw.get("cross_level_analysis"),
            evidence,
        )

        # These speculative sections are intentionally excluded from the
        # enterprise report. Keep empty compatibility fields for older clients.
        normalized["root_cause_analysis"] = []
        normalized["recommendations"] = []

        normalized[
            "technical_evidence"
        ] = evidence

        normalized[
            "ai_metadata"
        ] = {
            "provider": "groq",
            "model": self.model,
            "llm_used": True,
            "prompt_templates": prompt_references(),
        }

        return normalized

    # ==========================================================
    # VALIDATION SUMMARY
    # ==========================================================

    def _normalize_validation_summary(
        self,
        value: Any,
        evidence: dict[str, Any],
    ) -> list[dict[str, Any]]:

        if isinstance(
            value,
            list,
        ):
            result = []

            for item in value:

                if isinstance(
                    item,
                    dict,
                ):
                    result.append(
                        item
                    )

            return result

        if isinstance(
            value,
            dict,
        ):

            result = []

            for level, description in (
                value.items()
            ):

                result.append(
                    {
                        "level": str(
                            level
                        ),
                        "name": self._level_name(
                            str(level)
                        ),
                        "status": self._infer_status(
                            str(
                                description
                            )
                        ),
                        "summary": str(
                            description
                        ),
                    }
                )

            return result

        return self._build_validation_from_evidence(
            evidence
        )

    # ==========================================================
    # FINDINGS
    # ==========================================================

    def _normalize_findings(
        self,
        value: Any,
        evidence: dict[str, Any],
    ) -> list[dict[str, Any]]:

        if not isinstance(value, list):
            return self._fallback_findings_from_evidence(evidence)

        result = []

        def extract_strings(v: Any) -> list[str]:
            if isinstance(v, list):
                return [str(i) for i in v if i]
            if isinstance(v, str) and v:
                return [v]
            return []

        for index, finding in enumerate(value, start=1):
            if not isinstance(finding, dict):
                continue

            observed = extract_strings(finding.get("observed_evidence"))
            derived = extract_strings(finding.get("derived_statistics", finding.get("calculations")))
            
            explanations = extract_strings(finding.get("likely_explanation", finding.get("likely_explanations")))
            likely_explanation = explanations[0] if explanations else None

            result.append(
                {
                    "finding_id": finding.get("finding_id", f"F-{index:03d}"),
                    "title": self._string_value(finding.get("title"), f"Finding {index}"),
                    "category": self._string_value(finding.get("category"), "COMPARISON"),
                    "severity": self._normalize_severity(finding.get("severity", "MEDIUM")),
                    "observed_evidence": observed,
                    "derived_statistics": derived,
                    "likely_explanation": likely_explanation,
                    "impact": self._string_value(finding.get("impact"), "Potential comparison impact requires investigation."),
                    "recommended_actions": [],
                    "related_levels": self._normalize_levels(finding.get("related_levels"))
                }
            )

        return result or self._fallback_findings_from_evidence(evidence)

    # ==========================================================
    # CORRELATIONS
    # ==========================================================

    def _normalize_correlations(
        self,
        value: Any,
        evidence: dict[str, Any],
    ) -> list[dict[str, Any]]:

        if not isinstance(
            value,
            list,
        ):
            return self._fallback_correlations_from_evidence(evidence)

        result = []

        for index, item in enumerate(
            value,
            start=1,
        ):

            if not isinstance(
                item,
                dict,
            ):
                continue

            result.append(
                {
                    "correlation_id": item.get(
                        "correlation_id",
                        f"C-{index:03d}",
                    ),
                    "title": self._string_value(
                        item.get(
                            "title"
                        ),
                        f"Cross-level correlation {index}",
                    ),
                    "levels": self._normalize_levels(
                        item.get(
                            "levels"
                        )
                    ),
                    "evidence": self._correlation_evidence(item.get("evidence")),
                    "conclusion": self._string_value(
                        item.get(
                            "conclusion"
                        ),
                        "Evidence indicates a relationship between validation levels.",
                    ),
                }
            )

        return result or self._fallback_correlations_from_evidence(evidence)

    def _fallback_findings_from_evidence(
        self,
        evidence: dict[str, Any],
    ) -> list[dict[str, Any]]:
        validation_summary = {
            item["level"]: item
            for item in self._build_validation_from_evidence(evidence)
            if item.get("level")
        }
        severity = self._severity_from_evidence(evidence)
        findings = []

        for index, level in enumerate(("L1", "L2", "L3", "L4", "L5", "L6"), start=1):
            item = evidence.get("levels", {}).get(level)
            if not isinstance(item, dict):
                continue
            if str(item.get("status", "")).upper() != "FAIL":
                continue

            metrics = item.get("metrics", {})
            derived_statistics = [
                f"{key.replace('_', ' ')}: {value}"
                for key, value in metrics.items()
                if isinstance(value, (int, float, bool))
            ][:4]
            summary = validation_summary.get(level, {})

            findings.append(
                {
                    "finding_id": f"AUTO-{index:03d}",
                    "title": f"{level} {summary.get('name') or self._level_name(level)} validation failed",
                    "category": "VALIDATION",
                    "severity": severity,
                    "observed_evidence": [
                        summary.get("summary")
                        or "This validation level reported one or more differences."
                    ],
                    "derived_statistics": derived_statistics,
                    "likely_explanation": (
                        "The sanitized validation evidence shows a measurable "
                        "difference between the source and target datasets."
                    ),
                    "impact": (
                        "This failed level should be reviewed before the "
                        "comparison output is used for reporting or downstream "
                        "processing."
                    ),
                    "recommended_actions": [],
                    "related_levels": [level],
                }
            )

        return findings

    def _fallback_correlations_from_evidence(
        self,
        evidence: dict[str, Any],
    ) -> list[dict[str, Any]]:
        correlations = evidence.get("cross_level_correlations")
        if not isinstance(correlations, list):
            return []

        result = []
        for index, item in enumerate(correlations, start=1):
            if not isinstance(item, dict):
                continue
            result.append(
                {
                    "correlation_id": f"EVIDENCE-{index:03d}",
                    "title": self._string_value(
                        item.get("title") or item.get("type"),
                        f"Cross-level correlation {index}",
                    ),
                    "levels": self._normalize_levels(item.get("levels")),
                    "evidence": self._correlation_evidence(item),
                    "conclusion": self._string_value(
                        item.get("conclusion") or item.get("interpretation"),
                        "Evidence indicates a relationship between validation levels.",
                    ),
                }
            )

        return result

    @staticmethod
    def _correlation_evidence(value: Any) -> list[str]:
        if isinstance(value, str):
            return [value] if value.strip() else []
        if isinstance(value, list):
            statements = []
            for item in value:
                if isinstance(item, str) and item.strip():
                    statements.append(item)
                elif isinstance(item, dict):
                    statement = item.get("statement") or item.get("evidence")
                    if statement:
                        statements.append(str(statement))
            return statements
        if isinstance(value, dict):
            return [f"{key}: {item}" for key, item in value.items()]
        return []

    # ==========================================================
    # EVIDENCE
    # ==========================================================

    def _normalize_evidence_list(
        self,
        value: Any,
        kind: str,
    ) -> list[dict[str, Any]]:

        if value is None:
            return []

        # LLM sometimes returns a single dictionary.
        if isinstance(
            value,
            dict,
        ):

            result = []

            for key, item in value.items():

                result.append(
                    {
                        "kind": kind,
                        "statement": (
                            f"{key}: {self._string_value(item)}"
                        ),
                        "levels": self._extract_levels(
                            key
                        ),
                        "data": {},
                    }
                )

            return result

        # LLM sometimes returns a plain string.
        if isinstance(
            value,
            str,
        ):

            return [
                {
                    "kind": kind,
                    "statement": value,
                    "levels": [],
                    "data": {},
                }
            ]

        if not isinstance(
            value,
            list,
        ):
            return []

        result = []

        for item in value:

            if isinstance(
                item,
                str,
            ):

                result.append(
                    {
                        "kind": kind,
                        "statement": item,
                        "levels": [],
                        "data": {},
                    }
                )

            elif isinstance(
                item,
                dict,
            ):

                statement = (
                    item.get(
                        "statement"
                    )
                    or item.get(
                        "description"
                    )
                    or item.get(
                        "finding"
                    )
                    or item.get(
                        "explanation"
                    )
                    or json.dumps(
                        item,
                        default=str,
                    )
                )

                result.append(
                    {
                        "kind": item.get(
                            "kind",
                            kind,
                        ),
                        "statement": str(
                            statement
                        ),
                        "levels": self._normalize_levels(
                            item.get(
                                "levels"
                            )
                        ),
                        "data": {},
                    }
                )

        return result

    def _normalize_recommendations(
        self,
        value: Any,
    ) -> list[str]:

        if value is None:
            return []

        if isinstance(value, str):
            return [value]

        if isinstance(value, dict):
            result = []

            for key, item in value.items():
                if isinstance(item, str):
                    result.append(item)
                else:
                    result.append(
                        f"{key}: {item}"
                    )

            return result

        if isinstance(value, list):
            result = []

            for item in value:

                if isinstance(item, str):
                    result.append(item)

                elif isinstance(item, dict):

                    statement = (
                        item.get("statement")
                        or item.get("description")
                        or item.get("recommendation")
                        or item.get("action")
                    )

                    if statement:
                        result.append(
                            str(statement)
                        )
                    else:
                        result.append(
                            json.dumps(
                                item,
                                default=str,
                            )
                        )

            return result

        return [str(value)]

    # ==========================================================
    # HELPERS
    # ==========================================================

    @staticmethod
    def _string_value(
        value: Any,
        default: str = "",
    ) -> str:

        if value is None:
            return default

        if isinstance(
            value,
            str,
        ):
            return value

        return str(value)

    @staticmethod
    def _normalize_severity(
        value: Any,
    ) -> str:

        value = str(
            value or "MEDIUM"
        ).upper()

        if value not in {
            "INFO",
            "LOW",
            "MEDIUM",
            "HIGH",
            "CRITICAL",
        }:
            return "MEDIUM"

        return value

    @staticmethod
    def _normalize_levels(
        value: Any,
    ) -> list[str]:

        if isinstance(
            value,
            str,
        ):
            return [
                value
            ]

        if isinstance(
            value,
            list,
        ):
            return [
                str(item)
                for item in value
            ]

        return []

    @staticmethod
    def _extract_levels(
        text: Any,
    ) -> list[str]:

        text = str(
            text
        )

        levels = []

        for level in (
            "L1",
            "L2",
            "L3",
            "L4",
            "L5",
            "L6",
        ):

            if level in text:
                levels.append(
                    level
                )

        return levels

    @staticmethod
    def _level_name(
        level: str,
    ) -> str:

        names = {
            "L1": "Schema",
            "L2": "Volume",
            "L3": "Record",
            "L4": "Field Transformation",
            "L5": "Aggregate",
            "L6": "Data Quality",
        }

        return names.get(
            level,
            level,
        )

    @staticmethod
    def _infer_status(
        value: str,
    ) -> str:

        upper = value.upper()

        if "FAIL" in upper:
            return "FAIL"

        if "PASS" in upper:
            return "PASS"

        return "UNKNOWN"

    def _overall_status(
        self,
        raw: dict[str, Any],
        evidence: dict[str, Any],
    ) -> str:

        statuses = [
            str(level.get("status", "UNKNOWN")).upper()
            for level in evidence.get("levels", {}).values()
            if isinstance(level, dict)
        ]
        if "FAIL" in statuses:
            return "FAIL"
        if statuses and all(status == "PASS" for status in statuses):
            return "PASS"
        return "WARNING"

    def _severity_from_evidence(self, evidence: dict[str, Any]) -> str:
        levels = evidence.get("levels", {})
        failed = {
            level
            for level, value in levels.items()
            if isinstance(value, dict) and str(value.get("status", "")).upper() == "FAIL"
        }
        if not failed:
            return "INFO"
        if failed & {"L2", "L3"}:
            return "CRITICAL"
        if failed & {"L1", "L4", "L5"}:
            return "HIGH"
        return "MEDIUM"

    def _build_validation_from_evidence(
        self,
        evidence: dict[str, Any],
    ) -> list[dict[str, Any]]:

        result = []

        for level in (
            "L1",
            "L2",
            "L3",
            "L4",
            "L5",
            "L6",
        ):

            item = evidence.get("levels", {}).get(level)

            if not item:
                continue

            result.append(
                {
                    "level": level,
                    "name": self._level_name(
                        level
                    ),
                    "status": str(item.get("status", "UNKNOWN")).upper(),
                    "summary": self._validation_summary(level, item),
                }
            )

        return result

    @staticmethod
    def _validation_summary(level: str, item: dict[str, Any]) -> str:
        metrics = item.get("metrics", {})
        failed = metrics.get("checks_failed", metrics.get("mismatch_count", 0))
        if str(item.get("status", "")).upper() == "PASS":
            return "All configured checks passed."
        if failed is not None:
            return f"Validation reported {failed} failed check(s) or detected difference(s)."
        return f"{level} validation reported status {item.get('status', 'UNKNOWN')}."
