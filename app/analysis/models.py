from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field


Severity = Literal["INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"]


class L7Finding(BaseModel):
    finding_id: str
    title: str
    category: str
    severity: Severity
    observed_evidence: list[str] = Field(default_factory=list)
    derived_statistics: list[str] = Field(default_factory=list)
    likely_explanation: str | None = None
    impact: str = ""
    recommended_actions: list[str] = Field(default_factory=list)
    related_levels: list[str] = Field(default_factory=list)


class L7Report(BaseModel):
    report_version: str = "1.0"
    run_id: str
    generated_at: str
    overall_status: str
    overall_validation_percentage: float | None = None
    overall_data_match_percentage: float | None = None
    severity: Severity
    executive_summary: str
    overall_assessment: str
    validation_summary: list[dict[str, Any]] = Field(default_factory=list)
    key_findings: list[L7Finding] = Field(default_factory=list)
    cross_level_analysis: list[dict[str, Any]] = Field(default_factory=list)
    root_cause_analysis: list[dict[str, Any]] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)
    technical_evidence: dict[str, Any] = Field(default_factory=dict)
    ai_metadata: dict[str, Any] = Field(default_factory=dict)
