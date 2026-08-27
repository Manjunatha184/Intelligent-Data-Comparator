import React, { useEffect, useState } from "react";
import { Check, ChevronRight, Download, FileText, RefreshCw, TriangleAlert, X } from "lucide-react";

import { apiRequest } from "../../api/client";
import { Loading, Status } from "../../components/ui";
import { History } from "../history/History";

/* ============================================================
   RESULTS
============================================================ */

const RESULT_PAGE_SIZE = 50;

const LEVEL_NAMES = {
  L1: "Schema",
  L2: "Volume",
  L3: "Record",
  L4: "Field Transformation",
  L5: "Aggregate",
  L6: "Data Quality",
  L7: "Analysis & Recommendations",
};


function formatLabel(key) {
  if (!key) return "";
  return String(key).replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
}

function renderVal(value) {
  if (value === undefined || value === null || value === "") return "N/A";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function RowDataLink({ record }) {
  const [open, setOpen] = useState(false);
  if (!record || typeof record !== "object") return "N/A";

  return <>
    <button type="button" className="rowDataLink" onClick={() => setOpen(true)}>View row</button>
    {open && <div className="modalBackdrop" onClick={() => setOpen(false)}>
      <div className="modal rowDataModal" onClick={event => event.stopPropagation()}>
        <div className="modalHead">
          <div>
            <h2>Row data</h2>
            <p>Values captured for this comparison result</p>
          </div>
          <button type="button" className="iconButton" onClick={() => setOpen(false)}><X size={18} /></button>
        </div>
        <div className="rowDataTableWrap">
          <table className="rowDataTable">
            <thead><tr><th>Column</th><th>Value</th></tr></thead>
            <tbody>
              {Object.entries(record).map(([column, value]) => <tr key={column}>
                <td><b>{column}</b></td>
                <td>{renderVal(value)}</td>
              </tr>)}
            </tbody>
          </table>
        </div>
      </div>
    </div>}
  </>;
}

function formatMetricPercent(value) {
  if (value === undefined || value === null || value === "") return "N/A";
  if (typeof value === "string" && value.trim().endsWith("%")) return value;
  return formatNumber(value, true);
}

function getLevelSummary(level) {
  const status = String(level?.status || "PASS").toUpperCase();
  const m = level?.metrics || {};
  const d = level?.differences || {};
  if (status === "NOT_APPLICABLE") return m.reason || "Not applicable for this comparison strategy";
  if (status === "PASS") {
    if (level.level === "L3") return "All records were reconciled by business key or configured grouping fields";
    if (level.level === "L5") return "All aggregate rules matched";
    if (level.level === "L6") return Number(m.rules_total || 0) > 0 ? "All data-quality rules passed" : "No DQ rules executed";
    return "No differences detected";
  }
  if (level.level === "L1") {
    const n = m.mismatch_count ?? m.schema_drift_count ?? d.schema_drift?.items?.length ?? 0;
    return n ? `${n} schema difference${n === 1 ? "" : "s"} detected` : "Schema validation failed";
  }
  if (level.level === "L2") {
    const failed = (d.checks || []).filter(c => c.check !== "null_counts" && !c.matched).length;
    const nullFailed = Object.values((d.checks || []).find(c => c.check === "null_counts")?.columns || {}).filter(c => !c.matched).length;
    const n = failed + nullFailed;
    return n ? `${n} volume check${n === 1 ? "" : "s"} failed` : "Volume validation failed";
  }
  if (level.level === "L3") {
    if (m.comparison_mode === "GROUP_RECONCILIATION" || m.matching_mode === "GROUP_RECONCILIATION") {
      const missingBusinessKeys = Number(m.missing_business_key_count || 0);
      if (missingBusinessKeys) return `${missingBusinessKeys} business key${missingBusinessKeys === 1 ? " is" : "s are"} missing on one side`;
      const differences = m.group_difference_count;
      if (differences === undefined || differences === null) return "Group reconciliation results unavailable";
      return `${differences} group difference${differences === 1 ? "" : "s"} detected`;
    }
    const missing = Number(m.missing_key_count || 0);
    const extra = Number(m.extra_key_count || 0);
    const sourceDuplicates = Number(m.source_duplicate_key_count || 0);
    const targetDuplicates = Number(m.target_duplicate_key_count || 0);
    const issues = [];
    if (missing) issues.push(`${missing} business key${missing === 1 ? "" : "s"} missing in target`);
    if (extra) issues.push(`${extra} extra business key${extra === 1 ? "" : "s"} in target`);
    if (sourceDuplicates) issues.push(`${sourceDuplicates} duplicate key${sourceDuplicates === 1 ? "" : "s"} in source`);
    if (targetDuplicates) issues.push(`${targetDuplicates} duplicate key${targetDuplicates === 1 ? "" : "s"} in target`);
    if (issues.length) return issues.join("; ");
    return "Business-key reconciliation failed";
  }
  if (level.level === "L4") {
    const n = d.field_mismatches?.items?.length ?? m.mismatch_count ?? 0;
    return n ? `${n} field mismatch${n === 1 ? "" : "es"} detected` : "Field validation failed";
  }
  if (level.level === "L5") return `${m.checks_failed ?? 0} aggregate rule${m.checks_failed === 1 ? "" : "s"} failed`;
  if (level.level === "L6") return `${m.checks_failed ?? 0} data-quality rule${m.checks_failed === 1 ? "" : "s"} failed`;
  if (level.level === "L7") return m.findings_count ? `${m.findings_count} finding${m.findings_count === 1 ? "" : "s"} requiring review` : "No triage findings";
  return "Validation failed";
}

export function Results({ runId, onOpenRun, onBack, onOpenAnalysis, notify }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [showRawJson, setShowRawJson] = useState(false);
  const [expanded, setExpanded] = useState(() => new Set(["L1", "L2", "L4", "L6"]));

  async function loadResults() {
    if (!runId) {
      setData(null);
      return;
    }

    setLoading(true);
    setData(null);

    try {
      const result = await apiRequest(`/comparisons/${runId}/results`, { method: "GET" });
      setData(result);
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadResults();
  }, [runId]);

  if (!runId) return <History onOpenRun={onOpenRun} notify={notify} />;
  if (loading && !data) return <Loading text="Loading comparison results…" />;

  const baseLevels = data?.levels || [];
  const analysis = data?.analysis || null;

  // L7 is now a report, not another validation evidence card.
  // Keep the old L7 card only when the backend has not yet
  // returned the structured analysis report.
  const levels = analysis
    ? baseLevels.filter((level) => level.level !== "L7")
    : baseLevels;
  const scoredLevels = baseLevels.filter(l => !["NOT_APPLICABLE", "NOT RUN"].includes(String(l.status).toUpperCase()));
  const passedLevels = scoredLevels.filter(l => String(l.status).toUpperCase() === "PASS").length;
  const failedLevels = scoredLevels.filter(l => String(l.status).toUpperCase() === "FAIL").length;
  const overallStatus = String(data?.comparison_status || data?.status || "FAIL").toUpperCase();

  function toggle(levelId) {
    setExpanded(current => {
      const next = new Set(current);
      next.has(levelId) ? next.delete(levelId) : next.add(levelId);
      return next;
    });
  }

  return (
    <div className="resultsPage">
      <div className="wizardFooter">
        <h1 className="pageTitle" style={{ margin: 0 }}>
          Comparison results <small className="runIdText" style={{ fontSize: "12px", color: "var(--muted)", fontWeight: "normal", marginLeft: "6px" }}>({runId})</small>
        </h1>
        <div className="actionRow">
          <button type="button" className="secondary small" onClick={onBack}>
            ← Back to Results
          </button>
          <button className="secondary small" onClick={loadResults} disabled={loading}>
            <RefreshCw size={14} className={loading ? "spin" : ""} />
            Refresh
          </button>
          <button className="primary small" onClick={() => {
            if (!analysis) {
              notify("Analysis report is not available because L7 was not selected for this comparison.", "error");
              return;
            }
            onOpenAnalysis(runId);
          }}>
              <FileText size={14} />
              Analysis report
          </button>
          <button className="secondary small" onClick={() => setShowRawJson(true)}>
            <FileText size={14} />
            Raw evidence
          </button>
        </div>
      </div>

      <section className="resultSummaryGrid">
        <ResultSummaryCard label="Validation levels" value={baseLevels.length} />
        <ResultSummaryCard label="Passed" value={`${Math.round((passedLevels / (scoredLevels.length || 1)) * 100)}%`} tone="pass" />
        <ResultSummaryCard label="Failed" value={`${Math.round((failedLevels / (scoredLevels.length || 1)) * 100)}%`} tone="fail" />
        <ResultSummaryCard label="Overall" value={overallStatus} tone={overallStatus === "PASS" ? "pass" : "fail"} />
      </section>

      <section className="levelSummaryPanel">
        <div className="levelSummaryHead">
          <div><span className="sectionEyebrow">VALIDATION LEVELS</span><h3>Comparison overview</h3></div>
          <button className="textBtn" onClick={() => setExpanded(new Set(levels.map(l => l.level)))}>Expand all</button>
        </div>
        <div className="levelSummaryGrid">
          {levels.map(level => {
            const status = String(level.status || "PASS").toUpperCase();
            return (
              <button key={level.level} className={`levelSummaryItem ${status.toLowerCase()}`} onClick={() => toggle(level.level)}>
                <span className="levelSummaryCode">{level.level}</span>
                <span className="levelSummaryName">{LEVEL_NAMES[level.level] || level.name}</span>
                <Status status={status} />
                <span className="levelSummaryFinding">{getLevelSummary(level)}</span>
                {status === "REVIEW" && level.differences?.findings?.map((f, i) => (
                  <div key={i} className="likelyCause"><span className="lcLabel">POSSIBLE EXPLANATION</span> {f.likely_cause}</div>
                ))}
              </button>
            );
          })}
        </div>
      </section>

      <div className="resultLevelsClean">
        {levels.map((level) => (
          <ResultLevelClean
            key={level.level}
            level={level}
            expanded={expanded.has(level.level)}
            onToggle={() => toggle(level.level)}
          />
        ))}
      </div>

      {showRawJson && <RawEvidenceModal data={data} onClose={() => setShowRawJson(false)} />}
    </div>
  );
}

function fallbackAnalysisFindings(report, levels, validationSummary) {
  const existingFindings = Array.isArray(report?.key_findings)
    ? report.key_findings
    : [];
  if (existingFindings.length) return existingFindings;

  const summariesByLevel = new Map(
    (validationSummary || [])
      .filter((item) => item?.level)
      .map((item) => [String(item.level), item])
  );

  return Object.entries(levels || {})
    .filter(([, levelData]) => String(levelData?.status || "").toUpperCase() === "FAIL")
    .map(([levelKey, levelData], index) => {
      const summary = summariesByLevel.get(levelKey) || {};
      const metrics = levelData.metrics || {};
      const metricEvidence = Object.entries(metrics)
        .filter(([, value]) => typeof value === "number" || typeof value === "boolean")
        .slice(0, 4)
        .map(([key, value]) => `${key.replace(/_/g, " ")}: ${value}`);

      return {
        finding_id: `AUTO-${index + 1}`,
        title: `${levelKey} ${summary.name || levelData.name || "validation"} failed`,
        severity: report?.severity || "MEDIUM",
        observed_evidence: [
          summary.summary || "This validation level reported one or more differences.",
        ],
        derived_statistics: metricEvidence,
        likely_explanation: "The validation evidence indicates a measurable difference between the source and target datasets.",
        impact: "Review this failed level before relying on the comparison output for reporting or downstream processing.",
        related_levels: [levelKey],
      };
    });
}

export function L7AnalysisReportView({
  report,
  runId,
  onBack,
  onDownload,
}) {
  // The API normally returns the report directly; tolerate an envelope so
  // generated LLM fields are still rendered if the response is wrapped.
  report = report?.report || report?.analysis || report;
  const sanitized = report.technical_evidence?.sanitized_evidence || {};
  const levels = sanitized.levels || {};
  const validationSummary = report.validation_summary || [];
  const reportCorrelations = Array.isArray(report.cross_level_analysis)
    ? report.cross_level_analysis
    : [];
  const evidenceCorrelations = Array.isArray(sanitized.cross_level_correlations)
    ? sanitized.cross_level_correlations
    : [];
  const correlations = reportCorrelations.length
    ? reportCorrelations
    : evidenceCorrelations;
  const privacy = sanitized.privacy_policy || {};

  const execFindings = fallbackAnalysisFindings(report, levels, validationSummary);
  const readableEvidence = (value) => {
    if (value === null || value === undefined) return "â€”";
    if (["string", "number", "boolean"].includes(typeof value)) return String(value);
    if (Array.isArray(value)) return value.map(readableEvidence).join(", ");
    if (typeof value === "object") {
      if (value.statement) return String(value.statement);
      return Object.entries(value).map(([key, item]) => `${key.replace(/_/g, " ")}: ${readableEvidence(item)}`).join("; ");
    }
    return String(value);
  };

  return (
    <div className="stack">
      <h1 className="pageTitle">L7 Analysis Report</h1>
      <div className="pageActions">
        <div>
          <span className="sectionEyebrow">ANALYSIS REPORT</span>
          <h2>Comparison analysis</h2>
          <p className="runIdText">{runId}</p>
        </div>

        <div className="actionRow">
          <button
            type="button"
            className="secondary"
            onClick={onBack}
          >
            ← Back to results
          </button>
          <button
            type="button"
            className="primary"
            onClick={onDownload}
          >
            <Download size={14} />
            Download PDF
          </button>
        </div>
      </div>
      <div className="analysisReportStatus">
        <div>
          <span>Overall status</span>
          <strong>{report.overall_status || "—"}</strong>
        </div>

        <div>
          <span>Severity</span>
          <strong>{report.severity || "—"}</strong>
        </div>

        <div>
          <span>Timestamp</span>
          <strong>{report.generated_at ? new Date(report.generated_at).toLocaleString() : "—"}</strong>
        </div>
      </div>

      <AnalysisReportSection title="Executive Summary">
        <p style={{
          fontSize: "13px",
          lineHeight: "1.6",
          color: "var(--text-primary)",
          marginBottom: "1rem",
          whiteSpace: "pre-wrap"
        }}>
          {report.executive_summary || "No executive summary was generated for this run."}
        </p>
      </AnalysisReportSection>

      <AnalysisReportSection title="Overall Assessment">
        <p style={{
          fontSize: "13px",
          lineHeight: "1.6",
          color: "var(--text-secondary)",
          marginBottom: "1rem",
          whiteSpace: "pre-wrap"
        }}>
          {report.overall_assessment || "No overall assessment was generated for this run."}
        </p>
      </AnalysisReportSection>

      <AnalysisReportSection title="Validation Summary">
        <div className="analysisValidation">
          {(validationSummary.length ? validationSummary : Object.entries(levels).map(([level, data]) => ({ level, ...data }))).map((levelData, index) => {
            const levelKey = levelData.level || levelData.level_id || levelData.code || `L${index + 1}`;
            return (
              <div className="analysisValidationRow" key={levelKey}>
                <span className="analysisLevelCode">{levelKey}</span>
                <strong>
                  {levelKey === "L1" && "Schema"}
                  {levelKey === "L2" && "Volume"}
                  {levelKey === "L3" && "Record Matching"}
                  {levelKey === "L4" && "Field Comparison"}
                  {levelKey === "L5" && "Aggregation"}
                  {levelKey === "L6" && "Data Quality"}
                </strong>
                <Status status={levelData.status || "UNKNOWN"} />
                <span className="analysisValidationSummary">
                  {levelData.summary || "No summary was recorded for this validation level."}
                </span>
              </div>
            );
          })}
          {!validationSummary.length && !Object.keys(levels).length && (
            <div className="analysisEmpty">No validation summary was recorded for this run.</div>
          )}
        </div>
      </AnalysisReportSection>

      <AnalysisReportSection title="Key Findings">
        <div className="analysisFindingList">
          {execFindings.length ? execFindings.map((finding, idx) => (
            <article className="analysisFindingCard" key={idx}>
              <div className="analysisFindingTop">
                <h4>{finding.title}</h4>
                <span className={`analysisSeverity ${finding.severity?.toLowerCase()}`}>
                  {finding.severity}
                </span>
              </div>

              {finding.observed_evidence?.length > 0 && (
                <div style={{ marginTop: "1rem" }}>
                  <h5 style={{ fontSize: "11px", fontWeight: "700", textTransform: "uppercase", color: "var(--text-secondary)", marginBottom: "4px" }}>
                    Observed Evidence
                  </h5>
                  <ul className="analysisBulletList" style={{ marginTop: 0 }}>
                    {finding.observed_evidence.map((m, i) => (
                      <li key={i}>{m}</li>
                    ))}
                  </ul>
                </div>
              )}

              {finding.derived_statistics?.length > 0 && (
                <div style={{ marginTop: "1rem" }}>
                  <h5 style={{ fontSize: "11px", fontWeight: "700", textTransform: "uppercase", color: "var(--text-secondary)", marginBottom: "4px" }}>
                    Derived Metrics
                  </h5>
                  <ul className="analysisBulletList" style={{ marginTop: 0 }}>
                    {finding.derived_statistics.map((m, i) => (
                      <li key={i}>{m}</li>
                    ))}
                  </ul>
                </div>
              )}

              {finding.likely_explanation && (
                <div style={{ marginTop: "1rem" }}>
                  <h5 style={{ fontSize: "11px", fontWeight: "700", textTransform: "uppercase", color: "var(--text-secondary)", marginBottom: "4px" }}>
                    What this means
                  </h5>
                  <p style={{ margin: 0, fontSize: "0.9rem" }}>{finding.likely_explanation}</p>
                </div>
              )}

              {finding.impact && (
                <div style={{ marginTop: "1rem" }}>
                  <h5 style={{ fontSize: "11px", fontWeight: "700", textTransform: "uppercase", color: "var(--text-secondary)", marginBottom: "4px" }}>
                    Why this matters
                  </h5>
                  <p style={{ margin: 0, fontSize: "0.9rem" }}>{finding.impact}</p>
                </div>
              )}

            </article>
          )) : <div className="analysisEmpty">No key findings were reported for this run.</div>}
        </div>
      </AnalysisReportSection>

      <AnalysisReportSection title="How the validation levels relate" count={correlations.length}>
        {correlations.length === 0 ? (
          <div className="analysisEmpty">No cross-level correlations were established.</div>
        ) : (
          <div className="analysisCorrelationList">
            {correlations.map((item, index) => (
              <article className="analysisCorrelationCard" key={index}>
                <div className="analysisFindingTop">
                  <div>
                    <h4>{item.title || item.type || `Cross-level comparison ${index + 1}`}</h4>
                  </div>
                </div>
                <p>{item.conclusion || item.interpretation || "The supplied evidence shows a relationship between these validation levels."}</p>
                {(item.evidence || []).length > 0 && (
                  <ul className="analysisBulletList">
                    {item.evidence.map((evidenceItem, evidenceIndex) => (
                      <li key={evidenceIndex}>{readableEvidence(evidenceItem)}</li>
                    ))}
                  </ul>
                )}
                <ul className="analysisBulletList">
                  {Object.entries(item)
                    .filter(([k]) => !["correlation_id", "title", "type", "conclusion", "interpretation", "evidence", "levels"].includes(k))
                    .map(([k, v]) => (
                      <li key={k}>{k.replace(/_/g, " ")}: {readableEvidence(v)}</li>
                    ))}
                </ul>
                <div className="analysisLevelLinks" style={{ marginTop: "12px" }}>
                  {(item.levels || []).map((level) => (
                    <span key={level}>{level}</span>
                  ))}
                </div>
              </article>
            ))}
          </div>
        )}
      </AnalysisReportSection>

      <AnalysisReportSection title="Privacy">
        <p className="analysisLead" style={{ fontSize: "0.9rem", color: "var(--text-secondary)" }}>
          Privacy-safe analysis: raw client records, matched pairs, record keys and raw field values were not provided to the LLM.
          Analysis uses only derived structural and statistical evidence.
        </p>
      </AnalysisReportSection>

      <AnalysisReportSection title="Technical Evidence">
        <details>
          <summary style={{ cursor: "pointer", fontWeight: "600", marginBottom: "1rem" }}>
            View technical evidence JSON
          </summary>
          <pre className="analysisTechnicalEvidence">
            {JSON.stringify(report.technical_evidence || {}, null, 2)}
          </pre>
        </details>
      </AnalysisReportSection>
    </div>
  );
}

function AnalysisReportSection({
  title,
  count,
  children,
}) {
  return (
    <section className="analysisReportSection">
      <div className="analysisSectionHeading">
        <h3>{title}</h3>
        {Number(count) > 0 && (
          <span>{count}</span>
        )}
      </div>
      {children}
    </section>
  );
}

function AnalysisEvidenceGroup({
  title,
  items,
}) {
  if (!items?.length) return null;

  return (
    <div className="analysisEvidenceGroup">
      <h5>{title}</h5>

      <ul>
        {items.map((item, index) => (
          <li key={index}>
            <span className="analysisEvidenceTag">
              {item.kind || "EVIDENCE"}
            </span>
            <span>{item.statement}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function ResultSummaryCard({ label, value, tone = "" }) {
  const summaryClass = label.toLowerCase().replace(/\s+/g, "-");
  return <div className={`resultSummaryCard ${tone} summary-${summaryClass}`}><span>{label}</span><strong>{renderVal(value)}</strong></div>;
}

function ResultLevelClean({ level, expanded, onToggle }) {
  const status = String(level.status || "PASS").toUpperCase();
  const levelTone = status === "PASS" ? "pass" : status === "REVIEW" ? "review" : "fail";
  return (
    <section className={`resultLevelClean ${levelTone}`}>
      <button className="resultLevelHeader" onClick={onToggle}>
        <div className="resultLevelIdentity">
          <span className="resultLevelCode">{level.level}</span>
          <div><h3>{LEVEL_NAMES[level.level] || level.name}</h3><p>{getLevelSummary(level)}</p></div>
        </div>
        <div className="resultLevelHeaderRight">
          <Status status={status} />
          <ChevronRight className={expanded ? "rotate90" : ""} size={18} />
        </div>
      </button>
      {expanded && (
        <div className="resultLevelContent">
          {level.level === "L1" && <L1DetailsClean level={level} />}
          {level.level === "L2" && <L2DetailsClean level={level} />}
          {level.level === "L3" && <L3DetailsClean level={level} />}
          {level.level === "L4" && <L4DetailsClean level={level} />}
          {level.level === "L5" && <L5DetailsClean level={level} />}
          {level.level === "L6" && <L6DetailsClean level={level} />}
          {level.level === "L7" && <L7DetailsClean level={level} />}
        </div>
      )}
    </section>
  );
}

function ResultMetricGrid({ items }) {
  return <div className="resultMetricGrid">{items.map(([label, value], i) => (
    <div className="resultMetric" key={i}><span>{label}</span><strong>{renderVal(value)}</strong></div>
  ))}</div>;
}

function ExpandableEvidenceRow({ row, columns }) {
  const [open, setOpen] = useState(false);
  const details = row.source_records || row.target_records || row.record || row.source_record || row.target_record || row.source_failed_records || row.target_failed_records || row.rule;

  return (
    <>
      <tr onClick={() => details && setOpen(!open)} style={{ cursor: details ? "pointer" : "default" }}>
        {columns.map(c => <td key={c.key} className={c.className || ""}>{c.render ? c.render(row) : renderVal(row?.[c.key])}</td>)}
      </tr>
      {open && details && (
        <tr>
          <td colSpan={columns.length} className="expandedRowCell" style={{ padding: "12px", background: "#f8f9fa", borderBottom: "1px solid #eaeaea" }}>
            <div style={{ display: "flex", gap: "16px", overflowX: "auto" }}>
              {row.rule && <div style={{ flex: 1 }}><strong>Rule Definition:</strong><pre style={{ fontSize: "11px", background: "#fff", padding: "8px", border: "1px solid #ddd", borderRadius: "4px" }}>{JSON.stringify(row.rule, null, 2)}</pre></div>}
              {row.source_failed_records && <div style={{ flex: 1 }}><strong>Source Failed Records:</strong><pre style={{ fontSize: "11px", background: "#fff", padding: "8px", border: "1px solid #ddd", borderRadius: "4px" }}>{JSON.stringify(row.source_failed_records, null, 2)}</pre></div>}
              {row.target_failed_records && <div style={{ flex: 1 }}><strong>Target Failed Records:</strong><pre style={{ fontSize: "11px", background: "#fff", padding: "8px", border: "1px solid #ddd", borderRadius: "4px" }}>{JSON.stringify(row.target_failed_records, null, 2)}</pre></div>}
              {row.source_records && <div style={{ flex: 1 }}><strong>Source Records:</strong><pre style={{ fontSize: "11px", background: "#fff", padding: "8px", border: "1px solid #ddd", borderRadius: "4px" }}>{JSON.stringify(row.source_records, null, 2)}</pre></div>}
              {row.target_records && <div style={{ flex: 1 }}><strong>Target Records:</strong><pre style={{ fontSize: "11px", background: "#fff", padding: "8px", border: "1px solid #ddd", borderRadius: "4px" }}>{JSON.stringify(row.target_records, null, 2)}</pre></div>}
              {row.source_record && <div style={{ flex: 1 }}><strong>Source Record:</strong><pre style={{ fontSize: "11px", background: "#fff", padding: "8px", border: "1px solid #ddd", borderRadius: "4px" }}>{JSON.stringify(row.source_record, null, 2)}</pre></div>}
              {row.target_record && <div style={{ flex: 1 }}><strong>Target Record:</strong><pre style={{ fontSize: "11px", background: "#fff", padding: "8px", border: "1px solid #ddd", borderRadius: "4px" }}>{JSON.stringify(row.target_record, null, 2)}</pre></div>}
              {row.record && <div style={{ flex: 1 }}><strong>Failed Record:</strong><pre style={{ fontSize: "11px", background: "#fff", padding: "8px", border: "1px solid #ddd", borderRadius: "4px" }}>{JSON.stringify(row.record, null, 2)}</pre></div>}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function EvidenceTable({ title, count, columns, rows, emptyText = "No evidence available.", description, emptySuccess = false }) {
  return (
    <section className="evidenceSection">
      <div className="evidenceSectionHead"><h4>{title}</h4><span>{count ?? rows?.length ?? 0} items</span></div>
      {!rows?.length ? <div className={`evidenceEmpty${emptySuccess ? " success" : ""}`}>{emptySuccess && <Check size={16} />}{emptyText}</div> : (
        <>
          <PaginatedTable data={rows} pageSize={20} label="rows"
            renderHeader={() => columns.map(c => <th key={c.key}>{c.label}</th>)}
            renderRow={(row, i) => <ExpandableEvidenceRow key={row?.id || row?.key || row?.signature || i} row={row} columns={columns} />}
          />
          {description && <div style={{ fontSize: "12px", color: "var(--color-text-dim)", marginTop: "12px", padding: "0 4px" }}>{description}</div>}
        </>
      )}
    </section>
  );
}

function L1DetailsClean({ level }) {
  const m = level.metrics || {}, d = level.differences || {};
  const matched = d.matched_columns?.items || [];
  const differences = d.schema_differences?.items || d.schema_drift?.items || [];
  return <div className="detailsClean">
    <ResultMetricGrid items={[
      ["Source columns", m.source_column_count], ["Target columns", m.target_column_count],
      ["Source column coverage", formatMetricPercent(m.source_column_coverage_pct)],
      ["Target column coverage", formatMetricPercent(m.target_column_coverage_pct)],
      ["Matched columns", m.matched_column_count], ["Length mismatches", m.length_mismatch_count],
    ]} />
    <EvidenceTable title="Schema differences" rows={differences}
      columns={[
        { key: "type", label: "Difference", render: r => formatLabel(r.type || r.difference_type || "Schema change") },
        { key: "source_column", label: "Source column", render: r => r.source_column || r.column },
        { key: "target_column", label: "Target column", render: r => r.target_column || r.column },
        { key: "source", label: "Source", render: r => r.source_length ?? r.source_value ?? r.source_type },
        { key: "target", label: "Target", render: r => r.target_length ?? r.target_value ?? r.target_type },
        { key: "status", label: "Status", render: () => <Status status="FAIL" /> }
      ]} emptyText="No schema differences detected." />
    <EvidenceTable title="Matched columns" rows={matched}
      columns={[
        { key: "source_column", label: "Source column" }, { key: "target_column", label: "Target column" },
        { key: "status", label: "Status", render: () => <Status status="PASS" /> }
      ]} emptyText="No matched column evidence returned." />
  </div>;
}

function L2DetailsClean({ level }) {
  const m = level.metrics || {}, d = level.differences || {};
  const checks = (d.checks || []).filter(c => !["null_counts", "filtered_rows", "partition_rows"].includes(c.check));
  const nullCheck = (d.checks || []).find(c => c.check === "null_counts");
  const nullRows = Object.entries(nullCheck?.columns || {}).map(([column, value]) => ({ column, ...value })).filter(r => !r.matched);
  return <div className="detailsClean">
    <ResultMetricGrid items={[
      ["Total rows", `${renderVal(m.total_rows_source)} → ${renderVal(m.total_rows_target)}`],
      ["Row count change", formatMetricPercent(m.row_count_percent_change)],
      ["Volume coverage", formatMetricPercent(m.volume_coverage_pct)],
      ["Distinct business keys", `${renderVal(m.distinct_key_count_source)} → ${renderVal(m.distinct_key_count_target)}`],
      ["Distinct business-key change", formatMetricPercent(m.distinct_key_percent_change)],
      ["Duplicate business-key rows", `${renderVal(m.duplicate_key_count_source)} → ${renderVal(m.duplicate_key_count_target)}`],
      ["Source duplicate rate", formatMetricPercent(m.source_duplicate_key_rate_pct)],
      ["Target duplicate rate", formatMetricPercent(m.target_duplicate_key_rate_pct)]
    ]} />
    <EvidenceTable title="Validation checks" rows={checks}
      columns={[
        { key: "check", label: "Check", render: r => <b>{formatLabel(r.check)}</b> }, { key: "source", label: "Source" },
        { key: "target", label: "Target" }, { key: "difference", label: "Difference", render: r => Number(r.difference) > 0 ? `+${r.difference}` : renderVal(r.difference) },
        { key: "matched", label: "Status", render: r => <Status status={r.matched ? "PASS" : "FAIL"} /> }
      ]} emptyText="No volume checks returned." />
    <EvidenceTable title="Null count differences" rows={nullRows}
      columns={[
        { key: "column", label: "Column", render: r => <b>{r.column}</b> }, { key: "source", label: "Source nulls" },
        { key: "target", label: "Target nulls" }, { key: "difference", label: "Difference", render: r => Number(r.difference) > 0 ? `+${r.difference}` : renderVal(r.difference) },
        { key: "matched", label: "Status", render: r => <Status status={r.matched ? "PASS" : "FAIL"} /> }
      ]} emptyText="No null-count differences." />
  </div>;
}

function getRecordKey(r) {
  if (r.business_key !== undefined && r.business_key !== null) return r.business_key;
  if (r.key) {
    try {
      const parsed = JSON.parse(r.key);
      if (parsed && typeof parsed === "object") return Object.values(parsed).join(" + ");
    } catch {
      return r.key;
    }
  }
  return r.business_key ?? r.key ?? r.signature ?? r.record_key ?? "N/A";
}

function extractRecord(r) {
  return r.record || r.source_record || r.target_record || (r.source_records ? r.source_records[0] : null) || (r.target_records ? r.target_records[0] : null) || {};
}

function duplicateEvidenceColumns(rows, side) {
  const columns = getDynamicColumns(rows, `Duplicate key in ${side}`);
  return [
    columns[0],
    { key: "duplicate_count", label: "Occurrences", render: row => `${row.duplicate_count} times` },
    ...columns.slice(1),
  ];
}

const duplicateReconciliationColumns = [
  { key: "key", label: "Business key", render: r => <CopyableKey text={getRecordKey(r)} /> },
  { key: "source_occurrences", label: "Source occurrences" },
  { key: "target_occurrences", label: "Target occurrences" },
  { key: "compared_pairs", label: "Rows compared", render: r => renderVal(r.compared_pairs) },
  { key: "source_record", label: "Source row", render: r => <RowDataLink record={r.source_record} /> },
  { key: "target_record", label: "Target row", render: r => <RowDataLink record={r.target_record} /> },
];

function getDynamicColumns(rows, typeLabel, includeReason = true) {
  const baseCols = [
    { key: "key", label: "Business key", render: r => <CopyableKey text={getRecordKey(r)} /> }
  ];

  if (!rows || rows.length === 0) {
    return baseCols;
  }

  const sampleRecords = rows.slice(0, 5).map(extractRecord);
  const allKeys = new Set();
  sampleRecords.forEach(rec => Object.keys(rec).forEach(k => {
    if (k.toLowerCase() !== 'id' && k.toLowerCase() !== 'key') {
      allKeys.add(k);
    }
  }));

  const extraCols = Array.from(allKeys).slice(0, 5).map(k => ({
    key: k, label: k, render: r => {
      const rec = extractRecord(r);
      return renderVal(rec[k]);
    }
  }));

  const cols = [...baseCols, ...extraCols];
  if (includeReason) {
    cols.push({ key: "reason", label: "Reason", render: r => r.reason || typeLabel });
  }
  return cols;
}

function L3DetailsClean({ level }) {
  const m = level.metrics || {}, d = level.differences || {};

  if (m.comparison_mode === "GROUP_RECONCILIATION" || m.matching_mode === "GROUP_RECONCILIATION") {
    const groupRows = d.group_reconciliation?.items || d.group_reconciliation || [];
    return <>
      <L3SummaryCards metrics={m} groupRows={groupRows} />
      {m.row_reconciliation && <RowReconciliationDetails metrics={m.row_reconciliation} differences={d} showMetrics={false} showPrimaryDuplicates={false} />}
      <GroupReconciliationDetails level={level} showMetrics={false} />
    </>;
  }

  const missingRows = d.missing_records?.items || d.missing_keys?.items || [];
  const extraRows = d.extra_records?.items || d.extra_keys?.items || [];
  const duplicateSourceRows = d.duplicate_source_records?.items || [];
  const duplicateTargetRows = d.duplicate_target_records?.items || [];
  const duplicateKeyRows = d.duplicate_key_reconciliation?.items || [];
  const mismatchRows = d.record_mismatches?.items || d.mismatches?.items || [];
  const unmatchableSourceRows = d.unmatchable_source_records?.items || d.unmatchable_source_records || [];
  const unmatchableTargetRows = d.unmatchable_target_records?.items || d.unmatchable_target_records || [];

  const unmatchableRows = [
    ...unmatchableSourceRows.map(r => ({ ...r, side: "Source" })),
    ...unmatchableTargetRows.map(r => ({ ...r, side: "Target" }))
  ];

  return <div className="detailsClean">
    <ResultMetricGrid items={[
      ["Source records", m.source_record_count],
      ["Target records", m.target_record_count],
      ["Matched business keys", m.matched_key_count],
      ["Source coverage", formatMetricPercent(m.source_record_coverage_pct)],
      ["Target coverage", formatMetricPercent(m.target_record_coverage_pct)],
      ["Missing in target", m.missing_key_count],
      ["Missing rate", formatMetricPercent(m.missing_record_rate_pct)],
      ["Extra in target", m.extra_key_count],
      ["Extra rate", formatMetricPercent(m.extra_record_rate_pct)],
      ["Duplicate business-key rows in source", m.source_duplicate_key_count || 0],
      ["Duplicate business-key rows in target", m.target_duplicate_key_count || 0],
      ["Manual review", (m.unmatchable_source_count || 0) + (m.unmatchable_target_count || 0)]
    ]} />

    {missingRows.length > 0 && <EvidenceTable title="Missing in Target" rows={missingRows} columns={getDynamicColumns(missingRows, "Missing in target")} />}
    {extraRows.length > 0 && <EvidenceTable title="Extra in Target (Missing in Source)" rows={extraRows} columns={getDynamicColumns(extraRows, "Extra in target")} />}
    {duplicateKeyRows.length > 0 && <EvidenceTable title="Duplicate business-key reconciliation" rows={duplicateKeyRows} columns={duplicateReconciliationColumns} />}
    {!duplicateKeyRows.length && duplicateSourceRows.length > 0 && <EvidenceTable title="Duplicate Business Keys in Source" rows={duplicateSourceRows} columns={duplicateEvidenceColumns(duplicateSourceRows, "source")} />}
    {!duplicateKeyRows.length && duplicateTargetRows.length > 0 && <EvidenceTable title="Duplicate Business Keys in Target" rows={duplicateTargetRows} columns={duplicateEvidenceColumns(duplicateTargetRows, "target")} />}
    {mismatchRows.length > 0 && <EvidenceTable title="Record issues" rows={mismatchRows} columns={getDynamicColumns(mismatchRows, "MISMATCH")} />}

    {unmatchableRows.length > 0 && <EvidenceTable title="Unmatchable Records" rows={unmatchableRows} columns={[
      { key: "side", label: "Side", render: r => <b>{r.side}</b> },
      { key: "reason", label: "Reason", render: r => r.reason || "No usable matching attributes" },
      { key: "record", label: "Record", render: r => <RowDataLink record={r.record} /> }
    ]} />}

    {!missingRows.length && !extraRows.length && !duplicateSourceRows.length && !duplicateTargetRows.length && !mismatchRows.length && !unmatchableRows.length && <div className="evidenceEmpty success"><Check size={16} /> No record mismatches detected.</div>}
  </div>;
}

function L3SummaryCards({ metrics: m, groupRows }) {
  const unmatchedRows = groupRows.filter(r => ["MISSING_GROUP_IN_TARGET", "EXTRA_GROUP_IN_TARGET"].includes(r.status)).length;
  const duplicateGroups = groupRows.filter(r => r.status === "GROUP_DUPLICATE_ROWS").length;
  const aggregateMismatches = groupRows.filter(r => ["GROUP_VALUE_MISMATCH", "GROUP_ROW_COUNT_MISMATCH"].includes(r.status)).length;
  return <ResultMetricGrid items={[
    ["Source records", m.source_record_count],
    ["Target records", m.target_record_count],
    ["Matched business keys", m.row_reconciliation?.matched_key_count ?? m.matched_key_count],
    ["Missing in target", m.row_reconciliation?.missing_key_count ?? m.missing_key_count],
    ["Extra in target", m.row_reconciliation?.extra_key_count ?? m.extra_key_count],
    ["Missing business keys", m.missing_business_key_count || 0],
    ["Common groups", m.common_group_count],
    ["Groups with mismatch", m.groups_with_mismatch],
    ["Grouped duplicates", duplicateGroups],
    ["Unmatched rows", unmatchedRows],
    ["Aggregate mismatches", aggregateMismatches],
    ["Group coverage", `${formatMetricPercent(m.source_group_coverage)} / ${formatMetricPercent(m.target_group_coverage)}`],
  ]} />;
}

function RowReconciliationDetails({ metrics: m, differences: d, showMetrics = true, showPrimaryDuplicates = true }) {
  const missingRows = d.missing_records?.items || d.missing_keys?.items || [];
  const extraRows = d.extra_records?.items || d.extra_keys?.items || [];
  const duplicateSourceRows = d.duplicate_source_records?.items || [];
  const duplicateTargetRows = d.duplicate_target_records?.items || [];
  const duplicateKeyRows = d.duplicate_key_reconciliation?.items || [];
  return <div className="detailsClean">
    <h4>Row reconciliation</h4>
    {showMetrics && <ResultMetricGrid items={[
      ["Source records", m.source_record_count],
      ["Target records", m.target_record_count],
      ["Matched business keys", m.matched_key_count],
      ["Missing in target", m.missing_key_count],
      ["Extra in target", m.extra_key_count],
      ["Duplicate business-key rows in source", m.source_duplicate_key_count || 0],
      ["Duplicate business-key rows in target", m.target_duplicate_key_count || 0],
    ]} />}
    {missingRows.length > 0 && <EvidenceTable title="Missing in Target" rows={missingRows} columns={getDynamicColumns(missingRows, "Missing in target")} />}
    {extraRows.length > 0 && <EvidenceTable title="Extra in Target (Missing in Source)" rows={extraRows} columns={getDynamicColumns(extraRows, "Extra in target")} />}
    {showPrimaryDuplicates && duplicateKeyRows.length > 0 && <EvidenceTable title="Duplicate business-key reconciliation" rows={duplicateKeyRows} columns={duplicateReconciliationColumns} />}
    {showPrimaryDuplicates && !duplicateKeyRows.length && duplicateSourceRows.length > 0 && <EvidenceTable title="Duplicate Business Keys in Source" rows={duplicateSourceRows} columns={duplicateEvidenceColumns(duplicateSourceRows, "source")} />}
    {showPrimaryDuplicates && !duplicateKeyRows.length && duplicateTargetRows.length > 0 && <EvidenceTable title="Duplicate Business Keys in Target" rows={duplicateTargetRows} columns={duplicateEvidenceColumns(duplicateTargetRows, "target")} />}
  </div>;
}

function GroupReconciliationDetails({ level, showMetrics = true }) {
  const m = level.metrics || {};
  const rows = level.differences?.group_reconciliation?.items || level.differences?.group_reconciliation || [];
  const missingBusinessKeys = level.differences?.missing_business_keys?.items || [];
  const aggregateRows = rows.filter(r => ["GROUP_VALUE_MISMATCH", "GROUP_ROW_COUNT_MISMATCH"].includes(r.status));
  const duplicateGroupRows = rows.filter(r => r.status === "GROUP_DUPLICATE_ROWS");
  const notApplicableCount = aggregateRows.filter(r => r.status === "NOT_APPLICABLE").length;
  const applicableChecks = m.aggregate_checks_total ?? aggregateRows.length - notApplicableCount;
  const passedChecks = m.aggregate_checks_passed ?? aggregateRows.filter(r => r.status === "PASS").length;
  const failedChecks = m.aggregate_checks_failed ?? aggregateRows.filter(r => ["GROUP_VALUE_MISMATCH", "GROUP_ROW_COUNT_MISMATCH", "GROUP_DUPLICATE_ROWS"].includes(r.status)).length;
  const groupLabel = (row) => Array.isArray(row.group_key) ? row.group_key.map(value => value === null || value === undefined || value === "" ? "[NULL]" : value).join(" + ") : renderVal(row.group_key);
  const mismatchCount = (m.missing_group_count ?? 0) + (m.extra_group_count ?? 0) + (m.group_mismatch_count ?? 0);

  return <div className="detailsClean">
    {showMetrics && <ResultMetricGrid items={[
      ["Source groups", m.source_group_count],
      ["Target groups", m.target_group_count],
      ["Common groups", m.common_group_count],
      ["Missing groups in target", m.missing_group_count],
      ["Groups with mismatch", m.groups_with_mismatch],
      ["Aggregate field mismatches", m.aggregate_checks_failed],
      ["Applicable checks", applicableChecks],
      ["Passed", passedChecks],
      ["Failed", failedChecks],
      ["Source group coverage", formatMetricPercent(m.source_group_coverage)],
      ["Target group coverage", formatMetricPercent(m.target_group_coverage)]
    ]} />}
    {duplicateGroupRows.length > 0 && <EvidenceTable title="Grouped duplicate reconciliation" rows={duplicateGroupRows}
      columns={[
        { key: "group_key", label: "Matched grouping fields", render: groupLabel },
        { key: "source_aggregate", label: "Source occurrences", render: r => renderVal(r.source_aggregate) },
        { key: "target_aggregate", label: "Target occurrences", render: r => renderVal(r.target_aggregate) },
        { key: "source_record", label: "Source row", render: r => <RowDataLink record={r.source_record} /> },
        { key: "target_record", label: "Target row", render: r => <RowDataLink record={r.target_record} /> },
        { key: "status", label: "Status", render: () => <Status status="FAIL" /> }
      ]} />}
    <EvidenceTable title="Group aggregate mismatches" rows={aggregateRows} count={aggregateRows.length}
      columns={[
        { key: "group_key", label: "Group", render: groupLabel },
        { key: "source_column", label: "Source field" },
        { key: "target_column", label: "Target field" },
        { key: "operation", label: "Aggregation" },
        { key: "source_aggregate", label: "Source value", render: r => renderVal(r.source_aggregate) },
        { key: "target_aggregate", label: "Target value", render: r => renderVal(r.target_aggregate) },
        { key: "difference", label: "Difference", render: r => typeof r.difference === "number" && r.difference > 0 ? `+${formatNumber(r.difference)}` : renderVal(r.difference) },
        { key: "source_record", label: "Source row", render: r => <RowDataLink record={r.source_record} /> },
        { key: "target_record", label: "Target row", render: r => <RowDataLink record={r.target_record} /> },
        { key: "status", label: "Status", render: r => <Status status={r.status === "PASS" ? "PASS" : r.status === "NOT_APPLICABLE" ? "NOT_APPLICABLE" : "FAIL"} /> }
      ]} emptyText="No aggregate mismatches detected." emptySuccess />
    {rows.some(r => ["MISSING_GROUP_IN_TARGET", "EXTRA_GROUP_IN_TARGET"].includes(r.status)) && <EvidenceTable title="Unmatched rows" rows={rows.filter(r => ["MISSING_GROUP_IN_TARGET", "EXTRA_GROUP_IN_TARGET"].includes(r.status))}
      columns={[
        { key: "group_key", label: "Attempted matching attributes", render: groupLabel },
        { key: "status", label: "Issue", render: r => r.status === "MISSING_GROUP_IN_TARGET" ? "UNMATCHED SOURCE ROW" : "UNMATCHED TARGET ROW" },
        { key: "source_record", label: "Source row", render: r => <RowDataLink record={r.source_record} /> },
        { key: "target_record", label: "Target row", render: r => <RowDataLink record={r.target_record} /> },
        { key: "status", label: "Status", render: () => <Status status="FAIL" /> }
      ]} />}
    {missingBusinessKeys.length > 0 && <EvidenceTable title="Missing business keys" rows={missingBusinessKeys}
      columns={[
        { key: "group_key", label: "Matched attributes", render: groupLabel },
        { key: "source_key", label: "Source key", render: r => getRecordKey({ key: r.source_key }) },
        { key: "target_key", label: "Target key", render: r => getRecordKey({ key: r.target_key }) },
        { key: "source_record", label: "Source row", render: r => <RowDataLink record={r.source_record} /> },
        { key: "target_record", label: "Target row", render: r => <RowDataLink record={r.target_record} /> },
        { key: "reason", label: "Interpretation" }
      ]} />}
    {mismatchCount === 0 && <div className="evidenceEmpty success"><Check size={16} /> All compared groups matched.</div>}
  </div>;
}

function L4DetailsClean({ level }) {
  const m = level.metrics || {}, fields = level.differences?.field_mismatches?.items || [];
  const duplicatePairs = level.differences?.duplicate_matched_pairs?.items || [];
  return <div className="detailsClean">
    <ResultMetricGrid items={[
      ["Source records", m.source_record_count], ["Target records", m.target_record_count], ["Matched records", m.matched_record_count],
      ["Compared fields", m.compared_field_count], ["Matched fields", m.matched_field_count],
      ["Field conformity", formatMetricPercent(m.field_conformity_pct)],
      ["Mismatches", m.mismatch_count], ["Field mismatch rate", formatMetricPercent(m.field_mismatch_rate_pct)],
      ["Records with mismatch", m.records_with_mismatch],
      ["Affected record rate", formatMetricPercent(m.affected_record_rate_pct)],
      ["Duplicate business-key rows in source", m.source_duplicate_key_count || 0],
      ["Duplicate business-key rows in target", m.target_duplicate_key_count || 0]
    ]} />
    <EvidenceTable title="Duplicate rows compared by business key" rows={duplicatePairs}
      columns={duplicateReconciliationColumns}
      emptyText="No matched business key had duplicate rows." emptySuccess />
    <EvidenceTable title="Field mismatches" rows={fields}
      columns={[
        {
          key: "key",
          label: "Business key",
          render: r => <CopyableKey text={r.key || r.record_id} />
        },
        {
          key: "match_method",
          label: "Match method",
          render: () => "Business key"
        },
        { key: "source_column", label: "Source column", render: r => <b>{r.source_column}</b> },
        { key: "target_column", label: "Target column", render: r => <b>{r.target_column}</b> },
        { key: "source_value", label: "Source value", render: r => <span className="diffSource">{renderVal(r.source_value)}</span> },
        { key: "target_value", label: "Target value", render: r => <span className="diffTarget">{renderVal(r.target_value)}</span> },
        { key: "comparison_type", label: "Comparison", render: r => formatL4Comparison(r.comparison_type) },
        { key: "difference", label: "Difference", render: r => formatL4Difference(r) },
        { key: "tolerance", label: "Tolerance", render: r => formatL4Tolerance(r) },
        { key: "matched", label: "Status", render: r => <Status status={r.matched ? "PASS" : "FAIL"} /> }
      ]} emptyText="No field mismatches detected." emptySuccess />
  </div>;
}

function formatL4Difference(row) {
  const difference = row?.difference;
  if (typeof difference === "number") {
    return difference > 0 ? `+${formatNumber(difference)}` : formatNumber(difference);
  }
  if (difference !== undefined && difference !== null && difference !== "") return renderVal(difference);
  const sourceNull = row?.source_value === null || row?.source_value === undefined;
  const targetNull = row?.target_value === null || row?.target_value === undefined;
  if (sourceNull !== targetNull) return sourceNull ? "Value added" : "Value removed";
  return "Value changed";
}

function formatL4Comparison(value) {
  const normalized = String(value || "EXACT").toUpperCase().replace(/\s+/g, "_");
  return {
    EXACT: "Exact Match",
    PERCENTAGE_TOLERANCE: "Percentage Tolerance",
    NUMERIC_TOLERANCE: "Numeric Tolerance",
    TIME_TOLERANCE: "Time Tolerance",
    REGEX: "Regex",
  }[normalized] || formatLabel(value || "EXACT");
}

function formatL4Tolerance(row) {
  const tolerance = row?.tolerance;
  const toleranceType = row?.tolerance_type;
  if (tolerance === undefined || tolerance === null || tolerance === "") {
    return "N/A";
  }
  if (toleranceType === "PERCENTAGE" || row?.comparison_type === "PERCENTAGE_TOLERANCE") {
    return `${tolerance}%`;
  }
  if (typeof tolerance === "number") {
    return `±${formatNumber(tolerance)}`;
  }
  const numericTolerance = Number(tolerance);
  if (!Number.isNaN(numericTolerance)) {
    return `±${formatNumber(numericTolerance)}`;
  }
  return renderVal(tolerance);
}

function formatTolerance(tol) {
  if (!tol) return "No tolerance";
  if (typeof tol === "string") return tol;

  let abs = tol.absolute !== undefined ? tol.absolute : tol;
  let perc = tol.percentage;

  if ((abs === null || abs === undefined) && (perc === null || perc === undefined)) return "No tolerance";

  const parts = [];
  if (abs !== null && abs !== undefined && typeof abs === "number") parts.push(`${formatNumber(abs)} absolute`);
  if (perc !== null && perc !== undefined) parts.push(`${formatNumber(perc, true)}`);

  return parts.join(" / ") || String(tol);
}

function formatNumber(val, isPercentage = false) {
  if (val === null || val === undefined) return "—";
  let num = Number(val);
  if (isNaN(num)) return String(val);
  if (isPercentage) return Number(num.toFixed(2)) + "%";
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(num);
}

function L5DetailsClean({ level }) {
  const m = level.metrics || {}, rawRules = level.differences?.aggregate_results?.items || [];
  const rules = rawRules.flatMap(rule =>
    rule.grouped && Array.isArray(rule.group_results) && rule.group_results.length
      ? rule.group_results
      : [rule]
  );
  const hasGroupedResults = rules.some(r => r.group !== null && r.group !== undefined);

  let nullNote = null;
  if (rules.length === 1) {
    const r = rules[0];
    const sNulls = r.source_null_count || 0;
    const tNulls = r.target_null_count || 0;
    if (sNulls > 0 && tNulls > 0) {
      nullNote = `Source: ${sNulls} null${sNulls > 1 ? "s" : ""} ignored · Target: ${tNulls} null${tNulls > 1 ? "s" : ""} ignored`;
    } else if (sNulls > 0) {
      nullNote = `${sNulls} blank/null value${sNulls > 1 ? "s were" : " was"} ignored during the ${r.operation} calculation.`;
    } else if (tNulls > 0) {
      nullNote = `${tNulls} blank/null value${tNulls > 1 ? "s were" : " was"} ignored during the ${r.operation} calculation.`;
    }
  } else if (rules.length > 1) {
    let sNulls = 0; let tNulls = 0;
    rules.forEach(r => { sNulls += (r.source_null_count || 0); tNulls += (r.target_null_count || 0); });
    if (sNulls > 0 && tNulls > 0) {
      nullNote = `Source: ${sNulls} nulls ignored · Target: ${tNulls} nulls ignored`;
    } else if (sNulls > 0) {
      nullNote = `${sNulls} blank/null values were ignored across source aggregations.`;
    } else if (tNulls > 0) {
      nullNote = `${tNulls} blank/null values were ignored across target aggregations.`;
    }
  }

  return <div className="detailsClean">
    <ResultMetricGrid items={[
      ["Rules", m.rules_total],
      ["Checks", m.checks_total],
      ["Passed", m.checks_passed],
      ["Failed", m.checks_failed],
      ["Aggregate pass rate", formatMetricPercent(m.aggregate_check_pass_rate_pct)],
      ["Aggregate failure rate", formatMetricPercent(m.aggregate_check_failure_rate_pct)]
    ]} />
    <EvidenceTable title="Aggregate validation" rows={rules} description={nullNote}
      columns={[
        { key: "rule_name", label: "Rule", render: r => <b>{formatLabel(r.rule_name || r.rule_id)}</b> },
        ...(hasGroupedResults ? [{ key: "group", label: "Group", render: r => renderVal(r.group) }] : []),
        { key: "operation", label: "Function", render: r => `${r.operation || "—"}${r.source_column ? `(${r.source_column})` : ""}` },
        { key: "source", label: "Source", render: r => r.error ? <span className="failText" title={r.error}><X size={12} /> ERROR</span> : formatNumber(r.source) },
        { key: "target", label: "Target", render: r => r.error ? <span className="failText" title={r.error}><X size={12} /> ERROR</span> : formatNumber(r.target) },
        { key: "difference", label: "Difference", render: r => r.error ? <span className="failText" style={{ fontSize: "10px" }}>{r.error}</span> : formatNumber(r.difference) },
        { key: "tolerance", label: "Tolerance", render: r => r.error ? "—" : formatTolerance(r.tolerance) },
        { key: "matched", label: "Status", render: r => <Status status={r.matched ? "PASS" : "FAIL"} /> }
      ]} emptyText="No aggregate results returned." />
  </div>;
}

function L6DetailsClean({ level }) {
  const m = level.metrics || {}, results = level.differences?.dq_results?.items || [];

  const failedRecords = [];
  results.forEach(r => {
    if (r.source_failed_records) r.source_failed_records.forEach(fr => failedRecords.push({ ...fr, _found_in: "SOURCE" }));
    if (r.target_failed_records) r.target_failed_records.forEach(fr => failedRecords.push({ ...fr, _found_in: "TARGET" }));
  });

  return <div className="detailsClean">
    <ResultMetricGrid items={[
      ["Rules", m.rules_total], ["Checks", m.checks_total], ["Passed", m.checks_passed], ["Failed", m.checks_failed],
      ["Pass rate", formatMetricPercent(m.pass_percentage)],
      ["Failure rate", formatMetricPercent(m.failure_percentage)]
    ]} />
    <EvidenceTable title="Failed DQ Records" rows={failedRecords}
      columns={[
        {
          key: "business_key", label: "Business Key", render: r => (
            <CopyableKey text={r.business_key ?? "Not available"} />
          )
        },
        { key: "column", label: "Column", render: r => <b>{r.column}</b> },
        { key: "value", label: "Value", render: r => <span className="diffSource">{renderVal(r.value)}</span> },
        { key: "rule", label: "Rule", render: r => formatLabel(r.rule?.name || r.rule?.rule_id || "Unknown") },
        { key: "reason", label: "Failure Reason", render: r => r.reason },
        { key: "found_in", label: "Found In", render: r => r._found_in || "N/A" },
        { key: "status", label: "Status", render: r => <Status status={r.status || "FAIL"} /> }
      ]} emptyText="No failed data-quality records detected." emptySuccess />
  </div>;
}


function L7DetailsClean({ level }) {
  const m = level.metrics || {};
  const d = level.differences || {};
  const findings = d.findings || [];
  const recommendations = d.recommendations || [];

  return <div className="detailsClean triageDetails">
    <div className="triageSummary">
      <div className="triageSummaryIcon"><TriangleAlert size={18} /></div>
      <div>
        <span className="triageLabel">COMPARISON SUMMARY</span>
        <p>{d.root_cause_summary || "No triage summary available."}</p>
      </div>
    </div>

    <ResultMetricGrid items={[
      ["Findings", m.findings_count ?? findings.length],
      ["Recommendations", m.recommendations_count ?? recommendations.length],
      ["Review status", level.status || "REVIEW"]
    ]} />

    <EvidenceTable title="Likely causes" rows={findings} columns={[
      { key: "category", label: "Issue", render: r => <b>{r.category}</b> },
      { key: "severity", label: "Priority", render: r => <span className={`triageSeverity ${String(r.severity || "MEDIUM").toLowerCase()}`}>{r.severity || "MEDIUM"}</span> },
      { key: "summary", label: "What was detected", render: r => r.summary },
      { key: "likely_cause", label: "Likely cause", render: r => r.likely_cause },
      { key: "related_levels", label: "Evidence", render: r => (r.related_levels || []).join(", ") }
    ]} emptyText="No root-cause findings." />

    <EvidenceTable title="Recommended actions" rows={recommendations.map((text, i) => ({ id: i + 1, action: text }))} columns={[
      { key: "id", label: "#", render: r => <b>{r.id}</b> },
      { key: "action", label: "Recommended action", render: r => r.action }
    ]} emptyText="No remediation recommendations." />
  </div>;
}

function CopyableKey({ text }) {
  if (text === undefined || text === null) return "Not available";
  const full = String(text);
  if (full.length <= 20) return <span className="keyValue">{full}</span>;
  return <button type="button" className="keyValue keyButton" title="Copy full key" onClick={() => navigator.clipboard.writeText(full)}>
    {full.slice(0, 10)}…{full.slice(-8)}
  </button>;
}

function PaginatedTable({ data, renderHeader, renderRow, pageSize = 50, label = "items" }) {
  const [page, setPage] = useState(1);
  useEffect(() => setPage(1), [data]);
  if (!data || !data.length) return null;
  const totalPages = Math.max(1, Math.ceil(data.length / pageSize));
  const safePage = Math.min(page, totalPages);
  const start = (safePage - 1) * pageSize;
  const visible = data.slice(start, start + pageSize);
  return <div className="evidenceTableWrap">
    <div className="evidenceTableScroll">
      <table className="evidenceTable"><thead><tr>{renderHeader()}</tr></thead><tbody>{visible.map(renderRow)}</tbody></table>
    </div>
    <div className="evidencePagination">
      <span>Showing {start + 1}–{Math.min(start + visible.length, data.length)} of {data.length} {label}</span>
      {totalPages > 1 && <div>
        <button className="pageBtn" disabled={safePage === 1} onClick={() => setPage(safePage - 1)}>Previous</button>
        <span className="pageNumber">Page {safePage} of {totalPages}</span>
        <button className="pageBtn" disabled={safePage === totalPages} onClick={() => setPage(safePage + 1)}>Next</button>
      </div>}
    </div>
  </div>;
}

function RawEvidenceModal({ data, onClose }) {
  const bounded = {
    ...data,
    levels: (data?.levels || []).map(level => ({
      ...level,
      differences: Object.fromEntries(Object.entries(level.differences || {}).map(([key, value]) => {
        if (value && typeof value === "object" && Array.isArray(value.items)) {
          return [key, { ...value, items: value.items.slice(0, 25), truncated: value.truncated || value.items.length > 25 }];
        }
        if (Array.isArray(value)) return [key, { count: value.length, items: value.slice(0, 25), truncated: value.length > 25 }];
        return [key, value];
      }))
    }))
  };
  const json = JSON.stringify(bounded, null, 2);
  return <div className="modalBackdrop" onClick={onClose}>
    <div className="modal rawEvidenceModal" onClick={e => e.stopPropagation()}>
      <div className="modalHead">
        <div><span className="sectionEyebrow">TECHNICAL EVIDENCE</span><h3>Raw comparison response</h3></div>
        <div className="actionRow">
          <button className="secondary small" onClick={() => navigator.clipboard.writeText(json)}>Copy JSON</button>
          <button className="iconButton" onClick={onClose}><X size={16} /></button>
        </div>
      </div>
      <div className="rawEvidenceBody"><pre>{json}</pre></div>
    </div>
  </div>;
}
