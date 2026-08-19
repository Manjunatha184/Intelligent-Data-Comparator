import React, { useEffect, useState } from "react";
import { API_BASE, apiRequest } from "../api/client.js";
import Loading from "../components/ui/Loading.jsx";
import { L7AnalysisReportView } from "./Results.jsx";

export default function AnalysisPage({ runId, onBack, notify }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!runId) return;
    setLoading(true);
    apiRequest(`/comparisons/${runId}/results`, { method: "GET" })
      .then(setData)
      .catch(err => notify(err.message, "error"))
      .finally(() => setLoading(false));
  }, [runId, notify]);

  if (loading || !data) return <Loading text="Loading analysis report…" />;

  if (!data.analysis) return (
    <div className="empty" style={{ margin: "40px" }}>
      <b>No analysis report available for this run.</b>
      <button className="primary" onClick={onBack} style={{ marginTop: "20px" }}>Back to results</button>
    </div>
  );

  if (data.analysis?.error) return (
    <div className="empty" style={{ margin: "40px" }}>
      <b>Analysis report generation failed.</b>
      <p style={{ marginTop: "12px" }}>{data.analysis.error}</p>
      <button className="primary" onClick={onBack} style={{ marginTop: "20px" }}>Back to results</button>
    </div>
  );

  return (
    <div className="resultsPage" style={{ overflowY: "auto", height: "100%" }}>
      <L7AnalysisReportView
        report={data.analysis}
        runId={runId}
        onBack={onBack}
        onDownload={() => {
          window.open(
            `${API_BASE}/comparisons/${encodeURIComponent(runId)}/analysis/pdf`,
            "_blank",
            "noopener,noreferrer"
          );
        }}
      />
    </div>
  );
}
