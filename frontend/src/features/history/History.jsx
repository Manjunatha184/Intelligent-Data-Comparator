import React, { useEffect, useState } from "react";
import { Activity, Loader2, RefreshCw, Trash2 } from "lucide-react";

import { apiRequest } from "../../api/client";
import { Empty, Loading, Panel, Status } from "../../components/ui";

/* ============================================================
   HISTORY
============================================================ */

export function History({ onOpenRun, notify }) {
  const [runs, setRuns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [deletingRunId, setDeletingRunId] = useState(null);
  const [page, setPage] = useState(1);
  const pageSize = 12;

  async function loadRuns() {
    setLoading(true);
    try {
      const data = await apiRequest("/comparisons", { method: "GET" });
      const normalized = Array.isArray(data) ? data : [];
      normalized.sort((a, b) => {
        const aTime = Date.parse(a?.created_at || "") || 0;
        const bTime = Date.parse(b?.created_at || "") || 0;
        return bTime - aTime;
      });
      setRuns(normalized);
      setPage(1);
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadRuns();
  }, []);

  async function deleteRun(runId, e) {
    e.stopPropagation();
    if (!runId) return;
    if (!window.confirm("Are you sure you want to delete this comparison run?")) return;

    setDeletingRunId(runId);
    try {
      await apiRequest(`/comparisons/${runId}`, { method: "DELETE" });
      notify("Run deleted successfully");
      await loadRuns();
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setDeletingRunId(null);
    }
  }

  if (loading && !runs.length) return <Loading text="Loading history…" />;

  const totalPages = Math.max(1, Math.ceil(runs.length / pageSize));
  const safePage = Math.min(page, totalPages);
  const pageRuns = runs.slice((safePage - 1) * pageSize, safePage * pageSize);

  return (
    <div className="stack historyPage">
      <div className="wizardFooter">
        <h1 className="pageTitle" style={{ margin: 0 }}>Recent comparisons</h1>
        <button type="button" className="secondary small" onClick={loadRuns}>
          <RefreshCw size={14} className={loading ? "spin" : ""} /> Refresh
        </button>
      </div>
      <Panel>
        {!runs.length ? (
          <Empty
            icon={Activity}
            title="No history found"
            text="Run your first comparison to see it here."
          />
        ) : (
          <div className="tableWrapper">
            <table className="dataTable">
              <thead>
                <tr>
                  <th>Run ID</th>
                  <th>Status</th>
                  <th>Result</th>
                  <th>Started</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {pageRuns.map((r) => {
                  const runId = r?.run_id;
                  const createdAt = r?.created_at ? new Date(r.created_at) : null;
                  const createdLabel = createdAt && !Number.isNaN(createdAt.getTime())
                    ? createdAt.toLocaleString()
                    : "Not available";

                  return (
                    <tr
                      key={runId}
                      onClick={() => onOpenRun(runId)}
                      className="clickable"
                    >
                      <td className="codeCell" title={runId}>
                        {runId || "Unknown run"}
                      </td>
                      <td><Status status={r.status} /></td>
                      <td><Status status={r.comparison_status} /></td>
                      <td>{createdLabel}</td>
                      <td>
                        <button
                          type="button"
                          className="iconButton dangerIcon"
                          onClick={(e) => deleteRun(runId, e)}
                          title="Delete Run"
                          disabled={!runId || deletingRunId === runId}
                        >
                          {deletingRunId === runId ? (
                            <Loader2 size={15} className="spin" />
                          ) : (
                            <Trash2 size={15} />
                          )}
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            <div className="tablePagination">
              <span>Showing {(safePage - 1) * pageSize + 1}–{Math.min(safePage * pageSize, runs.length)} of {runs.length} runs</span>
              {totalPages > 1 && (
                <div className="tablePaginationActions">
                  <button type="button" className="pageBtn" disabled={safePage === 1} onClick={() => setPage(safePage - 1)}>Previous</button>
                  <span>Page {safePage} of {totalPages}</span>
                  <button type="button" className="pageBtn" disabled={safePage === totalPages} onClick={() => setPage(safePage + 1)}>Next</button>
                </div>
              )}
            </div>
          </div>
        )}
      </Panel>
    </div>
  );
}
