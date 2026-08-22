import React, { useEffect, useMemo, useState } from "react";
import { Activity, ChevronDown, Loader2, RefreshCw, Trash2 } from "lucide-react";

import { apiRequest } from "../../api/client";
import { Empty, Loading, Panel, Status } from "../../components/ui";

function formatDuration(startedAt, finishedAt) {
  if (!startedAt || !finishedAt) return "—";
  const started = new Date(startedAt).getTime();
  const finished = new Date(finishedAt).getTime();
  if (!Number.isFinite(started) || !Number.isFinite(finished) || finished < started) return "—";

  const totalMs = finished - started;
  if (totalMs < 1000) return `${totalMs} ms`;

  const totalSeconds = totalMs / 1000;
  if (totalSeconds < 60) return `${totalSeconds.toFixed(totalSeconds < 10 ? 1 : 0)} s`;

  const minutes = Math.floor(totalSeconds / 60);
  const seconds = Math.round(totalSeconds % 60);
  return `${minutes}m ${seconds}s`;
}

function configurationName(configurationId, configurationMap) {
  if (configurationId === undefined || configurationId === null) return "Unlinked comparison";
  return configurationMap.get(String(configurationId)) || `Comparison ${configurationId}`;
}

function HeaderFilter({ label, value, setValue, options }) {
  return (
    <span className="historyHeaderFilter">
      <span>{label}</span>
      <span className="historyFilterSelectWrap">
        <select
          aria-label={`Filter by ${label}`}
          value={value}
          onChange={(event) => setValue(event.target.value)}
          onClick={(event) => event.stopPropagation()}
        >
          <option value="ALL">All</option>
          {options.map((option) => (
            <option key={option} value={option}>{option}</option>
          ))}
        </select>
        <ChevronDown size={9} aria-hidden="true" />
      </span>
    </span>
  );
}

export function History({ onOpenRun, notify }) {
  const [runs, setRuns] = useState([]);
  const [configurationMap, setConfigurationMap] = useState(new Map());
  const [loading, setLoading] = useState(false);
  const [deletingRunId, setDeletingRunId] = useState(null);
  const [statusFilter, setStatusFilter] = useState("ALL");
  const [resultFilter, setResultFilter] = useState("ALL");
  const [page, setPage] = useState(1);
  const pageSize = 12;

  async function loadRuns() {
    setLoading(true);
    try {
      const [runData, configurationData] = await Promise.all([
        apiRequest("/comparisons", { method: "GET" }),
        apiRequest("/configurations", { method: "GET" }).catch(() => []),
      ]);

      const normalized = Array.isArray(runData) ? runData : [];
      normalized.sort((a, b) => {
        const aTime = Date.parse(a?.created_at || a?.started_at || "") || 0;
        const bTime = Date.parse(b?.created_at || b?.started_at || "") || 0;
        return bTime - aTime;
      });

      const names = new Map();
      (Array.isArray(configurationData) ? configurationData : []).forEach((item) => {
        names.set(String(item.configuration_id), item.name || `Comparison ${item.configuration_id}`);
      });

      setRuns(normalized);
      setConfigurationMap(names);
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

  useEffect(() => {
    setPage(1);
  }, [statusFilter, resultFilter]);

  async function deleteRun(runId, event) {
    event.stopPropagation();
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

  const statusOptions = useMemo(
    () => Array.from(new Set(runs.map((run) => String(run.status || "UNKNOWN").toUpperCase()))).sort(),
    [runs]
  );

  const resultOptions = useMemo(
    () => Array.from(new Set(runs.map((run) => String(run.comparison_status || "UNKNOWN").toUpperCase()))).sort(),
    [runs]
  );

  const filteredRuns = useMemo(() => runs.filter((run) => {
    const runStatus = String(run.status || "UNKNOWN").toUpperCase();
    const resultStatus = String(run.comparison_status || "UNKNOWN").toUpperCase();
    return (statusFilter === "ALL" || runStatus === statusFilter)
      && (resultFilter === "ALL" || resultStatus === resultFilter);
  }), [runs, statusFilter, resultFilter]);

  if (loading && !runs.length) return <Loading text="Loading history…" />;

  const totalPages = Math.max(1, Math.ceil(filteredRuns.length / pageSize));
  const safePage = Math.min(page, totalPages);
  const pageRuns = filteredRuns.slice((safePage - 1) * pageSize, safePage * pageSize);

  return (
    <div className="recentComparisonsPage">
      <header className="recentComparisonsHeader">
        <h1>Recent comparisons</h1>
        <button type="button" className="secondary small" onClick={loadRuns}>
          <RefreshCw size={14} className={loading ? "spin" : ""} /> Refresh
        </button>
      </header>

      <Panel className="recentComparisonsPanel">
        {!runs.length ? (
          <Empty
            icon={Activity}
            title="No history found"
            text="Run your first comparison to see it here."
          />
        ) : (
          <div className="recentComparisonsTableShell">
            <div className="recentComparisonsRowsScroll">
              <table className="dataTable recentComparisonsTable">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Run ID</th>
                    <th><HeaderFilter label="Status" value={statusFilter} setValue={setStatusFilter} options={statusOptions} /></th>
                    <th><HeaderFilter label="Result" value={resultFilter} setValue={setResultFilter} options={resultOptions} /></th>
                    <th>Duration</th>
                    <th>Started</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {pageRuns.map((run) => {
                    const runId = run?.run_id;
                    const startedValue = run?.started_at || run?.created_at;
                    const startedAt = startedValue ? new Date(startedValue) : null;
                    const startedLabel = startedAt && !Number.isNaN(startedAt.getTime())
                      ? startedAt.toLocaleString()
                      : "Not available";
                    const name = configurationName(run?.configuration_id, configurationMap);

                    return (
                      <tr key={runId} onClick={() => onOpenRun(runId)} className="clickable">
                        <td className="comparisonNameCell" title={name}><b>{name}</b></td>
                        <td className="codeCell" title={runId}>{runId || "Unknown run"}</td>
                        <td><Status status={run.status} /></td>
                        <td><Status status={run.comparison_status} /></td>
                        <td className="durationCell">{formatDuration(run?.started_at, run?.finished_at)}</td>
                        <td>{startedLabel}</td>
                        <td>
                          <button
                            type="button"
                            className="iconButton dangerIcon"
                            onClick={(event) => deleteRun(runId, event)}
                            title="Delete Run"
                            disabled={!runId || deletingRunId === runId}
                          >
                            {deletingRunId === runId ? <Loader2 size={15} className="spin" /> : <Trash2 size={15} />}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <footer className="tablePagination recentComparisonsPagination">
              <span>
                {filteredRuns.length
                  ? `Showing ${(safePage - 1) * pageSize + 1}–${Math.min(safePage * pageSize, filteredRuns.length)} of ${filteredRuns.length} runs`
                  : "No runs match the selected filters"}
              </span>
              {totalPages > 1 && (
                <div className="tablePaginationActions">
                  <button type="button" className="pageBtn" disabled={safePage === 1} onClick={() => setPage(safePage - 1)}>Previous</button>
                  <span>Page {safePage} of {totalPages}</span>
                  <button type="button" className="pageBtn" disabled={safePage === totalPages} onClick={() => setPage(safePage + 1)}>Next</button>
                </div>
              )}
            </footer>
          </div>
        )}
      </Panel>
    </div>
  );
}
