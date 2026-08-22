import React, { useEffect, useState } from "react";
import { Activity, ArrowRight, ChevronRight, Database, GitCompare, LayoutDashboard, Link2, Plus, Search, SlidersHorizontal, Zap } from "lucide-react";

import { API_BASE, apiRequest } from "./api/client";
import { ConnectionLine, Empty, Loading, Metric, Panel, Toast } from "./components/ui";
import { ComparisonBuilder } from "./features/comparisons/ComparisonBuilder";
import { SavedComparisons } from "./features/comparisons/SavedComparisons";
import { ConnectionManager, ConnectionModal } from "./features/connections/Connections";
import { L7AnalysisReportView } from "./features/results/Results";
import { ResultsConsole } from "./features/results/ResultsConsole";
import { RulesPage } from "./features/rules/RulesPage";

const LOCAL_COMPARISON_DRAFT_KEY = "lumera.comparison.workspace.v1";

function AnalysisPage({ runId, onBack, notify }) {
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
    <div className="resultsPage analysisResultsPage">
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

export default function App() {
  const [page, setPage] = useState(() => {
    const p = window.location.pathname;
    if (p.match(/^\/results\/([^/]+)\/analysis$/)) return "analysis";
    return "dashboard";
  });
  const [comparisonBuilderKey, setComparisonBuilderKey] = useState(0);
  const [connections, setConnections] = useState([]);
  const [loadingConnections, setLoadingConnections] = useState(false);
  const [connectionsError, setConnectionsError] = useState(null);
  const connectionsRequestId = React.useRef(0);
  const [connectionModalOpen, setConnectionModalOpen] = useState(false);
  const [activeRunId, setActiveRunId] = useState(() => {
    const match = window.location.pathname.match(/^\/results\/([^/]+)\/analysis$/);
    return match ? match[1] : null;
  });

  useEffect(() => {
    const onPopState = () => {
      const p = window.location.pathname;
      const match = p.match(/^\/results\/([^/]+)\/analysis$/);
      if (match) {
        setActiveRunId(match[1]);
        setPage("analysis");
      } else {
        setPage("dashboard");
      }
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    const isAnalysisUrl = /^\/results\/[^/]+\/analysis$/.test(window.location.pathname);
    if (page !== "analysis" && isAnalysisUrl) {
      window.history.replaceState(null, "", "/");
    }
  }, [page]);

  const [toast, setToast] = useState(null);

  function notify(message, type = "success") {
    setToast({ message, type });
    window.clearTimeout(window.__v1ToastTimer);
    window.__v1ToastTimer = window.setTimeout(() => setToast(null), 3500);
  }

  async function loadConnections() {
    const requestId = ++connectionsRequestId.current;
    setLoadingConnections(true);
    setConnectionsError(null);
    try {
      const data = await apiRequest("/connections");
      if (requestId !== connectionsRequestId.current) return;
      if (!Array.isArray(data)) throw new Error("Invalid connection list response.");
      setConnections(data);
    } catch (error) {
      if (requestId !== connectionsRequestId.current) return;
      setConnectionsError(error.message || "Unable to load connections");
      notify(error.message, "error");
    } finally {
      if (requestId === connectionsRequestId.current) setLoadingConnections(false);
    }
  }

  useEffect(() => { loadConnections(); }, []);
  useEffect(() => { if (page === "comparison-builder") loadConnections(); }, [page]);

  function handleComparisonComplete(runId) {
    setActiveRunId(runId);
    setPage("results");
  }

  function openResultsHistory() {
    setActiveRunId(null);
    setPage("results");
  }

  function backToResultsHistory() {
    setActiveRunId(null);
    setPage("results");
    window.history.pushState(null, "", "/");
  }

  function openAnalysis(runId) {
    setActiveRunId(runId);
    setPage("analysis");
    window.history.pushState(null, "", `/results/${runId}/analysis`);
  }

  function startNewComparison() {
    window.localStorage.removeItem(LOCAL_COMPARISON_DRAFT_KEY);
    setComparisonBuilderKey((current) => current + 1);
    setPage("comparison-builder");
  }

  function editComparison() {
    setComparisonBuilderKey((current) => current + 1);
    setPage("comparison-builder");
  }

  return (
    <div className="app">
      <TopNav onOpenRun={handleComparisonComplete} notify={notify} />
      <div className="appBody">
        <Sidebar page={page} setPage={setPage} onOpenResultsHistory={openResultsHistory} />

        <main className="main">
          <div className="body">
            {page === "dashboard" && (
              <Dashboard
                connections={connections}
                onNewComparison={startNewComparison}
                onConnections={() => setPage("connections")}
              />
            )}

            {page === "connections" && (
              <ConnectionManager
                connections={connections}
                loading={loadingConnections}
                reload={loadConnections}
                onAdd={() => setConnectionModalOpen(true)}
                notify={notify}
              />
            )}

            {page === "comparisons" && (
              <SavedComparisons
                onNew={startNewComparison}
                onEdit={editComparison}
                onRunComplete={handleComparisonComplete}
                notify={notify}
              />
            )}

            {page === "comparison-builder" && (
              <ComparisonBuilder
                key={comparisonBuilderKey}
                connections={connections}
                notify={notify}
                connectionsLoading={loadingConnections}
                connectionsError={connectionsError}
                reloadConnections={loadConnections}
                onComplete={handleComparisonComplete}
              />
            )}

            {page === "results" && (
              <ResultsConsole
                runId={activeRunId}
                onOpenRun={handleComparisonComplete}
                onBack={backToResultsHistory}
                onOpenAnalysis={openAnalysis}
                notify={notify}
              />
            )}

            {page === "analysis" && (
              <AnalysisPage
                runId={activeRunId}
                onBack={() => {
                  setPage("results");
                  window.history.pushState(null, "", "/");
                }}
                notify={notify}
              />
            )}

            {page === "rules" && <RulesPage notify={notify} />}
          </div>
        </main>
      </div>

      {connectionModalOpen && (
        <ConnectionModal
          onClose={() => setConnectionModalOpen(false)}
          onDone={() => {
            setConnectionModalOpen(false);
            loadConnections();
          }}
          notify={notify}
        />
      )}

      {toast && <Toast message={toast.message} type={toast.type} onClose={() => setToast(null)} />}
      <Footer />
    </div>
  );
}

function Footer() {
  return (
    <footer className="globalFooter">
      <div className="footerLeft">Lumera Corporation © 2026</div>
      <div className="footerRight"><span>Terms of Service</span><span>Privacy Policy</span><span>v1.0.0</span></div>
    </footer>
  );
}

function TopNav({ onOpenRun, notify }) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [searching, setSearching] = useState(false);
  const [open, setOpen] = useState(false);

  async function searchComparisons(value) {
    const needle = value.trim().toLowerCase();
    setQuery(value);
    if (!needle) {
      setResults([]);
      setOpen(false);
      return;
    }

    setSearching(true);
    try {
      const [runs, configurations] = await Promise.all([
        apiRequest("/comparisons", { method: "GET" }),
        apiRequest("/configurations", { method: "GET" }).catch(() => []),
      ]);
      const names = new Map((Array.isArray(configurations) ? configurations : []).map((item) => [
        String(item.configuration_id),
        item.name || `Comparison ${item.configuration_id}`,
      ]));

      const matches = (Array.isArray(runs) ? runs : []).filter((run) => {
        const name = names.get(String(run.configuration_id)) || `Comparison ${run.configuration_id}`;
        return String(run.run_id || "").toLowerCase().includes(needle)
          || name.toLowerCase().includes(needle);
      }).slice(0, 8).map((run) => ({
        ...run,
        comparisonName: names.get(String(run.configuration_id)) || `Comparison ${run.configuration_id}`,
      }));

      setResults(matches);
      setOpen(true);
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setSearching(false);
    }
  }

  function chooseResult(runId) {
    setOpen(false);
    setQuery("");
    onOpenRun(runId);
  }

  return (
    <header className="topnav">
      <div className="topnavBrand">
        <div className="brandLogoIcon"><span>L</span></div>
        <div className="brandName">Lumera</div>
        <div className="brandDivider">|</div>
        <div className="brandApp">VALIDATION<br/>CONSOLE</div>
      </div>
      <div className="topnavSearchSlot">
        <div className="topnavSearch">
          <Search size={13} />
          <input
            value={query}
            onChange={(event) => searchComparisons(event.target.value)}
            onFocus={() => query.trim() && setOpen(true)}
            placeholder="Search comparison name or Run ID"
            aria-label="Search comparisons"
          />
          {searching && <span className="topnavSearchBusy">…</span>}
          {open && (
            <div className="topnavSearchResults">
              {results.length ? results.map((result) => (
                <button key={result.run_id} type="button" onMouseDown={() => chooseResult(result.run_id)}>
                  <span><b>{result.comparisonName}</b><small>{result.run_id}</small></span>
                  <StatusDot value={result.comparison_status || result.status} />
                </button>
              )) : (
                <div className="topnavSearchEmpty">No matching comparison runs</div>
              )}
            </div>
          )}
        </div>
      </div>
      <div className="topnavRight">
        <span className="workspaceLabel">workspace / prod-data</span>
      </div>
    </header>
  );
}

function StatusDot({ value }) {
  const normalized = String(value || "UNKNOWN").toLowerCase();
  return <i className={`globalSearchStatus ${normalized}`} title={value || "Unknown"} />;
}

function Sidebar({ page, setPage, onOpenResultsHistory }) {
  return (
    <aside className="sidebar">
      <div className="workspace">WORKSPACE</div>
      <NavigationItem icon={LayoutDashboard} label="Overview" active={page === "dashboard"} onClick={() => setPage("dashboard")} />
      <NavigationItem icon={GitCompare} label="Comparisons" active={page === "comparisons" || page === "comparison-builder"} onClick={() => setPage("comparisons")} />
      <NavigationItem icon={Activity} label="Results" active={page === "results"} onClick={onOpenResultsHistory} />
      <div className="workspace">CONFIGURATION</div>
      <NavigationItem icon={Link2} label="Connection Manager" active={page === "connections"} onClick={() => setPage("connections")} />
      <NavigationItem icon={SlidersHorizontal} label="Rule Repository" active={page === "rules"} onClick={() => setPage("rules")} />
    </aside>
  );
}

function NavigationItem({ icon: Icon, label, active, disabled, onClick }) {
  return (
    <button className={`nav ${active ? "active" : ""}`} disabled={disabled} onClick={onClick}>
      <Icon size={17} /><span>{label}</span>{active && <ChevronRight size={14} />}
    </button>
  );
}

function Dashboard({ connections, onNewComparison, onConnections }) {
  const connectedCount = connections.filter((connection) => connection.status === "CONNECTED").length;
  return (
    <div className="stack">
      <div className="dashboardIntro">
        <div><span className="sectionEyebrow">DATA QUALITY WORKSPACE</span><h1>Comparison control center</h1><p>Monitor connections, configure validation, and review evidence.</p></div>
        <button className="primary" onClick={onNewComparison}><Plus size={15} /> New comparison</button>
      </div>
      <div className="stats">
        <Metric label="Authenticated connections" value={connections.length} sub={`${connectedCount} currently connected`} icon={Link2} />
        <Metric label="Comparison levels" value="6" sub="L1 Schema → L6 DQ" icon={GitCompare} />
        <Metric label="Connector types" value="2" sub="CSV · Databricks" icon={Database} />
        <Metric label="Execution engine" value="Ready" sub="Planner + task execution" icon={Zap} />
      </div>
      <div className="grid2">
        <Panel title="How the platform works">
          <div className="steps">
            {[["01", "Connect", "Save and test reusable source connections."], ["02", "Configure", "Choose sources, keys, levels and rules."], ["03", "Execute", "Planner creates only the tasks you selected."], ["04", "Review", "Inspect metrics and comparison evidence."]].map(([number, title, description]) => (
              <div className="step" key={number}><b>{number}</b><div><strong>{title}</strong><span>{description}</span></div><ArrowRight size={15} /></div>
            ))}
          </div>
        </Panel>
        <Panel title="Connection health" action={<button className="textBtn" onClick={onConnections}>Open manager</button>}>
          {connections.length === 0 ? <Empty icon={Link2} title="No connections yet" text="Add a source to start building comparisons." /> : connections.slice(0, 5).map((connection) => <ConnectionLine key={connection.connection_id} connection={connection} />)}
        </Panel>
      </div>
    </div>
  );
}