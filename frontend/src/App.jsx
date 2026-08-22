import React, { useEffect, useState } from "react";
import { Activity, ArrowRight, ChevronRight, Database, GitCompare, LayoutDashboard, Link2, Plus, SlidersHorizontal, Zap } from "lucide-react";

import { API_BASE, apiRequest } from "./api/client";
import { ConnectionLine, Empty, Loading, Metric, Panel, Toast } from "./components/ui";
import { ComparisonBuilder } from "./features/comparisons/ComparisonBuilder";
import { ConnectionManager, ConnectionModal } from "./features/connections/Connections";
import { L7AnalysisReportView } from "./features/results/Results";
import { ResultsConsole } from "./features/results/ResultsConsole";
import { RulesPage } from "./features/rules/RulesPage";

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

/* ============================================================
   APPLICATION
============================================================ */

export default function App() {
  const [page, setPage] = useState(() => {
    const p = window.location.pathname;
    if (p.match(/^\/results\/([^/]+)\/analysis$/)) return "analysis";
    return "dashboard";
  });

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

  const [toast, setToast] = useState(null);

  function notify(message, type = "success") {
    setToast({
      message,
      type,
    });

    window.clearTimeout(window.__v1ToastTimer);

    window.__v1ToastTimer = window.setTimeout(() => {
      setToast(null);
    }, 3500);
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

  useEffect(() => {
    loadConnections();
  }, []);

  useEffect(() => {
    if (page === "comparison") loadConnections();
  }, [page]);

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

  return (
    <div className="app">
      <Sidebar
        page={page}
        setPage={setPage}
        hasResults={Boolean(activeRunId)}
        onOpenResultsHistory={openResultsHistory}
      />

      <main className="main">
        <div className="body">
          {page === "dashboard" && (
            <Dashboard
              connections={connections}
              onNewComparison={() => setPage("comparison")}
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

          {page === "comparison" && (
            <ComparisonBuilder
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

      {toast && (
        <Toast
          message={toast.message}
          type={toast.type}
          onClose={() => setToast(null)}
        />
      )}
    </div>
  );
}

/* ============================================================
   SIDEBAR
============================================================ */

function Sidebar({ page, setPage, hasResults, onOpenResultsHistory }) {
  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brandWordmark">DATA COMPARATOR</div>
      </div>

      <div className="workspace">WORKSPACE</div>

      <NavigationItem
        icon={LayoutDashboard}
        label="Overview"
        active={page === "dashboard"}
        onClick={() => setPage("dashboard")}
      />

      <NavigationItem
        icon={GitCompare}
        label="Comparisons"
        active={page === "comparison"}
        onClick={() => setPage("comparison")}
      />

      <NavigationItem
        icon={Activity}
        label="Results"
        active={page === "results"}
        onClick={onOpenResultsHistory}
      />

      <div className="workspace">CONFIGURATION</div>

      <NavigationItem
        icon={Link2}
        label="Connection Manager"
        active={page === "connections"}
        onClick={() => setPage("connections")}
      />

      <NavigationItem
        icon={SlidersHorizontal}
        label="Rule Repository"
        active={page === "rules"}
        onClick={() => setPage("rules")}
      />

      <div className="sidebarBottom">
        <div className="apiState">
          <i />

          <div>
            <b>Backend online</b>
            <span>FastAPI · :8000</span>
          </div>
        </div>
      </div>
    </aside>
  );
}

function NavigationItem({
  icon: Icon,
  label,
  active,
  disabled,
  onClick,
}) {
  return (
    <button
      className={`nav ${active ? "active" : ""}`}
      disabled={disabled}
      onClick={onClick}
    >
      <Icon size={17} />

      <span>{label}</span>

      {active && <ChevronRight size={14} />}
    </button>
  );
}

/* ============================================================
   HEADER
============================================================ */

function Header() {
  return (
    <header className="header">
    </header>
  );
}

/* ============================================================
   DASHBOARD
============================================================ */

function Dashboard({
  connections,
  onNewComparison,
  onConnections,
}) {
  const connectedCount = connections.filter(
    (connection) => connection.status === "CONNECTED"
  ).length;

  return (
    <div className="stack">
      <div className="dashboardIntro">
        <div>
          <span className="sectionEyebrow">DATA QUALITY WORKSPACE</span>
          <h1>Comparison control center</h1>
          <p>Monitor connections, configure validation, and review evidence.</p>
        </div>
        <button className="primary" onClick={onNewComparison}>
          <Plus size={15} /> New comparison
        </button>
      </div>

      <div className="stats">
        <Metric
          label="Authenticated connections"
          value={connections.length}
          sub={`${connectedCount} currently connected`}
          icon={Link2}
        />

        <Metric
          label="Comparison levels"
          value="6"
          sub="L1 Schema → L6 DQ"
          icon={GitCompare}
        />

        <Metric
          label="Connector types"
          value="2"
          sub="CSV · Databricks"
          icon={Database}
        />

        <Metric
          label="Execution engine"
          value="Ready"
          sub="Planner + task execution"
          icon={Zap}
        />
      </div>

      <div className="grid2">
        <Panel title="How the platform works">
          <div className="steps">
            {[
              [
                "01",
                "Connect",
                "Save and test reusable source connections.",
              ],
              [
                "02",
                "Configure",
                "Choose sources, keys, levels and rules.",
              ],
              [
                "03",
                "Execute",
                "Planner creates only the tasks you selected.",
              ],
              [
                "04",
                "Review",
                "Inspect metrics and comparison evidence.",
              ],
            ].map(([number, title, description]) => (
              <div className="step" key={number}>
                <b>{number}</b>

                <div>
                  <strong>{title}</strong>
                  <span>{description}</span>
                </div>

                <ArrowRight size={15} />
              </div>
            ))}
          </div>
        </Panel>

        <Panel
          title="Connection health"
          action={
            <button
              className="textBtn"
              onClick={onConnections}
            >
              Open manager
            </button>
          }
        >
          {connections.length === 0 ? (
            <Empty
              icon={Link2}
              title="No connections yet"
              text="Add a source to start building comparisons."
            />
          ) : (
            connections.slice(0, 5).map((connection) => (
              <ConnectionLine
                key={connection.connection_id}
                connection={connection}
              />
            ))
          )}
        </Panel>
      </div>
    </div>
  );
}