import React, { useEffect, useState } from "react";
import { apiRequest } from "./api/client.js";
import Sidebar from "./components/layout/Sidebar.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import Connections from "./pages/Connections.jsx";
import ConnectionModal from "./components/connections/ConnectionModal.jsx";
import ComparisonBuilder from "./components/comparison/ComparisonBuilder.jsx";
import Results from "./pages/Results.jsx";
import AnalysisPage from "./pages/Analysis.jsx";
import Rules from "./pages/Rules.jsx";
import Toast from "./components/ui/Toast.jsx";

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
            <Connections
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
            <Results
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

          {page === "rules" && <Rules notify={notify} />}
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
