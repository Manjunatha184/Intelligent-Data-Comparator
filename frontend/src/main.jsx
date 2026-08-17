import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import "@fontsource/montserrat/latin-400.css";
import "@fontsource/montserrat/latin-500.css";
import "@fontsource/montserrat/latin-600.css";
import "@fontsource/montserrat/latin-700.css";

import {
  Activity,
  ArrowRight,
  Check,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  Database,
  Download,
  Eye,
  FileText,
  GitCompare,
  LayoutDashboard,
  Link2,
  Loader2,
  Plus,
  RefreshCw,
  ShieldCheck,
  SlidersHorizontal,
  Trash2,
  TriangleAlert,
  X,
  Zap,
} from "lucide-react";

import "./styles.css";

/* ============================================================
   CONSTANTS
============================================================ */

const API_BASE = "/api/v1";

const NUMERIC_TYPE_PATTERNS = [
  "INT",
  "INTEGER",
  "BIGINT",
  "SMALLINT",
  "TINYINT",
  "DECIMAL",
  "NUMERIC",
  "NUMBER",
  "FLOAT",
  "DOUBLE",
  "REAL",
];

function getColumnName(column) {
  return column?.name || column?.column_name || column?.column || "";
}

function getColumnType(column) {
  return String(column?.data_type || column?.type || "")
    .trim()
    .toUpperCase()
    .replace(/\(.*/, "");
}

function isNumericColumn(column) {
  const normalizedType = getColumnType(column);
  return NUMERIC_TYPE_PATTERNS.some((type) => normalizedType === type || normalizedType.includes(type));
}

function getSchemaColumnNames(schema) {
  return (schema || [])
    .map(getColumnName)
    .filter(Boolean);
}

function findSchemaColumn(schema, name) {
  return (schema || []).find((column) => getColumnName(column) === name);
}

function rowsEqual(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function defaultMappedPair(sourceSchema, targetSchema) {
  const sourceColumns = getSchemaColumnNames(sourceSchema);
  const targetColumns = getSchemaColumnNames(targetSchema);
  const sourceColumn = sourceColumns[0] || "";
  const targetColumn = targetColumns.includes(sourceColumn)
    ? sourceColumn
    : targetColumns[0] || "";

  return {
    source_column: sourceColumn,
    target_column: targetColumn,
  };
}

function isNumericMapping(mapping, sourceSchema, targetSchema) {
  const sourceColumn = findSchemaColumn(sourceSchema, mapping?.source_column);
  const targetColumn = findSchemaColumn(targetSchema, mapping?.target_column);
  return Boolean(sourceColumn && targetColumn && isNumericColumn(sourceColumn) && isNumericColumn(targetColumn));
}

const COMPARISON_LEVELS = [
  {
    id: "L1",
    name: "Schema",
    description: "Columns, data types, lengths and nullability",
  },
  {
    id: "L2",
    name: "Volume",
    description: "Row counts, nulls and key statistics",
  },
  {
    id: "L3",
    name: "Record",
    description: "Record and key-level comparison",
  },
  {
    id: "L4",
    name: "Field",
    description: "Field-by-field value comparison",
  },
  {
    id: "L5",
    name: "Aggregate",
    description: "Configured aggregate checks",
  },
  {
    id: "L6",
    name: "Data Quality",
    description: "Configured data-quality rules",
  },
  {
    id: "L7",
    name: "Analysis",
    description: "Plain-language analysis of findings and cross-level evidence",
  },
];

const COMPARISON_LEVEL_ICONS = {
  L1: Database,
  L2: Activity,
  L3: GitCompare,
  L4: Eye,
  L5: SlidersHorizontal,
  L6: ShieldCheck,
  L7: Zap,
};

const CONNECTORS = {
  csv: {
    label: "CSV / File",
    icon: FileText,
    description: "Upload a CSV file from your computer",
    fields: [],
  },

  databricks: {
    label: "Databricks SQL",
    icon: Database,
    description: "Databricks SQL warehouse datasets",
    fields: [
      {
        key: "server_hostname",
        label: "Server hostname",
        type: "text",
        placeholder: "dbc-xxxx.cloud.databricks.com",
        required: true,
      },
      {
        key: "http_path",
        label: "HTTP path",
        type: "text",
        placeholder: "/sql/1.0/warehouses/xxxx",
        required: true,
      },
      {
        key: "access_token",
        label: "Access token",
        type: "password",
        placeholder: "Enter access token",
        required: true,
      },
    ],
  },

};

/* ============================================================
   API
============================================================ */

async function apiRequest(path, options = {}) {
  const method = String(options.method || "GET").toUpperCase();
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    method,
    cache: method === "GET" ? "no-store" : options.cache,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });

  const text = await response.text();

  let data = null;

  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  if (!response.ok) {
    throw new Error(
      formatApiError(data?.detail) ||
      data?.message ||
      `Request failed with status ${response.status}`
    );
  }

  return data;
}

function formatApiError(detail) {
  if (!detail) return "";

  if (typeof detail === "string") return detail;

  if (Array.isArray(detail)) {
    return detail
      .map((item) => {
        const location = Array.isArray(item?.loc)
          ? item.loc.join(".")
          : item?.loc;
        return [location, item?.msg]
          .filter(Boolean)
          .join(": ");
      })
      .filter(Boolean)
      .join("; ");
  }

  return detail?.msg || "";
}

function compactList(value) {
  if (!value) return [];
  const list = Array.isArray(value) ? value : [value];
  return list.filter(Boolean);
}

function normalizeAggregateRulePayload(rule) {
  const payload = {
    ...(rule?.payload || rule || {}),
  };

  if (!payload.name && rule?.name) {
    payload.name = rule.name;
  }

  if (!payload.function && payload.operation) {
    payload.function = payload.operation;
  }

  if (payload.tolerance_pct === "" || payload.tolerance_pct === undefined) {
    delete payload.tolerance_pct;
  } else if (payload.tolerance_pct !== null) {
    payload.tolerance_pct = Number(payload.tolerance_pct);
  }

  if (payload.tolerance === "" || payload.tolerance === undefined) {
    delete payload.tolerance;
  } else if (typeof payload.tolerance !== "object" && payload.tolerance !== null) {
    payload.tolerance = Number(payload.tolerance);
  }

  payload.group_by_columns = compactList(payload.group_by_columns);
  payload.source_group_by = compactList(payload.source_group_by);
  payload.target_group_by = compactList(payload.target_group_by);

  return payload;
}

function normalizeDqRulePayload(rule) {
  const payload = {
    ...(rule?.payload || rule || {}),
  };

  if (!payload.name && rule?.name) {
    payload.name = rule.name;
  }

  if (payload.rule_type) {
    payload.rule_type = String(payload.rule_type).toUpperCase();
  }

  if (!payload.apply_to) {
    payload.apply_to = "BOTH";
  }

  if (payload.column) {
    if (!payload.source_column) payload.source_column = payload.column;
    if (!payload.target_column) payload.target_column = payload.column;
  }

  return payload;
}

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

function App() {
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

/* ============================================================
   CONNECTION MANAGER
============================================================ */

function ConnectionManager({
  connections,
  loading,
  reload,
  onAdd,
  notify,
}) {
  const [testingId, setTestingId] = useState(null);

  async function testConnection(connectionId) {
    setTestingId(connectionId);

    try {
      const result = await apiRequest(
        `/connections/${connectionId}/test`,
        {
          method: "POST",
        }
      );

      notify(
        result.message || "Connection test successful."
      );

      await reload();
    } catch (error) {
      notify(error.message, "error");

      await reload();
    } finally {
      setTestingId(null);
    }
  }

  async function deleteConnection(connectionId) {
    const confirmed = window.confirm(
      "Delete this connection?"
    );

    if (!confirmed) {
      return;
    }

    try {
      await apiRequest(
        `/connections/${connectionId}`,
        {
          method: "DELETE",
        }
      );

      notify("Connection deleted successfully.");

      await reload();
    } catch (error) {
      notify(error.message, "error");
    }
  }

  return (
    <div className="stack">
      <div className="wizardFooter">
        <h1 className="pageTitle" style={{ margin: 0 }}>Connection Manager</h1>

        <div className="actionRow">
          <button
            className="secondary"
            onClick={reload}
          >
            <RefreshCw size={15} />
            Refresh
          </button>

          <button
            className="primary"
            onClick={onAdd}
          >
            <Plus size={16} />
            Add connection
          </button>
        </div>
      </div>

      <div className="connectorCards">
        {Object.entries(CONNECTORS).map(
          ([connectorType, connector]) => {
            const Icon = connector.icon;

            const count = connections.filter(
              (connection) =>
                connection.connector_type === connectorType
            ).length;

            return (
              <div
                className="connectorCard"
                key={connectorType}
              >
                <div className="sourceIcon large">
                  <Icon size={19} />
                </div>

                <div className="grow">
                  <b>{connector.label}</b>
                  <span>{connector.description}</span>
                </div>

                <strong>{count}</strong>
              </div>
            );
          }
        )}
      </div>

      <Panel
        title="Configured connections"
        action={
          <span className="countPill">
            {connections.length} total
          </span>
        }
      >
        {loading ? (
          <Loading text="Loading connections…" />
        ) : connections.length === 0 ? (
          <Empty
            icon={Link2}
            title="No connections configured"
            text="Add CSV or Databricks connections to make them available in the comparison builder."
          />
        ) : (
          <div className="connectionTable">
            <div className="thead">
              <span>Connection</span>
              <span>Type</span>
              <span>Status</span>
              <span>ID</span>
              <span />
            </div>

            {connections.map((connection) => {
              const ConnectorIcon =
                CONNECTORS[
                  connection.connector_type
                ]?.icon || Database;

              return (
                <div
                  className="trow"
                  key={connection.connection_id}
                >
                  <div className="nameCell">
                    <div className="sourceIcon">
                      <ConnectorIcon size={15} />
                    </div>

                    <div>
                      <b>{connection.name}</b>
                      <span>
                        {connection.connector_type}
                      </span>
                    </div>
                  </div>

                  <div>
                    <span className="typeTag">
                      {
                        CONNECTORS[
                          connection.connector_type
                        ]?.label
                      }
                    </span>
                  </div>

                  <div>
                    <Status
                      status={connection.status}
                    />
                  </div>

                  <code>
                    #{connection.connection_id}
                  </code>

                  <div className="rowButtons">
                    <button
                      onClick={() =>
                        testConnection(
                          connection.connection_id
                        )
                      }
                      disabled={
                        testingId ===
                        connection.connection_id
                      }
                    >
                      {testingId ===
                        connection.connection_id ? (
                        <Loader2
                          className="spin"
                          size={14}
                        />
                      ) : (
                        <RefreshCw size={14} />
                      )}

                      Test
                    </button>

                    <button
                      className="danger"
                      onClick={() =>
                        deleteConnection(
                          connection.connection_id
                        )
                      }
                    >
                      <Trash2 size={14} />
                      Delete
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </Panel>
    </div>
  );
}

/* ============================================================
   CONNECTION MODAL
============================================================ */

function ConnectionModal({
  onClose,
  onDone,
  notify,
}) {
  const [connectorType, setConnectorType] =
    useState("csv");

  const [name, setName] = useState("");

  const [values, setValues] = useState({});

  const [saving, setSaving] = useState(false);
  const [csvFile, setCsvFile] = useState(null);
  const [csvUpload, setCsvUpload] = useState(null);
  const [uploadingCsv, setUploadingCsv] = useState(false);
  const connector = CONNECTORS[connectorType];

  function updateValue(key, value) {
    setValues((current) => ({
      ...current,
      [key]: value,
    }));
  }

  useEffect(() => {
    setValues({});
    setCsvFile(null);
    setCsvUpload(null);
  }, [connectorType]);

  async function handleCsvFile(event) {
    const file = event.target.files?.[0];

    if (!file) return;

    if (!file.name.toLowerCase().endsWith(".csv")) {
      notify("Please select a CSV file.", "error");
      event.target.value = "";
      return;
    }

    setCsvFile(file);
    setCsvUpload(null);
    setUploadingCsv(true);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const response = await fetch(
        "/api/v1/connections/upload-csv",
        {
          method: "POST",
          body: formData,
        }
      );

      if (!response.ok) {
        let message = "CSV upload failed.";

        try {
          const errorData = await response.json();
          message = errorData.detail || message;
        } catch {
          // keep default message
        }

        throw new Error(message);
      }

      const uploaded = await response.json();

      setCsvUpload(uploaded);

      notify(
        `${uploaded.filename} uploaded successfully.`
      );
    } catch (error) {
      setCsvFile(null);
      setCsvUpload(null);
      notify(error.message, "error");
    } finally {
      setUploadingCsv(false);
    }
  }

  async function submit(event) {
    event.preventDefault();

    if (!name.trim()) {
      notify(
        "Connection name is required.",
        "error"
      );

      return;
    }
    if (
      connectorType === "csv" &&
      !csvUpload?.path
    ) {
      notify(
        "Choose and upload a CSV file first.",
        "error"
      );

      return;
    }

    const missingField =
      connector.fields.find(
        (field) =>
          field.required &&
          !values[field.key]?.trim()
      );

    if (missingField) {
      notify(
        `Enter ${missingField.label}.`,
        "error"
      );

      return;
    }

    setSaving(true);

    try {
      await apiRequest("/connections", {
        method: "POST",
        body: JSON.stringify({
          name: name.trim(),
          connector_type: connectorType,
          properties:
            connectorType === "csv"
              ? {
                path: csvUpload.path,
                delimiter: ",",
                encoding: "utf-8",
                filename: csvUpload.filename,
                upload_id: csvUpload.upload_id,
              }
              : values,
        }),
      });

      notify(
        `${connector.label} connected successfully.`
      );

      onDone();
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setSaving(false);
    }
  }

  const ConnectorIcon = connector.icon;

  return (
    <div className="modalBackdrop">
      <div className="modal wide">
        <div className="modalHead">
          <div>
            <span className="sectionEyebrow">
              NEW CONNECTION
            </span>

            <h3>Connect a data source</h3>

            <p>
              The backend tests the connection before
              storing it.
            </p>
          </div>

          <button
            type="button"
            className="iconButton"
            onClick={onClose}
          >
            <X size={18} />
          </button>
        </div>

        <div className="connectorPicker">
          {Object.entries(CONNECTORS).map(
            ([key, item]) => {
              const Icon = item.icon;

              return (
                <button
                  type="button"
                  key={key}
                  className={
                    connectorType === key
                      ? "selected"
                      : ""
                  }
                  onClick={() =>
                    setConnectorType(key)
                  }
                >
                  <Icon size={17} />

                  <div>
                    <b>{item.label}</b>
                    <span>
                      {key === "csv" ? "File" : "SQL"}
                    </span>
                  </div>

                  {connectorType === key && (
                    <Check size={15} />
                  )}
                </button>
              );
            }
          )}
        </div>

        <form onSubmit={submit}>
          <div className="formGrid">
            <Field
              label="Connection name"
              required
            >
              <input
                type="text"
                value={name}
                placeholder="Finance source"
                onChange={(event) =>
                  setName(event.target.value)
                }
              />
            </Field>

            {connectorType === "csv" && (
              <Field
                label="CSV file"
                required
              >
                <div>
                  <label
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "10px",
                      minHeight: "44px",
                      padding: "7px 10px",
                      border: "1px solid var(--border)",
                      borderRadius: "10px",
                      background: "var(--surface)",
                      cursor: uploadingCsv || saving ? "not-allowed" : "pointer",
                      opacity: uploadingCsv || saving ? 0.65 : 1,
                    }}
                  >
                    <input
                      type="file"
                      accept=".csv,text/csv"
                      onChange={handleCsvFile}
                      disabled={uploadingCsv || saving}
                      style={{
                        position: "absolute",
                        width: 1,
                        height: 1,
                        opacity: 0,
                        pointerEvents: "none",
                      }}
                    />

                    <span
                      className="secondary"
                      style={{
                        display: "inline-flex",
                        alignItems: "center",
                        gap: "7px",
                        minHeight: "30px",
                        padding: "5px 10px",
                        borderRadius: "7px",
                        whiteSpace: "nowrap",
                      }}
                    >
                      <FileText size={15} />
                      {csvUpload ? "Change file" : "Choose CSV"}
                    </span>

                    <span
                      style={{
                        minWidth: 0,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                        color: csvUpload ? "var(--text)" : "var(--muted)",
                        fontSize: "13px",
                      }}
                    >
                      {uploadingCsv
                        ? "Uploading CSV..."
                        : csvUpload?.filename || "No file selected"}
                    </span>
                  </label>
                </div>
              </Field>
            )}

            {connector.fields.map((field) => (
              <Field
                key={field.key}
                label={field.label}
                required={field.required}
              >
                <input
                  type={field.type}
                  value={values[field.key] || ""}
                  placeholder={field.placeholder}
                  onChange={(event) =>
                    updateValue(
                      field.key,
                      event.target.value
                    )
                  }
                  autoComplete="off"
                />
              </Field>
            ))}

          </div>

          {connectorType === "databricks" && (
            <div className="infoBox">
              <ShieldCheck size={17} />

              <div>
                <b>Credential protection</b>

                <span>
                  Databricks access tokens are used for
                  connection testing and are masked by
                  the backend when returned.
                </span>
              </div>
            </div>
          )}

          <div className="modalFooter">
            <button
              type="button"
              className="secondary"
              onClick={onClose}
            >
              Cancel
            </button>

            <button
              type="submit"
              className="primary"
              disabled={
                saving ||
                uploadingCsv ||
                (
                  connectorType === "csv" &&
                  !csvUpload?.path
                )
              }
            >
              {saving ? (
                <>
                  <Loader2
                    className="spin"
                    size={15}
                  />
                  Testing…
                </>
              ) : (
                <>
                  <ShieldCheck size={15} />
                  Test & save connection
                </>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

/* ============================================================
   COMPARISON BUILDER
============================================================ */

function ComparisonBuilder({
  connections,
  notify,
  connectionsLoading,
  connectionsError,
  reloadConnections,
  onComplete,
}) {
  const [step, setStep] = useState(1);
  const [reviewModalOpen, setReviewModalOpen] = useState(false);

  const [sourceId, setSourceId] = useState("");

  const [targetId, setTargetId] = useState("");

  const [sourceDbCatalog, setSourceDbCatalog] = useState("");
  const [sourceDbSchema, setSourceDbSchema] = useState("");
  const [sourceDbTable, setSourceDbTable] = useState("");

  const [targetDbCatalog, setTargetDbCatalog] = useState("");
  const [targetDbSchema, setTargetDbSchema] = useState("");
  const [targetDbTable, setTargetDbTable] = useState("");

  const [levels, setLevels] = useState([]);

  const [sourceConnection, setSourceConnection] =
    useState(null);

  const [targetConnection, setTargetConnection] =
    useState(null);

  const [comparisonKeys, setComparisonKeys] =
    useState([
      {
        source_column: "",
        target_column: "",
      },
    ]);

  const [groupingAttributes, setGroupingAttributes] = useState([]);
  const [aggregationColumns, setAggregationColumns] = useState([]);

  const [selectedDqRuleIds, setSelectedDqRuleIds] = useState([]);

  const [selectedAggRuleIds, setSelectedAggRuleIds] =
    useState([]);

  const [availableRules, setAvailableRules] = useState([]);

  const [columnMappings, setColumnMappings] = useState([]);

  const [sourceSchema, setSourceSchema] = useState(null);
  const [targetSchema, setTargetSchema] = useState(null);
  const [sourceSchemaLoading, setSourceSchemaLoading] = useState(false);
  const [targetSchemaLoading, setTargetSchemaLoading] = useState(false);
  const [sourceSchemaError, setSourceSchemaError] = useState(null);
  const [targetSchemaError, setTargetSchemaError] = useState(null);
  const [sourceFilters, setSourceFilters] = useState([]);
  const [targetFilters, setTargetFilters] = useState([]);
  const [ignoredSourceColumns, setIgnoredSourceColumns] = useState([]);
  const [ignoredTargetColumns, setIgnoredTargetColumns] = useState([]);

  useEffect(() => {
    apiRequest("/rules").then(data => setAvailableRules(data || [])).catch(() => { });
  }, []);

  function refreshRules() {
    apiRequest("/rules")
      .then(data => setAvailableRules(data || []))
      .catch(() => { });
  }

  useEffect(() => {
    let cancelled = false;
    const isDatabricks = sourceConnection?.connector_type === "databricks";
    const ready = sourceConnection && (!isDatabricks || (sourceDbCatalog && sourceDbSchema && sourceDbTable));
    if (!ready) { setSourceSchema(null); return () => { cancelled = true; }; }
    if (step === 2 && sourceSchema) return () => { cancelled = true; };
    const properties = isDatabricks
      ? { ...sourceConnection.properties, connection_id: sourceConnection.connection_id, connection: { ...sourceConnection.properties }, catalog: sourceDbCatalog, schema: sourceDbSchema, table: sourceDbTable }
      : { ...sourceConnection.properties };
    setSourceSchema(null); setSourceSchemaError(null); setSourceSchemaLoading(true);
    apiRequest("/connections/schema", { method: "POST", body: JSON.stringify({ connector_type: sourceConnection.connector_type, properties }) })
      .then(data => { if (!cancelled) setSourceSchema(Array.isArray(data?.columns) ? data.columns : []); })
      .catch(error => { if (!cancelled) { setSourceSchema(null); setSourceSchemaError(error.message || "Unable to load table schema"); } })
      .finally(() => { if (!cancelled) setSourceSchemaLoading(false); });
    return () => { cancelled = true; };
  }, [sourceConnection, sourceDbCatalog, sourceDbSchema, sourceDbTable, step]);

  useEffect(() => {
    let cancelled = false;
    const isDatabricks = targetConnection?.connector_type === "databricks";
    const ready = targetConnection && (!isDatabricks || (targetDbCatalog && targetDbSchema && targetDbTable));
    if (!ready) { setTargetSchema(null); return () => { cancelled = true; }; }
    if (step === 2 && targetSchema) return () => { cancelled = true; };
    const properties = isDatabricks
      ? { ...targetConnection.properties, connection_id: targetConnection.connection_id, connection: { ...targetConnection.properties }, catalog: targetDbCatalog, schema: targetDbSchema, table: targetDbTable }
      : { ...targetConnection.properties };
    setTargetSchema(null); setTargetSchemaError(null); setTargetSchemaLoading(true);
    apiRequest("/connections/schema", { method: "POST", body: JSON.stringify({ connector_type: targetConnection.connector_type, properties }) })
      .then(data => { if (!cancelled) setTargetSchema(Array.isArray(data?.columns) ? data.columns : []); })
      .catch(error => { if (!cancelled) { setTargetSchema(null); setTargetSchemaError(error.message || "Unable to load table schema"); } })
      .finally(() => { if (!cancelled) setTargetSchemaLoading(false); });
    return () => { cancelled = true; };
  }, [targetConnection, targetDbCatalog, targetDbSchema, targetDbTable, step]);

  useEffect(() => {
    const sourceColumns = getSchemaColumnNames(sourceSchema);
    const targetColumns = getSchemaColumnNames(targetSchema);

    setComparisonKeys((current) => {
      const validPairs = (current || []).filter(
        (key) =>
          sourceColumns.includes(key.source_column) &&
          targetColumns.includes(key.target_column)
      );
      const next = validPairs.length
        ? [validPairs[0]]
        : [defaultMappedPair(sourceSchema, targetSchema)];

      return rowsEqual(current, next) ? current : next;
    });

    setColumnMappings((current) => {
      const next = (current || [])
        .filter(
          (mapping) =>
            sourceColumns.includes(mapping.source_column) &&
            targetColumns.includes(mapping.target_column)
        )
        .map((mapping) => {
          if (isNumericMapping(mapping, sourceSchema, targetSchema)) {
            return mapping;
          }

          const { tolerance_pct, ...withoutTolerance } = mapping;
          return withoutTolerance;
        });

      return rowsEqual(current, next) ? current : next;
    });


    setGroupingAttributes((current) => current.filter(mapping => sourceColumns.includes(mapping.source_column) && targetColumns.includes(mapping.target_column)));
    setAggregationColumns((current) => current.filter(mapping => sourceColumns.includes(mapping.source_column) && targetColumns.includes(mapping.target_column)));

    setSourceFilters((current) => current.filter((item) => sourceColumns.includes(item.field)));
    setTargetFilters((current) => current.filter((item) => targetColumns.includes(item.field)));
    setIgnoredSourceColumns((current) => current.filter((column) => sourceColumns.includes(column)));
    setIgnoredTargetColumns((current) => current.filter((column) => targetColumns.includes(column)));
  }, [sourceSchema, targetSchema]);

  const [running, setRunning] = useState(false);

  const connectedConnections =
    connections.filter(
      (connection) =>
        String(connection.status || "").trim().toUpperCase() === "CONNECTED"
    );

  const source = connectedConnections.find(
    (connection) =>
      String(connection.connection_id) ===
      String(sourceId)
  );

  const target = connectedConnections.find(
    (connection) =>
      String(connection.connection_id) ===
      String(targetId)
  );

  useEffect(() => {
    if (!sourceId) {
      setSourceConnection(null);
      return;
    }

    loadConnection(
      sourceId,
      setSourceConnection,
      notify
    );
  }, [sourceId]);

  useEffect(() => {
    if (!targetId) {
      setTargetConnection(null);
      return;
    }

    loadConnection(
      targetId,
      setTargetConnection,
      notify
    );
  }, [targetId]);

  function toggleLevel(levelId) {
    setLevels((current) => {
      if (current.includes(levelId)) {
        return current.filter(
          (level) => level !== levelId
        );
      }

      return [...current, levelId];
    });
  }

  async function runComparison() {
    if (!source || !target) {
      notify(
        "Select both source and target connections.",
        "error"
      );

      setStep(1);

      return;
    }

    if (
      !sourceConnection?.properties ||
      !targetConnection?.properties
    ) {
      notify(
        "Unable to load the selected connection properties.",
        "error"
      );

      return;
    }

    const validationLevels = levels.filter((level) => level !== "L7");
    if (!validationLevels.length) {
      notify(
        "Select at least one validation level before running the comparison.",
        "error"
      );

      setStep(2);

      return;
    }

    const sourceProperties = {
      ...sourceConnection.properties,
      connection_id: sourceConnection.connection_id,
    };

    const targetProperties = {
      ...targetConnection.properties,
      connection_id: targetConnection.connection_id,
    };

    /*
     * Databricks connector expects the connection
     * properties inside the dataset configuration,
     * and the metadata discovery state at the root.
     */
    if (
      sourceConnection.connector_type ===
      "databricks"
    ) {
      if (!sourceDbCatalog || !sourceDbSchema || !sourceDbTable) {
        notify("Select Catalog, Schema, and Table for the Databricks source.", "error");
        setStep(1);
        return;
      }
      sourceProperties.connection_id =
        sourceConnection.connection_id;

      sourceProperties.connection = {
        ...sourceConnection.properties,
      };
      sourceProperties.catalog = sourceDbCatalog;
      sourceProperties.schema = sourceDbSchema;
      sourceProperties.table = sourceDbTable;
    }

    if (
      targetConnection.connector_type ===
      "databricks"
    ) {
      if (!targetDbCatalog || !targetDbSchema || !targetDbTable) {
        notify("Select Catalog, Schema, and Table for the Databricks target.", "error");
        setStep(1);
        return;
      }
      targetProperties.connection_id =
        targetConnection.connection_id;
      targetProperties.connection = {
        ...targetConnection.properties,
      };
      targetProperties.catalog = targetDbCatalog;
      targetProperties.schema = targetDbSchema;
      targetProperties.table = targetDbTable;
    }

    const ignoredColumnsPayload = Array.from(new Set([
      ...ignoredSourceColumns,
      ...ignoredTargetColumns,
    ]));

    const comparisonKeyPayload = (comparisonKeys || [])
      .slice(0, 1)
      .filter((key) => key.source_column && key.target_column)
      .map((key) => ({
        source_column: key.source_column,
        target_column: key.target_column,
      }));

    if (!comparisonKeyPayload.length) {
      notify(
        "Select at least one mapped comparison key.",
        "error"
      );

      setStep(2);

      return;
    }

    if (comparisonKeyPayload.some((key) =>
      ignoredColumnsPayload.includes(key.source_column) ||
      ignoredColumnsPayload.includes(key.target_column)
    )) {
      notify("A record-matching key cannot also be an ignored column.", "error");
      setStep(2);
      return;
    }

    const hasGroupConfiguration = groupingAttributes.length || aggregationColumns.length;
    if (hasGroupConfiguration) {
      if (!groupingAttributes.length || groupingAttributes.some(item => !item.source_column || !item.target_column)) {
        notify("Select at least one complete grouping field pair.", "error"); setStep(2); return;
      }
      if (groupingAttributes.filter(item => item.source_column).length !== groupingAttributes.filter(item => item.target_column).length) {
        notify("Source and target grouping field counts must match.", "error"); setStep(2); return;
      }
      if (!aggregationColumns.length || aggregationColumns.some(item => !item.source_column || !item.target_column)) {
        notify("Select at least one complete aggregation field pair.", "error"); setStep(2); return;
      }
      if (aggregationColumns.filter(item => item.source_column).length !== aggregationColumns.filter(item => item.target_column).length) {
        notify("Source and target aggregation field counts must match.", "error"); setStep(2); return;
      }
      if (aggregationColumns.some(item => !item.operation)) {
        notify("Each aggregation mapping must have an operation.", "error"); setStep(2); return;
      }
      if (aggregationColumns.length !== new Set(aggregationColumns.map(item => item.source_column)).size || aggregationColumns.length !== new Set(aggregationColumns.map(item => item.target_column)).size) {
        notify("Aggregation fields cannot be duplicated.", "error"); setStep(2); return;
      }
      if (groupingAttributes.length !== new Set(groupingAttributes.map(item => item.source_column)).size || groupingAttributes.length !== new Set(groupingAttributes.map(item => item.target_column)).size) {
        notify("Grouping fields cannot be duplicated.", "error"); setStep(2); return;
      }
      if ([...groupingAttributes, ...aggregationColumns].some((item) =>
        ignoredColumnsPayload.includes(item.source_column) ||
        ignoredColumnsPayload.includes(item.target_column)
      )) {
        notify("Grouping and aggregation fields cannot also be ignored columns.", "error"); setStep(2); return;
      }
    }

    const columnMappingPayload = (columnMappings || [])
      .filter((mapping) => mapping.source_column && mapping.target_column)
      .map((mapping) => {
        const payloadMapping = {
          ...mapping,
        };

        if (
          mapping.tolerance_pct !== undefined &&
          mapping.tolerance_pct !== ""
        ) {
          payloadMapping.tolerance_pct = Number(mapping.tolerance_pct);
        } else {
          delete payloadMapping.tolerance_pct;
        }

        if (mapping.tolerance !== undefined && mapping.tolerance !== "") {
          payloadMapping.tolerance = Number(mapping.tolerance);
        } else {
          delete payloadMapping.tolerance;
        }

        return payloadMapping;
      });

    const payload = {

      source: {
        connector_type:
          sourceConnection.connector_type,
        properties: sourceProperties,
      },

      target: {
        connector_type:
          targetConnection.connector_type,
        properties: targetProperties,
      },

      comparison_levels: validationLevels,
      l7_enabled: levels.includes("L7"),

      comparison_keys: comparisonKeyPayload,

      column_mappings: columnMappingPayload,

      ignored_columns: ignoredColumnsPayload,

      aggregate_rules: availableRules
        .filter(r => selectedAggRuleIds.some(id => String(id) === String(r.rule_id)))
        .map(r => normalizeAggregateRulePayload(r)),

      dq_rules: availableRules
        .filter(r => selectedDqRuleIds.some(id => String(id) === String(r.rule_id)))
        .map(r => normalizeDqRulePayload(r)),

      source_filters: sourceFilters.filter(f => f.field).map(normalizeRowFilterPayload),
      target_filters: targetFilters.filter(f => f.field).map(normalizeRowFilterPayload),


      matching_mode: groupingAttributes.length || aggregationColumns.length
        ? "GROUP_RECONCILIATION"
        : "ROW_LEVEL",
      grouping_attributes: groupingAttributes,
      aggregation_columns: aggregationColumns,

      strategy_policy: {
        max_exact_rows: 100000,
        max_exact_bytes: 50000000,
        sampling_min_rows: 1000000,
        allow_sampling: false,
      },
    };

    setRunning(true);

    try {
      // --------------------------------------------------
      // STEP 1: SAVE CONFIGURATION
      // --------------------------------------------------

      const configurationResult = await apiRequest(
        "/configurations",
        {
          method: "POST",
          body: JSON.stringify({
            configuration: payload,
          }),
        }
      );

      const configurationId =
        configurationResult.configuration_id;

      if (!configurationId) {
        throw new Error(
          "Configuration was saved but no configuration ID was returned."
        );
      }

      // --------------------------------------------------
      // STEP 2: RUN COMPARISON USING DATABASE ID
      // --------------------------------------------------

      const comparisonPayload = {
        configuration_id: configurationId,
        ...payload,
      };

      const result = await apiRequest(
        "/comparisons",
        {
          method: "POST",
          body: JSON.stringify(
            comparisonPayload
          ),
        }
      );

      notify(
        `Comparison ${String(
          result.status
        ).toLowerCase()}.`
      );

      onComplete(result.run_id);

    } catch (error) {
      notify(error.message, "error");
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="stack comparisonBuilder">
      <div className="wizardFooter">
        <div style={{ display: "flex", alignItems: "center", gap: "15px" }}>
          <div className="builderHeading">
            <h1 className="pageTitle" style={{ margin: 0 }}>Create a comparison</h1>
            <div className="comparisonProgress" aria-label="Comparison setup progress">
              <span className={step >= 1 ? "active" : ""}><b>01</b> Define scope</span>
              <i />
              <span className={step >= 2 ? "active" : ""}><b>02</b> Review & run</span>
            </div>
          </div>
        </div>

        <div className="actionRow">
          {step > 1 && (
            <button
              className="secondary"
              onClick={() =>
                setStep((current) => current - 1)
              }
            >
              Back
            </button>
          )}

          {step === 1 ? (
            <button
              className="primary"
              disabled={!sourceId || !targetId || !levels.some((level) => level !== "L7")}
              onClick={() => setStep(2)}
            >
              Continue
              <ArrowRight size={15} />
            </button>
          ) : (
            <button
              className="primary"
              onClick={() => setReviewModalOpen(true)}
            >
              Review & Run
              <ArrowRight size={15} />
            </button>
          )}
        </div>
      </div>

      {step === 1 && (
        <div className="stack">
          <SourceStep
            connections={connectedConnections}
            source={source}
            target={target}
            sourceId={sourceId}
            targetId={targetId}
            setSourceId={setSourceId}
            setTargetId={setTargetId}
            sourceDbCatalog={sourceDbCatalog}
            setSourceDbCatalog={setSourceDbCatalog}
            sourceDbSchema={sourceDbSchema}
            setSourceDbSchema={setSourceDbSchema}
            sourceDbTable={sourceDbTable}
            setSourceDbTable={setSourceDbTable}
            targetDbCatalog={targetDbCatalog}
            setTargetDbCatalog={setTargetDbCatalog}
            targetDbSchema={targetDbSchema}
            setTargetDbSchema={setTargetDbSchema}
            targetDbTable={targetDbTable}
            setTargetDbTable={setTargetDbTable}
            notify={notify}
            connectionsLoading={connectionsLoading}
            connectionsError={connectionsError}
            reloadConnections={reloadConnections}
          />
          <LevelsStep
            levels={levels}
            toggleLevel={toggleLevel}
          />
        </div>
      )}

      {step === 2 && (
        <RulesStep
          comparisonKeys={comparisonKeys}
          setComparisonKeys={setComparisonKeys}
          groupingAttributes={groupingAttributes}
          setGroupingAttributes={setGroupingAttributes}
          aggregationColumns={aggregationColumns}
          setAggregationColumns={setAggregationColumns}
          availableRules={availableRules}
          selectedDqRuleIds={selectedDqRuleIds}
          setSelectedDqRuleIds={setSelectedDqRuleIds}
          selectedAggRuleIds={selectedAggRuleIds}
          setSelectedAggRuleIds={setSelectedAggRuleIds}
          notify={notify}
          onRulesChanged={refreshRules}
          levels={levels}
          columnMappings={columnMappings}
          setColumnMappings={setColumnMappings}
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          sourceSchemaLoading={sourceSchemaLoading}
          targetSchemaLoading={targetSchemaLoading}
          sourceSchemaError={sourceSchemaError}
          targetSchemaError={targetSchemaError}
          sourceFilters={sourceFilters}
          setSourceFilters={setSourceFilters}
          targetFilters={targetFilters}
          setTargetFilters={setTargetFilters}
          ignoredSourceColumns={ignoredSourceColumns}
          setIgnoredSourceColumns={setIgnoredSourceColumns}
          ignoredTargetColumns={ignoredTargetColumns}
          setIgnoredTargetColumns={setIgnoredTargetColumns}
        />
      )}

      {reviewModalOpen && (
        <ReviewModal
          source={sourceConnection}
          target={targetConnection}
          levels={levels}
          comparisonKeys={comparisonKeys}
          sourceFiltersCount={sourceFilters.length}
          targetFiltersCount={targetFilters.length}
          ignoredColumnsCount={new Set([...ignoredSourceColumns, ...ignoredTargetColumns]).size}
          mappingsCount={columnMappings.length}
          dqRulesCount={selectedDqRuleIds.length}
          aggregateRulesCount={selectedAggRuleIds.length}
          onClose={() => setReviewModalOpen(false)}
          onRun={runComparison}
          running={running}
        />
      )}

    </div>
  );
}

/* ============================================================
   CONNECTION DETAIL LOADING
============================================================ */

async function loadConnection(
  connectionId,
  setter,
  notify
) {
  try {
    const connection = await apiRequest(
      `/connections/${connectionId}`
    );

    setter(connection);
  } catch (error) {
    setter(null);
    notify(error.message, "error");
  }
}

/* ============================================================
   COMPARISON SOURCES
============================================================ */

function SourceStep({
  connections,
  source,
  target,
  sourceId,
  targetId,
  setSourceId,
  setTargetId,
  sourceDbCatalog,
  setSourceDbCatalog,
  sourceDbSchema,
  setSourceDbSchema,
  sourceDbTable,
  setSourceDbTable,
  targetDbCatalog,
  setTargetDbCatalog,
  targetDbSchema,
  setTargetDbSchema,
  targetDbTable,
  setTargetDbTable,
  notify,
  connectionsLoading,
  connectionsError,
  reloadConnections,
}) {
  return (
    <section className="scopeSourceSection">
      <div className="scopeSectionIntro">
        <div>
          <h2>Choose what you want to compare</h2>
        </div>
      </div>

      <div className="grid2 scopeSourceGrid">
      <Panel title="Source dataset" className="scopeDatasetCard">
        <ConnectionSelector
          label="Source connection"
          value={sourceId}
          setValue={setSourceId}
          connections={connections}
          loading={connectionsLoading}
          error={connectionsError}
          onRetry={reloadConnections}
        />
        {source?.connector_type === "databricks" && (
          <DatabricksSelector
            connection={source}
            catalog={sourceDbCatalog}
            setCatalog={setSourceDbCatalog}
            schema={sourceDbSchema}
            setSchema={setSourceDbSchema}
            table={sourceDbTable}
            setTable={setSourceDbTable}
            notify={notify}
          />
        )}
      </Panel>

      <Panel title="Target dataset" className="scopeDatasetCard">
        <ConnectionSelector
          label="Target connection"
          value={targetId}
          setValue={setTargetId}
          connections={connections}
          loading={connectionsLoading}
          error={connectionsError}
          onRetry={reloadConnections}
        />
        {target?.connector_type === "databricks" && (
          <DatabricksSelector
            connection={target}
            catalog={targetDbCatalog}
            setCatalog={setTargetDbCatalog}
            schema={targetDbSchema}
            setSchema={setTargetDbSchema}
            table={targetDbTable}
            setTable={setTargetDbTable}
            notify={notify}
          />
        )}
      </Panel>
      </div>
    </section>
  );
}

function DatabricksSelector({
  connection,
  catalog,
  setCatalog,
  schema,
  setSchema,
  table,
  setTable,
  notify
}) {
  const [catalogs, setCatalogs] = useState([]);
  const [schemas, setSchemas] = useState([]);
  const [tables, setTables] = useState([]);

  const [loadingCatalog, setLoadingCatalog] = useState(false);
  const [loadingSchema, setLoadingSchema] = useState(false);
  const [loadingTable, setLoadingTable] = useState(false);

  useEffect(() => {
    if (!connection) return;
    setLoadingCatalog(true);
    apiRequest("/connections/discover/catalogs", {
      method: "POST",
      body: JSON.stringify({
        connector_type: connection.connector_type,
        properties: {
          ...connection.properties,
          connection_id: connection.connection_id,
        }
      })
    })
      .then(res => setCatalogs(res || []))
      .catch(e => notify("Failed to load catalogs: " + e.message, "error"))
      .finally(() => setLoadingCatalog(false));
  }, [connection]);

  useEffect(() => {
    if (!connection || !catalog) {
      setSchemas([]);
      return;
    }
    setLoadingSchema(true);
    apiRequest("/connections/discover/schemas", {
      method: "POST",
      body: JSON.stringify({
        connector_type: connection.connector_type,
        properties: {
          ...connection.properties,
          connection_id: connection.connection_id,
        },
        catalog
      })
    })
      .then(res => setSchemas(res || []))
      .catch(e => notify("Failed to load schemas: " + e.message, "error"))
      .finally(() => setLoadingSchema(false));
  }, [connection, catalog]);

  useEffect(() => {
    if (!connection || !catalog || !schema) {
      setTables([]);
      return;
    }
    setLoadingTable(true);
    apiRequest("/connections/discover/tables", {
      method: "POST",
      body: JSON.stringify({
        connector_type: connection.connector_type,
        properties: {
          ...connection.properties,
          connection_id: connection.connection_id,
        },
        catalog,
        schema_name: schema
      })
    })
      .then(res => setTables(res || []))
      .catch(e => notify("Failed to load tables: " + e.message, "error"))
      .finally(() => setLoadingTable(false));
  }, [connection, catalog, schema]);

  return (
    <div style={{ marginTop: "16px", display: "flex", flexDirection: "column", gap: "12px" }}>
      <label className="field">
        <span>Catalog<em>*</em></span>
        <select value={catalog} onChange={e => { setCatalog(e.target.value); setSchema(""); setTable(""); }}>
          <option value="">{loadingCatalog ? "Loading catalogs..." : "Select catalog…"}</option>
          {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
      </label>

      <label className="field">
        <span>Schema<em>*</em></span>
        <select value={schema} onChange={e => { setSchema(e.target.value); setTable(""); }} disabled={!catalog}>
          <option value="">{loadingSchema ? "Loading schemas..." : "Select schema…"}</option>
          {schemas.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </label>

      <label className="field">
        <span>Table<em>*</em></span>
        <select value={table} onChange={e => setTable(e.target.value)} disabled={!schema}>
          <option value="">{loadingTable ? "Loading tables..." : "Select table…"}</option>
          {tables.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </label>
    </div>
  );
}

function ConnectionSelector({
  label,
  value,
  setValue,
  connections,
  loading = false,
  error = null,
  onRetry,
}) {
  const displayConnection = (connection) => {
    const properties = connection.properties || {};
    const filename =
      properties.filename ||
      properties.path?.split("/").pop();
    const dataset =
      connection.connector_type === "csv"
        ? filename
        : properties.table;

    return dataset
      ? `${connection.name} (${dataset})`
      : connection.name;
  };

  return (
    <div className="field connectionSelector">
      <span>
        {label}
        <em>*</em>
      </span>

      {loading && <div className="connectionSelectorState">Loading connections...</div>}
      {!loading && error && (
        <div className="connectionSelectorState connectionSelectorError">
          <span>Unable to load connections</span>
          <button type="button" className="textBtn" onClick={onRetry}>Retry</button>
        </div>
      )}
      {!loading && !error && connections.length === 0 && (
        <div className="connectionSelectorState">No connections available</div>
      )}
      {!loading && !error && connections.length > 0 && (
        <select
          value={value}
          onChange={(event) => setValue(event.target.value)}
          aria-label={label}
        >
          <option value="">Select an authenticated connection...</option>
          {connections.map((connection) => {
            return (
              <option
                key={connection.connection_id}
                value={String(connection.connection_id)}
              >
                {displayConnection(connection)} · {CONNECTORS[connection.connector_type]?.label || connection.connector_type}
              </option>
            );
          })}
        </select>
      )}
    </div>
  );
}

/* ============================================================
   LEVELS
============================================================ */

function LevelsStep({
  levels,
  toggleLevel,
}) {
  return (
    <Panel title="Comparison depth" className="scopeLevelsPanel">
      <div className="scopeLevelIntro">
        <p className="helper">
          Build the validation path from structural checks through plain-language analysis.
        </p>
        <span className="scopeSelectionCount">{levels.length} of {COMPARISON_LEVELS.length} selected</span>
      </div>

      <div className="levelGrid">
        {COMPARISON_LEVELS.map((level) => {
          const selected = levels.includes(
            level.id
          );
          const LevelIcon = COMPARISON_LEVEL_ICONS[level.id];

          return (
            <button
              type="button"
              key={level.id}
              className={`level ${selected ? "selected" : ""} level-${level.id}`}
              onClick={() => toggleLevel(level.id)}
            >
              <span className="levelVisual">
                <LevelIcon size={17} />
                <span className="levelCode">{level.id}</span>
              </span>

              <div>
                <b>{level.name}</b>
                <small>
                  {level.description}
                </small>
              </div>

              <span className="checkCircle">
                {selected && (
                  <Check size={13} />
                )}
              </span>
            </button>
          );
        })}
      </div>
    </Panel>
  );
}

/* ============================================================
   RULES
============================================================ */

function RuleSelectionModal({
  title,
  rules,
  selectedIds,
  onSelectionChange,
  onClose,
  category,
  sourceSchema,
  targetSchema,
  notify,
  onRulesChanged,
}) {
  const [ruleEditorOpen, setRuleEditorOpen] = useState(false);

  return (
    <div className="modalBackdrop">
      <div className="modal">
        <div className="modalHead">
          <div>
            <h3>{title}</h3>
            <p className="helper">Select rules from the repository</p>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            {category && (
              <button type="button" className="secondary small" onClick={() => setRuleEditorOpen(true)}>
                <Plus size={14} /> New rule
              </button>
            )}
            <button type="button" className="iconButton" onClick={onClose}>
              <X size={18} />
            </button>
          </div>
        </div>

        <div className="modalBody stack" style={{ maxHeight: "400px", overflowY: "auto", padding: "0 25px 20px" }}>
          {rules.length === 0 ? (
            <div className="empty compact" style={{ marginTop: "20px" }}>
              <b>No rules found</b>
            </div>
          ) : (
            <div className="ruleTable" style={{ marginTop: "20px" }}>
              {rules.map((rule) => (
                <label key={rule.rule_id} className="ruleCheckbox">
                  <input
                    type="checkbox"
                    checked={selectedIds.includes(rule.rule_id)}
                    onChange={(e) => {
                      if (e.target.checked)
                        onSelectionChange([...selectedIds, rule.rule_id]);
                      else
                        onSelectionChange(
                          selectedIds.filter((id) => id !== rule.rule_id)
                        );
                    }}
                  />
                  <div>
                    <b>{rule.name}</b>
                    <span>
                      {rule.rule_type === "DQ"
                        ? describeDqRule(rule.payload)
                        : `${String(rule.payload.function).toLowerCase()} on ${String(
                          rule.payload.source_column
                        ).toLowerCase()}`}
                    </span>
                  </div>
                </label>
              ))}
            </div>
          )}
        </div>
      </div>
      {ruleEditorOpen && (
        <RuleModal
          initialRuleType={category}
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          onClose={() => setRuleEditorOpen(false)}
          onDone={() => {
            setRuleEditorOpen(false);
            onRulesChanged?.();
          }}
          notify={notify}
        />
      )}
    </div>
  );
}

function describeDqRule(payload = {}) {
  const applyTo = String(payload.apply_to || "BOTH").toUpperCase();
  const sourceColumn = payload.source_column || payload.column;
  const targetColumn = payload.target_column || payload.column;
  const scopeLabel = applyTo === "SOURCE" ? sourceColumn : applyTo === "TARGET" ? targetColumn : `${sourceColumn} → ${targetColumn}`;
  return `${String(payload.rule_type || payload.type || "rule").toLowerCase()} on ${scopeLabel}`;
}

function schemaRuleOptions(schema, currentValue, schemaAware) {
  const schemaColumns = getSchemaColumnNames(schema);

  if (schemaAware) {
    return ["", ...schemaColumns];
  }

  return Array.from(
    new Set([
      "",
      ...schemaColumns,
      currentValue || "",
    ])
  );
}


function MultiSelectField({ options, selected, onChange, placeholder }) {
  const [open, setOpen] = React.useState(false);
  const rootRef = React.useRef(null);
  React.useEffect(() => {
    const close = (event) => { if (rootRef.current && !rootRef.current.contains(event.target)) setOpen(false); };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, []);
  const toggle = (value) => onChange(selected.includes(value) ? selected.filter(item => item !== value) : [...selected, value]);
  return <div className="multiSelectField" ref={rootRef}>
    <button type="button" className="multiSelectTrigger" aria-expanded={open} onClick={() => setOpen(!open)}>
      <span>{placeholder}</span><span className="multiSelectChevron">{open ? "▲" : "▼"}</span>
    </button>
    {open && <div className="multiSelectMenu" role="listbox">
      {options.map(option => <button type="button" role="option" aria-selected={selected.includes(option)} className="multiSelectOption" key={option} onClick={() => toggle(option)}>
        <span className="multiSelectCheck">{selected.includes(option) ? "✓" : ""}</span>{option}
      </button>)}
      {!options.length && <span className="multiSelectEmpty">No schema fields available</span>}
    </div>}
  </div>;
}

function RulesStep({
  comparisonKeys,
  setComparisonKeys,
  groupingAttributes,
  setGroupingAttributes,
  aggregationColumns,
  setAggregationColumns,
  availableRules,
  selectedDqRuleIds,
  setSelectedDqRuleIds,
  selectedAggRuleIds,
  setSelectedAggRuleIds,
  notify,
  onRulesChanged,
  levels,
  columnMappings,
  setColumnMappings,
  sourceSchema,
  targetSchema,
  sourceSchemaLoading,
  targetSchemaLoading,
  sourceSchemaError,
  targetSchemaError,
  sourceFilters,
  setSourceFilters,
  targetFilters,
  setTargetFilters,
  ignoredSourceColumns,
  setIgnoredSourceColumns,
  ignoredTargetColumns,
  setIgnoredTargetColumns,
}) {
  const [dqModalOpen, setDqModalOpen] = React.useState(false);
  const [aggModalOpen, setAggModalOpen] = React.useState(false);
  const [normalizationOpen, setNormalizationOpen] = React.useState({});
  const [pendingGroupingSource, setPendingGroupingSource] = React.useState("");
  const [pendingGroupingTarget, setPendingGroupingTarget] = React.useState("");
  const [pendingAggregationSource, setPendingAggregationSource] = React.useState("");
  const [pendingAggregationTarget, setPendingAggregationTarget] = React.useState("");

  const sourceColumnOptions = getSchemaColumnNames(sourceSchema);
  const targetColumnOptions = getSchemaColumnNames(targetSchema);

  const selectedKey = comparisonKeys?.[0] || {
    source_column: "",
    target_column: "",
  };

  const updateSelectedKey = (field, value) => {
    setComparisonKeys([
      {
        ...selectedKey,
        [field]: value,
      },
    ]);
  };


  const sourceGroupingFields = groupingAttributes.map((item) => item.source_column).filter(Boolean);
  const targetGroupingFields = groupingAttributes.map((item) => item.target_column).filter(Boolean);
  const sourceAggregationFields = aggregationColumns.map((item) => item.source_column).filter(Boolean);
  const targetAggregationFields = aggregationColumns.map((item) => item.target_column).filter(Boolean);
  const updatePairedSelection = (kind, side, values) => {
    const current = kind === "group" ? groupingAttributes : aggregationColumns;
    const selected = new Set(values);
    const field = `${side}_column`;
    const otherField = side === "source" ? "target_column" : "source_column";
    const existing = new Set(current.map((item) => item[field]).filter(Boolean));

    // Keep completed mappings intact by identity, not by the array position
    // of either multi-select. Only a newly selected field can fill an
    // intentionally incomplete mapping created on the opposite side.
    const next = current
      .map((item) => {
        if (!item[field] || selected.has(item[field])) return { ...item };
        const updated = { ...item, [field]: "" };
        if (kind === "aggregate" && side === "source") delete updated.operation;
        return updated;
      })
      .filter((item) => item.source_column || item.target_column);

    values.filter((value) => !existing.has(value)).forEach((value) => {
      // Automatic mappings prefer the same logical name. This keeps
      // Status -> Status and Region -> Region stable even if targets are
      // selected in a different order. Explicit mappings remain untouched.
      const pending = next.find(
        (item) => !item[field] && item[otherField] === value
      ) || next.find(
        (item) => !item[field] && item[otherField]
      );
      const mapping = pending || { source_column: "", target_column: "" };
      mapping[field] = value;
      if (kind === "aggregate" && mapping.source_column) {
        mapping.operation = automaticOperation(mapping.source_column, "source");
      }
      if (!pending) next.push(mapping);
    });

    kind === "group" ? setGroupingAttributes(next) : setAggregationColumns(next);
  };
  const selectionChips = (mappings, kind, side) => <div className="chipRow">
    {mappings.map((mapping, index) => {
      const value = mapping[`${side}_column`];
      if (!value) return null;
      return <span className="chip" key={`${kind}-${side}-${mapping.source_column}-${mapping.target_column}-${index}`}>
        {value}<button type="button" aria-label={`Remove ${value}`} onClick={() => {
          const current = kind === "group" ? groupingAttributes : aggregationColumns;
          const field = `${side}_column`;
          const next = current
            .map((item, mappingIndex) => {
              if (mappingIndex !== index) return { ...item };
              const updated = { ...item, [field]: "" };
              if (kind === "aggregate" && side === "source") delete updated.operation;
              return updated;
            })
            .filter((item) => item.source_column || item.target_column);
          kind === "group" ? setGroupingAttributes(next) : setAggregationColumns(next);
        }}>×</button></span>;
    })}
  </div>;
  const sourceTypeFor = (name) => getColumnType(findSchemaColumn(sourceSchema, name));
  const automaticOperation = (name, side) => {
    const schema = side === "source" ? sourceSchema : targetSchema;
    return isNumericColumn(findSchemaColumn(schema, name)) ? "AVG" : "MODE";
  };
  const FieldChips = ({ values, onRemove }) => (
    <div className="chipRow">
      {values.map((value, index) => <span className="chip" key={`${value}-${index}`}>{value}<button type="button" aria-label={`Remove ${value}`} onClick={() => onRemove(index)}>×</button></span>)}
    </div>
  );
  const MultiFieldPicker = ({ label, options, selected, onAdd }) => (
    <div className="field">
      <label>{label}</label>
      <select value="" onChange={(event) => onAdd(event.target.value)}>
        <option value="">Select fields...</option>
        {options.filter((option) => !selected.includes(option)).map(option => <option key={option} value={option}>{option}</option>)}
      </select>
    </div>
  );


  return (
    <div className="stack reviewRunStep">
      {(sourceSchemaLoading || targetSchemaLoading || sourceSchemaError || targetSchemaError) && (
        <div className="helper" role="status">
          {sourceSchemaError || targetSchemaError || (sourceSchemaLoading || targetSchemaLoading ? "Loading fields..." : "")}
        </div>
      )}
      <div className="filtersGrid">
        <FilterSection title="Source Filters" schema={sourceSchema} filters={sourceFilters} setFilters={setSourceFilters} />
        <FilterSection title="Target Filters" schema={targetSchema} filters={targetFilters} setFilters={setTargetFilters} />
      </div>
      <Panel title="Ignored columns" className="reviewRunCard ignoredColumnsCard">
        <p className="helper">Selected columns are excluded from every applicable comparison level.</p>
        <div className="formGrid">
          <div className="mappingPickerBlock">
            <label>Source columns to ignore</label>
            <MultiSelectField options={sourceColumnOptions} selected={ignoredSourceColumns} onChange={setIgnoredSourceColumns} placeholder="Select source columns" />
            <FieldChips values={ignoredSourceColumns} onRemove={(index) => setIgnoredSourceColumns(ignoredSourceColumns.filter((_, itemIndex) => itemIndex !== index))} />
          </div>
          <div className="mappingPickerBlock">
            <label>Target columns to ignore</label>
            <MultiSelectField options={targetColumnOptions} selected={ignoredTargetColumns} onChange={setIgnoredTargetColumns} placeholder="Select target columns" />
            <FieldChips values={ignoredTargetColumns} onRemove={(index) => setIgnoredTargetColumns(ignoredTargetColumns.filter((_, itemIndex) => itemIndex !== index))} />
          </div>
        </div>
      </Panel>
      <Panel title="Record matching" className="reviewRunCard recordMatchingCard">
        <div className="formGrid">
          <SelectField
            label="Source key"
            value={selectedKey.source_column || ""}
            setValue={(value) => updateSelectedKey("source_column", value)}
            options={["", ...sourceColumnOptions]}
          />

          <SelectField
            label="Target key"
            value={selectedKey.target_column || ""}
            setValue={(value) => updateSelectedKey("target_column", value)}
            options={["", ...targetColumnOptions]}
          />
        </div>


      </Panel>

      <Panel title="Group-Based Reconciliation" className="reviewRunCard reconciliationCard">
        <section className="reconciliationSection">
          <h4>Grouping fields</h4>
          <div className="formGrid">
            <div className="mappingPickerBlock">
              <label>Source grouping fields</label>
              <MultiSelectField options={sourceColumnOptions} selected={sourceGroupingFields} onChange={values => updatePairedSelection("group", "source", values)} placeholder="Select source fields" />
              {selectionChips(groupingAttributes, "group", "source")}
            </div>
            <div className="mappingPickerBlock">
              <label>Target grouping fields</label>
              <MultiSelectField options={targetColumnOptions} selected={targetGroupingFields} onChange={values => updatePairedSelection("group", "target", values)} placeholder="Select target fields" />
              {selectionChips(groupingAttributes, "group", "target")}
            </div>
          </div>
        </section>

        <section className="reconciliationSection">
          <h4>Aggregation fields</h4>
          <div className="formGrid">
            <div className="mappingPickerBlock">
              <label>Source aggregation fields</label>
              <MultiSelectField options={sourceColumnOptions} selected={sourceAggregationFields} onChange={values => updatePairedSelection("aggregate", "source", values)} placeholder="Select source fields" />
              {selectionChips(aggregationColumns, "aggregate", "source")}
            </div>
            <div className="mappingPickerBlock">
              <label>Target aggregation fields</label>
              <MultiSelectField options={targetColumnOptions} selected={targetAggregationFields} onChange={values => updatePairedSelection("aggregate", "target", values)} placeholder="Select target fields" />
              {selectionChips(aggregationColumns, "aggregate", "target")}
            </div>
          </div>
        </section>
        <p className="reconciliationNote">Numeric aggregation fields use AVG automatically; non-numeric aggregation fields use MODE automatically.</p>
      </Panel>

      <div className={`reviewRunSummaryGrid ${columnMappings?.length ? "hasMappings" : ""}`}>
      {!columnMappings || columnMappings.length === 0 ? (
        <Panel
          title="Column Mapping"
          className="reviewRunCard compactReviewCard"
          action={<button type="button" className="secondary small" onClick={() => setColumnMappings([{ source_column: "", target_column: "", tolerance_pct: undefined }])}>
            <Plus size={14} /> Add column mapping
          </button>}
        >
          <p className="reviewEmptyState">No mappings configured.</p>
        </Panel>
      ) : (
        <Panel title="Column Mapping" className="reviewRunCard">
          <div className="stack" style={{ gap: "10px" }}>
            {columnMappings.map((mapping, idx) => {
              const isNumericPair = isNumericMapping(mapping, sourceSchema, targetSchema);
              const updateMapping = (key, val) => {
                const copy = [...columnMappings];
                const updated = {
                  ...copy[idx],
                  [key]: val,
                };

                if (
                  key === "source_column" ||
                  key === "target_column"
                ) {
                  const nextMapping = {
                    ...updated,
                  };

                  if (!isNumericMapping(nextMapping, sourceSchema, targetSchema)) {
                    delete nextMapping.tolerance_pct;
                  }

                  copy[idx] = nextMapping;
                } else {
                  copy[idx] = updated;
                }

                setColumnMappings(copy);
              };

              return (
                <div key={idx} className="columnMappingRow" style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr) minmax(130px, 0.7fr) auto auto", gap: "10px", alignItems: "end", padding: "10px", border: "1px solid var(--line)", borderRadius: "8px", background: "#f8fafc" }}>
                  <div>
                    <SelectField
                      label="Source column"
                      value={mapping.source_column || ""}
                      options={["", ...sourceColumnOptions]}
                      setValue={v => updateMapping("source_column", v)}
                    />
                  </div>
                  <div>
                    <SelectField
                      label="Target column"
                      value={mapping.target_column || ""}
                      options={["", ...targetColumnOptions]}
                      setValue={v => updateMapping("target_column", v)}
                    />
                  </div>
                  <div>
                    <Field label="Tolerance (%)">
                      {isNumericPair ? (
                        <input
                          type="number"
                          min="0"
                          max="100"
                          step="any"
                          value={mapping.tolerance_pct === undefined ? "" : mapping.tolerance_pct}
                          onChange={e => updateMapping("tolerance_pct", e.target.value ? Number(e.target.value) : undefined)}
                        />
                      ) : (
                        <div style={{ padding: "8px 12px", background: "var(--surface)", border: "1px solid var(--line)", borderRadius: "6px", color: "var(--muted)", fontSize: "13px" }}>
                          N/A
                        </div>
                      )}
                    </Field>
                  </div>
                  <button type="button" className="secondary small" onClick={() => setNormalizationOpen(current => ({ ...current, [idx]: !current[idx] }))} style={{ marginBottom: "2px" }}>
                    {normalizationOpen[idx] ? "Hide" : "Configure"}
                  </button>
                  <button type="button" className="iconButton dangerIcon" title="Delete mapping" onClick={() => setColumnMappings(columnMappings.filter((_, i) => i !== idx))} style={{ marginBottom: "2px" }}><Trash2 size={15} /></button>
                  {normalizationOpen[idx] && (
                    <div style={{ gridColumn: "1 / -1", display: "flex", alignItems: "center", gap: "18px", flexWrap: "wrap", padding: "10px 4px 2px", borderTop: "1px solid var(--line)" }}>
                      <strong style={{ fontSize: "12px", color: "var(--text-secondary)" }}>Normalization options</strong>
                      {[["trim", "Trim whitespace"], ["case_insensitive", "Ignore case"], ["empty_as_null", "Empty string as null"]].map(([key, label]) => (
                        <label key={key} style={{ display: "inline-flex", alignItems: "center", gap: "6px", fontSize: "12px", color: "var(--text-secondary)", whiteSpace: "nowrap" }}>
                          <input type="checkbox" checked={Boolean(mapping.normalization?.[key])} onChange={e => updateMapping("normalization", { ...(mapping.normalization || {}), [key]: e.target.checked })} />
                          {label}
                        </label>
                      ))}
                      {isNumericPair && <label style={{ display: "inline-flex", alignItems: "center", gap: "7px", fontSize: "12px", color: "var(--text-secondary)", whiteSpace: "nowrap" }}>Round decimals<input style={{ width: "70px" }} type="number" min="0" step="1" value={mapping.normalization?.round ?? ""} onChange={e => {
                        const value = e.target.value;
                        updateMapping("normalization", { ...(mapping.normalization || {}), ...(value === "" ? { round: undefined } : { round: Number(value) }) });
                      }} /></label>}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
          <div style={{ marginTop: "15px" }}>
            <button type="button" className="secondary small" onClick={() => setColumnMappings([...(columnMappings || []), { source_column: "", target_column: "", tolerance_pct: undefined }])}>
              <Plus size={14} /> Add column mapping
            </button>
          </div>
        </Panel>
      )}

      <Panel title="Data quality rules (L6)" className="reviewRunCard compactReviewCard" action={<button type="button" className="secondary small" onClick={() => setDqModalOpen(true)}>Select rules</button>}>
        <p className="reviewEmptyState">
          {selectedDqRuleIds.length === 0 ? "No data-quality rules selected." : `${selectedDqRuleIds.length} data-quality rule${selectedDqRuleIds.length === 1 ? "" : "s"} selected.`}
        </p>
      </Panel>

      <Panel title="Aggregate rules (L5)" className="reviewRunCard compactReviewCard" action={<button type="button" className="secondary small" onClick={() => setAggModalOpen(true)}>Select rules</button>}>
        <p className="reviewEmptyState">
          {selectedAggRuleIds.length === 0 ? "No aggregate rules selected." : `${selectedAggRuleIds.length} aggregate rule${selectedAggRuleIds.length === 1 ? "" : "s"} selected.`}
        </p>
      </Panel>
      </div>

      {dqModalOpen && (
        <RuleSelectionModal
          title="Data quality rules (L6)"
          rules={availableRules.filter(r => r.rule_type === "DQ")}
          selectedIds={selectedDqRuleIds}
          onSelectionChange={setSelectedDqRuleIds}
          onClose={() => setDqModalOpen(false)}
          category="DQ"
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          notify={notify}
          onRulesChanged={onRulesChanged}
        />
      )}

      {aggModalOpen && (
        <RuleSelectionModal
          title="Aggregate rules (L5)"
          rules={availableRules.filter(r => r.rule_type === "AGGREGATE")}
          selectedIds={selectedAggRuleIds}
          onSelectionChange={setSelectedAggRuleIds}
          onClose={() => setAggModalOpen(false)}
          category="AGGREGATE"
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          notify={notify}
          onRulesChanged={onRulesChanged}
        />
      )}
    </div>
  );
}

const FILTER_OPERATORS = ["=", "!=", ">", ">=", "<", "<=", "IN", "IS NULL", "IS NOT NULL"];
const NULL_FILTER_OPERATORS = new Set(["IS NULL", "IS NOT NULL"]);

function normalizeRowFilterPayload(filter) {
  const operator = String(filter.operator || "=").trim().toUpperCase();
  return {
    ...filter,
    operator,
    value: NULL_FILTER_OPERATORS.has(operator) ? null : filter.value,
  };
}

function FilterSection({ title, schema, filters, setFilters }) {
  const columns = getSchemaColumnNames(schema);
  const add = () => setFilters([...filters, { field: columns[0] || "", operator: "=", value: "" }]);
  const update = (index, key, value) => setFilters(filters.map((item, i) => {
    if (i !== index) return item;
    const next = { ...item, [key]: value };
    if (key === "operator" && NULL_FILTER_OPERATORS.has(String(value).toUpperCase())) {
      next.value = null;
    }
    return next;
  }));
  return <Panel title={title} className="reviewRunCard filterCard">
    <div className="stack">
      {filters.map((item, index) => <div className="formGrid" key={`${title}-${index}`}>
        <SelectField label="Field" value={item.field} setValue={value => update(index, "field", value)} options={["", ...columns]} />
        <SelectField label="Operator" value={item.operator} setValue={value => update(index, "operator", value)} options={FILTER_OPERATORS} />
        {!item.operator.includes("NULL") && <Field label={item.operator === "IN" ? "Values (comma separated)" : "Value"}>
          <input value={Array.isArray(item.value) ? item.value.join(", ") : item.value} onChange={e => update(index, "value", item.operator === "IN" ? e.target.value.split(",").map(v => v.trim()).filter(Boolean) : e.target.value)} />
        </Field>}
        <button type="button" className="secondary" onClick={() => setFilters(filters.filter((_, i) => i !== index))}><X size={14} /> Remove</button>
      </div>)}
      <button type="button" className="secondary" onClick={add} disabled={!columns.length}><Plus size={14} /> Add filter</button>
    </div>
  </Panel>;
}

/* ============================================================
   REVIEW
============================================================ */

function ReviewModal({
  source,
  target,
  levels,
  comparisonKeys,
  sourceFiltersCount,
  targetFiltersCount,
  ignoredColumnsCount,
  mappingsCount,
  dqRulesCount,
  aggregateRulesCount,
  onClose,
  onRun,
  running
}) {
  return (
    <div className="modalBackdrop">
      <div className="modal">
        <div className="modalHead">
          <div>
            <h3>Review & Run</h3>
            <p className="helper">Review your comparison configuration before running</p>
          </div>
          <button type="button" className="iconButton" onClick={onClose} disabled={running}>
            <X size={18} />
          </button>
        </div>

        <div className="modalBody stack">
          <div className="reviewGrid">
            <Panel title="Configuration summary">
              <ReviewRow
                label="Source"
                value={
                  source?.name ||
                  "Not selected"
                }
              />

              <ReviewRow
                label="Target"
                value={
                  target?.name ||
                  "Not selected"
                }
              />

              <ReviewRow
                label="Levels"
                value={levels.join(" · ")}
              />

              <ReviewRow
                label="Record keys"
                value={(comparisonKeys || [])
                  .filter((key) => key.source_column && key.target_column)
                  .map((key) => `${key.source_column} → ${key.target_column}`)
                  .join(", ") || "Not selected"}
              />


              <ReviewRow label="Source filters" value={String(sourceFiltersCount)} />
              <ReviewRow label="Target filters" value={String(targetFiltersCount)} />
              <ReviewRow label="Ignored columns" value={String(ignoredColumnsCount)} />
              <ReviewRow label="Column mappings" value={String(mappingsCount)} />

              <ReviewRow
                label="DQ rules"
                value={String(dqRulesCount)}
              />

              <ReviewRow
                label="Aggregate rules"
                value={String(aggregateRulesCount)}
              />
            </Panel>
          </div>
        </div>

        <div className="modalFooter">
          <button type="button" className="secondary" onClick={onClose} disabled={running}>
            Cancel
          </button>
          <button className="primary" onClick={onRun} disabled={running}>
            {running ? (
              <>
                <Loader2 size={16} className="spin" />
                Executing…
              </>
            ) : (
              <>
                <Zap size={16} />
                Run comparison
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

function ReviewRow({ label, value }) {
  return (
    <div className="reviewRow">
      <span>{label}</span>
      <b>{value}</b>
    </div>
  );
}

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

function Results({ runId, onOpenRun, onBack, onOpenAnalysis, notify }) {
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

function L7AnalysisReportView({
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
  const correlations = report.cross_level_analysis || sanitized.cross_level_correlations || [];
  const privacy = sanitized.privacy_policy || {};

  const execFindings = report.key_findings || [];
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
        {count !== undefined && (
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
  if (r.key) {
    try {
      const parsed = JSON.parse(r.key);
      if (parsed && typeof parsed === "object") return Object.values(parsed).join(" + ");
    } catch {
      return r.key;
    }
  }
  const rec = r.record || r.source_record || r.target_record || (r.source_records ? r.source_records[0] : null) || (r.target_records ? r.target_records[0] : null);
  if (!rec) return r.key || r.signature || r.record_key || r.id || "N/A";
  const keys = ["id", "key", "ID", "Key", "uid", "uuid", "name", "Name", "email", "Email", "customer_id", "Customer_ID"];
  for (let k of keys) {
    if (rec[k] !== undefined) return rec[k];
  }
  return r.key || r.signature || r.record_key || Object.values(rec)[0] || "N/A";
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
          key: "record", label: "Record Key", render: r => {
            if (!r.record) return "Row Data";
            const keys = ["id", "key", "ID", "Key", "uid", "uuid", "name", "Name", "email", "Email", "customer_id", "Customer_ID"];
            for (let k of keys) {
              if (r.record[k] !== undefined) return <CopyableKey text={r.record[k]} />;
            }
            return <CopyableKey text={Object.values(r.record)[0] || "Row Data"} />;
          }
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

/* ============================================================
   HISTORY
============================================================ */

function History({ onOpenRun, notify }) {
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

/* ============================================================
   RULE REPOSITORY
============================================================ */

function RulesPage({ notify }) {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [editingRule, setEditingRule] = useState(null);

  async function loadRules() {
    setLoading(true);
    try {
      const data = await apiRequest("/rules");
      setRules(data || []);
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadRules();
  }, []);

  async function deleteRule(id) {
    if (!window.confirm("Delete this rule permanently?")) return;
    try {
      await apiRequest(`/rules/${id}`, { method: "DELETE" });
      notify("Rule deleted", "success");
      loadRules();
    } catch (error) {
      notify(error.message, "error");
    }
  }

  const dqRules = rules.filter((r) => r.rule_type === "DQ");
  const aggRules = rules.filter((r) => r.rule_type === "AGGREGATE");

  return (
    <div className="stack rulesRepositoryPage">
      <div className="wizardFooter">
        <h1 className="pageTitle" style={{ margin: 0 }}>Rule Repository</h1>
        <button className="primary" onClick={() => { setEditingRule(null); setModalOpen(true); }}>
          <Plus size={16} /> New rule
        </button>
      </div>

      <div className="rulesRepositorySections">
        <Panel title={`Data quality (${dqRules.length})`} className="ruleRepositorySection">
          {!dqRules.length ? (
            <Empty icon={ShieldCheck} title="No rules" text="Create DQ rules to validate field patterns." />
          ) : (
            <div className="tableWrapper ruleRepositoryTable">
              <table className="dataTable">
                <thead><tr><th>Name</th><th>Type</th><th>Column</th><th>Actions</th></tr></thead>
                <tbody>
                  {dqRules.map(r => (
                    <tr key={r.rule_id}>
                      <td><b>{r.name}</b></td>
                      <td><span className="typeTag">{r.payload.rule_type}</span></td>
                      <td>{describeDqRule(r.payload)}</td>
                      <td>
                        <div className="actionRow">
                          <button type="button" className="iconButton" title="Inspect or edit rule" onClick={() => { setEditingRule(r); setModalOpen(true); }}><Eye size={14} /></button>
                          <button type="button" className="iconButton dangerIcon" title="Delete rule" onClick={() => deleteRule(r.rule_id)}><Trash2 size={14} /></button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Panel>

        <Panel title={`Aggregate (${aggRules.length})`} className="ruleRepositorySection">
          {!aggRules.length ? (
            <Empty icon={SlidersHorizontal} title="No rules" text="Create Aggregate rules for sums/counts." />
          ) : (
            <div className="tableWrapper ruleRepositoryTable">
              <table className="dataTable">
                <thead><tr><th>Name</th><th>Fn</th><th>Columns</th><th>Actions</th></tr></thead>
                <tbody>
                  {aggRules.map(r => (
                    <tr key={r.rule_id}>
                      <td><b>{r.name}</b></td>
                      <td>{r.payload.function}</td>
                      <td>{r.payload.source_column} → {r.payload.target_column}</td>
                      <td>
                        <div className="actionRow">
                          <button type="button" className="iconButton" title="Inspect or edit rule" onClick={() => { setEditingRule(r); setModalOpen(true); }}><Eye size={14} /></button>
                          <button type="button" className="iconButton dangerIcon" title="Delete rule" onClick={() => deleteRule(r.rule_id)}><Trash2 size={14} /></button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Panel>
      </div>

      {modalOpen && (
        <RuleModal
          existingRule={editingRule}
          onClose={() => setModalOpen(false)}
          onDone={() => { setModalOpen(false); loadRules(); }}
          notify={notify}
        />
      )}
    </div>
  );
}

function RuleModal({ existingRule, initialRuleType, sourceSchema, targetSchema, onClose, onDone, notify }) {
  const dqRuleTypes = [
    ["PATTERN", "Pattern"],
    ["COMPLETENESS", "Completeness"],
    ["VALIDITY", "Validity"],
  ];
  const [ruleType, setRuleType] = useState(existingRule?.rule_type || initialRuleType || "DQ");
  const [name, setName] = useState(existingRule?.name || "");
  const [payload, setPayload] = useState(existingRule?.payload || {
    rule_id: `RULE-${Math.floor(Math.random() * 10000)}`,
    name: "", rule_type: "PATTERN", apply_to: "BOTH", source_column: "", target_column: "", regex: "", enabled: true
  });
  const [saving, setSaving] = useState(false);
  const categoryLocked = Boolean(initialRuleType);
  const schemaAware = Boolean(initialRuleType && (Array.isArray(sourceSchema) || Array.isArray(targetSchema)));
  const sourceColumnOptions = getSchemaColumnNames(sourceSchema);
  const targetColumnOptions = getSchemaColumnNames(targetSchema);
  const dqApplyTo = String(payload.apply_to || "BOTH").toUpperCase();
  const sourceRuleOptions = schemaRuleOptions(sourceSchema, payload.source_column || payload.column, schemaAware);
  const targetRuleOptions = schemaRuleOptions(targetSchema, payload.target_column || payload.column, schemaAware);

  // When switching top-level rule type
  useEffect(() => {
    if (!existingRule) {
      if (ruleType === "DQ") {
        const defaultSourceColumn = sourceColumnOptions[0] || "";
        const defaultTargetColumn = targetColumnOptions.includes(defaultSourceColumn)
          ? defaultSourceColumn
          : targetColumnOptions[0] || "";
        setPayload({ rule_id: `RULE-${Math.floor(Math.random() * 10000)}`, name, rule_type: "PATTERN", apply_to: "BOTH", source_column: defaultSourceColumn, target_column: defaultTargetColumn, regex: "", enabled: true });
      } else {
        const defaultSourceColumn = sourceColumnOptions[0] || "";
        const defaultTargetColumn = targetColumnOptions.includes(defaultSourceColumn)
          ? defaultSourceColumn
          : targetColumnOptions[0] || "";
        setPayload({ name, source_column: defaultSourceColumn, target_column: defaultTargetColumn, function: "SUM", group_by_columns: [], enabled: true });
      }
    }
  }, [ruleType]);

  useEffect(() => {
    if (existingRule || !schemaAware) return;

    setPayload((current) => {
      if (ruleType === "DQ") {
        const sourceColumn = sourceColumnOptions.includes(current.source_column)
          ? current.source_column
          : sourceColumnOptions[0] || "";
        const targetColumn = targetColumnOptions.includes(current.target_column)
          ? current.target_column
          : targetColumnOptions.includes(sourceColumn)
            ? sourceColumn
            : targetColumnOptions[0] || "";

        const next = {
          ...current,
          source_column: sourceColumn,
          target_column: targetColumn,
        };

        return rowsEqual(current, next) ? current : next;
      }

      const sourceColumn = sourceColumnOptions.includes(current.source_column)
        ? current.source_column
        : sourceColumnOptions[0] || "";
      const targetColumn = targetColumnOptions.includes(current.target_column)
        ? current.target_column
        : targetColumnOptions.includes(sourceColumn)
          ? sourceColumn
          : targetColumnOptions[0] || "";
      const next = {
        ...current,
        source_column: sourceColumn,
        target_column: targetColumn,
        source_group_by: (current.source_group_by || [])
          .filter((column) => sourceColumnOptions.includes(column)),
        target_group_by: (current.target_group_by || [])
          .filter((column) => targetColumnOptions.includes(column)),
      };

      return rowsEqual(current, next) ? current : next;
    });
  }, [ruleType, schemaAware, sourceColumnOptions.join("|"), targetColumnOptions.join("|")]);

  // Keep name in sync
  useEffect(() => {
    if (name !== payload.name) setPayload({ ...payload, name });
  }, [name]);

  async function save(e) {
    e.preventDefault();
    setSaving(true);
    try {
      let payloadToSave = { ...payload, name };

      if (ruleType === "DQ") {
        const applyTo = String(payloadToSave.apply_to || "BOTH").toUpperCase();
        const needsSource = applyTo === "SOURCE" || applyTo === "BOTH";
        const needsTarget = applyTo === "TARGET" || applyTo === "BOTH";

        if (needsSource && !payloadToSave.source_column) {
          throw new Error("Select a source field for this DQ rule.");
        }

        if (needsTarget && !payloadToSave.target_column) {
          throw new Error("Select a target field for this DQ rule.");
        }

        if (
          schemaAware &&
          needsSource &&
          !sourceColumnOptions.includes(payloadToSave.source_column)
        ) {
          throw new Error("Selected source field is not in the current source dataset.");
        }

        if (
          schemaAware &&
          needsTarget &&
          !targetColumnOptions.includes(payloadToSave.target_column)
        ) {
          throw new Error("Selected target field is not in the current target dataset.");
        }

        payloadToSave = {
          ...payloadToSave,
          rule_type: String(payloadToSave.rule_type || "PATTERN").toUpperCase(),
          apply_to: applyTo,
        };

        if (payloadToSave.rule_type === "VALIDITY") {
          if (!String(payloadToSave.regex || "").trim()) delete payloadToSave.regex;
          if (!Array.isArray(payloadToSave.allowed_values) || payloadToSave.allowed_values.length === 0) delete payloadToSave.allowed_values;
          delete payloadToSave.condition;
          delete payloadToSave.check;
          delete payloadToSave.transformation;
        }

        delete payloadToSave.column;

        if (applyTo === "SOURCE") {
          delete payloadToSave.target_column;
        } else if (applyTo === "TARGET") {
          delete payloadToSave.source_column;
        }
      }

      if (ruleType === "AGGREGATE" && schemaAware) {
        if (!payloadToSave.source_column || !sourceColumnOptions.includes(payloadToSave.source_column)) {
          throw new Error("Select a source column for this aggregate rule.");
        }

        if (!payloadToSave.target_column || !targetColumnOptions.includes(payloadToSave.target_column)) {
          throw new Error("Select a target column for this aggregate rule.");
        }

        const sourceGroupBy = (payloadToSave.source_group_by || [])
          .filter(Boolean);
        const targetGroupBy = (payloadToSave.target_group_by || [])
          .filter(Boolean);

        if (sourceGroupBy.length !== targetGroupBy.length) {
          throw new Error("Select both source and target group-by columns, or leave both blank.");
        }

        if (
          sourceGroupBy.some((column) => !sourceColumnOptions.includes(column))
        ) {
          throw new Error("Selected source group-by column is not in the current source dataset.");
        }

        if (
          targetGroupBy.some((column) => !targetColumnOptions.includes(column))
        ) {
          throw new Error("Selected target group-by column is not in the current target dataset.");
        }

        payloadToSave.source_group_by = sourceGroupBy;
        payloadToSave.target_group_by = targetGroupBy;
        payloadToSave.group_by_columns = [];
      }

      const body = { name, rule_type: ruleType, payload: payloadToSave };
      if (existingRule) {
        await apiRequest(`/rules/${existingRule.rule_id}`, { method: "PUT", body: JSON.stringify(body) });
        notify("Rule updated");
      } else {
        await apiRequest("/rules", { method: "POST", body: JSON.stringify(body) });
        notify("Rule created");
      }
      onDone();
    } catch (err) {
      notify(err.message, "error");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="modalBackdrop" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modalHead">
          <h2>{existingRule ? "Edit Rule" : "Add Rule"}</h2>
          <button type="button" className="iconButton" onClick={onClose}><X size={18} /></button>
        </div>
        <form onSubmit={save} className="modalBody stack">
          {!existingRule && !categoryLocked && (
            <SelectField label="Rule Category" value={ruleType} setValue={setRuleType} options={["DQ", "AGGREGATE"]} />
          )}
          <Field label="Rule Name" required><input value={name} onChange={e => setName(e.target.value)} required /></Field>

          {ruleType === "DQ" && (
            <>
              <SelectField label="Validation Type" value={String(payload.rule_type || "PATTERN").toUpperCase()} setValue={v => setPayload({ ...payload, rule_type: v })} options={dqRuleTypes} />
              <SelectField
                label="Apply to"
                value={dqApplyTo}
                setValue={v => setPayload({
                  ...payload,
                  apply_to: v,
                  source_column: v === "TARGET" ? payload.source_column : payload.source_column || payload.column || "",
                  target_column: v === "SOURCE" ? payload.target_column : payload.target_column || payload.column || "",
                })}
                options={["SOURCE", "TARGET", "BOTH"]}
              />
              {(dqApplyTo === "SOURCE" || dqApplyTo === "BOTH") && (
                <Field label="Source field" required>
                  <input value={payload.source_column || payload.column || ""} onChange={e => setPayload({ ...payload, source_column: e.target.value })} required />
                </Field>
              )}
              {(dqApplyTo === "TARGET" || dqApplyTo === "BOTH") && (
                <Field label="Target field" required>
                  <input value={payload.target_column || payload.column || ""} onChange={e => setPayload({ ...payload, target_column: e.target.value })} required />
                </Field>
              )}
              <Field label="Tolerance">
                <input type="number" min="0" step="any" value={payload.tolerance ?? ""} onChange={e => setPayload({ ...payload, tolerance: e.target.value === "" ? undefined : Number(e.target.value) })} />
              </Field>
              {String(payload.rule_type).toUpperCase() === "PATTERN" && <Field label="Regex Pattern" required><input value={payload.regex || ""} onChange={e => setPayload({ ...payload, regex: e.target.value })} required /></Field>}
              {String(payload.rule_type).toUpperCase() === "VALIDITY" && <>
                <Field label="Allowed values"><input value={(payload.allowed_values || []).join(", ")} onChange={e => setPayload({ ...payload, allowed_values: e.target.value.split(",").map(v => v.trim()).filter(Boolean) })} /></Field>
                <div className="grid2"><Field label="Minimum"><input type="number" value={payload.min ?? ""} onChange={e => setPayload({ ...payload, min: e.target.value === "" ? undefined : Number(e.target.value) })} /></Field><Field label="Maximum"><input type="number" value={payload.max ?? ""} onChange={e => setPayload({ ...payload, max: e.target.value === "" ? undefined : Number(e.target.value) })} /></Field></div>
                <Field label="Regex"><input value={payload.regex || ""} onChange={e => setPayload({ ...payload, regex: e.target.value })} /></Field>
              </>}
            </>
          )}

          {ruleType === "AGGREGATE" && (
            <>
              <SelectField label="Function" value={payload.function} setValue={v => setPayload({ ...payload, function: v })} options={["SUM", "COUNT", "AVG", "MIN", "MAX"]} />
              <div className="grid2">
                {schemaAware ? (
                  <>
                    <SelectField label="Source Column" value={payload.source_column || ""} setValue={v => setPayload({ ...payload, source_column: v })} options={sourceRuleOptions} />
                    <SelectField label="Target Column" value={payload.target_column || ""} setValue={v => setPayload({ ...payload, target_column: v })} options={targetRuleOptions} />
                  </>
                ) : (
                  <>
                    <Field label="Source Column" required><input value={payload.source_column} onChange={e => setPayload({ ...payload, source_column: e.target.value })} required /></Field>
                    <Field label="Target Column" required><input value={payload.target_column} onChange={e => setPayload({ ...payload, target_column: e.target.value })} required /></Field>
                  </>
                )}
              </div>
              <Field label="Tolerance (%)">
                <input type="number" min="0" step="any" value={payload.tolerance_pct === undefined ? "" : payload.tolerance_pct} onChange={e => setPayload({ ...payload, tolerance_pct: e.target.value ? Number(e.target.value) : undefined })} />
              </Field>
              <div className="grid2">
                {schemaAware ? (
                  <>
                    <SelectField
                      label="Source Group By"
                      value={payload.source_group_by?.[0] || ""}
                      setValue={v => setPayload({ ...payload, source_group_by: v ? [v] : [] })}
                      options={sourceRuleOptions}
                    />
                    <SelectField
                      label="Target Group By"
                      value={payload.target_group_by?.[0] || ""}
                      setValue={v => setPayload({ ...payload, target_group_by: v ? [v] : [] })}
                      options={targetRuleOptions}
                    />
                  </>
                ) : (
                  <>
                    <Field label="Source Group By"><input value={payload.source_group_by?.[0] || ""} onChange={e => setPayload({ ...payload, source_group_by: e.target.value ? [e.target.value] : [] })} /></Field>
                    <Field label="Target Group By"><input value={payload.target_group_by?.[0] || ""} onChange={e => setPayload({ ...payload, target_group_by: e.target.value ? [e.target.value] : [] })} /></Field>
                  </>
                )}
              </div>
            </>
          )}

          <div className="modalFooter">
            <button type="button" className="secondary" onClick={onClose}>Cancel</button>
            <button type="submit" className="primary" disabled={saving}>
              {saving ? <Loader2 size={15} className="spin" /> : <Check size={15} />} Save Rule
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

/* ============================================================
   SHARED UI
============================================================ */

function Metric({
  label,
  value,
  sub,
  icon: Icon,
}) {
  return (
    <div className="metric">
      <div className="metricIcon">
        <Icon size={17} />
      </div>

      <span>{label}</span>

      <strong>{value}</strong>

      <small>{sub}</small>
    </div>
  );
}

function Panel({
  title,
  action,
  children,
  className = "",
  collapsible = false,
  defaultExpanded = true,
}) {
  const [expanded, setExpanded] = useState(defaultExpanded);

  return (
    <section className={`panel ${className}`.trim()}>
      {(title || action || collapsible) && (
        <div
          className="panelHead"
          style={collapsible ? { cursor: "pointer" } : {}}
          onClick={() => collapsible && setExpanded(!expanded)}
        >
          <div>
            <h3>{title}</h3>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            {action}
            {collapsible && (
              <span style={{ color: "#a0aec0", display: "flex" }}>
                {expanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
              </span>
            )}
          </div>
        </div>
      )}

      {(!collapsible || expanded) && children}
    </section>
  );
}

function ConnectionLine({
  connection,
}) {
  const Icon =
    CONNECTORS[
      connection.connector_type
    ]?.icon || Database;

  return (
    <div className="connLine">
      <div className="sourceIcon">
        <Icon size={16} />
      </div>

      <div className="grow">
        <b>{connection.name}</b>

        <span>
          {
            CONNECTORS[
              connection.connector_type
            ]?.label ||
            connection.connector_type
          }
        </span>
      </div>

      <Status
        status={connection.status}
      />
    </div>
  );
}

function Status({ status }) {
  const normalized = String(
    status || "UNKNOWN"
  ).toLowerCase();

  return (
    <span
      className={`status ${normalized}`}
    >
      <i />
      {status || "UNKNOWN"}
    </span>
  );
}

function Empty({
  icon: Icon,
  title,
  text,
}) {
  return (
    <div className="empty">
      <Icon size={22} />

      <b>{title}</b>

      <span>{text}</span>
    </div>
  );
}

function Loading({ text }) {
  return (
    <div className="loading">
      <Loader2
        className="spin"
        size={20}
      />

      {text}
    </div>
  );
}

function Field({
  label,
  required = false,
  children,
}) {
  return (
    <label className="field">
      <span>
        {label}

        {required && <em>*</em>}
      </span>

      {children}
    </label>
  );
}

function SelectField({
  label,
  value,
  setValue,
  options,
}) {
  return (
    <Field label={label}>
      <select
        value={value}
        onChange={(event) =>
          setValue(event.target.value)
        }
      >
        {options.map((option) => (
          <option
            key={Array.isArray(option) ? option[0] : option}
            value={Array.isArray(option) ? option[0] : option}
          >
            {Array.isArray(option) ? option[1] : option}
          </option>
        ))}
      </select>
    </Field>
  );
}

function WizardItem({
  number,
  label,
  active,
  complete,
  onClick,
}) {
  return (
    <button
      type="button"
      className={`wizardItem ${active ? "active" : ""
        } ${complete ? "done" : ""}`}
      onClick={onClick}
    >
      <span>
        {complete ? (
          <Check size={13} />
        ) : (
          number
        )}
      </span>

      <b>{label}</b>
    </button>
  );
}

function Toast({
  message,
  type,
  onClose,
}) {
  return (
    <div className={`toast ${type}`}>
      <span>
        {type === "error" ? (
          <TriangleAlert size={17} />
        ) : (
          <Check size={17} />
        )}
      </span>

      {message}

      <button onClick={onClose}>
        <X size={14} />
      </button>
    </div>
  );
}

/* ============================================================
   MOUNT APPLICATION
============================================================ */

createRoot(
  document.getElementById("root")
).render(<App />);
