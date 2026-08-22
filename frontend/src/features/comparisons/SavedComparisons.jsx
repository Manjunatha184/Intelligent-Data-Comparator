import React, { useEffect, useMemo, useState } from "react";
import { Edit3, GitCompare, Loader2, Play, Plus, RefreshCw } from "lucide-react";

import { apiRequest } from "../../api/client";
import { Empty, Loading, Panel } from "../../components/ui";

const LOCAL_COMPARISON_DRAFT_KEY = "lumera.comparison.workspace.v1";

function formatTimestamp(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString();
}

function sourceLabel(configuration) {
  const source = configuration?.source;
  if (!source) return "—";
  const properties = source.properties || {};
  return properties.filename || properties.table || source.connector_type || "Source";
}

function targetLabel(configuration) {
  const target = configuration?.target;
  if (!target) return "—";
  const properties = target.properties || {};
  return properties.filename || properties.table || target.connector_type || "Target";
}

function executablePayload(configuration) {
  if (!configuration || !configuration.source || !configuration.target) return null;
  const { _meta, _workspace, ...payload } = configuration;
  return payload;
}

export function SavedComparisons({ onNew, onEdit, onRunComplete, notify }) {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(false);
  const [rerunningId, setRerunningId] = useState(null);
  const [search, setSearch] = useState("");

  async function loadConfigurations() {
    setLoading(true);
    try {
      const data = await apiRequest("/configurations");
      setItems(Array.isArray(data) ? data : []);
    } catch (error) {
      notify(error.message, "error");
      setItems([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadConfigurations();
  }, []);

  const visibleItems = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return items;
    return items.filter((item) =>
      String(item.name || "").toLowerCase().includes(needle) ||
      String(item.configuration_id || "").includes(needle)
    );
  }, [items, search]);

  function editConfiguration(item) {
    const configuration = item.configuration || {};
    const workspace = configuration._workspace;
    if (!workspace) {
      notify("This older configuration does not contain an editable workspace snapshot.", "error");
      return;
    }

    const restored = {
      ...workspace,
      comparisonName: item.name || workspace.comparisonName || "",
      configurationId: item.configuration_id,
      step: 1,
    };

    window.localStorage.setItem(LOCAL_COMPARISON_DRAFT_KEY, JSON.stringify(restored));
    onEdit(item.configuration_id);
  }

  async function rerunConfiguration(item) {
    const payload = executablePayload(item.configuration);
    if (item.status !== "SAVED" || !payload) {
      notify("Open this draft in Edit mode and complete it before running.", "error");
      return;
    }

    setRerunningId(item.configuration_id);
    try {
      const result = await apiRequest("/comparisons", {
        method: "POST",
        body: JSON.stringify({
          configuration_id: item.configuration_id,
          ...payload,
        }),
      });
      notify(`Comparison ${String(result.status).toLowerCase()}.`);
      onRunComplete(result.run_id);
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setRerunningId(null);
    }
  }

  return (
    <div className="stack savedComparisonsPage">
      <div className="wizardFooter">
        <div>
          <h1 className="pageTitle" style={{ margin: 0 }}>Comparisons</h1>
          <p className="helper savedComparisonsSubtitle">Reusable comparison configurations and drafts.</p>
        </div>
        <div className="actionRow">
          <button type="button" className="secondary" onClick={loadConfigurations} disabled={loading}>
            <RefreshCw size={14} className={loading ? "spin" : ""} /> Refresh
          </button>
          <button type="button" className="primary" onClick={onNew}>
            <Plus size={15} /> New comparison
          </button>
        </div>
      </div>

      <div className="savedComparisonsToolbar">
        <input
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search by comparison name or configuration ID"
          aria-label="Search saved comparisons"
        />
        <span>{visibleItems.length} configuration{visibleItems.length === 1 ? "" : "s"}</span>
      </div>

      <Panel className="savedComparisonsPanel">
        {loading && !items.length ? (
          <Loading text="Loading comparisons…" />
        ) : !visibleItems.length ? (
          <Empty
            icon={GitCompare}
            title={search ? "No matching comparisons" : "No saved comparisons yet"}
            text={search ? "Try another comparison name or configuration ID." : "Create or save a draft to see it here."}
          />
        ) : (
          <div className="savedComparisonsTableWrap">
            <table className="dataTable savedComparisonsTable">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Status</th>
                  <th>Source</th>
                  <th>Target</th>
                  <th>Updated</th>
                  <th>Config ID</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {visibleItems.map((item) => (
                  <tr key={item.configuration_id}>
                    <td><b>{item.name || `Comparison ${item.configuration_id}`}</b></td>
                    <td><span className={`configurationState ${String(item.status).toLowerCase()}`}>{item.status}</span></td>
                    <td title={sourceLabel(item.configuration)}>{sourceLabel(item.configuration)}</td>
                    <td title={targetLabel(item.configuration)}>{targetLabel(item.configuration)}</td>
                    <td>{formatTimestamp(item.updated_at)}</td>
                    <td><code>#{item.configuration_id}</code></td>
                    <td>
                      <div className="savedComparisonActions">
                        <button type="button" className="secondary small" onClick={() => editConfiguration(item)}>
                          <Edit3 size={13} /> Edit
                        </button>
                        <button
                          type="button"
                          className="primary small"
                          disabled={item.status !== "SAVED" || rerunningId === item.configuration_id}
                          onClick={() => rerunConfiguration(item)}
                          title={item.status === "DRAFT" ? "Complete this draft before running" : "Run this saved configuration again"}
                        >
                          {rerunningId === item.configuration_id ? <Loader2 size={13} className="spin" /> : <Play size={13} />}
                          Rerun
                        </button>
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
  );
}
