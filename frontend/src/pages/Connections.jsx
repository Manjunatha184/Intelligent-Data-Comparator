import React, { useState } from "react";
import { Database, Link2, Loader2, Plus, RefreshCw, Trash2 } from "lucide-react";
import { apiRequest } from "../api/client.js";
import { CONNECTORS } from "../config/comparison.js";
import Empty from "../components/ui/Empty.jsx";
import Loading from "../components/ui/Loading.jsx";
import Panel from "../components/ui/Panel.jsx";
import Status from "../components/ui/Status.jsx";

export default function Connections({ connections, loading, reload, onAdd, notify }) {
  const [testingId, setTestingId] = useState(null);

  async function testConnection(connectionId) {
    setTestingId(connectionId);
    try {
      const result = await apiRequest(`/connections/${connectionId}/test`, { method: "POST" });
      notify(result.message || "Connection test successful.");
      await reload();
    } catch (error) {
      notify(error.message, "error");
      await reload();
    } finally {
      setTestingId(null);
    }
  }

  async function deleteConnection(connectionId) {
    if (!window.confirm("Delete this connection?")) return;
    try {
      await apiRequest(`/connections/${connectionId}`, { method: "DELETE" });
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
          <button className="secondary" onClick={reload}>
            <RefreshCw size={15} /> Refresh
          </button>
          <button className="primary" onClick={onAdd}>
            <Plus size={16} /> Add connection
          </button>
        </div>
      </div>

      <div className="connectorCards">
        {Object.entries(CONNECTORS).map(([connectorType, connector]) => {
          const Icon = connector.icon;
          const count = connections.filter((connection) => connection.connector_type === connectorType).length;
          return (
            <div className="connectorCard" key={connectorType}>
              <div className="sourceIcon large"><Icon size={19} /></div>
              <div className="grow"><b>{connector.label}</b><span>{connector.description}</span></div>
              <strong>{count}</strong>
            </div>
          );
        })}
      </div>

      <Panel title="Configured connections" action={<span className="countPill">{connections.length} total</span>}>
        {loading ? (
          <Loading text="Loading connections…" />
        ) : connections.length === 0 ? (
          <Empty icon={Link2} title="No connections configured" text="Add CSV or Databricks connections to make them available in the comparison builder." />
        ) : (
          <div className="connectionTable">
            <div className="thead"><span>Connection</span><span>Type</span><span>Status</span><span>ID</span><span /></div>
            {connections.map((connection) => {
              const ConnectorIcon = CONNECTORS[connection.connector_type]?.icon || Database;
              return (
                <div className="trow" key={connection.connection_id}>
                  <div className="nameCell">
                    <div className="sourceIcon"><ConnectorIcon size={15} /></div>
                    <div><b>{connection.name}</b><span>{connection.connector_type}</span></div>
                  </div>
                  <div><span className="typeTag">{CONNECTORS[connection.connector_type]?.label}</span></div>
                  <div><Status status={connection.status} /></div>
                  <code>#{connection.connection_id}</code>
                  <div className="rowButtons">
                    <button onClick={() => testConnection(connection.connection_id)} disabled={testingId === connection.connection_id}>
                      {testingId === connection.connection_id ? <Loader2 className="spin" size={14} /> : <RefreshCw size={14} />} Test
                    </button>
                    <button className="danger" onClick={() => deleteConnection(connection.connection_id)}>
                      <Trash2 size={14} /> Delete
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
