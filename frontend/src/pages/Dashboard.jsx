import React from "react";
import { ArrowRight, Database, GitCompare, Link2, Plus, Zap } from "lucide-react";
import { CONNECTORS } from "../config/comparison.js";
import Empty from "../components/ui/Empty.jsx";
import Panel from "../components/ui/Panel.jsx";
import Status from "../components/ui/Status.jsx";

function Metric({ label, value, sub, icon: Icon }) {
  return (
    <div className="metric">
      <div className="metricIcon"><Icon size={17} /></div>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{sub}</small>
    </div>
  );
}

function ConnectionLine({ connection }) {
  const Icon = CONNECTORS[connection.connector_type]?.icon || Database;
  return (
    <div className="connLine">
      <div className="sourceIcon"><Icon size={16} /></div>
      <div className="grow">
        <b>{connection.name}</b>
        <span>{CONNECTORS[connection.connector_type]?.label || connection.connector_type}</span>
      </div>
      <Status status={connection.status} />
    </div>
  );
}

export default function Dashboard({ connections, onNewComparison, onConnections }) {
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
        <Metric label="Authenticated connections" value={connections.length} sub={`${connectedCount} currently connected`} icon={Link2} />
        <Metric label="Comparison levels" value="6" sub="L1 Schema → L6 DQ" icon={GitCompare} />
        <Metric label="Connector types" value="2" sub="CSV · Databricks" icon={Database} />
        <Metric label="Execution engine" value="Ready" sub="Planner + task execution" icon={Zap} />
      </div>

      <div className="grid2">
        <Panel title="How the platform works">
          <div className="steps">
            {[
              ["01", "Connect", "Save and test reusable source connections."],
              ["02", "Configure", "Choose sources, keys, levels and rules."],
              ["03", "Execute", "Planner creates only the tasks you selected."],
              ["04", "Review", "Inspect metrics and comparison evidence."],
            ].map(([number, title, description]) => (
              <div className="step" key={number}>
                <b>{number}</b>
                <div><strong>{title}</strong><span>{description}</span></div>
                <ArrowRight size={15} />
              </div>
            ))}
          </div>
        </Panel>

        <Panel
          title="Connection health"
          action={<button className="textBtn" onClick={onConnections}>Open manager</button>}
        >
          {connections.length === 0 ? (
            <Empty icon={Link2} title="No connections yet" text="Add a source to start building comparisons." />
          ) : (
            connections.slice(0, 5).map((connection) => (
              <ConnectionLine key={connection.connection_id} connection={connection} />
            ))
          )}
        </Panel>
      </div>
    </div>
  );
}
