import React from "react";
import { Check, Loader2, X } from "lucide-react";

function connectionName(connection) { return connection?.name || `Connection #${connection?.connection_id || "—"}`; }

export default function ReviewModal({ source, target, levels = [], comparisonKeys = [], sourceFiltersCount = 0, targetFiltersCount = 0, ignoredColumnsCount = 0, mappingsCount = 0, dqRulesCount = 0, aggregateRulesCount = 0, onClose, onRun, running }) {
  const key = comparisonKeys?.[0];
  const items = [
    ["Source", connectionName(source)],
    ["Target", connectionName(target)],
    ["Levels", levels.join(", ") || "None"],
    ["Primary key", key?.source_column && key?.target_column ? `${key.source_column} → ${key.target_column}` : "Not configured"],
    ["Field mappings", String(mappingsCount)],
    ["Source filters", String(sourceFiltersCount)],
    ["Target filters", String(targetFiltersCount)],
    ["Ignored columns", String(ignoredColumnsCount)],
    ["Aggregate rules", String(aggregateRulesCount)],
    ["DQ rules", String(dqRulesCount)],
  ];

  return <div className="modalBackdrop"><div className="modal wide reviewModal">
    <div className="modalHead"><div><span className="sectionEyebrow">FINAL REVIEW</span><h3>Review comparison</h3><p>Confirm the configuration before execution.</p></div><button type="button" className="iconButton" onClick={onClose} disabled={running}><X size={18} /></button></div>
    <div className="reviewGrid">{items.map(([label, value]) => <div className="reviewItem" key={label}><span>{label}</span><b>{value}</b></div>)}</div>
    <div className="modalFooter"><button type="button" className="secondary" onClick={onClose} disabled={running}>Back to configuration</button><button type="button" className="primary" onClick={onRun} disabled={running}>{running ? <><Loader2 className="spin" size={15} /> Running comparison…</> : <><Check size={15} /> Run comparison</>}</button></div>
  </div></div>;
}
