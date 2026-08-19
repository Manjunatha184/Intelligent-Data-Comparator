import React from "react";
import { Plus, X } from "lucide-react";

function describeDqRule(payload = {}) {
  const applyTo = String(payload.apply_to || "BOTH").toUpperCase();
  const sourceColumn = payload.source_column || payload.column;
  const targetColumn = payload.target_column || payload.column;
  const scope = applyTo === "SOURCE" ? sourceColumn : applyTo === "TARGET" ? targetColumn : `${sourceColumn} → ${targetColumn}`;
  return `${String(payload.rule_type || payload.type || "rule").toLowerCase()} on ${scope}`;
}

export default function RuleSelectionModal({ title, rules = [], selectedIds = [], onSelectionChange, onClose, category, onCreateRule }) {
  return (
    <div className="modalBackdrop">
      <div className="modal">
        <div className="modalHead">
          <div><h3>{title}</h3><p className="helper">Select rules from the repository</p></div>
          <div className="actionRow">
            {category && onCreateRule && <button type="button" className="secondary small" onClick={onCreateRule}><Plus size={14} /> New rule</button>}
            <button type="button" className="iconButton" onClick={onClose}><X size={18} /></button>
          </div>
        </div>
        <div className="modalBody stack ruleSelectionBody">
          {!rules.length ? <div className="empty compact"><b>No rules found</b></div> : (
            <div className="ruleTable">
              {rules.map((rule) => {
                const selected = selectedIds.some((id) => String(id) === String(rule.rule_id));
                return <label key={rule.rule_id} className="ruleCheckbox">
                  <input type="checkbox" checked={selected} onChange={(event) => onSelectionChange(event.target.checked ? [...selectedIds, rule.rule_id] : selectedIds.filter((id) => String(id) !== String(rule.rule_id)))} />
                  <div><b>{rule.name}</b><span>{rule.rule_type === "DQ" ? describeDqRule(rule.payload) : `${String(rule.payload?.function || rule.payload?.operation || "aggregate").toLowerCase()} on ${String(rule.payload?.source_column || "configured field").toLowerCase()}`}</span></div>
                </label>;
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
