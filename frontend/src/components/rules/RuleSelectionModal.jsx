import React, { useMemo, useState } from "react";
import { Check, Plus } from "lucide-react";
import Modal from "../ui/Modal.jsx";
import RuleModal from "./RuleModal.jsx";

export default function RuleSelectionModal({ title, ruleType, rules = [], selectedIds = [], sourceSchema, targetSchema, onClose, onApply, onCreated, notify }) {
  const [draft, setDraft] = useState(selectedIds);
  const [creating, setCreating] = useState(false);
  const available = useMemo(() => rules.filter((rule) => rule.rule_type === ruleType), [rules, ruleType]);

  function toggle(id) {
    setDraft((current) => current.includes(id) ? current.filter((item) => item !== id) : [...current, id]);
  }

  if (creating) {
    return <RuleModal
      initialRuleType={ruleType}
      sourceSchema={sourceSchema}
      targetSchema={targetSchema}
      notify={notify}
      onClose={() => setCreating(false)}
      onDone={() => { setCreating(false); onCreated?.(); }}
    />;
  }

  const footer = <>
    <button type="button" className="secondary" onClick={onClose}>Cancel</button>
    <button type="button" className="primary" onClick={() => onApply(draft)}><Check size={15} /> Apply selection</button>
  </>;

  return <Modal title={title} onClose={onClose} footer={footer} className="ruleSelectionModal">
    <div className="ruleSelectionToolbar">
      <span>{draft.length} selected</span>
      <button type="button" className="secondary small" onClick={() => setCreating(true)}><Plus size={14} /> Create new</button>
    </div>
    <div className="ruleSelectionList">
      {!available.length ? <div className="emptyRuleSelection">No {ruleType === "DQ" ? "data quality" : "aggregate"} rules yet.</div> : available.map((rule) => {
        const selected = draft.includes(rule.rule_id);
        return <button type="button" key={rule.rule_id} className={`ruleSelectionItem ${selected ? "selected" : ""}`} onClick={() => toggle(rule.rule_id)}>
          <span className="ruleSelectionCheck">{selected && <Check size={13} />}</span>
          <span><b>{rule.name}</b><small>{ruleType === "DQ" ? rule.payload?.rule_type : rule.payload?.function}</small></span>
        </button>;
      })}
    </div>
  </Modal>;
}
