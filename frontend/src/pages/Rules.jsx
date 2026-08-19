import React, { useEffect, useState } from "react";
import { Eye, Plus, ShieldCheck, SlidersHorizontal, Trash2 } from "lucide-react";
import { apiRequest } from "../api/client.js";
import Empty from "../components/ui/Empty.jsx";
import Panel from "../components/ui/Panel.jsx";
import RuleModal from "../components/rules/RuleModal.jsx";

function describeDqRule(payload = {}) {
  const applyTo = String(payload.apply_to || "BOTH").toUpperCase();
  if (applyTo === "SOURCE") return payload.source_column || payload.column || "—";
  if (applyTo === "TARGET") return payload.target_column || payload.column || "—";
  const source = payload.source_column || payload.column || "—";
  const target = payload.target_column || payload.column || "—";
  return source === target ? source : `${source} → ${target}`;
}

export default function Rules({ notify }) {
  const [rules, setRules] = useState([]);
  const [modalOpen, setModalOpen] = useState(false);
  const [editingRule, setEditingRule] = useState(null);

  async function loadRules() {
    try {
      setRules((await apiRequest("/rules")) || []);
    } catch (error) {
      notify(error.message, "error");
    }
  }

  useEffect(() => { loadRules(); }, []);

  async function deleteRule(id) {
    if (!window.confirm("Delete this rule permanently?")) return;
    try {
      await apiRequest(`/rules/${id}`, { method: "DELETE" });
      notify("Rule deleted", "success");
      await loadRules();
    } catch (error) {
      notify(error.message, "error");
    }
  }

  const dqRules = rules.filter((rule) => rule.rule_type === "DQ");
  const aggregateRules = rules.filter((rule) => rule.rule_type === "AGGREGATE");

  function edit(rule) { setEditingRule(rule); setModalOpen(true); }

  return <div className="stack rulesRepositoryPage">
    <div className="wizardFooter">
      <h1 className="pageTitle" style={{ margin: 0 }}>Rule Repository</h1>
      <button className="primary" onClick={() => { setEditingRule(null); setModalOpen(true); }}><Plus size={16} /> New rule</button>
    </div>

    <div className="rulesRepositorySections">
      <Panel title={`Data quality (${dqRules.length})`} className="ruleRepositorySection">
        {!dqRules.length ? <Empty icon={ShieldCheck} title="No rules" text="Create DQ rules to validate field patterns." /> : <div className="tableWrapper ruleRepositoryTable"><table className="dataTable">
          <thead><tr><th>Name</th><th>Type</th><th>Column</th><th>Actions</th></tr></thead>
          <tbody>{dqRules.map((rule) => <tr key={rule.rule_id}><td><b>{rule.name}</b></td><td><span className="typeTag">{rule.payload?.rule_type}</span></td><td>{describeDqRule(rule.payload)}</td><td><div className="actionRow"><button type="button" className="iconButton" title="Inspect or edit rule" onClick={() => edit(rule)}><Eye size={14} /></button><button type="button" className="iconButton dangerIcon" title="Delete rule" onClick={() => deleteRule(rule.rule_id)}><Trash2 size={14} /></button></div></td></tr>)}</tbody>
        </table></div>}
      </Panel>

      <Panel title={`Aggregate (${aggregateRules.length})`} className="ruleRepositorySection">
        {!aggregateRules.length ? <Empty icon={SlidersHorizontal} title="No rules" text="Create Aggregate rules for sums/counts." /> : <div className="tableWrapper ruleRepositoryTable"><table className="dataTable">
          <thead><tr><th>Name</th><th>Fn</th><th>Columns</th><th>Actions</th></tr></thead>
          <tbody>{aggregateRules.map((rule) => <tr key={rule.rule_id}><td><b>{rule.name}</b></td><td>{rule.payload?.function}</td><td>{rule.payload?.source_column} → {rule.payload?.target_column}</td><td><div className="actionRow"><button type="button" className="iconButton" title="Inspect or edit rule" onClick={() => edit(rule)}><Eye size={14} /></button><button type="button" className="iconButton dangerIcon" title="Delete rule" onClick={() => deleteRule(rule.rule_id)}><Trash2 size={14} /></button></div></td></tr>)}</tbody>
        </table></div>}
      </Panel>
    </div>

    {modalOpen && <RuleModal existingRule={editingRule} onClose={() => setModalOpen(false)} onDone={() => { setModalOpen(false); loadRules(); }} notify={notify} />}
  </div>;
}
