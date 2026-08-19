import React, { useState } from "react";
import RuleSelectionModal from "./RuleSelectionModal.jsx";

function RuleCard({ title, description, count, onOpen }) {
  return <button type="button" className="ruleSelectionCard" onClick={onOpen}><div><b>{title}</b><span>{description}</span></div><strong>{count}</strong></button>;
}

export default function RuleSelection({ availableRules = [], selectedDqRuleIds, setSelectedDqRuleIds, selectedAggRuleIds, setSelectedAggRuleIds, showDq = true, showAggregate = true }) {
  const [modal, setModal] = useState(null);
  const dqRules = availableRules.filter((rule) => rule.rule_type === "DQ");
  const aggregateRules = availableRules.filter((rule) => rule.rule_type !== "DQ");

  return <section className="comparisonSection">
    <div className="comparisonSectionHead"><div><h3>Validation rules</h3><p className="helper">Attach reusable rules only for the levels selected.</p></div></div>
    <div className="grid2">
      {showDq && <RuleCard title="Data quality rules" description="Rules executed by L6." count={selectedDqRuleIds.length} onOpen={() => setModal("dq")} />}
      {showAggregate && <RuleCard title="Aggregate rules" description="Configured aggregate checks for L5." count={selectedAggRuleIds.length} onOpen={() => setModal("aggregate")} />}
    </div>
    {modal === "dq" && <RuleSelectionModal title="Data quality rules" rules={dqRules} selectedIds={selectedDqRuleIds} onSelectionChange={setSelectedDqRuleIds} onClose={() => setModal(null)} category="DQ" />}
    {modal === "aggregate" && <RuleSelectionModal title="Aggregate rules" rules={aggregateRules} selectedIds={selectedAggRuleIds} onSelectionChange={setSelectedAggRuleIds} onClose={() => setModal(null)} category="AGGREGATE" />}
  </section>;
}
