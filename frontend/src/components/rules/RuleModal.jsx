import React, { useEffect, useMemo, useState } from "react";
import { Loader2, Save } from "lucide-react";
import { apiRequest } from "../../api/client.js";
import { getSchemaColumnNames } from "../../utils/schema.js";
import { normalizeAggregateRulePayload, normalizeDqRulePayload } from "../../utils/rules.js";
import Field from "../ui/Field.jsx";
import Modal from "../ui/Modal.jsx";
import SelectField from "../ui/SelectField.jsx";
import AggregateRuleForm from "./AggregateRuleForm.jsx";
import DqRuleForm from "./DqRuleForm.jsx";

function newPayload(type, sourceColumns, targetColumns) {
  const source = sourceColumns[0] || "";
  const target = targetColumns.includes(source) ? source : targetColumns[0] || "";
  if (type === "AGGREGATE") return { name: "", source_column: source, target_column: target, function: "SUM", source_group_by: [], target_group_by: [], enabled: true };
  return { rule_id: `RULE-${Math.floor(Math.random() * 10000)}`, name: "", rule_type: "PATTERN", apply_to: "BOTH", source_column: source, target_column: target, regex: "", enabled: true };
}

export default function RuleModal({ existingRule, initialRuleType, sourceSchema, targetSchema, onClose, onDone, notify }) {
  const sourceColumns = useMemo(() => getSchemaColumnNames(sourceSchema), [sourceSchema]);
  const targetColumns = useMemo(() => getSchemaColumnNames(targetSchema), [targetSchema]);
  const locked = Boolean(initialRuleType);
  const [ruleType, setRuleType] = useState(existingRule?.rule_type || initialRuleType || "DQ");
  const [name, setName] = useState(existingRule?.name || "");
  const [payload, setPayload] = useState(existingRule?.payload || newPayload(existingRule?.rule_type || initialRuleType || "DQ", sourceColumns, targetColumns));
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!existingRule) setPayload(newPayload(ruleType, sourceColumns, targetColumns));
  }, [ruleType]);

  async function save(event) {
    event.preventDefault();
    if (!name.trim()) return notify("Rule name is required.", "error");
    setSaving(true);
    try {
      let payloadToSave = { ...payload, name: name.trim() };
      if (ruleType === "DQ") {
        payloadToSave = normalizeDqRulePayload(payloadToSave);
        const applyTo = String(payloadToSave.apply_to || "BOTH").toUpperCase();
        if ((applyTo === "SOURCE" || applyTo === "BOTH") && !payloadToSave.source_column) throw new Error("Select a source field for this DQ rule.");
        if ((applyTo === "TARGET" || applyTo === "BOTH") && !payloadToSave.target_column) throw new Error("Select a target field for this DQ rule.");
        if (payloadToSave.rule_type === "PATTERN" && !String(payloadToSave.regex || "").trim()) throw new Error("Enter a regex pattern.");
      } else {
        payloadToSave = normalizeAggregateRulePayload(payloadToSave);
        if (!payloadToSave.source_column || !payloadToSave.target_column) throw new Error("Select source and target columns.");
        if ((payloadToSave.source_group_by || []).length !== (payloadToSave.target_group_by || []).length) throw new Error("Select both source and target group-by columns, or leave both blank.");
      }

      const body = { name: name.trim(), rule_type: ruleType, payload: payloadToSave };
      if (existingRule?.rule_id) await apiRequest(`/rules/${existingRule.rule_id}`, { method: "PUT", body: JSON.stringify(body) });
      else await apiRequest("/rules", { method: "POST", body: JSON.stringify(body) });
      notify(existingRule ? "Rule updated" : "Rule created", "success");
      onDone();
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setSaving(false);
    }
  }

  const footer = <>
    <button type="button" className="secondary" onClick={onClose}>Cancel</button>
    <button type="submit" form="rule-editor-form" className="primary" disabled={saving}>{saving ? <Loader2 size={15} className="spin" /> : <Save size={15} />} {saving ? "Saving…" : "Save rule"}</button>
  </>;

  return (
    <Modal title={existingRule ? "Edit rule" : `New ${ruleType === "DQ" ? "DQ" : "Aggregate"} rule`} onClose={onClose} footer={footer} className="ruleEditorModal">
      <form id="rule-editor-form" onSubmit={save} className="ruleEditorForm">
        {!locked && !existingRule && <SelectField label="Rule category" value={ruleType} setValue={setRuleType} options={[["DQ", "Data Quality"], ["AGGREGATE", "Aggregate"]]} />}
        <Field label="Rule name" required><input value={name} placeholder="Give this rule a clear name" onChange={(event) => setName(event.target.value)} /></Field>
        {ruleType === "DQ" ? <DqRuleForm payload={payload} setPayload={setPayload} sourceColumns={sourceColumns} targetColumns={targetColumns} /> : <AggregateRuleForm payload={payload} setPayload={setPayload} sourceColumns={sourceColumns} targetColumns={targetColumns} />}
      </form>
    </Modal>
  );
}
