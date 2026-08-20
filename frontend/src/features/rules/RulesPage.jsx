import React, { useEffect, useState } from "react";
import { Check, Eye, Loader2, Plus, ShieldCheck, SlidersHorizontal, Trash2, X } from "lucide-react";

import { apiRequest } from "../../api/client";
import { Empty, Field, Panel, SelectField } from "../../components/ui";
import { getSchemaColumnNames, rowsEqual } from "../../utils/schema";
import { describeDqRule, schemaRuleOptions } from "../comparisons/ComparisonBuilder";

/* ============================================================
   RULE REPOSITORY
============================================================ */

export function RulesPage({ notify }) {
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
                schemaAware ? (
                  <SelectField
                    label="Source field"
                    required
                    value={payload.source_column || payload.column || ""}
                    setValue={value => setPayload({ ...payload, source_column: value })}
                    options={sourceRuleOptions}
                  />
                ) : (
                  <Field label="Source field" required>
                    <input value={payload.source_column || payload.column || ""} onChange={e => setPayload({ ...payload, source_column: e.target.value })} required />
                  </Field>
                )
              )}
              {(dqApplyTo === "TARGET" || dqApplyTo === "BOTH") && (
                schemaAware ? (
                  <SelectField
                    label="Target field"
                    required
                    value={payload.target_column || payload.column || ""}
                    setValue={value => setPayload({ ...payload, target_column: value })}
                    options={targetRuleOptions}
                  />
                ) : (
                  <Field label="Target field" required>
                    <input value={payload.target_column || payload.column || ""} onChange={e => setPayload({ ...payload, target_column: e.target.value })} required />
                  </Field>
                )
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
