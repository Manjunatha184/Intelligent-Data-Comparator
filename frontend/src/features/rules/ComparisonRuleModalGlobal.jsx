import React, { useEffect, useState } from "react";
import { Check, Loader2, X } from "lucide-react";

import { apiRequest } from "../../api/client";
import { Field, SelectField } from "../../components/ui";
import { getSchemaColumnNames, rowsEqual } from "../../utils/schema";

function schemaRuleOptions(schema, currentValue, schemaAware) {
  const schemaColumns = getSchemaColumnNames(schema);
  if (schemaAware) return ["", ...schemaColumns];
  return Array.from(new Set(["", ...schemaColumns, currentValue || ""]));
}

function ComparisonRuleModal({
  existingRule,
  initialRuleType,
  sourceSchema,
  targetSchema,
  onClose,
  onDone,
  notify,
}) {
  const dqRuleTypes = [
    ["PATTERN", "Pattern"],
    ["COMPLETENESS", "Completeness"],
    ["VALIDITY", "Validity"],
  ];

  const [ruleType, setRuleType] = useState(existingRule?.rule_type || initialRuleType || "DQ");
  const [name, setName] = useState(existingRule?.name || "");
  const [payload, setPayload] = useState(existingRule?.payload || {
    rule_id: `RULE-${Math.floor(Math.random() * 10000)}`,
    name: "",
    rule_type: "PATTERN",
    apply_to: "BOTH",
    source_column: "",
    target_column: "",
    regex: "",
    enabled: true,
  });
  const [saving, setSaving] = useState(false);

  const categoryLocked = Boolean(initialRuleType);
  const schemaAware = Boolean(initialRuleType && (Array.isArray(sourceSchema) || Array.isArray(targetSchema)));
  const sourceColumnOptions = getSchemaColumnNames(sourceSchema);
  const targetColumnOptions = getSchemaColumnNames(targetSchema);
  const dqApplyTo = String(payload.apply_to || "BOTH").toUpperCase();
  const sourceRuleOptions = schemaRuleOptions(sourceSchema, payload.source_column || payload.column, schemaAware);
  const targetRuleOptions = schemaRuleOptions(targetSchema, payload.target_column || payload.column, schemaAware);

  useEffect(() => {
    if (existingRule) return;

    const defaultSourceColumn = sourceColumnOptions[0] || "";
    const defaultTargetColumn = targetColumnOptions.includes(defaultSourceColumn)
      ? defaultSourceColumn
      : targetColumnOptions[0] || "";

    if (ruleType === "DQ") {
      setPayload({
        rule_id: `RULE-${Math.floor(Math.random() * 10000)}`,
        name,
        rule_type: "PATTERN",
        apply_to: "BOTH",
        source_column: defaultSourceColumn,
        target_column: defaultTargetColumn,
        regex: "",
        enabled: true,
      });
    } else {
      setPayload({
        name,
        source_column: defaultSourceColumn,
        target_column: defaultTargetColumn,
        function: "SUM",
        group_by_columns: [],
        source_group_by: [],
        target_group_by: [],
        enabled: true,
      });
    }
  }, [ruleType]);

  useEffect(() => {
    if (existingRule || !schemaAware) return;

    setPayload((current) => {
      const sourceColumn = sourceColumnOptions.includes(current.source_column)
        ? current.source_column
        : sourceColumnOptions[0] || "";
      const targetColumn = targetColumnOptions.includes(current.target_column)
        ? current.target_column
        : targetColumnOptions.includes(sourceColumn)
          ? sourceColumn
          : targetColumnOptions[0] || "";

      if (ruleType === "DQ") {
        const next = { ...current, source_column: sourceColumn, target_column: targetColumn };
        return rowsEqual(current, next) ? current : next;
      }

      const next = {
        ...current,
        source_column: sourceColumn,
        target_column: targetColumn,
        source_group_by: (current.source_group_by || []).filter((column) => sourceColumnOptions.includes(column)),
        target_group_by: (current.target_group_by || []).filter((column) => targetColumnOptions.includes(column)),
      };
      return rowsEqual(current, next) ? current : next;
    });
  }, [
    ruleType,
    schemaAware,
    sourceColumnOptions.join("|"),
    targetColumnOptions.join("|"),
  ]);

  useEffect(() => {
    setPayload((current) => current.name === name ? current : { ...current, name });
  }, [name]);

  async function save(event) {
    event.preventDefault();
    if (!name.trim()) {
      notify("Enter a rule name.", "error");
      return;
    }

    setSaving(true);
    try {
      let payloadToSave = { ...payload, name: name.trim() };

      if (ruleType === "DQ") {
        const applyTo = String(payloadToSave.apply_to || "BOTH").toUpperCase();
        const needsSource = applyTo === "SOURCE" || applyTo === "BOTH";
        const needsTarget = applyTo === "TARGET" || applyTo === "BOTH";

        if (needsSource && !payloadToSave.source_column) throw new Error("Select a source field for this DQ rule.");
        if (needsTarget && !payloadToSave.target_column) throw new Error("Select a target field for this DQ rule.");
        if (schemaAware && needsSource && !sourceColumnOptions.includes(payloadToSave.source_column)) {
          throw new Error("Selected source field is not in the current source dataset.");
        }
        if (schemaAware && needsTarget && !targetColumnOptions.includes(payloadToSave.target_column)) {
          throw new Error("Selected target field is not in the current target dataset.");
        }

        payloadToSave.rule_type = String(payloadToSave.rule_type || "PATTERN").toUpperCase();
        payloadToSave.apply_to = applyTo;
        delete payloadToSave.column;

        if (payloadToSave.rule_type === "PATTERN" && !String(payloadToSave.regex || "").trim()) {
          throw new Error("Enter a regex pattern for this pattern rule.");
        }

        if (payloadToSave.rule_type === "VALIDITY") {
          if (!String(payloadToSave.regex || "").trim()) delete payloadToSave.regex;
          if (!Array.isArray(payloadToSave.allowed_values) || !payloadToSave.allowed_values.length) delete payloadToSave.allowed_values;
        }

        if (applyTo === "SOURCE") delete payloadToSave.target_column;
        if (applyTo === "TARGET") delete payloadToSave.source_column;
      }

      if (ruleType === "AGGREGATE") {
        if (!payloadToSave.source_column) throw new Error("Select a source column for this aggregate rule.");
        if (!payloadToSave.target_column) throw new Error("Select a target column for this aggregate rule.");

        if (schemaAware && !sourceColumnOptions.includes(payloadToSave.source_column)) {
          throw new Error("Selected source column is not in the current source dataset.");
        }
        if (schemaAware && !targetColumnOptions.includes(payloadToSave.target_column)) {
          throw new Error("Selected target column is not in the current target dataset.");
        }

        const sourceGroupBy = (payloadToSave.source_group_by || []).filter(Boolean);
        const targetGroupBy = (payloadToSave.target_group_by || []).filter(Boolean);
        if (sourceGroupBy.length !== targetGroupBy.length) {
          throw new Error("Select both source and target group-by columns, or leave both blank.");
        }
        payloadToSave.source_group_by = sourceGroupBy;
        payloadToSave.target_group_by = targetGroupBy;
        payloadToSave.group_by_columns = [];
      }

      const body = {
        name: name.trim(),
        rule_type: ruleType,
        payload: payloadToSave,
      };

      if (existingRule) {
        await apiRequest(`/rules/${existingRule.rule_id}`, {
          method: "PUT",
          body: JSON.stringify(body),
        });
        notify("Rule updated");
      } else {
        await apiRequest("/rules", {
          method: "POST",
          body: JSON.stringify(body),
        });
        notify("Rule created");
      }

      onDone?.();
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="modalBackdrop" onClick={onClose}>
      <div className="modal" onClick={(event) => event.stopPropagation()}>
        <div className="modalHead">
          <div>
            <h2>{existingRule ? "Edit Rule" : "Add Rule"}</h2>
            <p>{initialRuleType === "AGGREGATE" ? "Create an aggregate rule for this comparison" : initialRuleType === "DQ" ? "Create a data-quality rule for this comparison" : "Create a reusable rule"}</p>
          </div>
          <button type="button" className="iconButton" onClick={onClose}><X size={18} /></button>
        </div>

        <form onSubmit={save} className="modalBody stack">
          {!existingRule && !categoryLocked && (
            <SelectField label="Rule Category" value={ruleType} setValue={setRuleType} options={["DQ", "AGGREGATE"]} />
          )}

          <Field label="Rule Name" required>
            <input value={name} onChange={(event) => setName(event.target.value)} required />
          </Field>

          {ruleType === "DQ" && (
            <>
              <SelectField
                label="Validation Type"
                value={String(payload.rule_type || "PATTERN").toUpperCase()}
                setValue={(value) => setPayload({ ...payload, rule_type: value })}
                options={dqRuleTypes}
              />
              <SelectField
                label="Apply to"
                value={dqApplyTo}
                setValue={(value) => setPayload({ ...payload, apply_to: value })}
                options={["SOURCE", "TARGET", "BOTH"]}
              />

              {(dqApplyTo === "SOURCE" || dqApplyTo === "BOTH") && (
                schemaAware ? (
                  <SelectField label="Source field" required value={payload.source_column || ""} setValue={(value) => setPayload({ ...payload, source_column: value })} options={sourceRuleOptions} />
                ) : (
                  <Field label="Source field" required><input value={payload.source_column || ""} onChange={(event) => setPayload({ ...payload, source_column: event.target.value })} /></Field>
                )
              )}

              {(dqApplyTo === "TARGET" || dqApplyTo === "BOTH") && (
                schemaAware ? (
                  <SelectField label="Target field" required value={payload.target_column || ""} setValue={(value) => setPayload({ ...payload, target_column: value })} options={targetRuleOptions} />
                ) : (
                  <Field label="Target field" required><input value={payload.target_column || ""} onChange={(event) => setPayload({ ...payload, target_column: event.target.value })} /></Field>
                )
              )}

              <Field label="Tolerance">
                <input type="number" min="0" step="any" value={payload.tolerance ?? ""} onChange={(event) => setPayload({ ...payload, tolerance: event.target.value === "" ? undefined : Number(event.target.value) })} />
              </Field>

              {String(payload.rule_type).toUpperCase() === "PATTERN" && (
                <Field label="Regex Pattern" required>
                  <input value={payload.regex || ""} onChange={(event) => setPayload({ ...payload, regex: event.target.value })} required />
                </Field>
              )}

              {String(payload.rule_type).toUpperCase() === "VALIDITY" && (
                <>
                  <Field label="Allowed values">
                    <input value={(payload.allowed_values || []).join(", ")} onChange={(event) => setPayload({ ...payload, allowed_values: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) })} />
                  </Field>
                  <div className="grid2">
                    <Field label="Minimum"><input type="number" value={payload.min ?? ""} onChange={(event) => setPayload({ ...payload, min: event.target.value === "" ? undefined : Number(event.target.value) })} /></Field>
                    <Field label="Maximum"><input type="number" value={payload.max ?? ""} onChange={(event) => setPayload({ ...payload, max: event.target.value === "" ? undefined : Number(event.target.value) })} /></Field>
                  </div>
                  <Field label="Regex"><input value={payload.regex || ""} onChange={(event) => setPayload({ ...payload, regex: event.target.value })} /></Field>
                </>
              )}
            </>
          )}

          {ruleType === "AGGREGATE" && (
            <>
              <SelectField label="Function" value={payload.function || "SUM"} setValue={(value) => setPayload({ ...payload, function: value })} options={["SUM", "COUNT", "AVG", "MIN", "MAX"]} />
              <div className="grid2">
                {schemaAware ? (
                  <>
                    <SelectField label="Source Column" value={payload.source_column || ""} setValue={(value) => setPayload({ ...payload, source_column: value })} options={sourceRuleOptions} />
                    <SelectField label="Target Column" value={payload.target_column || ""} setValue={(value) => setPayload({ ...payload, target_column: value })} options={targetRuleOptions} />
                  </>
                ) : (
                  <>
                    <Field label="Source Column" required><input value={payload.source_column || ""} onChange={(event) => setPayload({ ...payload, source_column: event.target.value })} required /></Field>
                    <Field label="Target Column" required><input value={payload.target_column || ""} onChange={(event) => setPayload({ ...payload, target_column: event.target.value })} required /></Field>
                  </>
                )}
              </div>

              <Field label="Tolerance (%)">
                <input type="number" min="0" step="any" value={payload.tolerance_pct ?? ""} onChange={(event) => setPayload({ ...payload, tolerance_pct: event.target.value === "" ? undefined : Number(event.target.value) })} />
              </Field>

              <div className="grid2">
                {schemaAware ? (
                  <>
                    <SelectField label="Source Group By" value={payload.source_group_by?.[0] || ""} setValue={(value) => setPayload({ ...payload, source_group_by: value ? [value] : [] })} options={sourceRuleOptions} />
                    <SelectField label="Target Group By" value={payload.target_group_by?.[0] || ""} setValue={(value) => setPayload({ ...payload, target_group_by: value ? [value] : [] })} options={targetRuleOptions} />
                  </>
                ) : (
                  <>
                    <Field label="Source Group By"><input value={payload.source_group_by?.[0] || ""} onChange={(event) => setPayload({ ...payload, source_group_by: event.target.value ? [event.target.value] : [] })} /></Field>
                    <Field label="Target Group By"><input value={payload.target_group_by?.[0] || ""} onChange={(event) => setPayload({ ...payload, target_group_by: event.target.value ? [event.target.value] : [] })} /></Field>
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

// ComparisonBuilder currently references RuleModal as a global component.
// Register the shared comparison rule editor before the application renders.
globalThis.RuleModal = ComparisonRuleModal;

export { ComparisonRuleModal };
