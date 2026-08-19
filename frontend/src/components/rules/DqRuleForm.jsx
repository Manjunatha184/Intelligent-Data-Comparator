import React from "react";
import Field from "../ui/Field.jsx";
import SelectField from "../ui/SelectField.jsx";

const DQ_RULE_TYPES = [
  ["PATTERN", "Pattern"],
  ["COMPLETENESS", "Completeness"],
  ["VALIDITY", "Validity"],
];

export default function DqRuleForm({ payload, setPayload, sourceColumns = [], targetColumns = [] }) {
  const applyTo = String(payload.apply_to || "BOTH").toUpperCase();
  const type = String(payload.rule_type || "PATTERN").toUpperCase();
  const sourceOptions = sourceColumns.map((column) => [column, column]);
  const targetOptions = targetColumns.map((column) => [column, column]);

  function patch(next) {
    setPayload((current) => ({ ...current, ...next }));
  }

  return (
    <div className="ruleFormGrid">
      <SelectField label="Validation type" value={type} setValue={(value) => patch({ rule_type: value })} options={DQ_RULE_TYPES} />
      <SelectField label="Apply to" value={applyTo} setValue={(value) => patch({ apply_to: value })} options={[["BOTH", "Source & Target"], ["SOURCE", "Source only"], ["TARGET", "Target only"]]} />

      {(applyTo === "SOURCE" || applyTo === "BOTH") && (
        sourceOptions.length ? (
          <SelectField label="Source field" required value={payload.source_column || ""} setValue={(value) => patch({ source_column: value })} options={sourceOptions} placeholder="Select source field" />
        ) : (
          <Field label="Source field" required><input value={payload.source_column || ""} onChange={(event) => patch({ source_column: event.target.value })} /></Field>
        )
      )}

      {(applyTo === "TARGET" || applyTo === "BOTH") && (
        targetOptions.length ? (
          <SelectField label="Target field" required value={payload.target_column || ""} setValue={(value) => patch({ target_column: value })} options={targetOptions} placeholder="Select target field" />
        ) : (
          <Field label="Target field" required><input value={payload.target_column || ""} onChange={(event) => patch({ target_column: event.target.value })} /></Field>
        )
      )}

      {type === "PATTERN" && (
        <Field label="Regex pattern" required>
          <input value={payload.regex || ""} placeholder="^[A-Z]{3}$" onChange={(event) => patch({ regex: event.target.value })} />
        </Field>
      )}

      {type === "VALIDITY" && (
        <Field label="Allowed values">
          <input
            value={Array.isArray(payload.allowed_values) ? payload.allowed_values.join(", ") : ""}
            placeholder="ACTIVE, INACTIVE"
            onChange={(event) => patch({ allowed_values: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) })}
          />
        </Field>
      )}
    </div>
  );
}
