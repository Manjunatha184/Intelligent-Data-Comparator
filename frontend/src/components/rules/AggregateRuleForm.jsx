import React from "react";
import Field from "../ui/Field.jsx";
import SelectField from "../ui/SelectField.jsx";

export default function AggregateRuleForm({ payload, setPayload, sourceColumns = [], targetColumns = [] }) {
  const sourceOptions = sourceColumns.map((column) => [column, column]);
  const targetOptions = targetColumns.map((column) => [column, column]);

  function patch(next) {
    setPayload((current) => ({ ...current, ...next }));
  }

  return (
    <div className="ruleFormGrid">
      <SelectField label="Function" value={payload.function || "SUM"} setValue={(value) => patch({ function: value })} options={[["SUM", "SUM"], ["AVG", "AVG"], ["MIN", "MIN"], ["MAX", "MAX"], ["COUNT", "COUNT"]]} />

      {sourceOptions.length ? (
        <SelectField label="Source column" required value={payload.source_column || ""} setValue={(value) => patch({ source_column: value })} options={sourceOptions} placeholder="Select source column" />
      ) : (
        <Field label="Source column" required><input value={payload.source_column || ""} onChange={(event) => patch({ source_column: event.target.value })} /></Field>
      )}

      {targetOptions.length ? (
        <SelectField label="Target column" required value={payload.target_column || ""} setValue={(value) => patch({ target_column: value })} options={targetOptions} placeholder="Select target column" />
      ) : (
        <Field label="Target column" required><input value={payload.target_column || ""} onChange={(event) => patch({ target_column: event.target.value })} /></Field>
      )}

      <Field label="Tolerance">
        <input type="number" step="any" value={payload.tolerance ?? ""} placeholder="Optional" onChange={(event) => patch({ tolerance: event.target.value })} />
      </Field>

      <Field label="Tolerance %">
        <input type="number" step="any" value={payload.tolerance_pct ?? ""} placeholder="Optional" onChange={(event) => patch({ tolerance_pct: event.target.value })} />
      </Field>

      <SelectField label="Source group by" value={(payload.source_group_by || [])[0] || ""} setValue={(value) => patch({ source_group_by: value ? [value] : [] })} options={sourceOptions} placeholder="No grouping" />
      <SelectField label="Target group by" value={(payload.target_group_by || [])[0] || ""} setValue={(value) => patch({ target_group_by: value ? [value] : [] })} options={targetOptions} placeholder="No grouping" />
    </div>
  );
}
