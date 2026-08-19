import React from "react";
import SelectField from "../ui/SelectField.jsx";

export default function KeyMapping({ comparisonKeys = [], setComparisonKeys, sourceColumns = [], targetColumns = [] }) {
  const selected = comparisonKeys[0] || { source_column: "", target_column: "" };
  function update(field, value) { setComparisonKeys([{ ...selected, [field]: value }]); }
  return (
    <div className="grid2 keyMappingGrid">
      <SelectField label="Source primary key" value={selected.source_column || ""} setValue={(value) => update("source_column", value)} options={sourceColumns.map((column) => [column, column])} placeholder="Select source key" />
      <SelectField label="Target primary key" value={selected.target_column || ""} setValue={(value) => update("target_column", value)} options={targetColumns.map((column) => [column, column])} placeholder="Select target key" />
    </div>
  );
}
