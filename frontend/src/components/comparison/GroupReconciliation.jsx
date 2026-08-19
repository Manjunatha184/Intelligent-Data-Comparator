import React from "react";
import MultiSelectField from "../ui/MultiSelectField.jsx";

function selectedSide(mappings, side) {
  return mappings.map((item) => item[`${side}_column`]).filter(Boolean);
}

function updateSide(current, side, values, aggregate = false) {
  const field = `${side}_column`;
  const other = side === "source" ? "target_column" : "source_column";
  const selected = new Set(values);
  const existing = new Set(current.map((item) => item[field]).filter(Boolean));
  const next = current.map((item) => selected.has(item[field]) || !item[field] ? { ...item } : { ...item, [field]: "" }).filter((item) => item.source_column || item.target_column);
  values.filter((value) => !existing.has(value)).forEach((value) => {
    const pending = next.find((item) => !item[field] && item[other] === value) || next.find((item) => !item[field] && item[other]);
    const mapping = pending || { source_column: "", target_column: "" };
    mapping[field] = value;
    if (aggregate && !mapping.operation) mapping.operation = "SUM";
    if (!pending) next.push(mapping);
  });
  return next;
}

export default function GroupReconciliation({ groupingAttributes, setGroupingAttributes, aggregationColumns, setAggregationColumns, sourceColumns, targetColumns }) {
  return (
    <div className="groupReconciliationGrid">
      <div className="mappingSection">
        <b>Grouping fields</b>
        <div className="grid2">
          <MultiSelectField options={sourceColumns} selected={selectedSide(groupingAttributes, "source")} onChange={(values) => setGroupingAttributes(updateSide(groupingAttributes, "source", values))} placeholder="Select source grouping fields" />
          <MultiSelectField options={targetColumns} selected={selectedSide(groupingAttributes, "target")} onChange={(values) => setGroupingAttributes(updateSide(groupingAttributes, "target", values))} placeholder="Select target grouping fields" />
        </div>
      </div>
      <div className="mappingSection">
        <b>Aggregation fields</b>
        <div className="grid2">
          <MultiSelectField options={sourceColumns} selected={selectedSide(aggregationColumns, "source")} onChange={(values) => setAggregationColumns(updateSide(aggregationColumns, "source", values, true))} placeholder="Select source aggregation fields" />
          <MultiSelectField options={targetColumns} selected={selectedSide(aggregationColumns, "target")} onChange={(values) => setAggregationColumns(updateSide(aggregationColumns, "target", values, true))} placeholder="Select target aggregation fields" />
        </div>
      </div>
    </div>
  );
}
