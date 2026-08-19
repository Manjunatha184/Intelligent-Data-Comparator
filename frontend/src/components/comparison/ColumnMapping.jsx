import React from "react";
import { Plus, Trash2 } from "lucide-react";
import { getSchemaColumnNames, isNumericMapping } from "../../utils/schema.js";
import Field from "../ui/Field.jsx";
import SelectField from "../ui/SelectField.jsx";

export default function ColumnMapping({ mappings = [], setMappings, sourceSchema, targetSchema }) {
  const sourceColumns = getSchemaColumnNames(sourceSchema);
  const targetColumns = getSchemaColumnNames(targetSchema);
  const sourceOptions = sourceColumns.map((column) => [column, column]);
  const targetOptions = targetColumns.map((column) => [column, column]);

  function update(index, patch) {
    setMappings((current) => current.map((item, itemIndex) => itemIndex === index ? { ...item, ...patch } : item));
  }

  function addMapping() {
    const source = sourceColumns.find((column) => !mappings.some((mapping) => mapping.source_column === column)) || "";
    const target = targetColumns.includes(source)
      ? source
      : targetColumns.find((column) => !mappings.some((mapping) => mapping.target_column === column)) || "";
    setMappings((current) => [...current, { source_column: source, target_column: target, normalize: { trim: false, ignore_case: false, empty_as_null: false } }]);
  }

  function remove(index) {
    setMappings((current) => current.filter((_, itemIndex) => itemIndex !== index));
  }

  return (
    <section className="comparisonSection">
      <div className="comparisonSectionHead">
        <div><h3>Field mapping</h3><p className="helper">Map source fields to target fields for L4 comparison.</p></div>
        <button type="button" className="secondary small" onClick={addMapping}><Plus size={14} /> Add mapping</button>
      </div>

      <div className="mappingRows">
        {mappings.map((mapping, index) => {
          const numeric = isNumericMapping(mapping, sourceSchema, targetSchema);
          return (
            <div className="mappingRow normalizedMappingRow" key={`${mapping.source_column}-${mapping.target_column}-${index}`}>
              <SelectField label="Source field" value={mapping.source_column || ""} setValue={(value) => update(index, { source_column: value })} options={sourceOptions} placeholder="Select source field" />
              <SelectField label="Target field" value={mapping.target_column || ""} setValue={(value) => update(index, { target_column: value })} options={targetOptions} placeholder="Select target field" />
              {numeric && (
                <Field label="Tolerance %">
                  <input type="number" min="0" step="any" value={mapping.tolerance_pct ?? ""} placeholder="Optional" onChange={(event) => update(index, { tolerance_pct: event.target.value })} />
                </Field>
              )}
              <button type="button" className="iconButton dangerIcon mappingRemove" title="Remove mapping" onClick={() => remove(index)}><Trash2 size={14} /></button>
            </div>
          );
        })}
        {!mappings.length && <div className="empty compact"><b>No field mappings configured</b><span>Add mappings when L4 field comparison is selected.</span></div>}
      </div>
    </section>
  );
}
