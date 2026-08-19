import React from "react";
import { Plus, Trash2 } from "lucide-react";
import { getSchemaColumnNames } from "../../utils/schema.js";
import Field from "../ui/Field.jsx";
import SelectField from "../ui/SelectField.jsx";

const OPERATORS = [["=", "Equals"], ["!=", "Not equal"], [">", "Greater than"], [">=", "Greater than or equal"], ["<", "Less than"], ["<=", "Less than or equal"], ["contains", "Contains"], ["is_null", "Is null"], ["is_not_null", "Is not null"]];

export default function FilterEditor({ title, schema, filters = [], setFilters }) {
  const columns = getSchemaColumnNames(schema).map((column) => [column, column]);
  const update = (index, patch) => setFilters((current) => current.map((filter, itemIndex) => itemIndex === index ? { ...filter, ...patch } : filter));
  const add = () => setFilters((current) => [...current, { field: "", operator: "=", value: "" }]);
  const remove = (index) => setFilters((current) => current.filter((_, itemIndex) => itemIndex !== index));

  return (
    <section className="comparisonSection filterEditor">
      <div className="comparisonSectionHead"><div><h3>{title}</h3><p className="helper">Limit rows before comparison without changing the dataset.</p></div><button type="button" className="secondary small" onClick={add}><Plus size={14} /> Add filter</button></div>
      <div className="filterRows">
        {filters.map((filter, index) => {
          const unary = ["is_null", "is_not_null"].includes(filter.operator);
          return <div className="filterRow" key={index}>
            <SelectField label="Field" value={filter.field || ""} setValue={(value) => update(index, { field: value })} options={columns} placeholder="Select field" />
            <SelectField label="Operator" value={filter.operator || "="} setValue={(value) => update(index, { operator: value })} options={OPERATORS} />
            {!unary && <Field label="Value"><input value={filter.value ?? ""} onChange={(event) => update(index, { value: event.target.value })} placeholder="Filter value" /></Field>}
            <button type="button" className="iconButton dangerIcon" title="Remove filter" onClick={() => remove(index)}><Trash2 size={14} /></button>
          </div>;
        })}
        {!filters.length && <div className="empty compact"><span>No filters configured.</span></div>}
      </div>
    </section>
  );
}
