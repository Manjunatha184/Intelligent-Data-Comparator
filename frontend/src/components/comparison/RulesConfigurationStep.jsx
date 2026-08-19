import React from "react";
import { Plus, Trash2, X } from "lucide-react";
import { getSchemaColumnNames, isNumericMapping } from "../../utils/schema.js";
import Panel from "../ui/Panel.jsx";
import Field from "../ui/Field.jsx";
import SelectField from "../ui/SelectField.jsx";
import RuleModal from "../rules/RuleModal.jsx";

function RuleSelectionModal({
  title,
  rules,
  selectedIds,
  onSelectionChange,
  onClose,
  category,
  sourceSchema,
  targetSchema,
  notify,
  onRulesChanged,
}) {
  const [ruleEditorOpen, setRuleEditorOpen] = useState(false);

  return (
    <div className="modalBackdrop">
      <div className="modal">
        <div className="modalHead">
          <div>
            <h3>{title}</h3>
            <p className="helper">Select rules from the repository</p>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            {category && (
              <button type="button" className="secondary small" onClick={() => setRuleEditorOpen(true)}>
                <Plus size={14} /> New rule
              </button>
            )}
            <button type="button" className="iconButton" onClick={onClose}>
              <X size={18} />
            </button>
          </div>
        </div>

        <div className="modalBody stack" style={{ maxHeight: "400px", overflowY: "auto", padding: "0 25px 20px" }}>
          {rules.length === 0 ? (
            <div className="empty compact" style={{ marginTop: "20px" }}>
              <b>No rules found</b>
            </div>
          ) : (
            <div className="ruleTable" style={{ marginTop: "20px" }}>
              {rules.map((rule) => (
                <label key={rule.rule_id} className="ruleCheckbox">
                  <input
                    type="checkbox"
                    checked={selectedIds.includes(rule.rule_id)}
                    onChange={(e) => {
                      if (e.target.checked)
                        onSelectionChange([...selectedIds, rule.rule_id]);
                      else
                        onSelectionChange(
                          selectedIds.filter((id) => id !== rule.rule_id)
                        );
                    }}
                  />
                  <div>
                    <b>{rule.name}</b>
                    <span>
                      {rule.rule_type === "DQ"
                        ? describeDqRule(rule.payload)
                        : `${String(rule.payload.function).toLowerCase()} on ${String(
                          rule.payload.source_column
                        ).toLowerCase()}`}
                    </span>
                  </div>
                </label>
              ))}
            </div>
          )}
        </div>
      </div>
      {ruleEditorOpen && (
        <RuleModal
          initialRuleType={category}
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          onClose={() => setRuleEditorOpen(false)}
          onDone={() => {
            setRuleEditorOpen(false);
            onRulesChanged?.();
          }}
          notify={notify}
        />
      )}
    </div>
  );
}

function describeDqRule(payload = {}) {
  const applyTo = String(payload.apply_to || "BOTH").toUpperCase();
  const sourceColumn = payload.source_column || payload.column;
  const targetColumn = payload.target_column || payload.column;
  const scopeLabel = applyTo === "SOURCE" ? sourceColumn : applyTo === "TARGET" ? targetColumn : `${sourceColumn} → ${targetColumn}`;
  return `${String(payload.rule_type || payload.type || "rule").toLowerCase()} on ${scopeLabel}`;
}

function schemaRuleOptions(schema, currentValue, schemaAware) {
  const schemaColumns = getSchemaColumnNames(schema);

  if (schemaAware) {
    return ["", ...schemaColumns];
  }

  return Array.from(
    new Set([
      "",
      ...schemaColumns,
      currentValue || "",
    ])
  );
}


function MultiSelectField({ options, selected, onChange, placeholder }) {
  const [open, setOpen] = React.useState(false);
  const rootRef = React.useRef(null);
  React.useEffect(() => {
    const close = (event) => { if (rootRef.current && !rootRef.current.contains(event.target)) setOpen(false); };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, []);
  const toggle = (value) => onChange(selected.includes(value) ? selected.filter(item => item !== value) : [...selected, value]);
  return <div className="multiSelectField" ref={rootRef}>
    <button type="button" className="multiSelectTrigger" aria-expanded={open} onClick={() => setOpen(!open)}>
      <span>{placeholder}</span><span className="multiSelectChevron">{open ? "▲" : "▼"}</span>
    </button>
    {open && <div className="multiSelectMenu" role="listbox">
      {options.map(option => <button type="button" role="option" aria-selected={selected.includes(option)} className="multiSelectOption" key={option} onClick={() => toggle(option)}>
        <span className="multiSelectCheck">{selected.includes(option) ? "✓" : ""}</span>{option}
      </button>)}
      {!options.length && <span className="multiSelectEmpty">No schema fields available</span>}
    </div>}
  </div>;
}

export default function RulesConfigurationStep({
  comparisonKeys,
  setComparisonKeys,
  groupingAttributes,
  setGroupingAttributes,
  aggregationColumns,
  setAggregationColumns,
  availableRules,
  selectedDqRuleIds,
  setSelectedDqRuleIds,
  selectedAggRuleIds,
  setSelectedAggRuleIds,
  notify,
  onRulesChanged,
  levels,
  columnMappings,
  setColumnMappings,
  sourceSchema,
  targetSchema,
  sourceSchemaLoading,
  targetSchemaLoading,
  sourceSchemaError,
  targetSchemaError,
  sourceFilters,
  setSourceFilters,
  targetFilters,
  setTargetFilters,
  ignoredSourceColumns,
  setIgnoredSourceColumns,
  ignoredTargetColumns,
  setIgnoredTargetColumns,
}) {
  const [dqModalOpen, setDqModalOpen] = React.useState(false);
  const [aggModalOpen, setAggModalOpen] = React.useState(false);
  const [normalizationOpen, setNormalizationOpen] = React.useState({});
  const [pendingGroupingSource, setPendingGroupingSource] = React.useState("");
  const [pendingGroupingTarget, setPendingGroupingTarget] = React.useState("");
  const [pendingAggregationSource, setPendingAggregationSource] = React.useState("");
  const [pendingAggregationTarget, setPendingAggregationTarget] = React.useState("");

  const sourceColumnOptions = getSchemaColumnNames(sourceSchema);
  const targetColumnOptions = getSchemaColumnNames(targetSchema);

  const selectedKey = comparisonKeys?.[0] || {
    source_column: "",
    target_column: "",
  };

  const updateSelectedKey = (field, value) => {
    setComparisonKeys([
      {
        ...selectedKey,
        [field]: value,
      },
    ]);
  };


  const sourceGroupingFields = groupingAttributes.map((item) => item.source_column).filter(Boolean);
  const targetGroupingFields = groupingAttributes.map((item) => item.target_column).filter(Boolean);
  const sourceAggregationFields = aggregationColumns.map((item) => item.source_column).filter(Boolean);
  const targetAggregationFields = aggregationColumns.map((item) => item.target_column).filter(Boolean);
  const updatePairedSelection = (kind, side, values) => {
    const current = kind === "group" ? groupingAttributes : aggregationColumns;
    const selected = new Set(values);
    const field = `${side}_column`;
    const otherField = side === "source" ? "target_column" : "source_column";
    const existing = new Set(current.map((item) => item[field]).filter(Boolean));

    // Keep completed mappings intact by identity, not by the array position
    // of either multi-select. Only a newly selected field can fill an
    // intentionally incomplete mapping created on the opposite side.
    const next = current
      .map((item) => {
        if (!item[field] || selected.has(item[field])) return { ...item };
        const updated = { ...item, [field]: "" };
        if (kind === "aggregate" && side === "source") delete updated.operation;
        return updated;
      })
      .filter((item) => item.source_column || item.target_column);

    values.filter((value) => !existing.has(value)).forEach((value) => {
      // Automatic mappings prefer the same logical name. This keeps
      // Status -> Status and Region -> Region stable even if targets are
      // selected in a different order. Explicit mappings remain untouched.
      const pending = next.find(
        (item) => !item[field] && item[otherField] === value
      ) || next.find(
        (item) => !item[field] && item[otherField]
      );
      const mapping = pending || { source_column: "", target_column: "" };
      mapping[field] = value;
      if (kind === "aggregate" && mapping.source_column) {
        mapping.operation = automaticOperation(mapping.source_column, "source");
      }
      if (!pending) next.push(mapping);
    });

    kind === "group" ? setGroupingAttributes(next) : setAggregationColumns(next);
  };
  const selectionChips = (mappings, kind, side) => <div className="chipRow">
    {mappings.map((mapping, index) => {
      const value = mapping[`${side}_column`];
      if (!value) return null;
      return <span className="chip" key={`${kind}-${side}-${mapping.source_column}-${mapping.target_column}-${index}`}>
        {value}<button type="button" aria-label={`Remove ${value}`} onClick={() => {
          const current = kind === "group" ? groupingAttributes : aggregationColumns;
          const field = `${side}_column`;
          const next = current
            .map((item, mappingIndex) => {
              if (mappingIndex !== index) return { ...item };
              const updated = { ...item, [field]: "" };
              if (kind === "aggregate" && side === "source") delete updated.operation;
              return updated;
            })
            .filter((item) => item.source_column || item.target_column);
          kind === "group" ? setGroupingAttributes(next) : setAggregationColumns(next);
        }}>×</button></span>;
    })}
  </div>;
  const sourceTypeFor = (name) => getColumnType(findSchemaColumn(sourceSchema, name));
  const automaticOperation = (name, side) => {
    const schema = side === "source" ? sourceSchema : targetSchema;
    return isNumericColumn(findSchemaColumn(schema, name)) ? "AVG" : "MODE";
  };
  const FieldChips = ({ values, onRemove }) => (
    <div className="chipRow">
      {values.map((value, index) => <span className="chip" key={`${value}-${index}`}>{value}<button type="button" aria-label={`Remove ${value}`} onClick={() => onRemove(index)}>×</button></span>)}
    </div>
  );
  const MultiFieldPicker = ({ label, options, selected, onAdd }) => (
    <div className="field">
      <label>{label}</label>
      <select value="" onChange={(event) => onAdd(event.target.value)}>
        <option value="">Select fields...</option>
        {options.filter((option) => !selected.includes(option)).map(option => <option key={option} value={option}>{option}</option>)}
      </select>
    </div>
  );


  return (
    <div className="stack reviewRunStep">
      {(sourceSchemaLoading || targetSchemaLoading || sourceSchemaError || targetSchemaError) && (
        <div className="helper" role="status">
          {sourceSchemaError || targetSchemaError || (sourceSchemaLoading || targetSchemaLoading ? "Loading fields..." : "")}
        </div>
      )}
      <div className="filtersGrid">
        <FilterSection title="Source Filters" schema={sourceSchema} filters={sourceFilters} setFilters={setSourceFilters} />
        <FilterSection title="Target Filters" schema={targetSchema} filters={targetFilters} setFilters={setTargetFilters} />
      </div>
      <Panel title="Ignored columns" className="reviewRunCard ignoredColumnsCard">
        <p className="helper">Selected columns are excluded from every applicable comparison level.</p>
        <div className="formGrid">
          <div className="mappingPickerBlock">
            <label>Source columns to ignore</label>
            <MultiSelectField options={sourceColumnOptions} selected={ignoredSourceColumns} onChange={setIgnoredSourceColumns} placeholder="Select source columns" />
            <FieldChips values={ignoredSourceColumns} onRemove={(index) => setIgnoredSourceColumns(ignoredSourceColumns.filter((_, itemIndex) => itemIndex !== index))} />
          </div>
          <div className="mappingPickerBlock">
            <label>Target columns to ignore</label>
            <MultiSelectField options={targetColumnOptions} selected={ignoredTargetColumns} onChange={setIgnoredTargetColumns} placeholder="Select target columns" />
            <FieldChips values={ignoredTargetColumns} onRemove={(index) => setIgnoredTargetColumns(ignoredTargetColumns.filter((_, itemIndex) => itemIndex !== index))} />
          </div>
        </div>
      </Panel>
      <Panel title="Record matching" className="reviewRunCard recordMatchingCard">
        <div className="formGrid">
          <SelectField
            label="Source key"
            value={selectedKey.source_column || ""}
            setValue={(value) => updateSelectedKey("source_column", value)}
            options={["", ...sourceColumnOptions]}
          />

          <SelectField
            label="Target key"
            value={selectedKey.target_column || ""}
            setValue={(value) => updateSelectedKey("target_column", value)}
            options={["", ...targetColumnOptions]}
          />
        </div>


      </Panel>

      <Panel title="Group-Based Reconciliation" className="reviewRunCard reconciliationCard">
        <section className="reconciliationSection">
          <h4>Grouping fields</h4>
          <div className="formGrid">
            <div className="mappingPickerBlock">
              <label>Source grouping fields</label>
              <MultiSelectField options={sourceColumnOptions} selected={sourceGroupingFields} onChange={values => updatePairedSelection("group", "source", values)} placeholder="Select source fields" />
              {selectionChips(groupingAttributes, "group", "source")}
            </div>
            <div className="mappingPickerBlock">
              <label>Target grouping fields</label>
              <MultiSelectField options={targetColumnOptions} selected={targetGroupingFields} onChange={values => updatePairedSelection("group", "target", values)} placeholder="Select target fields" />
              {selectionChips(groupingAttributes, "group", "target")}
            </div>
          </div>
        </section>

        <section className="reconciliationSection">
          <h4>Aggregation fields</h4>
          <div className="formGrid">
            <div className="mappingPickerBlock">
              <label>Source aggregation fields</label>
              <MultiSelectField options={sourceColumnOptions} selected={sourceAggregationFields} onChange={values => updatePairedSelection("aggregate", "source", values)} placeholder="Select source fields" />
              {selectionChips(aggregationColumns, "aggregate", "source")}
            </div>
            <div className="mappingPickerBlock">
              <label>Target aggregation fields</label>
              <MultiSelectField options={targetColumnOptions} selected={targetAggregationFields} onChange={values => updatePairedSelection("aggregate", "target", values)} placeholder="Select target fields" />
              {selectionChips(aggregationColumns, "aggregate", "target")}
            </div>
          </div>
        </section>
        <p className="reconciliationNote">Numeric aggregation fields use AVG automatically; non-numeric aggregation fields use MODE automatically.</p>
      </Panel>

      <div className={`reviewRunSummaryGrid ${columnMappings?.length ? "hasMappings" : ""}`}>
      {!columnMappings || columnMappings.length === 0 ? (
        <Panel
          title="Column Mapping"
          className="reviewRunCard compactReviewCard"
          action={<button type="button" className="secondary small" onClick={() => setColumnMappings([{ source_column: "", target_column: "", tolerance_pct: undefined }])}>
            <Plus size={14} /> Add column mapping
          </button>}
        >
          <p className="reviewEmptyState">No mappings configured.</p>
        </Panel>
      ) : (
        <Panel title="Column Mapping" className="reviewRunCard">
          <div className="stack" style={{ gap: "10px" }}>
            {columnMappings.map((mapping, idx) => {
              const isNumericPair = isNumericMapping(mapping, sourceSchema, targetSchema);
              const updateMapping = (key, val) => {
                const copy = [...columnMappings];
                const updated = {
                  ...copy[idx],
                  [key]: val,
                };

                if (
                  key === "source_column" ||
                  key === "target_column"
                ) {
                  const nextMapping = {
                    ...updated,
                  };

                  if (!isNumericMapping(nextMapping, sourceSchema, targetSchema)) {
                    delete nextMapping.tolerance_pct;
                  }

                  copy[idx] = nextMapping;
                } else {
                  copy[idx] = updated;
                }

                setColumnMappings(copy);
              };

              return (
                <div key={idx} className="columnMappingRow" style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr) minmax(130px, 0.7fr) auto auto", gap: "10px", alignItems: "end", padding: "10px", border: "1px solid var(--line)", borderRadius: "8px", background: "#f8fafc" }}>
                  <div>
                    <SelectField
                      label="Source column"
                      value={mapping.source_column || ""}
                      options={["", ...sourceColumnOptions]}
                      setValue={v => updateMapping("source_column", v)}
                    />
                  </div>
                  <div>
                    <SelectField
                      label="Target column"
                      value={mapping.target_column || ""}
                      options={["", ...targetColumnOptions]}
                      setValue={v => updateMapping("target_column", v)}
                    />
                  </div>
                  <div>
                    <Field label="Tolerance (%)">
                      {isNumericPair ? (
                        <input
                          type="number"
                          min="0"
                          max="100"
                          step="any"
                          value={mapping.tolerance_pct === undefined ? "" : mapping.tolerance_pct}
                          onChange={e => updateMapping("tolerance_pct", e.target.value ? Number(e.target.value) : undefined)}
                        />
                      ) : (
                        <div style={{ padding: "8px 12px", background: "var(--surface)", border: "1px solid var(--line)", borderRadius: "6px", color: "var(--muted)", fontSize: "13px" }}>
                          N/A
                        </div>
                      )}
                    </Field>
                  </div>
                  <button type="button" className="secondary small" onClick={() => setNormalizationOpen(current => ({ ...current, [idx]: !current[idx] }))} style={{ marginBottom: "2px" }}>
                    {normalizationOpen[idx] ? "Hide" : "Configure"}
                  </button>
                  <button type="button" className="iconButton dangerIcon" title="Delete mapping" onClick={() => setColumnMappings(columnMappings.filter((_, i) => i !== idx))} style={{ marginBottom: "2px" }}><Trash2 size={15} /></button>
                  {normalizationOpen[idx] && (
                    <div style={{ gridColumn: "1 / -1", display: "flex", alignItems: "center", gap: "18px", flexWrap: "wrap", padding: "10px 4px 2px", borderTop: "1px solid var(--line)" }}>
                      <strong style={{ fontSize: "12px", color: "var(--text-secondary)" }}>Normalization options</strong>
                      {[["trim", "Trim whitespace"], ["case_insensitive", "Ignore case"], ["empty_as_null", "Empty string as null"]].map(([key, label]) => (
                        <label key={key} style={{ display: "inline-flex", alignItems: "center", gap: "6px", fontSize: "12px", color: "var(--text-secondary)", whiteSpace: "nowrap" }}>
                          <input type="checkbox" checked={Boolean(mapping.normalization?.[key])} onChange={e => updateMapping("normalization", { ...(mapping.normalization || {}), [key]: e.target.checked })} />
                          {label}
                        </label>
                      ))}
                      {isNumericPair && <label style={{ display: "inline-flex", alignItems: "center", gap: "7px", fontSize: "12px", color: "var(--text-secondary)", whiteSpace: "nowrap" }}>Round decimals<input style={{ width: "70px" }} type="number" min="0" step="1" value={mapping.normalization?.round ?? ""} onChange={e => {
                        const value = e.target.value;
                        updateMapping("normalization", { ...(mapping.normalization || {}), ...(value === "" ? { round: undefined } : { round: Number(value) }) });
                      }} /></label>}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
          <div style={{ marginTop: "15px" }}>
            <button type="button" className="secondary small" onClick={() => setColumnMappings([...(columnMappings || []), { source_column: "", target_column: "", tolerance_pct: undefined }])}>
              <Plus size={14} /> Add column mapping
            </button>
          </div>
        </Panel>
      )}

      <Panel title="Data quality rules (L6)" className="reviewRunCard compactReviewCard" action={<button type="button" className="secondary small" onClick={() => setDqModalOpen(true)}>Select rules</button>}>
        <p className="reviewEmptyState">
          {selectedDqRuleIds.length === 0 ? "No data-quality rules selected." : `${selectedDqRuleIds.length} data-quality rule${selectedDqRuleIds.length === 1 ? "" : "s"} selected.`}
        </p>
      </Panel>

      <Panel title="Aggregate rules (L5)" className="reviewRunCard compactReviewCard" action={<button type="button" className="secondary small" onClick={() => setAggModalOpen(true)}>Select rules</button>}>
        <p className="reviewEmptyState">
          {selectedAggRuleIds.length === 0 ? "No aggregate rules selected." : `${selectedAggRuleIds.length} aggregate rule${selectedAggRuleIds.length === 1 ? "" : "s"} selected.`}
        </p>
      </Panel>
      </div>

      {dqModalOpen && (
        <RuleSelectionModal
          title="Data quality rules (L6)"
          rules={availableRules.filter(r => r.rule_type === "DQ")}
          selectedIds={selectedDqRuleIds}
          onSelectionChange={setSelectedDqRuleIds}
          onClose={() => setDqModalOpen(false)}
          category="DQ"
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          notify={notify}
          onRulesChanged={onRulesChanged}
        />
      )}

      {aggModalOpen && (
        <RuleSelectionModal
          title="Aggregate rules (L5)"
          rules={availableRules.filter(r => r.rule_type === "AGGREGATE")}
          selectedIds={selectedAggRuleIds}
          onSelectionChange={setSelectedAggRuleIds}
          onClose={() => setAggModalOpen(false)}
          category="AGGREGATE"
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          notify={notify}
          onRulesChanged={onRulesChanged}
        />
      )}
    </div>
  );
}

const FILTER_OPERATORS = ["=", "!=", ">", ">=", "<", "<=", "IN", "IS NULL", "IS NOT NULL"];
const NULL_FILTER_OPERATORS = new Set(["IS NULL", "IS NOT NULL"]);

function normalizeRowFilterPayload(filter) {
  const operator = String(filter.operator || "=").trim().toUpperCase();
  return {
    ...filter,
    operator,
    value: NULL_FILTER_OPERATORS.has(operator) ? null : filter.value,
  };
}

function FilterSection({ title, schema, filters, setFilters }) {
  const columns = getSchemaColumnNames(schema);
  const add = () => setFilters([...filters, { field: columns[0] || "", operator: "=", value: "" }]);
  const update = (index, key, value) => setFilters(filters.map((item, i) => {
    if (i !== index) return item;
    const next = { ...item, [key]: value };
    if (key === "operator" && NULL_FILTER_OPERATORS.has(String(value).toUpperCase())) {
      next.value = null;
    }
    return next;
  }));
  return <Panel title={title} className="reviewRunCard filterCard">
    <div className="stack">
      {filters.map((item, index) => <div className="formGrid" key={`${title}-${index}`}>
        <SelectField label="Field" value={item.field} setValue={value => update(index, "field", value)} options={["", ...columns]} />
        <SelectField label="Operator" value={item.operator} setValue={value => update(index, "operator", value)} options={FILTER_OPERATORS} />
        {!item.operator.includes("NULL") && <Field label={item.operator === "IN" ? "Values (comma separated)" : "Value"}>
          <input value={Array.isArray(item.value) ? item.value.join(", ") : item.value} onChange={e => update(index, "value", item.operator === "IN" ? e.target.value.split(",").map(v => v.trim()).filter(Boolean) : e.target.value)} />
        </Field>}
        <button type="button" className="secondary" onClick={() => setFilters(filters.filter((_, i) => i !== index))}><X size={14} /> Remove</button>
      </div>)}
      <button type="button" className="secondary" onClick={add} disabled={!columns.length}><Plus size={14} /> Add filter</button>
    </div>
  </Panel>;
}

/* ============================================================
   REVIEW
============================================================ */

