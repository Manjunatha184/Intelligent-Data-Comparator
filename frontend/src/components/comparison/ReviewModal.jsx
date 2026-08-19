import React from "react";
import { Loader2, X, Zap } from "lucide-react";
import Panel from "../ui/Panel.jsx";

export default function ReviewModal({
  source,
  target,
  levels,
  comparisonKeys,
  sourceFiltersCount,
  targetFiltersCount,
  ignoredColumnsCount,
  mappingsCount,
  dqRulesCount,
  aggregateRulesCount,
  onClose,
  onRun,
  running
}) {
  return (
    <div className="modalBackdrop">
      <div className="modal">
        <div className="modalHead">
          <div>
            <h3>Review & Run</h3>
            <p className="helper">Review your comparison configuration before running</p>
          </div>
          <button type="button" className="iconButton" onClick={onClose} disabled={running}>
            <X size={18} />
          </button>
        </div>

        <div className="modalBody stack">
          <div className="reviewGrid">
            <Panel title="Configuration summary">
              <ReviewRow
                label="Source"
                value={
                  source?.name ||
                  "Not selected"
                }
              />

              <ReviewRow
                label="Target"
                value={
                  target?.name ||
                  "Not selected"
                }
              />

              <ReviewRow
                label="Levels"
                value={levels.join(" · ")}
              />

              <ReviewRow
                label="Record keys"
                value={(comparisonKeys || [])
                  .filter((key) => key.source_column && key.target_column)
                  .map((key) => `${key.source_column} → ${key.target_column}`)
                  .join(", ") || "Not selected"}
              />


              <ReviewRow label="Source filters" value={String(sourceFiltersCount)} />
              <ReviewRow label="Target filters" value={String(targetFiltersCount)} />
              <ReviewRow label="Ignored columns" value={String(ignoredColumnsCount)} />
              <ReviewRow label="Column mappings" value={String(mappingsCount)} />

              <ReviewRow
                label="DQ rules"
                value={String(dqRulesCount)}
              />

              <ReviewRow
                label="Aggregate rules"
                value={String(aggregateRulesCount)}
              />
            </Panel>
          </div>
        </div>

        <div className="modalFooter">
          <button type="button" className="secondary" onClick={onClose} disabled={running}>
            Cancel
          </button>
          <button className="primary" onClick={onRun} disabled={running}>
            {running ? (
              <>
                <Loader2 size={16} className="spin" />
                Executing…
              </>
            ) : (
              <>
                <Zap size={16} />
                Run comparison
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}


function ReviewRow({ label, value }) {
  return (
    <div className="reviewRow">
      <span>{label}</span>
      <b>{value}</b>
    </div>
  );
}

/* ============================================================
   RESULTS
============================================================ */

const RESULT_PAGE_SIZE = 50;

const LEVEL_NAMES = {
  L1: "Schema",
  L2: "Volume",
  L3: "Record",
  L4: "Field Transformation",
  L5: "Aggregate",
  L6: "Data Quality",
  L7: "Analysis & Recommendations",
};


