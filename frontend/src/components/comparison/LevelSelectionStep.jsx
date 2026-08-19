import React from "react";
import { Check } from "lucide-react";
import { COMPARISON_LEVELS, COMPARISON_LEVEL_ICONS } from "../../config/comparison.js";
import Panel from "../ui/Panel.jsx";

export default function LevelSelectionStep({ levels, toggleLevel }) {
  return (
    <Panel title="Comparison depth" className="scopeLevelsPanel">
      <div className="scopeLevelIntro">
        <p className="helper">Build the validation path from structural checks through plain-language analysis.</p>
        <span className="scopeSelectionCount">{levels.length} of {COMPARISON_LEVELS.length} selected</span>
      </div>
      <div className="levelGrid">
        {COMPARISON_LEVELS.map((level) => {
          const selected = levels.includes(level.id);
          const Icon = COMPARISON_LEVEL_ICONS[level.id];
          return (
            <button type="button" key={level.id} className={`level ${selected ? "selected" : ""} level-${level.id}`} onClick={() => toggleLevel(level.id)}>
              <span className="levelVisual"><Icon size={17} /><span className="levelCode">{level.id}</span></span>
              <div><b>{level.name}</b><small>{level.description}</small></div>
              <span className="checkCircle">{selected && <Check size={13} />}</span>
            </button>
          );
        })}
      </div>
    </Panel>
  );
}
