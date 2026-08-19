import React, { useState } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";

export default function Panel({
  title,
  action,
  children,
  className = "",
  collapsible = false,
  defaultExpanded = true,
}) {
  const [expanded, setExpanded] = useState(defaultExpanded);

  return (
    <section className={`panel ${className}`.trim()}>
      {(title || action || collapsible) && (
        <div
          className="panelHead"
          style={collapsible ? { cursor: "pointer" } : {}}
          onClick={() => collapsible && setExpanded(!expanded)}
        >
          <div><h3>{title}</h3></div>
          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            {action}
            {collapsible && (
              <span style={{ color: "#a0aec0", display: "flex" }}>
                {expanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
              </span>
            )}
          </div>
        </div>
      )}
      {(!collapsible || expanded) && children}
    </section>
  );
}
