import React, { useState } from "react";
import {
  Check,
  ChevronDown,
  ChevronUp,
  Database,
  Loader2,
  TriangleAlert,
  X,
} from "lucide-react";
import { CONNECTORS } from "../constants/app";

export function Metric({ label, value, sub, icon: Icon }) {
  return <div className="metric"><div className="metricIcon"><Icon size={17} /></div><span>{label}</span><strong>{value}</strong><small>{sub}</small></div>;
}

export function Panel({ title, action, children, className = "", collapsible = false, defaultExpanded = true }) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  return <section className={`panel ${className}`.trim()}>
    {(title || action || collapsible) && <div className="panelHead" style={collapsible ? { cursor: "pointer" } : {}} onClick={() => collapsible && setExpanded(!expanded)}>
      <div><h3>{title}</h3></div>
      <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>{action}{collapsible && <span style={{ color: "#a0aec0", display: "flex" }}>{expanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}</span>}</div>
    </div>}
    {(!collapsible || expanded) && children}
  </section>;
}

export function ConnectionLine({ connection }) {
  const Icon = CONNECTORS[connection.connector_type]?.icon || Database;
  return <div className="connLine"><div className="sourceIcon"><Icon size={16} /></div><div className="grow"><b>{connection.name}</b><span>{CONNECTORS[connection.connector_type]?.label || connection.connector_type}</span></div><Status status={connection.status} /></div>;
}

export function Status({ status }) {
  const normalized = String(status || "UNKNOWN").toLowerCase();
  return <span className={`status ${normalized}`}><i />{status || "UNKNOWN"}</span>;
}

export function Empty({ icon: Icon, title, text }) {
  return <div className="empty"><Icon size={22} /><b>{title}</b><span>{text}</span></div>;
}

export function Loading({ text }) {
  return <div className="loading"><Loader2 className="spin" size={20} />{text}</div>;
}

export function Field({ label, required = false, children }) {
  return <label className="field"><span>{label}{required && <em>*</em>}</span>{children}</label>;
}

export function SelectField({ label, value, setValue, options, required = false }) {
  return <Field label={label} required={required}><select value={value} onChange={(event) => setValue(event.target.value)}>{options.map((option) => <option key={Array.isArray(option) ? option[0] : option} value={Array.isArray(option) ? option[0] : option}>{Array.isArray(option) ? option[1] : option}</option>)}</select></Field>;
}

export function WizardItem({ number, label, active, complete, onClick }) {
  return <button type="button" className={`wizardItem ${active ? "active" : ""} ${complete ? "done" : ""}`} onClick={onClick}><span>{complete ? <Check size={13} /> : number}</span><b>{label}</b></button>;
}

export function Toast({ message, type, onClose }) {
  return <div className={`toast ${type}`}><span>{type === "error" ? <TriangleAlert size={17} /> : <Check size={17} />}</span>{message}<button onClick={onClose}><X size={14} /></button></div>;
}
