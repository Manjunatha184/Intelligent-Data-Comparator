import React from "react";

export default function Field({ label, required = false, children }) {
  return (
    <label className="field">
      <span>
        {label}
        {required && <em>*</em>}
      </span>
      {children}
    </label>
  );
}
