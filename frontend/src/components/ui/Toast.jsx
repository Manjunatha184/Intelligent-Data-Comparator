import React from "react";
import { Check, TriangleAlert, X } from "lucide-react";

export default function Toast({ message, type = "success", onClose }) {
  return (
    <div className={`toast ${type}`} role="status">
      <div className="toastIcon">
        {type === "error" ? <TriangleAlert size={16} /> : <Check size={16} />}
      </div>
      <span>{message}</span>
      <button type="button" onClick={onClose} aria-label="Close notification">
        <X size={14} />
      </button>
    </div>
  );
}
