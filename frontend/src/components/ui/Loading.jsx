import React from "react";
import { Loader2 } from "lucide-react";

export default function Loading({ text = "Loading…" }) {
  return (
    <div className="loading">
      <Loader2 size={18} className="spin" />
      <span>{text}</span>
    </div>
  );
}
