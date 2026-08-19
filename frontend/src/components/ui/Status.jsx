import React from "react";

export default function Status({ status }) {
  const normalized = String(status || "UNKNOWN").toLowerCase();
  return (
    <span className={`status ${normalized}`}>
      <i />
      {status || "UNKNOWN"}
    </span>
  );
}
