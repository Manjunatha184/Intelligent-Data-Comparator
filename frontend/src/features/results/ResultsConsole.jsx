import React, { useEffect, useRef, useState } from "react";

import { apiRequest } from "../../api/client";
import { Results } from "./Results";

const LEVEL_ORDER = ["L1", "L2", "L3", "L4", "L5", "L6", "L7"];

function firstAvailableLevel(data) {
  const available = new Set((data?.levels || []).map((item) => item.level));
  return LEVEL_ORDER.find((level) => available.has(level)) || "L1";
}

function findEngine(data) {
  const direct =
    data?.execution_location ||
    data?.execution_engine ||
    data?.engine ||
    data?.metadata?.execution_location ||
    data?.metadata?.execution_engine;

  if (direct) return String(direct).toUpperCase();

  for (const level of data?.levels || []) {
    const value =
      level?.metrics?.execution_location ||
      level?.metrics?.execution_engine ||
      level?.metrics?.engine;
    if (value) return String(value).toUpperCase();
  }

  return "N/A";
}

function findDuration(data) {
  const milliseconds =
    data?.duration_ms ??
    data?.total_ms ??
    data?.execution_time_ms ??
    data?.metadata?.duration_ms ??
    data?.metadata?.total_ms;

  if (milliseconds !== undefined && milliseconds !== null && milliseconds !== "") {
    const value = Number(milliseconds);
    if (Number.isFinite(value)) {
      if (value >= 1000) return `${(value / 1000).toFixed(value >= 10000 ? 1 : 2)}s`;
      return `${value.toFixed(0)}ms`;
    }
  }

  const seconds = data?.duration_seconds ?? data?.metadata?.duration_seconds;
  if (seconds !== undefined && seconds !== null && seconds !== "") {
    const value = Number(seconds);
    if (Number.isFinite(value)) return `${value.toFixed(value >= 10 ? 1 : 2)}s`;
  }

  return "N/A";
}

function RunMetaStrip({ data }) {
  if (!data) return null;

  const status = String(data.status || "N/A").toUpperCase();
  const result = String(data.comparison_status || data.result || "N/A").toUpperCase();
  const engine = findEngine(data);
  const duration = findDuration(data);

  return (
    <div className="runMetaStrip">
      <div className="runMetaItem">
        <span>STATUS</span>
        <strong className={`meta-${status.toLowerCase()}`}>{status}</strong>
      </div>
      <div className="runMetaItem">
        <span>RESULT</span>
        <strong className={`meta-${result.toLowerCase()}`}>{result}</strong>
      </div>
      <div className="runMetaItem">
        <span>ENGINE</span>
        <strong>{engine}</strong>
      </div>
      <div className="runMetaItem">
        <span>DURATION</span>
        <strong>{duration}</strong>
      </div>
    </div>
  );
}

export function ResultsConsole(props) {
  const { runId, notify } = props;
  const [activeLevel, setActiveLevel] = useState("L1");
  const [meta, setMeta] = useState(null);
  const rootRef = useRef(null);

  useEffect(() => {
    setActiveLevel("L1");
    setMeta(null);

    if (!runId) return;

    let cancelled = false;
    apiRequest(`/comparisons/${runId}/results`, { method: "GET" })
      .then((data) => {
        if (cancelled) return;
        setMeta(data);
        setActiveLevel(firstAvailableLevel(data));
      })
      .catch((error) => {
        if (!cancelled && notify) notify(error.message, "error");
      });

    return () => {
      cancelled = true;
    };
  }, [runId]);

  function ensureActiveCardExpanded(level) {
    window.requestAnimationFrame(() => {
      const root = rootRef.current;
      if (!root) return;

      const index = LEVEL_ORDER.indexOf(level);
      if (index < 0) return;

      const card = root.querySelector(
        `.resultLevelsClean > .resultLevelClean:nth-child(${index + 1})`
      );

      if (!card) return;
      if (card.querySelector(".resultLevelContent")) return;

      const header = card.querySelector(".resultLevelHeader");
      if (header) header.click();
    });
  }

  function handleClickCapture(event) {
    const summaryItem = event.target.closest?.(".levelSummaryItem");
    if (!summaryItem) return;

    const level = summaryItem.querySelector(".levelSummaryCode")?.textContent?.trim();
    if (!LEVEL_ORDER.includes(level)) return;

    setActiveLevel(level);
    ensureActiveCardExpanded(level);
  }

  const activeClass = `active-${activeLevel}`;

  return (
    <div
      ref={rootRef}
      className={`resultsConsole ${activeClass}`}
      onClickCapture={handleClickCapture}
    >
      {runId && <RunMetaStrip data={meta} />}
      <Results {...props} />
    </div>
  );
}
