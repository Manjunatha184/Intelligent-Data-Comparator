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

function formatDurationMs(milliseconds) {
  const value = Number(milliseconds);
  if (!Number.isFinite(value) || value < 0) return "N/A";
  if (value < 1000) return `${Math.round(value)}ms`;

  const totalSeconds = value / 1000;
  if (totalSeconds < 60) return `${totalSeconds.toFixed(totalSeconds < 10 ? 1 : 0)}s`;

  const minutes = Math.floor(totalSeconds / 60);
  const seconds = Math.round(totalSeconds % 60);
  if (minutes < 60) return `${minutes}m ${seconds}s`;

  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `${hours}h ${remainingMinutes}m`;
}

function findDuration(data) {
  const milliseconds =
    data?.duration_ms ??
    data?.total_ms ??
    data?.execution_time_ms ??
    data?.metadata?.duration_ms ??
    data?.metadata?.total_ms;

  if (milliseconds !== undefined && milliseconds !== null && milliseconds !== "") {
    return formatDurationMs(milliseconds);
  }

  const seconds = data?.duration_seconds ?? data?.metadata?.duration_seconds;
  if (seconds !== undefined && seconds !== null && seconds !== "") {
    const value = Number(seconds);
    if (Number.isFinite(value)) return formatDurationMs(value * 1000);
  }

  const startedAt = data?.started_at;
  const finishedAt = data?.finished_at;
  if (startedAt && finishedAt) {
    const started = new Date(startedAt).getTime();
    const finished = new Date(finishedAt).getTime();
    if (Number.isFinite(started) && Number.isFinite(finished) && finished >= started) {
      return formatDurationMs(finished - started);
    }
  }

  return "N/A";
}

function percentage(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "N/A";

  // Do not round a very-high-but-imperfect data match to a misleading 100.00%.
  if (number > 99.99 && number < 100) return `${number.toFixed(6)}%`;
  return `${number.toFixed(2)}%`;
}

function validationPercentage(data) {
  const deterministicLevels = (data?.levels || []).filter((level) => {
    const code = String(level?.level || "").toUpperCase();
    const status = String(level?.status || "").toUpperCase();
    return ["L1", "L2", "L3", "L4", "L5", "L6"].includes(code)
      && !["NOT_APPLICABLE", "NOT RUN"].includes(status);
  });

  if (!deterministicLevels.length) return null;
  const passed = deterministicLevels.filter(
    (level) => String(level?.status || "").toUpperCase() === "PASS"
  ).length;
  return (passed / deterministicLevels.length) * 100;
}

function firstFinite(...values) {
  for (const value of values) {
    const number = Number(value);
    if (Number.isFinite(number)) return number;
  }
  return null;
}

function dataMatchPercentage(data) {
  const l3 = (data?.levels || []).find(
    (level) => String(level?.level || "").toUpperCase() === "L3"
  );
  const l4 = (data?.levels || []).find(
    (level) => String(level?.level || "").toUpperCase() === "L4"
  );

  // Record coverage: how much of the actual source/target row population can
  // be aligned. Missing, extra, duplicate/ambiguous records reduce this score.
  const l3Metrics = l3?.metrics || {};
  const sourceRecords = firstFinite(
    l3Metrics.source_record_count,
    l3Metrics.total_rows_source,
    data?.datasets?.source?.records
  );
  const targetRecords = firstFinite(
    l3Metrics.target_record_count,
    l3Metrics.total_rows_target,
    data?.datasets?.target?.records
  );
  const matchedRecords = firstFinite(
    l3Metrics.matched_record_count,
    l3Metrics.primary_matched_record_count,
    l3Metrics.matched_key_count,
    l3Metrics.primary_matched_count
  );

  let recordMatch = null;
  if (
    sourceRecords !== null && sourceRecords >= 0 &&
    targetRecords !== null && targetRecords >= 0 &&
    matchedRecords !== null && matchedRecords >= 0
  ) {
    const population = Math.max(sourceRecords, targetRecords);
    if (population > 0) recordMatch = Math.min(1, matchedRecords / population);
  }

  // Field conformity: use raw counts rather than a pre-rounded percentage.
  // This prevents 11 mismatches in millions of comparisons being displayed as
  // an exact 100% match.
  const l4Metrics = l4?.metrics || {};
  const matchedFields = firstFinite(l4Metrics.matched_field_count);
  const mismatchedFields = firstFinite(
    l4Metrics.mismatch_count,
    l4Metrics.field_mismatch_count
  );
  const comparedFields = firstFinite(
    l4Metrics.compared_field_values,
    l4Metrics.compared_field_count,
    l4Metrics.total_field_comparisons
  );

  let fieldMatch = null;
  if (matchedFields !== null && matchedFields >= 0 && mismatchedFields !== null && mismatchedFields >= 0) {
    const total = matchedFields + mismatchedFields;
    if (total > 0) fieldMatch = Math.min(1, matchedFields / total);
  } else if (comparedFields !== null && comparedFields > 0 && mismatchedFields !== null && mismatchedFields >= 0) {
    fieldMatch = Math.max(0, Math.min(1, (comparedFields - mismatchedFields) / comparedFields));
  }

  // Data Match is intentionally based on the data population, not the number
  // of validation levels that passed. Record coverage and field conformity
  // represent different data defects, so combine them multiplicatively.
  if (recordMatch !== null && fieldMatch !== null) {
    return Math.max(0, Math.min(100, recordMatch * fieldMatch * 100));
  }
  if (fieldMatch !== null) return fieldMatch * 100;
  if (recordMatch !== null) return recordMatch * 100;
  return null;
}

function RunMetaStrip({ data }) {
  if (!data) return null;

  const status = String(data.status || "N/A").toUpperCase();
  const result = String(data.comparison_status || data.result || "N/A").toUpperCase();
  const engine = findEngine(data);
  const duration = findDuration(data);
  const validationScore = validationPercentage(data);
  const dataMatch = dataMatchPercentage(data);

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
        <span>VALIDATION SCORE</span>
        <strong>{percentage(validationScore)}</strong>
      </div>
      <div className="runMetaItem">
        <span>DATA MATCH</span>
        <strong>{percentage(dataMatch)}</strong>
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
    Promise.all([
      apiRequest(`/comparisons/${runId}/results`, { method: "GET" }),
      apiRequest("/comparisons", { method: "GET" }).catch(() => []),
    ])
      .then(([data, runs]) => {
        if (cancelled) return;
        const timing = Array.isArray(runs)
          ? runs.find((item) => item?.run_id === runId)
          : null;
        setMeta(timing ? { ...data, started_at: timing.started_at, finished_at: timing.finished_at } : data);
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
