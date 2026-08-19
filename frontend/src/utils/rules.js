function compactList(value) {
  if (!value) return [];
  const list = Array.isArray(value) ? value : [value];
  return list.filter(Boolean);
}

export function normalizeAggregateRulePayload(rule) {
  const payload = { ...(rule?.payload || rule || {}) };

  if (!payload.name && rule?.name) payload.name = rule.name;
  if (!payload.function && payload.operation) payload.function = payload.operation;

  if (payload.tolerance_pct === "" || payload.tolerance_pct === undefined) {
    delete payload.tolerance_pct;
  } else if (payload.tolerance_pct !== null) {
    payload.tolerance_pct = Number(payload.tolerance_pct);
  }

  if (payload.tolerance === "" || payload.tolerance === undefined) {
    delete payload.tolerance;
  } else if (typeof payload.tolerance !== "object" && payload.tolerance !== null) {
    payload.tolerance = Number(payload.tolerance);
  }

  payload.group_by_columns = compactList(payload.group_by_columns);
  payload.source_group_by = compactList(payload.source_group_by);
  payload.target_group_by = compactList(payload.target_group_by);

  return payload;
}

export function normalizeDqRulePayload(rule) {
  const payload = { ...(rule?.payload || rule || {}) };

  if (!payload.name && rule?.name) payload.name = rule.name;
  if (payload.rule_type) payload.rule_type = String(payload.rule_type).toUpperCase();
  if (!payload.apply_to) payload.apply_to = "BOTH";

  if (payload.column) {
    if (!payload.source_column) payload.source_column = payload.column;
    if (!payload.target_column) payload.target_column = payload.column;
  }

  return payload;
}
