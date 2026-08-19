const NUMERIC_TYPE_PATTERNS = [
  "INT",
  "INTEGER",
  "BIGINT",
  "SMALLINT",
  "TINYINT",
  "DECIMAL",
  "NUMERIC",
  "NUMBER",
  "FLOAT",
  "DOUBLE",
  "REAL",
];

export function getColumnName(column) {
  return column?.name || column?.column_name || column?.column || "";
}

export function getColumnType(column) {
  return String(column?.data_type || column?.type || "")
    .trim()
    .toUpperCase()
    .replace(/\(.*/, "");
}

export function isNumericColumn(column) {
  const normalizedType = getColumnType(column);
  return NUMERIC_TYPE_PATTERNS.some(
    (type) => normalizedType === type || normalizedType.includes(type)
  );
}

export function getSchemaColumnNames(schema) {
  return (schema || []).map(getColumnName).filter(Boolean);
}

export function findSchemaColumn(schema, name) {
  return (schema || []).find((column) => getColumnName(column) === name);
}

export function rowsEqual(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

export function defaultMappedPair(sourceSchema, targetSchema) {
  const sourceColumns = getSchemaColumnNames(sourceSchema);
  const targetColumns = getSchemaColumnNames(targetSchema);
  const sourceColumn = sourceColumns[0] || "";
  const targetColumn = targetColumns.includes(sourceColumn)
    ? sourceColumn
    : targetColumns[0] || "";

  return { source_column: sourceColumn, target_column: targetColumn };
}

export function isNumericMapping(mapping, sourceSchema, targetSchema) {
  const sourceColumn = findSchemaColumn(sourceSchema, mapping?.source_column);
  const targetColumn = findSchemaColumn(targetSchema, mapping?.target_column);
  return Boolean(
    sourceColumn &&
    targetColumn &&
    isNumericColumn(sourceColumn) &&
    isNumericColumn(targetColumn)
  );
}
