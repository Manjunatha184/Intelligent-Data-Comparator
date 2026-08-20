import React, { useEffect, useState } from "react";
import { ArrowRight, Check, Loader2, Plus, Trash2, X, Zap } from "lucide-react";

import { apiRequest } from "../../api/client";
import { Empty, Field, Loading, Panel, SelectField } from "../../components/ui";
import { COMPARISON_LEVELS, COMPARISON_LEVEL_ICONS, CONNECTORS } from "../../constants/app";
import { defaultMappedPair, findSchemaColumn, getColumnType, getSchemaColumnNames, isNumericColumn, isNumericMapping, rowsEqual } from "../../utils/schema";
import { normalizeAggregateRulePayload, normalizeDqRulePayload } from "../../utils/rules";

/* ============================================================
   COMPARISON BUILDER
============================================================ */

export function ComparisonBuilder({
  connections,
  notify,
  connectionsLoading,
  connectionsError,
  reloadConnections,
  onComplete,
}) {
  const [step, setStep] = useState(1);
  const [reviewModalOpen, setReviewModalOpen] = useState(false);

  const [sourceId, setSourceId] = useState("");

  const [targetId, setTargetId] = useState("");

  const [sourceDbCatalog, setSourceDbCatalog] = useState("");
  const [sourceDbSchema, setSourceDbSchema] = useState("");
  const [sourceDbTable, setSourceDbTable] = useState("");

  const [targetDbCatalog, setTargetDbCatalog] = useState("");
  const [targetDbSchema, setTargetDbSchema] = useState("");
  const [targetDbTable, setTargetDbTable] = useState("");

  const [levels, setLevels] = useState([]);

  const [sourceConnection, setSourceConnection] =
    useState(null);

  const [targetConnection, setTargetConnection] =
    useState(null);

  const [comparisonKeys, setComparisonKeys] =
    useState([
      {
        source_column: "",
        target_column: "",
      },
    ]);

  const [groupingAttributes, setGroupingAttributes] = useState([]);
  const [aggregationColumns, setAggregationColumns] = useState([]);

  const [selectedDqRuleIds, setSelectedDqRuleIds] = useState([]);

  const [selectedAggRuleIds, setSelectedAggRuleIds] =
    useState([]);

  const [availableRules, setAvailableRules] = useState([]);

  const [columnMappings, setColumnMappings] = useState([]);

  const [sourceSchema, setSourceSchema] = useState(null);
  const [targetSchema, setTargetSchema] = useState(null);
  const [sourceSchemaLoading, setSourceSchemaLoading] = useState(false);
  const [targetSchemaLoading, setTargetSchemaLoading] = useState(false);
  const [sourceSchemaError, setSourceSchemaError] = useState(null);
  const [targetSchemaError, setTargetSchemaError] = useState(null);
  const [sourceFilters, setSourceFilters] = useState([]);
  const [targetFilters, setTargetFilters] = useState([]);
  const [ignoredSourceColumns, setIgnoredSourceColumns] = useState([]);
  const [ignoredTargetColumns, setIgnoredTargetColumns] = useState([]);

  useEffect(() => {
    apiRequest("/rules").then(data => setAvailableRules(data || [])).catch(() => { });
  }, []);

  function refreshRules() {
    apiRequest("/rules")
      .then(data => setAvailableRules(data || []))
      .catch(() => { });
  }

  useEffect(() => {
    let cancelled = false;
    const isDatabricks = sourceConnection?.connector_type === "databricks";
    const ready = sourceConnection && (!isDatabricks || (sourceDbCatalog && sourceDbSchema && sourceDbTable));
    if (!ready) { setSourceSchema(null); return () => { cancelled = true; }; }
    if (step === 2 && sourceSchema) return () => { cancelled = true; };
    const properties = isDatabricks
      ? { ...sourceConnection.properties, connection_id: sourceConnection.connection_id, connection: { ...sourceConnection.properties }, catalog: sourceDbCatalog, schema: sourceDbSchema, table: sourceDbTable }
      : { ...sourceConnection.properties };
    setSourceSchema(null); setSourceSchemaError(null); setSourceSchemaLoading(true);
    apiRequest("/connections/schema", { method: "POST", body: JSON.stringify({ connector_type: sourceConnection.connector_type, properties }) })
      .then(data => { if (!cancelled) setSourceSchema(Array.isArray(data?.columns) ? data.columns : []); })
      .catch(error => { if (!cancelled) { setSourceSchema(null); setSourceSchemaError(error.message || "Unable to load table schema"); } })
      .finally(() => { if (!cancelled) setSourceSchemaLoading(false); });
    return () => { cancelled = true; };
  }, [sourceConnection, sourceDbCatalog, sourceDbSchema, sourceDbTable, step]);

  useEffect(() => {
    let cancelled = false;
    const isDatabricks = targetConnection?.connector_type === "databricks";
    const ready = targetConnection && (!isDatabricks || (targetDbCatalog && targetDbSchema && targetDbTable));
    if (!ready) { setTargetSchema(null); return () => { cancelled = true; }; }
    if (step === 2 && targetSchema) return () => { cancelled = true; };
    const properties = isDatabricks
      ? { ...targetConnection.properties, connection_id: targetConnection.connection_id, connection: { ...targetConnection.properties }, catalog: targetDbCatalog, schema: targetDbSchema, table: targetDbTable }
      : { ...targetConnection.properties };
    setTargetSchema(null); setTargetSchemaError(null); setTargetSchemaLoading(true);
    apiRequest("/connections/schema", { method: "POST", body: JSON.stringify({ connector_type: targetConnection.connector_type, properties }) })
      .then(data => { if (!cancelled) setTargetSchema(Array.isArray(data?.columns) ? data.columns : []); })
      .catch(error => { if (!cancelled) { setTargetSchema(null); setTargetSchemaError(error.message || "Unable to load table schema"); } })
      .finally(() => { if (!cancelled) setTargetSchemaLoading(false); });
    return () => { cancelled = true; };
  }, [targetConnection, targetDbCatalog, targetDbSchema, targetDbTable, step]);

  useEffect(() => {
    const sourceColumns = getSchemaColumnNames(sourceSchema);
    const targetColumns = getSchemaColumnNames(targetSchema);

    setComparisonKeys((current) => {
      const validPairs = (current || []).filter(
        (key) =>
          sourceColumns.includes(key.source_column) &&
          targetColumns.includes(key.target_column)
      );
      const next = validPairs.length
        ? [validPairs[0]]
        : [defaultMappedPair(sourceSchema, targetSchema)];

      return rowsEqual(current, next) ? current : next;
    });

    setColumnMappings((current) => {
      const next = (current || [])
        .filter(
          (mapping) =>
            sourceColumns.includes(mapping.source_column) &&
            targetColumns.includes(mapping.target_column)
        )
        .map((mapping) => {
          if (isNumericMapping(mapping, sourceSchema, targetSchema)) {
            return mapping;
          }

          const { tolerance_pct, ...withoutTolerance } = mapping;
          return withoutTolerance;
        });

      return rowsEqual(current, next) ? current : next;
    });


    setGroupingAttributes((current) => current.filter(mapping => sourceColumns.includes(mapping.source_column) && targetColumns.includes(mapping.target_column)));
    setAggregationColumns((current) => current.filter(mapping => sourceColumns.includes(mapping.source_column) && targetColumns.includes(mapping.target_column)));

    setSourceFilters((current) => current.filter((item) => sourceColumns.includes(item.field)));
    setTargetFilters((current) => current.filter((item) => targetColumns.includes(item.field)));
    setIgnoredSourceColumns((current) => current.filter((column) => sourceColumns.includes(column)));
    setIgnoredTargetColumns((current) => current.filter((column) => targetColumns.includes(column)));
  }, [sourceSchema, targetSchema]);

  const [running, setRunning] = useState(false);

  const connectedConnections =
    connections.filter(
      (connection) =>
        String(connection.status || "").trim().toUpperCase() === "CONNECTED"
    );

  const source = connectedConnections.find(
    (connection) =>
      String(connection.connection_id) ===
      String(sourceId)
  );

  const target = connectedConnections.find(
    (connection) =>
      String(connection.connection_id) ===
      String(targetId)
  );

  useEffect(() => {
    if (!sourceId) {
      setSourceConnection(null);
      return;
    }

    loadConnection(
      sourceId,
      setSourceConnection,
      notify
    );
  }, [sourceId]);

  useEffect(() => {
    if (!targetId) {
      setTargetConnection(null);
      return;
    }

    loadConnection(
      targetId,
      setTargetConnection,
      notify
    );
  }, [targetId]);

  function toggleLevel(levelId) {
    setLevels((current) => {
      if (current.includes(levelId)) {
        return current.filter(
          (level) => level !== levelId
        );
      }

      return [...current, levelId];
    });
  }

  async function runComparison() {
    if (!source || !target) {
      notify(
        "Select both source and target connections.",
        "error"
      );

      setStep(1);

      return;
    }

    if (
      !sourceConnection?.properties ||
      !targetConnection?.properties
    ) {
      notify(
        "Unable to load the selected connection properties.",
        "error"
      );

      return;
    }

    const validationLevels = levels.filter((level) => level !== "L7");
    if (!validationLevels.length) {
      notify(
        "Select at least one validation level before running the comparison.",
        "error"
      );

      setStep(2);

      return;
    }

    const sourceProperties = {
      ...sourceConnection.properties,
      connection_id: sourceConnection.connection_id,
    };

    const targetProperties = {
      ...targetConnection.properties,
      connection_id: targetConnection.connection_id,
    };

    /*
     * Databricks connector expects the connection
     * properties inside the dataset configuration,
     * and the metadata discovery state at the root.
     */
    if (
      sourceConnection.connector_type ===
      "databricks"
    ) {
      if (!sourceDbCatalog || !sourceDbSchema || !sourceDbTable) {
        notify("Select Catalog, Schema, and Table for the Databricks source.", "error");
        setStep(1);
        return;
      }
      sourceProperties.connection_id =
        sourceConnection.connection_id;

      sourceProperties.connection = {
        ...sourceConnection.properties,
      };
      sourceProperties.catalog = sourceDbCatalog;
      sourceProperties.schema = sourceDbSchema;
      sourceProperties.table = sourceDbTable;
    }

    if (
      targetConnection.connector_type ===
      "databricks"
    ) {
      if (!targetDbCatalog || !targetDbSchema || !targetDbTable) {
        notify("Select Catalog, Schema, and Table for the Databricks target.", "error");
        setStep(1);
        return;
      }
      targetProperties.connection_id =
        targetConnection.connection_id;
      targetProperties.connection = {
        ...targetConnection.properties,
      };
      targetProperties.catalog = targetDbCatalog;
      targetProperties.schema = targetDbSchema;
      targetProperties.table = targetDbTable;
    }

    const ignoredColumnsPayload = Array.from(new Set([
      ...ignoredSourceColumns,
      ...ignoredTargetColumns,
    ]));

    const comparisonKeyPayload = (comparisonKeys || [])
      .slice(0, 1)
      .filter((key) => key.source_column && key.target_column)
      .map((key) => ({
        source_column: key.source_column,
        target_column: key.target_column,
      }));

    if (!comparisonKeyPayload.length) {
      notify(
        "Select at least one mapped comparison key.",
        "error"
      );

      setStep(2);

      return;
    }

    if (comparisonKeyPayload.some((key) =>
      ignoredColumnsPayload.includes(key.source_column) ||
      ignoredColumnsPayload.includes(key.target_column)
    )) {
      notify("A record-matching key cannot also be an ignored column.", "error");
      setStep(2);
      return;
    }

    const hasGroupConfiguration = groupingAttributes.length || aggregationColumns.length;
    if (hasGroupConfiguration) {
      if (!groupingAttributes.length || groupingAttributes.some(item => !item.source_column || !item.target_column)) {
        notify("Select at least one complete grouping field pair.", "error"); setStep(2); return;
      }
      if (groupingAttributes.filter(item => item.source_column).length !== groupingAttributes.filter(item => item.target_column).length) {
        notify("Source and target grouping field counts must match.", "error"); setStep(2); return;
      }
      if (!aggregationColumns.length || aggregationColumns.some(item => !item.source_column || !item.target_column)) {
        notify("Select at least one complete aggregation field pair.", "error"); setStep(2); return;
      }
      if (aggregationColumns.filter(item => item.source_column).length !== aggregationColumns.filter(item => item.target_column).length) {
        notify("Source and target aggregation field counts must match.", "error"); setStep(2); return;
      }
      if (aggregationColumns.some(item => !item.operation)) {
        notify("Each aggregation mapping must have an operation.", "error"); setStep(2); return;
      }
      if (aggregationColumns.length !== new Set(aggregationColumns.map(item => item.source_column)).size || aggregationColumns.length !== new Set(aggregationColumns.map(item => item.target_column)).size) {
        notify("Aggregation fields cannot be duplicated.", "error"); setStep(2); return;
      }
      if (groupingAttributes.length !== new Set(groupingAttributes.map(item => item.source_column)).size || groupingAttributes.length !== new Set(groupingAttributes.map(item => item.target_column)).size) {
        notify("Grouping fields cannot be duplicated.", "error"); setStep(2); return;
      }
      if ([...groupingAttributes, ...aggregationColumns].some((item) =>
        ignoredColumnsPayload.includes(item.source_column) ||
        ignoredColumnsPayload.includes(item.target_column)
      )) {
        notify("Grouping and aggregation fields cannot also be ignored columns.", "error"); setStep(2); return;
      }
    }

    const columnMappingPayload = (columnMappings || [])
      .filter((mapping) => mapping.source_column && mapping.target_column)
      .map((mapping) => {
        const payloadMapping = {
          ...mapping,
        };

        if (
          mapping.tolerance_pct !== undefined &&
          mapping.tolerance_pct !== ""
        ) {
          payloadMapping.tolerance_pct = Number(mapping.tolerance_pct);
        } else {
          delete payloadMapping.tolerance_pct;
        }

        if (mapping.tolerance !== undefined && mapping.tolerance !== "") {
          payloadMapping.tolerance = Number(mapping.tolerance);
        } else {
          delete payloadMapping.tolerance;
        }

        return payloadMapping;
      });

    const payload = {

      source: {
        connector_type:
          sourceConnection.connector_type,
        properties: sourceProperties,
      },

      target: {
        connector_type:
          targetConnection.connector_type,
        properties: targetProperties,
      },

      comparison_levels: validationLevels,
      l7_enabled: levels.includes("L7"),

      comparison_keys: comparisonKeyPayload,

      column_mappings: columnMappingPayload,

      ignored_columns: ignoredColumnsPayload,

      aggregate_rules: availableRules
        .filter(r => selectedAggRuleIds.some(id => String(id) === String(r.rule_id)))
        .map(r => normalizeAggregateRulePayload(r)),

      dq_rules: availableRules
        .filter(r => selectedDqRuleIds.some(id => String(id) === String(r.rule_id)))
        .map(r => normalizeDqRulePayload(r)),

      source_filters: sourceFilters.filter(f => f.field).map(normalizeRowFilterPayload),
      target_filters: targetFilters.filter(f => f.field).map(normalizeRowFilterPayload),


      matching_mode: groupingAttributes.length || aggregationColumns.length
        ? "GROUP_RECONCILIATION"
        : "ROW_LEVEL",
      grouping_attributes: groupingAttributes,
      aggregation_columns: aggregationColumns,

      strategy_policy: {
        max_exact_rows: 100000,
        max_exact_bytes: 50000000,
        sampling_min_rows: 1000000,
        allow_sampling: false,
      },
    };

    setRunning(true);

    try {
      // --------------------------------------------------
      // STEP 1: SAVE CONFIGURATION
      // --------------------------------------------------

      const configurationResult = await apiRequest(
        "/configurations",
        {
          method: "POST",
          body: JSON.stringify({
            configuration: payload,
          }),
        }
      );

      const configurationId =
        configurationResult.configuration_id;

      if (!configurationId) {
        throw new Error(
          "Configuration was saved but no configuration ID was returned."
        );
      }

      // --------------------------------------------------
      // STEP 2: RUN COMPARISON USING DATABASE ID
      // --------------------------------------------------

      const comparisonPayload = {
        configuration_id: configurationId,
        ...payload,
      };

      const result = await apiRequest(
        "/comparisons",
        {
          method: "POST",
          body: JSON.stringify(
            comparisonPayload
          ),
        }
      );

      notify(
        `Comparison ${String(
          result.status
        ).toLowerCase()}.`
      );

      onComplete(result.run_id);

    } catch (error) {
      notify(error.message, "error");
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="stack comparisonBuilder">
      <div className="wizardFooter">
        <div style={{ display: "flex", alignItems: "center", gap: "15px" }}>
          <div className="builderHeading">
            <h1 className="pageTitle" style={{ margin: 0 }}>Create a comparison</h1>
            <div className="comparisonProgress" aria-label="Comparison setup progress">
              <span className={step >= 1 ? "active" : ""}><b>01</b> Define scope</span>
              <i />
              <span className={step >= 2 ? "active" : ""}><b>02</b> Review & run</span>
            </div>
          </div>
        </div>

        <div className="actionRow">
          {step > 1 && (
            <button
              className="secondary"
              onClick={() =>
                setStep((current) => current - 1)
              }
            >
              Back
            </button>
          )}

          {step === 1 ? (
            <button
              className="primary"
              disabled={!sourceId || !targetId || !levels.some((level) => level !== "L7")}
              onClick={() => setStep(2)}
            >
              Continue
              <ArrowRight size={15} />
            </button>
          ) : (
            <button
              className="primary"
              onClick={() => setReviewModalOpen(true)}
            >
              Review & Run
              <ArrowRight size={15} />
            </button>
          )}
        </div>
      </div>

      {step === 1 && (
        <div className="stack">
          <SourceStep
            connections={connectedConnections}
            source={source}
            target={target}
            sourceId={sourceId}
            targetId={targetId}
            setSourceId={setSourceId}
            setTargetId={setTargetId}
            sourceDbCatalog={sourceDbCatalog}
            setSourceDbCatalog={setSourceDbCatalog}
            sourceDbSchema={sourceDbSchema}
            setSourceDbSchema={setSourceDbSchema}
            sourceDbTable={sourceDbTable}
            setSourceDbTable={setSourceDbTable}
            targetDbCatalog={targetDbCatalog}
            setTargetDbCatalog={setTargetDbCatalog}
            targetDbSchema={targetDbSchema}
            setTargetDbSchema={setTargetDbSchema}
            targetDbTable={targetDbTable}
            setTargetDbTable={setTargetDbTable}
            notify={notify}
            connectionsLoading={connectionsLoading}
            connectionsError={connectionsError}
            reloadConnections={reloadConnections}
          />
          <LevelsStep
            levels={levels}
            toggleLevel={toggleLevel}
          />
        </div>
      )}

      {step === 2 && (
        <RulesStep
          comparisonKeys={comparisonKeys}
          setComparisonKeys={setComparisonKeys}
          groupingAttributes={groupingAttributes}
          setGroupingAttributes={setGroupingAttributes}
          aggregationColumns={aggregationColumns}
          setAggregationColumns={setAggregationColumns}
          availableRules={availableRules}
          selectedDqRuleIds={selectedDqRuleIds}
          setSelectedDqRuleIds={setSelectedDqRuleIds}
          selectedAggRuleIds={selectedAggRuleIds}
          setSelectedAggRuleIds={setSelectedAggRuleIds}
          notify={notify}
          onRulesChanged={refreshRules}
          levels={levels}
          columnMappings={columnMappings}
          setColumnMappings={setColumnMappings}
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          sourceSchemaLoading={sourceSchemaLoading}
          targetSchemaLoading={targetSchemaLoading}
          sourceSchemaError={sourceSchemaError}
          targetSchemaError={targetSchemaError}
          sourceFilters={sourceFilters}
          setSourceFilters={setSourceFilters}
          targetFilters={targetFilters}
          setTargetFilters={setTargetFilters}
          ignoredSourceColumns={ignoredSourceColumns}
          setIgnoredSourceColumns={setIgnoredSourceColumns}
          ignoredTargetColumns={ignoredTargetColumns}
          setIgnoredTargetColumns={setIgnoredTargetColumns}
        />
      )}

      {reviewModalOpen && (
        <ReviewModal
          source={sourceConnection}
          target={targetConnection}
          levels={levels}
          comparisonKeys={comparisonKeys}
          sourceFiltersCount={sourceFilters.length}
          targetFiltersCount={targetFilters.length}
          ignoredColumnsCount={new Set([...ignoredSourceColumns, ...ignoredTargetColumns]).size}
          mappingsCount={columnMappings.length}
          dqRulesCount={selectedDqRuleIds.length}
          aggregateRulesCount={selectedAggRuleIds.length}
          onClose={() => setReviewModalOpen(false)}
          onRun={runComparison}
          running={running}
        />
      )}

    </div>
  );
}

/* ============================================================
   CONNECTION DETAIL LOADING
============================================================ */

async function loadConnection(
  connectionId,
  setter,
  notify
) {
  try {
    const connection = await apiRequest(
      `/connections/${connectionId}`
    );

    setter(connection);
  } catch (error) {
    setter(null);
    notify(error.message, "error");
  }
}

/* ============================================================
   COMPARISON SOURCES
============================================================ */

function SourceStep({
  connections,
  source,
  target,
  sourceId,
  targetId,
  setSourceId,
  setTargetId,
  sourceDbCatalog,
  setSourceDbCatalog,
  sourceDbSchema,
  setSourceDbSchema,
  sourceDbTable,
  setSourceDbTable,
  targetDbCatalog,
  setTargetDbCatalog,
  targetDbSchema,
  setTargetDbSchema,
  targetDbTable,
  setTargetDbTable,
  notify,
  connectionsLoading,
  connectionsError,
  reloadConnections,
}) {
  return (
    <section className="scopeSourceSection">
      <div className="scopeSectionIntro">
        <div>
          <h2>Choose what you want to compare</h2>
        </div>
      </div>

      <div className="grid2 scopeSourceGrid">
      <Panel title="Source dataset" className="scopeDatasetCard">
        <ConnectionSelector
          label="Source connection"
          value={sourceId}
          setValue={setSourceId}
          connections={connections}
          loading={connectionsLoading}
          error={connectionsError}
          onRetry={reloadConnections}
        />
        {source?.connector_type === "databricks" && (
          <DatabricksSelector
            connection={source}
            catalog={sourceDbCatalog}
            setCatalog={setSourceDbCatalog}
            schema={sourceDbSchema}
            setSchema={setSourceDbSchema}
            table={sourceDbTable}
            setTable={setSourceDbTable}
            notify={notify}
          />
        )}
      </Panel>

      <Panel title="Target dataset" className="scopeDatasetCard">
        <ConnectionSelector
          label="Target connection"
          value={targetId}
          setValue={setTargetId}
          connections={connections}
          loading={connectionsLoading}
          error={connectionsError}
          onRetry={reloadConnections}
        />
        {target?.connector_type === "databricks" && (
          <DatabricksSelector
            connection={target}
            catalog={targetDbCatalog}
            setCatalog={setTargetDbCatalog}
            schema={targetDbSchema}
            setSchema={setTargetDbSchema}
            table={targetDbTable}
            setTable={setTargetDbTable}
            notify={notify}
          />
        )}
      </Panel>
      </div>
    </section>
  );
}

function DatabricksSelector({
  connection,
  catalog,
  setCatalog,
  schema,
  setSchema,
  table,
  setTable,
  notify
}) {
  const [catalogs, setCatalogs] = useState([]);
  const [schemas, setSchemas] = useState([]);
  const [tables, setTables] = useState([]);

  const [loadingCatalog, setLoadingCatalog] = useState(false);
  const [loadingSchema, setLoadingSchema] = useState(false);
  const [loadingTable, setLoadingTable] = useState(false);

  useEffect(() => {
    if (!connection) return;
    setLoadingCatalog(true);
    apiRequest("/connections/discover/catalogs", {
      method: "POST",
      body: JSON.stringify({
        connector_type: connection.connector_type,
        properties: {
          ...connection.properties,
          connection_id: connection.connection_id,
        }
      })
    })
      .then(res => setCatalogs(res || []))
      .catch(e => notify("Failed to load catalogs: " + e.message, "error"))
      .finally(() => setLoadingCatalog(false));
  }, [connection]);

  useEffect(() => {
    if (!connection || !catalog) {
      setSchemas([]);
      return;
    }
    setLoadingSchema(true);
    apiRequest("/connections/discover/schemas", {
      method: "POST",
      body: JSON.stringify({
        connector_type: connection.connector_type,
        properties: {
          ...connection.properties,
          connection_id: connection.connection_id,
        },
        catalog
      })
    })
      .then(res => setSchemas(res || []))
      .catch(e => notify("Failed to load schemas: " + e.message, "error"))
      .finally(() => setLoadingSchema(false));
  }, [connection, catalog]);

  useEffect(() => {
    if (!connection || !catalog || !schema) {
      setTables([]);
      return;
    }
    setLoadingTable(true);
    apiRequest("/connections/discover/tables", {
      method: "POST",
      body: JSON.stringify({
        connector_type: connection.connector_type,
        properties: {
          ...connection.properties,
          connection_id: connection.connection_id,
        },
        catalog,
        schema_name: schema
      })
    })
      .then(res => setTables(res || []))
      .catch(e => notify("Failed to load tables: " + e.message, "error"))
      .finally(() => setLoadingTable(false));
  }, [connection, catalog, schema]);

  return (
    <div style={{ marginTop: "16px", display: "flex", flexDirection: "column", gap: "12px" }}>
      <label className="field">
        <span>Catalog<em>*</em></span>
        <select value={catalog} onChange={e => { setCatalog(e.target.value); setSchema(""); setTable(""); }}>
          <option value="">{loadingCatalog ? "Loading catalogs..." : "Select catalog…"}</option>
          {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
      </label>

      <label className="field">
        <span>Schema<em>*</em></span>
        <select value={schema} onChange={e => { setSchema(e.target.value); setTable(""); }} disabled={!catalog}>
          <option value="">{loadingSchema ? "Loading schemas..." : "Select schema…"}</option>
          {schemas.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </label>

      <label className="field">
        <span>Table<em>*</em></span>
        <select value={table} onChange={e => setTable(e.target.value)} disabled={!schema}>
          <option value="">{loadingTable ? "Loading tables..." : "Select table…"}</option>
          {tables.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </label>
    </div>
  );
}

function ConnectionSelector({
  label,
  value,
  setValue,
  connections,
  loading = false,
  error = null,
  onRetry,
}) {
  const displayConnection = (connection) => {
    const properties = connection.properties || {};
    const filename =
      properties.filename ||
      properties.path?.split("/").pop();
    const dataset =
      connection.connector_type === "csv"
        ? filename
        : properties.table;

    return dataset
      ? `${connection.name} (${dataset})`
      : connection.name;
  };

  return (
    <div className="field connectionSelector">
      <span>
        {label}
        <em>*</em>
      </span>

      {loading && <div className="connectionSelectorState">Loading connections...</div>}
      {!loading && error && (
        <div className="connectionSelectorState connectionSelectorError">
          <span>Unable to load connections</span>
          <button type="button" className="textBtn" onClick={onRetry}>Retry</button>
        </div>
      )}
      {!loading && !error && connections.length === 0 && (
        <div className="connectionSelectorState">No connections available</div>
      )}
      {!loading && !error && connections.length > 0 && (
        <select
          value={value}
          onChange={(event) => setValue(event.target.value)}
          aria-label={label}
        >
          <option value="">Select an authenticated connection...</option>
          {connections.map((connection) => {
            return (
              <option
                key={connection.connection_id}
                value={String(connection.connection_id)}
              >
                {displayConnection(connection)} · {CONNECTORS[connection.connector_type]?.label || connection.connector_type}
              </option>
            );
          })}
        </select>
      )}
    </div>
  );
}

/* ============================================================
   LEVELS
============================================================ */

function LevelsStep({
  levels,
  toggleLevel,
}) {
  return (
    <Panel title="Comparison depth" className="scopeLevelsPanel">
      <div className="scopeLevelIntro">
        <p className="helper">
          Build the validation path from structural checks through plain-language analysis.
        </p>
        <span className="scopeSelectionCount">{levels.length} of {COMPARISON_LEVELS.length} selected</span>
      </div>

      <div className="levelGrid">
        {COMPARISON_LEVELS.map((level) => {
          const selected = levels.includes(
            level.id
          );
          const LevelIcon = COMPARISON_LEVEL_ICONS[level.id];

          return (
            <button
              type="button"
              key={level.id}
              className={`level ${selected ? "selected" : ""} level-${level.id}`}
              onClick={() => toggleLevel(level.id)}
            >
              <span className="levelVisual">
                <LevelIcon size={17} />
                <span className="levelCode">{level.id}</span>
              </span>

              <div>
                <b>{level.name}</b>
                <small>
                  {level.description}
                </small>
              </div>

              <span className="checkCircle">
                {selected && (
                  <Check size={13} />
                )}
              </span>
            </button>
          );
        })}
      </div>
    </Panel>
  );
}

/* ============================================================
   RULES
============================================================ */

function RuleSelectionModal({
  title,
  rules,
  selectedIds,
  onSelectionChange,
  onClose,
  category,
  sourceSchema,
  targetSchema,
  notify,
  onRulesChanged,
}) {
  const [ruleEditorOpen, setRuleEditorOpen] = useState(false);

  return (
    <div className="modalBackdrop">
      <div className="modal">
        <div className="modalHead">
          <div>
            <h3>{title}</h3>
            <p className="helper">Select rules from the repository</p>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            {category && (
              <button type="button" className="secondary small" onClick={() => setRuleEditorOpen(true)}>
                <Plus size={14} /> New rule
              </button>
            )}
            <button type="button" className="iconButton" onClick={onClose}>
              <X size={18} />
            </button>
          </div>
        </div>

        <div className="modalBody stack" style={{ maxHeight: "400px", overflowY: "auto", padding: "0 25px 20px" }}>
          {rules.length === 0 ? (
            <div className="empty compact" style={{ marginTop: "20px" }}>
              <b>No rules found</b>
            </div>
          ) : (
            <div className="ruleTable" style={{ marginTop: "20px" }}>
              {rules.map((rule) => (
                <label key={rule.rule_id} className="ruleCheckbox">
                  <input
                    type="checkbox"
                    checked={selectedIds.includes(rule.rule_id)}
                    onChange={(e) => {
                      if (e.target.checked)
                        onSelectionChange([...selectedIds, rule.rule_id]);
                      else
                        onSelectionChange(
                          selectedIds.filter((id) => id !== rule.rule_id)
                        );
                    }}
                  />
                  <div>
                    <b>{rule.name}</b>
                    <span>
                      {rule.rule_type === "DQ"
                        ? describeDqRule(rule.payload)
                        : `${String(rule.payload.function).toLowerCase()} on ${String(
                          rule.payload.source_column
                        ).toLowerCase()}`}
                    </span>
                  </div>
                </label>
              ))}
            </div>
          )}
        </div>
      </div>
      {ruleEditorOpen && (
        <RuleModal
          initialRuleType={category}
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          onClose={() => setRuleEditorOpen(false)}
          onDone={() => {
            setRuleEditorOpen(false);
            onRulesChanged?.();
          }}
          notify={notify}
        />
      )}
    </div>
  );
}

export function describeDqRule(payload = {}) {
  const applyTo = String(payload.apply_to || "BOTH").toUpperCase();
  const sourceColumn = payload.source_column || payload.column;
  const targetColumn = payload.target_column || payload.column;
  const scopeLabel = applyTo === "SOURCE" ? sourceColumn : applyTo === "TARGET" ? targetColumn : `${sourceColumn} → ${targetColumn}`;
  return `${String(payload.rule_type || payload.type || "rule").toLowerCase()} on ${scopeLabel}`;
}

export function schemaRuleOptions(schema, currentValue, schemaAware) {
  const schemaColumns = getSchemaColumnNames(schema);

  if (schemaAware) {
    return ["", ...schemaColumns];
  }

  return Array.from(
    new Set([
      "",
      ...schemaColumns,
      currentValue || "",
    ])
  );
}


function MultiSelectField({ options, selected, onChange, placeholder }) {
  const [open, setOpen] = React.useState(false);
  const rootRef = React.useRef(null);
  React.useEffect(() => {
    const close = (event) => { if (rootRef.current && !rootRef.current.contains(event.target)) setOpen(false); };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, []);
  const toggle = (value) => onChange(selected.includes(value) ? selected.filter(item => item !== value) : [...selected, value]);
  return <div className="multiSelectField" ref={rootRef}>
    <button type="button" className="multiSelectTrigger" aria-expanded={open} onClick={() => setOpen(!open)}>
      <span>{placeholder}</span><span className="multiSelectChevron">{open ? "▲" : "▼"}</span>
    </button>
    {open && <div className="multiSelectMenu" role="listbox">
      {options.map(option => <button type="button" role="option" aria-selected={selected.includes(option)} className="multiSelectOption" key={option} onClick={() => toggle(option)}>
        <span className="multiSelectCheck">{selected.includes(option) ? "✓" : ""}</span>{option}
      </button>)}
      {!options.length && <span className="multiSelectEmpty">No schema fields available</span>}
    </div>}
  </div>;
}

function RulesStep({
  comparisonKeys,
  setComparisonKeys,
  groupingAttributes,
  setGroupingAttributes,
  aggregationColumns,
  setAggregationColumns,
  availableRules,
  selectedDqRuleIds,
  setSelectedDqRuleIds,
  selectedAggRuleIds,
  setSelectedAggRuleIds,
  notify,
  onRulesChanged,
  levels,
  columnMappings,
  setColumnMappings,
  sourceSchema,
  targetSchema,
  sourceSchemaLoading,
  targetSchemaLoading,
  sourceSchemaError,
  targetSchemaError,
  sourceFilters,
  setSourceFilters,
  targetFilters,
  setTargetFilters,
  ignoredSourceColumns,
  setIgnoredSourceColumns,
  ignoredTargetColumns,
  setIgnoredTargetColumns,
}) {
  const [dqModalOpen, setDqModalOpen] = React.useState(false);
  const [aggModalOpen, setAggModalOpen] = React.useState(false);
  const [normalizationOpen, setNormalizationOpen] = React.useState({});
  const [pendingGroupingSource, setPendingGroupingSource] = React.useState("");
  const [pendingGroupingTarget, setPendingGroupingTarget] = React.useState("");
  const [pendingAggregationSource, setPendingAggregationSource] = React.useState("");
  const [pendingAggregationTarget, setPendingAggregationTarget] = React.useState("");

  const sourceColumnOptions = getSchemaColumnNames(sourceSchema);
  const targetColumnOptions = getSchemaColumnNames(targetSchema);

  const selectedKey = comparisonKeys?.[0] || {
    source_column: "",
    target_column: "",
  };

  const updateSelectedKey = (field, value) => {
    setComparisonKeys([
      {
        ...selectedKey,
        [field]: value,
      },
    ]);
  };


  const sourceGroupingFields = groupingAttributes.map((item) => item.source_column).filter(Boolean);
  const targetGroupingFields = groupingAttributes.map((item) => item.target_column).filter(Boolean);
  const sourceAggregationFields = aggregationColumns.map((item) => item.source_column).filter(Boolean);
  const targetAggregationFields = aggregationColumns.map((item) => item.target_column).filter(Boolean);
  const logicalColumnName = (value) => String(value || "")
    .trim()
    .toLowerCase()
    .replace(/^(source|target|src|tgt)[_\s-]+/, "");
  const mappedCounterpart = (value, side) => {
    const explicitMapping = (columnMappings || []).find((mapping) =>
      side === "source"
        ? mapping.source_column === value && mapping.target_column
        : mapping.target_column === value && mapping.source_column
    );
    if (explicitMapping) {
      return side === "source"
        ? explicitMapping.target_column
        : explicitMapping.source_column;
    }

    const counterpartOptions = side === "source"
      ? targetColumnOptions
      : sourceColumnOptions;
    const exactName = counterpartOptions.find(
      (option) => option.toLowerCase() === String(value).toLowerCase()
    );
    if (exactName) return exactName;

    const logicalName = logicalColumnName(value);
    const logicalMatches = counterpartOptions.filter(
      (option) => logicalColumnName(option) === logicalName
    );
    return logicalMatches.length === 1 ? logicalMatches[0] : "";
  };
  const updatePairedSelection = (kind, side, values) => {
    const current = kind === "group" ? groupingAttributes : aggregationColumns;
    const selected = new Set(values);
    const field = `${side}_column`;
    const otherField = side === "source" ? "target_column" : "source_column";
    const existing = new Set(current.map((item) => item[field]).filter(Boolean));

    // Keep completed mappings intact by identity, not by the array position
    // of either multi-select. Only a newly selected field can fill an
    // intentionally incomplete mapping created on the opposite side.
    const next = current
      .map((item) => {
        if (!item[field] || selected.has(item[field])) return { ...item };
        const updated = { ...item, [field]: "" };
        if (kind === "aggregate" && side === "source") delete updated.operation;
        return updated;
      })
      .filter((item) => item.source_column || item.target_column);

    values.filter((value) => !existing.has(value)).forEach((value) => {
      // Resolve logical counterparts instead of pairing by selection order.
      const counterpart = mappedCounterpart(value, side);
      const pending = counterpart
        ? next.find(
          (item) => !item[field] && item[otherField] === counterpart
        )
        : null;
      const mapping = pending || { source_column: "", target_column: "" };
      mapping[field] = value;
      if (kind === "aggregate" && mapping.source_column) {
        mapping.operation = automaticOperation(mapping.source_column, "source");
      }
      if (!pending) next.push(mapping);
    });

    kind === "group" ? setGroupingAttributes(next) : setAggregationColumns(next);
  };
  const selectionChips = (mappings, kind, side) => <div className="chipRow">
    {mappings.map((mapping, index) => {
      const value = mapping[`${side}_column`];
      if (!value) return null;
      return <span className="chip" key={`${kind}-${side}-${mapping.source_column}-${mapping.target_column}-${index}`}>
        {value}<button type="button" aria-label={`Remove ${value}`} onClick={() => {
          const current = kind === "group" ? groupingAttributes : aggregationColumns;
          const field = `${side}_column`;
          const next = current
            .map((item, mappingIndex) => {
              if (mappingIndex !== index) return { ...item };
              const updated = { ...item, [field]: "" };
              if (kind === "aggregate" && side === "source") delete updated.operation;
              return updated;
            })
            .filter((item) => item.source_column || item.target_column);
          kind === "group" ? setGroupingAttributes(next) : setAggregationColumns(next);
        }}>×</button></span>;
    })}
  </div>;
  const sourceTypeFor = (name) => getColumnType(findSchemaColumn(sourceSchema, name));
  const automaticOperation = (name, side) => {
    const schema = side === "source" ? sourceSchema : targetSchema;
    return isNumericColumn(findSchemaColumn(schema, name)) ? "AVG" : "MODE";
  };
  const FieldChips = ({ values, onRemove }) => (
    <div className="chipRow">
      {values.map((value, index) => <span className="chip" key={`${value}-${index}`}>{value}<button type="button" aria-label={`Remove ${value}`} onClick={() => onRemove(index)}>×</button></span>)}
    </div>
  );
  const MultiFieldPicker = ({ label, options, selected, onAdd }) => (
    <div className="field">
      <label>{label}</label>
      <select value="" onChange={(event) => onAdd(event.target.value)}>
        <option value="">Select fields...</option>
        {options.filter((option) => !selected.includes(option)).map(option => <option key={option} value={option}>{option}</option>)}
      </select>
    </div>
  );


  return (
    <div className="stack reviewRunStep">
      {(sourceSchemaLoading || targetSchemaLoading || sourceSchemaError || targetSchemaError) && (
        <div className="helper" role="status">
          {sourceSchemaError || targetSchemaError || (sourceSchemaLoading || targetSchemaLoading ? "Loading fields..." : "")}
        </div>
      )}
      <div className="filtersGrid">
        <FilterSection title="Source Filters" schema={sourceSchema} filters={sourceFilters} setFilters={setSourceFilters} />
        <FilterSection title="Target Filters" schema={targetSchema} filters={targetFilters} setFilters={setTargetFilters} />
      </div>
      <Panel title="Ignored columns" className="reviewRunCard ignoredColumnsCard">
        <p className="helper">Selected columns are excluded from every applicable comparison level.</p>
        <div className="formGrid">
          <div className="mappingPickerBlock">
            <label>Source columns to ignore</label>
            <MultiSelectField options={sourceColumnOptions} selected={ignoredSourceColumns} onChange={setIgnoredSourceColumns} placeholder="Select source columns" />
            <FieldChips values={ignoredSourceColumns} onRemove={(index) => setIgnoredSourceColumns(ignoredSourceColumns.filter((_, itemIndex) => itemIndex !== index))} />
          </div>
          <div className="mappingPickerBlock">
            <label>Target columns to ignore</label>
            <MultiSelectField options={targetColumnOptions} selected={ignoredTargetColumns} onChange={setIgnoredTargetColumns} placeholder="Select target columns" />
            <FieldChips values={ignoredTargetColumns} onRemove={(index) => setIgnoredTargetColumns(ignoredTargetColumns.filter((_, itemIndex) => itemIndex !== index))} />
          </div>
        </div>
      </Panel>
      <Panel title="Record matching" className="reviewRunCard recordMatchingCard">
        <div className="formGrid">
          <SelectField
            label="Source key"
            value={selectedKey.source_column || ""}
            setValue={(value) => updateSelectedKey("source_column", value)}
            options={["", ...sourceColumnOptions]}
          />

          <SelectField
            label="Target key"
            value={selectedKey.target_column || ""}
            setValue={(value) => updateSelectedKey("target_column", value)}
            options={["", ...targetColumnOptions]}
          />
        </div>


      </Panel>

      <Panel title="Group-Based Reconciliation" className="reviewRunCard reconciliationCard">
        <section className="reconciliationSection">
          <h4>Grouping fields</h4>
          <div className="formGrid">
            <div className="mappingPickerBlock">
              <label>Source grouping fields</label>
              <MultiSelectField options={sourceColumnOptions} selected={sourceGroupingFields} onChange={values => updatePairedSelection("group", "source", values)} placeholder="Select source fields" />
              {selectionChips(groupingAttributes, "group", "source")}
            </div>
            <div className="mappingPickerBlock">
              <label>Target grouping fields</label>
              <MultiSelectField options={targetColumnOptions} selected={targetGroupingFields} onChange={values => updatePairedSelection("group", "target", values)} placeholder="Select target fields" />
              {selectionChips(groupingAttributes, "group", "target")}
            </div>
          </div>
        </section>

        <section className="reconciliationSection">
          <h4>Aggregation fields</h4>
          <div className="formGrid">
            <div className="mappingPickerBlock">
              <label>Source aggregation fields</label>
              <MultiSelectField options={sourceColumnOptions} selected={sourceAggregationFields} onChange={values => updatePairedSelection("aggregate", "source", values)} placeholder="Select source fields" />
              {selectionChips(aggregationColumns, "aggregate", "source")}
            </div>
            <div className="mappingPickerBlock">
              <label>Target aggregation fields</label>
              <MultiSelectField options={targetColumnOptions} selected={targetAggregationFields} onChange={values => updatePairedSelection("aggregate", "target", values)} placeholder="Select target fields" />
              {selectionChips(aggregationColumns, "aggregate", "target")}
            </div>
          </div>
        </section>
        <p className="reconciliationNote">Numeric aggregation fields use AVG automatically; non-numeric aggregation fields use MODE automatically.</p>
      </Panel>

      <div className={`reviewRunSummaryGrid ${columnMappings?.length ? "hasMappings" : ""}`}>
      {!columnMappings || columnMappings.length === 0 ? (
        <Panel
          title="Column Mapping"
          className="reviewRunCard compactReviewCard"
          action={<button type="button" className="secondary small" onClick={() => setColumnMappings([{ source_column: "", target_column: "", tolerance_pct: undefined }])}>
            <Plus size={14} /> Add column mapping
          </button>}
        >
          <p className="reviewEmptyState">No mappings configured.</p>
        </Panel>
      ) : (
        <Panel title="Column Mapping" className="reviewRunCard">
          <div className="stack" style={{ gap: "10px" }}>
            {columnMappings.map((mapping, idx) => {
              const isNumericPair = isNumericMapping(mapping, sourceSchema, targetSchema);
              const updateMapping = (key, val) => {
                const copy = [...columnMappings];
                const updated = {
                  ...copy[idx],
                  [key]: val,
                };

                if (
                  key === "source_column" ||
                  key === "target_column"
                ) {
                  const nextMapping = {
                    ...updated,
                  };

                  if (!isNumericMapping(nextMapping, sourceSchema, targetSchema)) {
                    delete nextMapping.tolerance_pct;
                  }

                  copy[idx] = nextMapping;
                } else {
                  copy[idx] = updated;
                }

                setColumnMappings(copy);
              };

              return (
                <div key={idx} className="columnMappingRow" style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr) minmax(130px, 0.7fr) auto auto", gap: "10px", alignItems: "end", padding: "10px", border: "1px solid var(--line)", borderRadius: "8px", background: "#f8fafc" }}>
                  <div>
                    <SelectField
                      label="Source column"
                      value={mapping.source_column || ""}
                      options={["", ...sourceColumnOptions]}
                      setValue={v => updateMapping("source_column", v)}
                    />
                  </div>
                  <div>
                    <SelectField
                      label="Target column"
                      value={mapping.target_column || ""}
                      options={["", ...targetColumnOptions]}
                      setValue={v => updateMapping("target_column", v)}
                    />
                  </div>
                  <div>
                    <Field label="Tolerance (%)">
                      {isNumericPair ? (
                        <input
                          type="number"
                          min="0"
                          max="100"
                          step="any"
                          value={mapping.tolerance_pct === undefined ? "" : mapping.tolerance_pct}
                          onChange={e => updateMapping("tolerance_pct", e.target.value ? Number(e.target.value) : undefined)}
                        />
                      ) : (
                        <div style={{ padding: "8px 12px", background: "var(--surface)", border: "1px solid var(--line)", borderRadius: "6px", color: "var(--muted)", fontSize: "13px" }}>
                          N/A
                        </div>
                      )}
                    </Field>
                  </div>
                  <button type="button" className="secondary small" onClick={() => setNormalizationOpen(current => ({ ...current, [idx]: !current[idx] }))} style={{ marginBottom: "2px" }}>
                    {normalizationOpen[idx] ? "Hide" : "Configure"}
                  </button>
                  <button type="button" className="iconButton dangerIcon" title="Delete mapping" onClick={() => setColumnMappings(columnMappings.filter((_, i) => i !== idx))} style={{ marginBottom: "2px" }}><Trash2 size={15} /></button>
                  {normalizationOpen[idx] && (
                    <div style={{ gridColumn: "1 / -1", display: "flex", alignItems: "center", gap: "18px", flexWrap: "wrap", padding: "10px 4px 2px", borderTop: "1px solid var(--line)" }}>
                      <strong style={{ fontSize: "12px", color: "var(--text-secondary)" }}>Normalization options</strong>
                      {[["trim", "Trim whitespace"], ["case_insensitive", "Ignore case"], ["empty_as_null", "Empty string as null"]].map(([key, label]) => (
                        <label key={key} style={{ display: "inline-flex", alignItems: "center", gap: "6px", fontSize: "12px", color: "var(--text-secondary)", whiteSpace: "nowrap" }}>
                          <input type="checkbox" checked={Boolean(mapping.normalization?.[key])} onChange={e => updateMapping("normalization", { ...(mapping.normalization || {}), [key]: e.target.checked })} />
                          {label}
                        </label>
                      ))}
                      {isNumericPair && <label style={{ display: "inline-flex", alignItems: "center", gap: "7px", fontSize: "12px", color: "var(--text-secondary)", whiteSpace: "nowrap" }}>Round decimals<input style={{ width: "70px" }} type="number" min="0" step="1" value={mapping.normalization?.round ?? ""} onChange={e => {
                        const value = e.target.value;
                        updateMapping("normalization", { ...(mapping.normalization || {}), ...(value === "" ? { round: undefined } : { round: Number(value) }) });
                      }} /></label>}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
          <div style={{ marginTop: "15px" }}>
            <button type="button" className="secondary small" onClick={() => setColumnMappings([...(columnMappings || []), { source_column: "", target_column: "", tolerance_pct: undefined }])}>
              <Plus size={14} /> Add column mapping
            </button>
          </div>
        </Panel>
      )}

      <Panel title="Data quality rules (L6)" className="reviewRunCard compactReviewCard" action={<button type="button" className="secondary small" onClick={() => setDqModalOpen(true)}>Select rules</button>}>
        <p className="reviewEmptyState">
          {selectedDqRuleIds.length === 0 ? "No data-quality rules selected." : `${selectedDqRuleIds.length} data-quality rule${selectedDqRuleIds.length === 1 ? "" : "s"} selected.`}
        </p>
      </Panel>

      <Panel title="Aggregate rules (L5)" className="reviewRunCard compactReviewCard" action={<button type="button" className="secondary small" onClick={() => setAggModalOpen(true)}>Select rules</button>}>
        <p className="reviewEmptyState">
          {selectedAggRuleIds.length === 0 ? "No aggregate rules selected." : `${selectedAggRuleIds.length} aggregate rule${selectedAggRuleIds.length === 1 ? "" : "s"} selected.`}
        </p>
      </Panel>
      </div>

      {dqModalOpen && (
        <RuleSelectionModal
          title="Data quality rules (L6)"
          rules={availableRules.filter(r => r.rule_type === "DQ")}
          selectedIds={selectedDqRuleIds}
          onSelectionChange={setSelectedDqRuleIds}
          onClose={() => setDqModalOpen(false)}
          category="DQ"
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          notify={notify}
          onRulesChanged={onRulesChanged}
        />
      )}

      {aggModalOpen && (
        <RuleSelectionModal
          title="Aggregate rules (L5)"
          rules={availableRules.filter(r => r.rule_type === "AGGREGATE")}
          selectedIds={selectedAggRuleIds}
          onSelectionChange={setSelectedAggRuleIds}
          onClose={() => setAggModalOpen(false)}
          category="AGGREGATE"
          sourceSchema={sourceSchema}
          targetSchema={targetSchema}
          notify={notify}
          onRulesChanged={onRulesChanged}
        />
      )}
    </div>
  );
}

const FILTER_OPERATORS = ["=", "!=", ">", ">=", "<", "<=", "IN", "IS NULL", "IS NOT NULL"];
const NULL_FILTER_OPERATORS = new Set(["IS NULL", "IS NOT NULL"]);

function normalizeRowFilterPayload(filter) {
  const operator = String(filter.operator || "=").trim().toUpperCase();
  return {
    ...filter,
    operator,
    value: NULL_FILTER_OPERATORS.has(operator) ? null : filter.value,
  };
}

function FilterSection({ title, schema, filters, setFilters }) {
  const columns = getSchemaColumnNames(schema);
  const add = () => setFilters([...filters, { field: columns[0] || "", operator: "=", value: "" }]);
  const update = (index, key, value) => setFilters(filters.map((item, i) => {
    if (i !== index) return item;
    const next = { ...item, [key]: value };
    if (key === "operator" && NULL_FILTER_OPERATORS.has(String(value).toUpperCase())) {
      next.value = null;
    }
    return next;
  }));
  return <Panel title={title} className="reviewRunCard filterCard">
    <div className="stack">
      {filters.map((item, index) => <div className="formGrid" key={`${title}-${index}`}>
        <SelectField label="Field" value={item.field} setValue={value => update(index, "field", value)} options={["", ...columns]} />
        <SelectField label="Operator" value={item.operator} setValue={value => update(index, "operator", value)} options={FILTER_OPERATORS} />
        {!item.operator.includes("NULL") && <Field label={item.operator === "IN" ? "Values (comma separated)" : "Value"}>
          <input value={Array.isArray(item.value) ? item.value.join(", ") : item.value} onChange={e => update(index, "value", item.operator === "IN" ? e.target.value.split(",").map(v => v.trim()).filter(Boolean) : e.target.value)} />
        </Field>}
        <button type="button" className="secondary" onClick={() => setFilters(filters.filter((_, i) => i !== index))}><X size={14} /> Remove</button>
      </div>)}
      <button type="button" className="secondary" onClick={add} disabled={!columns.length}><Plus size={14} /> Add filter</button>
    </div>
  </Panel>;
}

/* ============================================================
   REVIEW
============================================================ */

function ReviewModal({
  source,
  target,
  levels,
  comparisonKeys,
  sourceFiltersCount,
  targetFiltersCount,
  ignoredColumnsCount,
  mappingsCount,
  dqRulesCount,
  aggregateRulesCount,
  onClose,
  onRun,
  running
}) {
  return (
    <div className="modalBackdrop">
      <div className="modal">
        <div className="modalHead">
          <div>
            <h3>Review & Run</h3>
            <p className="helper">Review your comparison configuration before running</p>
          </div>
          <button type="button" className="iconButton" onClick={onClose} disabled={running}>
            <X size={18} />
          </button>
        </div>

        <div className="modalBody stack">
          <div className="reviewGrid">
            <Panel title="Configuration summary">
              <ReviewRow
                label="Source"
                value={
                  source?.name ||
                  "Not selected"
                }
              />

              <ReviewRow
                label="Target"
                value={
                  target?.name ||
                  "Not selected"
                }
              />

              <ReviewRow
                label="Levels"
                value={levels.join(" · ")}
              />

              <ReviewRow
                label="Record keys"
                value={(comparisonKeys || [])
                  .filter((key) => key.source_column && key.target_column)
                  .map((key) => `${key.source_column} → ${key.target_column}`)
                  .join(", ") || "Not selected"}
              />


              <ReviewRow label="Source filters" value={String(sourceFiltersCount)} />
              <ReviewRow label="Target filters" value={String(targetFiltersCount)} />
              <ReviewRow label="Ignored columns" value={String(ignoredColumnsCount)} />
              <ReviewRow label="Column mappings" value={String(mappingsCount)} />

              <ReviewRow
                label="DQ rules"
                value={String(dqRulesCount)}
              />

              <ReviewRow
                label="Aggregate rules"
                value={String(aggregateRulesCount)}
              />
            </Panel>
          </div>
        </div>

        <div className="modalFooter">
          <button type="button" className="secondary" onClick={onClose} disabled={running}>
            Cancel
          </button>
          <button className="primary" onClick={onRun} disabled={running}>
            {running ? (
              <>
                <Loader2 size={16} className="spin" />
                Executing…
              </>
            ) : (
              <>
                <Zap size={16} />
                Run comparison
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

function ReviewRow({ label, value }) {
  return (
    <div className="reviewRow">
      <span>{label}</span>
      <b>{value}</b>
    </div>
  );
}
