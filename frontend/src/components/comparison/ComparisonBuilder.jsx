import React, { useEffect, useState } from "react";
import { ArrowRight } from "lucide-react";
import { apiRequest } from "../../api/client.js";
import { defaultMappedPair, getSchemaColumnNames, isNumericMapping, rowsEqual } from "../../utils/schema.js";
import { normalizeAggregateRulePayload, normalizeDqRulePayload } from "../../utils/rules.js";
import SourceTargetStep from "./SourceTargetStep.jsx";
import LevelSelectionStep from "./LevelSelectionStep.jsx";
import RulesConfigurationStep from "./RulesConfigurationStep.jsx";
import ReviewModal from "./ReviewModal.jsx";

export default function ComparisonBuilder({
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
          <SourceTargetStep
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
          <LevelSelectionStep
            levels={levels}
            toggleLevel={toggleLevel}
          />
        </div>
      )}

      {step === 2 && (
        <RulesConfigurationStep
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


const NULL_FILTER_OPERATORS = new Set(["IS NULL", "IS NOT NULL"]);

function normalizeRowFilterPayload(filter) {
  const operator = String(filter.operator || "=").trim().toUpperCase();
  return {
    ...filter,
    operator,
    value: NULL_FILTER_OPERATORS.has(operator) ? null : filter.value,
  };
}

