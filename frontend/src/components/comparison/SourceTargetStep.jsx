import React from "react";
import Panel from "../ui/Panel.jsx";
import ConnectionSelector from "./ConnectionSelector.jsx";
import DatabricksSelector from "./DatabricksSelector.jsx";

export default function SourceTargetStep({ connections, source, target, sourceId, targetId, setSourceId, setTargetId, sourceDbCatalog, setSourceDbCatalog, sourceDbSchema, setSourceDbSchema, sourceDbTable, setSourceDbTable, targetDbCatalog, setTargetDbCatalog, targetDbSchema, setTargetDbSchema, targetDbTable, setTargetDbTable, notify, connectionsLoading, connectionsError, reloadConnections }) {
  return (
    <section className="scopeSourceSection">
      <div className="scopeSectionIntro"><div><h2>Choose what you want to compare</h2></div></div>
      <div className="grid2 scopeSourceGrid">
        <Panel title="Source dataset" className="scopeDatasetCard">
          <ConnectionSelector label="Source connection" value={sourceId} setValue={setSourceId} connections={connections} loading={connectionsLoading} error={connectionsError} onRetry={reloadConnections} />
          {source?.connector_type === "databricks" && <DatabricksSelector connection={source} catalog={sourceDbCatalog} setCatalog={setSourceDbCatalog} schema={sourceDbSchema} setSchema={setSourceDbSchema} table={sourceDbTable} setTable={setSourceDbTable} notify={notify} />}
        </Panel>
        <Panel title="Target dataset" className="scopeDatasetCard">
          <ConnectionSelector label="Target connection" value={targetId} setValue={setTargetId} connections={connections} loading={connectionsLoading} error={connectionsError} onRetry={reloadConnections} />
          {target?.connector_type === "databricks" && <DatabricksSelector connection={target} catalog={targetDbCatalog} setCatalog={setTargetDbCatalog} schema={targetDbSchema} setSchema={setTargetDbSchema} table={targetDbTable} setTable={setTargetDbTable} notify={notify} />}
        </Panel>
      </div>
    </section>
  );
}
