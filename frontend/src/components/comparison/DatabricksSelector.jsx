import React, { useEffect, useState } from "react";
import { apiRequest } from "../../api/client.js";
import SelectField from "../ui/SelectField.jsx";

export default function DatabricksSelector({ connection, catalog, setCatalog, schema, setSchema, table, setTable, notify }) {
  const [catalogs, setCatalogs] = useState([]);
  const [schemas, setSchemas] = useState([]);
  const [tables, setTables] = useState([]);
  const [loadingCatalog, setLoadingCatalog] = useState(false);
  const [loadingSchema, setLoadingSchema] = useState(false);
  const [loadingTable, setLoadingTable] = useState(false);

  useEffect(() => {
    if (!connection) return;
    setLoadingCatalog(true);
    apiRequest("/connections/discover/catalogs", { method: "POST", body: JSON.stringify({ connector_type: connection.connector_type, properties: { ...connection.properties, connection_id: connection.connection_id } }) })
      .then((response) => setCatalogs(response || []))
      .catch((error) => notify("Failed to load catalogs: " + error.message, "error"))
      .finally(() => setLoadingCatalog(false));
  }, [connection]);

  useEffect(() => {
    if (!connection || !catalog) { setSchemas([]); return; }
    setLoadingSchema(true);
    apiRequest("/connections/discover/schemas", { method: "POST", body: JSON.stringify({ connector_type: connection.connector_type, properties: { ...connection.properties, connection_id: connection.connection_id }, catalog }) })
      .then((response) => setSchemas(response || []))
      .catch((error) => notify("Failed to load schemas: " + error.message, "error"))
      .finally(() => setLoadingSchema(false));
  }, [connection, catalog]);

  useEffect(() => {
    if (!connection || !catalog || !schema) { setTables([]); return; }
    setLoadingTable(true);
    apiRequest("/connections/discover/tables", { method: "POST", body: JSON.stringify({ connector_type: connection.connector_type, properties: { ...connection.properties, connection_id: connection.connection_id }, catalog, schema_name: schema }) })
      .then((response) => setTables(response || []))
      .catch((error) => notify("Failed to load tables: " + error.message, "error"))
      .finally(() => setLoadingTable(false));
  }, [connection, catalog, schema]);

  return (
    <div className="databricksSelector">
      <SelectField label="Catalog" required value={catalog} setValue={(value) => { setCatalog(value); setSchema(""); setTable(""); }} options={catalogs.map((item) => [item, item])} placeholder={loadingCatalog ? "Loading catalogs..." : "Select catalog…"} />
      <SelectField label="Schema" required value={schema} setValue={(value) => { setSchema(value); setTable(""); }} options={schemas.map((item) => [item, item])} placeholder={loadingSchema ? "Loading schemas..." : "Select schema…"} disabled={!catalog} />
      <SelectField label="Table" required value={table} setValue={setTable} options={tables.map((item) => [item, item])} placeholder={loadingTable ? "Loading tables..." : "Select table…"} disabled={!schema} />
    </div>
  );
}
