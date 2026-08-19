import React from "react";
import { CONNECTORS } from "../../config/comparison.js";
import SelectField from "../ui/SelectField.jsx";

export default function ConnectionSelector({ label, value, setValue, connections, loading = false, error = null, onRetry }) {
  const options = (connections || []).map((connection) => {
    const properties = connection.properties || {};
    const filename = properties.filename || properties.path?.split("/").pop();
    const dataset = connection.connector_type === "csv" ? filename : properties.table;
    const name = dataset ? `${connection.name} (${dataset})` : connection.name;
    const connector = CONNECTORS[connection.connector_type]?.label || connection.connector_type;
    return [String(connection.connection_id), `${name} · ${connector}`];
  });

  return (
    <div className="connectionSelector">
      {loading && <div className="connectionSelectorState">Loading connections...</div>}
      {!loading && error && (
        <div className="connectionSelectorState connectionSelectorError">
          <span>Unable to load connections</span>
          <button type="button" className="textBtn" onClick={onRetry}>Retry</button>
        </div>
      )}
      {!loading && !error && !options.length && <div className="connectionSelectorState">No connections available</div>}
      {!loading && !error && options.length > 0 && (
        <SelectField
          label={label}
          required
          value={value}
          setValue={setValue}
          options={options}
          placeholder="Select an authenticated connection..."
        />
      )}
    </div>
  );
}
