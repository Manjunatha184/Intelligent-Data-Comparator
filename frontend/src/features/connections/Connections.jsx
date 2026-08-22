import React, { useEffect, useState } from "react";
import { Check, Database, FileText, Link2, Loader2, Plus, RefreshCw, ShieldCheck, Trash2, X } from "lucide-react";

import { apiRequest } from "../../api/client";
import { Empty, Field, Loading, Panel, Status } from "../../components/ui";
import { CONNECTORS } from "../../constants/app";

/* ============================================================
   CONNECTION MANAGER
============================================================ */

export function ConnectionManager({
  connections,
  loading,
  reload,
  onAdd,
  notify,
}) {
  const [testingId, setTestingId] = useState(null);

  async function testConnection(connectionId) {
    setTestingId(connectionId);

    try {
      const result = await apiRequest(
        `/connections/${connectionId}/test`,
        {
          method: "POST",
        }
      );

      notify(
        result.message || "Connection test successful."
      );

      await reload();
    } catch (error) {
      notify(error.message, "error");

      await reload();
    } finally {
      setTestingId(null);
    }
  }

  async function deleteConnection(connectionId) {
    const confirmed = window.confirm(
      "Delete this connection?"
    );

    if (!confirmed) {
      return;
    }

    try {
      await apiRequest(
        `/connections/${connectionId}`,
        {
          method: "DELETE",
        }
      );

      notify("Connection deleted successfully.");

      await reload();
    } catch (error) {
      notify(error.message, "error");
    }
  }

  return (
    <div className="stack connectionManagerPage">
      <div className="wizardFooter">
        <h1 className="pageTitle" style={{ margin: 0 }}>Connection Manager</h1>

        <div className="actionRow">
          <button
            className="secondary"
            onClick={reload}
          >
            <RefreshCw size={15} />
            Refresh
          </button>

          <button
            className="primary"
            onClick={onAdd}
          >
            <Plus size={16} />
            Add connection
          </button>
        </div>
      </div>

      <div className="connectorCards">
        {Object.entries(CONNECTORS).map(
          ([connectorType, connector]) => {
            const Icon = connector.icon;

            const count = connections.filter(
              (connection) =>
                connection.connector_type === connectorType
            ).length;

            return (
              <div
                className="connectorCard"
                key={connectorType}
              >
                <div className="sourceIcon large">
                  <Icon size={19} />
                </div>

                <div className="grow">
                  <b>{connector.label}</b>
                  <span>{connector.description}</span>
                </div>

                <strong>{count}</strong>
              </div>
            );
          }
        )}
      </div>

      <Panel
        title="Configured connections"
        className="connectionListPanel"
        action={
          <span className="countPill">
            {connections.length} total
          </span>
        }
      >
        {loading ? (
          <Loading text="Loading connections…" />
        ) : connections.length === 0 ? (
          <Empty
            icon={Link2}
            title="No connections configured"
            text="Add CSV or Databricks connections to make them available in the comparison builder."
          />
        ) : (
          <div className="connectionTable">
            <div className="thead">
              <span>Connection</span>
              <span>Type</span>
              <span>Status</span>
              <span>ID</span>
              <span />
            </div>

            <div className="connectionRowsScroll">
              {connections.map((connection) => {
                const ConnectorIcon =
                  CONNECTORS[
                    connection.connector_type
                  ]?.icon || Database;

                return (
                  <div
                    className="trow"
                    key={connection.connection_id}
                  >
                    <div className="nameCell">
                      <div className="sourceIcon">
                        <ConnectorIcon size={15} />
                      </div>

                      <div>
                        <b>{connection.name}</b>
                        <span>
                          {connection.connector_type}
                        </span>
                      </div>
                    </div>

                    <div>
                      <span className="typeTag">
                        {
                          CONNECTORS[
                            connection.connector_type
                          ]?.label
                        }
                      </span>
                    </div>

                    <div>
                      <Status
                        status={connection.status}
                      />
                    </div>

                    <code>
                      #{connection.connection_id}
                    </code>

                    <div className="rowButtons">
                      <button
                        onClick={() =>
                          testConnection(
                            connection.connection_id
                          )
                        }
                        disabled={
                          testingId ===
                          connection.connection_id
                        }
                      >
                        {testingId ===
                          connection.connection_id ? (
                          <Loader2
                            className="spin"
                            size={14}
                          />
                        ) : (
                          <RefreshCw size={14} />
                        )}

                        Test
                      </button>

                      <button
                        className="danger"
                        onClick={() =>
                          deleteConnection(
                            connection.connection_id
                          )
                        }
                      >
                        <Trash2 size={14} />
                        Delete
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </Panel>
    </div>
  );
}

/* ============================================================
   CONNECTION MODAL
============================================================ */

export function ConnectionModal({
  onClose,
  onDone,
  notify,
}) {
  const [connectorType, setConnectorType] =
    useState("csv");

  const [name, setName] = useState("");

  const [values, setValues] = useState({});

  const [saving, setSaving] = useState(false);
  const [csvFile, setCsvFile] = useState(null);
  const [csvUpload, setCsvUpload] = useState(null);
  const [uploadingCsv, setUploadingCsv] = useState(false);
  const connector = CONNECTORS[connectorType];

  function updateValue(key, value) {
    setValues((current) => ({
      ...current,
      [key]: value,
    }));
  }

  useEffect(() => {
    setValues({});
    setCsvFile(null);
    setCsvUpload(null);
  }, [connectorType]);

  async function handleCsvFile(event) {
    const file = event.target.files?.[0];

    if (!file) return;

    if (!file.name.toLowerCase().endsWith(".csv")) {
      notify("Please select a CSV file.", "error");
      event.target.value = "";
      return;
    }

    setCsvFile(file);
    setCsvUpload(null);
    setUploadingCsv(true);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const response = await fetch(
        "/api/v1/connections/upload-csv",
        {
          method: "POST",
          body: formData,
        }
      );

      if (!response.ok) {
        let message = "CSV upload failed.";

        try {
          const errorData = await response.json();
          message = errorData.detail || message;
        } catch {
          // keep default message
        }

        throw new Error(message);
      }

      const uploaded = await response.json();

      setCsvUpload(uploaded);

      notify(
        `${uploaded.filename} uploaded successfully.`
      );
    } catch (error) {
      setCsvFile(null);
      setCsvUpload(null);
      notify(error.message, "error");
    } finally {
      setUploadingCsv(false);
    }
  }

  async function submit(event) {
    event.preventDefault();

    if (!name.trim()) {
      notify(
        "Connection name is required.",
        "error"
      );

      return;
    }
    if (
      connectorType === "csv" &&
      !csvUpload?.path
    ) {
      notify(
        "Choose and upload a CSV file first.",
        "error"
      );

      return;
    }

    const missingField =
      connector.fields.find(
        (field) =>
          field.required &&
          !values[field.key]?.trim()
      );

    if (missingField) {
      notify(
        `Enter ${missingField.label}.`,
        "error"
      );

      return;
    }

    setSaving(true);

    try {
      await apiRequest("/connections", {
        method: "POST",
        body: JSON.stringify({
          name: name.trim(),
          connector_type: connectorType,
          properties:
            connectorType === "csv"
              ? {
                path: csvUpload.path,
                delimiter: ",",
                encoding: "utf-8",
                filename: csvUpload.filename,
                upload_id: csvUpload.upload_id,
              }
              : values,
        }),
      });

      notify(
        `${connector.label} connected successfully.`
      );

      onDone();
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setSaving(false);
    }
  }

  const ConnectorIcon = connector.icon;

  return (
    <div className="modalBackdrop">
      <div className="modal wide">
        <div className="modalHead">
          <div>
            <span className="sectionEyebrow">
              NEW CONNECTION
            </span>

            <h3>Connect a data source</h3>

            <p>
              The backend tests the connection before
              storing it.
            </p>
          </div>

          <button
            type="button"
            className="iconButton"
            onClick={onClose}
          >
            <X size={18} />
          </button>
        </div>

        <div className="connectorPicker">
          {Object.entries(CONNECTORS).map(
            ([key, item]) => {
              const Icon = item.icon;

              return (
                <button
                  type="button"
                  key={key}
                  className={
                    connectorType === key
                      ? "selected"
                      : ""
                  }
                  onClick={() =>
                    setConnectorType(key)
                  }
                >
                  <Icon size={17} />

                  <div>
                    <b>{item.label}</b>
                    <span>
                      {key === "csv" ? "File" : "SQL"}
                    </span>
                  </div>

                  {connectorType === key && (
                    <Check size={15} />
                  )}
                </button>
              );
            }
          )}
        </div>

        <form onSubmit={submit}>
          <div className="formGrid">
            <Field
              label="Connection name"
              required
            >
              <input
                type="text"
                value={name}
                placeholder="Finance source"
                onChange={(event) =>
                  setName(event.target.value)
                }
              />
            </Field>

            {connectorType === "csv" && (
              <Field
                label="CSV file"
                required
              >
                <div>
                  <label
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "10px",
                      minHeight: "44px",
                      padding: "7px 10px",
                      border: "1px solid var(--border)",
                      borderRadius: "10px",
                      background: "var(--surface)",
                      cursor: uploadingCsv || saving ? "not-allowed" : "pointer",
                      opacity: uploadingCsv || saving ? 0.65 : 1,
                    }}
                  >
                    <input
                      type="file"
                      accept=".csv,text/csv"
                      onChange={handleCsvFile}
                      disabled={uploadingCsv || saving}
                      style={{
                        position: "absolute",
                        width: 1,
                        height: 1,
                        opacity: 0,
                        pointerEvents: "none",
                      }}
                    />

                    <span
                      className="secondary"
                      style={{
                        display: "inline-flex",
                        alignItems: "center",
                        gap: "7px",
                        minHeight: "30px",
                        padding: "5px 10px",
                        borderRadius: "7px",
                        whiteSpace: "nowrap",
                      }}
                    >
                      <FileText size={15} />
                      {csvUpload ? "Change file" : "Choose CSV"}
                    </span>

                    <span
                      style={{
                        minWidth: 0,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                        color: csvUpload ? "var(--text)" : "var(--muted)",
                        fontSize: "13px",
                      }}
                    >
                      {uploadingCsv
                        ? "Uploading CSV..."
                        : csvUpload?.filename || "No file selected"}
                    </span>
                  </label>
                </div>
              </Field>
            )}

            {connector.fields.map((field) => (
              <Field
                key={field.key}
                label={field.label}
                required={field.required}
              >
                <input
                  type={field.type}
                  value={values[field.key] || ""}
                  placeholder={field.placeholder}
                  onChange={(event) =>
                    updateValue(
                      field.key,
                      event.target.value
                    )
                  }
                  autoComplete="off"
                />
              </Field>
            ))}

          </div>

          {connectorType === "databricks" && (
            <div className="infoBox">
              <ShieldCheck size={17} />

              <div>
                <b>Credential protection</b>

                <span>
                  Databricks access tokens are used for
                  connection testing and are masked by
                  the backend when returned.
                </span>
              </div>
            </div>
          )}

          <div className="modalFooter">
            <button
              type="button"
              className="secondary"
              onClick={onClose}
            >
              Cancel
            </button>

            <button
              type="submit"
              className="primary"
              disabled={
                saving ||
                uploadingCsv ||
                (
                  connectorType === "csv" &&
                  !csvUpload?.path
                )
              }
            >
              {saving ? (
                <>
                  <Loader2
                    className="spin"
                    size={15}
                  />
                  Testing…
                </>
              ) : (
                <>
                  <ShieldCheck size={15} />
                  Test & save connection
                </>
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
