import React, { useEffect, useState } from "react";
import { Check, FileText, Loader2, ShieldCheck, X } from "lucide-react";
import { apiRequest } from "../../api/client.js";
import { CONNECTORS } from "../../config/comparison.js";
import Field from "../ui/Field.jsx";

export default function ConnectionModal({ onClose, onDone, notify }) {
  const [connectorType, setConnectorType] = useState("csv");
  const [name, setName] = useState("");
  const [values, setValues] = useState({});
  const [saving, setSaving] = useState(false);
  const [csvFile, setCsvFile] = useState(null);
  const [csvUpload, setCsvUpload] = useState(null);
  const [uploadingCsv, setUploadingCsv] = useState(false);
  const connector = CONNECTORS[connectorType];

  function updateValue(key, value) {
    setValues((current) => ({ ...current, [key]: value }));
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
      const response = await fetch("/api/v1/connections/upload-csv", { method: "POST", body: formData });
      if (!response.ok) {
        let message = "CSV upload failed.";
        try {
          const errorData = await response.json();
          message = errorData.detail || message;
        } catch {}
        throw new Error(message);
      }
      const uploaded = await response.json();
      setCsvUpload(uploaded);
      notify(`${uploaded.filename} uploaded successfully.`);
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
    if (!name.trim()) return notify("Connection name is required.", "error");
    if (connectorType === "csv" && !csvUpload?.path) return notify("Choose and upload a CSV file first.", "error");

    const missingField = connector.fields.find((field) => field.required && !values[field.key]?.trim());
    if (missingField) return notify(`Enter ${missingField.label}.`, "error");

    setSaving(true);
    try {
      await apiRequest("/connections", {
        method: "POST",
        body: JSON.stringify({
          name: name.trim(),
          connector_type: connectorType,
          properties: connectorType === "csv"
            ? { path: csvUpload.path, delimiter: ",", encoding: "utf-8", filename: csvUpload.filename, upload_id: csvUpload.upload_id }
            : values,
        }),
      });
      notify(`${connector.label} connected successfully.`);
      onDone();
    } catch (error) {
      notify(error.message, "error");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="modalBackdrop">
      <div className="modal wide">
        <div className="modalHead">
          <div>
            <span className="sectionEyebrow">NEW CONNECTION</span>
            <h3>Connect a data source</h3>
            <p>The backend tests the connection before storing it.</p>
          </div>
          <button type="button" className="iconButton" onClick={onClose}><X size={18} /></button>
        </div>

        <div className="connectorPicker">
          {Object.entries(CONNECTORS).map(([key, item]) => {
            const Icon = item.icon;
            return (
              <button type="button" key={key} className={connectorType === key ? "selected" : ""} onClick={() => setConnectorType(key)}>
                <Icon size={17} />
                <div><b>{item.label}</b><span>{key === "csv" ? "File" : "SQL"}</span></div>
                {connectorType === key && <Check size={15} />}
              </button>
            );
          })}
        </div>

        <form onSubmit={submit}>
          <div className="formGrid">
            <Field label="Connection name" required>
              <input type="text" value={name} placeholder="Finance source" onChange={(event) => setName(event.target.value)} />
            </Field>

            {connectorType === "csv" && (
              <Field label="CSV file" required>
                <div>
                  <label className="filePickerControl">
                    <input type="file" accept=".csv,text/csv" onChange={handleCsvFile} disabled={uploadingCsv || saving} />
                    <span className="secondary"><FileText size={15} />{csvUpload ? "Change file" : "Choose CSV"}</span>
                    <span>{uploadingCsv ? "Uploading CSV..." : csvUpload?.filename || csvFile?.name || "No file selected"}</span>
                  </label>
                </div>
              </Field>
            )}

            {connector.fields.map((field) => (
              <Field key={field.key} label={field.label} required={field.required}>
                <input type={field.type} value={values[field.key] || ""} placeholder={field.placeholder} onChange={(event) => updateValue(field.key, event.target.value)} autoComplete="off" />
              </Field>
            ))}
          </div>

          {connectorType === "databricks" && (
            <div className="infoBox">
              <ShieldCheck size={17} />
              <div><b>Credential protection</b><span>Databricks access tokens are used for connection testing and are masked by the backend when returned.</span></div>
            </div>
          )}

          <div className="modalFooter">
            <button type="button" className="secondary" onClick={onClose}>Cancel</button>
            <button type="submit" className="primary" disabled={saving || uploadingCsv || (connectorType === "csv" && !csvUpload?.path)}>
              {saving ? <><Loader2 className="spin" size={15} />Testing…</> : <><ShieldCheck size={15} />Test & save connection</>}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
