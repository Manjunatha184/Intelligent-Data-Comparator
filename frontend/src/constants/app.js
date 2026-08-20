import {
  Activity,
  Database,
  Eye,
  FileText,
  GitCompare,
  ShieldCheck,
  SlidersHorizontal,
  Zap,
} from "lucide-react";

export const COMPARISON_LEVELS = [
  { id: "L1", name: "Schema", description: "Columns, data types, lengths and nullability" },
  { id: "L2", name: "Volume", description: "Row counts, nulls and key statistics" },
  { id: "L3", name: "Record", description: "Record and key-level comparison" },
  { id: "L4", name: "Field", description: "Field-by-field value comparison" },
  { id: "L5", name: "Aggregate", description: "Configured aggregate checks" },
  { id: "L6", name: "Data Quality", description: "Configured data-quality rules" },
  { id: "L7", name: "Analysis", description: "Plain-language analysis of findings and cross-level evidence" },
];

export const COMPARISON_LEVEL_ICONS = {
  L1: Database,
  L2: Activity,
  L3: GitCompare,
  L4: Eye,
  L5: SlidersHorizontal,
  L6: ShieldCheck,
  L7: Zap,
};

export const CONNECTORS = {
  csv: {
    label: "CSV / File",
    icon: FileText,
    description: "Upload a CSV file from your computer",
    fields: [],
  },
  databricks: {
    label: "Databricks SQL",
    icon: Database,
    description: "Databricks SQL warehouse datasets",
    fields: [
      {
        key: "server_hostname",
        label: "Server hostname",
        type: "text",
        placeholder: "dbc-xxxx.cloud.databricks.com",
        required: true,
      },
      {
        key: "http_path",
        label: "HTTP path",
        type: "text",
        placeholder: "/sql/1.0/warehouses/xxxx",
        required: true,
      },
      {
        key: "access_token",
        label: "Access token",
        type: "password",
        placeholder: "Enter access token",
        required: true,
      },
    ],
  },
};
