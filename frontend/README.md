# V1 Comparator Frontend

React + JavaScript + Vite frontend for the existing FastAPI V1 Comparator backend.

## Run

```bash
npm install
npm run dev
```

Frontend: http://localhost:5173
Backend expected: http://127.0.0.1:8000

The Vite proxy forwards `/api/*` to the FastAPI backend.

## Implemented

- Enterprise-style clean UI
- Connection Manager for CSV and Databricks
- Connection testing and deletion
- Human-friendly four-step comparison builder
- Existing authenticated connections are selected instead of re-entering JSON
- Comparison request maps to the current `ComparisonRequest` backend schema
- Results page for L1-L6
- Rule repository workspace
- No TypeScript

## Important

The comparison builder fetches `/api/v1/connections/{id}` after a connection is selected so CSV properties such as `path` are included in the comparison request. Secret fields are masked by the existing backend, so Databricks credentials are resolved server-side by connection reference during execution.
