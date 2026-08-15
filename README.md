# Lumera Data Comparator

Lumera is a configuration-driven source-to-target data validation platform for comparing datasets across structural, record, field, aggregate, and data-quality dimensions. It combines a guided React interface, a FastAPI orchestration layer, PostgreSQL persistence, and Apache Spark execution with an optional privacy-safe LLM analysis report.

The platform is designed for reconciliation workflows such as migration validation, ETL verification, regression testing, release certification, and ongoing data-quality monitoring.

## Contents

- [Capabilities](#capabilities)
- [Validation levels](#validation-levels)
- [Architecture](#architecture)
- [Quick start with Docker](#quick-start-with-docker)
- [Using the application](#using-the-application)
- [Comparison configuration](#comparison-configuration)
- [Local development](#local-development)
- [API overview](#api-overview)
- [Environment variables](#environment-variables)
- [Project structure](#project-structure)
- [Privacy and security](#privacy-and-security)
- [Operations and troubleshooting](#operations-and-troubleshooting)
- [Verification](#verification)

## Capabilities

- Compare CSV files and Databricks datasets through saved, reusable connections.
- Run up to six deterministic validation layers and an optional seventh analysis layer.
- Match records using source-to-target business-key mappings.
- Map differently named columns and normalize values before comparison.
- Configure absolute or percentage tolerances for numeric comparisons.
- Filter source and target populations independently before row-based validation.
- Ignore intentional source-only or target-only columns throughout applicable comparison levels.
- Reconcile grouped datasets and compare configured aggregations.
- Define reusable aggregate and data-quality rules in the rule repository.
- Choose execution strategies based on dataset size and connector capabilities.
- Track progress, cancel active runs, inspect paginated evidence, and retain run history.
- Generate a plain-language L7 report and downloadable PDF without sending raw records to the LLM.

## Validation levels

| Level | Name | What it validates |
| --- | --- | --- |
| L1 | Schema | Column presence, mapped names, data types, lengths, and nullability. |
| L2 | Volume | Row counts, null counts, and key statistics for the filtered comparison population. |
| L3 | Record Matching | Source-only, target-only, duplicate, and matched records using configured keys or record hashes. |
| L4 | Field Comparison | Field-by-field conformity across matched records, including mappings, normalization, and tolerance. |
| L5 | Aggregate | Configured aggregate functions, grouping fields, and permitted differences. |
| L6 | Data Quality | Completeness, validity, patterns, consistency, timeliness, referential integrity, distribution, conditional, and transformation rules. |
| L7 | Analysis | A privacy-safe, plain-language interpretation of the derived evidence from L1-L6. |

L1-L6 are deterministic comparison layers. L7 is optional and requires a Groq API key. The selected levels are converted into an immutable execution plan, with eligible tasks executed concurrently.

## Architecture

```mermaid
flowchart LR
    UI[React web application] -->|/api/v1| API[FastAPI service]
    API --> PLAN[Strategy planner]
    PLAN --> EXEC[Execution engine]
    EXEC --> SPARK[Apache Spark cluster]
    LOCAL --> COMP[L1-L6 comparators]
    SPARK --> COMP
    CSV[CSV files] --> EXEC
    DBX[Databricks] --> EXEC
    API <--> PG[(PostgreSQL)]
    COMP --> EVIDENCE[Metrics and sanitized evidence]
    EVIDENCE --> PG
    EVIDENCE -->|L7 only| LLM[Groq analysis]
    LLM --> REPORT[Web and PDF report]
```

The browser is served by Nginx, which proxies `/api/` requests to FastAPI. PostgreSQL stores connections, configurations, rules, execution plans, run state, results, evidence, and analysis reports. Spark provides distributed execution, while the planner can also select local, chunked, sampled, hash, aggregate, or connector-pushdown strategies where supported.

## Quick start with Docker

### Prerequisites

- Docker Desktop or Docker Engine with Docker Compose v2
- Enough local capacity for PostgreSQL, a Spark master, two 1 GB Spark workers, the API, and the frontend
- A Groq API key only if L7 analysis will be used

### 1. Configure the environment

Create a `.env` file in the repository root:

```dotenv
POSTGRES_PASSWORD=replace-with-a-strong-password

# Optional: required only for L7 analysis
GROQ_API_KEY=
GROQ_MODEL=llama-3.3-70b-versatile

# Optional Spark tuning
SPARK_SQL_SHUFFLE_PARTITIONS=8
SPARK_EVIDENCE_LIMIT=100
```

Never commit `.env` or real access tokens. Environment files are excluded by the repository's `.gitignore`.

### 2. Build and start the platform

```bash
docker compose up --build -d
docker compose ps
```

### 3. Open the services

| Service | URL |
| --- | --- |
| Lumera UI | <http://localhost:5173> |
| FastAPI documentation | <http://localhost:8000/docs> |
| Backend health check | <http://localhost:8000/health> |
| Spark master UI | <http://localhost:8080> |
| Spark worker UIs | <http://localhost:8081> and <http://localhost:8082> |
| PostgreSQL | `localhost:5432` |

The first API startup warms the Spark session and can take longer than subsequent starts.

### 4. Stop the platform

```bash
docker compose down
```

The PostgreSQL data remains in the `comparator_postgres_data` volume. Running `docker compose down -v` also removes persisted database and Spark volume data and should only be used when a full reset is intended.

## Using the application

1. Open **Connection Manager** and register a source and target connection.
2. For CSV, upload or select the files to compare. For Databricks, configure the host, HTTP path, access token, and dataset location, then test the connection.
3. Open **Comparisons** and select the source and target datasets.
4. Select the required validation levels. Enable L7 only when an LLM analysis report is needed.
5. In **Review & Run**, configure filters, ignored columns, comparison keys, grouping, aggregation fields, column mappings, tolerances, and reusable rules.
6. Start the run and monitor its progress on the results page.
7. Inspect each level's metrics and paginated evidence. If L7 ran successfully, review the analysis and download its PDF report.

Sample CSV datasets are available in [`data/`](data/).

## Comparison configuration

### Connections

The application currently registers two user-facing connector types:

- **CSV** — schema discovery, record iteration, chunked reads, and file-based execution.
- **Databricks** — connection testing, catalog/schema/table discovery, SQL data access, and supported pushdown operations.

Saved secret fields such as tokens and passwords are masked in API responses and restored internally when a saved connection is used. Keep deployment logs and the PostgreSQL instance access-controlled because connection metadata is operationally sensitive.

### Filters

Source and target filters are independent. Supported operators are:

```text
=  !=  >  >=  <  <=  IN  IS NULL  IS NOT NULL
```

All filters on one dataset are combined with `AND`. `IN` requires a non-empty list; `IS NULL` and `IS NOT NULL` do not accept a value. Filters are applied before row-based L2-L6 calculations so counts, matching, fields, aggregates, and data-quality checks operate on the intended population. L1 remains a structural schema check.

### Ignored columns

An ignored column is excluded from every applicable comparison layer. Ignoring either side of a configured column mapping excludes the complete logical pair. A comparison key, grouping field, or aggregation field cannot also be ignored; the API rejects these conflicting configurations before execution.

This is useful when one side intentionally contains operational metadata such as an ingestion timestamp, audit field, generated identifier, or version column.

### Keys and mappings

- **Comparison keys** map source business keys to target business keys for record matching.
- **Column mappings** align fields with different names.
- **Normalization** supports trimming whitespace, case-insensitive comparison, treating empty values as null, and numeric rounding.
- **Tolerance** supports absolute numeric tolerance and percentage tolerance from `0` to `100`.
- **Comparison type** may be exact, numeric/tolerance based, or regular-expression based where configured.

### Group reconciliation and rules

Grouping attributes pair source and target dimensions. Aggregation columns define the measures used for group-based reconciliation. L5 rules support aggregate functions and absolute or percentage tolerances. L6 rules can target the source, target, or both datasets and are reusable through the rule repository.

### L7 analysis and evidence

L7 receives a derived evidence payload rather than raw client data. The evidence builder removes raw records, matched pairs, record keys, and raw field values, then supplies structural findings, counts, rates, statuses, and cross-level observations to the language model. The generated report is available in the UI and as a PDF.

L7 output is an explanatory aid; deterministic L1-L6 results remain the system of record for validation status.

## Local development

Docker Compose is the recommended full-stack workflow because the backend requires PostgreSQL and initializes Spark during startup. For direct development, run the infrastructure first and then start each application separately.

### Backend

Requirements: Python 3.11 or later, PostgreSQL, Java 17, and a compatible Spark 3.5 installation when Spark execution is enabled.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
$env:DATABASE_URL = "postgresql://comparator:comparator@localhost:5432/comparator"
$env:SPARK_MASTER_URL = "spark://localhost:7077"
uvicorn app.api.main:app --reload --host 0.0.0.0 --port 8000
```

For L7, also set `GROQ_API_KEY` and optionally `GROQ_MODEL` in the process environment or a local `.env` file.

### Frontend

```bash
cd frontend
npm ci
npm run dev
```

The Vite development server runs on port `5173` and proxies `/api` to `http://127.0.0.1:8000`.

### Production frontend build

```bash
cd frontend
npm run build
```

The production container serves the generated assets with Nginx and forwards API requests to the backend service.

## API overview

All domain endpoints use the `/api/v1` prefix. The OpenAPI schema at `/docs` is the authoritative request and response reference.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Service health check. |
| `POST` | `/api/v1/connections/upload-csv` | Upload a CSV dataset. |
| `POST` | `/api/v1/connections` | Create and validate a saved connection. |
| `GET` | `/api/v1/connections` | List saved connections. |
| `GET` | `/api/v1/connections/{connection_id}` | Retrieve a saved connection. |
| `POST` | `/api/v1/connections/{connection_id}/test` | Test a saved connection. |
| `DELETE` | `/api/v1/connections/{connection_id}` | Delete a connection. |
| `POST` | `/api/v1/connections/schema` | Discover dataset columns and types. |
| `POST` | `/api/v1/connections/discover/catalogs` | List Databricks catalogs. |
| `POST` | `/api/v1/connections/discover/schemas` | List Databricks schemas. |
| `POST` | `/api/v1/connections/discover/tables` | List Databricks tables. |
| `POST` | `/api/v1/configurations` | Persist a named comparison configuration. |
| `POST` | `/api/v1/comparisons` | Validate a request, create a plan, and start a run. |
| `GET` | `/api/v1/comparisons` | List comparison runs. |
| `GET` | `/api/v1/comparisons/{run_id}` | Read run status and progress. |
| `POST` | `/api/v1/comparisons/{run_id}/cancel` | Request cancellation of an active run. |
| `GET` | `/api/v1/comparisons/{run_id}/results` | Read consolidated comparison results. |
| `GET` | `/api/v1/comparisons/{run_id}/evidence/{level}` | Read paginated evidence for L1-L6. |
| `GET` | `/api/v1/comparisons/{run_id}/analysis/pdf` | Download the L7 PDF report. |
| `DELETE` | `/api/v1/comparisons/{run_id}` | Delete a persisted run. |
| `POST/GET` | `/api/v1/rules` | Create or list reusable rules. |
| `PUT/DELETE` | `/api/v1/rules/{rule_id}` | Update or delete a reusable rule. |

### Minimal comparison request shape

```json
{
  "configuration_id": 1,
  "source": {
    "connector_type": "csv",
    "properties": {"path": "/app/data/source_t1.csv"}
  },
  "target": {
    "connector_type": "csv",
    "properties": {"path": "/app/data/target_t1.csv"}
  },
  "comparison_levels": ["L1", "L2", "L3", "L4"],
  "l7_enabled": false,
  "comparison_keys": [
    {"source_column": "EmployeeID", "target_column": "EmployeeID"}
  ],
  "column_mappings": [],
  "ignored_columns": [],
  "aggregate_rules": [],
  "dq_rules": [],
  "source_filters": [],
  "target_filters": [],
  "matching_mode": "ROW_LEVEL",
  "grouping_attributes": [],
  "aggregation_columns": [],
  "strategy_policy": {
    "max_exact_rows": 100000,
    "max_exact_bytes": 104857600,
    "sampling_min_rows": 1000000,
    "allow_sampling": false,
    "prefer_pushdown": true
  }
}
```

The API returns a `run_id`, `plan_id`, task count, and task IDs. Poll the status endpoint until the run reaches a terminal state, then retrieve results and level evidence.

## Environment variables

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `DATABASE_URL` | Yes | Set by Compose | SQLAlchemy/PostgreSQL connection URL. |
| `POSTGRES_PASSWORD` | Recommended | `comparator` in Compose | Password used by the bundled PostgreSQL service. |
| `GROQ_API_KEY` | For L7 | None | API credential for language-model analysis. |
| `GROQ_MODEL` | No | `llama-3.3-70b-versatile` | Groq model used by L7. |
| `SPARK_MASTER_URL` | For clustered Spark | Set by Compose | Spark master URL; omitted execution may use the local Spark default. |
| `SPARK_DRIVER_HOST` | In Compose | `backend` | Host advertised by the Spark driver. |
| `SPARK_APP_NAME` | No | `V1-Comparator` | Spark application name. |
| `SPARK_SQL_SHUFFLE_PARTITIONS` | No | `16` in code, `8` in Compose | Default Spark SQL shuffle partition count. |
| `SPARK_EVIDENCE_LIMIT` | No | `100` | Maximum evidence items collected by Spark operations. |
| `SPARK_LOG_LEVEL` | No | `WARN` | Spark context log level. |
| `SPARK_LOCAL_DIRS` | No | `/tmp/spark-local` in Compose | Spark temporary storage directory. |
| `SPARK_JARS` | No | Empty | Optional JDBC JAR path registered with Spark. |
| `SPARK_TINY_FILE_BYTES` | No | `4194304` | Threshold for tiny-file partition tuning. |
| `SPARK_TINY_SHUFFLE_PARTITIONS` | No | `1` | Shuffle partitions for tiny files. |
| `SPARK_SMALL_FILE_BYTES` | No | `134217728` | Threshold for small-file partition tuning. |
| `SPARK_SMALL_SHUFFLE_PARTITIONS` | No | `4` | Shuffle partitions for small files. |
| `SPARK_DATABRICKS_CHUNK_SIZE` | No | `1000` | Row batch size used when Spark loads Databricks data. |

## Project structure

```text
.
|-- app/
|   |-- analysis/          # L7 evidence shaping, prompts, models, and Groq client
|   |-- api/               # FastAPI application, routes, and schemas
|   |-- comparators/       # L1-L6 deterministic comparison implementations
|   |-- connectors/        # CSV, Databricks, metadata, filters, and provider registry
|   |-- domain/            # Validated runtime configuration models
|   |-- execution/         # Planning dispatch, workers, Spark/local execution, retries
|   |-- persistence/       # PostgreSQL models and repository
|   `-- strategy/          # Dataset analysis and execution-strategy selection
|-- data/                  # Sample and container-accessible datasets
|-- frontend/
|   |-- src/               # React application, styles, and Lumera assets
|   |-- Dockerfile         # Node build and Nginx runtime
|   `-- nginx.conf         # SPA routing and backend proxy
|-- Dockerfile             # Backend image
|-- docker-compose.yml     # Full local platform
`-- requirements.txt       # Python dependencies
```

## Privacy and security

- Raw records are required by deterministic comparators but are not included in the L7 LLM payload.
- The L7 evidence builder uses derived structural and statistical evidence and excludes raw values, record keys, and matched record pairs.
- Connection API responses mask recognized password, token, secret, credential, and API-key fields.
- Store production secrets in a secrets manager or orchestrator-managed environment variables, not source control.
- Restrict PostgreSQL, Spark UIs, and FastAPI documentation at the network boundary in production; the Compose ports are intended for local development.
- Use least-privilege, read-only credentials for source and target systems whenever possible.
- Review your organization's data-handling requirements before enabling any external LLM integration.

## Operations and troubleshooting

### Inspect service state and logs

```bash
docker compose ps
docker compose logs -f backend
docker compose logs -f spark-master spark-worker-1 spark-worker-2
```

### Backend does not start

- Confirm PostgreSQL is healthy with `docker compose ps`.
- Verify `DATABASE_URL` points to an accessible database.
- Check that port `8000` is free.
- During direct development, ensure Java and Spark are installed and `SPARK_MASTER_URL` is reachable.

### A comparison remains pending or fails during execution

- Check the backend and Spark worker logs for the run and task IDs.
- Open the Spark master UI and confirm both workers are alive.
- Confirm that container dataset paths are under `/app/data`; host-only paths are not automatically visible inside containers.
- For Databricks, retest the saved connection and confirm catalog, schema, table, warehouse HTTP path, and token permissions.

### L7 fails while L1-L6 succeed

- Confirm `GROQ_API_KEY` is set in the backend container.
- Verify the configured `GROQ_MODEL` is available to the account.
- L7 failure does not alter deterministic findings already produced by L1-L6.

### Results are unexpectedly different

- Confirm source and target filters describe equivalent populations.
- Verify key and column mappings in both directions.
- Check whether operational columns should be ignored.
- Review normalization and tolerance settings before treating formatting or rounding differences as data defects.
- Inspect the evidence endpoint for the specific validation level instead of relying only on the overall status.

## Verification

The repository currently does not declare an automated test suite. Use these checks after making changes:

```bash
python -m compileall -q app
cd frontend
npm ci
npm run build
```

For an integration smoke test, start Docker Compose, confirm `/health` returns a healthy response, create CSV connections using the sample files, run L1-L4, and inspect the results and evidence pages.

## License

No license file is currently included. Unless the project owner adds one, no rights should be assumed beyond the permissions explicitly granted by the owner.
