# irp-integration

A Python client library for the [Moody's Intelligent Risk Platform (IRP) APIs](https://developer.rms.com/). Built to serve as a foundation for larger Moody's integration projects — use it with Jupyter Notebooks, Azure Functions, or any orchestration layer to build end-to-end risk analysis workflows.

Not all Moody's API functionality is covered yet, but the most common operations are available and the library is actively maintained. Contributions are welcome — feel free to fork and modify to fit your project's needs.

## Installation

```bash
pip install irp-integration
```

To include Data Bridge (SQL Server) support:

```bash
pip install irp-integration[databridge]
```

> **Note:** Data Bridge requires [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) to be installed on your system.

## Quick Start

```python
from irp_integration import IRPClient

# Requires environment variables (see Configuration below)
client = IRPClient()

# Search EDMs
edms = client.edm.search_edms(filter = f'exposureName = "my_edm"')

# Get portfolios for an EDM
edm = edms[0]
exposure_id = edm['exposureId']
portfolios = client.portfolio.search_portfolios(exposure_id = exposure_id)

# Run analysis on a portfolio
edm_name = edm['exposureName']
portfolio = portfolios[0]
portfolio_name = portfolio['portfolioName']
client.analysis.submit_portfolio_analysis_job(
    edm_name=edm_name,
    portfolio_name=portfolio_name,
    job_name="Readme Analysis",
    model_profile_id=4418,
    output_profile_id=123,
    event_rate_scheme_id=739,
    treaty_names=['Working Excess Treaty 1'],
    tag_names=['Tag1', 'Tag2']
)
```

## Configuration

The library reads configuration from environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `RISK_MODELER_BASE_URL` | Yes | Moody's Risk Modeler API base URL |
| `RISK_MODELER_API_KEY` | Yes | API authentication key |
| `RISK_MODELER_RESOURCE_GROUP_ID` | Yes | Resource group ID for your organization |

You can set these in your shell, or use a `.env` file with [python-dotenv](https://pypi.org/project/python-dotenv/):

```python
from dotenv import load_dotenv
load_dotenv()

from irp_integration import IRPClient
client = IRPClient()
```

### Data Bridge Configuration

The Data Bridge module (`client.databridge`) connects directly to Moody's SQL Server databases via ODBC. It requires separate setup from the REST API.

**Prerequisites:**

1. Install the optional dependency: `pip install irp-integration[databridge]`
2. Install [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server):
   - **Windows:** Download and run the MSI installer from Microsoft
   - **Linux (Debian/Ubuntu):** `sudo apt-get install -y unixodbc-dev && sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18`
   - **macOS:** `brew install microsoft/mssql-release/msodbcsql18`

**Environment variables (per connection):**

Each named connection uses the prefix `MSSQL_{CONNECTION_NAME}_`:

| Variable | Required | Description |
|----------|----------|-------------|
| `MSSQL_{NAME}_SERVER` | Yes | Server hostname or IP |
| `MSSQL_{NAME}_USER` | Yes | SQL Server username |
| `MSSQL_{NAME}_PASSWORD` | Yes | SQL Server password |
| `MSSQL_{NAME}_PORT` | No | Port (default: 1433) |

**Global settings:**

| Variable | Default | Description |
|----------|---------|-------------|
| `MSSQL_DRIVER` | `ODBC Driver 18 for SQL Server` | ODBC driver name |
| `MSSQL_TRUST_CERT` | `yes` | Trust server certificate |
| `MSSQL_TIMEOUT` | `30` | Connection timeout in seconds |

**Example:**

```bash
# .env file
MSSQL_DATABRIDGE_SERVER=databridge.company.com
MSSQL_DATABRIDGE_USER=svc_account
MSSQL_DATABRIDGE_PASSWORD=secretpassword
```

```python
from irp_integration.databridge import DataBridgeManager

db = DataBridgeManager(default_connection='DATABRIDGE')

# Inline query with parameters
df = db.execute_query(
    "SELECT * FROM portfolios WHERE value > {{ min_value }}",
    params={'min_value': 1000000},
    database='DataWarehouse'
)

# Execute SQL script from file
results = db.execute_query_from_file(
    'C:/sql/extract_policies.sql',
    params={'cycle_name': 'Q1-2025'},
    database='AnalyticsDB'
)
```

## Features

- **Automatic retry** with exponential backoff for transient errors (429, 5xx)
- **Workflow polling** — submit long-running operations and automatically poll to completion
- **Batch workflow execution** — run multiple workflows in parallel and wait for all to finish
- **Structured logging** via Python's `logging` module for visibility into API calls and workflow progress
- **Connection pooling** via persistent HTTP sessions
- **Input validation** with descriptive error messages
- **Custom exception hierarchy** for structured error handling
- **S3 upload/download** with multipart transfer support
- **Data Bridge (SQL Server)** — direct SQL execution against Moody's Data Bridge with parameterized queries and file-based scripts
- **Type hints** on all public methods

## Modules

| Manager | Description |
|---------|-------------|
| `client.edm` | Exposure Data Manager — create, upgrade, duplicate, and delete EDMs |
| `client.portfolio` | Portfolio CRUD, geocoding, and hazard processing |
| `client.mri_import` | MRI (CSV) data import workflow — bucket creation, file upload, mapping, and execution |
| `client.treaty` | Reinsurance treaty creation, LOB assignment, and reference data |
| `client.analysis` | Risk analysis execution, profiles, event rate schemes, and analysis groups |
| `client.rdm` | Results Data Mart — export analysis results to RDM |
| `client.risk_data_job` | Risk data job status tracking |
| `client.import_job` | Platform import job management (EDM/RDM imports) |
| `client.export_job` | Platform export job management — status, polling, and result download |
| `client.databridge` | Data Bridge (SQL Server) — parameterized queries, file-based SQL execution |
| `client.reference_data` | Tags, currencies, and other reference data lookups |

## Error Handling

The library uses a custom exception hierarchy:

```python
from irp_integration.exceptions import (
    IRPIntegrationError,          # Base exception
    IRPAPIError,                  # HTTP/API errors
    IRPValidationError,           # Input validation failures
    IRPWorkflowError,             # Workflow execution failures
    IRPReferenceDataError,        # Reference data lookup failures
    IRPFileError,                 # File operation failures
    IRPJobError,                  # Job management errors
    IRPDataBridgeError,           # Data Bridge base error
    IRPDataBridgeConnectionError, # SQL Server connection failures
    IRPDataBridgeQueryError,      # SQL query execution failures
)
```

## API Documentation

For detailed API endpoint documentation, see [docs/api.md](docs/api.md).

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
