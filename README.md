# irp-integration

A Python client library for the [Moody's Intelligent Risk Platform (IRP) APIs](https://developer.rms.com/). Built to serve as a foundation for larger Moody's integration projects — use it with Jupyter Notebooks, Azure Functions, or any orchestration layer to build end-to-end risk analysis workflows.

Not all Moody's API functionality is covered yet, but the most common operations are available and the library is actively maintained. Contributions are welcome — feel free to fork and modify to fit your project's needs.

## Installation

```bash
pip install irp-integration
```

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

## Features

- **Automatic retry** with exponential backoff for transient errors (429, 5xx)
- **Workflow polling** — submit long-running operations and automatically poll to completion
- **Batch workflow execution** — run multiple workflows in parallel and wait for all to finish
- **Structured logging** via Python's `logging` module for visibility into API calls and workflow progress
- **Connection pooling** via persistent HTTP sessions
- **Input validation** with descriptive error messages
- **Custom exception hierarchy** for structured error handling
- **S3 upload/download** with multipart transfer support
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
| `client.reference_data` | Tags, currencies, and other reference data lookups |

## Error Handling

The library uses a custom exception hierarchy:

```python
from irp_integration.exceptions import (
    IRPIntegrationError,     # Base exception
    IRPAPIError,             # HTTP/API errors
    IRPValidationError,      # Input validation failures
    IRPWorkflowError,        # Workflow execution failures
    IRPReferenceDataError,   # Reference data lookup failures
    IRPFileError,            # File operation failures
    IRPJobError,             # Job management errors
)
```

## API Documentation

For detailed API endpoint documentation, see [docs/api.md](docs/api.md).

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
