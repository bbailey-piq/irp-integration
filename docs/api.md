# API Reference

Python client library for Moody's Risk Modeler API. Provides managers for exposure data (EDM), portfolios, MRI imports, treaties, analysis execution, RDM export, and reference data lookups.

## Table of Contents

- [Client](#client)
- [EDMManager](#edmmanager)
- [PortfolioManager](#portfoliomanager)
- [MRIImportManager](#mriimportmanager)
- [TreatyManager](#treatymanager)
- [AnalysisManager](#analysismanager)
- [RDMManager](#rdmmanager)
- [RiskDataJobManager](#riskdatajobmanager)
- [ImportJobManager](#importjobmanager)
- [ExportJobManager](#exportjobmanager)
- [S3Manager](#s3manager)
- [ReferenceDataManager](#referencedatamanager)
- [DataBridgeManager](#databridgemanager)
- [Exceptions](#exceptions)
- [Common Patterns](#common-patterns)

---

## Client

HTTP client with retry logic, workflow polling, and batch workflow execution.

### Constructor

```python
Client()
```

Reads credentials from environment variables. Configures a `requests.Session` with retry logic (5 retries with exponential backoff for status codes 429, 500, 502, 503, 504).

**Environment variables (all required):**

| Variable | Description |
|---|---|
| `RISK_MODELER_BASE_URL` | API base URL |
| `RISK_MODELER_API_KEY` | API authentication key |
| `RISK_MODELER_RESOURCE_GROUP_ID` | Resource group ID |

**Raises:** `IRPAPIError` if any required environment variable is missing.

---

### `request`

```python
def request(
    self,
    method: str,
    path: str,
    *,
    full_url: Optional[str] = None,
    base_url: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Union[Dict[str, Any], List[Any]]] = None,
    headers: Dict[str, str] = {},
    timeout: Optional[int] = None,
    stream: bool = False
) -> requests.Response
```

Make an HTTP request to the API. All API calls should go through this method.

**Args:**

| Parameter | Description |
|---|---|
| `method` | HTTP method (GET, POST, PUT, DELETE, etc.) |
| `path` | API path (e.g., `/riskmodeler/v1/workflows`) |
| `full_url` | Full URL (overrides path/base_url if provided) |
| `base_url` | Base URL override |
| `params` | Query parameters |
| `json` | JSON request body |
| `headers` | Additional headers |
| `timeout` | Request timeout in seconds (default: 200) |
| `stream` | Enable streaming response |

**Returns:** `requests.Response`

**Raises:** `IRPAPIError` on HTTP errors or request failures.

---

### `get_workflow`

```python
def get_workflow(self, workflow_id: int) -> Dict[str, Any]
```

Retrieve workflow status by workflow ID.

**Args:**

| Parameter | Description |
|---|---|
| `workflow_id` | Workflow ID |

**Returns:** Dict containing workflow status details.

---

### `poll_workflow_to_completion`

```python
def poll_workflow_to_completion(
    self,
    workflow_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll a workflow by ID until it reaches a completed status or times out.

**Args:**

| Parameter | Description |
|---|---|
| `workflow_id` | Workflow ID |
| `interval` | Polling interval in seconds (default: 10) |
| `timeout` | Maximum timeout in seconds (default: 600000) |

**Returns:** Final workflow status dict.

**Raises:** `IRPJobError` on timeout.

---

### `poll_workflow`

```python
def poll_workflow(
    self,
    workflow_url: str,
    interval: int = 10,
    timeout: int = 600000
) -> requests.Response
```

Poll a workflow by URL until it reaches a completed status or times out. Typically used with the `location` header returned from a workflow submission.

**Args:**

| Parameter | Description |
|---|---|
| `workflow_url` | Full URL to workflow endpoint |
| `interval` | Polling interval in seconds (default: 10) |
| `timeout` | Maximum timeout in seconds (default: 600000) |

**Returns:** Final workflow `requests.Response`.

**Raises:** `IRPWorkflowError` on timeout.

---

### `poll_workflow_batch_to_completion`

```python
def poll_workflow_batch_to_completion(
    self,
    workflow_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> requests.Response
```

Poll multiple workflows simultaneously until all complete or timeout. Handles pagination automatically (fetches in pages of 100).

**Args:**

| Parameter | Description |
|---|---|
| `workflow_ids` | List of workflow IDs to poll |
| `interval` | Polling interval in seconds (default: 20) |
| `timeout` | Maximum timeout in seconds (default: 600000) |

**Returns:** Response with all workflows combined.

**Raises:** `IRPWorkflowError` on timeout.

---

### `execute_workflow`

```python
def execute_workflow(
    self,
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Union[Dict[str, Any], List[Any]]] = None,
    headers: Dict[str, str] = {},
    timeout: Optional[int] = None,
    stream: bool = False
) -> requests.Response
```

Convenience method that submits a request and automatically polls the workflow to completion. Extracts the workflow URL from the `location` header of 201/202 responses.

**Args:** Same as `request()` (excluding `full_url` and `base_url`).

**Returns:** Final workflow response after completion.

**Raises:** `IRPAPIError` if request fails or location header is missing. `IRPWorkflowError` if workflow times out.

---

## EDMManager

Manager for EDM (Exposure Data Management) operations including creation, deletion, upgrade, duplication, and associated data retrieval.

### `validate_unique_edms`

```python
def validate_unique_edms(self, edm_names: List[str]) -> None
```

Validate that the given EDM names do not already exist.

**Args:**

| Parameter | Description |
|---|---|
| `edm_names` | List of EDM names to validate |

**Raises:** `IRPAPIError` if any EDM names already exist.

---

### `search_database_servers`

```python
def search_database_servers(self, filter: str = "") -> List[Dict[str, Any]]
```

Search database servers with optional filtering.

**Args:**

| Parameter | Description |
|---|---|
| `filter` | Optional filter string (e.g., `serverName="databridge-1"`) |

**Returns:** List of database server dicts.

---

### `search_exposure_sets`

```python
def search_exposure_sets(self, filter: str = "") -> List[Dict[str, Any]]
```

Search exposure sets with optional filtering.

**Args:**

| Parameter | Description |
|---|---|
| `filter` | Optional filter string |

**Returns:** List of exposure set dicts.

---

### `create_exposure_set`

```python
def create_exposure_set(self, name: str) -> int
```

Create a new exposure set.

**Args:**

| Parameter | Description |
|---|---|
| `name` | Name of the exposure set |

**Returns:** The exposure set ID (int).

---

### `search_edms`

```python
def search_edms(self, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]
```

Search EDMs (exposures) with optional filtering and pagination.

**Args:**

| Parameter | Description |
|---|---|
| `filter` | Optional filter string (e.g., `exposureName="MyEDM"`) |
| `limit` | Maximum results per page (default: 100) |
| `offset` | Offset for pagination (default: 0) |

**Returns:** List of EDM dicts.

---

### `search_edms_paginated`

```python
def search_edms_paginated(self, filter: str = "") -> List[Dict[str, Any]]
```

Search all EDMs with automatic pagination. Fetches all pages of results.

**Args:**

| Parameter | Description |
|---|---|
| `filter` | Optional filter string |

**Returns:** Complete list of all matching EDMs across all pages.

---

### `submit_create_edm_job`

```python
def submit_create_edm_job(self, edm_name: str, server_name: str = "databridge-1") -> Tuple[int, Dict[str, Any]]
```

Submit a job to create a new EDM (exposure). Validates the database server exists and creates an exposure set if needed.

**Args:**

| Parameter | Description |
|---|---|
| `edm_name` | Name of the EDM |
| `server_name` | Database server name (default: `"databridge-1"`) |

**Returns:** Tuple of `(job_id, request_body)`.

---

### `submit_create_edm_jobs`

```python
def submit_create_edm_jobs(self, edm_data_list: List[Dict[str, Any]]) -> List[int]
```

Submit multiple EDM creation jobs. Each dict in the list must contain `server_name` and `edm_name`. Validates all names are unique before submission.

**Args:**

| Parameter | Description |
|---|---|
| `edm_data_list` | List of dicts with `server_name` and `edm_name` |

**Returns:** List of job IDs.

---

### `submit_upgrade_edm_data_version_job`

```python
def submit_upgrade_edm_data_version_job(self, edm_name: str, edm_version: str) -> Tuple[int, Dict[str, Any]]
```

Submit a job to upgrade an EDM's data version.

**Args:**

| Parameter | Description |
|---|---|
| `edm_name` | Name of the EDM to upgrade |
| `edm_version` | Target data version (e.g., `"22"`) |

**Returns:** Tuple of `(job_id, request_body)`.

---

### `submit_upgrade_edm_data_version_jobs`

```python
def submit_upgrade_edm_data_version_jobs(self, edm_data_list: List[Dict[str, Any]]) -> List[int]
```

Submit multiple EDM data version upgrade jobs. Each dict must contain `edm_name` and `edm_version`.

**Args:**

| Parameter | Description |
|---|---|
| `edm_data_list` | List of dicts with `edm_name` and `edm_version` |

**Returns:** List of job IDs.

---

### `poll_data_version_upgrade_job_batch_to_completion`

```python
def poll_data_version_upgrade_job_batch_to_completion(
    self,
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

Poll multiple EDM data version upgrade jobs until all complete or timeout.

**Args:**

| Parameter | Description |
|---|---|
| `job_ids` | List of job IDs |
| `interval` | Polling interval in seconds (default: 20) |
| `timeout` | Maximum timeout in seconds (default: 600000) |

**Returns:** List of final job status dicts.

---

### `delete_edm`

```python
def delete_edm(self, edm_name: str) -> Dict[str, Any]
```

Delete an EDM and all its associated analyses. Looks up the EDM by name, deletes all analyses first, then submits the EDM deletion job and polls to completion.

**Args:**

| Parameter | Description |
|---|---|
| `edm_name` | Name of the EDM to delete |

**Returns:** Final delete job status dict.

---

### `submit_delete_edm_job`

```python
def submit_delete_edm_job(self, exposure_id: int) -> int
```

Submit a job to delete an EDM by exposure ID.

**Args:**

| Parameter | Description |
|---|---|
| `exposure_id` | ID of the exposure (EDM) |

**Returns:** Job ID (int).

---

### `get_cedants_by_edm`

```python
def get_cedants_by_edm(self, exposure_id: int) -> List[Dict[str, Any]]
```

Retrieve cedants for an EDM. Used in treaty creation to get cedant IDs.

**Args:**

| Parameter | Description |
|---|---|
| `exposure_id` | Exposure ID |

**Returns:** List of cedant dicts.

---

### `get_lobs_by_edm`

```python
def get_lobs_by_edm(self, exposure_id: int) -> List[Dict[str, Any]]
```

Retrieve lines of business (LOBs) for an EDM. Used in treaty creation for LOB assignment.

**Args:**

| Parameter | Description |
|---|---|
| `exposure_id` | Exposure ID |

**Returns:** List of LOB dicts.

---

### `submit_edm_import_job`

```python
def submit_edm_import_job(
    self,
    edm_name: str,
    edm_file_path: str,
    server_name: str = "sql-instance-1"
) -> Tuple[int, Dict[str, Any]]
```

Submit an EDM import job with S3 file upload. Handles the complete workflow: create import folder, upload `.bak` file to S3, create/get exposure set, and submit the import job.

**Args:**

| Parameter | Description |
|---|---|
| `edm_name` | Name for the EDM |
| `edm_file_path` | Path to the `.bak` file to import |
| `server_name` | Database server name (default: `"sql-instance-1"`) |

**Returns:** Tuple of `(job_id, request_body)`.

---

## PortfolioManager

Manager for portfolio operations including creation, retrieval, geocoding, and hazard processing.

### `get_portfolio_by_id`

```python
def get_portfolio_by_id(self, exposure_id: int, portfolio_id: int) -> Dict[str, Any]
```

Retrieve portfolio details by portfolio ID.

**Args:**

| Parameter | Description |
|---|---|
| `exposure_id` | Exposure ID |
| `portfolio_id` | Portfolio ID |

**Returns:** Dict containing portfolio details.

---

### `get_portfolio_metadata`

```python
def get_portfolio_metadata(self, exposure_id: int, portfolio_id: int) -> Dict[str, Any]
```

Retrieve portfolio metadata (metrics) by portfolio ID.

**Args:**

| Parameter | Description |
|---|---|
| `exposure_id` | Exposure ID |
| `portfolio_id` | Portfolio ID |

**Returns:** Dict containing portfolio metadata.

---

### `search_portfolios`

```python
def search_portfolios(self, exposure_id: int, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]
```

Search portfolios within an exposure with optional filtering and pagination.

**Args:**

| Parameter | Description |
|---|---|
| `exposure_id` | Exposure ID |
| `filter` | Optional filter string |
| `limit` | Maximum results per page (default: 100) |
| `offset` | Offset for pagination (default: 0) |

**Returns:** List of portfolio dicts.

---

### `search_portfolios_paginated`

```python
def search_portfolios_paginated(self, exposure_id: int, filter: str = "") -> List[Dict[str, Any]]
```

Search all portfolios within an exposure with automatic pagination.

**Args:**

| Parameter | Description |
|---|---|
| `exposure_id` | Exposure ID |
| `filter` | Optional filter string |

**Returns:** Complete list of all matching portfolios across all pages.

---

### `search_accounts_by_portfolio`

```python
def search_accounts_by_portfolio(self, exposure_id: int, portfolio_id: int) -> List[Dict[str, Any]]
```

Retrieve accounts within a portfolio.

**Args:**

| Parameter | Description |
|---|---|
| `exposure_id` | Exposure ID |
| `portfolio_id` | Portfolio ID |

**Returns:** List of account dicts.

---

### `create_portfolio`

```python
def create_portfolio(
    self,
    edm_name: str,
    portfolio_name: str,
    portfolio_number: str = "",
    description: str = ""
) -> Tuple[int, Dict[str, Any]]
```

Create a new portfolio in an EDM. Looks up the EDM by name, validates the portfolio name is unique, and creates the portfolio.

**Args:**

| Parameter | Description |
|---|---|
| `edm_name` | Name of the EDM |
| `portfolio_name` | Name for the new portfolio |
| `portfolio_number` | Portfolio number (defaults to portfolio_name; truncated to 20 chars) |
| `description` | Portfolio description (auto-generated if empty) |

**Returns:** Tuple of `(portfolio_id, request_body)`.

---

### `create_portfolios`

```python
def create_portfolios(self, portfolio_data_list: List[Dict[str, Any]]) -> List[int]
```

Create multiple portfolios. Each dict must contain `edm_name`, `portfolio_name`, `portfolio_number`, and `description`.

**Args:**

| Parameter | Description |
|---|---|
| `portfolio_data_list` | List of portfolio data dicts |

**Returns:** List of portfolio IDs.

---

### `submit_geohaz_job`

```python
def submit_geohaz_job(
    self,
    portfolio_name: str,
    edm_name: str,
    version: str = "22.0",
    hazard_eq: bool = False,
    hazard_ws: bool = False,
    geocode_layer_options: Optional[Dict[str, Any]] = None,
    hazard_layer_options: Optional[Dict[str, Any]] = None
) -> Tuple[int, Dict[str, Any]]
```

Submit a geocoding and/or hazard processing job on a portfolio. Always runs geocoding; optionally adds earthquake and/or windstorm hazard layers.

**Args:**

| Parameter | Description |
|---|---|
| `portfolio_name` | Name of the portfolio |
| `edm_name` | Name of the EDM containing the portfolio |
| `version` | Geocode/hazard engine version (default: `"22.0"`) |
| `hazard_eq` | Enable earthquake hazard (default: False) |
| `hazard_ws` | Enable windstorm hazard (default: False) |
| `geocode_layer_options` | Custom geocode layer options (optional) |
| `hazard_layer_options` | Custom hazard layer options (optional) |

**Returns:** Tuple of `(job_id, request_body)`.

---

### `submit_geohaz_jobs`

```python
def submit_geohaz_jobs(self, geohaz_data_list: List[Dict[str, Any]]) -> List[int]
```

Submit multiple geohaz jobs. Each dict must contain `edm_name`, `portfolio_name`, `version`, `hazard_eq`, and `hazard_ws`.

**Returns:** List of job IDs.

---

### `get_geohaz_job`

```python
def get_geohaz_job(self, job_id: int) -> Dict[str, Any]
```

Retrieve geohaz job status by job ID.

**Returns:** Dict containing job status details.

---

### `poll_geohaz_job_to_completion`

```python
def poll_geohaz_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll a geohaz job until completion or timeout.

**Returns:** Final job status dict.

**Raises:** `IRPJobError` on timeout.

---

### `poll_geohaz_job_batch_to_completion`

```python
def poll_geohaz_job_batch_to_completion(
    self,
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

Poll multiple geohaz jobs until all complete or timeout.

**Returns:** List of final job status dicts.

**Raises:** `IRPJobError` on timeout.

---

## MRIImportManager

Manager for MRI (Multi-Risk Insurance) data imports. Handles file upload to S3 and import job submission.

### `submit_mri_import_job`

```python
def submit_mri_import_job(
    self,
    edm_name: str,
    portfolio_name: str,
    accounts_file_path: str,
    locations_file_path: str,
    mapping_file_path: Optional[str] = None,
    delimiter: str = "TAB"
) -> Tuple[int, Dict[str, Any]]
```

Submit an MRI import job via the Platform Import API. Handles the complete workflow:

1. Look up EDM and portfolio
2. Create import folder (get S3 credentials)
3. Upload accounts, locations, and optionally mapping files to S3
4. Submit import job

**Args:**

| Parameter | Description |
|---|---|
| `edm_name` | Target EDM name |
| `portfolio_name` | Target portfolio name within the EDM |
| `accounts_file_path` | Path to accounts CSV file |
| `locations_file_path` | Path to locations CSV file |
| `mapping_file_path` | Optional path to `.mff` mapping file |
| `delimiter` | File delimiter (default: `"TAB"`) |

**Returns:** Tuple of `(job_id, request_body)`.

---

## TreatyManager

Manager for reinsurance treaty operations including creation, search, and LOB assignment.

### `search_treaties`

```python
def search_treaties(self, exposure_id: int, filter: str = '', limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]
```

Search treaties for a given exposure ID with optional filtering and pagination.

**Args:**

| Parameter | Description |
|---|---|
| `exposure_id` | Exposure ID |
| `filter` | Optional filter string |
| `limit` | Maximum results per page (default: 100) |
| `offset` | Offset for pagination (default: 0) |

**Returns:** List of treaty dicts.

---

### `search_treaties_paginated`

```python
def search_treaties_paginated(self, exposure_id: int, filter: str = '') -> List[Dict[str, Any]]
```

Search all treaties with automatic pagination.

**Returns:** Complete list of all matching treaties across all pages.

---

### `create_treaty`

```python
def create_treaty(
    self,
    edm_name: str,
    treaty_name: str,
    treaty_number: str,
    treaty_type: str,
    per_risk_limit: float,
    occurrence_limit: float,
    attachment_point: float,
    inception_date: str,
    expiration_date: str,
    currency_name: str,
    attachment_basis: str,
    attachment_level: str,
    pct_covered: float,
    pct_placed: float,
    pct_share: float,
    pct_retention: float,
    premium: float,
    num_reinstatements: int,
    pct_reinstatement_charge: float,
    aggregate_limit: float,
    aggregate_deductible: float,
    priority: int,
) -> Tuple[int, Dict[str, Any]]
```

Create a treaty with full parameters. Looks up the EDM, cedant, and currency by name, then creates the treaty and auto-assigns all LOBs from the EDM.

**Key Args:**

| Parameter | Description |
|---|---|
| `edm_name` | EDM name to create the treaty in |
| `treaty_name` | Treaty name |
| `treaty_number` | Treaty number (max 20 chars) |
| `treaty_type` | Must be one of: `Catastrophe`, `Corporate Catastrophe`, `Non-Catastrophe`, `Quota Share`, `Stop Loss`, `Surplus Share`, `Working Excess` |
| `attachment_basis` | Must be one of: `Losses Occurring`, `Risks Attaching` |
| `attachment_level` | Must be one of: `Account`, `Portfolio`, `Policy`, `Location` |
| `inception_date` | ISO format date string |
| `expiration_date` | ISO format date string |
| `currency_name` | Currency name (e.g., `"US Dollar"`) |

**Returns:** Tuple of `(treaty_id, request_body)`.

---

### `create_treaties`

```python
def create_treaties(self, treaty_data_list: List[Dict[str, Any]]) -> List[int]
```

Create multiple treaties from a list of data dicts containing all required treaty fields.

**Returns:** List of treaty IDs.

---

### `create_treaty_lob`

```python
def create_treaty_lob(self, exposure_id: int, treaty_id: int, lob_id: int, lobName: str) -> int
```

Create a Line of Business (LOB) assignment for a treaty.

**Args:**

| Parameter | Description |
|---|---|
| `exposure_id` | Exposure ID |
| `treaty_id` | Treaty ID |
| `lob_id` | LOB ID |
| `lobName` | LOB name |

**Returns:** Created LOB ID (int).

---

## AnalysisManager

Manager for analysis operations including portfolio analysis submission, job tracking, analysis grouping, result retrieval (ELT, EP, Stats, PLT), and analysis deletion.

### `get_analysis_by_id`

```python
def get_analysis_by_id(self, analysis_id: int) -> Dict[str, Any]
```

Retrieve analysis details by ID.

**Returns:** Dict containing analysis details.

---

### `get_analysis_by_name`

```python
def get_analysis_by_name(self, analysis_name: str, edm_name: str) -> Dict[str, Any]
```

Get an analysis by name and EDM name.

**Args:**

| Parameter | Description |
|---|---|
| `analysis_name` | Name of the analysis |
| `edm_name` | Name of the EDM |

**Returns:** Dict containing analysis details.

**Raises:** `IRPAPIError` if not found or multiple matches.

---

### `get_analysis_by_app_analysis_id`

```python
def get_analysis_by_app_analysis_id(self, app_analysis_id: int) -> Dict[str, Any]
```

Retrieve analysis by application analysis ID (the ID used in the UI).

**Returns:** Dict containing `analysisId`, `exposureResourceId`, `analysisName`, `engineType`, `uri`, and `raw` (full response).

---

### `search_analyses`

```python
def search_analyses(self, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]
```

Search analysis results with optional filtering and pagination.

**Args:**

| Parameter | Description |
|---|---|
| `filter` | Optional filter string (e.g., `analysisName = "MyAnalysis"`) |
| `limit` | Maximum results per page (default: 100) |
| `offset` | Offset for pagination (default: 0) |

**Returns:** List of analysis result dicts.

---

### `search_analyses_paginated`

```python
def search_analyses_paginated(self, filter: str = "") -> List[Dict[str, Any]]
```

Search all analysis results with automatic pagination.

**Returns:** Complete list of all matching analysis results across all pages.

---

### `search_analysis_jobs`

```python
def search_analysis_jobs(self, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]
```

Search analysis jobs with optional filtering.

**Returns:** List of analysis job dicts.

---

### `delete_analysis`

```python
def delete_analysis(self, analysis_id: int) -> None
```

Delete an analysis by ID.

---

### `submit_portfolio_analysis_job`

```python
def submit_portfolio_analysis_job(
    self,
    edm_name: str,
    portfolio_name: str,
    job_name: str,
    analysis_profile_name: str,
    output_profile_name: str,
    event_rate_scheme_name: str,
    treaty_names: List[str],
    tag_names: List[str],
    currency: Dict[str, str] = None,
    skip_duplicate_check: bool = False,
    franchise_deductible: bool = False,
    min_loss_threshold: float = 1.0,
    treat_construction_occupancy_as_unknown: bool = True,
    num_max_loss_event: int = 1
) -> Tuple[int, Dict[str, Any]]
```

Submit a portfolio analysis job. Resolves all reference data (model profile, output profile, event rate scheme, treaties, tags) by name before submission. Automatically determines job type (DLM vs HD) from the model profile.

**Key Args:**

| Parameter | Description |
|---|---|
| `edm_name` | Name of the EDM |
| `portfolio_name` | Name of the portfolio to analyze |
| `job_name` | Unique name for the analysis job |
| `analysis_profile_name` | Model profile name |
| `output_profile_name` | Output profile name |
| `event_rate_scheme_name` | Event rate scheme name (required for DLM, optional for HD) |
| `treaty_names` | List of treaty names to apply |
| `tag_names` | List of tag names to apply |
| `currency` | Optional currency config (auto-resolved if None) |
| `skip_duplicate_check` | Skip name uniqueness check (for batch use) |

**Returns:** Tuple of `(job_id, request_body)`.

---

### `submit_portfolio_analysis_jobs`

```python
def submit_portfolio_analysis_jobs(self, analysis_data_list: List[Dict[str, Any]]) -> List[int]
```

Submit multiple portfolio analysis jobs. Pre-validates that no analysis names already exist. Each dict must contain: `edm_name`, `portfolio_name`, `job_name`, `analysis_profile_name`, `output_profile_name`, `event_rate_scheme_name`, `treaty_names`, `tag_names`.

**Returns:** List of job IDs.

---

### `get_analysis_job`

```python
def get_analysis_job(self, job_id: int) -> Dict[str, Any]
```

Retrieve analysis job status by job ID.

**Returns:** Dict containing job status details.

---

### `poll_analysis_job_to_completion`

```python
def poll_analysis_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll an analysis job until completion or timeout.

**Returns:** Final job status dict.

**Raises:** `IRPJobError` on timeout.

---

### `poll_analysis_job_batch_to_completion`

```python
def poll_analysis_job_batch_to_completion(
    self,
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

Poll multiple analysis jobs until all complete or timeout. Handles pagination.

**Returns:** List of final job status dicts.

**Raises:** `IRPJobError` on timeout.

---

### `submit_analysis_grouping_job`

```python
def submit_analysis_grouping_job(
    self,
    group_name: str,
    analysis_names: List[str],
    simulate_to_plt: bool = False,
    num_simulations: int = 50000,
    propagate_detailed_losses: bool = False,
    reporting_window_start: str = "01/01/2021",
    simulation_window_start: str = "01/01/2021",
    simulation_window_end: str = "12/31/2021",
    region_peril_simulation_set: List[Dict[str, Any]] = None,
    description: str = "",
    currency: Dict[str, str] = None,
    analysis_edm_map: Optional[Dict[str, str]] = None,
    group_names: Optional[set] = None,
    skip_missing: bool = True
) -> Dict[str, Any]
```

Submit an analysis grouping job to combine multiple analyses or groups. Resolves analysis/group names to URIs. Auto-builds `regionPerilSimulationSet` when not provided (required for mixed ELT/PLT grouping).

**Key Args:**

| Parameter | Description |
|---|---|
| `group_name` | Name for the analysis group |
| `analysis_names` | List of analysis and/or group names to include |
| `simulate_to_plt` | Whether to simulate to PLT (default: False) |
| `num_simulations` | Number of simulations (default: 50000) |
| `analysis_edm_map` | Optional mapping of analysis names to EDM names for disambiguation |
| `group_names` | Optional set of known group names (looked up differently from analyses) |
| `skip_missing` | Skip missing analyses instead of raising errors (default: True) |

**Returns:** Dict with `job_id`, `skipped`, `skipped_items`, `included_items`. If all items are missing, returns `job_id=None` and `skipped=True`.

---

### `submit_analysis_grouping_jobs`

```python
def submit_analysis_grouping_jobs(
    self,
    grouping_data_list: List[Dict[str, Any]],
    analysis_edm_map: Optional[Dict[str, str]] = None,
    group_names: Optional[set] = None,
    skip_missing: bool = True
) -> List[int]
```

Submit multiple analysis grouping jobs. Each dict must contain `group_name` and `analysis_names`.

**Returns:** List of job IDs (excludes skipped jobs).

---

### `build_region_peril_simulation_set`

```python
def build_region_peril_simulation_set(self, analysis_ids: List[int]) -> List[Dict[str, Any]]
```

Build `regionPerilSimulationSet` from analysis/group IDs for grouping requests. Handles both ELT (DLM) and PLT (HD) frameworks, compound perils, and engine version merging.

**Args:**

| Parameter | Description |
|---|---|
| `analysis_ids` | List of analysis or group IDs |

**Returns:** List of region/peril simulation set entry dicts. Returns empty list if all analyses have compound perils or if pure ELT with unambiguous rate schemes.

---

### `get_analysis_grouping_job`

```python
def get_analysis_grouping_job(self, job_id: int) -> Dict[str, Any]
```

Retrieve analysis grouping job status by job ID.

**Returns:** Dict containing job status details.

---

### `poll_analysis_grouping_job_to_completion`

```python
def poll_analysis_grouping_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll an analysis grouping job until completion or timeout.

**Returns:** Final job status dict.

**Raises:** `IRPJobError` on timeout.

---

### `poll_analysis_grouping_job_batch_to_completion`

```python
def poll_analysis_grouping_job_batch_to_completion(
    self,
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

Poll multiple analysis grouping jobs until all complete or timeout.

**Returns:** List of final job status dicts.

**Raises:** `IRPJobError` on timeout.

---

### `get_elt`

```python
def get_elt(
    self,
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int,
    filter: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> List[Dict[str, Any]]
```

Retrieve Event Loss Table (ELT) for an analysis.

**Args:**

| Parameter | Description |
|---|---|
| `analysis_id` | Analysis ID |
| `perspective_code` | One of `GR` (Gross), `GU` (Ground-Up), `RL` (Reinsurance Layer) |
| `exposure_resource_id` | Exposure resource ID (portfolio ID from analysis) |
| `filter` | Optional filter (e.g., `"eventId IN (1, 2, 3)"`) |
| `limit` | Maximum records to return |
| `offset` | Records to skip (for pagination) |

**Returns:** List of ELT records.

---

### `get_ep`

```python
def get_ep(
    self,
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int
) -> List[Dict[str, Any]]
```

Retrieve Exceedance Probability (EP) metrics for an analysis.

**Args:**

| Parameter | Description |
|---|---|
| `analysis_id` | Analysis ID |
| `perspective_code` | One of `GR`, `GU`, `RL` |
| `exposure_resource_id` | Exposure resource ID |

**Returns:** List of EP curve data (OEP, AEP, CEP, TCE).

---

### `get_stats`

```python
def get_stats(
    self,
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int
) -> List[Dict[str, Any]]
```

Retrieve statistics for an analysis.

**Args:**

| Parameter | Description |
|---|---|
| `analysis_id` | Analysis ID |
| `perspective_code` | One of `GR`, `GU`, `RL` |
| `exposure_resource_id` | Exposure resource ID |

**Returns:** List of statistical metrics.

---

### `get_plt`

```python
def get_plt(
    self,
    analysis_id: int,
    perspective_code: str,
    exposure_resource_id: int,
    filter: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> List[Dict[str, Any]]
```

Retrieve Period Loss Table (PLT) for an analysis. Only available for HD (High Definition) analyses.

**Args:**

| Parameter | Description |
|---|---|
| `analysis_id` | Analysis ID |
| `perspective_code` | One of `GR`, `GU`, `RL` |
| `exposure_resource_id` | Exposure resource ID |
| `filter` | Optional filter string |
| `limit` | Maximum records (default: 100000) |
| `offset` | Records to skip |

**Returns:** List of PLT records.

---

### `get_regions`

```python
def get_regions(self, analysis_id: int) -> List[Dict[str, Any]]
```

Retrieve region/peril breakdown for an analysis or group. Used to build the `regionPerilSimulationSet` for grouping requests.

**Returns:** List of region dicts containing `region`, `subRegion`, `peril`, `rateSchemeId`, `framework`, `petId`, `engineVersion`, etc.

---

### `submit_analysis_export_job`

```python
def submit_analysis_export_job(
    self,
    analysis_id: int,
    loss_details: List[Dict[str, Any]],
    file_extension: str = "PARQUET"
) -> Tuple[int, Dict[str, Any]]
```

Submit an analysis results export job. Accepts either an `analysisId` or `appAnalysisId` — tries `analysisId` first, then falls back to `appAnalysisId`. Resolves the analysis to a resource URI and submits the export to the platform export API.

**Args:**

| Parameter | Description |
|---|---|
| `analysis_id` | Analysis ID or app analysis ID |
| `loss_details` | List of loss detail configs, each with `metricType`, `outputLevels`, and `perspectiveCodes` |
| `file_extension` | Export file format (default: `"PARQUET"`) |

**Returns:** Tuple of `(job_id, request_body)`.

**Raises:** `IRPAPIError` if analysis not found or request fails.

---

## RDMManager

Manager for RDM (Results Data Mart) export and import operations including database management and group access.

### `export_analyses_to_rdm`

```python
def export_analyses_to_rdm(
    self,
    server_name: str,
    rdm_name: str,
    analysis_names: List[str],
    skip_missing: bool = False
) -> Dict[str, Any]
```

Export multiple analyses to RDM and poll to completion. Convenience method combining `submit_rdm_export_job` and `poll_rdm_export_job_to_completion`.

**Args:**

| Parameter | Description |
|---|---|
| `server_name` | Database server name |
| `rdm_name` | Name for the RDM |
| `analysis_names` | List of analysis names to export |
| `skip_missing` | Skip missing analyses instead of raising error |

**Returns:** Final export job status dict (or skip result if all items missing).

---

### `submit_rdm_export_job`

```python
def submit_rdm_export_job(
    self,
    server_name: str,
    rdm_name: str,
    analysis_names: List[str],
    database_id: Optional[int] = None,
    analysis_edm_map: Optional[Dict[str, str]] = None,
    group_names: Optional[set] = None,
    skip_missing: bool = True
) -> Dict[str, Any]
```

Submit an RDM export job. Validates server exists, checks RDM name uniqueness, resolves analysis/group names to URIs. Automatically detects PLT framework and sets `exportHdLossesAs` accordingly.

**Key Args:**

| Parameter | Description |
|---|---|
| `server_name` | Database server name |
| `rdm_name` | Name for the RDM |
| `analysis_names` | List of analysis and group names to export |
| `database_id` | Optional database ID (for appending to existing RDM) |
| `analysis_edm_map` | Optional mapping of analysis names to EDM names |
| `group_names` | Optional set of known group names |
| `skip_missing` | Skip missing items (default: True) |

**Returns:** Dict with `job_id`, `skipped`, `skipped_items`, `included_items`.

---

### `get_rdm_export_job`

```python
def get_rdm_export_job(self, job_id: int) -> Dict[str, Any]
```

Retrieve RDM export job status by job ID.

**Returns:** Dict containing job status details.

---

### `poll_rdm_export_job_to_completion`

```python
def poll_rdm_export_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll RDM export job until completion or timeout.

**Returns:** Final job status dict.

**Raises:** `IRPJobError` on timeout.

---

### `get_rdm_database_id`

```python
def get_rdm_database_id(self, rdm_name: str, server_name: str = "databridge-1") -> int
```

Get database ID for an existing RDM by name prefix.

**Returns:** Database ID (int).

---

### `get_rdm_database_full_name`

```python
def get_rdm_database_full_name(self, rdm_name: str, server_name: str = "databridge-1") -> str
```

Get full database name for an existing RDM by name prefix (RDMs have a random suffix appended).

**Returns:** Full database name string.

---

### `search_databases`

```python
def search_databases(self, server_name: str, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]
```

Search databases on a server with optional filtering and pagination.

**Args:**

| Parameter | Description |
|---|---|
| `server_name` | Name of the database server |
| `filter` | Optional filter string (e.g., `'databaseName LIKE "MyRDM*"'`) |
| `limit` | Maximum results per page (default: 100) |
| `offset` | Offset for pagination (default: 0) |

**Returns:** List of database records.

---

### `search_databases_paginated`

```python
def search_databases_paginated(self, server_name: str, filter: str = "") -> List[Dict[str, Any]]
```

Search all databases on a server with automatic pagination.

**Returns:** Complete list of all matching database records across all pages.

---

### `submit_delete_rdm_job`

```python
def submit_delete_rdm_job(self, rdm_name: str, server_name: str = "databridge-1") -> str
```

Submit a job to delete an RDM from the databridge server.

**Returns:** Job ID string.

---

### `get_databridge_job`

```python
def get_databridge_job(self, job_id: str) -> str
```

Get the status of a databridge job.

**Returns:** Job status string (e.g., `"Enqueued"`, `"Processing"`, `"Succeeded"`).

---

### `poll_delete_rdm_job_to_completion`

```python
def poll_delete_rdm_job_to_completion(
    self,
    job_id: str,
    interval: int = 10,
    timeout: int = 600000
) -> str
```

Poll delete RDM job until completion or timeout.

**Returns:** Final job status string (`"Succeeded"`).

**Raises:** `IRPJobError` if job fails or times out.

---

### `add_group_access_to_rdm`

```python
def add_group_access_to_rdm(
    self,
    database_name: str,
    group_id: Optional[str] = None,
    server_name: str = "databridge-1"
) -> Dict[str, Any]
```

Add group access to an RDM database. If `group_id` is not provided, reads from the `DATABRIDGE_GROUP_ID` environment variable.

**Args:**

| Parameter | Description |
|---|---|
| `database_name` | Name of the RDM database |
| `group_id` | Group ID to grant access (falls back to env var) |
| `server_name` | Database server name (default: `"databridge-1"`) |

**Returns:** API response dict (empty dict on 204 success).

---

### `search_imported_rdms`

```python
def search_imported_rdms(self, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]
```

Search imported RDMs with optional filtering and pagination.

**Returns:** List of imported RDM records.

---

### `submit_rdm_import_job`

```python
def submit_rdm_import_job(
    self,
    rdm_name: str,
    edm_name: str,
    rdm_file_path: str
) -> Tuple[int, Dict[str, Any]]
```

Submit an RDM import job with S3 file upload. Handles the complete workflow: look up EDM, create import folder, upload `.bak` file to S3, and submit the import job.

**Args:**

| Parameter | Description |
|---|---|
| `rdm_name` | Name for the RDM |
| `edm_name` | Name of the EDM to import into |
| `rdm_file_path` | Path to the `.bak` file to import |

**Returns:** Tuple of `(job_id, request_body)`.

---

## RiskDataJobManager

Manager for unified risk data job tracking via the `/platform/riskdata/v1/jobs` endpoint. Provides status retrieval, polling, and batch polling for all platform risk data jobs.

### `get_risk_data_job`

```python
def get_risk_data_job(self, job_id: int) -> Dict[str, Any]
```

Retrieve job status by job ID.

**Returns:** Dict containing job status details.

---

### `search_risk_data_jobs`

```python
def search_risk_data_jobs(self, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]
```

Search risk data jobs with optional filtering and pagination.

**Returns:** List of risk data job dicts.

---

### `poll_risk_data_job_to_completion`

```python
def poll_risk_data_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll a risk data job until completion or timeout.

**Returns:** Final job status dict.

**Raises:** `IRPJobError` on timeout.

---

### `poll_risk_data_job_batch_to_completion`

```python
def poll_risk_data_job_batch_to_completion(
    self,
    job_ids: List[int],
    interval: int = 20,
    timeout: int = 600000
) -> List[Dict[str, Any]]
```

Poll multiple risk data jobs until all complete or timeout. Handles pagination.

**Returns:** List of final job status dicts.

**Raises:** `IRPJobError` on timeout.

---

## ImportJobManager

Centralized interface for submitting, tracking, and polling platform import jobs. Routes to the appropriate manager (EDM, RDM, or MRI) based on import type.

### `submit_job`

```python
def submit_job(self, import_type: str, **kwargs) -> Tuple[int, Dict[str, Any]]
```

Submit an import job, routing to the appropriate manager based on type.

**Args:**

| Parameter | Description |
|---|---|
| `import_type` | Type of import: `"EDM"`, `"RDM"`, or `"MRI"` |
| `**kwargs` | Arguments passed to the underlying submit method (see below) |

**For `EDM`** (routes to `EDMManager.submit_edm_import_job`):
- `edm_name`, `edm_file_path`, `server_name`

**For `RDM`** (routes to `RDMManager.submit_rdm_import_job`):
- `rdm_name`, `edm_name`, `rdm_file_path`

**For `MRI`** (routes to `MRIImportManager.submit_mri_import_job`):
- `edm_name`, `portfolio_name`, `accounts_file_path`, `locations_file_path`, `mapping_file_path`, `delimiter`

**Returns:** Tuple of `(job_id, request_body)`.

**Raises:** `IRPValidationError` if `import_type` is invalid.

---

### `get_import_job`

```python
def get_import_job(self, job_id: int) -> Dict[str, Any]
```

Get import job status by job ID.

**Returns:** Dict containing job status details.

---

### `poll_import_job_to_completion`

```python
def poll_import_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll an import job until completion or timeout.

**Returns:** Final job status dict.

**Raises:** `IRPJobError` on timeout.

---

## ExportJobManager

Manager for platform export job operations including status retrieval, polling, and result download.

### `get_export_job`

```python
def get_export_job(self, job_id: int) -> Dict[str, Any]
```

Get export job status by job ID.

**Args:**

| Parameter | Description |
|---|---|
| `job_id` | Export job ID |

**Returns:** Dict containing job status details.

---

### `poll_export_job_to_completion`

```python
def poll_export_job_to_completion(
    self,
    job_id: int,
    interval: int = 10,
    timeout: int = 600000
) -> Dict[str, Any]
```

Poll an export job until completion or timeout.

**Args:**

| Parameter | Description |
|---|---|
| `job_id` | Export job ID |
| `interval` | Polling interval in seconds (default: 10) |
| `timeout` | Maximum timeout in seconds (default: 600000) |

**Returns:** Final job status dict.

**Raises:** `IRPJobError` on timeout.

---

### `download_export_results`

```python
def download_export_results(self, job_id: int, output_dir: str) -> str
```

Download exported analysis results for a completed export job. Extracts the `downloadUrl` from the `DOWNLOAD_RESULTS` task and streams the zip file to the output directory. Creates the output directory if it doesn't exist.

**Args:**

| Parameter | Description |
|---|---|
| `job_id` | Export job ID (must be FINISHED) |
| `output_dir` | Directory to save the downloaded file |

**Returns:** Path to the downloaded file.

**Raises:** `IRPJobError` if job is not finished. `IRPAPIError` if download URL not found or download fails.

---

## S3Manager

Manager for S3 upload and CloudFront/presigned URL download operations. Uses temporary credentials provided by Moody's Risk Modeler API.

### Constructor

```python
S3Manager(transfer_config: Optional[TransferConfig] = None)
```

**Args:**

| Parameter | Description |
|---|---|
| `transfer_config` | Optional boto3 `TransferConfig` for multipart uploads. Defaults to 8MB threshold/chunks, 10 concurrent threads. |

---

### `upload_file`

```python
def upload_file(
    self,
    file_path: str,
    upload_details: Dict[str, Any],
    content_type: Optional[str] = None
) -> None
```

Upload a file to S3 using credentials from the API create-import-folder response. Parses the upload URL and base64-encoded credentials automatically.

**Args:**

| Parameter | Description |
|---|---|
| `file_path` | Path to the file to upload |
| `upload_details` | Upload details dict containing `uploadUrl` and `presignParams` |
| `content_type` | Optional content type override (inferred from extension if omitted) |

---

### `upload_fileobj`

```python
def upload_fileobj(
    self,
    fileobj: BinaryIO,
    upload_details: Dict[str, Any],
    content_type: str
) -> None
```

Upload a file-like object (e.g., `BytesIO`) to S3 using credentials from the API response.

**Args:**

| Parameter | Description |
|---|---|
| `fileobj` | File-like object in binary read mode |
| `upload_details` | Upload details dict containing `uploadUrl` and `presignParams` |
| `content_type` | Content type for the upload (required) |

---

### `upload_file_from_credentials`

```python
def upload_file_from_credentials(
    self,
    file_path: str,
    credentials: Dict[str, str],
    bucket: str,
    key: str,
    content_type: Optional[str] = None
) -> None
```

Upload a file to S3 using pre-decoded credentials. Lower-level method for cases where credentials are already decoded.

**Args:**

| Parameter | Description |
|---|---|
| `file_path` | Path to the file to upload |
| `credentials` | Dict with `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`, `s3_region` |
| `bucket` | S3 bucket name |
| `key` | S3 object key |
| `content_type` | Optional content type override |

---

### `download_from_url`

```python
def download_from_url(
    self,
    url: str,
    destination_path: str,
    chunk_size: int = 8192,
    timeout: int = 300
) -> None
```

Download a file from a CloudFront or presigned URL to a local path.

**Args:**

| Parameter | Description |
|---|---|
| `url` | Full URL including signed parameters |
| `destination_path` | Local path to save the file |
| `chunk_size` | Download chunk size in bytes (default: 8192) |
| `timeout` | Request timeout in seconds (default: 300) |

---

### `download_from_url_to_fileobj`

```python
def download_from_url_to_fileobj(
    self,
    url: str,
    fileobj: BinaryIO,
    chunk_size: int = 8192,
    timeout: int = 300
) -> None
```

Download a file from a CloudFront or presigned URL to a file-like object.

**Args:**

| Parameter | Description |
|---|---|
| `url` | Full URL including signed parameters |
| `fileobj` | File-like object in binary write mode |
| `chunk_size` | Download chunk size in bytes (default: 8192) |
| `timeout` | Request timeout in seconds (default: 300) |

---

## ReferenceDataManager

Manager for reference data operations including model profiles, output profiles, event rate schemes, currencies, tags, simulation sets, PET metadata, and software model version mappings.

### `get_model_profiles`

```python
def get_model_profiles(self) -> Dict[str, Any]
```

Retrieve all model profiles.

**Returns:** Dict containing model profile list.

---

### `get_model_profile_by_name`

```python
def get_model_profile_by_name(self, profile_name: str) -> Dict[str, Any]
```

Retrieve model profile by name.

**Args:**

| Parameter | Description |
|---|---|
| `profile_name` | Model profile name (e.g., `"DLM CBHU v23"`) |

**Returns:** Dict with `count` and `items` array.

---

### `get_output_profiles`

```python
def get_output_profiles(self) -> List[Dict[str, Any]]
```

Retrieve all output profiles.

**Returns:** List of output profile dicts.

---

### `get_output_profile_by_name`

```python
def get_output_profile_by_name(self, profile_name: str) -> List[Dict[str, Any]]
```

Retrieve output profile by name.

**Args:**

| Parameter | Description |
|---|---|
| `profile_name` | Output profile name |

**Returns:** List of matching output profile dicts.

---

### `get_event_rate_schemes`

```python
def get_event_rate_schemes(self) -> Dict[str, Any]
```

Retrieve all active event rate schemes.

**Returns:** Dict containing event rate scheme list.

---

### `get_event_rate_scheme_by_name`

```python
def get_event_rate_scheme_by_name(
    self,
    scheme_name: str,
    peril_code: str = None,
    model_region_code: str = None
) -> Dict[str, Any]
```

Retrieve event rate scheme by name with optional peril and region filtering. Use `peril_code` and `model_region_code` to disambiguate when the same scheme name exists for multiple peril/region combinations.

**Args:**

| Parameter | Description |
|---|---|
| `scheme_name` | Event rate scheme name |
| `peril_code` | Optional peril code (e.g., `"CS"`, `"WS"`) |
| `model_region_code` | Optional model region code (e.g., `"NACS"`, `"NAWS"`) |

**Returns:** Dict with `count` and `items` array.

---

### `search_currencies`

```python
def search_currencies(self, where_clause: str = "") -> Dict[str, Any]
```

Search currencies with optional filtering.

**Returns:** Dict containing currency list.

---

### `search_currency_scheme_vintages`

```python
def search_currency_scheme_vintages(self, where_clause: str = "") -> Dict[str, Any]
```

Search currency scheme vintages with optional filtering.

**Returns:** Dict containing currency scheme vintage list.

---

### `get_latest_currency_scheme_vintage`

```python
def get_latest_currency_scheme_vintage(self) -> Dict[str, Any]
```

Get the latest RMS currency scheme vintage by effective date.

**Returns:** Dict containing the currency scheme vintage with the most recent `effectiveDate`.

---

### `get_analysis_currency`

```python
def get_analysis_currency(self) -> Dict[str, str]
```

Get currency dict for analysis requests. Attempts to retrieve the latest RMS currency scheme vintage from the API; falls back to default values on failure.

**Returns:** Currency dict with `asOfDate`, `code`, `scheme`, and `vintage`.

---

### `get_currency_by_name`

```python
def get_currency_by_name(self, currency_name: str) -> Dict[str, Any]
```

Retrieve currency by name.

**Args:**

| Parameter | Description |
|---|---|
| `currency_name` | Currency name (e.g., `"US Dollar"`) |

**Returns:** Dict containing currency details (`currencyId`, `currencyCode`, `currencyName`).

---

### `get_tag_by_name`

```python
def get_tag_by_name(self, tag_name: str) -> List[Dict[str, Any]]
```

Retrieve tag by name.

**Returns:** List of matching tag dicts.

---

### `create_tag`

```python
def create_tag(self, tag_name: str) -> Dict[str, str]
```

Create a new tag.

**Returns:** Dict with `id` (the created tag ID).

---

### `get_tag_ids_from_tag_names`

```python
def get_tag_ids_from_tag_names(self, tag_names: List[str]) -> List[int]
```

Get or create tags by names and return their IDs. Creates tags that do not already exist.

**Returns:** List of tag IDs.

---

### `get_all_simulation_sets`

```python
def get_all_simulation_sets(self) -> List[Dict[str, Any]]
```

Get all active simulation sets. Simulation sets map event rate scheme IDs to simulation set IDs for ELT-based analyses.

**Returns:** List of simulation set dicts.

---

### `get_simulation_set_by_event_rate_scheme_id`

```python
def get_simulation_set_by_event_rate_scheme_id(self, event_rate_scheme_id: int) -> Dict[str, Any]
```

Get simulation set by event rate scheme ID. For ELT analyses, the `simulationSetId` in grouping requests comes from this lookup.

**Returns:** Simulation set dict with `id` being the `simulationSetId`.

---

### `get_simulation_set_by_region_peril_and_engine`

```python
def get_simulation_set_by_region_peril_and_engine(
    self, region_code: str, peril_code: str, engine_version: str
) -> Dict[str, Any]
```

Fallback method to get simulation set by region code, peril code, and engine version. When multiple sets match, returns the one with the highest ID (most recent).

**Args:**

| Parameter | Description |
|---|---|
| `region_code` | Region code (e.g., `"NA"`, `"US"`) |
| `peril_code` | Peril code (e.g., `"WS"`, `"EQ"`) |
| `engine_version` | Engine version (e.g., `"RL23"`, `"HDv2.0"`) |

**Returns:** Simulation set dict.

---

### `get_all_pet_metadata`

```python
def get_all_pet_metadata(self) -> List[Dict[str, Any]]
```

Get all PET (Probabilistic Event Table) metadata. PET metadata maps PET IDs to simulation set IDs for PLT/HD-based analyses.

**Returns:** List of PET metadata dicts.

---

### `get_pet_metadata_by_id`

```python
def get_pet_metadata_by_id(self, pet_id: int) -> Dict[str, Any]
```

Get PET metadata by PET ID. For PLT/HD analyses, `simulationSetId` = `petId`.

**Returns:** PET metadata dict.

---

### `get_all_software_model_version_map`

```python
def get_all_software_model_version_map(self) -> List[Dict[str, Any]]
```

Get all active software model version mappings. Maps engine versions to model versions for grouping requests.

**Returns:** List of version map dicts.

---

### `get_model_version_by_engine_version`

```python
def get_model_version_by_engine_version(self, engine_version: str) -> str
```

Get model version for a given engine version.

**Args:**

| Parameter | Description |
|---|---|
| `engine_version` | Engine version string (e.g., `"HDv2.0"`, `"RL23"`) |

**Returns:** Model version string (e.g., `"2.0"`, `"23.0"`).

---

### `get_model_version_by_engine_region_peril`

```python
def get_model_version_by_engine_region_peril(
    self, engine_version: str, region_code: str, peril_code: str
) -> str
```

Get model version with precise matching using engine version, region code, and peril code.

**Args:**

| Parameter | Description |
|---|---|
| `engine_version` | Engine version string |
| `region_code` | Region code (e.g., `"NA"`, `"US"`) |
| `peril_code` | Peril code (e.g., `"WS"`, `"EQ"`) |

**Returns:** Model version string.

---

## DataBridgeManager

Manager for SQL Server (Data Bridge) operations. Provides direct SQL connectivity via pyodbc with parameterized query execution.

Unlike other managers, DataBridgeManager does not depend on the HTTP Client. It connects directly to SQL Server and can be used standalone or via `client.databridge`.

**Requires:** `pip install irp-integration[databridge]` and [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### Constructor

```python
DataBridgeManager(default_connection: str = 'TEST')
```

**Args:**

| Parameter | Description |
|---|---|
| `default_connection` | Default connection name used when no connection is specified in method calls |

**Environment variables (per connection):**

| Variable | Required | Description |
|---|---|---|
| `MSSQL_{NAME}_SERVER` | Yes | Server hostname or IP |
| `MSSQL_{NAME}_USER` | Yes | SQL Server username |
| `MSSQL_{NAME}_PASSWORD` | Yes | SQL Server password |
| `MSSQL_{NAME}_PORT` | No | Port (default: 1433) |

**Global environment variables:**

| Variable | Default | Description |
|---|---|---|
| `MSSQL_DRIVER` | `ODBC Driver 18 for SQL Server` | ODBC driver name |
| `MSSQL_TRUST_CERT` | `yes` | Trust server certificate |
| `MSSQL_TIMEOUT` | `30` | Connection timeout in seconds |

---

### `get_connection_config`

```python
def get_connection_config(self, connection_name: Optional[str] = None) -> Dict[str, str]
```

Get connection configuration for a named MSSQL connection from environment variables.

**Args:**

| Parameter | Description |
|---|---|
| `connection_name` | Name of the connection (e.g., `'DATABRIDGE'`). Defaults to the manager's `default_connection`. |

**Returns:** Dictionary with connection parameters (server, port, driver, user, password, etc.).

**Raises:** `IRPValidationError` if required environment variables are missing.

---

### `build_connection_string`

```python
def build_connection_string(self, connection_name: Optional[str] = None, database: Optional[str] = None) -> str
```

Build ODBC connection string for SQL Server.

**Args:**

| Parameter | Description |
|---|---|
| `connection_name` | Name of the connection. Defaults to the manager's `default_connection`. |
| `database` | Optional database name to include in the connection string |

**Returns:** ODBC connection string.

---

### `get_connection`

```python
@contextmanager
def get_connection(self, connection_name: Optional[str] = None, database: Optional[str] = None)
```

Context manager for SQL Server database connections. Automatically handles connection lifecycle.

**Args:**

| Parameter | Description |
|---|---|
| `connection_name` | Name of the connection. Defaults to the manager's `default_connection`. |
| `database` | Optional database name to connect to |

**Yields:** `pyodbc.Connection` object.

**Raises:** `IRPDataBridgeConnectionError` if connection fails.

---

### `test_connection`

```python
def test_connection(self, connection_name: Optional[str] = None) -> bool
```

Test if a SQL Server connection is working.

**Args:**

| Parameter | Description |
|---|---|
| `connection_name` | Name of the connection to test. Defaults to the manager's `default_connection`. |

**Returns:** `True` if connection successful, `False` otherwise.

---

### `execute_query`

```python
def execute_query(
    self,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    connection: Optional[str] = None,
    database: Optional[str] = None
) -> pd.DataFrame
```

Execute SELECT query and return results as DataFrame. Supports `{{ param_name }}` parameter placeholders.

**Args:**

| Parameter | Description |
|---|---|
| `query` | SQL SELECT query (supports `{{ param_name }}` placeholders) |
| `params` | Query parameters as dictionary |
| `connection` | SQL Server connection name. Defaults to the manager's `default_connection`. |
| `database` | Optional database name |

**Returns:** pandas DataFrame with query results.

**Raises:** `IRPDataBridgeQueryError` if query execution fails.

---

### `execute_scalar`

```python
def execute_scalar(
    self,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    connection: Optional[str] = None,
    database: Optional[str] = None
) -> Any
```

Execute query and return single scalar value (first column of first row).

**Args:**

| Parameter | Description |
|---|---|
| `query` | SQL query returning single value |
| `params` | Query parameters |
| `connection` | SQL Server connection name. Defaults to the manager's `default_connection`. |
| `database` | Optional database name |

**Returns:** Single value from query result (or `None` if no results).

**Raises:** `IRPDataBridgeQueryError` if query execution fails.

---

### `execute_command`

```python
def execute_command(
    self,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    connection: Optional[str] = None,
    database: Optional[str] = None
) -> int
```

Execute non-query command (INSERT, UPDATE, DELETE) and return rows affected.

**Args:**

| Parameter | Description |
|---|---|
| `query` | SQL command |
| `params` | Query parameters |
| `connection` | SQL Server connection name. Defaults to the manager's `default_connection`. |
| `database` | Optional database name |

**Returns:** Number of rows affected.

**Raises:** `IRPDataBridgeQueryError` if command execution fails.

---

### `execute_query_from_file`

```python
def execute_query_from_file(
    self,
    file_path: str,
    params: Optional[Dict[str, Any]] = None,
    connection: Optional[str] = None,
    database: Optional[str] = None
) -> List[pd.DataFrame]
```

Execute SQL query from file and return results as list of DataFrames. Handles multi-statement scripts (e.g., scripts with USE statements followed by SELECT). Each result set is returned as a separate DataFrame.

**Args:**

| Parameter | Description |
|---|---|
| `file_path` | Path to SQL file (absolute or relative to cwd) |
| `params` | Query parameters (supports `{{ param_name }}` placeholders) |
| `connection` | SQL Server connection name. Defaults to the manager's `default_connection`. |
| `database` | Optional database name |

**Returns:** List of pandas DataFrames, one per result set.

**Raises:** `IRPValidationError` if file does not exist. `IRPDataBridgeQueryError` if query execution fails.

---

### Parameter Substitution

SQL queries and scripts support named parameters using `{{ param_name }}` syntax. Parameters are context-aware:

**Value contexts** (escaped and quoted):
```sql
SELECT * FROM table WHERE id = {{ user_id }} AND name = {{ user_name }}
-- With params={'user_id': 123, 'user_name': 'John'}
-- Becomes: SELECT * FROM table WHERE id = 123 AND name = 'John'
```

**Identifier contexts** (raw substitution, no quoting):
```sql
-- Inside square brackets:
USE [{{ db_name }}]
-- Becomes: USE [my_database]

-- As part of table names:
SELECT * FROM CombinedData_{{ date_val }}_Working
-- Becomes: SELECT * FROM CombinedData_20250115_Working

-- Inside string literals:
SELECT 'Modeling_{{ date_val }}_Moodys' as table_name
-- Becomes: SELECT 'Modeling_202501_Moodys' as table_name
```

**SQL injection protection:**
- String values have single quotes escaped (doubled)
- Numeric values are inserted directly
- NULL values produce the `NULL` keyword
- Identifier values are validated to contain only safe characters

---

## Exceptions

| Exception | Description |
|---|---|
| `IRPIntegrationError` | Base exception for all IRP integration errors |
| `IRPAPIError` | HTTP/API request or response errors |
| `IRPValidationError` | Input validation failures |
| `IRPWorkflowError` | Workflow execution failures (timeout, error status) |
| `IRPReferenceDataError` | Reference data lookup failures |
| `IRPFileError` | File operation failures (not found, upload errors) |
| `IRPJobError` | Job management errors (submission, polling, timeout) |
| `IRPDataBridgeError` | Data Bridge (SQL Server) base error |
| `IRPDataBridgeConnectionError` | SQL Server connection failures (bad credentials, unreachable server) |
| `IRPDataBridgeQueryError` | SQL query execution failures, parameter substitution errors |

All exceptions inherit from `IRPIntegrationError`. Data Bridge exceptions inherit from `IRPDataBridgeError`.

---

## Common Patterns

### Authentication

All requests use the `Authorization` header with the API key and `x-rms-resource-group-id` header from environment variables. These are set automatically on the `requests.Session`.

### Environment Variables

All three environment variables are required with no defaults:

| Variable | Description |
|---|---|
| `RISK_MODELER_BASE_URL` | API base URL |
| `RISK_MODELER_API_KEY` | API authentication key |
| `RISK_MODELER_RESOURCE_GROUP_ID` | Resource group ID |

Optional environment variable:

| Variable | Description |
|---|---|
| `DATABRIDGE_GROUP_ID` | Group ID for RDM access control (used by `RDMManager.add_group_access_to_rdm`) |

### Workflow Pattern

Most operations are asynchronous and return a job ID rather than immediate results:

1. **Submit** -- POST/DELETE request returns 201/202 with `location` header containing a job/workflow URL
2. **Extract ID** -- Job ID is extracted from the `location` header
3. **Poll** -- Poll the job endpoint until status is `FINISHED`, `FAILED`, or `CANCELLED`

```python
# Single job: submit + poll
job_id, _ = client.edm.submit_create_edm_job(edm_name="MyEDM")
result = client.risk_data_job.poll_risk_data_job_to_completion(job_id)

# Batch: submit multiple + poll all
job_ids = client.edm.submit_create_edm_jobs(edm_data_list)
results = client.risk_data_job.poll_risk_data_job_batch_to_completion(job_ids)
```

### Workflow Statuses

```python
WORKFLOW_COMPLETED_STATUSES = ['FINISHED', 'FAILED', 'CANCELLED']
WORKFLOW_IN_PROGRESS_STATUSES = ['QUEUED', 'PENDING', 'RUNNING', 'CANCEL_REQUESTED', 'CANCELLING']
```

### Pagination

Many search methods offer both a standard version (with `limit`/`offset` parameters) and a `_paginated` variant that automatically fetches all pages:

```python
# Manual pagination
page = client.edm.search_edms(filter='...', limit=100, offset=0)

# Automatic pagination
all_edms = client.edm.search_edms_paginated(filter='...')
```

### Error Handling

- HTTP errors are enriched with the server response message (up to 200 characters)
- Retry logic: 5 retries with exponential backoff for status codes 429, 500, 502, 503, 504
- Default request timeout: 200 seconds
- Default polling timeout: 600,000 seconds

### Resource IDs

Resource IDs are extracted from the `location` header after creation:

```python
resource_id = response.headers['location'].split('/')[-1]
```

### Session Management

Uses `requests.Session` with:
- Connection pooling via `HTTPAdapter`
- Automatic retry configuration
- Persistent authentication headers across requests
