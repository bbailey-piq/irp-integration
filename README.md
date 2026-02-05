# IRP Integration API Endpoint Documentation

This document provides comprehensive documentation of all API endpoints used throughout the `irp_integration` module, as demonstrated in the `IRP_Integration_Demo.ipynb` notebook.

## Table of Contents
- [Workflow Management](#workflow-management)
- [EDM (Exposure Data Manager) Operations](#edm-exposure-data-manager-operations)
- [Portfolio Operations](#portfolio-operations)
- [MRI Import (Exposure Data Import)](#mri-import-exposure-data-import)
- [Treaty Operations](#treaty-operations)
- [Analysis Operations](#analysis-operations)
- [Tag Operations](#tag-operations)
- [RDM (Results Data Mart) Export](#rdm-results-data-mart-export)
- [Reference Data](#reference-data)
- [Common Patterns](#common-patterns)

---

## Workflow Management

### Overview: Workflow ID Collection and Batch Polling Pattern

Most operations in the RMS API (EDM creation, GeoHaz processing, portfolio analysis, etc.) are asynchronous and follow a workflow pattern:

1. **Submit Request**: POST/DELETE request to create a long-running operation
2. **Extract Workflow ID**: API responds with 201/202 status and `location` header containing workflow URL
   ```
   location: {baseUrl}/riskmodeler/v1/workflows/{workflowId}
   ```
3. **Collect Workflow IDs**: Extract workflow ID from location header and add to a list
4. **Batch Poll**: Use collected workflow IDs to poll multiple workflows simultaneously

**Example Workflow ID Collection**:
```python
workflow_ids = []

# Submit multiple analysis jobs
for config in analysis_configs:
    response = client.request('POST', '/riskmodeler/v2/portfolios/1/process', json=config)
    # Extract workflow ID from location header
    workflow_url = response.headers['location']
    workflow_id = workflow_url.split('/')[-1]
    workflow_ids.append(workflow_id)

# Poll all workflows in batch
client.poll_workflow_batch(workflow_ids)
```

**Benefits of Batch Polling**:
- Monitor multiple long-running operations simultaneously
- More efficient than polling each workflow individually
- Reduces API calls and improves performance
- Useful for scenarios like submitting multiple portfolio analyses with different parameters

---

### GET /riskmodeler/v1/workflows

**When**: Used to poll multiple workflow jobs in batch mode

**Where**: Called by `Client.poll_workflow_batch()` method

**Why**: Monitor status of multiple long-running operations simultaneously (e.g., multiple analysis jobs)

**How**:
```python
# Query Parameters
{
    "ids": "workflow_id1,workflow_id2,workflow_id3",  # Comma-separated workflow IDs
    "limit": 100,   # Maximum number of results per page (default: 100)
    "offset": 0     # Starting offset for pagination (default: 0)
}

# Returns
{
    "totalMatchCount": 250,  # Total number of matching workflows
    "workflows": [
        {
            "id": 12345,
            "status": "FINISHED",  # or QUEUED, PENDING, RUNNING, FAILED, CANCEL_REQUESTED, CANCELLING, CANCELLED
            "progress": 100,
            ...
        }
    ]
}
```

**Pagination Handling**:
The `poll_workflow_batch()` method automatically handles pagination when monitoring large batches of workflows:
- Fetches workflows in pages of 100 (limit parameter)
- Uses `totalMatchCount` from response to determine when all workflows have been retrieved
- Increments `offset` by `limit` for each subsequent page
- Aggregates all workflows across pages before checking completion status
- Returns response with all workflows combined in the `workflows` array

**Example Flow**:
```python
# For 250 workflow IDs:
# Page 1: offset=0, limit=100   → workflows 1-100
# Page 2: offset=100, limit=100 → workflows 101-200
# Page 3: offset=200, limit=100 → workflows 201-250
# All pages aggregated before status check
```

### GET /riskmodeler/v1/workflows/{workflow_id}

**When**: After submitting any workflow operation

**Where**: Called by `Client.poll_workflow()` method

**Why**: Monitor individual workflow job status until completion

**How**:
- Uses full URL from response `location` header
- Polls every 10 seconds (default interval)
- Checks for status in: `FINISHED`, `FAILED`, `CANCELLED`, `QUEUED`, `PENDING`, `RUNNING`
- Continues until workflow reaches a completed state

---

## EDM (Exposure Data Manager) Operations

### GET /riskmodeler/v2/datasources

**When**: Retrieving EDM information or checking if EDM exists

**Where**: `EDMManager.get_all_edms()` and `EDMManager.get_edm_by_name()`

**Why**: List all datasources or search for specific EDM by name

**How**:
```python
# Query Parameters (for filtering)
{
    "q": "datasourceName=CBHU_Automated"
}

# Returns list of datasource objects
{
    "searchItems": [...]
}
```

### POST /riskmodeler/v2/datasources

**When**: Creating new EDM or upgrading EDM version

**Where**: `EDMManager.create_edm()` and `EDMManager.upgrade_edm_version()`

**Why**: Initialize new exposure database or upgrade to latest data version

**How**:
```python
# For creating new EDM
# Query Parameters
{
    "datasourcename": "CBHU_Automated",
    "servername": "databridge-1",
    "operation": "CREATE"
}

# For upgrading EDM version
# Query Parameters
{
    "datasourcename": "CBHU_Automated",
    "operation": "EDM_DATA_UPGRADE"
}

# Returns 201/202 with location header containing workflow URL
# Response Headers
{
    "location": "https://api-euw1.rms-ppe.com/riskmodeler/v1/workflows/22812421"
}
```

**Demo Usage**: Creates "CBHU_Automated" EDM on "databridge-1" server

### DELETE /riskmodeler/v2/datasources/{edm_name}

**When**: Removing an EDM from system

**Where**: `EDMManager.delete_edm()`

**Why**: Clean up unused exposure databases

**How**: URL path parameter contains EDM name to delete

### POST /riskmodeler/v2/exports

**When**: Duplicating an EDM with all its data

**Where**: `EDMManager.duplicate_edm()`

**Why**: Create copy of EDM (e.g., for testing or backup)

**How**:
```python
# Request Body
{
    "createnew": True,
    "exportType": "EDM",
    "sourceDatasource": "CBHU_Automated",
    "destinationDatasource": "np_CBHU_Automated",
    "exposureType": "PORTFOLIO",
    "exposureIds": [1, 2, 3],  # Portfolio IDs to include
    "download": False,
    "exportFormat": "BAK",
    "exportOptions": {
        "exportAccounts": True,
        "exportLocations": True,
        "exportPerilDetailsInfo": True,
        "exportPolicies": True,
        "exportReinsuranceInfo": True,
        "exportTreaties": True
    },
    "preserveIds": True,
    "sqlVersion": 2019,
    "type": "ExposureExportInput"
}
```

### GET /riskmodeler/v1/cedants

**When**: Need to retrieve cedant (insurance company) information

**Where**: `EDMManager.get_cedants_by_edm()` - used in treaty creation

**Why**: Get cedant IDs required for treaty setup

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated",
    "fields": "id, name",
    "limit": 100
}

# Returns
{
    "searchItems": [
        {"id": 1, "name": "Cedant Name"}
    ]
}
```

**Demo Usage**: Retrieves cedants to assign to treaty

### GET /riskmodeler/v1/lobs

**When**: Need line of business data

**Where**: `EDMManager.get_lobs_by_edm()` - used in treaty creation

**Why**: Get LOB IDs to associate with treaties

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated",
    "fields": "id, name",
    "limit": 100
}

# Returns
{
    "searchItems": [
        {"id": 1, "name": "Property"}
    ]
}
```

**Demo Usage**: Retrieves LOBs to assign to treaty

---

## Portfolio Operations

### POST /riskmodeler/v2/portfolios

**When**: Creating new portfolio within an EDM

**Where**: `PortfolioManager.create_portfolio()`

**Why**: Organize exposure data into logical groupings

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated"
}

# Request Body
{
    "name": "CBHU_Automated",
    "number": "1",
    "description": ""
}

# Returns 201 with location header
# Response Headers
{
    "location": "/riskmodeler/v2/portfolios/1"
}
```

**Demo Usage**: Creates "CBHU_Automated" portfolio

### GET /riskmodeler/v2/portfolios

**When**: Listing portfolios in an EDM

**Where**: `PortfolioManager.get_portfolios_by_edm_name()`

**Why**: Retrieve portfolio list (e.g., for duplication operations)

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated"
}
```

### GET /riskmodeler/v2/portfolios/{portfolio_id}

**When**: Getting specific portfolio details

**Where**: `PortfolioManager.get_portfolio_by_edm_name_and_id()`

**Why**: Retrieve detailed portfolio information

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated"
}
# URL path contains portfolio ID
```

### POST /riskmodeler/v2/portfolios/{portfolio_id}/geohaz

**When**: After importing exposure data, before running analysis

**Where**: `PortfolioManager.geohaz_portfolio()`

**Why**: Geocode locations and/or run hazard calculations for earthquake/windstorm

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated"
}

# Request Body - Array of layer definitions
[
    {
        "name": "geocode",
        "type": "geocode",
        "engineType": "RL",
        "version": "22.0",
        "layerOptions": {
            "aggregateTriggerEnabled": "true",
            "geoLicenseType": "0",
            "skipPrevGeocoded": False
        }
    },
    {
        "name": "earthquake",
        "type": "hazard",
        "engineType": "RL",
        "version": "22.0",
        "layerOptions": {
            "overrideUserDef": False,
            "skipPrevHazard": False
        }
    },
    {
        "name": "windstorm",
        "type": "hazard",
        "engineType": "RL",
        "version": "22.0",
        "layerOptions": {
            "overrideUserDef": False,
            "skipPrevHazard": False
        }
    }
]
```

**Demo Usage**: Runs geocoding + earthquake + windstorm hazard for 2579 locations

### POST /riskmodeler/v2/portfolios/{portfolio_id}/process

**When**: Running catastrophe analysis on portfolio

**Where**: `AnalysisManager.analyze_portfolio()` and `submit_analysis_job()`

**Why**: Execute risk modeling with specified profiles and settings

**How**:
```python
# Request Body
{
    "currency": {
        "asOfDate": "2018-11-15",
        "code": "USD",
        "scheme": "RMS",
        "vintage": "RL18.1"
    },
    "edm": "CBHU_Automated",
    "eventRateSchemeId": 739,
    "exposureType": "PORTFOLIO",
    "id": "1",  # Portfolio ID
    "modelProfileId": 4418,
    "outputProfileId": 123,
    "treaties": ["1"],
    "tagIds": [1202],
    "globalAnalysisSettings": {
        "franchiseDeductible": False,
        "minLossThreshold": "1.00",
        "treatConstructionOccupancyAsUnknown": True,
        "numMaxLossEvent": 1
    },
    "jobName": "CBHU_Analysis"
}

# Returns 201/202 with workflow URL
```

**Demo Usage**: Runs analysis with different event rate schemes (2023 vs 2025 rates)

---

## MRI Import (Exposure Data Import)

### POST /riskmodeler/v1/storage

**When**: Beginning MRI import process

**Where**: `MRIImportManager.create_aws_bucket()`

**Why**: Create temporary S3 bucket for file staging

**How**:
```python
# No request body required

# Returns bucket URL in location header
# Response Headers
{
    "location": "https://api-euw1.rms-ppe.com/riskmodeler/v1/storage/12345"
}
```

**Demo Usage**: Creates bucket before uploading account/location files

### POST {bucket_url}/path

**When**: Getting credentials to upload files to S3

**Where**: `MRIImportManager.get_file_credentials()`

**Why**: Obtain temporary AWS credentials for secure file upload

**How**:
```python
# Base URL is the bucket URL from previous step
# Request Body
{
    "fileName": "Modeling_202503_Moodys_CBHU_Account.csv",
    "fileSize": 100,  # In KB
    "fileType": "account"  # or "location"
}

# Returns base64-encoded credentials
# Response Headers
{
    "location": "https://.../storage/12345/files/67890"  # Contains file_id
}

# Response Body
{
    "accessKeyId": "base64_encoded_access_key",
    "secretAccessKey": "base64_encoded_secret_key",
    "sessionToken": "base64_encoded_session_token",
    "s3Path": "base64_encoded_s3_path",
    "s3Region": "base64_encoded_region"
}
```

**Demo Usage**: Gets credentials for accounts CSV and locations CSV

### AWS S3 PUT (External Service)

**When**: Uploading exposure files to S3

**Where**: `MRIImportManager.upload_file_to_s3()`

**Why**: Transfer account/location CSV files to RMS cloud storage

**How**:
```python
# Uses boto3 SDK with temporary credentials
# S3 Key format: {s3_path}/{file_id}-{filename}
# Content-Type: "text/csv"

s3.put_object(
    Bucket=bucket_name,
    Key=s3_key,
    Body=file_contents,
    ContentType='text/csv'
)
```

**Demo Usage**: Uploads "Modeling_202503_Moodys_CBHU_Account.csv" and locations file

### POST /riskmodeler/v1/imports/createmapping/{bucket_id}

**When**: Uploading field mapping configuration

**Where**: `MRIImportManager.upload_mapping_file()`

**Why**: Define how CSV columns map to RMS data model fields

**How**:
```python
# URL path contains bucket ID
# Request Body: JSON mapping configuration from file

# Returns mapping file ID in location header
# Response Headers
{
    "location": "/riskmodeler/v1/imports/.../mapping/98765"
}
```

**Demo Usage**: Uploads "mapping.json"

### POST /riskmodeler/v1/imports

**When**: Executing the actual import workflow

**Where**: `MRIImportManager.execute_mri_import()`

**Why**: Process staged files and import into EDM portfolio

**How**:
```python
# Request Body
{
    "importType": "MRI",
    "bucketId": 12345,
    "dataSourceName": "CBHU_Automated",
    "accountsFileId": 67890,
    "locationsFileId": 67891,
    "mappingFileId": 98765,
    "delimiter": "COMMA",
    "skipLines": 1,  # Header row
    "currency": "USD",
    "portfolioId": 1,
    "appendLocations": False
}

# Returns workflow with import summary
# Workflow output includes: "Imported 2579 Accounts and 2579 Locations"
```

**Demo Usage**: Imports 2579 accounts and 2579 locations

---

## Treaty Operations

### GET /riskmodeler/v1/treaties

**When**: Listing treaties in an EDM

**Where**: `TreatyManager.get_treaties_by_edm()`

**Why**: Retrieve existing reinsurance treaty structures

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated",
    "limit": 100
}
```

### POST /riskmodeler/v1/treaties

**When**: Creating new reinsurance treaty

**Where**: `TreatyManager.create_treaty()`

**Why**: Define treaty structure for reinsurance analysis

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated"
}

# Request Body
{
    "treatyNumber": "CBHU_Treaty",
    "treatyName": "CBHU_Treaty",
    "treatyType": {"code": "WE", "name": "Working Excess"},
    "riskLimit": 3000000,
    "occurLimit": 9000000,
    "attachPt": 2000000,
    "cedant": {"id": 1, "name": "Cedant Name"},
    "effectDate": "2025-10-15T17:49:10.637Z",
    "expireDate": "2026-10-15T17:49:10.637Z",
    "currency": {"code": "USD", "name": "US Dollar"},
    "attachBasis": {"code": "LO", "name": "Losses Occurring"},
    "attachLevel": {"code": "L", "name": "Location"},
    "pcntCovered": 100,
    "pcntPlaced": 95,
    "pcntRiShare": 100,
    "pcntRetent": 100,
    "premium": 0,
    "numOfReinst": 99,
    "reinstCharge": 0,
    "aggregateLimit": 0,
    "aggregateDeductible": 0,
    "priority": 1,
    "retentAmt": "",
    "isValid": True,
    "userId1": "",
    "userId2": "",
    "maolAmount": "",
    "lobs": [{"id": 1, "name": "Property"}],
    "tagIds": []
}

# Returns treaty ID in location header
# Response Headers
{
    "location": "/riskmodeler/v1/treaties/1"
}
```

**Demo Usage**: Creates "Working Excess" treaty with $2M attachment, $3M risk limit

### POST /riskmodeler/v1/treaties/lob/batch

**When**: Assigning lines of business to treaty

**Where**: `TreatyManager.assign_lobs()`

**Why**: Associate LOBs with treaty for proper loss allocation

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated"
}

# Request Body - Array of batch operations
[
    {
        "body": "{'id': 1}",
        "method": "POST",
        "path": "/1/lob"  # treaty_id/lob
    },
    {
        "body": "{'id': 2}",
        "method": "POST",
        "path": "/1/lob"
    }
]
```

**Demo Usage**: Assigns all LOBs from EDM to treaty

### GET /riskmodeler/v1/domains/RMS/tablespace/System/entities/TreatyType/values

**When**: Retrieving treaty type reference data

**Where**: `TreatyManager.get_treaty_types_by_edm()`

**Why**: Get valid treaty types (e.g., "Working Excess", "Surplus Share")

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated",
    "fields": "code,name",
    "limit": 100
}
```

### GET /riskmodeler/v1/domains/RMS/tablespace/System/entities/AttachBasis/values

**When**: Retrieving attachment basis options

**Where**: `TreatyManager.get_treaty_attachment_bases_by_edm()`

**Why**: Get valid attachment bases (e.g., "Losses Occurring")

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated",
    "fields": "code,name",
    "limit": 100
}
```

### GET /riskmodeler/v1/domains/RMS/tablespace/System/entities/AttachLevel/values

**When**: Retrieving attachment level options

**Where**: `TreatyManager.get_treaty_attachment_levels_by_edm()`

**Why**: Get valid attachment levels (e.g., "Location", "Account")

**How**:
```python
# Query Parameters
{
    "datasource": "CBHU_Automated",
    "fields": "code,name",
    "limit": 100
}
```

---

## Analysis Operations

### GET /analysis-settings/modelprofiles

**When**: Looking up analysis profile by name

**Where**: `AnalysisManager.get_model_profile_by_name()`

**Why**: Get model profile ID needed for analysis execution

**How**:
```python
# Query Parameters
{
    "name": "DLM CBHU v23"
}

# Returns
{
    "count": 1,
    "items": [
        {"id": 4418, "name": "DLM CBHU v23"}
    ]
}
```

**Demo Usage**: Retrieves "DLM CBHU v23" profile

### GET /analysis-settings/outputprofiles

**When**: Looking up output profiles by name

**Where**: `AnalysisManager.get_output_profile_by_name()`

**Why**: Get output profile ID for specifying result types

**How**:
```python
# Query Parameters
{
    "name": "Patched Portfolio Level Only (EP, ELT, Stats)"
}

# Returns array with profile details
[
    {"id": 123, "name": "Patched Portfolio Level Only (EP, ELT, Stats)"}
]
```

**Demo Usage**: Retrieves "Patched Portfolio Level Only (EP, ELT, Stats)"

### GET /data-store/referencetables/eventratescheme

**When**: Looking up event rate schemes by name

**Where**: `AnalysisManager.get_event_rate_scheme_by_name()`

**Why**: Get event rate scheme ID for loss calculations

**How**:
```python
# Query Parameters
{
    "where": 'eventRateSchemeName="RMS 2025 Stochastic Event Rates"'
}

# Returns
{
    "count": 1,
    "items": [
        {"eventRateSchemeId": 739, "eventRateSchemeName": "RMS 2025 Stochastic Event Rates"}
    ]
}
```

**Demo Usage**: Retrieves "RMS 2023 Stochastic Event Rates" and "RMS 2025 Stochastic Event Rates"

### GET /riskmodeler/v2/analyses

**When**: Retrieving analysis details by ID

**Where**: `AnalysisManager.get_analysis_by_id()`

**Why**: Get analysis information after completion

**How**:
```python
# Query Parameters
{
    "q": "id=33188"
}
```

### GET /platform/riskdata/v1/analyses

**When**: Getting platform analyses by IDs

**Where**: `AnalysisManager.get_analyses_by_ids()`

**Why**: Retrieve analysis URIs needed for RDM export

**How**:
```python
# Query Parameters
{
    "filter": "appAnalysisId IN (33252,33253,33254)"
}

# Returns array with uri field for each analysis
[
    {"uri": "/platform/riskdata/v1/analyses/3117761"},
    {"uri": "/platform/riskdata/v1/analyses/3117782"}
]
```

### POST /riskmodeler/v2/analysis-groups

**When**: Creating analysis group for comparison/aggregation

**Where**: `AnalysisManager.create_analysis_group()`

**Why**: Group multiple analyses for combined results

**How**:
```python
# Request Body
{
    "analysisIds": [33252, 33253],
    "name": "CBHU Analysis Group",
    "currency": {
        "asOfDate": "2018-11-15",
        "code": "USD",
        "scheme": "RMS",
        "vintage": "RL18.1"
    },
    "simulateToPLT": True,
    "numOfSimulations": 50000,
    "propagateDetailedLosses": False,
    "reportingWindowStart": "01/01/2021",
    "simulationWindowStart": "01/01/2021",
    "simulationWindowEnd": "12/31/2021",
    "regionPerilSimulationSet": [],
    "description": ""
}

# Returns workflow creating grouped analysis
```

**Demo Usage**: Groups two analyses with different event rate schemes

---

## Tag Operations

### GET /data-store/referencedata/v1/tags

**When**: Searching for existing tags by name

**Where**: `ReferenceDataManager.get_tag_by_name()`

**Why**: Check if tag exists before creating, get tag ID

**How**:
```python
# Query Parameters
{
    "isActive": True,
    "filter": "TAGNAME = 'cbhu_analyses'"
}

# Returns array with tagId field
[
    {"tagId": 1202, "tagName": "cbhu_analyses"}
]
```

### POST /data-store/referencedata/v1/tags

**When**: Creating new tag for analysis organization

**Where**: `ReferenceDataManager.create_tag()`

**Why**: Create tags for grouping/filtering analyses

**How**:
```python
# Request Body
{
    "tagName": "cbhu_analyses"
}

# Returns tag ID in location header
# Response Headers
{
    "location": "/data-store/referencedata/v1/tags/1202"
}
```

**Demo Usage**: Tag "cbhu_analyses" used to organize multiple analysis jobs

---

## RDM (Results Data Mart) Export

### POST /platform/export/v1/jobs

**When**: Exporting analysis results to RDM for downstream consumption

**Where**: `RDMManager.export_analyses_to_rdm()`

**Why**: Make results available in RDM database for reporting/analytics

**How**:
```python
# Request Headers
{
    "Authorization": "api_key",
    "x-rms-resource-group-id": "resource_group_id_from_env"
}

# Request Body
{
    "exportType": "RDM_DATABRIDGE",
    "resourceType": "analyses",
    "resourceUris": [
        "/platform/riskdata/v1/analyses/3117782",
        "/platform/riskdata/v1/analyses/3117761"
    ],
    "settings": {
        "serverId": 88094,
        "rdmName": "CBHU_Automated"
    }
}

# Returns workflow that exports analyses to RDM
```

**Demo Usage**: Exports grouped analysis and individual analyses to "CBHU_Automated" RDM

---

## Reference Data

### GET /riskmodeler/v1/domains/Client/tablespace/UserConfig/entities/currency/values

**When**: Retrieving currency options

**Where**: `ReferenceDataManager.get_currencies()`

**Why**: Get currency code/name for treaty and analysis setup

**How**:
```python
# Query Parameters
{
    "fields": "code,name"
}

# Returns
{
    "entityItems": {
        "values": [
            {"code": "USD", "name": "US Dollar"}
        ]
    }
}
```

**Demo Usage**: Retrieves "US Dollar" for treaty creation

---

## Common Patterns

### Authentication
All requests use `Authorization` header with API key from environment:
```python
headers = {
    'Authorization': os.environ.get('RISK_MODELER_API_KEY')
}
```

### Base URL
Configurable via environment variable:
```python
base_url = os.environ.get('RISK_MODELER_BASE_URL', 'https://api-euw1.rms-ppe.com')
```

### Workflow Pattern
Long-running operations follow this pattern:
1. Submit request (POST/DELETE)
2. Receive 201/202 response with `location` header
3. Poll workflow URL until status is `FINISHED`, `FAILED`, or `CANCELLED`
4. Check workflow output for results

### Error Handling
- HTTP errors are enriched with server response body for debugging
- Retry logic: 5 retries with exponential backoff for 429, 500, 502, 503, 504 errors
- Timeouts: 200s default for requests, 600s default for workflow polling

### Resource IDs
Resource IDs are typically extracted from the `location` header after creation:
```python
resource_id = response.headers['location'].split('/')[-1]
```

### Session Management
Uses requests.Session with:
- Connection pooling via HTTPAdapter
- Automatic retry configuration
- Persistent headers across requests

---

## Configuration

Required environment variables:
- `RISK_MODELER_BASE_URL`: API base URL (default: https://api-euw1.rms-ppe.com)
- `RISK_MODELER_API_KEY`: API authentication key
- `RISK_MODELER_RESOURCE_GROUP_ID`: Resource group ID (required for RDM export)

## Example Workflow

See `IRP_Integration_Demo.ipynb` for a complete end-to-end workflow demonstrating:
1. EDM creation
2. Portfolio creation
3. MRI import (accounts + locations)
4. Treaty creation and LOB assignment
5. EDM version upgrade
6. GeoHaz processing (geocoding + hazard)
7. Single analysis execution
8. Batch analysis submission with different parameters
9. Analysis grouping
10. RDM export
