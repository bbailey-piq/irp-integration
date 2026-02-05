# API Endpoint Constants

# Workflow / Job endpoints
GET_WORKFLOWS = '/riskmodeler/v1/workflows'
GET_WORKFLOW_BY_ID = '/riskmodeler/v1/workflows/{workflow_id}'
GET_RISK_DATA_JOB_BY_ID = '/platform/riskdata/v1/jobs/{job_id}'
SEARCH_RISK_DATA_JOBS = '/platform/riskdata/v1/jobs'

# Workflow statuses
WORKFLOW_COMPLETED_STATUSES = ['FINISHED', 'FAILED', 'CANCELLED'] # https://developer.rms.com/risk-modeler/docs/workflow-engine#polling-workflow-job-and-operation-statuses
WORKFLOW_IN_PROGRESS_STATUSES = ['QUEUED', 'PENDING', 'RUNNING', 'CANCEL_REQUESTED', 'CANCELLING']

# EDM/Datasource endpoints
SEARCH_DATABASE_SERVERS = '/platform/riskdata/v1/dataservers'
SEARCH_EXPOSURE_SETS = '/platform/riskdata/v1/exposuresets'
CREATE_EXPOSURE_SET = '/platform/riskdata/v1/exposuresets'
SEARCH_EDMS = '/platform/riskdata/v1/exposures'
CREATE_EDM = '/platform/riskdata/v1/exposuresets/{exposureSetId}/exposures'
UPGRADE_EDM_DATA_VERSION = '/platform/riskdata/v1/exposures/{exposureId}/data-upgrade'
DELETE_EDM = '/platform/riskdata/v1/exposures/{exposureId}'
GET_CEDANTS = '/platform/riskdata/v1/exposures/{exposureId}/cedants'
GET_LOBS = '/platform/riskdata/v1/exposures/{exposureId}/lobs'

# MRI Import Endpoints
CREATE_IMPORT_FOLDER = '/platform/import/v1/folders'
CREATE_IMPORT_JOB = '/platform/import/v1/jobs'
CREATE_AWS_BUCKET = '/riskmodeler/v1/storage'
CREATE_MAPPING = '/riskmodeler/v1/imports/createmapping/{bucket_id}'
EXECUTE_IMPORT = '/riskmodeler/v1/imports'

# Portfolio endpoints
SEARCH_PORTFOLIOS = '/platform/riskdata/v1/exposures/{exposureId}/portfolios'
SEARCH_ACCOUNTS_BY_PORTFOLIO = '/platform/riskdata/v1/exposures/{exposureId}/portfolios/{id}/accounts'
CREATE_PORTFOLIO = '/platform/riskdata/v1/exposures/{exposureId}/portfolios'
GET_PORTFOLIO_BY_ID = '/platform/riskdata/v1/exposures/{exposureId}/portfolios/{id}'
GEOHAZ_PORTFOLIO = '/platform/geohaz/v1/jobs'
GET_GEOHAZ_JOB = '/platform/geohaz/v1/jobs/{jobId}'

# Treaty endpoints
SEARCH_TREATIES = 'platform/riskdata/v1/exposures/{exposureId}/treaties'
CREATE_TREATY = '/platform/riskdata/v1/exposures/{exposureId}/treaties'
TREATY_TYPES = {
    'Catastrophe': 'CATA',
    'Corporate Catastrophe': 'CORP',
    'Non-Catastrophe': 'NCAT',
    'Quota Share': 'QUOT',
    'Stop Loss': 'STOP',
    'Surplus Share': 'SURP',
    'Working Excess': 'WORK'
}
TREATY_ATTACHMENT_BASES = {
    'Losses Occurring': 'L',
    'Risks Attaching': 'R'
}
TREATY_ATTACHMENT_LEVELS = {
    'Account': 'ACCT',
    'Portfolio': 'PORT',
    'Policy': 'POL',
    'Location': 'LOC'
}
CREATE_TREATY_LOB = '/platform/riskdata/v1/exposures/{exposureId}/treaties/{id}/lob'

# Analysis endpoints
SEARCH_ANALYSIS_JOBS = '/platform/model/v1/jobs'
CREATE_ANALYSIS_JOB = '/platform/model/v1/jobs'
GET_ANALYSIS_JOB = '/platform/model/v1/jobs/{jobId}'
SEARCH_ANALYSIS_RESULTS = '/platform/riskdata/v1/analyses'
GET_ANALYSIS_RESULT = '/platform/riskdata/v1/analyses/{analysisId}'
CREATE_ANALYSIS_GROUP = '/platform/grouping/v1/jobs'
GET_ANALYSIS_GROUPING_JOB = '/platform/grouping/v1/jobs/{jobId}'
DELETE_ANALYSIS = '/platform/riskdata/v1/analyses/{analysisId}'

# Analysis results endpoints (ELT, EP, Stats, PLT)
GET_ANALYSIS_ELT = '/platform/riskdata/v1/analyses/{analysisId}/elt'
GET_ANALYSIS_EP = '/platform/riskdata/v1/analyses/{analysisId}/ep'
GET_ANALYSIS_STATS = '/platform/riskdata/v1/analyses/{analysisId}/stats'
GET_ANALYSIS_PLT = '/platform/riskdata/v1/analyses/{analysisId}/plt'
GET_ANALYSIS_REGIONS = '/platform/riskdata/v1/analyses/{analysisId}/regions'

# Perspective codes for analysis results
PERSPECTIVE_CODES = ['GR', 'GU', 'RL']  # Gross, Ground-Up, Reinsurance Layer

GET_MODEL_PROFILES = '/analysis-settings/modelprofiles'
GET_OUTPUT_PROFILES = '/analysis-settings/outputprofiles'
GET_EVENT_RATE_SCHEME = '/data-store/referencetables/eventratescheme'

# Tag endpoints
GET_TAGS = '/platform/referencedata/v1/tags'
CREATE_TAG = '/platform/referencedata/v1/tags'

# RDM endpoints
CREATE_RDM_EXPORT_JOB = '/platform/export/v1/jobs'
GET_EXPORT_JOB = '/platform/export/v1/jobs/{jobId}'
SEARCH_DATABASES = '/platform/riskdata/v1/dataservers/{serverId}/databases'
DELETE_RDM = '/databridge/v1/sql-instances/{instanceName}/databases/{rdmName}'
GET_DATABRIDGE_JOB = '/databridge/v1/jobs/{jobId}'
UPDATE_GROUP_ACCESS = '/databridge/v1/sql-instances/{instanceName}/Databases/{databaseName}'

# Currency endpoints
SEARCH_CURRENCIES = '/data-store/referencetables/currency'
SEARCH_CURRENCY_SCHEME_VINTAGES = '/data-store/referencetables/currencyschemevintage'

# Simulation/Model reference data endpoints
SEARCH_SIMULATION_SETS = '/data-store/referenceTables/SimulationSet'
SEARCH_PET_METADATA = '/data-store/referenceTables/PETMetadata'
SEARCH_SOFTWARE_MODEL_VERSION_MAP = '/data-store/referenceTables/SoftwareModelVersionMap'
