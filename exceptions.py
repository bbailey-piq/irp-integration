"""
Custom exception classes for IRP Integration module.

These exceptions provide clear, structured error handling for different
failure scenarios when interacting with Moody's Risk Modeler API.
"""


class IRPIntegrationError(Exception):
    """Base exception for all IRP integration errors."""
    pass


class IRPAPIError(IRPIntegrationError):
    """
    API request or response errors.

    Raised when HTTP requests fail, responses are malformed,
    or API returns unexpected status codes.
    """
    pass


class IRPValidationError(IRPIntegrationError):
    """
    Input validation errors.

    Raised when method parameters fail validation checks
    (e.g., empty strings, invalid IDs, missing files).
    """
    pass


class IRPWorkflowError(IRPIntegrationError):
    """
    Workflow execution errors.

    Raised when workflows fail to complete successfully,
    timeout, or return error status.
    """
    pass


class IRPReferenceDataError(IRPIntegrationError):
    """
    Reference data lookup errors.

    Raised when required reference data (treaty types, currencies, etc.)
    cannot be found or retrieved.
    """
    pass


class IRPFileError(IRPIntegrationError):
    """
    File operation errors.

    Raised when file operations fail (file not found, invalid format,
    upload errors, etc.).
    """
    pass


class IRPJobError(IRPIntegrationError):
    """
    Job management errors.

    Raised when job submission, status retrieval,
    or result fetching encounters issues.
    """
    pass
