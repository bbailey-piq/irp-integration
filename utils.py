"""
Utility functions for IRP Integration module.

Provides common helper functions for response parsing, data extraction,
and reference data lookup operations.
"""

import base64
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import requests
from .exceptions import IRPAPIError, IRPReferenceDataError


def get_workspace_root() -> Path:
    """
    Get workspace root directory, working in both VS Code and JupyterLab.

    Returns:
        Path to workspace directory
    """
    cwd = Path.cwd()

    # If we're in workspace or below it, navigate up to workspace
    if 'workspace' in cwd.parts:
        workspace_index = cwd.parts.index('workspace')
        return Path(*cwd.parts[:workspace_index + 1])

    # Otherwise check if workspace exists as subdirectory
    if (cwd / 'workspace').exists():
        return cwd / 'workspace'

    raise IRPAPIError(f"Cannot find workspace directory from {cwd}")


def get_cycle_file_directories() -> Dict[str, str]:
    """
    Get file directories for the active cycle.

    Determines the appropriate directories for data files and mapping files
    based on whether there's an active cycle.

    Returns:
        Dict with keys:
            - 'data': Directory for CSV data files (accounts, locations)
            - 'mapping': Directory for mapping files (mapping.json)

    Directory structure:
        - Active cycle: {cycle_name}/files/data and {cycle_name}/files/mapping
        - No active cycle: _Tools/files/working_files and _Tools/files/mapping

    Example:
        ```python
        dirs = get_cycle_file_directories()
        accounts_path = os.path.join(dirs['data'], 'accounts.csv')
        mapping_path = os.path.join(dirs['mapping'], 'mapping.json')
        ```
    """
    workspace_root = get_workspace_root()

    try:
        from helpers.cycle import get_active_cycle
        active_cycle = get_active_cycle()

        if active_cycle:
            # Use active cycle's directories
            cycle_name = active_cycle['cycle_name']
            return {
                'data': str(workspace_root / "workflows" / f"Active_{cycle_name}" / "files" / "data"),
                'mapping': str(workspace_root / "workflows" / f"Active_{cycle_name}" / "files" / "mapping")
            }
        else:
            # No active cycle, use _Tools directories
            return {
                'data': str(workspace_root / "workflows" / "_Tools" / "files" / "working_files"),
                'mapping': str(workspace_root / "workflows" / "_Tools" / "files" / "mapping")
            }
    except Exception:
        # Fallback to _Tools if cycle lookup fails
        return {
            'data': str(workspace_root / "workflows" / "_Tools" / "files" / "working_files"),
            'mapping': str(workspace_root / "workflows" / "_Tools" / "files" / "mapping")
        }


def get_location_header(
    response: requests.Response,
    error_context: str = "response"
) -> str:
    """
    Get Location header from response.

    Args:
        response: HTTP response object

    Returns:
        Location header value, or empty string if not found
    """
    if 'location' not in response.headers:
        raise IRPAPIError(
            f"Location header missing from {error_context}"
        )
    return response.headers.get('location', '')


def extract_id_from_location_header(
    response: requests.Response,
    error_context: str = "response",
) -> str:
    """
    Extract ID from Location header in HTTP response.

    Args:
        response: HTTP response object
        error_context: Context description for error message

    Returns:
        Extracted ID string

    Raises:
        IRPAPIError: If Location header is missing
    """
    location = get_location_header(response, error_context)
    resource_id = location.split('/')[-1]
    if not resource_id:
        raise IRPAPIError(
            f"Could not extract ID from Location header: {location}"
        )
    return resource_id


def decode_base64_field(encoded_value: str, field_name: str) -> str:
    """
    Decode a base64-encoded field value.

    Args:
        encoded_value: Base64-encoded string
        field_name: Field name for error message

    Returns:
        Decoded string

    Raises:
        IRPAPIError: If decoding fails
    """
    try:
        return base64.b64decode(encoded_value).decode("utf-8")
    except Exception as e:
        raise IRPAPIError(
            f"Failed to decode base64 field '{field_name}': {e}"
        )
    

def decode_mri_credentials(response_json: Dict[str, Any]) -> Dict[str, str]:
    """
    Decode base64 credentials from MRI import file credentials response.

    Args:
        response_json: Response JSON containing encoded credentials

    Returns:
        Dict with decoded credential fields

    Raises:
        IRPAPIError: If required fields missing or decoding fails
    """
    required_fields = ['accessKeyId', 'secretAccessKey', 'sessionToken', 's3Path', 's3Region']
    missing = [f for f in required_fields if f not in response_json]
    if missing:
        raise IRPAPIError(
            f"MRI credentials response missing fields: {', '.join(missing)}"
        )

    try:
        return {
            'aws_access_key_id': decode_base64_field(response_json['accessKeyId'], 'accessKeyId'),
            'aws_secret_access_key': decode_base64_field(response_json['secretAccessKey'], 'secretAccessKey'),
            'aws_session_token': decode_base64_field(response_json['sessionToken'], 'sessionToken'),
            's3_path': decode_base64_field(response_json['s3Path'], 's3Path'),
            's3_region': decode_base64_field(response_json['s3Region'], 's3Region')
        }
    except IRPAPIError:
        raise
    except Exception as e:
        raise IRPAPIError(f"Failed to decode MRI credentials: {e}")


def decode_presign_params(presign_params: Dict[str, Any]) -> Dict[str, str]:
    """
    Decode base64 credentials from MRI import file credentials response.

    Args:
        response_json: Response JSON containing encoded credentials

    Returns:
        Dict with decoded credential fields

    Raises:
        IRPAPIError: If required fields missing or decoding fails
    """
    required_fields = ['accessKeyId', 'secretAccessKey', 'sessionToken', 'path', 'region']
    missing = [f for f in required_fields if f not in presign_params]
    if missing:
        raise IRPAPIError(
            f"Presign params response missing fields: {', '.join(missing)}"
        )

    try:
        return {
            'aws_access_key_id': decode_base64_field(presign_params['accessKeyId'], 'accessKeyId'),
            'aws_secret_access_key': decode_base64_field(presign_params['secretAccessKey'], 'secretAccessKey'),
            'aws_session_token': decode_base64_field(presign_params['sessionToken'], 'sessionToken'),
            's3_path': decode_base64_field(presign_params['path'], 'path'),
            's3_region': decode_base64_field(presign_params['region'], 'region')
        }
    except IRPAPIError:
        raise
    except Exception as e:
        raise IRPAPIError(f"Failed to decode MRI credentials: {e}")


def find_reference_data_by_name(
    data_list: List[Dict[str, Any]],
    target_name: str,
    name_field: str = "name",
    data_type: str = "reference data"
) -> Dict[str, Any]:
    """
    Find reference data item by name from a list.

    Args:
        data_list: List of reference data dicts
        target_name: Name to search for
        name_field: Field name containing the name (default: "name")
        data_type: Type description for error message

    Returns:
        Matching reference data dict

    Raises:
        IRPReferenceDataError: If item not found or list is empty
    """
    if not data_list:
        raise IRPReferenceDataError(
            f"No {data_type} available to search"
        )

    match = next(
        (item for item in data_list if item.get(name_field) == target_name),
        None
    )

    if match is None:
        available_names = [item.get(name_field, '<unnamed>') for item in data_list[:5]]
        names_str = ', '.join(available_names)
        if len(data_list) > 5:
            names_str += f", ... ({len(data_list) - 5} more)"

        raise IRPReferenceDataError(
            f"{data_type} '{target_name}' not found. Available: {names_str}"
        )

    return match


def extract_analysis_id_from_workflow_response(workflow: Dict[str, Any]) -> Optional[str]:
    """
    Extract analysis ID from workflow response.

    Args:
        workflow: Workflow response dict

    Returns:
        Analysis ID if found, None otherwise

    Raises:
        IRPAPIError: If required fields are missing from workflow response
    """
    try:
        return workflow['output']['analysisId']
    except (KeyError, TypeError) as e:
        raise IRPAPIError(
            f"Missing required field in workflow response: {e}"
        ) from e
