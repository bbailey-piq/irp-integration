"""
Utility functions for IRP Integration module.

Provides common helper functions for response parsing, data extraction,
and reference data lookup operations.
"""

import base64
from typing import Dict, List, Any, Optional
import requests
from .exceptions import IRPAPIError, IRPReferenceDataError


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
