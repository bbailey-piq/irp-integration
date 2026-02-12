"""
Client for IRP Integration API requests.

Handles HTTP requests with retry logic, workflow polling,
and batch workflow execution.
"""

import json
import logging
import requests
import time
import os
from typing import Dict, List, Any, Optional, Union
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from .constants import  GET_WORKFLOWS, WORKFLOW_COMPLETED_STATUSES, WORKFLOW_IN_PROGRESS_STATUSES, GET_WORKFLOW_BY_ID
from .exceptions import IRPAPIError, IRPJobError, IRPWorkflowError
from .validators import validate_list_not_empty, validate_non_empty_string, validate_positive_int
from .utils import get_location_header

logger = logging.getLogger(__name__)


class Client:

    """Client for Moody's Risk Modeler API."""

    def __init__(self) -> None:
        """
        Initialize API client with credentials from environment.

        Environment variables:
            RISK_MODELER_BASE_URL: API base URL
            RISK_MODELER_API_KEY: API authentication key
            RISK_MODELER_RESOURCE_GROUP_ID: Resource group ID

        Raises:
            IRPAPIError: If any required environment variable is missing
        """
        base_url = os.environ.get('RISK_MODELER_BASE_URL')
        api_key = os.environ.get('RISK_MODELER_API_KEY')
        resource_group_id = os.environ.get('RISK_MODELER_RESOURCE_GROUP_ID')

        if not base_url:
            raise IRPAPIError("Missing required environment variable: RISK_MODELER_BASE_URL")
        if not api_key:
            raise IRPAPIError("Missing required environment variable: RISK_MODELER_API_KEY")
        if not resource_group_id:
            raise IRPAPIError("Missing required environment variable: RISK_MODELER_RESOURCE_GROUP_ID")

        self.base_url = base_url
        self.timeout = 200

        session = requests.Session()
        session.headers.update({
            'Authorization': api_key,
            'x-rms-resource-group-id': resource_group_id,
        })

        retry = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PUT", "PATCH", "DELETE"),
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        self.session = session

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
    ) -> requests.Response:
        """
        Make HTTP request to API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            path: API path (e.g., '/api/v1/datasources')
            full_url: Full URL (overrides path/base_url if provided)
            base_url: Base URL (overrides default if provided)
            params: Query parameters
            json: JSON request body
            headers: Additional headers
            timeout: Request timeout in seconds
            stream: Enable streaming response

        Returns:
            HTTP response object

        Raises:
            IRPAPIError: If HTTP request fails
        """
        validate_non_empty_string(method, "method")

        if full_url:
            url = full_url
        else:
            if base_url:
                url = f"{base_url}/{path.lstrip('/')}"
            else:
                url = f"{self.base_url}/{path.lstrip('/')}"

        logger.debug("%s %s", method, url)

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                headers=headers,
                timeout=timeout or self.timeout,
                stream=stream,
            )
            response.raise_for_status()
        except requests.HTTPError as e:
            status_code = response.status_code
            # Extract only a safe error-message field — never log the full body
            safe_msg = ""
            try:
                body = response.json()
                server_msg = body.get("message") or body.get("error") or ""
                if server_msg:
                    safe_msg = f" | server: {str(server_msg)[:200]}"
            except Exception:
                pass
            logger.error(
                "HTTP request failed: %s %s (status %s)%s",
                method, url, status_code, safe_msg,
            )
            raise IRPAPIError(
                f"HTTP request failed: {method} {url} (status {status_code}){safe_msg}"
            ) from e
        except requests.RequestException as e:
            logger.error("Request error: %s %s — %s", method, url, e)
            raise IRPAPIError(f"Request error: {e}") from e

        logger.debug("%s %s — %s", method, url, response.status_code)
        return response


    def get_workflow(self, workflow_id: int) -> Dict[str, Any]:
        """
        Retrieve workflow status by workflow ID.

        Args:
            workflow_id: Workflow ID

        Returns:
            Dict containing workflow status details

        Raises:
            IRPValidationError: If workflow_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(workflow_id, "workflow_id")

        try:
            response = self.request('GET', GET_WORKFLOW_BY_ID.format(workflow_id=workflow_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get workflow status for workflow ID {workflow_id}: {e}")


    def poll_workflow_to_completion(
        self,
        workflow_id: int,
        interval: int = 10,
        timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll workflow until completion or timeout.

        Args:
            workflow_id: Workflow ID
            interval: Polling interval in seconds
            timeout: Maximum timeout in seconds
        """
        validate_positive_int(workflow_id, "workflow_id")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            logger.info("Polling workflow ID %s", workflow_id)
            job_data = self.get_workflow(workflow_id)
            try:
                status = job_data['status']
                progress = job_data['progress']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing 'status' or 'progress' in job response for workflow ID {workflow_id}: {e}"
                ) from e
            logger.info("Workflow %s status: %s; progress: %s", workflow_id, status, progress)
            if status in WORKFLOW_COMPLETED_STATUSES:
                return job_data

            if time.time() - start > timeout:
                logger.error("Workflow %s timed out after %s seconds. Last status: %s", workflow_id, timeout, status)
                raise IRPJobError(
                    f"Risk data workflow ID {workflow_id} did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)


    def poll_workflow(
        self,
        workflow_url: str,
        interval: int = 10,
        timeout: int = 600000
    ) -> requests.Response:
        """
        Poll workflow until completion or timeout.

        Args:
            workflow_url: Full URL to workflow endpoint
            interval: Polling interval in seconds
            timeout: Maximum timeout in seconds

        Returns:
            Final workflow response

        Raises:
            IRPValidationError: If workflow_url is invalid
            IRPWorkflowError: If workflow times out
        """
        validate_non_empty_string(workflow_url, "workflow_url")
        
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            logger.info("Polling workflow URL %s", workflow_url)
            response = self.request('GET', '', full_url=workflow_url)
            workflow_data = response.json()
            status = workflow_data.get('status', '')
            progress = workflow_data.get('progress', '')
            logger.info("Workflow status: %s; progress: %s", status, progress)

            if status in WORKFLOW_COMPLETED_STATUSES:
                return response

            if time.time() - start > timeout:
                logger.error("Workflow timed out after %s seconds. Last status: %s", timeout, status)
                raise IRPWorkflowError(
                    f"Workflow did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)

    def poll_workflow_batch_to_completion(
        self,
        workflow_ids: List[int],
        interval: int = 20,
        timeout: int = 600000
    ) -> requests.Response:
        """
        Poll multiple workflows until all complete or timeout.

        Args:
            workflow_ids: List of workflow IDs to poll
            interval: Polling interval in seconds
            timeout: Maximum timeout in seconds

        Returns:
            Response with all workflows combined

        Raises:
            IRPValidationError: If inputs are invalid
            IRPWorkflowError: If workflows time out
        """
        validate_list_not_empty(workflow_ids, "workflow_ids")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            logger.info("Polling batch workflow IDs: %s", ",".join(str(item) for item in workflow_ids))

            # Fetch all workflows across all pages
            all_workflows = []
            offset = 0
            limit = 100

            while True:
                params = {
                    'ids': ','.join(str(item) for item in workflow_ids),
                    'limit': limit,
                    'offset': offset
                }
                response = self.request('GET', GET_WORKFLOWS, params=params)
                response_data = response.json()

                try:
                    total_match_count = response_data['totalMatchCount']
                except (KeyError, TypeError) as e:
                    raise IRPAPIError(
                        f"Missing 'totalMatchCount' in workflow batch response: {e}"
                    ) from e

                workflows = response_data.get('workflows', [])

                all_workflows.extend(workflows)

                # Check if we've fetched all workflows
                if len(all_workflows) >= total_match_count:
                    break

                # Move to next page
                offset += limit

            # Check if all workflows are completed
            all_completed = True
            for workflow in all_workflows:
                status = workflow.get('status', '')
                if status in WORKFLOW_IN_PROGRESS_STATUSES:
                    all_completed = False
                    break

            if all_completed:
                # Return the last response but with all workflows combined
                response_data['workflows'] = all_workflows
                return response

            if time.time() - start > timeout:
                logger.error("Batch workflows timed out after %s seconds", timeout)
                raise IRPWorkflowError(
                    f"Batch workflows did not complete within {timeout} seconds"
                )
            time.sleep(interval)

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
    ) -> requests.Response:
        """
        Execute workflow: submit request and poll until completion.

        This is a convenience method that combines request submission
        with automatic workflow polling.

        Args:
            method: HTTP method (POST, DELETE, etc.)
            path: API path
            params: Query parameters
            json: JSON request body
            headers: Additional headers
            timeout: Request timeout in seconds
            stream: Enable streaming response

        Returns:
            Final workflow response after completion

        Raises:
            IRPAPIError: If request fails
            IRPWorkflowError: If workflow times out
        """
        logger.info("Submitting workflow request...")
        response = self.request(
            method, path,
            params=params,
            json=json,
            headers=headers,
            timeout=timeout,
            stream=stream
        )

        if response.status_code not in (201, 202):
            return response

        workflow_url = get_location_header(response)
        if not workflow_url:
            raise IRPAPIError(
                "Workflow submission succeeded but Location header is missing"
            )

        return self.poll_workflow(workflow_url)