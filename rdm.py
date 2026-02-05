"""
RDM (Risk Data Model) export operations.

Handles exporting analysis results to RDM via databridge.
"""

import os
import time
from typing import Dict, List, Any, Optional

from helpers.irp_integration.utils import extract_id_from_location_header
from .client import Client
from .constants import CREATE_RDM_EXPORT_JOB, GET_EXPORT_JOB, SEARCH_DATABASES, WORKFLOW_COMPLETED_STATUSES, DELETE_RDM, GET_DATABRIDGE_JOB, UPDATE_GROUP_ACCESS
from .exceptions import IRPAPIError, IRPJobError
from .validators import validate_non_empty_string, validate_list_not_empty, validate_positive_int

class RDMManager:
    """Manager for RDM export operations."""

    def __init__(self, client: Client, analysis_manager: Optional[Any] = None, edm_manager: Optional[Any] = None) -> None:
        """
        Initialize RDM manager.

        Args:
            client: IRP API client instance
            analysis_manager: Optional AnalysisManager instance
        """
        self.client = client
        self._analysis_manager = analysis_manager
        self._edm_manager = edm_manager

    @property
    def analysis_manager(self):
        """Lazy-loaded analysis manager to avoid circular imports."""
        if self._analysis_manager is None:
            from .analysis import AnalysisManager
            self._analysis_manager = AnalysisManager(self.client)
        return self._analysis_manager
    
    @property
    def edm_manager(self):
        """Lazy-loaded edm manager to avoid circular imports."""
        if self._edm_manager is None:
            from .edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager


    def export_analyses_to_rdm(
            self,
            server_name: str,
            rdm_name: str,
            analysis_names: List[str],
            skip_missing: bool = False
    ) -> Dict[str, Any]:
        """
        Export multiple analyses to RDM (Risk Data Model) and poll to completion.

        Args:
            server_name: Database server name
            rdm_name: Name for the RDM
            analysis_names: List of analysis names to export
            skip_missing: If True, skip missing analyses instead of raising an error

        Returns:
            Dict containing final export job status

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If export fails or analyses not found
        """
        result = self.submit_rdm_export_job(
            server_name=server_name,
            rdm_name=rdm_name,
            analysis_names=analysis_names,
            skip_missing=skip_missing
        )

        # If job was skipped (all items missing), return the skip result
        if result.get('skipped'):
            return result

        rdm_export_job_id = result['job_id']
        return self.poll_rdm_export_job_to_completion(rdm_export_job_id)


    def submit_rdm_export_job(
            self,
            server_name: str,
            rdm_name: str,
            analysis_names: List[str],
            database_id: Optional[int] = None,
            analysis_edm_map: Optional[Dict[str, str]] = None,
            group_names: Optional[set] = None,
            skip_missing: bool = True
    ) -> Dict[str, Any]:
        """
        Submit RDM export job.

        Performs validation (server lookup, RDM existence check, analysis URI
        resolution) and submits the export job.

        Args:
            server_name: Database server name
            rdm_name: Name for the RDM
            analysis_names: List of analysis and group names to export
            database_id: Optional database ID (for appending to existing RDM)
            analysis_edm_map: Optional mapping of analysis names to EDM names.
                Used to look up analyses by name + EDM (since analysis names are only
                unique within an EDM). If not provided, lookups use name only.
            group_names: Optional set of known group names. Items in this set are
                looked up as groups (by name only), all others are looked up as
                analyses (by name + EDM if mapping provided).
            skip_missing: If True (default), skip analyses/groups that don't exist
                instead of raising an error. If all items are missing, returns
                a result with job_id=None and skipped=True.

        Returns:
            Dict containing:
                - job_id: RDM export job ID (int), or None if skipped
                - skipped: True if job was skipped (all items missing)
                - skipped_items: List of item names that were not found and skipped
                - included_items: List of item names that were found and included
                - skip_reason: Reason for skipping (if skipped=True)

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If job submission fails, or if skip_missing=False and items not found
        """
        validate_non_empty_string(server_name, "server_name")
        validate_non_empty_string(rdm_name, "rdm_name")
        validate_list_not_empty(analysis_names, "analysis_names")

        # Initialize defaults
        if analysis_edm_map is None:
            analysis_edm_map = {}
        if group_names is None:
            group_names = set()

        # Look up server ID
        database_servers = self.edm_manager.search_database_servers(filter=f"serverName=\"{server_name}\"")
        try:
            server_id = database_servers[0]['serverId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract server ID for server '{server_name}': {e}"
            ) from e

        # Check if RDM with same name already exists (only for new RDM creation, not appending)
        if not database_id:
            existing_rdms = self.search_databases(
                server_name=server_name,
                filter=f"databaseName LIKE \"{rdm_name}*\""
            )
            if existing_rdms:
                existing_name = existing_rdms[0].get('databaseName', rdm_name)
                raise IRPAPIError(
                    f"RDM with name '{rdm_name}' already exists on server '{server_name}' "
                    f"(found: '{existing_name}'). Please use a different RDM name or delete "
                    f"the existing RDM first."
                )

        # Resolve analysis/group names to URIs, tracking skipped items and frameworks
        resource_uris = []
        skipped_items = []
        included_items = []
        include_export_hd_losses = False  # Only set for PLT analyses/groups

        for name in analysis_names:
            # Determine if this is a group name or an analysis name
            if name in group_names:
                # Group names are globally unique - search by name only
                # Must filter by engineType = "Group" to find groups specifically
                analysis_response = self.analysis_manager.search_analyses(filter=f'analysisName = "{name}" AND engineType = "Group"')
                if len(analysis_response) == 0:
                    if skip_missing:
                        skipped_items.append(name)
                        continue
                    raise IRPAPIError(f"Group with this name does not exist: {name}")
                if len(analysis_response) > 1:
                    raise IRPAPIError(f"Duplicate groups exist with name: {name}")
            else:
                # Analysis names - search by name + EDM if mapping provided
                edm_name = analysis_edm_map.get(name)
                if edm_name:
                    filter_str = f"analysisName = \"{name}\" AND exposureName = \"{edm_name}\""
                    analysis_response = self.analysis_manager.search_analyses(filter=filter_str)
                    if len(analysis_response) == 0:
                        if skip_missing:
                            skipped_items.append(name)
                            continue
                        raise IRPAPIError(f"Analysis '{name}' not found for EDM '{edm_name}'")
                    if len(analysis_response) > 1:
                        raise IRPAPIError(f"Multiple analyses found with name '{name}' for EDM '{edm_name}'")
                else:
                    # Fallback to name-only search (legacy behavior)
                    analysis_response = self.analysis_manager.search_analyses(filter=f"analysisName = \"{name}\"")
                    if len(analysis_response) == 0:
                        if skip_missing:
                            skipped_items.append(name)
                            continue
                        raise IRPAPIError(f"Analysis with this name does not exist: {name}")
                    if len(analysis_response) > 1:
                        raise IRPAPIError(f"Duplicate analyses exist with name: {name}.")

            try:
                resource_uris.append(analysis_response[0]['uri'])
                included_items.append(name)

                # Check analysisFramework to determine if exportHdLossesAs is needed
                # Groups in Moody's are stored as analyses, so they also have analysisFramework
                analysis_framework = analysis_response[0].get('analysisFramework', 'ELT')
                if analysis_framework == 'PLT':
                    include_export_hd_losses = True
            except (KeyError, IndexError, TypeError) as e:
                raise IRPAPIError(
                    f"Failed to extract URI for '{name}': {e}"
                ) from e

        # If all items were skipped, return a skip result instead of submitting
        if not resource_uris:
            return {
                'job_id': None,
                'skipped': True,
                'skipped_items': skipped_items,
                'included_items': [],
                'skip_reason': f"All {len(skipped_items)} analyses/groups were not found"
            }

        # Build settings - use databaseId if provided (appending to existing RDM),
        # otherwise use rdmName (creating new RDM)
        if database_id:
            settings = {
                "databaseId": database_id,
                "serverId": server_id,
            }
        else:
            settings = {
                "rdmName": rdm_name,
                "serverId": server_id,
            }

        # Only include exportHdLossesAs for PLT analyses/groups
        if include_export_hd_losses:
            settings["exportHdLossesAs"] = "PLT"

        data = {
            "exportType": "RDM_DATABRIDGE",
            "resourceType": "analyses",
            "settings": settings,
            "resourceUris": resource_uris
        }

        try:
            response = self.client.request('POST', CREATE_RDM_EXPORT_JOB, json=data)
            job_id = extract_id_from_location_header(response, "analysis job submission")
            return {
                'job_id': int(job_id),
                'skipped': False,
                'skipped_items': skipped_items,
                'included_items': included_items,
                'http_request_body': data
            }
        except Exception as e:
            raise IRPAPIError(f"Failed to submit rdm export job : {e}")


    def get_rdm_export_job(self, job_id: int) -> Dict[str, Any]:
        """
        Retrieve RDM export job status by job ID.

        Args:
            job_id: Job ID

        Returns:
            Dict containing job status details

        Raises:
            IRPValidationError: If job_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(job_id, "job_id")

        try:
            response = self.client.request('GET', GET_EXPORT_JOB.format(jobId=job_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get rdm export job status for job ID {job_id}: {e}")


    def poll_rdm_export_job_to_completion(
            self,
            job_id: int,
            interval: int = 10,
            timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll RDM export job until completion or timeout.

        Args:
            job_id: Job ID
            interval: Polling interval in seconds (default: 10)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            Final job status details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If job times out
            IRPAPIError: If polling fails
        """
        validate_positive_int(job_id, "job_id")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling RDM export job ID {job_id}")
            job_data = self.get_rdm_export_job(job_id)
            try:
                status = job_data['status']
                progress = job_data['progress']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing 'status' or 'progress' in job response for job ID {job_id}: {e}"
                ) from e
            print(f"Job status: {status}; Percent complete: {progress}")
            if status in WORKFLOW_COMPLETED_STATUSES:
                return job_data
            
            if time.time() - start > timeout:
                raise IRPJobError(
                    f"RDM Export job ID {job_id} did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)

    def get_rdm_database_id(self, rdm_name: str, server_name: str = "databridge-1") -> int:
        """
        Get database ID for an existing RDM by name.

        Args:
            rdm_name: Name of the RDM
            server_name: Name of the database server (default: "databridge-1")

        Returns:
            Database ID

        Raises:
            IRPAPIError: If RDM not found
        """
        databases = self.search_databases(
            server_name=server_name,
            filter=f"databaseName LIKE \"{rdm_name}*\""
        )
        if not databases:
            raise IRPAPIError(f"RDM '{rdm_name}' not found on server '{server_name}'")
        elif len(databases) > 1:
            raise IRPAPIError(f"Multiple RDMs found with name '{rdm_name}' on server '{server_name}'")

        try:
            return databases[0]['databaseId']
        except (KeyError, IndexError) as e:
            raise IRPAPIError(f"Failed to extract databaseId for RDM '{rdm_name}': {e}")

    def get_rdm_database_full_name(self, rdm_name: str, server_name: str = "databridge-1") -> str:
        """
        Get full database name for an existing RDM by name prefix.

        Args:
            rdm_name: Name prefix of the RDM
            server_name: Name of the database server (default: "databridge-1")

        Returns:
            Full database name

        Raises:
            IRPAPIError: If RDM not found
        """
        databases = self.search_databases(
            server_name=server_name,
            filter=f"databaseName LIKE \"{rdm_name}*\""
        )
        if not databases:
            raise IRPAPIError(f"RDM '{rdm_name}' not found on server '{server_name}'")
        elif len(databases) > 1:
            raise IRPAPIError(f"Multiple RDMs found with name '{rdm_name}' on server '{server_name}'")

        try:
            return databases[0]['databaseName']
        except (KeyError, IndexError) as e:
            raise IRPAPIError(f"Failed to extract databaseName for RDM '{rdm_name}': {e}")

    def search_databases(self, server_name: str, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search databases on a server.

        Args:
            server_name: Name of the database server
            filter: Optional filter string (e.g., 'databaseName="MyRDM"')
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)

        Returns:
            List of database records

        Raises:
            IRPAPIError: If request fails
        """
        # Look up server ID first
        database_servers = self.edm_manager.search_database_servers(filter=f"serverName=\"{server_name}\"")
        if not database_servers:
            raise IRPAPIError(f"Database server '{server_name}' not found")

        try:
            server_id = database_servers[0]['serverId']
        except (KeyError, IndexError) as e:
            raise IRPAPIError(f"Failed to extract server ID: {e}")

        params: Dict[str, Any] = {'limit': limit, 'offset': offset}
        if filter:
            params['filter'] = filter

        try:
            response = self.client.request(
                'GET',
                SEARCH_DATABASES.format(serverId=server_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search databases: {e}")

    def search_databases_paginated(self, server_name: str, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search all databases on a server with automatic pagination.

        Fetches all pages of results matching the filter criteria.

        Args:
            server_name: Name of the database server
            filter: Optional filter string (e.g., 'databaseName="MyRDM"')

        Returns:
            Complete list of all matching database records across all pages

        Raises:
            IRPAPIError: If request fails
        """
        all_results = []
        offset = 0
        limit = 100

        while True:
            results = self.search_databases(server_name=server_name, filter=filter, limit=limit, offset=offset)
            all_results.extend(results)

            # If we got fewer results than the limit, we've reached the end
            if len(results) < limit:
                break
            offset += limit

        return all_results

    def submit_delete_rdm_job(self, rdm_name: str, server_name: str = "databridge-1") -> str:
        """
        Submit job to delete an RDM from the databridge server.

        Args:
            rdm_name: Name prefix of the RDM to delete
            server_name: Name of the database server (default: "databridge-1")

        Returns:
            Job ID for the delete operation

        Raises:
            IRPAPIError: If RDM not found or delete request fails
        """
        validate_non_empty_string(rdm_name, "rdm_name")
        validate_non_empty_string(server_name, "server_name")

        # Get the full RDM name (with random suffix)
        rdm_full_name = self.get_rdm_database_full_name(rdm_name, server_name)

        # Submit delete request
        try:
            response = self.client.request(
                'DELETE',
                DELETE_RDM.format(instanceName=server_name, rdmName=rdm_full_name)
            )
            response_data = response.json()
            job_id = response_data.get('jobId')

            if not job_id:
                raise IRPAPIError(f"Delete RDM response did not contain jobId: {response_data}")

            return job_id
        except Exception as e:
            raise IRPAPIError(f"Failed to delete RDM '{rdm_name}': {e}") from e

    def get_databridge_job(self, job_id: str) -> str:
        """
        Get the status of a databridge job.

        Args:
            job_id: Job ID from databridge operation (e.g., delete RDM)

        Returns:
            Job status string

        Raises:
            IRPAPIError: If request fails
        """
        validate_non_empty_string(job_id, "job_id")

        try:
            response = self.client.request(
                'GET',
                GET_DATABRIDGE_JOB.format(jobId=job_id)
            )
            # API returns a string directly
            return response.text
        except Exception as e:
            raise IRPAPIError(f"Failed to get databridge job status for '{job_id}': {e}") from e

    def poll_delete_rdm_job_to_completion(
            self,
            job_id: str,
            interval: int = 10,
            timeout: int = 600000
    ) -> str:
        """
        Poll delete RDM job until completion or timeout.

        Valid statuses:
        - "Enqueued": Job queued for processing
        - "Processing": Job in progress
        - "Succeeded": Job completed successfully
        - Any other status is treated as an error

        Args:
            job_id: Job ID from delete operation
            interval: Polling interval in seconds (default: 10)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            Final job status string ("Succeeded")

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If job fails or times out
            IRPAPIError: If polling fails
        """
        validate_non_empty_string(job_id, "job_id")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        valid_in_progress_statuses = {"Enqueued", "Processing"}
        success_status = "Succeeded"

        start = time.time()
        while True:
            print(f"Polling delete RDM job ID {job_id}")
            status = self.get_databridge_job(job_id)
            print(f"Job status: {status}")

            # Check if job completed successfully
            if status == success_status:
                return status

            # Check if job failed
            if status not in valid_in_progress_statuses:
                raise IRPJobError(
                    f"Delete RDM job ID {job_id} failed with status: {status}"
                )

            # Check timeout
            if time.time() - start > timeout:
                raise IRPJobError(
                    f"Delete RDM job ID {job_id} did not complete within {timeout} seconds. Last status: {status}"
                )

            time.sleep(interval)

    def add_group_access_to_rdm(
            self,
            database_name: str,
            group_id: Optional[str] = None,
            server_name: str = "databridge-1"
    ) -> Dict[str, Any]:
        """
        Add group access to an RDM database.

        Args:
            database_name: Name of the RDM database
            group_id: Group ID to grant access to. If None, uses DATABRIDGE_GROUP_ID
                environment variable.
            server_name: Name of the database server (default: "databridge-1")

        Returns:
            Dict containing the API response

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails or group_id is not configured
        """
        validate_non_empty_string(database_name, "database_name")
        validate_non_empty_string(server_name, "server_name")

        # Get group_id from environment if not provided
        if group_id is None:
            group_id = os.environ.get('DATABRIDGE_GROUP_ID')
            if not group_id:
                raise IRPAPIError(
                    "group_id parameter not provided and DATABRIDGE_GROUP_ID "
                    "environment variable is not set"
                )

        validate_non_empty_string(group_id, "group_id")

        # Build request payload
        data = [
            {
                "operation": "Add",
                "targetProperty": "groupId",
                "value": group_id
            }
        ]

        try:
            response = self.client.request(
                'PATCH',
                UPDATE_GROUP_ACCESS.format(instanceName=server_name, databaseName=database_name),
                json=data
            )
            # API returns 204 No Content on success
            if response.status_code == 204:
                return {}
            return response.json()
        except Exception as e:
            raise IRPAPIError(
                f"Failed to add group access to RDM '{database_name}': {e}"
            ) from e
