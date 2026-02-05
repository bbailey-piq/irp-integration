"""
EDM (Exposure Data Management) operations.

Handles datasource creation, duplication, deletion, and
associated data retrieval (cedants, LOBs).
"""

import json
import time
from typing import Dict, Any, List, Optional, Tuple
from .client import Client
from .constants import SEARCH_DATABASE_SERVERS, SEARCH_EXPOSURE_SETS, CREATE_EXPOSURE_SET, SEARCH_EDMS, CREATE_EDM, UPGRADE_EDM_DATA_VERSION, DELETE_EDM, GET_CEDANTS, GET_LOBS, WORKFLOW_IN_PROGRESS_STATUSES
from .exceptions import IRPAPIError, IRPJobError, IRPReferenceDataError
from .validators import validate_non_empty_string, validate_positive_int, validate_list_not_empty
from .utils import extract_id_from_location_header

class EDMManager:
    """Manager for EDM (Exposure Data Management) operations."""

    def __init__(
            self, 
            client: Client, 
            portfolio_manager: Optional[Any] = None, 
            analysis_manager: Optional[Any] = None,
            job_manager: Optional[Any] = None
    ) -> None:
        """
        Initialize EDM manager.

        Args:
            client: IRP API client instance
            portfolio_manager: Optional PortfolioManager instance
        """
        self.client = client
        self._portfolio_manager = portfolio_manager
        self._analysis_manager = analysis_manager
        self._job_manager = job_manager


    @property
    def portfolio_manager(self):
        """Lazy-loaded portfolio manager to avoid circular imports."""
        if self._portfolio_manager is None:
            from .portfolio import PortfolioManager
            self._portfolio_manager = PortfolioManager(self.client)
        return self._portfolio_manager
    
    @property
    def analysis_manager(self):
        """Lazy-loaded analysis manager to avoid circular imports."""
        if self._analysis_manager is None:
            from .analysis import AnalysisManager
            self._analysis_manager = AnalysisManager(self.client)
        return self._analysis_manager
    
    @property
    def job_manager(self):
        """Lazy-loaded job manager to avoid circular imports."""
        if self._job_manager is None:
            from .job import JobManager
            self._job_manager = JobManager(self.client)
        return self._job_manager


    def validate_unique_edms(self, edm_names: List[str]) -> None:
        """
        Validate that EDM names are unique (don't already exist).

        Args:
            edm_names: List of EDM names to validate

        Raises:
            IRPAPIError: If any EDM names already exist
        """
        quoted = ", ".join(json.dumps(e) for e in edm_names)
        edms = self.search_edms(filter=f"exposureName IN ({quoted})")
        if (len(edms) > 0):
            try:
                existing_names = ', '.join(json.dumps(s['exposureName']) for s in edms)
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Failed to extract EDM names from search response: {e}"
                ) from e
            raise IRPAPIError(f"The following EDMs already exist: {existing_names}; please use unique names")


    def submit_create_edm_jobs(self, edm_data_list: List[Dict[str, Any]]) -> List[int]:
        """
        Submit multiple EDM creation jobs.

        Args:
            edm_data_list: List of EDM data dicts, each containing:
                - server_name: str
                - edm_name: str

        Returns:
            List of job IDs

        Raises:
            IRPValidationError: If edm_data_list is empty or invalid
            IRPAPIError: If EDM creation fails or duplicate names exist
        """
        validate_list_not_empty(edm_data_list, "edm_data")

        self.validate_unique_edms(list(e['edm_name'] for e in edm_data_list))

        job_ids = []
        for edm_data in edm_data_list:
            # Validate edm_data_list entry
            try:
                server_name = edm_data['server_name']
                edm_name = edm_data['edm_name']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing 'server_name' or 'server_name' in create edm data: {e}"
                ) from e

            # Submit job (returns tuple of job_id, request_body - we only need job_id here)
            job_id, _ = self.submit_create_edm_job(
                edm_name=edm_name,
                server_name=server_name
            )
            job_ids.append(job_id)
        
        return job_ids


    def search_database_servers(self, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search database servers.

        Args:
            filter: Optional filter string for server names

        Returns:
            Dict containing list of database servers
        """
        params = {}
        if filter:
            params['filter'] = filter
        try:
            response = self.client.request('GET', SEARCH_DATABASE_SERVERS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search database servers: {e}")


    def search_exposure_sets(self, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search exposure sets.

        Args:
            filter: Optional filter string for exposure set names

        Returns:
            Dict containing list of exposure sets
        """
        params = {}
        if filter:
            params['filter'] = filter
        try:
            response = self.client.request('GET', SEARCH_EXPOSURE_SETS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search exposure sets: {e}")
        
    
    def create_exposure_set(self, name: str) -> int:
        """
        Create a new exposure set.

        Args:
            name: Name of the exposure set

        Returns:
            The exposure set ID
        """
        validate_non_empty_string(name, "name")
        data = {"exposureSetName": name}
        try:
            response = self.client.request('POST', CREATE_EXPOSURE_SET, json=data)
            exposure_set_id = extract_id_from_location_header(response, "exposure set creation")
            return int(exposure_set_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to create exposure set '{name}': {e}")


    def search_edms(self, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search EDMs (exposures).

        Args:
            filter: Optional filter string for EDM names
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)

        Returns:
            List of EDM dictionaries
        """
        params = {'limit': limit, 'offset': offset}
        if filter:
            params['filter'] = filter
        try:
            response = self.client.request('GET', SEARCH_EDMS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search EDMs: {e}")

    def search_edms_paginated(self, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search all EDMs with automatic pagination.

        Fetches all pages of results matching the filter criteria.

        Args:
            filter: Optional filter string for EDM names

        Returns:
            Complete list of all matching EDMs across all pages
        """
        all_results = []
        offset = 0
        limit = 100

        while True:
            results = self.search_edms(filter=filter, limit=limit, offset=offset)
            all_results.extend(results)

            # If we got fewer results than the limit, we've reached the end
            if len(results) < limit:
                break
            offset += limit

        return all_results
        

    def submit_create_edm_job(self, edm_name: str, server_name: str = "databridge-1") -> Tuple[int, Dict[str, Any]]:
        """
        Submit job to create a new EDM (exposure).

        Args:
            edm_name: Name of the EDM
            server_name: Name of the database server (default: "databridge-1")

        Returns:
            Tuple of (job_id, request_body) where request_body is the HTTP request payload
        """
        validate_non_empty_string(edm_name, "edm_name")

        # Validate Database Server exists
        database_servers = self.search_database_servers(filter=f"serverName=\"{server_name}\"")
        if (len(database_servers) != 1):
            raise IRPReferenceDataError(f"Database server {server_name} not found: {database_servers}")
        try:
            database_server_id = database_servers[0]['serverId']
        except (KeyError, TypeError, IndexError) as e:
            raise IRPAPIError(
                f"Failed to extract server ID: {e}"
            ) from e

        # Validate Exposure Set exists; create if it does not exist
        exposure_sets = self.search_exposure_sets(filter=f"exposureSetName={edm_name}")
        if (len(exposure_sets) > 0):
            try:
                exposure_set_id = exposure_sets[0]['exposureSetId']
            except (KeyError, TypeError, IndexError) as e:
                raise IRPAPIError(
                    f"Missing 'exposureSetId' index 0 does not exist in database server data: {e}"
                ) from e
        else:
            exposure_set_id = self.create_exposure_set(name=edm_name)

        data = {
            "exposureName": edm_name,
            "serverId": database_server_id
        }
        try:
            response = self.client.request(
                'POST',
                CREATE_EDM.format(exposureSetId=exposure_set_id),
                json=data
            )
            job_id = extract_id_from_location_header(response, "EDM creation")
            return int(job_id), data
        except Exception as e:
            raise IRPAPIError(f"Failed to create EDM '{edm_name}': {e}")


    def submit_upgrade_edm_data_version_jobs(self, edm_data_list: List[Dict[str, Any]]) -> List[int]:
        """
        Submit multiple EDM data version upgrade jobs.

        Args:
            edm_data_list: List of EDM upgrade data dicts, each containing:
                - edm_name: str
                - edm_version: str

        Returns:
            List of job IDs

        Raises:
            IRPValidationError: If edm_data_list is empty or invalid
            IRPAPIError: If upgrade submission fails or EDM not found
        """
        validate_list_not_empty(edm_data_list, "edm_data")

        job_ids = []
        for edm_data in edm_data_list:
            try:
                edm_name = edm_data['edm_name']
                edm_version = edm_data['edm_version']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing upgrade edm version data: {e}"
                ) from e

            # Submit job (returns tuple of job_id, request_body - we only need job_id here)
            job_id, _ = self.submit_upgrade_edm_data_version_job(
                edm_name=edm_name,
                edm_version=edm_version
            )
            job_ids.append(job_id)

        return job_ids


    def submit_upgrade_edm_data_version_job(self, edm_name: str, edm_version: str) -> Tuple[int, Dict[str, Any]]:
        """
        Submit job to upgrade EDM data version.

        Args:
            edm_name: Name of the EDM to upgrade
            edm_version: Target EDM data version (e.g., "22")

        Returns:
            Tuple of (job_id, request_body) where request_body is the HTTP request payload

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If EDM not found or upgrade fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(edm_version, "edm_version")

        # Look up EDM to get exposure_id
        edms = self.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if len(edms) != 1:
            raise IRPAPIError(f"Expected 1 EDM with name '{edm_name}', found {len(edms)}")
        try:
            exposure_id = edms[0]['exposureId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(f"Failed to extract exposure ID for EDM '{edm_name}': {e}") from e

        try:
            data = {"edmDataVersion": edm_version}
            response = self.client.request(
                'POST',
                UPGRADE_EDM_DATA_VERSION.format(exposureId=exposure_id),
                json=data
            )
            job_id = extract_id_from_location_header(response, "EDM data version upgrade")
            return int(job_id), data
        except Exception as e:
            raise IRPAPIError(f"Failed to upgrade EDM data version for EDM '{edm_name}': {e}")


    def poll_data_version_upgrade_job_batch_to_completion(
            self,
            job_ids: List[int],
            interval: int = 20,
            timeout: int = 600000
    ) -> List[Dict[str, Any]]:
        """
        Poll multiple EDM data version upgrade jobs until all complete or timeout.

        Args:
            job_ids: List of job IDs
            interval: Polling interval in seconds (default: 20)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            List of final job status details for all jobs

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If jobs time out
            IRPAPIError: If polling fails
        """
        validate_list_not_empty(job_ids, "job_ids")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling batch upgrade edm version job ids: {','.join(str(item) for item in job_ids)}")

            all_completed = False
            all_jobs = []
            for job_id in job_ids:
                workflow_response = self.client.get_workflow(job_id)
                all_jobs.append(workflow_response)
                try:
                    status = workflow_response['status']
                except (KeyError, TypeError) as e:
                    raise IRPAPIError(
                        f"Missing 'status' in workflow response for job ID {job_id}: {e}"
                    ) from e
                if status in WORKFLOW_IN_PROGRESS_STATUSES:
                    all_jobs = []
                    break
                all_completed = True

            if all_completed:
                return all_jobs
            
            if time.time() - start > timeout:
                raise IRPJobError(
                    f"Batch upgrade edm version jobs did not complete within {timeout} seconds"
                )
            time.sleep(interval)


    def delete_edm(self, edm_name: str) -> Dict[str, Any]:
        """
        Delete an EDM and all its associated analyses.

        Args:
            edm_name: Name of EDM to delete

        Returns:
            Dict containing final delete job status

        Raises:
            IRPValidationError: If edm_name is invalid
            IRPAPIError: If EDM not found or deletion fails
        """
        validate_non_empty_string(edm_name, "edm_name")

        edms = self.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if (len(edms) != 1):
            raise IRPAPIError(f"Expected 1 EDM with name {edm_name}, found {len(edms)}")
        try:
            exposure_id = edms[0]['exposureId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract exposure ID for EDM '{edm_name}': {e}"
            ) from e

        analyses = self.analysis_manager.search_analyses(filter=f"exposureName=\"{edm_name}\"")
        for analysis in analyses:
            try:
                self.analysis_manager.delete_analysis(analysis['analysisId'])
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Failed to extract analysis ID from analysis data: {e}"
                ) from e

        delete_edm_job_id = self.submit_delete_edm_job(exposure_id)
        return self.job_manager.poll_risk_data_job_to_completion(delete_edm_job_id)


    def submit_delete_edm_job(self, exposure_id: int) -> int:
        """
        Submit job to delete an EDM (exposure).

        Args:
            exposure_id: ID of the exposure (EDM)

        Returns:
            The job ID
        """
        validate_positive_int(exposure_id, "exposure_id")
        try:
            response = self.client.request(
                'DELETE',
                DELETE_EDM.format(exposureId=exposure_id)
            )
            job_id = extract_id_from_location_header(response, "EDM deletion")
            return int(job_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to delete EDM with exposure ID '{exposure_id}': {e}")


    def get_cedants_by_edm(self, exposure_id: int) -> List[Dict[str, Any]]:
        """
        Retrieve cedants for an EDM.

        Args:
            exposure_id: Exposure ID

        Returns:
            List of cedant data

        Raises:
            IRPValidationError: If exposure_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "edm_name")
        try:
            response = self.client.request('GET', GET_CEDANTS.format(exposureId=exposure_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get cedants for exposure ID '{exposure_id}': {e}")


    def get_lobs_by_edm(self, exposure_id: int) -> List[Dict[str, Any]]:
        """
        Retrieve lines of business (LOBs) for an EDM.

        Args:
            edm_name: Name of EDM

        Returns:
            Dict containing LOB list

        Raises:
            IRPValidationError: If edm_name is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "edm_name")
        try:
            response = self.client.request('GET', GET_LOBS.format(exposureId=exposure_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get LOBs for exposure ID '{exposure_id}': {e}")
