"""
Portfolio management operations.

Handles portfolio creation, retrieval, and geocoding/hazard operations.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from zoneinfo import ZoneInfo

from .client import Client
from .constants import GET_PORTFOLIO_BY_ID, GET_PORTFOLIO_METADATA, CREATE_PORTFOLIO, GET_GEOHAZ_JOB, SEARCH_PORTFOLIOS, GEOHAZ_PORTFOLIO, WORKFLOW_COMPLETED_STATUSES, WORKFLOW_IN_PROGRESS_STATUSES, SEARCH_ACCOUNTS_BY_PORTFOLIO
from .exceptions import IRPAPIError, IRPJobError
from .validators import validate_list_not_empty, validate_non_empty_string, validate_positive_int
from .utils import extract_id_from_location_header

logger = logging.getLogger(__name__)


class PortfolioManager:
    """Manager for portfolio operations."""

    def __init__(self, client: Client, edm_manager: Optional[Any] = None) -> None:
        """
        Initialize portfolio manager.

        Args:
            client: IRP API client instance
        """
        self.client = client
        self._edm_manager = edm_manager

    @property
    def edm_manager(self):
        """Lazy-loaded edm manager to avoid circular imports."""
        if self._edm_manager is None:
            from .edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager


    def get_portfolio_by_id(self, exposure_id: int, portfolio_id: int) -> Dict[str, Any]:
        """
        Retrieve portfolio details by portfolio ID.

        Args:
            exposure_id: Exposure ID
            portfolio_id: Portfolio ID

        Returns:
            Dict containing portfolio details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(portfolio_id, "portfolio_id")

        try:
            response = self.client.request('GET', GET_PORTFOLIO_BY_ID.format(exposureId=exposure_id, id=portfolio_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get portfolio details for exposure ID '{exposure_id}' and portfolio ID '{portfolio_id}': {e}")
        
    def get_portfolio_metadata(self, exposure_id: int, portfolio_id: int) -> Dict[str, Any]:
        """
        Retrieve portfolio metadata by portfolio ID.

        Args:
            exposure_id: Exposure ID
            portfolio_id: Portfolio ID

        Returns:
            Dict containing portfolio metadata details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(portfolio_id, "portfolio_id")

        try:
            response = self.client.request('GET', GET_PORTFOLIO_METADATA.format(exposureId=exposure_id, id=portfolio_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get portfolio metadata for exposure ID '{exposure_id}' and portfolio ID '{portfolio_id}': {e}")
    
    def search_portfolios(self, exposure_id: int, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search portfolios within an exposure.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter string for portfolio names
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)

        Returns:
            List of portfolio dictionaries
        """
        validate_positive_int(exposure_id, "exposure_id")

        params: Dict[str, Any] = {'limit': limit, 'offset': offset}
        if filter:
            params['filter'] = filter

        try:
            response = self.client.request(
                'GET',
                SEARCH_PORTFOLIOS.format(exposureId=exposure_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search portfolios for exposure ID '{exposure_id}': {e}")

    def search_portfolios_paginated(self, exposure_id: int, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search all portfolios within an exposure with automatic pagination.

        Fetches all pages of results matching the filter criteria.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter string for portfolio names

        Returns:
            Complete list of all matching portfolios across all pages
        """
        validate_positive_int(exposure_id, "exposure_id")

        all_results = []
        offset = 0
        limit = 100

        while True:
            results = self.search_portfolios(exposure_id=exposure_id, filter=filter, limit=limit, offset=offset)
            all_results.extend(results)

            # If we got fewer results than the limit, we've reached the end
            if len(results) < limit:
                break
            offset += limit

        return all_results


    def search_accounts_by_portfolio(self, exposure_id: int, portfolio_id: int) -> List[Dict[str, Any]]:
        """
        Search portfolios within an exposure.

        Args:
            exposure_id: Exposure ID
            portfolio_id: Portfolio ID

        Returns:
            Dict containing list of accounts
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(portfolio_id, "portfolio_id")

        try:
            response = self.client.request('GET', SEARCH_ACCOUNTS_BY_PORTFOLIO.format(exposureId=exposure_id, id=portfolio_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search portfolio accounts for exposure ID '{exposure_id}' and portfolio ID '{portfolio_id}': {e}")


    def create_portfolios(self, portfolio_data_list: List[Dict[str, Any]]) -> List[int]:
        """
        Create multiple portfolios.

        Args:
            portfolio_data_list: List of portfolio data dicts, each containing:
                - edm_name: str
                - portfolio_name: str
                - portfolio_number: str
                - description: str

        Returns:
            List of portfolio IDs

        Raises:
            IRPValidationError: If portfolio_data_list is empty or invalid
            IRPAPIError: If portfolio creation fails or duplicate names exist
        """
        validate_list_not_empty(portfolio_data_list, "portfolio_data_list")

        portfolio_ids = []
        for portfolio_data in portfolio_data_list:
            try:
                edm_name = portfolio_data['edm_name']
                portfolio_name = portfolio_data['portfolio_name']
                portfolio_number = portfolio_data['portfolio_number']
                description = portfolio_data['description']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing value in create portfolio data: {e}"
                ) from e
            
            # Returns tuple of (portfolio_id, request_body) - we only need portfolio_id here
            portfolio_id, _ = self.create_portfolio(
                edm_name=edm_name,
                portfolio_name=portfolio_name,
                portfolio_number=portfolio_number,
                description=description
            )
            portfolio_ids.append(portfolio_id)

        return portfolio_ids


    def create_portfolio(
        self,
        edm_name: str,
        portfolio_name: str,
        portfolio_number: str = "",
        description: str = ""
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Create new portfolio in EDM.

        Args:
            exposure_id: ID of EDM datasource
            portfolio_name: Name for new portfolio
            portfolio_number: Portfolio number (default: "1")
            description: Portfolio description (default: "")

        Returns:
            Tuple of (portfolio_id, request_body) where request_body is the HTTP request payload

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(portfolio_name, "portfolio_name")

        edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if (len(edms) != 1):
            raise IRPAPIError(f"Expected 1 EDM with name {edm_name}, found {len(edms)}")
        try:
            exposure_id = edms[0]['exposureId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract exposure ID for EDM '{edm_name}': {e}"
            ) from e

        portfolios = self.search_portfolios(exposure_id=exposure_id, filter=f"portfolioName=\"{portfolio_name}\"")
        if (len(portfolios) > 0):
            raise IRPAPIError(f"{len(portfolios)} portfolios found with name {portfolio_name}, please use a unique name")

        if not portfolio_number:
            portfolio_number = portfolio_name
        if not description:
            description = "Portfolio created via API on " + datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S %Z")

        data = {
            "portfolioName": portfolio_name,
            "portfolioNumber": portfolio_number[:20],
            "description": description,
        }

        try:
            logger.info("Creating portfolio '%s' in exposure ID %s", portfolio_name, exposure_id)
            response = self.client.request('POST', CREATE_PORTFOLIO.format(exposureId=exposure_id), json=data)
            portfolio_id = extract_id_from_location_header(response, "portfolio creation")
            logger.info("Portfolio created — ID: %s", portfolio_id)
            return int(portfolio_id), data
        except Exception as e:
            raise IRPAPIError(f"Failed to create portfolio '{portfolio_name}' in exposure id '{exposure_id}': {e}")


    def submit_geohaz_jobs(self, geohaz_data_list: List[Dict[str, Any]]) -> List[int]:
        """
        Submit multiple geohaz jobs (geocoding and hazard operations).

        Args:
            geohaz_data_list: List of geohaz data dicts, each containing:
                - edm_name: str
                - portfolio_name: str
                - version: str
                - hazard_eq: bool
                - hazard_ws: bool

        Returns:
            List of job IDs

        Raises:
            IRPValidationError: If geohaz_data_list is empty or invalid
            IRPAPIError: If job submission fails or resources not found
        """
        validate_list_not_empty(geohaz_data_list, "geohaz_data_list")

        job_ids = []
        for geohaz_data in geohaz_data_list:
            try:
                edm_name = geohaz_data['edm_name']
                portfolio_name = geohaz_data['portfolio_name']
                version = geohaz_data['version']
                hazard_eq = geohaz_data['hazard_eq']
                hazard_ws = geohaz_data['hazard_ws']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing geohaz job data: {e}"
                ) from e

            # Returns tuple of (job_id, request_body) - we only need job_id here
            job_id, _ = self.submit_geohaz_job(
                portfolio_name=portfolio_name,
                edm_name=edm_name,
                version=version,
                hazard_eq=hazard_eq,
                hazard_ws=hazard_ws
            )
            job_ids.append(job_id)

        return job_ids
        

    def submit_geohaz_job(self,
                          portfolio_name: str,
                          edm_name: str,
                          version: str = "22.0",
                          hazard_eq: bool = False,
                          hazard_ws: bool = False,
                          geocode_layer_options: Optional[Dict[str, Any]] = None,
                          hazard_layer_options: Optional[Dict[str, Any]] = None
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Execute geocoding and/or hazard operations on portfolio.

        Args:
            portfolio_name: Name of the portfolio
            edm_name: Name of the EDM containing the portfolio
            version: Geocode version (default: "22.0")
            hazard_eq: Enable earthquake hazard (default: False)
            hazard_ws: Enable windstorm hazard (default: False)

        Returns:
            Tuple of (job_id, request_body) where request_body is the HTTP request payload

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If workflow fails or times out
        """
        validate_non_empty_string(portfolio_name, "portfolio_name")
        validate_non_empty_string(edm_name, "edm_name")

        # Look up EDM to get exposure_id
        edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if len(edms) != 1:
            raise IRPAPIError(f"Expected 1 EDM with name '{edm_name}', found {len(edms)}")
        try:
            exposure_id = edms[0]['exposureId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(f"Failed to extract exposure ID for EDM '{edm_name}': {e}") from e

        # Look up portfolio to get portfolio_uri and portfolio_id
        portfolios = self.search_portfolios(exposure_id=exposure_id, filter=f"portfolioName=\"{portfolio_name}\"")
        if len(portfolios) != 1:
            raise IRPAPIError(f"Expected 1 portfolio with name '{portfolio_name}', found {len(portfolios)}")
        try:
            portfolio_uri = portfolios[0]['uri']
            portfolio_id = portfolios[0]['portfolioId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(f"Failed to extract portfolio details for portfolio '{portfolio_name}': {e}") from e

        # Check if portfolio has locations to GeoHaz
        accounts = self.search_accounts_by_portfolio(exposure_id=exposure_id, portfolio_id=portfolio_id)
        if len(accounts) == 0:
            raise IRPAPIError(f"Portfolio '{portfolio_name}' does not have any Accounts/Locations to be GeoHaz'd")

        # Validate locations count
        try:
            locations_count = 0
            for account in accounts:
                locations_count += account['locationsCount']
                if locations_count > 0:
                    break
        except (KeyError, TypeError, IndexError) as e:
            raise IRPAPIError(f"Failed to validate locations count for portfolio '{portfolio_name}': {e}") from e

        if locations_count == 0:
            raise IRPAPIError(f"Portfolio '{portfolio_name}' has accounts but no locations to be GeoHaz'd")

        if geocode_layer_options is None:
            geocode_layer_options = {
                "aggregateTriggerEnabled": "true",
                "geoLicenseType": "0",
                "skipPrevGeocoded": False
            }

        if hazard_layer_options is None:
            hazard_layer_options = {
                "overrideUserDef": False,
                "skipPrevHazard": False
            }

        data = {
            "resourceUri": portfolio_uri,
            "resourceType": "portfolio",
            "settings": {
                "layers": [
                    {
                        "type": "geocode",
                        "name": "geocode",
                        "engineType": "RL",
                        "version": version,
                        "layerOptions": geocode_layer_options
                    }
                ]
            }
        }

        if hazard_eq:
            data['settings']['layers'].append(
                {
                    "type": "hazard",
                    "name": "earthquake",
                    "engineType": "RL",
                    "version": version,
                    "layerOptions": hazard_layer_options
                }
            )

        if hazard_ws:
            data['settings']['layers'].append(
                {
                    "type": "hazard",
                    "name": "windstorm",
                    "engineType": "RL",
                    "version": version,
                    "layerOptions": hazard_layer_options
                }
            )

        try:
            logger.info("Submitting GeoHaz job for portfolio '%s'", portfolio_name)
            response = self.client.request(
                'POST',
                GEOHAZ_PORTFOLIO,
                json=data
            )
            job_id = extract_id_from_location_header(response, "portfolio geohaz")
            logger.info("GeoHaz job submitted — job ID: %s", job_id)
            return int(job_id), data
        except Exception as e:
            raise IRPAPIError(f"Failed to execute geohaz for portfolio '{portfolio_uri}': {e}")
        
    
    def get_geohaz_job(self, job_id: int) -> Dict[str, Any]:
        """
        Retrieve geohaz job status by job ID.

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
            response = self.client.request('GET', GET_GEOHAZ_JOB.format(jobId=job_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get geohaz job status for job ID {job_id}: {e}")


    def poll_geohaz_job_to_completion(
        self,
        job_id: int,
        interval: int = 10,
        timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll geohaz job until completion or timeout.

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
            logger.info("Polling GeoHaz job ID %s", job_id)
            job_data = self.get_geohaz_job(job_id)
            try:
                status = job_data['status']
                progress = job_data['progress']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing 'status' or 'progress' in job response for job ID {job_id}: {e}"
                ) from e
            logger.info("GeoHaz job %s status: %s; progress: %s", job_id, status, progress)
            if status in WORKFLOW_COMPLETED_STATUSES:
                return job_data

            if time.time() - start > timeout:
                logger.error("GeoHaz job %s timed out after %s seconds. Last status: %s", job_id, timeout, status)
                raise IRPJobError(
                    f"GeoHaz job ID {job_id} did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)


    def poll_geohaz_job_batch_to_completion(
            self,
            job_ids: List[int],
            interval: int = 20,
            timeout: int = 600000
    ) -> List[Dict[str, Any]]:
        """
        Poll multiple geohaz jobs until all complete or timeout.

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
            logger.info("Polling batch GeoHaz job IDs: %s", ",".join(str(item) for item in job_ids))

            all_completed = False
            all_jobs = []
            for job_id in job_ids:
                workflow_response = self.get_geohaz_job(job_id)
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
                logger.error("Batch GeoHaz jobs timed out after %s seconds", timeout)
                raise IRPJobError(
                    f"Batch geohaz jobs did not complete within {timeout} seconds"
                )
            time.sleep(interval)
