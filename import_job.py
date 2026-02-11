"""
Import job management operations.

Provides a centralized interface for submitting, tracking, and polling
platform import jobs. Uses the /platform/import/v1/jobs endpoint.
"""

import logging
import time
from typing import Dict, Any, Optional, Tuple

from .client import Client
from .constants import GET_IMPORT_JOB, WORKFLOW_COMPLETED_STATUSES
from .exceptions import IRPAPIError, IRPJobError, IRPValidationError
from .validators import validate_positive_int, validate_non_empty_string

logger = logging.getLogger(__name__)


class ImportJobManager:
    """Manager for platform import job operations (EDM, RDM, MRI)."""

    VALID_IMPORT_TYPES = {"EDM", "RDM", "MRI"}

    def __init__(
        self,
        client: Client,
        edm_manager: Optional[Any] = None,
        rdm_manager: Optional[Any] = None,
        mri_manager: Optional[Any] = None
    ) -> None:
        """
        Initialize ImportJobManager.

        Args:
            client: IRP API client instance
            edm_manager: Optional EDMManager instance for EDM import routing
            rdm_manager: Optional RDMManager instance for RDM import routing
        """
        self.client = client
        self._edm_manager = edm_manager
        self._rdm_manager = rdm_manager
        self._mri_manager = mri_manager

    @property
    def edm_manager(self):
        """Lazy-loaded EDM manager to avoid circular imports."""
        if self._edm_manager is None:
            from .edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager

    @property
    def rdm_manager(self):
        """Lazy-loaded RDM manager to avoid circular imports."""
        if self._rdm_manager is None:
            from .rdm import RDMManager
            self._rdm_manager = RDMManager(self.client)
        return self._rdm_manager
    
    @property
    def mri_manager(self):
        """Lazy-loaded MRI manager to avoid circular imports."""
        if self._mri_manager is None:
            from .mri_import import MRIImportManager
            self._mri_manager = MRIImportManager(self.client)
        return self._mri_manager

    def submit_job(self, import_type: str, **kwargs) -> Tuple[int, Dict[str, Any]]:
        """
        Submit an import job, routing to the appropriate manager based on type.

        Args:
            import_type: Type of import - "EDM", "RDM", or "MRI"
            **kwargs: Arguments passed to the underlying submit method.

                For EDM (routed to EDMManager.submit_edm_import_job):
                    edm_name (str): Name for the EDM
                    edm_file_path (str): Path to the .bak file
                    server_name (str): Database server name (default: "sql-instance-1")

                For RDM (routed to RDMManager.submit_rdm_import_job):
                    rdm_name (str): Name for the RDM
                    edm_name (str): Name of the target EDM
                    rdm_file_path (str): Path to the .bak file

                For MRI (routed to MRIImportManager.submit_mri_import_job):
                    edm_name (str): Target EDM name
                    portfolio_name (str): Target portfolio name
                    accounts_file_path (str): Path to accounts CSV file
                    locations_file_path (str): Path to locations CSV file
                    mapping_file_path (str, optional): Path to .mff mapping file
                    delimiter (str): File delimiter (default: "TAB")

        Returns:
            Tuple of (job_id, request_body)

        Raises:
            IRPValidationError: If import_type is invalid or kwargs are wrong
            IRPAPIError: If submission fails
        """
        validate_non_empty_string(import_type, "import_type")
        import_type_upper = import_type.upper()

        if import_type_upper not in self.VALID_IMPORT_TYPES:
            raise IRPValidationError(
                f"Invalid import_type '{import_type}'. "
                f"Must be one of: {', '.join(sorted(self.VALID_IMPORT_TYPES))}"
            )

        if import_type_upper == "EDM":
            return self.edm_manager.submit_edm_import_job(**kwargs)
        elif import_type_upper == "RDM":
            return self.rdm_manager.submit_rdm_import_job(**kwargs)
        else:
            return self.mri_manager.submit_mri_import_job(**kwargs)

    def get_import_job(self, job_id: int) -> Dict[str, Any]:
        """
        Get import job status by job ID.

        Args:
            job_id: Import job ID

        Returns:
            Dict containing job status details

        Raises:
            IRPValidationError: If job_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(job_id, "job_id")

        try:
            response = self.client.request('GET', GET_IMPORT_JOB.format(jobId=job_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get import job status for job ID {job_id}: {e}")

    def poll_import_job_to_completion(
        self,
        job_id: int,
        interval: int = 10,
        timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll import job until completion or timeout.

        Args:
            job_id: Import job ID
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
            logger.info("Polling import job ID %s", job_id)
            job_data = self.get_import_job(job_id)
            try:
                status = job_data['status']
                progress = job_data.get('progress', 0)
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing 'status' in job response for job ID {job_id}: {e}"
                ) from e
            logger.info("Job %s status: %s; progress: %s", job_id, status, progress)
            if status in WORKFLOW_COMPLETED_STATUSES:
                return job_data

            if time.time() - start > timeout:
                logger.error("Import job %s timed out after %s seconds. Last status: %s", job_id, timeout, status)
                raise IRPJobError(
                    f"Import job ID {job_id} did not complete within {timeout} seconds. "
                    f"Last status: {status}"
                )
            time.sleep(interval)
