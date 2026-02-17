"""
Export job management operations.

Provides an interface for tracking and polling platform export jobs.
Uses the /platform/export/v1/jobs endpoint.
"""

import logging
import os
import time
from typing import Dict, Any
from urllib.parse import unquote, urlparse

import requests

from .client import Client
from .constants import GET_EXPORT_JOB, WORKFLOW_COMPLETED_STATUSES
from .exceptions import IRPAPIError, IRPJobError
from .validators import validate_positive_int, validate_non_empty_string

logger = logging.getLogger(__name__)


class ExportJobManager:
    """Manager for platform export job operations."""

    def __init__(self, client: Client) -> None:
        """
        Initialize ExportJobManager.

        Args:
            client: IRP API client instance
        """
        self.client = client

    def get_export_job(self, job_id: int) -> Dict[str, Any]:
        """
        Get export job status by job ID.

        Args:
            job_id: Export job ID

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
            raise IRPAPIError(f"Failed to get export job status for job ID {job_id}: {e}")

    def poll_export_job_to_completion(
        self,
        job_id: int,
        interval: int = 10,
        timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll export job until completion or timeout.

        Args:
            job_id: Export job ID
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
            logger.info("Polling export job ID %s", job_id)
            job_data = self.get_export_job(job_id)
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
                logger.error("Export job %s timed out after %s seconds. Last status: %s", job_id, timeout, status)
                raise IRPJobError(
                    f"Export job ID {job_id} did not complete within {timeout} seconds. "
                    f"Last status: {status}"
                )
            time.sleep(interval)

    def download_export_results(self, job_id: int, output_dir: str) -> str:
        """
        Download exported analysis results for a completed export job.

        Fetches the job, extracts the downloadUrl from the DOWNLOAD_RESULTS task,
        and streams the zip file to the output directory.

        Args:
            job_id: Export job ID (must be FINISHED)
            output_dir: Directory to save the downloaded file

        Returns:
            Path to the downloaded file

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If job is not finished
            IRPAPIError: If download URL not found or download fails
        """
        validate_positive_int(job_id, "job_id")
        validate_non_empty_string(output_dir, "output_dir")

        job_data = self.get_export_job(job_id)
        status = job_data.get('status')
        if status != 'FINISHED':
            raise IRPJobError(
                f"Export job {job_id} is not finished (status: {status}). "
                "Cannot download results."
            )

        # Extract downloadUrl from the DOWNLOAD_RESULTS task
        download_url = None
        for task in job_data.get('tasks', []):
            if task.get('name') == 'DOWNLOAD_RESULTS':
                download_url = task.get('output', {}).get('log', {}).get('downloadUrl')
                break

        if not download_url:
            raise IRPAPIError(f"No download URL found in export job {job_id}")

        # Extract filename from URL path (e.g., "23530777_usfl_commercial_Losses.zip")
        url_path = unquote(urlparse(download_url).path)
        filename = url_path.rsplit('/', 1)[-1]
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)

        try:
            logger.info("Downloading export results for job %s to %s", job_id, output_path)
            response = requests.get(download_url, stream=True, timeout=300)
            response.raise_for_status()

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info("Downloaded export results: %s", output_path)
            return output_path
        except requests.RequestException as e:
            raise IRPAPIError(f"Failed to download export results for job {job_id}: {e}")
