import json
import time
from typing import Any, Dict, List
from helpers.irp_integration.client import Client
from helpers.irp_integration.constants import GET_RISK_DATA_JOB_BY_ID, SEARCH_RISK_DATA_JOBS, WORKFLOW_COMPLETED_STATUSES, WORKFLOW_IN_PROGRESS_STATUSES
from helpers.irp_integration.exceptions import IRPAPIError, IRPJobError
from helpers.irp_integration.validators import validate_list_not_empty, validate_positive_int


class JobManager:

    def __init__(self, client: Client) -> None:
        self.client = client


    def get_risk_data_job(self, job_id: int) -> Dict[str, Any]:
        """
        Retrieve job status by job ID.

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
            response = self.client.request('GET', GET_RISK_DATA_JOB_BY_ID.format(job_id=job_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get job status for job ID {job_id}: {e}")


    def search_risk_data_jobs(self, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search risk data jobs with optional filtering.

        Args:
            filter: Optional filter string (default: "")
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)

        Returns:
            List of risk data job dicts

        Raises:
            IRPAPIError: If search fails
        """
        params: Dict[str, Any] = {
            'limit': limit,
            'offset': offset
        }
        if filter:
            params['filter'] = filter

        try:
            response = self.client.request('GET', SEARCH_RISK_DATA_JOBS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search risk data jobs : {e}")
        

    def poll_risk_data_job_to_completion(
            self,
            job_id: int,
            interval: int = 10,
            timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll risk data job until completion or timeout.

        Args:
            job_id: Job ID
            interval: Polling interval in seconds
            timeout: Maximum timeout in seconds
        """
        validate_positive_int(job_id, "job_id")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling risk data job ID {job_id}")
            job_data = self.get_risk_data_job(job_id)
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
                    f"Risk data job ID {job_id} did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)


    def poll_risk_data_job_batch_to_completion(
            self,
            job_ids: List[int],
            interval: int = 20,
            timeout: int = 600000
    ) -> List[Dict[str, Any]]:
        """
        Poll multiple risk data jobs until all complete or timeout.

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
            print(f"Polling batch risk data job ids: {','.join(str(item) for item in job_ids)}")

            # Fetch all workflows across all pages
            all_jobs = []
            offset = 0
            limit = 100
            while True:
                quoted = ", ".join(json.dumps(str(s)) for s in job_ids)
                filter_statement = f"jobId IN ({quoted})"
                job_response = self.search_risk_data_jobs(
                    filter=filter_statement,
                    limit=limit,
                    offset=offset
                )
                if len(job_response) == 0:
                    break

                all_jobs.extend(job_response)

                # Check if we've fetched all workflows
                if len(all_jobs) >= len(job_ids):
                    break

                # Move to next page
                offset += limit

            # Check if all workflows are completed
            all_completed = True
            for job in all_jobs:
                status = job.get('status', '')
                if status in WORKFLOW_IN_PROGRESS_STATUSES:
                    all_completed = False
                    break

            if all_completed:
                return all_jobs

            if time.time() - start > timeout:
                raise IRPJobError(
                    f"Batch risk data jobs did not complete within {timeout} seconds"
                )
            time.sleep(interval)
