"""
MRI Import Manager for IRP Integration.

Handles Multi-Risk Insurance (MRI) data imports including file uploads
to AWS S3 and import execution via Moody's Risk Modeler API.
"""

from typing import Dict, Any, List, Optional, Tuple
import boto3
from boto3.s3.transfer import TransferConfig
import requests
import json
import os
import time
import pandas as pd
from .client import Client
from .constants import (
    CREATE_AWS_BUCKET,
    CREATE_MAPPING,
    EXECUTE_IMPORT,
    GET_WORKFLOW_BY_ID,
    GET_WORKFLOWS,
    WORKFLOW_IN_PROGRESS_STATUSES
)
from .exceptions import IRPFileError, IRPAPIError, IRPValidationError, IRPJobError
from .validators import (
    validate_non_empty_string,
    validate_file_exists,
    validate_positive_int,
    validate_list_not_empty
)
from .utils import decode_mri_credentials, extract_id_from_location_header, get_location_header


class MRIImportManager:
    """Manager for MRI import operations."""

    def __init__(self, client: Client, edm_manager: Optional[Any] = None, portfolio_manager: Optional[Any] = None):
        """
        Initialize MRI Import Manager.

        Args:
            client: Client instance for API requests
        """
        self.client = client
        self._edm_manager = edm_manager
        self._portfolio_manager = portfolio_manager

    @property
    def edm_manager(self):
        """Lazy-loaded edm manager to avoid circular imports."""
        if self._edm_manager is None:
            from .edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager
    
    @property
    def portfolio_manager(self):
        """Lazy-loaded portfolio manager to avoid circular imports."""
        if self._portfolio_manager is None:
            from .portfolio import PortfolioManager
            self._portfolio_manager = PortfolioManager(self.client)
        return self._portfolio_manager

    @staticmethod
    def _add_missing_sources(headers: list, items: list) -> bool:
        """Add missing source entries for CSV headers not in mapping."""
        # Build set of existing sources (uppercase for case-insensitive comparison)
        existing_sources_upper = {item['source'].upper() for item in items}

        modified = False
        for header in headers:
            header_upper = header.upper()

            if header_upper in existing_sources_upper:
                continue  # Already has a source entry

            # Add new entry: HEADER_UPPER -> HEADER_UPPER
            items.append({
                'source': header_upper,
                'destination': header_upper
            })
            print(f"  Added mapping: {header_upper} -> {header_upper}")
            modified = True

        return modified

    @staticmethod
    def _sync_mapping_with_csv_headers(
        mapping_file_path: str,
        accounts_file_path: str,
        locations_file_path: str
    ) -> None:
        """
        Update mapping.json to include any CSV headers not already present as sources.
        """
        # Load mapping
        with open(mapping_file_path, 'r') as f:
            mapping = json.load(f)

        modified = False

        # Process account CSV (tab-delimited)
        account_headers = list(pd.read_csv(accounts_file_path, nrows=0, sep='\t').columns)
        account_items = mapping.get('accountItems', [])
        modified |= MRIImportManager._add_missing_sources(account_headers, account_items)

        # Process location CSV (tab-delimited)
        location_headers = list(pd.read_csv(locations_file_path, nrows=0, sep='\t').columns)
        location_items = mapping.get('locationItems', [])
        modified |= MRIImportManager._add_missing_sources(location_headers, location_items)

        # Save if modified
        if modified:
            with open(mapping_file_path, 'w') as f:
                json.dump(mapping, f, indent=4)
            print("Updated mapping.json with new source entries")

    def create_aws_bucket(self) -> requests.Response:
        """
        Create an AWS S3 bucket for file uploads.

        Returns:
            Response with Location header containing bucket URL

        Raises:
            IRPAPIError: If bucket creation fails
        """
        return self.client.request('POST', CREATE_AWS_BUCKET)

    def get_file_credentials(
        self,
        bucket_url: str,
        filename: str,
        filesize: int,
        file_type: str
    ) -> Dict[str, Any]:
        """
        Get temporary AWS credentials for file upload.

        Args:
            bucket_url: Bucket URL from create_aws_bucket response
            filename: Name of file to upload
            filesize: File size in kilobytes
            file_type: Type of file ('account' or 'location')

        Returns:
            Dict with decoded credentials including:
                - filename: str
                - file_id: str
                - aws_access_key_id: str
                - aws_secret_access_key: str
                - aws_session_token: str
                - s3_path: str
                - s3_region: str

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If credential request fails or response is malformed
        """
        validate_non_empty_string(bucket_url, "bucket_url")
        validate_non_empty_string(filename, "filename")
        validate_positive_int(filesize, "filesize")
        validate_non_empty_string(file_type, "file_type")

        data = {
            "fileName": filename,
            "fileSize": filesize,
            "fileType": file_type
        }

        try:
            response = self.client.request('POST', 'path', base_url=bucket_url, json=data)
            response_json = response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get file credentials: {e}")

        # Extract file ID from location header
        file_id = extract_id_from_location_header(response)

        # Decode credentials
        decoded_creds = decode_mri_credentials(response_json)

        return {
            'filename': filename,
            'file_id': file_id,
            **decoded_creds
        }

    def upload_file_to_s3(self, credentials: Dict[str, str], file_path: str) -> None:
        """
        Upload file to S3 using temporary credentials.

        Args:
            credentials: Credentials dict from get_file_credentials
            file_path: Path to file to upload

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If file upload fails
        """
        validate_file_exists(file_path, "file_path")

        # Validate required credential fields
        required_fields = ['aws_access_key_id', 'aws_secret_access_key',
                          'aws_session_token', 's3_region', 's3_path',
                          'file_id', 'filename']
        missing = [f for f in required_fields if f not in credentials]
        if missing:
            raise IRPValidationError(
                f"credentials missing required fields: {', '.join(missing)}"
            )

        try:
            print(f'Uploading file {file_path} to s3...')
            session = boto3.Session(
                aws_access_key_id=credentials['aws_access_key_id'],
                aws_secret_access_key=credentials['aws_secret_access_key'],
                aws_session_token=credentials['aws_session_token'],
                region_name=credentials['s3_region']
            )
            s3 = session.client("s3")

            # Parse S3 path
            s3_path_parts = credentials['s3_path'].split('/', 1)
            bucket = s3_path_parts[0]
            prefix = s3_path_parts[1] if len(s3_path_parts) > 1 else ""
            key = f"{prefix}/{credentials['file_id']}-{credentials['filename']}"

            # Configure transfer settings for optimized multipart uploads
            # Automatically handles multipart uploads for files > 8MB
            config = TransferConfig(
                multipart_threshold=8 * 1024 * 1024,  # 8MB threshold
                max_concurrency=10,                    # 10 concurrent threads
                multipart_chunksize=8 * 1024 * 1024,   # 8MB chunks
                use_threads=True
            )

            # Use upload_file for automatic multipart handling and better performance
            s3.upload_file(
                file_path,
                bucket,
                key,
                ExtraArgs={'ContentType': 'text/csv'},
                Config=config
            )
            print('File uploaded!')
        except FileNotFoundError:
            raise IRPFileError(f"File not found: {file_path}")
        except Exception as e:
            raise IRPFileError(f"Failed to upload file to S3: {e}")

    def upload_mapping_file(self, file_path: str, bucket_id: str) -> requests.Response:
        """
        Upload MRI mapping file to bucket.

        Args:
            file_path: Path to JSON mapping file
            bucket_id: Bucket ID from create_aws_bucket

        Returns:
            Response from mapping file upload

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If file cannot be read or is invalid JSON
            IRPAPIError: If upload fails
        """
        validate_file_exists(file_path, "file_path")
        validate_non_empty_string(bucket_id, "bucket_id")

        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
        except FileNotFoundError:
            raise IRPFileError(f"Mapping file not found: {file_path}")
        except json.JSONDecodeError as e:
            raise IRPFileError(
                f"Invalid JSON in mapping file '{file_path}': {e}"
            )
        except Exception as e:
            raise IRPFileError(
                f"Failed to read mapping file '{file_path}': {e}"
            )

        try:
            return self.client.request(
                'POST',
                CREATE_MAPPING.format(bucket_id=bucket_id),
                json=data
            )
        except Exception as e:
            raise IRPAPIError(f"Failed to upload mapping file: {e}")

    def get_import_job(self, workflow_id: int) -> Dict[str, Any]:
        """
        Retrieve MRI import workflow status by workflow ID.

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
            response = self.client.request('GET', GET_WORKFLOW_BY_ID.format(workflow_id=workflow_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get import job status for workflow ID {workflow_id}: {e}")

    def submit_import_job(
        self,
        edm_name: str,
        portfolio_id: int,
        bucket_id: int,
        accounts_file_id: int,
        locations_file_id: int,
        mapping_file_id: int,
        delimiter: str = "COMMA",
        skip_lines: int = 1,
        currency: str = "USD",
        append_locations: bool = False
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Submit MRI import job without polling (returns immediately).

        This method submits the import workflow after files have been uploaded.
        Use poll_import_job_to_completion() to track the workflow to completion.
        For a complete end-to-end import with polling, use import_from_files() instead.

        Args:
            edm_name: Target EDM name
            portfolio_id: Target portfolio ID
            bucket_id: AWS bucket ID
            accounts_file_id: Uploaded accounts file ID
            locations_file_id: Uploaded locations file ID
            mapping_file_id: Uploaded mapping file ID
            delimiter: File delimiter (default: "COMMA")
            skip_lines: Number of header lines to skip (default: 1)
            currency: Currency code (default: "USD")
            append_locations: Append to existing locations (default: False)

        Returns:
            Tuple of (workflow_id, request_body) where request_body is the HTTP request payload

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If import submission fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(portfolio_id, "portfolio_id")
        validate_positive_int(bucket_id, "bucket_id")
        validate_positive_int(accounts_file_id, "accounts_file_id")
        validate_positive_int(locations_file_id, "locations_file_id")
        validate_positive_int(mapping_file_id, "mapping_file_id")

        data = {
            "importType": "MRI",
            "bucketId": bucket_id,
            "dataSourceName": edm_name,
            "accountsFileId": accounts_file_id,
            "locationsFileId": locations_file_id,
            "mappingFileId": mapping_file_id,
            "delimiter": delimiter,
            "skipLines": skip_lines,
            "currency": currency,
            "portfolioId": portfolio_id,
            "appendLocations": append_locations
        }

        try:
            response = self.client.request('POST', EXECUTE_IMPORT, json=data)
            workflow_id = extract_id_from_location_header(response, "MRI import submission")
            return int(workflow_id), data
        except Exception as e:
            raise IRPAPIError(f"Failed to submit MRI import: {e}")
            

    def poll_import_job_batch_to_completion(
        self,
        workflow_ids: List[int],
        interval: int = 20,
        timeout: int = 600000
    ) -> List[Dict[str, Any]]:
        """
        Poll multiple MRI import workflows until all complete or timeout.

        Args:
            workflow_ids: List of workflow IDs
            interval: Polling interval in seconds (default: 20)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            List of final workflow status details for all workflows

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If workflows time out
            IRPAPIError: If polling fails
        """
        validate_list_not_empty(workflow_ids, "workflow_ids")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling batch import workflow ids: {','.join(str(item) for item in workflow_ids)}")

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
                response = self.client.request('GET', GET_WORKFLOWS, params=params)
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
                return all_workflows

            if time.time() - start > timeout:
                raise IRPJobError(
                    f"Batch import workflows did not complete within {timeout} seconds"
                )
            time.sleep(interval)


    def submit_mri_import_job(
        self,
        edm_name: str,
        portfolio_name: str,
        accounts_file_name: str,
        locations_file_name: str,
        mapping_file_name: str,
        files_directory: Optional[str] = None,
        mapping_directory: Optional[str] = None,
        delimiter: str = "COMMA",
        skip_lines: int = 1,
        currency: str = "USD",
        append_locations: bool = False
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Submit a single MRI import job (without polling).

        This method handles the complete submission process for one import:
        1. Lookup EDM and portfolio
        2. Validate file paths
        3. Create AWS bucket
        4. Upload accounts, locations, and mapping files to S3
        5. Submit import job

        Args:
            edm_name: Target EDM name
            portfolio_name: Target portfolio name
            accounts_file_name: Accounts CSV file name
            locations_file_name: Locations CSV file name
            mapping_file_name: Mapping JSON file name
            files_directory: Directory containing the CSV files (optional).
                If None, automatically determines based on active cycle
            mapping_directory: Directory containing the mapping file (optional).
                If None, automatically determines based on active cycle
            delimiter: File delimiter (default: "COMMA")
            skip_lines: Number of header lines to skip (default: 1)
            currency: Currency code (default: "USD")
            append_locations: Append to existing locations (default: False)

        Returns:
            Tuple of (workflow_id, request_body) where request_body is the HTTP request payload

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If files cannot be found or uploaded
            IRPAPIError: If any API call fails

        Example:
            ```python
            # Automatic directory resolution
            workflow_id = mri_manager.submit_mri_import_job(
                edm_name="RM_EDM_202511_Quarterly_USFL",
                portfolio_name="USFL_Other",
                accounts_file_name="Modeling_202511_Moodys_Quarterly_OtherFld_Account.csv",
                locations_file_name="Modeling_202511_Moodys_Quarterly_OtherFld_Location.csv",
                mapping_file_name="mapping.json"
            )

            # Explicit directory paths
            workflow_id = mri_manager.submit_mri_import_job(
                edm_name="RM_EDM_202511_Quarterly_USFL",
                portfolio_name="USFL_Other",
                accounts_file_name="Modeling_202511_Moodys_Quarterly_OtherFld_Account.csv",
                locations_file_name="Modeling_202511_Moodys_Quarterly_OtherFld_Location.csv",
                mapping_file_name="mapping.json",
                files_directory="/path/to/data",
                mapping_directory="/path/to/mapping"
            )
            ```
        """
        # Validate inputs
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(portfolio_name, "portfolio_name")
        validate_non_empty_string(accounts_file_name, "accounts_file_name")
        validate_non_empty_string(locations_file_name, "locations_file_name")
        validate_non_empty_string(mapping_file_name, "mapping_file_name")

        # Determine directories if not provided
        if files_directory is None or mapping_directory is None:
            from .utils import get_cycle_file_directories
            file_dirs = get_cycle_file_directories()

            if files_directory is None:
                files_directory = file_dirs['data']
            if mapping_directory is None:
                mapping_directory = file_dirs['mapping']

        # Lookup EDM
        print(f"Looking up EDM: {edm_name}")
        edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if len(edms) != 1:
            raise IRPAPIError(f"Expected 1 EDM with name {edm_name}, found {len(edms)}")
        try:
            exposure_id = edms[0]['exposureId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract exposure ID for EDM '{edm_name}': {e}"
            ) from e

        # Lookup Portfolio
        print(f"Looking up portfolio: {portfolio_name}")
        portfolios = self.portfolio_manager.search_portfolios(
            exposure_id=exposure_id,
            filter=f"portfolioName=\"{portfolio_name}\""
        )
        if len(portfolios) == 0:
            raise IRPAPIError(f"Portfolio with name {portfolio_name} not found")
        if len(portfolios) > 1:
            raise IRPAPIError(
                f"{len(portfolios)} portfolios found with name {portfolio_name}, please use a unique name"
            )
        try:
            portfolio_id = portfolios[0]['portfolioId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract portfolio ID for portfolio '{portfolio_name}': {e}"
            ) from e

        # Build file paths
        accounts_file_path = os.path.join(files_directory, accounts_file_name)
        locations_file_path = os.path.join(files_directory, locations_file_name)

        # Use mapping_directory if provided, otherwise use files_directory
        mapping_dir = mapping_directory if mapping_directory else files_directory
        mapping_file_path = os.path.join(mapping_dir, mapping_file_name)

        # Validate files exist
        validate_file_exists(accounts_file_path, "accounts_file_name")
        validate_file_exists(locations_file_path, "locations_file_name")
        validate_file_exists(mapping_file_path, "mapping_file_name")

        # Sync mapping.json with CSV headers (add any missing source entries)
        self._sync_mapping_with_csv_headers(mapping_file_path, accounts_file_path, locations_file_path)

        # Get file sizes
        accounts_size_kb = self.get_file_size_kb(accounts_file_path)
        locations_size_kb = self.get_file_size_kb(locations_file_path)

        if accounts_size_kb < 0 or locations_size_kb < 0:
            raise IRPFileError("Failed to determine file sizes")

        # Create AWS bucket
        print('Creating AWS bucket...')
        bucket_response = self.create_aws_bucket()
        print('AWS bucket created!')
        bucket_url = get_location_header(bucket_response, "AWS bucket creation response")
        bucket_id = extract_id_from_location_header(bucket_response, "AWS bucket creation response")

        # Upload accounts file
        print(f'Uploading accounts file: {accounts_file_name}')
        accounts_credentials = self.get_file_credentials(
            bucket_url,
            os.path.basename(accounts_file_name),
            accounts_size_kb,
            "account"
        )
        print('Access credentials received')
        self.upload_file_to_s3(accounts_credentials, accounts_file_path)

        # Upload locations file
        print(f'Uploading locations file: {locations_file_name}')
        locations_credentials = self.get_file_credentials(
            bucket_url,
            os.path.basename(locations_file_name),
            locations_size_kb,
            "location"
        )
        self.upload_file_to_s3(locations_credentials, locations_file_path)

        # Upload mapping file
        print(f'Uploading mapping file: {mapping_file_name}')
        mapping_response = self.upload_mapping_file(mapping_file_path, bucket_id)
        mapping_file_id = mapping_response.json()

        # Submit MRI import (without polling)
        print(f'Submitting import job for {edm_name}/{portfolio_name}...')
        workflow_id, http_request_body = self.submit_import_job(
            edm_name,
            int(portfolio_id),
            int(bucket_id),
            int(accounts_credentials['file_id']),
            int(locations_credentials['file_id']),
            mapping_file_id,
            delimiter=delimiter,
            skip_lines=skip_lines,
            currency=currency,
            append_locations=append_locations
        )
        print(f'Import job submitted with workflow ID: {workflow_id}')
        return workflow_id, http_request_body


    def get_file_size_kb(self, file_path: str) -> int:
        """
        Get file size in kilobytes.

        Args:
            file_path: Path to file

        Returns:
            File size in kilobytes, or -1 if file does not exist

        Note:
            This method returns -1 for backwards compatibility.
            Consider using validate_file_exists() instead for better error handling.
        """
        if not os.path.exists(file_path):
            return -1

        file_size_bytes = os.path.getsize(file_path)
        file_size_kb = int(file_size_bytes / 1024)
        return file_size_kb
