"""
MRI Import Manager for IRP Integration.

Handles Multi-Risk Insurance (MRI) data imports via the Platform Import API.
Files are uploaded to S3 and import jobs are submitted through the
/platform/import/v1 endpoints.
"""

import os
from typing import Dict, Any, Optional, Tuple

from .client import Client
from .constants import CREATE_IMPORT_FOLDER, SUBMIT_IMPORT_JOB
from .exceptions import IRPAPIError
from .validators import validate_non_empty_string, validate_file_exists
from .s3 import S3Manager
from .utils import extract_id_from_location_header


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

    def submit_mri_import_job(
        self,
        edm_name: str,
        portfolio_name: str,
        accounts_file_path: str,
        locations_file_path: str,
        mapping_file_path: Optional[str] = None,
        delimiter: str = "TAB"
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Submit an MRI import job via the Platform Import API.

        This method handles the complete MRI import workflow:
        1. Look up EDM and portfolio
        2. Create import folder (get S3 credentials)
        3. Upload accounts, locations, and optionally mapping files to S3
        4. Submit import job

        Args:
            edm_name: Target EDM name
            portfolio_name: Target portfolio name within the EDM
            accounts_file_path: Path to accounts CSV file
            locations_file_path: Path to locations CSV file
            mapping_file_path: Optional path to .mff mapping file
            delimiter: File delimiter (default: "TAB")

        Returns:
            Tuple of (job_id, request_body) where request_body is the HTTP request payload

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If file upload fails
            IRPAPIError: If any API call fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(portfolio_name, "portfolio_name")
        validate_file_exists(accounts_file_path, "accounts_file_path")
        validate_file_exists(locations_file_path, "locations_file_path")
        if mapping_file_path is not None:
            validate_file_exists(mapping_file_path, "mapping_file_path")

        s3_manager = S3Manager()

        # Step 1: Look up EDM
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

        # Step 2: Look up portfolio
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

        resource_uri = f'/platform/riskdata/v1/exposures/{exposure_id}/portfolios/{portfolio_id}'

        # Step 3: Create import folder
        file_types = ["accountsFile", "locationsFile"]
        if mapping_file_path is not None:
            file_types.append("mappingFile")

        folder_data = {
            "folderType": "MRI",
            "properties": {
                "fileExtension": "csv",
                "fileTypes": file_types
            }
        }
        print("Creating import folder...")
        response = self.client.request('POST', CREATE_IMPORT_FOLDER, json=folder_data)
        folder_response = response.json()

        try:
            folder_id = folder_response['folderId']
            upload_details = folder_response['uploadDetails']
        except (KeyError, TypeError) as e:
            raise IRPAPIError(
                f"Create import folder response missing required fields: {e}"
            ) from e

        # Step 4: Upload files to S3
        print(f"Uploading accounts file: {os.path.basename(accounts_file_path)}")
        try:
            accounts_upload = upload_details['accountsFile']
        except KeyError as e:
            raise IRPAPIError(f"Upload details missing accountsFile: {e}") from e
        s3_manager.upload_file(accounts_file_path, accounts_upload)

        print(f"Uploading locations file: {os.path.basename(locations_file_path)}")
        try:
            locations_upload = upload_details['locationsFile']
        except KeyError as e:
            raise IRPAPIError(f"Upload details missing locationsFile: {e}") from e
        s3_manager.upload_file(locations_file_path, locations_upload)

        if mapping_file_path is not None:
            print(f"Uploading mapping file: {os.path.basename(mapping_file_path)}")
            try:
                mapping_upload = upload_details['mappingFile']
            except KeyError as e:
                raise IRPAPIError(f"Upload details missing mappingFile: {e}") from e
            s3_manager.upload_file(mapping_file_path, mapping_upload)

        # Step 5: Submit import job
        settings = {
            "folderId": int(folder_id),
            "delimiter": delimiter
        }
        import_data = {
            "importType": "MRI",
            "resourceUri": resource_uri,
            "settings": settings
        }

        print(f"Submitting import job for {edm_name}/{portfolio_name}...")
        response = self.client.request('POST', SUBMIT_IMPORT_JOB, json=import_data)
        job_id = extract_id_from_location_header(response, "MRI import job submission")

        print(f"Import job submitted with job ID: {job_id}")
        return int(job_id), import_data
