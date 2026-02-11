"""
S3 upload and download operations for IRP Integration.

Handles file uploads to AWS S3 using temporary credentials provided by
Moody's Risk Modeler API, and file downloads from CloudFront/presigned URLs.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, BinaryIO
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import TransferConfig
import requests

from .exceptions import IRPFileError, IRPValidationError
from .validators import validate_non_empty_string, validate_file_exists
from .utils import decode_presign_params


logger = logging.getLogger(__name__)


# Content type mapping by file extension
CONTENT_TYPE_MAP = {
    '.bak': 'application/octet-stream',
    '.csv': 'text/csv',
    '.json': 'application/json',
    '.txt': 'text/plain',
    '.zip': 'application/zip',
    '.xml': 'application/xml',
}

DEFAULT_CONTENT_TYPE = 'application/octet-stream'


class S3Manager:
    """Manager for S3 upload and CloudFront download operations."""

    def __init__(self, transfer_config: Optional[TransferConfig] = None) -> None:
        """
        Initialize S3 Manager.

        Args:
            transfer_config: Optional boto3 TransferConfig for multipart uploads.
                If not provided, uses default optimized settings.
        """
        self._transfer_config = transfer_config or TransferConfig(
            multipart_threshold=8 * 1024 * 1024,  # 8MB threshold
            max_concurrency=10,                    # 10 concurrent threads
            multipart_chunksize=8 * 1024 * 1024,   # 8MB chunks
            use_threads=True
        )

    # =========================================================================
    # UPLOAD METHODS
    # =========================================================================

    def upload_file(
        self,
        file_path: str,
        upload_details: Dict[str, Any],
        content_type: Optional[str] = None
    ) -> None:
        """
        Upload file to S3 using credentials from API response.

        This method handles the S3 upload for EDM/RDM import workflows.
        It extracts the upload URL and credentials from the API response
        and performs a multipart upload.

        Args:
            file_path: Path to the file to upload
            upload_details: Upload details dict from create import folder response,
                containing 'uploadUrl' and 'presignParams' (with base64-encoded
                credentials)
            content_type: Optional content type override. If not provided,
                inferred from file extension.

        Raises:
            IRPValidationError: If parameters are invalid or required fields missing
            IRPFileError: If file upload fails

        Example:
            ```python
            # From create import folder response:
            # response['uploadDetails']['exposureFile']
            upload_details = {
                "fileUri": "platform/import/v1/folders/39073/files/105108",
                "presignParams": {
                    "accessKeyId": "<base64>",
                    "secretAccessKey": "<base64>",
                    "sessionToken": "<base64>",
                    "path": "<base64>",
                    "region": "<base64>"
                },
                "uploadUrl": "https://bucket.s3.amazonaws.com/path/to/file.bak"
            }
            s3_manager.upload_file("/path/to/file.bak", upload_details)
            ```
        """
        validate_file_exists(file_path, "file_path")

        # Extract and validate required fields from upload_details
        upload_url, credentials = self._parse_upload_details(upload_details)

        # Parse S3 bucket and key from upload URL
        bucket, key = self._parse_s3_url(upload_url)

        # Determine content type
        resolved_content_type = self._resolve_content_type(file_path, content_type)

        # Perform upload
        self._upload_to_s3(
            file_path=file_path,
            bucket=bucket,
            key=key,
            credentials=credentials,
            content_type=resolved_content_type
        )

    def upload_fileobj(
        self,
        fileobj: BinaryIO,
        upload_details: Dict[str, Any],
        content_type: str
    ) -> None:
        """
        Upload file-like object to S3 using credentials from API response.

        Args:
            fileobj: File-like object (e.g., BytesIO, open file in 'rb' mode)
            upload_details: Upload details dict from create import folder response
            content_type: Content type for the upload (required for streams)

        Raises:
            IRPValidationError: If parameters are invalid or required fields missing
            IRPFileError: If file upload fails
        """
        validate_non_empty_string(content_type, "content_type")

        # Extract and validate required fields from upload_details
        upload_url, credentials = self._parse_upload_details(upload_details)

        # Parse S3 bucket and key from upload URL
        bucket, key = self._parse_s3_url(upload_url)

        # Perform upload
        self._upload_fileobj_to_s3(
            fileobj=fileobj,
            bucket=bucket,
            key=key,
            credentials=credentials,
            content_type=content_type
        )

    def upload_file_from_credentials(
        self,
        file_path: str,
        credentials: Dict[str, str],
        bucket: str,
        key: str,
        content_type: Optional[str] = None
    ) -> None:
        """
        Upload file to S3 using pre-decoded credentials.

        Lower-level method for cases where credentials are already decoded
        (e.g., MRI import workflow).

        Args:
            file_path: Path to the file to upload
            credentials: Dict with decoded AWS credentials:
                - aws_access_key_id: str
                - aws_secret_access_key: str
                - aws_session_token: str
                - s3_region: str
            bucket: S3 bucket name
            key: S3 object key (path within bucket)
            content_type: Optional content type override

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If file upload fails
        """
        validate_file_exists(file_path, "file_path")
        validate_non_empty_string(bucket, "bucket")
        validate_non_empty_string(key, "key")

        # Validate credentials
        self._validate_credentials(credentials)

        # Determine content type
        resolved_content_type = self._resolve_content_type(file_path, content_type)

        # Perform upload
        self._upload_to_s3(
            file_path=file_path,
            bucket=bucket,
            key=key,
            credentials=credentials,
            content_type=resolved_content_type
        )

    # =========================================================================
    # DOWNLOAD METHODS
    # =========================================================================

    def download_from_url(
        self,
        url: str,
        destination_path: str,
        chunk_size: int = 8192,
        timeout: int = 300
    ) -> None:
        """
        Download file from CloudFront or presigned URL to local path.

        Args:
            url: Full URL including any signed parameters
            destination_path: Local path to save the file
            chunk_size: Download chunk size in bytes (default: 8192)
            timeout: Request timeout in seconds (default: 300)

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If download fails or file cannot be written
        """
        validate_non_empty_string(url, "url")
        validate_non_empty_string(destination_path, "destination_path")

        logger.info("Downloading from URL to %s...", destination_path)

        try:
            response = requests.get(url, stream=True, timeout=timeout)
            response.raise_for_status()

            with open(destination_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

            logger.info("Download complete: %s", destination_path)

        except requests.RequestException as e:
            logger.error("Failed to download from URL: %s", e)
            raise IRPFileError(f"Failed to download from URL: {e}")
        except IOError as e:
            logger.error("Failed to write file '%s': %s", destination_path, e)
            raise IRPFileError(f"Failed to write file '{destination_path}': {e}")

    def download_from_url_to_fileobj(
        self,
        url: str,
        fileobj: BinaryIO,
        chunk_size: int = 8192,
        timeout: int = 300
    ) -> None:
        """
        Download file from CloudFront or presigned URL to file-like object.

        Args:
            url: Full URL including any signed parameters
            fileobj: File-like object to write to (must be opened in binary write mode)
            chunk_size: Download chunk size in bytes (default: 8192)
            timeout: Request timeout in seconds (default: 300)

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If download fails or write fails
        """
        validate_non_empty_string(url, "url")

        logger.info("Downloading from URL to file object...")

        try:
            response = requests.get(url, stream=True, timeout=timeout)
            response.raise_for_status()

            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    fileobj.write(chunk)

            logger.info("Download complete")

        except requests.RequestException as e:
            logger.error("Failed to download from URL: %s", e)
            raise IRPFileError(f"Failed to download from URL: {e}")
        except IOError as e:
            logger.error("Failed to write to file object: %s", e)
            raise IRPFileError(f"Failed to write to file object: {e}")

    # =========================================================================
    # PRIVATE METHODS
    # =========================================================================

    def _parse_upload_details(
        self,
        upload_details: Dict[str, Any]
    ) -> tuple:
        """
        Parse and validate upload details from API response.

        Args:
            upload_details: Upload details dict containing uploadUrl and presignParams

        Returns:
            Tuple of (upload_url, decoded_credentials)

        Raises:
            IRPValidationError: If required fields are missing
        """
        # Validate uploadUrl exists
        upload_url = upload_details.get('uploadUrl')
        if not upload_url:
            raise IRPValidationError(
                "upload_details missing required field: uploadUrl"
            )

        # Validate and decode presignParams
        presign_params = upload_details.get('presignParams')
        if not presign_params:
            raise IRPValidationError(
                "upload_details missing required field: presignParams"
            )

        # Use existing utility to decode base64 credentials
        credentials = decode_presign_params(presign_params)

        return upload_url, credentials

    def _parse_s3_url(self, upload_url: str) -> tuple:
        """
        Parse S3 bucket and key from upload URL.

        Supports both path-style and virtual-hosted-style S3 URLs:
        - Path-style: https://s3.region.amazonaws.com/bucket/key
        - Virtual-hosted: https://bucket.s3.amazonaws.com/key
        - Virtual-hosted with region: https://bucket.s3.region.amazonaws.com/key

        Args:
            upload_url: Full S3 upload URL

        Returns:
            Tuple of (bucket_name, object_key)

        Raises:
            IRPValidationError: If URL cannot be parsed
        """
        try:
            parsed = urlparse(upload_url)
            host = parsed.netloc
            path = parsed.path.lstrip('/')

            # Virtual-hosted style: bucket.s3.amazonaws.com or bucket.s3.region.amazonaws.com
            if '.s3.' in host or '.s3-' in host:
                bucket = host.split('.s3')[0]
                key = path
            # Path-style: s3.amazonaws.com/bucket/key or s3.region.amazonaws.com/bucket/key
            elif host.startswith('s3.') or host.startswith('s3-'):
                parts = path.split('/', 1)
                bucket = parts[0]
                key = parts[1] if len(parts) > 1 else ''
            else:
                raise ValueError(f"Unrecognized S3 URL format: {upload_url}")

            if not bucket or not key:
                raise ValueError(f"Could not extract bucket/key from URL: {upload_url}")

            logger.debug("Parsed S3 URL — bucket: %s, key: %s", bucket, key)
            return bucket, key

        except Exception as e:
            raise IRPValidationError(
                f"Failed to parse S3 URL '{upload_url}': {e}"
            )

    def _validate_credentials(self, credentials: Dict[str, str]) -> None:
        """
        Validate that credentials dict contains all required fields.

        Args:
            credentials: Credentials dict to validate

        Raises:
            IRPValidationError: If required fields are missing
        """
        required_fields = [
            'aws_access_key_id',
            'aws_secret_access_key',
            'aws_session_token',
            's3_region'
        ]
        missing = [f for f in required_fields if f not in credentials or not credentials[f]]
        if missing:
            raise IRPValidationError(
                f"credentials missing required fields: {', '.join(missing)}"
            )

    def _resolve_content_type(
        self,
        file_path: str,
        content_type: Optional[str]
    ) -> str:
        """
        Resolve content type from explicit value or file extension.

        Args:
            file_path: Path to file (used to infer extension)
            content_type: Explicit content type (takes precedence if provided)

        Returns:
            Resolved content type string
        """
        if content_type:
            return content_type

        extension = Path(file_path).suffix.lower()
        resolved = CONTENT_TYPE_MAP.get(extension, DEFAULT_CONTENT_TYPE)
        logger.debug("Resolved content type for '%s': %s", extension, resolved)
        return resolved

    def _upload_to_s3(
        self,
        file_path: str,
        bucket: str,
        key: str,
        credentials: Dict[str, str],
        content_type: str
    ) -> None:
        """
        Perform the actual S3 file upload.

        Args:
            file_path: Local file path
            bucket: S3 bucket name
            key: S3 object key
            credentials: Decoded AWS credentials
            content_type: Content type for the upload

        Raises:
            IRPFileError: If upload fails
        """
        try:
            logger.info("Uploading file to S3...")
            logger.debug("Upload target: s3://%s/%s", bucket, key)

            session = boto3.Session(
                aws_access_key_id=credentials['aws_access_key_id'],
                aws_secret_access_key=credentials['aws_secret_access_key'],
                aws_session_token=credentials['aws_session_token'],
                region_name=credentials['s3_region']
            )
            s3_client = session.client("s3")

            s3_client.upload_file(
                file_path,
                bucket,
                key,
                ExtraArgs={'ContentType': content_type},
                Config=self._transfer_config
            )

            logger.info("File uploaded successfully")

        except FileNotFoundError:
            logger.error("File not found: %s", file_path)
            raise IRPFileError(f"File not found: {file_path}")
        except Exception as e:
            logger.error("Failed to upload file to S3: %s", e)
            raise IRPFileError(f"Failed to upload file to S3: {e}")

    def _upload_fileobj_to_s3(
        self,
        fileobj: BinaryIO,
        bucket: str,
        key: str,
        credentials: Dict[str, str],
        content_type: str
    ) -> None:
        """
        Perform the actual S3 file object upload.

        Args:
            fileobj: File-like object to upload
            bucket: S3 bucket name
            key: S3 object key
            credentials: Decoded AWS credentials
            content_type: Content type for the upload

        Raises:
            IRPFileError: If upload fails
        """
        try:
            logger.info("Uploading file object to S3...")
            logger.debug("Upload target: s3://%s/%s", bucket, key)

            session = boto3.Session(
                aws_access_key_id=credentials['aws_access_key_id'],
                aws_secret_access_key=credentials['aws_secret_access_key'],
                aws_session_token=credentials['aws_session_token'],
                region_name=credentials['s3_region']
            )
            s3_client = session.client("s3")

            s3_client.upload_fileobj(
                fileobj,
                bucket,
                key,
                ExtraArgs={'ContentType': content_type},
                Config=self._transfer_config
            )

            logger.info("File uploaded successfully")

        except Exception as e:
            logger.error("Failed to upload file object to S3: %s", e)
            raise IRPFileError(f"Failed to upload file object to S3: {e}")
