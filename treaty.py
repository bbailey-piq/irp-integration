"""
Treaty Manager for IRP Integration.

Handles treaty-related operations including creation, retrieval,
and Line of Business (LOB) assignments.
"""

from typing import Dict, List, Any, Tuple
from .client import Client
from .constants import (
    CREATE_TREATY,
    SEARCH_TREATIES,
    TREATY_TYPES,
    TREATY_ATTACHMENT_BASES,
    TREATY_ATTACHMENT_LEVELS,
    CREATE_TREATY_LOB
)
from .exceptions import IRPAPIError, IRPValidationError, IRPReferenceDataError
from .validators import validate_list_not_empty, validate_non_empty_string, validate_positive_int, validate_non_negative_float, validate_non_negative_int
from .utils import extract_id_from_location_header


class TreatyManager:
    """Manager for treaty operations."""

    def __init__(self, client: Client, edm_manager=None, reference_data_manager=None):
        """
        Initialize Treaty Manager.

        Args:
            client: Client instance for API requests
            edm_manager: Optional EDMManager instance (lazy-loaded if None)
            reference_data_manager: Optional ReferenceDataManager instance (lazy-loaded if None)
        """
        self.client = client
        self._edm_manager = edm_manager
        self._reference_data_manager = reference_data_manager

    @property
    def edm_manager(self):
        """Lazy-load EDMManager to avoid circular imports."""
        if self._edm_manager is None:
            from .edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager

    @property
    def reference_data_manager(self):
        """Lazy-load ReferenceDataManager to avoid circular imports."""
        if self._reference_data_manager is None:
            from .reference_data import ReferenceDataManager
            self._reference_data_manager = ReferenceDataManager(self.client)
        return self._reference_data_manager


    def search_treaties(self, exposure_id: int, filter: str = '', limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search treaties for a given exposure ID.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter string
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)

        Returns:
            List of treaty dictionaries

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If API request fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        params = {'limit': limit, 'offset': offset}
        if filter:
            params['filter'] = filter
        try:
            response = self.client.request('GET', SEARCH_TREATIES.format(exposureId=exposure_id), params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search treaties: {e}")

    def search_treaties_paginated(self, exposure_id: int, filter: str = '') -> List[Dict[str, Any]]:
        """
        Search all treaties for a given exposure ID with automatic pagination.

        Fetches all pages of results matching the filter criteria.

        Args:
            exposure_id: Exposure ID
            filter: Optional filter string

        Returns:
            Complete list of all matching treaties across all pages

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If API request fails
        """
        validate_positive_int(exposure_id, "exposure_id")

        all_results = []
        offset = 0
        limit = 100

        while True:
            results = self.search_treaties(exposure_id=exposure_id, filter=filter, limit=limit, offset=offset)
            all_results.extend(results)

            # If we got fewer results than the limit, we've reached the end
            if len(results) < limit:
                break
            offset += limit

        return all_results


    def create_treaties(self, treaty_data_list: List[Dict[str, Any]]) -> List[int]:
        """
        Create multiple treaties.

        Args:
            treaty_data_list: List of treaty data dicts, each containing all required treaty fields

        Returns:
            List of treaty IDs

        Raises:
            IRPValidationError: If treaty_data_list is empty or invalid
            IRPAPIError: If treaty creation fails or EDM not found
        """
        validate_list_not_empty(treaty_data_list, "treaty_data_list")

        treaty_ids = []
        for treaty_data in treaty_data_list:
            try:
                # Returns tuple of (treaty_id, request_body) - we only need treaty_id here
                treaty_id, _ = self.create_treaty(
                    edm_name=treaty_data['edm_name'],
                    treaty_name=treaty_data['treaty_name'],
                    treaty_number=treaty_data['treaty_number'],
                    treaty_type=treaty_data['treaty_type'],
                    per_risk_limit=treaty_data['per_risk_limit'],
                    occurrence_limit=treaty_data['occurrence_limit'],
                    attachment_point=treaty_data['attachment_point'],
                    inception_date=treaty_data['inception_date'],
                    expiration_date=treaty_data['expiration_date'],
                    currency_name=treaty_data['currency_name'],
                    attachment_basis=treaty_data['attachment_basis'],
                    attachment_level=treaty_data['attachment_level'],
                    pct_covered=treaty_data['pct_covered'],
                    pct_placed=treaty_data['pct_placed'],
                    pct_share=treaty_data['pct_share'],
                    pct_retention=treaty_data['pct_retention'],
                    premium=treaty_data['premium'],
                    num_reinstatements=treaty_data['num_reinstatements'],
                    pct_reinstatement_charge=treaty_data['pct_reinstatement_charge'],
                    aggregate_limit=treaty_data['aggregate_limit'],
                    aggregate_deductible=treaty_data['aggregate_deductible'],
                    priority=treaty_data['priority']
                )
                treaty_ids.append(treaty_id)
            except KeyError as e:
                raise IRPAPIError(f"Missing data in create treaty data: {e}") from e

        return treaty_ids


    def create_treaty(
            self,
            edm_name: str,
            treaty_name: str,
            treaty_number: str,
            treaty_type: str,
            per_risk_limit: float,
            occurrence_limit: float,
            attachment_point: float,
            inception_date: str,
            expiration_date: str,
            currency_name: str,
            attachment_basis: str,
            attachment_level: str,
            pct_covered: float,
            pct_placed: float,
            pct_share: float,
            pct_retention: float,
            premium: float,
            num_reinstatements: int,
            pct_reinstatement_charge: float,
            aggregate_limit: float,
            aggregate_deductible: float,
            priority: int,
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Create a treaty with provided parameters.

        Args:
            edm_name: EDM name to create the treaty in
            treaty_name: Treaty name
            treaty_number: Treaty number (max 20 chars)
            treaty_type: Treaty type (must be in TREATY_TYPES)
            per_risk_limit: Per risk limit amount
            occurrence_limit: Occurrence limit amount
            attachment_point: Attachment point amount
            inception_date: Inception date (ISO format)
            expiration_date: Expiration date (ISO format)
            currency_name: Currency name (e.g., "US Dollar")
            attachment_basis: Attachment basis (must be in TREATY_ATTACHMENT_BASES)
            attachment_level: Attachment level (must be in TREATY_ATTACHMENT_LEVELS)
            pct_covered: Percent covered
            pct_placed: Percent placed
            pct_share: Percent share
            pct_retention: Percent retention
            premium: Premium amount
            num_reinstatements: Number of reinstatements
            pct_reinstatement_charge: Percent reinstatement charge
            aggregate_limit: Aggregate limit amount
            aggregate_deductible: Aggregate deductible amount
            priority: Priority

        Returns:
            Tuple of (treaty_id, request_body) where request_body is the HTTP request payload

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If treaty creation fails or EDM not found
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(treaty_name, "treaty_name")
        validate_non_empty_string(treaty_number, "treaty_number")
        validate_non_empty_string(treaty_type, "treaty_type")
        validate_non_empty_string(inception_date, "inception_date")
        validate_non_empty_string(expiration_date, "expiration_date")
        validate_non_empty_string(currency_name, "currency")
        validate_non_empty_string(attachment_basis, "attachment_basis")
        validate_non_empty_string(attachment_level, "attachment_level")
        validate_non_negative_float(per_risk_limit, "per_risk_limit")
        validate_non_negative_float(occurrence_limit, "occurrence_limit")
        validate_non_negative_float(attachment_point, "attachment_point")
        validate_non_negative_float(pct_covered, "pct_covered")
        validate_non_negative_float(pct_placed, "pct_placed")
        validate_non_negative_float(pct_share, "pct_share")
        validate_non_negative_float(pct_retention, "pct_retention")
        validate_non_negative_float(premium, "premium")
        validate_non_negative_int(num_reinstatements, "num_reinstatements")
        validate_non_negative_float(pct_reinstatement_charge, "pct_reinstatement_charge")
        validate_non_negative_float(aggregate_limit, "aggregate_limit")
        validate_non_negative_float(aggregate_deductible, "aggregate_deductible")

        if treaty_type not in TREATY_TYPES:
            raise IRPValidationError(
                f"Invalid treaty_type '{treaty_type}'. Must be one of: {list(TREATY_TYPES.keys())}"
            )

        if attachment_basis not in TREATY_ATTACHMENT_BASES:
            raise IRPValidationError(
                f"Invalid attachment_basis '{attachment_basis}'. Must be one of: {list(TREATY_ATTACHMENT_BASES.keys())}"
            )

        if attachment_level not in TREATY_ATTACHMENT_LEVELS:
            raise IRPValidationError(
                f"Invalid attachment_level '{attachment_level}'. Must be one of: {list(TREATY_ATTACHMENT_LEVELS.keys())}"
            )

        # Look up EDM to get exposure_id
        edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if len(edms) != 1:
            raise IRPAPIError(f"Expected 1 EDM with name '{edm_name}', found {len(edms)}")
        try:
            exposure_id = edms[0]['exposureId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(f"Failed to extract exposure ID for EDM '{edm_name}': {e}") from e

        try:
            cedant_response = self.edm_manager.get_cedants_by_edm(exposure_id)
            if not cedant_response:
                raise IRPReferenceDataError(f"No cedants found for EDM '{edm_name}'")
            if len(cedant_response) > 1:
                raise IRPReferenceDataError(f"Multiple cedants found for EDM '{edm_name}'")
            cedant = cedant_response[0]
            cedant_data = {
                "cedantId": cedant["cedantId"],
                "cedantName": cedant["cedantName"]
            }
        except (KeyError, TypeError) as e:
            raise IRPAPIError(
                f"Missing required fields in cedant data for EDM '{edm_name}': {e}"
            ) from e
        except IRPReferenceDataError:
            raise
        except Exception as e:
            raise IRPAPIError(f"Failed to retrieve cedants for EDM '{edm_name}': {e}")
        
        try:
            currency_response = self.reference_data_manager.get_currency_by_name(currency_name)
            currency_data = {
                "id": currency_response["currencyId"],
                "code": currency_response["currencyCode"],
                "name": currency_response["currencyName"]
            }
        except (KeyError, TypeError) as e:
            raise IRPAPIError(
                f"Missing required fields in currency data for currency '{currency_name}': {e}"
            ) from e

        data = {
            "treatyName": treaty_name,
            "treatyNumber": treaty_number[:20],  # Truncate to 20 chars
            "treatyType": TREATY_TYPES[treaty_type],
            "riskLimit": per_risk_limit,
            "occurrenceLimit": occurrence_limit,
            "attachmentPoint": attachment_point,
            "effectiveDate": inception_date,
            "expirationDate": expiration_date,
            "currency": currency_data,
            "attachmentBasis": TREATY_ATTACHMENT_BASES[attachment_basis],
            "attachmentLevel": TREATY_ATTACHMENT_LEVELS[attachment_level],
            "percentageCovered": pct_covered,
            "percentagePlaced": pct_placed,
            "percentageRiShare": pct_share,
            "percentageRetention": pct_retention,
            "premium": premium,
            "numberOfReinstatements": num_reinstatements,
            "reinstatementCharge": pct_reinstatement_charge,
            "aggregateLimit": aggregate_limit,
            "aggregateDeductible": aggregate_deductible,
            "priority": priority,
            "cedant": cedant_data
        }

        try:
            response = self.client.request('POST', CREATE_TREATY.format(exposureId=exposure_id), json=data)
            treaty_id = extract_id_from_location_header(response, "treaty creation")

            lobs = self.edm_manager.get_lobs_by_edm(exposure_id)
            for lob in lobs:
                self.create_treaty_lob(exposure_id, int(treaty_id), int(lob['lobId']), lob['lobName'])

            return int(treaty_id), data
        except KeyError as e:
            raise IRPAPIError(f"Missing expected LOB field during treaty creation: {e}")
        except Exception as e:
            raise IRPAPIError(f"Failed to create treaty '{treaty_name}': {e}")
        

    def create_treaty_lob(self, exposure_id: int, treaty_id: int, lob_id: int, lobName: str) -> int:
        """
        Create a Line of Business (LOB) for a treaty.

        Args:
            exposure_id: Exposure ID
            treaty_id: Treaty ID
            lob_id: LOB ID
            lobName: LOB Name

        Returns:
            LOB ID of the created LOB

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If LOB creation fails
        """
        validate_positive_int(exposure_id, "exposure_id")
        validate_positive_int(treaty_id, "treaty_id")
        validate_positive_int(lob_id, "lob_id")
        validate_non_empty_string(lobName, "lobName")

        data = {
            "lobId": lob_id,
            "lobName": lobName
        }

        try:
            response = self.client.request(
                'POST',
                CREATE_TREATY_LOB.format(exposureId=exposure_id, id=treaty_id),
                json=data
            )
            created_lob_id = extract_id_from_location_header(response, "treaty LOB creation")
            return int(created_lob_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to create treaty LOB '{lobName}': {e}")
