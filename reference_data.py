"""
Reference data management operations.

Handles retrieval and creation of reference data including
model profiles, output profiles, event rate schemes, currencies, and tags.
"""

from typing import Dict, List, Any
from .client import Client
from .constants import (
    SEARCH_CURRENCIES, SEARCH_CURRENCY_SCHEME_VINTAGES, GET_TAGS, CREATE_TAG,
    GET_MODEL_PROFILES, GET_OUTPUT_PROFILES, GET_EVENT_RATE_SCHEME,
    SEARCH_SIMULATION_SETS, SEARCH_PET_METADATA, SEARCH_SOFTWARE_MODEL_VERSION_MAP
)
from .exceptions import IRPAPIError
from .validators import validate_non_empty_string, validate_list_not_empty, validate_positive_int
from .utils import extract_id_from_location_header


def _build_analysis_currency_dict(vintage: Dict[str, Any]) -> Dict[str, str]:
    """
    Build currency dict for analysis requests from a currency scheme vintage.

    Args:
        vintage: Currency scheme vintage dict from API with keys:
            - effectiveDate: ISO date string (e.g., "2025-05-28T00:00:00.000Z")
            - currencySchemeCode: Scheme code (e.g., "RMS")
            - vintage: Vintage code (e.g., "RL25")

    Returns:
        Currency dict with asOfDate (date only), code, scheme, and vintage
    """
    # Extract date portion only (API returns full timestamp but expects date only)
    effective_date = vintage["effectiveDate"].split("T")[0]
    return {
        "asOfDate": effective_date,
        "code": "USD",
        "scheme": vintage["currencySchemeCode"],
        "vintage": vintage["vintage"]
    }


def _build_default_analysis_currency_dict() -> Dict[str, str]:
    """
    Build default currency dict for analysis requests.

    Note: This is a fallback helper used when the currency scheme
    vintage cannot be retrieved from the API.

    Returns:
        Currency dict with default values
    """
    return {
        "asOfDate": "2025-05-28",
        "code": "USD",
        "scheme": "RMS",
        "vintage": "RL25"
    }


class ReferenceDataManager:
    """Manager for reference data operations."""

    def __init__(self, client: Client) -> None:
        """
        Initialize reference data manager.

        Args:
            client: IRP API client instance
        """
        self.client = client


    def get_model_profiles(self) -> Dict[str, Any]:
        """
        Retrieve all model profiles.

        Returns:
            Dict containing model profile list

        Raises:
            IRPAPIError: If request fails
        """
        try:
            response = self.client.request('GET', GET_MODEL_PROFILES)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get model profiles: {e}")


    def get_model_profile_by_name(self, profile_name: str) -> Dict[str, Any]:
        """
        Retrieve model profile by name.

        Args:
            profile_name: Model profile name

        Returns:
            Dict containing model profile details

        Raises:
            IRPValidationError: If profile_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(profile_name, "profile_name")

        params = {'name': profile_name}

        try:
            response = self.client.request('GET', GET_MODEL_PROFILES, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get model profile '{profile_name}': {e}")


    def get_output_profiles(self) -> List[Dict[str, Any]]:
        """
        Retrieve all output profiles.

        Returns:
            Dict containing output profile list

        Raises:
            IRPAPIError: If request fails
        """
        try:
            response = self.client.request('GET', GET_OUTPUT_PROFILES)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get output profiles: {e}")


    def get_output_profile_by_name(self, profile_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve output profile by name.

        Args:
            profile_name: Output profile name

        Returns:
            Dict containing output profile details

        Raises:
            IRPValidationError: If profile_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(profile_name, "profile_name")

        params = {'name': profile_name}

        try:
            response = self.client.request('GET', GET_OUTPUT_PROFILES, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get output profile '{profile_name}': {e}")


    def get_event_rate_schemes(self) -> Dict[str, Any]:
        """
        Retrieve all active event rate schemes.

        Returns:
            Dict containing event rate scheme list

        Raises:
            IRPAPIError: If request fails
        """
        params = {'where': 'isActive=True'}

        try:
            response = self.client.request('GET', GET_EVENT_RATE_SCHEME, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get event rate schemes: {e}")


    def get_event_rate_scheme_by_name(
        self,
        scheme_name: str,
        peril_code: str = None,
        model_region_code: str = None
    ) -> Dict[str, Any]:
        """
        Retrieve event rate scheme by name with optional peril and region filtering.

        When the same event rate scheme name exists for multiple peril/region combinations,
        use the peril_code and model_region_code parameters to filter to the correct one.
        These values can be obtained from the corresponding model profile.

        Args:
            scheme_name: Event rate scheme name
            peril_code: Optional peril code (e.g., "CS", "WS") to filter results
            model_region_code: Optional model region code (e.g., "NACS", "NAWS") to filter results

        Returns:
            Dict containing event rate scheme details

        Raises:
            IRPValidationError: If scheme_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(scheme_name, "scheme_name")

        # Build where clause with optional peril and region filters
        where_parts = [f'eventRateSchemeName="{scheme_name}"']
        if peril_code:
            where_parts.append(f'perilCode="{peril_code}"')
        if model_region_code:
            where_parts.append(f'modelRegionCode="{model_region_code}"')

        params = {'where': ' AND '.join(where_parts)}

        try:
            response = self.client.request('GET', GET_EVENT_RATE_SCHEME, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get event rate scheme '{scheme_name}': {e}")


    def search_currencies(self, where_clause: str = "") -> Dict[str, Any]:
        """
        Search currencies with optional filtering.

        Args:
            where_clause: Optional filter clause

        Returns:
            List containing currencies

        Raises:
            IRPAPIError: If request fails
        """
        params = {}
        if where_clause:
            params['where'] = where_clause

        try:
            response = self.client.request('GET', SEARCH_CURRENCIES, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search currencies: {e}")


    def search_currency_scheme_vintages(self, where_clause: str = "") -> Dict[str, Any]:
        """
        Search currency scheme vintages with optional filtering.

        Args:
            where_clause: Optional filter clause

        Returns:
            Dict containing currency scheme vintages

        Raises:
            IRPAPIError: If request fails
        """
        params = {}
        if where_clause:
            params['where'] = where_clause

        try:
            response = self.client.request('GET', SEARCH_CURRENCY_SCHEME_VINTAGES, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search currency scheme vintages: {e}")


    def get_latest_currency_scheme_vintage(self) -> Dict[str, Any]:
        """
        Get the latest RMS currency scheme vintage by effective date.

        Returns:
            Dict containing the currency scheme vintage with the most recent effectiveDate

        Raises:
            IRPAPIError: If request fails or no vintages found
        """
        where_clause = "currencySchemeCode=\"RMS\""
        response = self.search_currency_scheme_vintages(where_clause)

        try:
            items = response['items']
            if not items:
                raise IRPAPIError("No RMS currency scheme vintages found")
            latest = max(items, key=lambda x: x['effectiveDate'])
            return latest
        except KeyError as e:
            raise IRPAPIError(f"Failed to extract items from currency scheme vintages response: {e}") from e


    def get_analysis_currency(self) -> Dict[str, str]:
        """
        Get currency dict for analysis requests.

        Attempts to get the latest RMS currency scheme vintage from the API.
        Falls back to default values if the API call fails.

        Returns:
            Currency dict with asOfDate, code, scheme, and vintage
        """
        try:
            latest_vintage = self.get_latest_currency_scheme_vintage()
            return _build_analysis_currency_dict(latest_vintage)
        except IRPAPIError:
            return _build_default_analysis_currency_dict()


    def get_currency_by_name(self, currency_name: str) -> Dict[str, Any]:
        """
        Retrieve currency by name.

        Args:
            currency_name: Currency name

        Returns:
            Dict containing currency details

        Raises:
            IRPValidationError: If currency_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(currency_name, "currency_name")
        where_clause = f"currencyName=\"{currency_name}\""
        currencies_response = self.search_currencies(where_clause)
        try:
            currency = currencies_response['items'][0]
            return currency
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract currency '{currency_name}' from search response: {e}"
            ) from e


    def get_tag_by_name(self, tag_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve tag by name.

        Args:
            tag_name: Tag name

        Returns:
            List of dicts containing tag details

        Raises:
            IRPValidationError: If tag_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(tag_name, "tag_name")

        params = {
            "isActive": True,
            "filter": f"TAGNAME = '{tag_name}'"
        }

        try:
            response = self.client.request('GET', GET_TAGS, params=params)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get tag '{tag_name}': {e}")


    def create_tag(self, tag_name: str) -> Dict[str, str]:
        """
        Create new tag.

        Args:
            tag_name: Tag name

        Returns:
            Dict with tag ID

        Raises:
            IRPValidationError: If tag_name is invalid
            IRPAPIError: If request fails
        """
        validate_non_empty_string(tag_name, "tag_name")

        data = {"tagName": tag_name}

        try:
            response = self.client.request('POST', CREATE_TAG, json=data)
            tag_id = extract_id_from_location_header(response, "tag creation")
            return {"id": tag_id}
        except Exception as e:
            raise IRPAPIError(f"Failed to create tag '{tag_name}': {e}")


    def get_tag_ids_from_tag_names(self, tag_names: List[str]) -> List[int]:
        """
        Get or create tags by names and return their IDs.

        This method will create tags if they don't already exist.

        Args:
            tag_names: List of tag names

        Returns:
            List of tag IDs

        Raises:
            IRPValidationError: If tag_names is empty
            IRPAPIError: If request fails
        """
        validate_list_not_empty(tag_names, "tag_names")

        tag_ids = []
        for tag_name in tag_names:
            tag_search_response = self.get_tag_by_name(tag_name)
            if len(tag_search_response) > 0:
                try:
                    tag_id = tag_search_response[0]['tagId']
                except (KeyError, IndexError, TypeError) as e:
                    raise IRPAPIError(
                        f"Failed to extract tag ID from search response for '{tag_name}': {e}"
                    ) from e
                tag_ids.append(int(tag_id))
            else:
                created_tag = self.create_tag(tag_name)
                try:
                    tag_id = created_tag['id']
                except (KeyError, TypeError) as e:
                    raise IRPAPIError(
                        f"Failed to extract tag ID from created tag response for '{tag_name}': {e}"
                    ) from e
                tag_ids.append(int(tag_id))

        return tag_ids

    def get_all_simulation_sets(self) -> List[Dict[str, Any]]:
        """
        Get all active simulation sets.

        Simulation sets map event rate scheme IDs to simulation set IDs
        for ELT-based analyses. This fetches all active sets which can be
        filtered locally by event rate scheme ID.

        Returns:
            List of simulation set dicts

        Raises:
            IRPAPIError: If request fails
        """
        params = {
            'isActive': True,
            'isActivePEQ': True,
            'sort': 'id',
            'sortOrder': 1,
            'where': 'isActive=true'
        }

        try:
            response = self.client.request('GET', SEARCH_SIMULATION_SETS, params=params)
            return response.json().get('items', [])
        except Exception as e:
            raise IRPAPIError(f"Failed to get simulation sets: {e}")

    def get_simulation_set_by_event_rate_scheme_id(self, event_rate_scheme_id: int) -> Dict[str, Any]:
        """
        Get simulation set by event rate scheme ID.

        For ELT analyses, the simulationSetId in grouping requests comes from
        this lookup using the eventRateSchemeId from the analysis regions.

        Args:
            event_rate_scheme_id: Event rate scheme ID from analysis regions

        Returns:
            Dict containing simulation set details with 'id' being the simulationSetId

        Raises:
            IRPAPIError: If request fails or simulation set not found
        """

        simulation_sets = self.get_all_simulation_sets()

        for sim_set in simulation_sets:
            if sim_set.get('eventRateSchemeId') == event_rate_scheme_id:
                return sim_set

        raise IRPAPIError(
            f"No simulation set found for event rate scheme ID {event_rate_scheme_id}"
        )

    def get_simulation_set_by_region_peril_and_engine(
        self, region_code: str, peril_code: str, engine_version: str
    ) -> Dict[str, Any]:
        """
        Get simulation set by regionCode, perilCode, and engineVersion.

        This is a fallback method used when eventRateSchemeId is not available.
        The lookup uses regionCode + perilCode to build the broader modelRegionCode
        (e.g., "NA" + "WS" = "NAWS") since SimulationSet entries use broader regional
        codes, not sub-region-specific codes like "HTWS".

        Note: When multiple simulation sets match, returns the one with highest id
        (most recent). For precise matching, use get_simulation_set_by_event_rate_scheme_id
        with the eventRateSchemeId from the analysis additionalProperties.

        Args:
            region_code: Region code (e.g., "NA", "US", "CB")
            peril_code: Peril code (e.g., "WS", "EQ", "FL")
            engine_version: Engine version (e.g., "RL23", "HDv2.0")

        Returns:
            Dict containing simulation set details with 'id' being the simulationSetId

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails or simulation set not found
        """
        validate_non_empty_string(region_code, "region_code")
        validate_non_empty_string(peril_code, "peril_code")
        validate_non_empty_string(engine_version, "engine_version")

        # Build the broader modelRegionCode for SimulationSet lookup
        # e.g., "NA" + "WS" = "NAWS"
        sim_set_model_region_code = region_code + peril_code

        simulation_sets = self.get_all_simulation_sets()

        # Find matching simulation sets
        matching_sets = []
        for sim_set in simulation_sets:
            if sim_set.get('modelRegionCode') == sim_set_model_region_code:
                # Check if engineVersion is in the rlVersion list
                rl_version_str = sim_set.get('rlVersion', '')
                # rlVersion is comma-separated with spaces: "RL16, RL17, RL18"
                rl_versions = [v.strip() for v in rl_version_str.split(',')]
                if engine_version in rl_versions:
                    matching_sets.append(sim_set)

        if not matching_sets:
            raise IRPAPIError(
                f"No simulation set found for regionCode '{region_code}', "
                f"perilCode '{peril_code}', engineVersion '{engine_version}'"
            )

        # If only one match, return it
        if len(matching_sets) == 1:
            return matching_sets[0]

        # Multiple matches - return highest id (most recent)
        return max(matching_sets, key=lambda x: x.get('id', 0))

    def get_all_pet_metadata(self) -> List[Dict[str, Any]]:
        """
        Get all PET (Probabilistic Event Table) metadata.

        PET metadata maps PET IDs to simulation set IDs for PLT/HD-based analyses.

        Returns:
            List of PET metadata dicts

        Raises:
            IRPAPIError: If request fails
        """
        params = {'limit': 500, 'offset': 0}

        try:
            response = self.client.request('GET', SEARCH_PET_METADATA, params=params)
            return response.json().get('items', [])
        except Exception as e:
            raise IRPAPIError(f"Failed to get PET metadata: {e}")

    def get_pet_metadata_by_id(self, pet_id: int) -> Dict[str, Any]:
        """
        Get PET metadata by PET ID.

        For PLT/HD analyses, the simulationSetId in grouping requests is the
        PET ID itself (the 'id' field from PET metadata).

        Args:
            pet_id: PET ID from analysis regions

        Returns:
            Dict containing PET metadata details

        Raises:
            IRPValidationError: If pet_id is invalid
            IRPAPIError: If request fails or PET not found
        """
        validate_positive_int(pet_id, "pet_id")

        pet_metadata_list = self.get_all_pet_metadata()

        for pet in pet_metadata_list:
            if pet.get('id') == pet_id:
                return pet

        raise IRPAPIError(f"No PET metadata found for PET ID {pet_id}")

    def get_all_software_model_version_map(self) -> List[Dict[str, Any]]:
        """
        Get all active software model version mappings.

        This maps engine versions to model versions for grouping requests.

        Returns:
            List of version map dicts

        Raises:
            IRPAPIError: If request fails
        """
        params = {'isActive': True}

        try:
            response = self.client.request('GET', SEARCH_SOFTWARE_MODEL_VERSION_MAP, params=params)
            return response.json().get('items', [])
        except Exception as e:
            raise IRPAPIError(f"Failed to get software model version map: {e}")

    def get_model_version_by_engine_version(self, engine_version: str) -> str:
        """
        Get model version for a given engine version.

        Note: This method looks for any entry matching the softwareVersionCode.
        For more precise matching, use get_model_version_by_engine_and_region.

        Args:
            engine_version: Engine version string (e.g., "HDv2.0", "RL23")

        Returns:
            Model version string (e.g., "2.0", "23.0")

        Raises:
            IRPValidationError: If engine_version is invalid
            IRPAPIError: If request fails or mapping not found
        """
        validate_non_empty_string(engine_version, "engine_version")

        version_maps = self.get_all_software_model_version_map()

        for version_map in version_maps:
            if version_map.get('softwareVersionCode') == engine_version:
                return version_map['modelVersionCode']

        raise IRPAPIError(
            f"No model version mapping found for engine version '{engine_version}'"
        )

    def get_model_version_by_engine_region_peril(
        self, engine_version: str, region_code: str, peril_code: str
    ) -> str:
        """
        Get model version for a given engine version, region code, and peril code.

        This provides a precise lookup using the broader modelRegionCode (e.g., "NAWS")
        built from regionCode + perilCode, since SoftwareModelVersionMap uses broader
        codes, not sub-region-specific codes like "HTWS".

        Args:
            engine_version: Engine version string (e.g., "HDv2.0", "RL23")
            region_code: Region code (e.g., "NA", "US", "CB")
            peril_code: Peril code (e.g., "WS", "EQ", "FL")

        Returns:
            Model version string (e.g., "2.0", "11.0")

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If request fails or mapping not found
        """
        validate_non_empty_string(engine_version, "engine_version")
        validate_non_empty_string(region_code, "region_code")
        validate_non_empty_string(peril_code, "peril_code")

        # Build the broader modelRegionCode for lookup (e.g., "NA" + "WS" = "NAWS")
        broader_model_region_code = region_code + peril_code

        version_maps = self.get_all_software_model_version_map()

        for version_map in version_maps:
            if (version_map.get('softwareVersionCode') == engine_version and
                    version_map.get('modelRegionCode') == broader_model_region_code):
                return version_map['modelVersionCode']

        raise IRPAPIError(
            f"No model version mapping found for engine version '{engine_version}', "
            f"region code '{region_code}', peril code '{peril_code}'"
        )