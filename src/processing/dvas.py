import base64
import logging
from datetime import datetime, timezone
from typing import Literal

import requests

from processing import utils
from processing.config import Config
from processing.metadata_api import MetadataApi


class DvasError(Exception):
    pass


class Dvas:
    """Class for managing Cloudnet file metadata operations in the DVAS API."""

    def __init__(self, config: Config, md_api: MetadataApi):
        self.config = config
        self.md_api = md_api
        self.session = self._init_session()

    def upload(self, file: dict):
        """Upload Cloudnet file metadata to DVAS API and update Cloudnet data portal"""
        landing_page_url = utils.build_file_landing_page_url(file["uuid"])
        logging.info(f"Uploading {landing_page_url} metadata to DVAS")
        if not file["pid"]:
            logging.error("Skipping - volatile file")
            return
        if "geophysical" not in file["product"]["type"]:
            logging.error("Skipping - only geophysical products supported for now")
            return
        if "categorize" in file["product"]["id"]:
            logging.error("Skipping - categorize file")
            return
        if not file["site"]["dvasId"]:
            logging.error("Skipping - not DVAS site")
            return
        try:
            dvas_metadata = DvasMetadata(file, self.md_api)
            dvas_json = dvas_metadata.create_dvas_json()
            if len(dvas_json["md_content_information"]["attribute_descriptions"]) == 0:
                logging.error("Skipping - no ACTRIS variables")
                return
            self._delete_old_versions(file)
            dvas_id = self._post(dvas_json)
            self.md_api.update_dvas_info(
                file["uuid"], dvas_json["md_metadata"]["datestamp"], dvas_id
            )
        except DvasError as err:
            logging.error(f"Failed to upload {file['filename']} to DVAS")
            logging.debug(err)

    def delete(self, file: dict):
        """Delete Cloudnet file metadata from DVAS API"""
        logging.warning(
            f"Deleting Cloudnet file {file['uuid']} with dvasId {file['dvasId']} from DVAS"
        )
        url = f"{self.config.dvas_portal_url}/Metadata/delete/{file['dvasId']}"
        self._delete(url)

    def delete_all(self):
        """Delete all Cloudnet file metadata from DVAS API"""
        url = f"{self.config.dvas_portal_url}/Metadata/delete/all/{self.config.dvas_provider_id}"
        self._delete(url)
        logging.info("Done. All Cloudnet files deleted from DVAS")

    def _delete(self, url: str):
        auth = base64.b64encode(
            f"{self.config.dvas_username}:{self.config.dvas_password}".encode()
        ).decode()
        headers = {"X-Authorization": f"Basic {auth}"}
        res = self.session.delete(url, headers=headers)
        if not res.ok:
            raise DvasError(res)
        logging.debug(f"DELETE successful: {res.status_code} {res.text}")

    def _delete_old_versions(self, file: dict):
        """Delete all versions of the given file from DVAS API. To be used before posting new version."""
        versions = self.md_api.get(
            f"api/files/{file['uuid']}/versions", {"properties": ["dvasId"]}
        )
        for version in versions:
            if version["dvasId"] is None:
                continue
            logging.debug(f"Deleting version {version['uuid']} of {file['filename']}")
            try:
                self.delete(version)
            except DvasError as err:
                logging.error(f"Failed to delete {version['dvasId']} from DVAS")
                logging.debug(err)

    def _post(self, metadata: dict) -> int:
        res = self.session.post(
            f"{self.config.dvas_portal_url}/Metadata/add", json=metadata
        )
        if not res.ok:
            raise DvasError(f"POST to DVAS API failed: {res.status_code} {res.text}")
        logging.debug(f"POST to DVAS API successful: {res.status_code} {res.text}")
        dvas_id = res.headers["Location"].rsplit("/", 1)[-1]
        return int(dvas_id)

    def _init_session(self) -> requests.Session:
        s = utils.make_session()
        s.headers.update({"X-Authorization": f"Bearer {self.config.dvas_access_token}"})
        return s


class DvasMetadata:
    """Create metadata for DVAS API from Cloudnet file metadata"""

    def __init__(self, file: dict, md_api: MetadataApi):
        self.file = file
        self.md_api = md_api
        self._product = file["product"]
        self._site = file["site"]

    def create_dvas_json(self) -> dict:
        time_begin = (
            self.file["startTime"]
            or f"{self.file['measurementDate']}T00:00:00.0000000Z"
        )
        time_end = (
            self.file["stopTime"] or f"{self.file['measurementDate']}T23:59:59.9999999Z"
        )
        return {
            "md_metadata": {
                "file_identifier": self.file["filename"],
                "language": "en",
                "hierarchy_level": "dataset",
                "online_resource": {"linkage": "https://cloudnet.fmi.fi/"},
                "datestamp": datetime.now(timezone.utc).isoformat(),
                "contact": [
                    {
                        "first_name": "Ewan",
                        "last_name": "O'Connor",
                        "organisation_name": "Finnish Meteorological Institute (FMI)",
                        "role_code": ["pointOfContact"],
                        "country_code": "FI",
                    }
                ],
            },
            "md_identification": {
                "abstract": self._parse_title(),
                "title": self._parse_title(),
                "date_type": "creation",
                "contact": [
                    {
                        "first_name": "Simo",
                        "last_name": "Tukiainen",
                        "organisation_name": "Finnish Meteorological Institute (FMI)",
                        "role_code": ["processor"],
                        "country_code": "FI",
                    }
                ],
                "online_resource": {
                    "linkage": f"https://cloudnet.fmi.fi/file/{self.file['uuid']}"
                },
                "identifier": {
                    "pid": self.file["pid"],
                    "type": "handle",
                },
                "date": time_begin,
            },
            "md_constraints": {
                "access_constraints": "license",
                "use_constraints": "license",
                "other_constraints": "N/A",
                "data_licence": "CC-BY-4.0",
                "metadata_licence": "CC-BY-4.0",
                "citation": self._fetch_credits("citation"),
                "acknowledgement": self._fetch_credits("acknowledgements"),
            },
            "md_keywords": {
                "keywords": [
                    "FMI",
                    "ACTRIS",
                    self._product["humanReadableName"],
                ]
            },
            "md_data_identification": {
                "language": "en",
                "topic_category": "climatologyMeteorologyAtmosphere",
                "description": "time series of profile measurements",
                "facility_identifier": self._site["dvasId"],
            },
            "ex_geographic_bounding_box": {
                "west_bound_longitude": self._site["longitude"],
                "east_bound_longitude": self._site["longitude"],
                "south_bound_latitude": self._site["latitude"],
                "north_bound_latitude": self._site["latitude"],
            },
            "ex_temporal_extent": {
                "time_period_begin": time_begin,
                "time_period_end": time_end,
            },
            "md_content_information": {
                "attribute_descriptions": self._parse_variable_names(),
                "content_type": "physicalMeasurement",
            },
            "md_distribution_information": [
                {
                    "data_format": "netcdf",
                    "version_data_format": self._parse_netcdf_version(),
                    "dataset_url": self.file["downloadUrl"],
                    "protocol": "HTTP",
                    "transfersize": self._calc_file_size(),
                    "description": "Direct download of data file",
                    "function": "download",
                    "restriction": {
                        "set": False,
                    },
                }
            ],
            "md_actris_specific": {
                "facility_type": "observation platform, fixed",
                "product_type": "observation",
                "matrix": "cloud phase",
                "sub_matrix": None,
                "instrument_type": self._parse_instrument_type(),
                "program_affiliation": self._parse_affiliation(),
                "variable_statistical_property": None,
                "legacy_data": self.file["legacy"],
                "observation_timeliness": self._parse_timeliness(),
                "data_product": self._parse_data_product(),
            },
            "dq_data_quality_information": {
                "level": "dataset",
                "compliance": self._parse_compliance(),
                "quality_control_extent": "full quality control applied",
                "quality_control_outcome": self._parse_qc_outcome(),
            },
        }

    def _parse_variable_names(self) -> list[str]:
        # https://prod-actris-md.nilu.no/Vocabulary/ContentAttribute
        variables = utils.get_from_data_portal_api("api/products/variables")
        file_vars = list(
            filter(lambda var: var["id"] == self._product["id"], variables)
        )[0]
        return [
            v["actrisName"]
            for v in file_vars["variables"]
            if v["actrisName"] is not None
        ]

    def _parse_affiliation(self) -> list[str]:
        # https://prod-actris-md.nilu.no/vocabulary/networkprogram
        affiliation = ["CLOUDNET"]
        if "arm" in self._site["type"]:
            affiliation.append("ARM")
        if "cloudnet" in self._site["type"]:
            affiliation.append("ACTRIS")
        return affiliation

    def _parse_instrument_type(self) -> list[str]:
        # https://prod-actris-md.nilu.no/vocabulary/instrumenttype
        clu_to_dvas_map = {
            "radar": "cloud radar",
            "lidar": "lidar",
            "mwr": "microwave radiometer",
            "disdrometer": "particle size spectrometer",
            "doppler-lidar": "Doppler lidar",
        }
        instruments = self._find_instrument_types(self.file["uuid"])
        dvas_instruments = []
        for instrument in instruments:
            dvas_instruments.append(clu_to_dvas_map[instrument])
        return dvas_instruments

    def _find_instrument_types(self, uuid: str) -> list[str]:
        """Recursively find instrument types from source files."""
        instruments = []
        json_data = utils.get_from_data_portal_api(f"api/files/{uuid}")
        assert isinstance(json_data, dict)
        if "instrument" in json_data and json_data["instrument"] is not None:
            instruments.append(json_data["instrument"]["type"])
        source_ids = json_data.get("sourceFileIds", [])
        if source_ids:
            for source_uuid in source_ids:
                instruments.extend(self._find_instrument_types(source_uuid))
        return instruments

    def _parse_timeliness(self) -> str:
        # https://prod-actris-md.nilu.no/vocabulary/observationtimeliness
        clu_to_dvas_map = {
            "nrt": "near real-time",
            "rrt": "real real-time",
            "scheduled": "scheduled",
        }
        return clu_to_dvas_map[self.file["timeliness"]]

    def _parse_data_product(self) -> str:
        """Description of the data product"""
        return f"{self._parse_timeliness()} data"

    def _parse_compliance(self) -> str:
        return (
            "ACTRIS legacy"
            if self.file["measurementDate"] < "2023-04-25"
            else "ACTRIS associated"
        )

    def _parse_qc_outcome(self) -> str:
        outcome_map = {
            "pass": "1 - Good",
            "info": "3 - Questionable/suspect",
            "warning": "3 - Questionable/suspect",
            "error": "4 - Bad",
        }
        unknown_outcome = "2 - Not evaluated, not available or unknown"
        return outcome_map.get(self.file["errorLevel"], unknown_outcome)

    def _parse_netcdf_version(self) -> str:
        return self.file["format"]

    def _parse_title(self) -> str:
        return (
            f"{self._product['humanReadableName']} data "
            f"derived from cloud remote sensing measurements "
            f"at {self._site['humanReadableName']}"
        )

    def _calc_file_size(self) -> float:
        file_size = int(self.file["size"]) / 1000 / 1000  # MB
        return round(file_size, 3)

    def _fetch_credits(self, type: Literal["citation", "acknowledgements"]) -> str:
        params = {"format": "txt"}
        response = self.md_api.get(
            f"api/reference/{self.file['uuid']}/{type}", params, json=False
        )
        return response.text
