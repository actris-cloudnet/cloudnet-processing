import base64
import datetime
import logging
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
            dvas_timestamp = datetime.datetime.now(datetime.timezone.utc)
            dvas_json = dvas_metadata.create_dvas_json(dvas_timestamp)
            if not dvas_json["variables"]:
                logging.error("Skipping - no ACTRIS variables")
                return
            self._delete_old_versions(file)
            dvas_id = self._post(dvas_json)
            self.md_api.update_dvas_info(file["uuid"], dvas_timestamp, dvas_id)
        except DvasError:
            logging.exception(f"Failed to upload {file['filename']} to DVAS")

    def delete(self, file: dict):
        """Delete Cloudnet file metadata from DVAS API"""
        logging.warning(
            f"Deleting Cloudnet file {file['uuid']} with dvasId {file['dvasId']} from DVAS"
        )
        url = f"{self.config.dvas_portal_url}/metadata/delete/pid/{file['pid']}"
        self._delete(url)

    def delete_all(self):
        """Delete all Cloudnet file metadata from DVAS API"""
        url = f"{self.config.dvas_portal_url}/metadata/delete/all/{self.config.dvas_provider_id}"
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
            f"api/files/{file['uuid']}/versions", {"properties": ["dvasId", "pid"]}
        )
        for version in versions:
            if version["dvasId"] is None:
                continue
            logging.debug(f"Deleting version {version['uuid']} of {file['filename']}")
            try:
                self.delete(version)
                self.md_api.clean_dvas_info(version["uuid"])
            except DvasError as err:
                logging.error(f"Failed to delete {version['dvasId']} from DVAS")
                logging.debug(err)

    def _post(self, metadata: dict) -> str:
        res = self.session.post(
            f"{self.config.dvas_portal_url}/metadata/add", json=metadata
        )
        if not res.ok:
            raise DvasError(f"POST to DVAS API failed: {res.status_code} {res.text}")
        logging.debug(f"POST to DVAS API successful: {res.status_code} {res.text}")
        res = self.session.post(
            f"{self.config.dvas_portal_url}/metadata/pid",
            json={"pid": metadata["md_identification"]["identifier"]["pid"]},
        )
        dvas_id = res.json()[0]["id"]
        return dvas_id

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

    def create_dvas_json(self, timestamp: datetime.datetime) -> dict:
        time_begin = (
            self.file["startTime"]
            or f"{self.file['measurementDate']}T00:00:00.0000000Z"
        )
        time_end = (
            self.file["stopTime"] or f"{self.file['measurementDate']}T23:59:59.9999999Z"
        )
        timeliness = self._parse_timeliness()
        instruments = list(self._find_instruments(self.file["uuid"]).values())
        return {
            "dataset_metadata": {
                "repository": {"repository_id": "CLU"},
                "time_file_created": self.file["createdAt"],
                "time_metadata_created": timestamp.isoformat(),
                "time_content_revised": self.file["updatedAt"],
            },
            "identification": {
                "identifier": {"pid": self.file["pid"], "pid_type": "ePIC"},
                "title": self._parse_title(),
                "abstract": self._parse_title(),
                "roles": [
                    {
                        "role_code": ["pointOfContact"],
                        "person": {
                            "first_name": "Ewan",
                            "last_name": "O'Connor",
                            "affiliation": {
                                "name": "Finnish Meteorological Institute",
                                "pid": "https://ror.org/05hppb561",
                                "pid_type": "other PID",
                                "country_code": "FI",
                            },
                            "orcid": "https://orcid.org/0000-0001-9834-5100",
                        },
                    },
                    {
                        "role_code": ["processor"],
                        "person": {
                            "first_name": "Simo",
                            "last_name": "Tukiainen",
                            "affiliation": {
                                "name": "Finnish Meteorological Institute",
                                "pid": "https://ror.org/05hppb561",
                                "pid_type": "other PID",
                                "country_code": "FI",
                            },
                            "orcid": "https://orcid.org/0000-0002-0651-4622",
                        },
                    },
                ],
            },
            "usage_information": {
                "data_licence": "CC-BY-4.0",
                "metadata_licence": "CC0-1.0",
                "citation": self._fetch_credits("citation"),
                "acknowledgement": self._fetch_credits("acknowledgements"),
            },
            "product_type": "observation",
            "facility": {
                "identifier": self._site["dvasId"],
            },
            "spatial_extent": {
                "type": "LineString",
                "coordinates": [
                    [
                        self._site["longitude"],
                        self._site["latitude"],
                        self._site["altitude"],
                    ],
                    [
                        self._site["longitude"],
                        self._site["latitude"],
                        self._site["altitude"] + 12_000,
                    ],
                ],
            },
            "temporal_extent": {
                "time_period_begin": time_begin,
                "time_period_end": time_end,
            },
            "variables": [
                {
                    "variable_name": variable_name,
                    "variable_matrix": "cloud phase",
                    "variable_geometry": "atmospheric vertical profile",
                    "timeliness": timeliness,
                    "instrument": instruments,
                    "data_quality_control": [
                        {
                            "compliance": self._parse_compliance(),
                            "quality_control_extent": "full quality control applied",
                            "quality_control_mechanism": "automatic quality control",
                            "quality_control_outcome": self._parse_qc_outcome(),
                        }
                    ],
                    "framework": [
                        {"framework": framework}
                        for framework in self._parse_frameworks()
                    ],
                    "temporal_resolution": "P30S",
                }
                for variable_name in self._parse_variable_names()
            ],
            "distribution_information": [
                {
                    "data_format": self._parse_netcdf_version(),
                    "dataset_url": self.file["downloadUrl"],
                    "protocol": "HTTP",
                    "access_restriction": {
                        "restricted": False,
                    },
                    "transfersize": {"size": int(self.file["size"]), "unit": "B"},
                }
            ],
            "provenance": [
                {
                    "title": software["title"],
                    "url": software["url"],
                }
                for software in self.file["software"]
            ],
        }

    def _parse_variable_names(self) -> list[str]:
        # https://prod-actris-md.nilu.no/Vocabulary/ContentAttribute
        file_vars = self.md_api.get(f"api/products/{self._product['id']}/variables")
        return [v["actrisName"] for v in file_vars if v["actrisName"] is not None]

    def _parse_frameworks(self) -> list[str]:
        affiliation = ["CLOUDNET"]
        if "arm" in self._site["type"]:
            affiliation.append("ARM")
        if "cloudnet" in self._site["type"]:
            affiliation.append("ACTRIS")
        return affiliation

    def _find_instruments(self, uuid: str) -> dict[str, dict]:
        """Recursively find instruments from source files."""
        instruments = {}
        json_data = utils.get_from_data_portal_api(f"api/files/{uuid}")
        assert isinstance(json_data, dict)
        if "instrument" in json_data and json_data["instrument"] is not None:
            instruments[json_data["instrument"]["pid"]] = {
                "instrument_pid": json_data["instrument"]["pid"],
                "instrument_type": json_data["instrument"]["type"],
                "instrument_name": json_data["instrument"]["name"],
            }
        source_ids = json_data.get("sourceFileIds", [])
        if source_ids:
            for source_uuid in source_ids:
                instruments.update(self._find_instruments(source_uuid))
        return instruments

    def _parse_timeliness(self) -> str:
        # https://prod-actris-md.nilu.no/vocabulary/observationtimeliness
        clu_to_dvas_map = {
            "nrt": "near real-time",
            "rrt": "real real-time",
            "scheduled": "scheduled",
        }
        return clu_to_dvas_map[self.file["timeliness"]]

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

    def _fetch_credits(self, type: Literal["citation", "acknowledgements"]) -> str:
        params = {"format": "txt"}
        response = self.md_api.get(
            f"api/reference/{self.file['uuid']}/{type}", params, json=False
        )
        return response.text
