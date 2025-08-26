import base64
import datetime
import logging
from typing import Literal
from uuid import UUID

import requests
from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import ProductMetadata, VersionMetadata

from processing import utils
from processing.config import Config
from processing.metadata_api import MetadataApi


class DvasError(Exception):
    pass


class Dvas:
    """Class for managing Cloudnet file metadata operations in the DVAS API."""

    def __init__(self, config: Config, md_api: MetadataApi, client: APIClient):
        self.config = config
        self.md_api = md_api
        self.session = self._init_session()
        self.client = client

    def upload(self, file: ProductMetadata):
        """Upload Cloudnet file metadata to DVAS API and update Cloudnet data portal"""
        landing_page_url = utils.build_file_landing_page_url(file.uuid)
        logging.info(f"Uploading {landing_page_url} metadata to DVAS")
        if not file.pid:
            logging.error("Skipping - volatile file")
            return
        if "geophysical" not in file.product.type:
            logging.error("Skipping - only geophysical products supported for now")
            return
        if "categorize" in file.product.id:
            logging.error("Skipping - categorize file")
            return
        if not file.site.dvas_id:
            logging.error("Skipping - not DVAS site")
            return
        try:
            dvas_metadata = DvasMetadata(file, self.md_api, self.client)
            dvas_timestamp = datetime.datetime.now(datetime.timezone.utc)
            dvas_json = dvas_metadata.create_dvas_json(dvas_timestamp)
            if not dvas_json["variables"]:
                logging.error("Skipping - no ACTRIS variables")
                return
            self._delete_old_versions(file)
            dvas_id = self._post(dvas_json)
            self.md_api.update_dvas_info(file.uuid, dvas_timestamp, dvas_id)
        except DvasError:
            logging.exception(f"Failed to upload {file.filename} to DVAS")

    def delete(self, file: VersionMetadata):
        """Delete Cloudnet file metadata from DVAS API"""
        logging.warning(
            f"Deleting Cloudnet file {file.uuid} with dvasId {file.dvas_id} from DVAS"
        )
        url = f"{self.config.dvas_portal_url}/metadata/delete/pid/{file.pid}"
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

    def _delete_old_versions(self, file: ProductMetadata):
        """Delete all versions of the given file from DVAS API. To be used before posting new version."""
        versions = self.client.versions(file.uuid)
        for version in versions:
            if version.dvas_id is None:
                continue
            logging.debug(f"Deleting version {version.uuid} of {file.filename}")
            try:
                self.delete(version)
                self.md_api.clean_dvas_info(version.uuid)
            except DvasError as err:
                logging.error(f"Failed to delete {version.dvas_id} from DVAS")
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

    def __init__(self, file: ProductMetadata, md_api: MetadataApi, client: APIClient):
        self.file = file
        self.md_api = md_api
        self.client = client

    def create_dvas_json(self, timestamp: datetime.datetime) -> dict:
        time_begin = self.file.start_time or datetime.datetime.combine(
            self.file.measurement_date, datetime.time(0, 0, 0), datetime.timezone.utc
        )
        time_end = self.file.stop_time or datetime.datetime.combine(
            self.file.measurement_date,
            datetime.time(23, 59, 59, 999999),
            datetime.timezone.utc,
        )
        timeliness = self._parse_timeliness()
        instruments = self.client.source_instruments(self.file.uuid)
        compliance = self._parse_compliance()
        qc_outcome = self._parse_qc_outcome()
        frameworks = self._parse_frameworks()
        return {
            "dataset_metadata": {
                "repository": {"repository_id": "CLU"},
                "time_file_created": self.file.created_at.isoformat(),
                "time_metadata_created": timestamp.isoformat(),
                "time_content_revised": self.file.updated_at.isoformat(),
            },
            "identification": {
                "identifier": {"pid": self.file.pid, "pid_type": "ePIC"},
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
                "identifier": self.file.site.dvas_id,
            },
            "spatial_extent": {
                "type": "LineString",
                "coordinates": [
                    [
                        self.file.site.longitude,
                        self.file.site.latitude,
                        self.file.site.altitude,
                    ],
                    [
                        self.file.site.longitude,
                        self.file.site.latitude,
                        self.file.site.altitude + 12_000,
                    ],
                ],
            },
            "temporal_extent": {
                "time_period_begin": time_begin.isoformat(),
                "time_period_end": time_end.isoformat(),
            },
            "variables": [
                {
                    "variable_name": variable_name,
                    "variable_matrix": "cloud phase",
                    "variable_geometry": "atmospheric vertical profile",
                    "timeliness": timeliness,
                    "instrument": [
                        {
                            "instrument_pid": instrument.pid,
                            "instrument_type": instrument.type,
                            "instrument_name": instrument.name,
                        }
                        for instrument in instruments
                    ],
                    "data_quality_control": [
                        {
                            "compliance": compliance,
                            "quality_control_extent": "full quality control applied",
                            "quality_control_mechanism": "automatic quality control",
                            "quality_control_outcome": qc_outcome,
                        }
                    ],
                    "framework": [{"framework": framework} for framework in frameworks],
                    "temporal_resolution": "P30S",
                }
                for variable_name in self._parse_variable_names()
            ],
            "distribution_information": [
                {
                    "data_format": self._parse_netcdf_version(),
                    "dataset_url": self.file.download_url,
                    "protocol": "HTTP",
                    "access_restriction": {
                        "restricted": False,
                    },
                    "transfersize": {"size": self.file.size, "unit": "B"},
                }
            ],
            # TODO:
            # "provenance": [
            #     {
            #         "title": software.title,
            #         "url": software.url,
            #     }
            #     for software in self.file.software
            # ],
        }

    def _parse_variable_names(self) -> list[str]:
        # https://prod-actris-md.nilu.no/Vocabulary/ContentAttribute
        file_vars = self.md_api.get(f"api/products/{self.file.product.id}/variables")
        return [v["actrisName"] for v in file_vars if v["actrisName"] is not None]

    def _parse_frameworks(self) -> list[str]:
        affiliation = ["CLOUDNET"]
        if "arm" in self.file.site.type:
            affiliation.append("ARM")
        if "cloudnet" in self.file.site.type:
            affiliation.append("ACTRIS")
        return affiliation

    def _parse_timeliness(self) -> str:
        # https://prod-actris-md.nilu.no/vocabulary/observationtimeliness
        clu_to_dvas_map = {
            "nrt": "near real-time",
            "rrt": "real real-time",
            "scheduled": "scheduled",
        }
        return clu_to_dvas_map[self.file.timeliness]

    def _parse_compliance(self) -> str:
        return (
            "ACTRIS legacy"
            if self.file.measurement_date < datetime.date(2023, 4, 25)
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
        if self.file.error_level is None:
            return unknown_outcome
        return outcome_map.get(self.file.error_level, unknown_outcome)

    def _parse_netcdf_version(self) -> str:
        return self.file.format

    def _parse_title(self) -> str:
        return (
            f"{self.file.product.human_readable_name} data "
            f"derived from cloud remote sensing measurements "
            f"at {self.file.site.human_readable_name}"
        )

    def _fetch_credits(self, type: Literal["citation", "acknowledgements"]) -> str:
        params = {"format": "txt"}
        response = self.md_api.get(
            f"api/reference/{self.file.uuid}/{type}", params, json=False
        )
        return response.text
