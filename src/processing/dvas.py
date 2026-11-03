import base64
import datetime
import logging
from typing import Any, Literal
from uuid import UUID

import requests
from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import (
    ExtendedProductMetadata,
    ProductMetadata,
    Site,
    VersionMetadata,
)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from processing import utils
from processing.config import Config
from processing.metadata_api import MetadataApi


class DvasAdapter(HTTPAdapter):
    def __init__(self) -> None:
        retry_strategy = Retry(
            total=10,
            backoff_factor=0.1,
            status_forcelist=[408, 429, 500, 502, 503, 504, 524],
            # DVAS only uses POST but retry should be safe as id is always
            # specified in the body.
            allowed_methods=["POST"],
        )
        super().__init__(max_retries=retry_strategy)


class DvasError(Exception):
    pass


ACTRIS_ERIC_START = datetime.date(2023, 4, 25)


def parse_compliance(site: Site, measurement_date: datetime.date) -> str:
    """Deduce ACTRIS compliance from the site labelling status.

    Link: https://vocabulary.actris.nilu.no/actris_vocab/compliance
    """
    if measurement_date < ACTRIS_ERIC_START:
        return "ACTRIS legacy"
    match site.labelling_status:
        case "labelled":
            return "ACTRIS labelled"
        case "initially-accepted":
            return "ACTRIS compliant"
        case _:
            return "ACTRIS associated"


class DvasV3:
    """Class for managing Cloudnet file metadata operations in the DVAS API."""

    def __init__(self, config: Config, md_api: MetadataApi, client: APIClient) -> None:
        self.config = config
        self.md_api = md_api
        self.session = requests.Session()
        adapter = DvasAdapter()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.token: str | None = None
        self.token_expires: datetime.datetime | None = None
        self.client = client

    def upload(self, file: ExtendedProductMetadata) -> None:
        """Upload Cloudnet file metadata to DVAS API and update Cloudnet data portal"""
        landing_page_url = utils.build_file_landing_page_url(file.uuid)
        logging.info("Uploading %s metadata to DVAS", landing_page_url)
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
            dvas_metadata = DvasMetadataV3(file, self.md_api, self.client)
            dvas_timestamp = datetime.datetime.now(datetime.timezone.utc)
            dvas_json = dvas_metadata.create_dvas_json(dvas_timestamp)
            if not dvas_json["variables"]:
                logging.error("Skipping - no ACTRIS variables")
                return
            self._delete_old_versions(file)
            self._post_metadata([dvas_json])
            self.md_api.update_dvas_info(file.uuid, dvas_timestamp)
        except DvasError:
            logging.exception("Failed to upload %s to DVAS", file.filename)

    def _delete_old_versions(self, file: ExtendedProductMetadata) -> None:
        """Delete all versions of the given file from DVAS API. To be used before posting new version."""
        versions = self.client.versions(file.uuid)
        for version in versions:
            if version.uuid == file.uuid:
                continue
            logging.debug("Deleting version %s of %s", version.uuid, file.filename)
            try:
                self._delete_metadata(version)
                self.md_api.clean_dvas_info(version.uuid)
            except DvasError as err:
                logging.error("Failed to delete %s from DVAS", version.pid)
                logging.debug(err)

    def _delete_metadata(self, file: VersionMetadata) -> None:
        """Delete Cloudnet file metadata from DVAS API"""
        logging.warning("Deleting Cloudnet file %s from DVAS", file.uuid)
        self._request("POST", "/api/provider/metadata/delete", json=[file.pid])

    def _post_metadata(self, metadata: list[dict]) -> None:
        self._request("POST", "/api/provider/metadata/add", json=metadata)

    def _request(
        self,
        method: str,
        endpoint: str,
        json: Any | None = None,  # noqa: ANN401
    ) -> None:
        if self.token_expires is None or self.token_expires < datetime.datetime.now():
            self._refresh_access_token()
        url = self.config.dvas_portal_url + endpoint
        res = self.session.request(
            method,
            url,
            json=json,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=10,
        )
        # Retry once if unauthorized.
        if res.status_code == 401:
            self._refresh_access_token()
            res = self.session.request(
                method,
                url,
                json=json,
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=10,
            )
        res.raise_for_status()

    def _refresh_access_token(self) -> None:
        res = self.session.post(
            f"{self.config.dvas_portal_url}/api/auth/token",
            json={
                "username": self.config.dvas_client_id,
                "password": self.config.dvas_client_secret,
            },
            timeout=10,
        )
        res.raise_for_status()
        token = res.json()
        self.token = token["access_token"]
        self.token_expires = datetime.datetime.now() + datetime.timedelta(
            seconds=token["expires_in"] * 0.9
        )


class DvasMetadataV3:
    """Create metadata for DVAS API from Cloudnet file metadata"""

    def __init__(
        self, file: ExtendedProductMetadata, md_api: MetadataApi, client: APIClient
    ) -> None:
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
                    "access_restriction": {"restricted": False},
                    "transfersize": {"size": self.file.size, "unit": "B"},
                }
            ],
            "provenance": [
                {
                    "title": software.title,
                    "url": software.url,
                }
                for software in self.file.software
            ],
        }

    def _parse_variable_names(self) -> list[str]:
        # https://vocabulary.actris.nilu.no/actris_vocab/variable
        file_vars = self.md_api.get(f"api/products/{self.file.product.id}/variables")
        return [v["actrisName"] for v in file_vars if v["actrisName"] is not None]

    def _parse_frameworks(self) -> list[str]:
        # https://vocabulary.actris.nilu.no/actris_controlled_lists/framework
        affiliation = ["CLOUDNET"]
        if "arm" in self.file.site.type:
            affiliation.append("ARM")
        if "cloudnet" in self.file.site.type:
            affiliation.append("ACTRIS")
        return affiliation

    def _parse_timeliness(self) -> str:
        # https://vocabulary.actris.nilu.no/actris_vocab/timeliness
        clu_to_dvas_map = {
            "nrt": "near real-time",
            "rrt": "real real-time",
            "scheduled": "scheduled",
        }
        return clu_to_dvas_map[self.file.timeliness]

    def _parse_compliance(self) -> str:
        return parse_compliance(self.file.site, self.file.measurement_date)

    def _parse_qc_outcome(self) -> str:
        # https://vocabulary.actris.nilu.no/actris_vocab/qualitycontroloutcome
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
