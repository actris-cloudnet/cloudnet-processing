import base64
import logging
import os
from datetime import datetime, timezone

import requests

from data_processing import utils
from data_processing.metadata_api import MetadataApi


class DvasError(Exception):
    pass


class Dvas:
    """Class for managing Cloudnet file metadata operations in the DVAS API."""

    DVAS_URL = f"{os.environ['DVAS_PORTAL_URL']}/Metadata"
    DVAS_ACCESS_TOKEN = os.environ["DVAS_ACCESS_TOKEN"]
    DVAS_USERNAME = os.environ["DVAS_USERNAME"]
    DVAS_PASSWORD = os.environ["DVAS_PASSWORD"]
    CLU_ID = "11"

    def __init__(self):
        self.session = self._init_session()

    def upload(self, md_api: MetadataApi, file: dict):
        """Upload Cloudnet file metadata to DVAS API and update Cloudnet data portal"""
        landing_page_url = utils.build_file_landing_page_url(file["uuid"])
        logging.info(f"Uploading {landing_page_url} metadata to DVAS")
        if not file["pid"]:
            logging.error("Skipping - volatile file")
            return
        if file["product"]["level"] != "2" and not file["product"]["id"] == "model":
            logging.error("Skipping - only L2 and model products supported")
            return
        try:
            dvas_metadata = DvasMetadata(file)
            dvas_json = dvas_metadata.create_dvas_json()
            if len(dvas_json["md_content_information"]["attribute_descriptions"]) == 0:
                logging.error("Skipping - no ACTRIS variables")
                return
            self._post(dvas_json)
            md_api.update_dvas_timestamp(
                file["uuid"], dvas_json["md_metadata"]["datestamp"]
            )
        except DvasError as err:
            logging.error(f"Failed to upload {file['filename']} to DVAS")
            logging.debug(err)

    def get(self, pid: str) -> dict:
        """Fetch metadata for a single Cloudnet file from DVAS API"""
        url = f"{self.DVAS_URL}/pid/{pid}/type/Handle"
        res = self.session.get(url)
        if not res.ok:
            raise DvasError(f"GET failed: {res.status_code} {res.text}")
        return res.json()

    def delete(self, identifier: int):
        """Delete Cloudnet file metadata from DVAS API"""
        logging.warning(f"Deleting Cloudnet file {identifier} from DVAS")
        url = f"{self.DVAS_URL}/delete/{identifier}"
        self._delete(url)

    def delete_all(self):
        """Delete all Cloudnet file metadata from DVAS API"""
        url = f"{self.DVAS_URL}/delete/all/{self.CLU_ID}"
        self._delete(url)
        logging.info("Done. All Cloudnet files deleted from DVAS")

    def _delete(self, url: str):
        auth = (
            base64.b64encode(f"{self.DVAS_USERNAME}:{self.DVAS_PASSWORD}".encode())
            .decode()
            .strip()
        )
        headers = {"X-Authorization": f"Basic {auth}"}
        res = self.session.delete(url, headers=headers)
        if not res.ok:
            raise DvasError(res)
        logging.debug(f"DELETE successful: {res.status_code} {res.text}")

    def _post(self, metadata: dict):
        res = self.session.post(f"{self.DVAS_URL}/add", json=metadata)
        if not res.ok:
            raise DvasError(f"POST to DVAS API failed: {res.status_code} {res.text}")
        logging.debug(f"POST to DVAS API successful: {res.status_code} {res.text}")

    def _init_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"X-Authorization": f"Bearer {self.DVAS_ACCESS_TOKEN}"})
        return s


class DvasMetadata:
    """Create metadata for DVAS API from Cloudnet file metadata"""

    def __init__(self, file: dict):
        self.file = file
        self._product = file["product"]
        self._site = file["site"]
        self._is_model_data = self._product["id"] == "model"

    def create_dvas_json(self) -> dict:
        time_begin = f"{self.file['measurementDate']}T00:00:00.0000000Z"
        time_end = f"{self.file['measurementDate']}T23:59:59.9999999Z"
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
                "access_constraints": "otherRestrictions",
                "use_constraints": "license",
                "other_constraints": "http://actris.nilu.no/Content/Documents/DataPolicy.pdf",
                "data_license": "https://creativecommons.org/licenses/by/4.0/",
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
                "facility_identifier": self._parse_facility_identifier(),
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
                "facility_type": self._parse_facility_type(),
                "product_type": self._parse_product_type(),
                "matrix": "cloud phase",
                "sub_matrix": None,
                "instrument_type": self._parse_instrument_type(),
                "program_affiliation": self._parse_affiliation(),
                "variable_statistical_property": [
                    "arithmetic mean"
                ],  # Now mandatory but should be optional
                "legacy_data": self.file["legacy"],
                "observation_timeliness": self._parse_timeliness(),
                "data_product": self._parse_data_product(),
            },
            "dq_data_quality_information": {
                "level": "dataset",
                "compliance": self._parse_compliance(),
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
        }
        if self._is_model_data:
            return ["not_applicable"]
        elif self._product["id"] in clu_to_dvas_map:
            return [clu_to_dvas_map[self._product["id"]]]
        elif self._product["level"] == "2":
            return list(clu_to_dvas_map.values())
        raise DvasError(f"Instrument type {self._product['id']} not implemented")

    def _parse_facility_identifier(self):
        return None if self._is_model_data else self._site["dvasId"]

    def _parse_facility_type(self):
        return None if self._is_model_data else "observation platform, fixed"

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
        prefix = "model" if self._is_model_data else self._parse_timeliness()
        return f"{prefix} data"

    def _parse_product_type(self) -> str:
        return "model" if self._is_model_data else "observation"

    def _parse_compliance(self) -> str:
        return "ACTRIS legacy" if self.file["legacy"] else "ACTRIS compliant"

    def _parse_netcdf_version(self) -> str:
        return self.file["format"]

    def _parse_title(self) -> str:
        if self._is_model_data:
            return f"Model profile data at {self._site['humanReadableName']}"
        return (
            f"Ground-based remote sensing observations "
            f"of {self._product['humanReadableName']} "
            f"at {self._site['humanReadableName']}"
        )

    def _calc_file_size(self) -> float:
        file_size = int(self.file["size"]) / 1000 / 1000  # MB
        return round(file_size, 3)