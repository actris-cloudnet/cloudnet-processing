#!/usr/bin/env python3
"""Master script for Model evaluation processing."""
import logging
import warnings
from tempfile import NamedTemporaryFile

import requests
from cloudnetpy.model_evaluation.plotting.plotting import generate_L3_day_plots
from cloudnetpy.model_evaluation.products import product_resampling
from cloudnetpy.utils import date_range
from requests.exceptions import RequestException

from data_processing import processing_tools, utils
from data_processing.processing_tools import ProcessBase, Uuid
from data_processing.utils import MiscError, RawDataMissingError, make_session

warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)


def main(args, storage_session: requests.Session | None = None):
    if storage_session is None:
        storage_session = make_session()
    config = utils.read_main_conf()
    _start_date, _stop_date = utils.get_processing_dates(args)
    start_date = utils.isodate2date(_start_date)
    stop_date = utils.isodate2date(_stop_date)
    process = ProcessModelEvaluation(args, config, storage_session=storage_session)
    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        process.date_str = date_str
        for product in args.products:
            if product not in utils.get_product_types("3"):
                raise ValueError("No such product")
            if product == "model":
                continue
            processing_tools.clean_dir(process.temp_dir.name)
            logging.info(f"Processing {product} product, {args.site} {date_str}")
            uuid = Uuid()
            uuid.volatile, filename = process.fetch_volatile_uuid(product)
            try:
                models_metadata = process.fetch_model_params()
            except MiscError as err:
                logging.warning(err)
                continue
            for model, m_meta in models_metadata.items():
                if product in utils.get_product_types(level="3"):
                    try:
                        uuid = process.process_level3_day(uuid, product, model, m_meta)

                        if filename is None:
                            filename = process.get_l3_product_key(product, model)

                        process.upload_product(product, uuid, model, filename)
                        process.create_and_upload_images(product, uuid.product, model)
                        result = process.upload_quality_report(
                            process.temp_file.name, uuid.product
                        )
                        process.print_info(uuid, result)
                    except (RawDataMissingError, MiscError, NotImplementedError) as err:
                        logging.warning(err)
                    except (RequestException, RuntimeError, ValueError) as err:
                        utils.send_slack_alert(err, "data", args, date_str, product)


class ProcessModelEvaluation(ProcessBase):
    def get_l3_product_key(self, product: str, model: str) -> str:
        assert isinstance(self.date_str, str)
        return f"{self.date_str.replace('-', '')}_{self.site}_{product}_downsampled_{model}.nc"

    def process_level3_day(
        self, uuid: Uuid, full_product: str, model: str, model_meta: list
    ) -> Uuid:
        l3_product = utils.full_product_to_l3_product(full_product)
        l2_product = self.get_l2for_l3_product(l3_product)
        input_model_files = []
        payload = self._get_payload(product=l2_product)
        metadata = self.md_api.get("api/files", payload)
        if metadata:
            l2_file = self._storage_api.download_product(
                metadata[0], self.temp_dir.name
            )
            self._check_response_length(metadata)
            metadict = {l2_product: metadata[0], "model": model_meta[0][0]}
            self._check_source_status(full_product, metadict)
        else:
            raise MiscError("Missing input level 2 file")
        for m_meta in model_meta:
            m_file = self._storage_api.download_product(m_meta[0], self.temp_dir.name)
            input_model_files.append(m_file)
            # TODO: Raise error if no model meta, warning if only one meta missing
        uuid.product = product_resampling.process_L3_day_product(
            model,
            l3_product,
            input_model_files,
            l2_file,
            self.temp_file.name,
            uuid=uuid.volatile,
            overwrite=True,
        )
        return uuid

    def fetch_model_params(self, model: str = "ecmwf") -> dict:
        # POC: only EC
        payload = self._get_payload()
        if model:
            payload = self._get_payload(model=model)
        else:
            payload["allModels"] = True
        metadata = self.md_api.get("api/model-files", payload)
        model_metas = self._sort_model_meta2dict(metadata)
        return model_metas

    def create_and_upload_images(
        self,
        product: str,
        uuid: str,
        model_or_instrument_id: str,
        legacy: bool = False,
        instrument_pid: str | None = None,
    ) -> None:
        if "hidden" in self._site_type:
            logging.info("Skipping plotting for hidden site")
            return
        temp_file = NamedTemporaryFile(suffix=".png")
        visualizations = []
        product_s3key = self.get_l3_product_key(product, model_or_instrument_id)
        fields = utils.get_fields_for_l3_plot(product, model_or_instrument_id)
        l3_product = utils.full_product_to_l3_product(product)

        # Statistic plot
        generate_L3_day_plots(
            self.temp_file.name,
            l3_product,
            model_or_instrument_id,
            var_list=fields,
            image_name=temp_file.name,
            fig_type="statistic",
            stats=("area",),
            title=False,
        )
        visualizations.append(
            self._upload_img(temp_file.name, product_s3key, uuid, product, "area", None)
        )
        # Statistic error plot
        generate_L3_day_plots(
            self.temp_file.name,
            l3_product,
            model_or_instrument_id,
            var_list=fields,
            image_name=temp_file.name,
            fig_type="statistic",
            stats=("error",),
            title=False,
        )
        visualizations.append(
            self._upload_img(
                temp_file.name, product_s3key, uuid, product, "error", None
            )
        )
        # Single plots
        # Check this potential error here
        for field in fields:  # pylint: disable=not-an-iterable
            generate_L3_day_plots(
                self.temp_file.name,
                l3_product,
                model_or_instrument_id,
                var_list=[field],
                image_name=temp_file.name,
                fig_type="single",
                title=False,
            )
            visualizations.append(
                self._upload_img(
                    temp_file.name, product_s3key, uuid, product, field, None
                )
            )
        self.md_api.put_images(visualizations, uuid)

    @staticmethod
    def get_l2for_l3_product(product: str) -> str:
        if product == "cf":
            return "categorize"
        if product == "iwc":
            return "iwc"
        if product == "lwc":
            return "lwc"
        raise ValueError(f"Invalid product {product}")


def add_arguments(subparser):
    parser = subparser.add_parser(
        "me", help="Process Cloudnet model evaluation (Level 3) data."
    )
    parser.add_argument(
        "-r",
        "--reprocess",
        action="store_true",
        help="Process new version of the stable files and reprocess volatile " "files.",
        default=False,
    )
    return subparser
