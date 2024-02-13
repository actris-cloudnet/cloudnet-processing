import datetime
import logging
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

from data_processing import nc_header_augmenter
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi
from data_processing.utils import make_session, read_main_conf
from processing.processor import ModelParams, Processor


def main():
    config = read_main_conf()
    session = make_session()
    md_api = MetadataApi(config, session)
    storage_api = StorageApi(config, session)
    processor = Processor(md_api, storage_api)
    for metadata in processor.get_unprocessed_model_uploads():
        with TemporaryDirectory() as directory:
            params = ModelParams(
                site=metadata["site"]["id"],
                date=datetime.date.fromisoformat(metadata["measurementDate"]),
                model=metadata["model"]["id"],
            )
            process_model(processor, params, Path(directory))


def process_model(processor: Processor, params: ModelParams, directory: Path):
    site = processor.get_site(params.site)
    is_hidden = "hidden" in site["type"]

    upload_meta = processor.get_model_upload(params)
    if not upload_meta:
        return
    full_paths, raw_uuids = processor.download_raw_data([upload_meta], directory)

    if file_meta := processor.get_model_file(params):
        if not file_meta["volatile"]:
            logging.warning("Stable model file found. Replacing...")
        product_uuid = uuid.UUID(file_meta["uuid"])
        filename = file_meta["filename"]
    else:
        product_uuid = generate_uuid()
        filename = generate_filename(params)

    harmonize_model(params, full_paths[0], product_uuid)
    processor.upload_file(params, full_paths[0], filename)
    if is_hidden:
        logging.info("Skipping plotting for hidden site")
    else:
        processor.create_and_upload_images(
            full_paths[0], "model", product_uuid, filename, directory
        )
    processor.upload_quality_report(full_paths[0], product_uuid)
    processor.update_statuses(raw_uuids, "processed")


def generate_uuid() -> uuid.UUID:
    return uuid.uuid4()


def generate_filename(params: ModelParams) -> str:
    return f"{params.date:%Y%m%d}_{params.site}_{params.model}.nc"


def harmonize_model(params: ModelParams, full_path: Path, uuid: uuid.UUID):
    data = {
        "site_name": params.site,
        "date": params.date.isoformat(),
        "uuid": str(uuid),
        "full_path": full_path,
        "model": params.model,
        "instrument": None,
    }
    nc_header_augmenter.harmonize_model_file(data)


if __name__ == "__main__":
    main()
