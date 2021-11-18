#!/usr/bin/env python3
"""Script for creating and putting missing images into s3 / database."""
import argparse
import requests
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi
from data_processing import utils
from tempfile import TemporaryDirectory


def main():
    """The main function."""

    config = utils.read_main_conf()
    md_api = MetadataApi(config, requests.session())
    storage_api = StorageApi(config, requests.session())

    site_metadata = md_api.get('api/sites', {'modelSites': True})
    sites = [site['id'] for site in site_metadata]

    for site in sites:

        payload = {'location': site, 'allVersions': True, 'developer': True}
        metadata = md_api.get('api/files', payload)
        for row in metadata:

            product_uuid = row['uuid']
            temp_dir = TemporaryDirectory()

            vis_meta = md_api.get(f'api/visualizations/{product_uuid}', {})['visualizations']
            fields_to_plot = utils.get_fields_for_plot(row['product']['id'])[0]

            if len(vis_meta) != len(fields_to_plot):
                full_path = storage_api.download_product(row, temp_dir.name)
                print('IMAGE GENERATION CODE NEEDS TO BE UPDATED, NOT DOING ANYTHING')
                """ IMAGE GENERATION CODE HAS CHANGED, THIS NO LOGNER WORKS
                img_metadata = storage_api.upload_image(full_path,
                                                        row['filename'],
                                                        product_uuid,
                                                        row['product']['id'])
                md_api.put_images(img_metadata, product_uuid)
                """


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Put missing image files to database.')

    ARGS = parser.parse_args()
    main()
