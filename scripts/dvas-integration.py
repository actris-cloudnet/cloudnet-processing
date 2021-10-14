#!/usr/bin/env python3
import os
from data_processing import utils
from datetime import datetime
import re
import requests


def create_title(file):
    return f"{file['product']['humanReadableName']} file from {file['site']['humanReadableName']} on {file['measurementDate']}"


def parse_affiliation(site):
    affiliation = ['CLOUDNET']
    if 'arm' in site['type']:
        affiliation.append('ARM')
    if 'cloudnet' in site['type']:
        affiliation.append('ACTRIS_NRT')
    return affiliation


def parse_instrument_type(product):
    if product['id'] == 'model':
        return ['UNKNOWN']
    elif product['level'] == '1b':
        return [product['id']]
    else:
        return ['radar', 'lidar', 'mwr']


def main():
    lastsuccesspath = 'scripts/last-success'
    lastsuccessfile = open(lastsuccesspath, 'r')
    lines = lastsuccessfile.readlines()
    lastsuccessfile.close()

    startdate = lines[0].strip()
    enddate = datetime.now().isoformat()

    payload = dict(volatile=False, updatedAtFrom=startdate, updatedAtTo=enddate)
    print(payload)

    variables = utils.get_from_data_portal_api('api/products/variables')
    if 'status' in variables and variables['status'] >= 300:
        raise requests.HTTPError(variables['errors'][0])
    files = utils.get_from_data_portal_api('api/files', payload)
    if 'status' in files and files['status'] >= 300:
        raise requests.HTTPError(files['errors'][0])

    print(f'About to upload {len(files)} files.')

    for file in files:
        site = file['site']
        product = file['product']
        file_vars = list(filter(lambda var: var['id'] == product['id'], variables))
        var_ids = list(map(lambda var: re.sub(r'.*-', '', var['id']), file_vars[0]['variables']))
        pid = { 'id': file['pid'].replace('https://hdl.handle.net/', ''), 'type': 'Handle' } if file['pid'] else { 'id': None, 'type': 'N/A' }

        actris_json = {
          'md_metadata': { # mandatory
            'file_identifier': file['filename'],
            'language': 'en', # mandatory
            'hierarchy_level': 'dataset', # mandatory, fixed list ['attribute','attributeType','collectionHardware','collectionSession','dataset','series','nonGeographicDataset','dimensionGroup','feature','featureType','propertyType','fieldSession','software','service','model','tile']
            'datestamp': datetime.now().isoformat(), # mandatory
            'contact': [{ # mandatory
              'first_name': 'Ewan', # mandatory
              'last_name': 'O\'Connor', # mandatory
              'organisation_name': 'Finnish Meteorological Institute (FMI)', # mandatory
              'role_code': ['pointOfContact'], # mandatory, fixed list ['resourceProvider','custodian','owner,'user,'distributor,'originator,'pointOfContact,'principalInvestigator,'processor,'publisher,'author]
              'country': 'Finland', # mandatory
              'country_code': 'FI'
            }],
            'online_resource': { # mandatory
              'linkage': 'https://cloudnet.fmi.fi/' # mandatory
            }
          },
          'md_identification': { # mandatory
            'abstract': create_title(file), # mandatory
            'title': create_title(file), # mandatory
            'identifier': pid, # optional
            'date': file['measurementDate'], # mandatory
            'date_type': 'creation', # mandatory, fixed list ['publication', 'revision', 'creation'
            'contact': [{ # mandatory
              'first_name': 'Simo', # mandatory
              'last_name': 'Tukiainen', # mandatory
              'organisation_name': 'Finnish Meteorological Institute (FMI)', # mandatory
              'role_code': ['processor'], # mandatory, see fixed list in example above
              'country': 'Finland', # mandatory
              'country_code': 'FI'
            }],
            'online_resource': { # mandatory
              'linkage': f"https://cloudnet.fmi.fi/file/{file['uuid']}" # mandatory
            }
          },
          'md_constraints': { # mandatory
            'access_constraints': 'otherRestrictions', # mandatory
            'use_constraints': 'otherRestrictions', # mandatory
            'other_constraints': 'http://actris.nilu.no/Content/Documents/DataPolicy.pdf', # mandatory
          },
          'md_keywords': { # mandatory
            'keywords': ['FMI', 'ACTRIS', product['humanReadableName']] # mandatory, limit on 60 character keyword
          },
          'md_data_identification': { # mandatory
            'language': 'en', # mandatory
            'topic_category': 'climatologyMeteorologyAtmosphere', # mandatory
            'description': 'time series of point measurements', # mandatory
            'station_identifier': 'INO' # mandatory, fixed list will be provided
          },
          'ex_geographic_bounding_box': { # mandatory
            'west_bound_longitude': site['longitude'], # mandatory
            'east_bound_longitude': site['longitude'], # mandatory
            'south_bound_latitude': site['latitude'], # mandatory
            'north_bound_latitude': site['latitude'] # mandatory
          },
          'ex_temporal_extent': { # mandatory
            'time_period_begin': file['measurementDate'], # mandatory
            'time_period_end': file['measurementDate'] # mandatory
          },
          'ex_vertical_extent': { # optional
            'minimum_value': None, # optional
            'maximum_value': None, # optional
            'unit_of_measure': 'm above sea level' # optional
          },
          'md_content_information': { # mandatory
            'attribute_descriptions': list(map(lambda var: re.sub(r'[-_]/g', '.', var), var_ids)), # mandatory, list of parameters
            'content_type': 'physicalMeasurement' # mandatory, fixed list ['image','thematicClassification','physicalMeasurement']
          },
          'md_distribution_information': { # mandatory
            'data_format': file['format'], # mandatory
            'version_data_format': file['format'], # mandatory
            'transfersize': file['size'], # optional
            'dataset_url': file['downloadUrl'], # mandatory
            'protocol': 'http', # mandatory, fixed list ['http','opendap']
            'description': 'Direct download of data file', # optional
            'function': 'download', # mandatory
            'restriction': {
              'set': False, # mandatory
            }
          },
          'md_actris_specific': { # mandatory
            'platform_type': 'surface_station', # mandatory ["surface_station", "simulation_chamber", "ballon"]
            'product_type': 'model' if product['id'] == 'model' else 'observation', # mandatory ["model", "observation", "fundamental_parameter"]
            'matrix': 'cloud', # mandatory ["cloud", "gas", "particle", "met"]
            'sub_matrix': 'Unknown', # mandatory
            'instrument_type': parse_instrument_type(product), # mandatory
            'program_affiliation': parse_affiliation(site), # mandatory, fixed list ['ACTRIS', 'AMAP', 'AMAP_public','EUSAAR','EMEP','ACTRIS_preliminary','GAW-WDCA','GAW-WDCRG','NOAA-ESRL']
            'legacy_data': file['legacy'], # mandatory
            'data_level': product['level'][0], # mandatory, fixed list [0, 1, 2, 3]
            'data_sublevel': product['level'][1] if 1 < len(product['level']) else None, # optional
            'data_product': 'near-realtime-data' if file['quality'] == 'nrt' else 'quality assured data' # mandatory, need fixed list e.g. ['higher level data','quality assured data', 'near-realtime-data']
          },
          'dq_data_quality_information': { # optional
            'level': 'dataset', # optional, fixed list ['attribute', 'attributeType', 'collectionHardware', 'collectionSession', 'dataset', 'series', 'nonGeographicDataset', 'dimensionGroup', 'feature', 'featureType', 'propertyType', 'fieldSession', 'software', 'service', 'model', 'tile']
          },
        }

        headers = {'X-Authorization': f"Bearer {os.environ['DVAS_PORTAL_TOKEN']}"}
        res = requests.post(f"{os.environ['DVAS_PORTAL_URL']}/Metadata/add", json=actris_json, headers=headers)
        res.raise_for_status()

    filehandle = open(lastsuccesspath, 'w')
    filehandle.write(enddate)
    filehandle.close()


if __name__ == "__main__":
    main()
