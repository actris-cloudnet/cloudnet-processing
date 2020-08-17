import os
import shutil
import requests

# 1. Upload metadata:

hash_sum = '1ed679842745d1b1809c63d203a0c2bfd2f2b43e15123b306ea9f7ec4c089b71'
filename = 'chm15k_20200405.nc'

shutil.copyfile(f'tests/data/input/{filename}', f'tests/data/api_input/{filename}')
folder = 'processed_data/granada/'
if os.path.exists(folder):
    shutil.rmtree(folder)

meta = {
  'filename': filename,
  'measurementDate': '2020-04-05',
  'hashSum': hash_sum,
  'product': 'lidar'
}
url = 'http://localhost:3000/protected/upload-metadata/'
res = requests.post(url, json=meta, auth=('granada', 'test'))
print(res.status_code, res.text)

# 2. Upload actual file:

url = f'http://localhost:5700/data/{hash_sum}'
headers = {
  'accept': 'application/json',
  'Content-Type': 'multipart/form-data'
}
full_path = f'tests/data/api_input/{filename}'
files = {'file_submitted': (full_path,
                            open(full_path, 'rb'),
                            'application/x-netcdf',
                            {'Content-Type': 'multipart/form-data', 'accept': 'application/json'})
         }

res = requests.post(url, files=files)
print(res.status_code, res.text)


