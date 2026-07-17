import csv
import datetime
from pathlib import Path

import numpy as np

C_TO_K = 273.15
HPA_TO_PA = 100


def read_wls_environmental_data(full_path: Path) -> dict:
    time_str = None
    data = {}
    with open(full_path, encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        for row in reader:
            key = row["Name"]
            try:
                value = float(row["Mean"])
            except ValueError:
                continue
            if row["Unit"] == "°C":
                value += C_TO_K
            elif row["Unit"] == "hPa":
                value *= HPA_TO_PA
            if time_str is not None and time_str != row["Timestamp"]:
                raise ValueError("Multiple timestamps detected")
            time_str = row["Timestamp"]
            data[key] = np.array([value])
    if time_str is None:
        raise ValueError("Failed to read any data")
    time = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
    data["time"] = np.array([time])
    return data
