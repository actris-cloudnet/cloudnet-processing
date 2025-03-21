from collections import defaultdict
from os import PathLike
from typing import Any

import ceilopyter
import numpy as np

C_TO_K = 273.15


def read_ct25k(filename: str | PathLike) -> dict:
    time, msgs = ceilopyter.read_ct_file(filename)
    data: dict[str, Any] = defaultdict(list)
    for msg in msgs:
        data["laser_pulse_energy"].append(msg.laser_pulse_energy)
        data["laser_temperature"].append(msg.laser_temperature + C_TO_K)
        data["receiver_sensitivity"].append(msg.receiver_sensitivity)
        data["window_contamination"].append(msg.window_contamination)
        data["background_light"].append(msg.background_light)
        for key, value in vars(msg.status).items():
            data[key].append(int(value))
    result = {key: np.array(value) for key, value in data.items()}
    result["time"] = np.array(time, dtype="datetime64")
    return result


def read_cl31_cl51(filename: str | PathLike) -> dict:
    time, msgs = ceilopyter.read_cl_file(filename)
    data: dict[str, Any] = defaultdict(list)
    for msg in msgs:
        data["laser_pulse_energy"].append(msg.laser_pulse_energy)
        data["laser_temperature"].append(msg.laser_temperature + C_TO_K)
        data["window_transmission"].append(msg.window_transmission)
        data["background_light"].append(msg.background_light)
        for key, value in vars(msg.status).items():
            data[key].append(int(value))
    result = {key: np.array(value) for key, value in data.items()}
    result["time"] = np.array(time, dtype="datetime64")
    return result


def read_cs135(filename: str | PathLike) -> dict:
    time, msgs = ceilopyter.read_cs_file(filename)
    data: dict[str, Any] = defaultdict(list)
    for msg in msgs:
        data["laser_pulse_energy"].append(msg.laser_pulse_energy)
        data["laser_temperature"].append(msg.laser_temperature + C_TO_K)
        data["window_transmission"].append(msg.window_transmission)
        data["background_light"].append(msg.background_light)
        for key, value in vars(msg.status).items():
            data[key].append(int(value))
    result = {key: np.array(value) for key, value in data.items()}
    result["time"] = np.array(time, dtype="datetime64")
    return result
