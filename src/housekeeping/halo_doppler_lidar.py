from pathlib import Path

import doppy

C_TO_K = 273.15


def read_halo_doppler_lidar(full_path: Path) -> dict:
    sp = doppy.raw.HaloSysParams.from_src(full_path)
    return {
        "time": sp.time.astype("datetime64[s]"),
        "internal_temperature": sp.internal_temperature + C_TO_K,
        "internal_relative_humidity": sp.internal_relative_humidity,
        "supply_voltage": sp.supply_voltage,
        "acquisition_card_temperature": sp.acquisition_card_temperature + C_TO_K,
        "platform_pitch_angle": sp.platform_pitch_angle,
        "platform_roll_angle": sp.platform_roll_angle,
    }
