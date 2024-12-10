import doppy

C_TO_K = 273.15


def read_halo_doppler_lidar(src: bytes) -> dict:
    sp = doppy.raw.HaloSysParams.from_src(src)
    return {
        "time": sp.time.astype("datetime64[s]"),
        "internal_temperature": sp.internal_temperature + C_TO_K,
        "internal_relative_humidity": sp.internal_relative_humidity,
        "supply_voltage": sp.supply_voltage,
        "acquisition_card_temperature": sp.acquisition_card_temperature + C_TO_K,
        "pitch": sp.pitch,
        "roll": sp.roll,
    }
