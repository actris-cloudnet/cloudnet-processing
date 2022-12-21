import netCDF4

from .exceptions import UnsupportedFile
from .utils import cftime2datetime64, decode_bits

STATUS_CODES_V1 = [
    ("signal_quality_error", 1),
    ("signal_recording_error", 1),
    ("signal_value_error", 1),
    ("mainboard_error", 1),
    ("netcdf_create_error", 1),
    ("netcdf_write_error", 1),
    ("rs485_error", 1),
    ("sd_card_error", 1),
    ("detector_voltage_error", 1),
    ("internal_temperature_warning", 1),
    ("laser_optical_module_temperature_error", 1),
    # Next two bits were combined into 'laser_controller_error' in firmware
    # version 0.733:
    ("laser_trigger_error", 1),
    ("laser_driver_board_temperature_warning", 1),
    ("laser_interlock_error", 1),
    ("laser_head_temperature_error", 1),
    ("laser_replace_warning", 1),
    ("signal_quality_warning", 1),
    ("windows_contaminated_warning", 1),
    ("signal_processing_warning", 1),
    ("laser_detector_warning", 1),
    ("file_system_warning", 1),
    ("rs485_warning", 1),
    ("afd_warning", 1),
    ("configuration_warning", 1),
    ("laser_optical_module_temperature_warning", 1),
    ("external_temperature_warning", 1),
    ("detector_temperature_warning", 1),
    ("laser_warning", 1),
    ("layers_note", 1),
    ("started_note", 1),
    ("standby_note", 1),
]


STATUS_CODES_V2 = [
    ("signal_quality_error", 1),
    ("signal_recording_error", 1),
    ("signal_value_error", 1),
    ("mainboard_error", 1),
    ("netcdf_create_error", 1),
    ("netcdf_write_error", 1),
    ("rs485_error", 1),
    ("sd_card_error", 1),
    ("detector_voltage_error", 1),
    ("internal_temperature_warning", 1),
    ("laser_optical_module_temperature_error", 1),
    ("laser_trigger_error", 1),
    # Introduced in firmware version 0.733, combined with 'mainboard_error' in
    # version 1.070:
    ("firmware_error", 1),
    ("laser_controller_error", 1),
    ("laser_head_temperature_error", 1),
    ("laser_replace_warning", 1),
    ("signal_quality_warning", 1),
    ("windows_contaminated_warning", 1),
    ("signal_processing_warning", 1),
    ("laser_detector_warning", 1),
    ("file_system_warning", 1),
    ("rs485_warning", 1),
    ("afd_warning", 1),
    ("configuration_warning", 1),
    ("laser_optical_module_temperature_warning", 1),
    ("external_temperature_warning", 1),
    ("detector_temperature_warning", 1),
    ("laser_warning", 1),
    ("layers_note", 1),
    ("started_note", 1),
    ("standby_note", 1),
]


STATUS_CODES_V3 = [
    ("signal_quality_error", 1),
    ("signal_recording_error", 1),
    ("signal_value_error", 1),
    ("mainboard_error", 1),
    ("netcdf_create_error", 1),
    ("netcdf_write_error", 1),
    ("rs485_error", 1),
    ("sd_card_error", 1),
    ("detector_voltage_error", 1),
    ("internal_temperature_warning", 1),
    ("laser_optical_module_temperature_error", 1),
    ("laser_trigger_error", 1),
    # Introduced in firmware version 1.070:
    ("ntp_note", 1),
    ("laser_controller_error", 1),
    ("laser_head_temperature_error", 1),
    ("laser_replace_warning", 1),
    ("signal_quality_warning", 1),
    ("windows_contaminated_warning", 1),
    ("signal_processing_warning", 1),
    ("laser_detector_warning", 1),
    ("file_system_warning", 1),
    ("rs485_warning", 1),
    ("afd_warning", 1),
    ("configuration_warning", 1),
    ("laser_optical_module_temperature_warning", 1),
    ("external_temperature_warning", 1),
    ("detector_temperature_warning", 1),
    ("laser_warning", 1),
    ("layers_note", 1),
    ("started_note", 1),
    ("standby_note", 1),
]


def read_chm15k(nc: netCDF4.Dataset) -> dict:
    measurements = {var: nc[var][:] for var in nc.variables.keys()}
    measurements["time"] = cftime2datetime64(nc["time"])

    try:
        versions = nc.software_version.split()
        firmware_version = versions[2]
    except (IndexError, AttributeError) as exc:
        raise UnsupportedFile("Unknown firmware version") from exc

    if firmware_version < "0.733":
        status_bits = decode_bits(nc.variables["error_ext"][:], STATUS_CODES_V1)
        status_bits["laser_controller_error"] = (
            status_bits["laser_driver_board_temperature_warning"]
            | status_bits["laser_interlock_error"]
        )
        del status_bits["laser_driver_board_temperature_warning"]
        del status_bits["laser_interlock_error"]
    elif firmware_version < "1.070":
        status_bits = decode_bits(nc.variables["error_ext"][:], STATUS_CODES_V2)
        status_bits["mainboard_error"] |= status_bits["firmware_error"]
        del status_bits["firmware_error"]
    else:
        status_bits = decode_bits(nc.variables["error_ext"][:], STATUS_CODES_V3)

    return measurements | status_bits
