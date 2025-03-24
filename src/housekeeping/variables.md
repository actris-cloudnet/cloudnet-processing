# Housekeeping variables

## Table of contents

- [basta](#basta)
- [chm15k](#chm15k)
- [cl31-cl51](#cl31-cl51)
- [cl61](#cl61)
- [cs135](#cs135)
- [ct25k](#ct25k)
- [halo-doppler-lidar](#halo-doppler-lidar)
- [hatpro](#hatpro)
- [rpg-fmcw-94](#rpg-fmcw-94)

## basta

<table>
<tr>
<th>Variable</th>
<th>Unit</th>
<th>Description</th>
</tr>
<tr>
<td>

`controller_error_code`

</td>
<td></td>
<td>

Unspecified error code

</td>
</tr>
<tr>
<td>

`free_disk_space`

</td>
<td>MB</td>
<td>

</td>
</tr>
<tr>
<td>

`peltier_fan1_current`

</td>
<td>A</td>
<td>

</td>
</tr>
<tr>
<td>

`peltier_fan1_voltage`

</td>
<td>V</td>
<td>

</td>
</tr>
<tr>
<td>

`peltier_fan2_current`

</td>
<td>A</td>
<td>

</td>
</tr>
<tr>
<td>

`peltier_fan2_voltage`

</td>
<td>V</td>
<td>

</td>
</tr>
<tr>
<td>

`peltier_fet_temperature`

</td>
<td>K</td>
<td>

Peltier FET temperature

</td>
</tr>
<tr>
<td>

`peltier_output`

</td>
<td>percent</td>
<td>

</td>
</tr>
<tr>
<td>

`peltier_power_supply_current`

</td>
<td>A</td>
<td>

</td>
</tr>
<tr>
<td>

`peltier_power_supply_voltage`

</td>
<td>V</td>
<td>

</td>
</tr>
<tr>
<td>

`peltier_set_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`positioner_azimuth_angle`

</td>
<td>degree</td>
<td>

</td>
</tr>
<tr>
<td>

`positioner_elevation_angle`

</td>
<td>degree</td>
<td>

</td>
</tr>
<tr>
<td>

`radar_air_flow`

</td>
<td>L/min</td>
<td>

</td>
</tr>
<tr>
<td>

`radar_amplifier_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`radar_box_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`radar_oscillator_status`

</td>
<td></td>
<td>

1 = PLO, 0 = VCO

</td>
</tr>
<tr>
<td>

`radar_phase_lock`

</td>
<td></td>
<td>

1 = phase lock, 0 = otherwise

</td>
</tr>
<tr>
<td>

`radar_pitch_angle`

</td>
<td>degree</td>
<td>

</td>
</tr>
<tr>
<td>

`radar_power_supply_current`

</td>
<td>A</td>
<td>

</td>
</tr>
<tr>
<td>

`radar_power_supply_voltage`

</td>
<td>V</td>
<td>

</td>
</tr>
<tr>
<td>

`radar_relative_humidity`

</td>
<td>percent</td>
<td>

</td>
</tr>
<tr>
<td>

`radar_roll_angle`

</td>
<td>degree</td>
<td>

</td>
</tr>
<tr>
<td>

`radar_thermal_cutoff_50`

</td>
<td></td>
<td>

1 = thermal cutoff at 50°C, 0 = otherwise

</td>
</tr>
<tr>
<td>

`radar_transmitted_power`

</td>
<td>dBm</td>
<td>

</td>
</tr>
<tr>
<td>

`radar_vco_frequency`

</td>
<td>MHz</td>
<td>

Voltage Controlled Oscillator frequency

</td>
</tr>
<tr>
<td>

`radar_yaw_angle`

</td>
<td>degree</td>
<td>

</td>
</tr>
<tr>
<td>

`soft_thermal_cutoff_threshold`

</td>
<td>K</td>
<td>

Temperature limit of soft cutoff

</td>
</tr>
<tr>
<td>

`wind_blower_state`

</td>
<td></td>
<td>

Unspecified code

</td>
</tr>
</table>

## chm15k

<table>
<tr>
<th>Variable</th>
<th>Unit</th>
<th>Description</th>
</tr>
<tr>
<td>

`afd_warning`

</td>
<td></td>
<td>

1 = AFD problem, 0 = otherwise

</td>
</tr>
<tr>
<td>

`configuration_warning`

</td>
<td></td>
<td>

1 = Configuration problem, 0 = otherwise

</td>
</tr>
<tr>
<td>

`detector_quality`

</td>
<td>percent</td>
<td>

Quality of detector signal

</td>
</tr>
<tr>
<td>

`detector_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`detector_temperature_warning`

</td>
<td></td>
<td>

1 = Detector temperature out of range, 0 = otherwise

</td>
</tr>
<tr>
<td>

`detector_voltage_error`

</td>
<td></td>
<td>

1 = Detector voltage control failed or cable absent or defective, 0 = otherwise

</td>
</tr>
<tr>
<td>

`external_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`external_temperature_warning`

</td>
<td></td>
<td>

1 = External temperature warning, 0 = otherwise

</td>
</tr>
<tr>
<td>

`file_system_warning`

</td>
<td></td>
<td>

1 = File system, fsck repaired bad sectors, 0 = otherwise

</td>
</tr>
<tr>
<td>

`internal_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`internal_temperature_warning`

</td>
<td></td>
<td>

1 = Inner housing temperature out of range, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_controller_error`

</td>
<td></td>
<td>

1 = Laser controller error, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_detector_warning`

</td>
<td></td>
<td>

1 = Laser detector misaligned or receiver window soiled, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_head_temperature_error`

</td>
<td></td>
<td>

1 = Laser head temperature error, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_operating_time`

</td>
<td>h</td>
<td>

</td>
</tr>
<tr>
<td>

`laser_optical_module_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`laser_optical_module_temperature_error`

</td>
<td></td>
<td>

1 = Laser optical unit temperature error, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_optical_module_temperature_warning`

</td>
<td></td>
<td>

1 = Laser optical unit temperature warning, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_pulses`

</td>
<td>pulses/second</td>
<td>

number of laser pulses

</td>
</tr>
<tr>
<td>

`laser_quality`

</td>
<td>percent</td>
<td>

Laser quality index

</td>
</tr>
<tr>
<td>

`laser_replace_warning`

</td>
<td></td>
<td>

1 = Replace laser – ageing, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_trigger_error`

</td>
<td></td>
<td>

1 = Laser trigger not detected or laser disabled (safety-related), 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_warning`

</td>
<td></td>
<td>

1 = General laser issue, 0 = otherwise

</td>
</tr>
<tr>
<td>

`layers_note`

</td>
<td></td>
<td>

1 = Number of layers > 3 and telegram selected, 0 = otherwise

</td>
</tr>
<tr>
<td>

`mainboard_error`

</td>
<td></td>
<td>

1 = Mainboard detection failed (APD bias) or firmware and CPU do not match, 0 = otherwise

</td>
</tr>
<tr>
<td>

`netcdf_create_error`

</td>
<td></td>
<td>

1 = Create new NetCDF file error, 0 = otherwise

</td>
</tr>
<tr>
<td>

`netcdf_write_error`

</td>
<td></td>
<td>

1 = Write / add to NetCDF error, 0 = otherwise

</td>
</tr>
<tr>
<td>

`ntp_note`

</td>
<td></td>
<td>

1 = NTP problem, 0 = otherwise (introduced in firmware version 1.070 released in November 2020)

</td>
</tr>
<tr>
<td>

`optical_quality`

</td>
<td>percent</td>
<td>

Optical quality index

</td>
</tr>
<tr>
<td>

`rs485_error`

</td>
<td></td>
<td>

1 = RS485 telegram cannot be generated and transmitted, 0 = otherwise

</td>
</tr>
<tr>
<td>

`rs485_warning`

</td>
<td></td>
<td>

1 = RS485 baud rate / transfer mode reset, 0 = otherwise

</td>
</tr>
<tr>
<td>

`sd_card_error`

</td>
<td></td>
<td>

1 = SD card absent or defective, 0 = otherwise

</td>
</tr>
<tr>
<td>

`signal_baseline`

</td>
<td>photons/second</td>
<td>

baseline raw signal

</td>
</tr>
<tr>
<td>

`signal_processing_warning`

</td>
<td></td>
<td>

1 = Signal processing warning, 0 = otherwise

</td>
</tr>
<tr>
<td>

`signal_quality_error`

</td>
<td></td>
<td>

1 = Signal quality error, 0 = otherwise

</td>
</tr>
<tr>
<td>

`signal_quality_warning`

</td>
<td></td>
<td>

1 = Signal quality – high noise level, 0 = otherwise

</td>
</tr>
<tr>
<td>

`signal_recording_error`

</td>
<td></td>
<td>

1 = Signal recording error, 0 = otherwise

</td>
</tr>
<tr>
<td>

`signal_stddev`

</td>
<td>photons/second</td>
<td>

standard deviation of raw signal

</td>
</tr>
<tr>
<td>

`signal_value_error`

</td>
<td></td>
<td>

1 = Signal values null or void, 0 = otherwise

</td>
</tr>
<tr>
<td>

`standby_note`

</td>
<td></td>
<td>

1 = Standby mode on, 0 = otherwise

</td>
</tr>
<tr>
<td>

`started_note`

</td>
<td></td>
<td>

1 = Device was started, 0 = otherwise

</td>
</tr>
<tr>
<td>

`windows_contaminated_warning`

</td>
<td></td>
<td>

1 = Windows contaminated, 0 = otherwise

</td>
</tr>
</table>

## cl31-cl51

<table>
<tr>
<th>Variable</th>
<th>Unit</th>
<th>Description</th>
</tr>
<tr>
<td>

`background_light`

</td>
<td>mV</td>
<td>

Background light

</td>
</tr>
<tr>
<td>

`background_radiance_warning`

</td>
<td></td>
<td>

1 = High background radiance, 0 = otherwise

</td>
</tr>
<tr>
<td>

`battery_fail_warning`

</td>
<td></td>
<td>

1 = Battery failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`battery_low_warning`

</td>
<td></td>
<td>

1 = Battery voltage low, 0 = otherwise

</td>
</tr>
<tr>
<td>

`battery_power_status`

</td>
<td></td>
<td>

1 = Working from battery, 0 = otherwise

</td>
</tr>
<tr>
<td>

`blower_fail_warning`

</td>
<td></td>
<td>

1 = Blower failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`blower_heater_status`

</td>
<td></td>
<td>

1 = Blower heater is on, 0 = otherwise

</td>
</tr>
<tr>
<td>

`blower_status`

</td>
<td></td>
<td>

1 = Blower is on, 0 = otherwise

</td>
</tr>
<tr>
<td>

`ceilometer_board_fail_alarm`

</td>
<td></td>
<td>

1 = Ceilometer engine board failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`ceilometer_board_warning`

</td>
<td></td>
<td>

1 = Ceilometer engine board failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`coaxial_cable_fail_alarm`

</td>
<td></td>
<td>

1 = Coaxial cable failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`heater_fault_warning`

</td>
<td></td>
<td>

1 = Heater fault, 0 = otherwise

</td>
</tr>
<tr>
<td>

`humidity_high_warning`

</td>
<td></td>
<td>

1 = High humidity, 0 = otherwise

</td>
</tr>
<tr>
<td>

`humidity_sensor_fail_warning`

</td>
<td></td>
<td>

1 = Humidity sensor failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`internal_heater_status`

</td>
<td></td>
<td>

1 = Internal heater is on, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_monitor_fail_warning`

</td>
<td></td>
<td>

1 = Laser monitor failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_pulse_energy`

</td>
<td>percent</td>
<td>

Laser pulse energy

</td>
</tr>
<tr>
<td>

`laser_temperature`

</td>
<td>K</td>
<td>

Laser temperature

</td>
</tr>
<tr>
<td>

`light_path_obstruction_alarm`

</td>
<td></td>
<td>

1 = Light path obstruction, 0 = otherwise

</td>
</tr>
<tr>
<td>

`manual_blower_status`

</td>
<td></td>
<td>

1 = Manual blower control, 0 = otherwise

</td>
</tr>
<tr>
<td>

`manual_data_status`

</td>
<td></td>
<td>

1 = Manual data acquisition settings are effective, 0 = otherwise

</td>
</tr>
<tr>
<td>

`memory_error_alarm`

</td>
<td></td>
<td>

1 = Memory error, 0 = otherwise

</td>
</tr>
<tr>
<td>

`polling_mode_status`

</td>
<td></td>
<td>

1 = Polling mode is on, 0 = otherwise

</td>
</tr>
<tr>
<td>

`receiver_fail_alarm`

</td>
<td></td>
<td>

1 = Receiver failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`receiver_saturation_alarm`

</td>
<td></td>
<td>

1 = Receiver saturation, 0 = otherwise

</td>
</tr>
<tr>
<td>

`receiver_warning`

</td>
<td></td>
<td>

1 = Receiver warning, 0 = otherwise

</td>
</tr>
<tr>
<td>

`self_test_status`

</td>
<td></td>
<td>

1 = Self test in progress, 0 = otherwise

</td>
</tr>
<tr>
<td>

`standby_status`

</td>
<td></td>
<td>

1 = Standby mode is on, 0 = otherwise

</td>
</tr>
<tr>
<td>

`tilt_angle_warning`

</td>
<td></td>
<td>

1 = Tilt angle > 45 degrees warning, 0 = otherwise

</td>
</tr>
<tr>
<td>

`transmitter_expire_warning`

</td>
<td></td>
<td>

1 = Transmitter expires, 0 = otherwise

</td>
</tr>
<tr>
<td>

`transmitter_fail_alarm`

</td>
<td></td>
<td>

1 = Transmitter failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`transmitter_shutoff_alarm`

</td>
<td></td>
<td>

1 = Transmitter shut-off, 0 = otherwise

</td>
</tr>
<tr>
<td>

`voltage_fail_alarm`

</td>
<td></td>
<td>

1 = Voltage failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`window_contam_warning`

</td>
<td></td>
<td>

1 = Window contamination, 0 = otherwise

</td>
</tr>
<tr>
<td>

`window_transmission`

</td>
<td>percent</td>
<td>

Window transmission

</td>
</tr>
</table>

## cl61

<table>
<tr>
<th>Variable</th>
<th>Unit</th>
<th>Description</th>
</tr>
<tr>
<td>

`background_radiance`

</td>
<td></td>
<td>

</td>
</tr>
<tr>
<td>

`data_generation_status`

</td>
<td></td>
<td>

0 = OK, 3 = Data generation failure.

</td>
</tr>
<tr>
<td>

`datacom_overall_status`

</td>
<td></td>
<td>

0 = OK.

</td>
</tr>
<tr>
<td>

`device_controller_electronics_status`

</td>
<td></td>
<td>

0 = OK, 3 = Temperature sensor failure. Restart device and if issue persists, replace device control module.

</td>
</tr>
<tr>
<td>

`device_controller_overall_status`

</td>
<td></td>
<td>

0 = OK, 2 = At least one warning. Check device control module warnings, 3 = At least one alarm. Check device control module warnings and alarms.

</td>
</tr>
<tr>
<td>

`device_controller_temperature_status`

</td>
<td></td>
<td>

0 = OK, 3 = Temperature sensor failure. Restart device and if issue persists, replace device control module.

</td>
</tr>
<tr>
<td>

`device_overall_status`

</td>
<td></td>
<td>

0 = OK, 2 = At least one warning, 3 = At least one alarm.

</td>
</tr>
<tr>
<td>

`internal_heater_status`

</td>
<td></td>
<td>

on/off (1/0)

</td>
</tr>
<tr>
<td>

`internal_heater_warning`

</td>
<td></td>
<td>

0 = OK, 2 = Failure in inside heater.

</td>
</tr>
<tr>
<td>

`internal_pressure`

</td>
<td>Pa</td>
<td>

</td>
</tr>
<tr>
<td>

`internal_relative_humidity`

</td>
<td>percent</td>
<td>

</td>
</tr>
<tr>
<td>

`internal_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`laser_power`

</td>
<td>percent</td>
<td>

</td>
</tr>
<tr>
<td>

`laser_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`maintenance_overall_status`

</td>
<td></td>
<td>

0 = OK, 1 = Prepare for maintenance, 2 = Maintenance recommended, 3 = Immediate maintenance needed.

</td>
</tr>
<tr>
<td>

`measurement_data_destination_not_set_status`

</td>
<td></td>
<td>

0 = Reporting destination URL for output data set, 1 = Reporting destination URL for output data not set.

</td>
</tr>
<tr>
<td>

`measurement_status`

</td>
<td></td>
<td>

0 = OK, 2 = Measurement failure, contact Vaisala.

</td>
</tr>
<tr>
<td>

`optics_unit_accelerometer_status`

</td>
<td></td>
<td>

0 = OK, 3 = Tilt angle sensor failure. Restart device.

</td>
</tr>
<tr>
<td>

`optics_unit_electronics_status`

</td>
<td></td>
<td>

0 = OK, 3 = Memory failure. Restart device and if issue persists, replace device control module.

</td>
</tr>
<tr>
<td>

`optics_unit_memory_status`

</td>
<td></td>
<td>

0 = OK, 3 = Memory failure. Restart device and if issue persists, replace device control module.

</td>
</tr>
<tr>
<td>

`optics_unit_overall_status`

</td>
<td></td>
<td>

0 = OK, 2 = At least one warning. Check optics warnings, 3 = At least one alarm. Check optics warnings and alarms.

</td>
</tr>
<tr>
<td>

`optics_unit_tilt_angle_status`

</td>
<td></td>
<td>

0 = OK, 2 = Tilt angle too steep.

</td>
</tr>
<tr>
<td>

`receiver_electronics_status`

</td>
<td></td>
<td>

0 = OK, 3 = Memory failure. Restart device and if issue persists, replace receiver.

</td>
</tr>
<tr>
<td>

`receiver_memory_status`

</td>
<td></td>
<td>

0 = OK, 3 = Memory failure. Restart device and if issue persists, replace receiver.

</td>
</tr>
<tr>
<td>

`receiver_overall_status`

</td>
<td></td>
<td>

0 = OK, 2 = At least one warning. Check receiver warnings, 3 = At least one alarm. Check receiver warnings and alarms.

</td>
</tr>
<tr>
<td>

`receiver_sensitivity_status`

</td>
<td></td>
<td>

0 = OK, 2 = Receiver failure.

</td>
</tr>
<tr>
<td>

`receiver_solar_saturation_status`

</td>
<td></td>
<td>

0 = OK, 3 = Receiver saturated by direct sunlight. Measurements are invalid.

</td>
</tr>
<tr>
<td>

`receiver_voltage_status`

</td>
<td></td>
<td>

0 = OK, 3 = Receiver voltage limit exceeded.

</td>
</tr>
<tr>
<td>

`recently_started_status`

</td>
<td></td>
<td>

0 = Normal operation, 1 = Device recently restarted.

</td>
</tr>
<tr>
<td>

`servo_drive_control_status`

</td>
<td></td>
<td>

0 = OK, 3 = Polarizator servo control failure. Restart device and if issue persists, replace device control module.

</td>
</tr>
<tr>
<td>

`servo_drive_electronics_status`

</td>
<td></td>
<td>

0 = OK, 1 = Polarizator not ready, 2 = Polarizator initialization failure, 3 = Polarizator failure. Restart device and if issue persists, replace device control module.

</td>
</tr>
<tr>
<td>

`servo_drive_memory_status`

</td>
<td></td>
<td>

0 = OK, 3 = Memory failure. Restart device and if issue persists, replace device control module.

</td>
</tr>
<tr>
<td>

`servo_drive_overall_status`

</td>
<td></td>
<td>

0 = OK, 2 = At least one warning. Check servo drive module warnings, 3 = At least one alarm. Check servo drive module warnings and alarms.

</td>
</tr>
<tr>
<td>

`servo_drive_ready_status`

</td>
<td></td>
<td>

0 = OK, 2 = Recent restart, waiting servo to be ready, 3 = Polarizator servo is not ready, contact Vaisala.

</td>
</tr>
<tr>
<td>

`transmitter_electronics_status`

</td>
<td></td>
<td>

0 = OK, 3 = Laser power failure and memory failure. Restart device and if issue persists, replace transmitter.

</td>
</tr>
<tr>
<td>

`transmitter_enclosure_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`transmitter_light_source_power_status`

</td>
<td></td>
<td>

0 = OK, 2 = Transmitter laser power low, contact Vaisala, 3 = Transmitter laser power very low, contact Vaisala.

</td>
</tr>
<tr>
<td>

`transmitter_light_source_safety_status`

</td>
<td></td>
<td>

0 = OK, 3 = Safety limit exceeded.

</td>
</tr>
<tr>
<td>

`transmitter_light_source_status`

</td>
<td></td>
<td>

0 = OK, 2 = Warning, 3 = Transmitter laser power failure, contact Vaisala.

</td>
</tr>
<tr>
<td>

`transmitter_memory_status`

</td>
<td></td>
<td>

0 = OK, 3 = Memory failure. Restart device and if issue persists, replace transmitter.

</td>
</tr>
<tr>
<td>

`transmitter_overall_status`

</td>
<td></td>
<td>

0 = OK, 2 = At least one warning. Check transmitter warnings, 3 = At least one alarm. Check transmitter warnings and alarms.

</td>
</tr>
<tr>
<td>

`window_blocking_status`

</td>
<td></td>
<td>

0 = OK, 2 = Device has detected temporary blockage during the previous 3 minutes, 3 = Device has detected window blockage. Check device.

</td>
</tr>
<tr>
<td>

`window_blower_fan_status`

</td>
<td></td>
<td>

0 = OK, 2 = Blower fan failure.

</td>
</tr>
<tr>
<td>

`window_blower_heater_status`

</td>
<td></td>
<td>

on/off (1/0)

</td>
</tr>
<tr>
<td>

`window_blower_heater_warning`

</td>
<td></td>
<td>

0 = OK, 2 = Blower heater failure.

</td>
</tr>
<tr>
<td>

`window_blower_status`

</td>
<td></td>
<td>

on/off (1/0)

</td>
</tr>
<tr>
<td>

`window_condition`

</td>
<td>percent</td>
<td>

100 for a clean, 0 for a totally dirty window

</td>
</tr>
<tr>
<td>

`window_condition_status`

</td>
<td></td>
<td>

0 = OK, 1 = Window slightly contaminated. Window cleaning recommended, 2 = Window moderately contaminated. Window cleaning required, 3 = Window dirty. Immediate window cleaning required.

</td>
</tr>
</table>

## cs135

<table>
<tr>
<th>Variable</th>
<th>Unit</th>
<th>Description</th>
</tr>
<tr>
<td>

`background_light`

</td>
<td>mV</td>
<td>

Background light

</td>
</tr>
<tr>
<td>

`battery_voltage_warning`

</td>
<td></td>
<td>

1 = The lead acid battery voltage is reading low, 0 = otherwise

</td>
</tr>
<tr>
<td>

`dsp_calibration_alarm`

</td>
<td></td>
<td>

1 = DSP factory calibration stored in flash has failed its signature check, 0 = otherwise

</td>
</tr>
<tr>
<td>

`dsp_clock_warning`

</td>
<td></td>
<td>

1 = DSP clock out of specification, 0 = otherwise

</td>
</tr>
<tr>
<td>

`dsp_inclinometer_comm_alarm`

</td>
<td></td>
<td>

1 = No communications between DSP and inclinometer board, 0 = otherwise

</td>
</tr>
<tr>
<td>

`dsp_os_alarm`

</td>
<td></td>
<td>

1 = DSP board OS signature test failed, 0 = otherwise

</td>
</tr>
<tr>
<td>

`dsp_psu_comm_alarm`

</td>
<td></td>
<td>

1 = No communications between DSP and PSU, 0 = otherwise

</td>
</tr>
<tr>
<td>

`dsp_psu_warning`

</td>
<td></td>
<td>

1 = DSP boards on board PSUs are out of bounds, 0 = otherwise

</td>
</tr>
<tr>
<td>

`dsp_ram_alarm`

</td>
<td></td>
<td>

1 = DSP board RAM test failed, 0 = otherwise

</td>
</tr>
<tr>
<td>

`dsp_temp_humidity_comm_alarm`

</td>
<td></td>
<td>

1 = Communications to the DSP board temperature and humidity chip have failed, 0 = otherwise

</td>
</tr>
<tr>
<td>

`dsp_voltage_warning`

</td>
<td></td>
<td>

1 = DSP input supply voltage is low, 0 = otherwise

</td>
</tr>
<tr>
<td>

`heater_blower_alarm`

</td>
<td></td>
<td>

1 = External heater blower failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`heater_temp_warning`

</td>
<td></td>
<td>

1 = The external heater blower assembly temperature is out of bounds, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_drive_current_alarm`

</td>
<td></td>
<td>

1 = Laser max drive current exceeded, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_max_power_alarm`

</td>
<td></td>
<td>

1 = Laser max power exceeded, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_monitor_temp_warning`

</td>
<td></td>
<td>

1 = Laser power monitor temperature out of range, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_monitor_test_alarm`

</td>
<td></td>
<td>

1 = Laser power monitor test fail, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_obscured_warning`

</td>
<td></td>
<td>

1 = Laser is obscured. This can only be set if the laser is on, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_off_status`

</td>
<td></td>
<td>

1 = Laser is off, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_power_alarm`

</td>
<td></td>
<td>

1 = Laser did not achieve significant output power, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_pulse_energy`

</td>
<td>percent</td>
<td>

Laser pulse energy

</td>
</tr>
<tr>
<td>

`laser_runtime_alarm`

</td>
<td></td>
<td>

1 = Laser run time or maximum laser drive current has been exceeded, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_shutdown_status`

</td>
<td></td>
<td>

1 = Laser shutdown by top board, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_temp_alarm`

</td>
<td></td>
<td>

1 = Laser shut down due to operating temperature out of range, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_temp_warning`

</td>
<td></td>
<td>

1 = Laser temperature out of range, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_temperature`

</td>
<td>K</td>
<td>

Laser temperature

</td>
</tr>
<tr>
<td>

`laser_thermistor_alarm`

</td>
<td></td>
<td>

1 = Laser thermistor failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_window_warning`

</td>
<td></td>
<td>

1 = Photo diode and Laser windows are dirty (This can only be set if the laser is on), 0 = otherwise

</td>
</tr>
<tr>
<td>

`mains_supply_alarm`

</td>
<td></td>
<td>

1 = Mains supply has failed (Required a PSU to be present), 0 = otherwise

</td>
</tr>
<tr>
<td>

`photo_diode_calibrator_alarm`

</td>
<td></td>
<td>

1 = Photo diode calibrator has failed, 0 = otherwise

</td>
</tr>
<tr>
<td>

`photo_diode_calibrator_temp_warning`

</td>
<td></td>
<td>

1 = Photo diode calibrator temperature is out of range, 0 = otherwise

</td>
</tr>
<tr>
<td>

`photo_diode_radiance_warning`

</td>
<td></td>
<td>

1 = Photo diode background radiance is out of range, 0 = otherwise

</td>
</tr>
<tr>
<td>

`photo_diode_saturation_warning`

</td>
<td></td>
<td>

1 = Photo diode is saturated, 0 = otherwise

</td>
</tr>
<tr>
<td>

`photo_diode_temp_warning`

</td>
<td></td>
<td>

1 = Photo diode temperature is out of range, 0 = otherwise

</td>
</tr>
<tr>
<td>

`psu_os_alarm`

</td>
<td></td>
<td>

1 = PSU OS has failed its signature check, 0 = otherwise

</td>
</tr>
<tr>
<td>

`psu_temp_warning`

</td>
<td></td>
<td>

1 = The PSUs internal temperature is high, 0 = otherwise

</td>
</tr>
<tr>
<td>

`self_test_status`

</td>
<td></td>
<td>

1 = Self-test active, 0 = otherwise

</td>
</tr>
<tr>
<td>

`sensor_gain_warning`

</td>
<td></td>
<td>

1 = The sensor could not reach the desired gain levels, 0 = otherwise

</td>
</tr>
<tr>
<td>

`sensor_humidity_warning`

</td>
<td></td>
<td>

1 = The sensors internal humidity is high, 0 = otherwise

</td>
</tr>
<tr>
<td>

`tilt_angle_warning`

</td>
<td></td>
<td>

1 = Tilt beyond limit set by user, default 45 degrees, 0 = otherwise

</td>
</tr>
<tr>
<td>

`top_adc_dac_warning`

</td>
<td></td>
<td>

1 = TOP boards ADC and DAC are not within specifications, 0 = otherwise

</td>
</tr>
<tr>
<td>

`top_dsp_comm_alarm`

</td>
<td></td>
<td>

1 = Communications have failed between TOP board and the DSP, 0 = otherwise

</td>
</tr>
<tr>
<td>

`top_os_alarm`

</td>
<td></td>
<td>

1 = TOP board OS signature test has failed, 0 = otherwise

</td>
</tr>
<tr>
<td>

`top_psu_warning`

</td>
<td></td>
<td>

1 = TOP boards on board PSUs are out of bounds, 0 = otherwise

</td>
</tr>
<tr>
<td>

`top_storage_alarm`

</td>
<td></td>
<td>

1 = TOP board non-volatile storage is corrupt, 0 = otherwise

</td>
</tr>
<tr>
<td>

`user_settings_alarm`

</td>
<td></td>
<td>

1 = User setting stored in flash failed their signature checks, 0 = otherwise

</td>
</tr>
<tr>
<td>

`watchdog_status`

</td>
<td></td>
<td>

1 = Watch dog counter updated, 0 = otherwise

</td>
</tr>
<tr>
<td>

`window_transmission`

</td>
<td>percent</td>
<td>

Window transmission

</td>
</tr>
</table>

## ct25k

<table>
<tr>
<th>Variable</th>
<th>Unit</th>
<th>Description</th>
</tr>
<tr>
<td>

`background_light`

</td>
<td>mV</td>
<td>

Background light

</td>
</tr>
<tr>
<td>

`background_radiance_warning`

</td>
<td></td>
<td>

1 = High background radiance, 0 = otherwise

</td>
</tr>
<tr>
<td>

`battery_low_warning`

</td>
<td></td>
<td>

1 = Battery low, 0 = otherwise

</td>
</tr>
<tr>
<td>

`battery_power_status`

</td>
<td></td>
<td>

1 = Working from battery, 0 = otherwise

</td>
</tr>
<tr>
<td>

`blower_fail_warning`

</td>
<td></td>
<td>

1 = Blower failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`blower_heater_status`

</td>
<td></td>
<td>

1 = Blower heater is ON, 0 = otherwise

</td>
</tr>
<tr>
<td>

`blower_status`

</td>
<td></td>
<td>

1 = Blower is ON, 0 = otherwise

</td>
</tr>
<tr>
<td>

`humidity_high_warning`

</td>
<td></td>
<td>

1 = Relative humidity is high > 85 %, 0 = otherwise

</td>
</tr>
<tr>
<td>

`internal_heater_status`

</td>
<td></td>
<td>

1 = Internal heater is ON, 0 = otherwise

</td>
</tr>
<tr>
<td>

`internal_temp_warning`

</td>
<td></td>
<td>

1 = Internal temperature high or low, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_fail_alarm`

</td>
<td></td>
<td>

1 = Laser failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_power_low_warning`

</td>
<td></td>
<td>

1 = Laser power low, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_pulse_energy`

</td>
<td>%</td>
<td>

Laser pulse energy

</td>
</tr>
<tr>
<td>

`laser_temp_shutoff_alarm`

</td>
<td></td>
<td>

1 = Laser temperature shut-off, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_temp_warning`

</td>
<td></td>
<td>

1 = Laser temperature high or low, 0 = otherwise

</td>
</tr>
<tr>
<td>

`laser_temperature`

</td>
<td>K</td>
<td>

Laser temperature

</td>
</tr>
<tr>
<td>

`manual_blower_status`

</td>
<td></td>
<td>

1 = Manual blower control, 0 = otherwise

</td>
</tr>
<tr>
<td>

`manual_settings_status`

</td>
<td></td>
<td>

1 = Manual settings are effective, 0 = otherwise

</td>
</tr>
<tr>
<td>

`polling_mode_status`

</td>
<td></td>
<td>

1 = Polling mode is ON, 0 = otherwise

</td>
</tr>
<tr>
<td>

`receiver_crosstalk_warning`

</td>
<td></td>
<td>

1 = Receiver optical cross-talk compensation poor, 0 = otherwise

</td>
</tr>
<tr>
<td>

`receiver_fail_alarm`

</td>
<td></td>
<td>

1 = Receiver failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`receiver_sensitivity`

</td>
<td>%</td>
<td>

Receiver sensitivity

</td>
</tr>
<tr>
<td>

`single_seq_mode_status`

</td>
<td></td>
<td>

1 = Single sequence mode is ON, 0 = otherwise

</td>
</tr>
<tr>
<td>

`tilt_angle_warning`

</td>
<td></td>
<td>

1 = Tilt angle is > 45 degrees, 0 = otherwise

</td>
</tr>
<tr>
<td>

`voltage_fail_alarm`

</td>
<td></td>
<td>

1 = Voltage failure, 0 = otherwise

</td>
</tr>
<tr>
<td>

`voltage_range_warning`

</td>
<td></td>
<td>

1 = Voltage high or low, 0 = otherwise

</td>
</tr>
<tr>
<td>

`window_contam_warning`

</td>
<td></td>
<td>

1 = Window contaminated, 0 = otherwise

</td>
</tr>
<tr>
<td>

`window_contamination`

</td>
<td>mV</td>
<td>

Window contamination

</td>
</tr>
</table>

## halo-doppler-lidar

<table>
<tr>
<th>Variable</th>
<th>Unit</th>
<th>Description</th>
</tr>
<tr>
<td>

`acquisition_card_temperature`

</td>
<td>K</td>
<td>

Acquisition card temperature

</td>
</tr>
<tr>
<td>

`internal_relative_humidity`

</td>
<td>percent</td>
<td>

</td>
</tr>
<tr>
<td>

`internal_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`platform_pitch_angle`

</td>
<td>degree</td>
<td>

Platform pitch angle

</td>
</tr>
<tr>
<td>

`platform_roll_angle`

</td>
<td>degree</td>
<td>

Platform roll angle

</td>
</tr>
<tr>
<td>

`supply_voltage`

</td>
<td>V</td>
<td>

Supply voltage

</td>
</tr>
</table>

## hatpro

<table>
<tr>
<th>Variable</th>
<th>Unit</th>
<th>Description</th>
</tr>
<tr>
<td>

`alarm_status`

</td>
<td></td>
<td>

The alarm flag is activated in the following cases:

- interference or failure of a channel that is used in one of the retrievals
- thermal receiver stability not sufficient for measurement
- noise diode failure of one of the receivers
- ambient target thermal sensor not stable

</td>
</tr>
<tr>
<td>

`ambient_target_sensor1_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`ambient_target_sensor2_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`ambient_target_stability_status`

</td>
<td></td>
<td>

Some radiometers are using two ambient target temperature sensors for monitoring the target’s physical temperature. When the temperature readings of these two sensors differ by more than 0.3 K, the flag turns to ‘1’. ‘0’ = sensors ok.

</td>
</tr>
<tr>
<td>

`boundary_layer_mode_status`

</td>
<td></td>
<td>

‘1’ = boundary layer scanning active, ‘0’ = BL-mode not active

</td>
</tr>
<tr>
<td>

`dew_blower_speed_status`

</td>
<td></td>
<td>

‘1’ = high speed mode, ‘0’ = low speed mode

</td>
</tr>
<tr>
<td>

`dly_quality_level`

</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>

`dly_quality_reason`

</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>

`flash_memory_free`

</td>
<td>kB</td>
<td>

Remaining flash memory

</td>
</tr>
<tr>
<td>

`gain_calibration_status`

</td>
<td></td>
<td>

‘1’ = gain calibration running (using internal ambient target), ‘0’ = not active

</td>
</tr>
<tr>
<td>

`hpc_quality_level`

</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>

`hpc_quality_reason`

</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>

`humidity_profiler_channel1_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`humidity_profiler_channel2_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`humidity_profiler_channel3_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`humidity_profiler_channel4_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`humidity_profiler_channel5_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`humidity_profiler_channel6_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`humidity_profiler_channel7_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`humidity_profiler_noise_diode_status`

</td>
<td></td>
<td>

‘1’ = noise diode of humidity profiler ok, ‘0’ = noise diode not working

</td>
</tr>
<tr>
<td>

`humidity_profiler_stability`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`humidity_profiler_stability_status`

</td>
<td></td>
<td>

‘0’ = unknown, not enough data samples recorded yet, ‘1’ = stability ok, ‘2’ = not sufficiently stable

</td>
</tr>
<tr>
<td>

`humidity_profiler_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`iwv_quality_level`

</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>

`iwv_quality_reason`

</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>

`lpr_quality_level`

</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>

`lpr_quality_reason`

</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>

`lwp_quality_level`

</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>

`lwp_quality_reason`

</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>

`noise_calibration_status`

</td>
<td></td>
<td>

‘1’ = noise calibration running, ‘0’ = not active

</td>
</tr>
<tr>
<td>

`noise_diode_status`

</td>
<td></td>
<td>

‘0’ = noise diode is turned off for the current sample, ‘1’ = noise diode is turned on for the current sample.

</td>
</tr>
<tr>
<td>

`power_failure_status`

</td>
<td></td>
<td>

‘1’ = a power failure has occurred recently. When a new MDF has been started automatically after a power failure, the ‘1’ flag is kept for 1000 seconds and switching back to ‘0’ afterwards. ‘0’ = no power failure occurred.

</td>
</tr>
<tr>
<td>

`rain_status`

</td>
<td></td>
<td>

‘1’ means raining, ‘0’ = no rain

</td>
</tr>
<tr>
<td>

`sky_tipping_calibration_status`

</td>
<td></td>
<td>

‘1’ = sky tipping calibration running, ‘0’ = not active

</td>
</tr>
<tr>
<td>

`sta_quality_level`

</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>

`sta_quality_reason`

</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>

`temperature_profiler_channel1_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`temperature_profiler_channel2_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`temperature_profiler_channel3_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`temperature_profiler_channel4_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`temperature_profiler_channel5_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`temperature_profiler_channel6_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`temperature_profiler_channel7_status`

</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>

`temperature_profiler_noise_diode_status`

</td>
<td></td>
<td>

‘1’ = noise diode of temperature profiler ok, ‘0’ = noise diode not working

</td>
</tr>
<tr>
<td>

`temperature_profiler_stability`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`temperature_profiler_stability_status`

</td>
<td></td>
<td>

‘0’ = unknown, not enough data samples recorded yet, ‘1’ = stability ok, ‘2’ = not sufficiently stable

</td>
</tr>
<tr>
<td>

`temperature_profiler_temperature`

</td>
<td>K</td>
<td>

</td>
</tr>
<tr>
<td>

`tpb_quality_level`

</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>

`tpb_quality_reason`

</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>

`tpc_quality_level`

</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>

`tpc_quality_reason`

</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
</table>

## rpg-fmcw-94

<table>
<tr>
<th>Variable</th>
<th>Unit</th>
<th>Description</th>
</tr>
<tr>
<td>

`blower_status`

</td>
<td></td>
<td>

1 = blower is on, 0 = otherwise

</td>
</tr>
<tr>
<td>

`direct_detection_voltage`

</td>
<td>V</td>
<td>

Direct detection channel voltage of current sample

</td>
</tr>
<tr>
<td>

`environment_temperature`

</td>
<td>K</td>
<td>

Environment temperature of current sample

</td>
</tr>
<tr>
<td>

`hatpro_humidity_status`

</td>
<td></td>
<td>

1 = humidity profiles are from a coupled HATPRO, 0 = otherwise

</td>
</tr>
<tr>
<td>

`hatpro_temperature_status`

</td>
<td></td>
<td>

1 = temperature profile is from a coupled HATPRO, 0 = otherwise

</td>
</tr>
<tr>
<td>

`heater_status`

</td>
<td></td>
<td>

1 = heater is on, 0 = otherwise (please note, that no RPG radar has a physical heater)

</td>
</tr>
<tr>
<td>

`pc_temperature`

</td>
<td>K</td>
<td>

PC temperature of current sample

</td>
</tr>
<tr>
<td>

`receiver_temperature`

</td>
<td>K</td>
<td>

Receiver temperature of current sample

</td>
</tr>
<tr>
<td>

`transmitter_power`

</td>
<td>K</td>
<td>

Transmitter power of current sample

</td>
</tr>
<tr>
<td>

`transmitter_temperature`

</td>
<td>K</td>
<td>

Transmitter temperature of current sample

</td>
</tr>
</table>
