# Housekeeping variables

## Table of contents

- [basta](#basta)
- [chm15k](#chm15k)
- [cl61](#cl61)
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

`internal_heater_status`

</td>
<td></td>
<td>

on/off (1/0)

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

`transmitter_enclosure_temperature`

</td>
<td>K</td>
<td>

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
