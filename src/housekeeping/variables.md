# Housekeeping variables

## Table of contents

- [hatpro](#hatpro)
- [rpg-fmcw-94](#rpg-fmcw-94)
- [chm15k](#chm15k)

## hatpro

<table>
<tr>
<th>Variable</th>
<th>Unit</th>
<th>Description</th>
</tr>
<tr>
<td>`alarm`</td>
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
<td>`ambient_target_sensor1_temperature`</td>
<td>K</td>
<td>



</td>
</tr>
<tr>
<td>`ambient_target_sensor2_temperature`</td>
<td>K</td>
<td>



</td>
</tr>
<tr>
<td>`ambient_target_stability_status`</td>
<td></td>
<td>

Some radiometers are using two ambient target temperature sensors for monitoring the target’s physical temperature. When the temperature readings of these two sensors differ by more than 0.3 K, the flag turns to ‘1’. ‘0’ = sensors ok.

</td>
</tr>
<tr>
<td>`boundary_layer_mode_status`</td>
<td></td>
<td>

‘1’ = boundary layer scanning active, ‘0’ = BL-mode not active

</td>
</tr>
<tr>
<td>`dew_blower_speed_status`</td>
<td></td>
<td>

‘1’ = high speed mode, ‘0’ = low speed mode

</td>
</tr>
<tr>
<td>`dly_quality_level`</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>`dly_quality_reason`</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>`gain_calibration_status`</td>
<td></td>
<td>

‘1’ = gain calibration running (using internal ambient target), ‘0’ = not active

</td>
</tr>
<tr>
<td>`hpc_quality_level`</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>`hpc_quality_reason`</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>`humidity_profiler_channel1_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`humidity_profiler_channel2_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`humidity_profiler_channel3_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`humidity_profiler_channel4_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`humidity_profiler_channel5_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`humidity_profiler_channel6_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`humidity_profiler_channel7_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`humidity_profiler_noise_diode_status`</td>
<td></td>
<td>

 ‘1’ = noise diode of humidity profiler ok, ‘0’ = noise diode not working

</td>
</tr>
<tr>
<td>`humidity_profiler_stability`</td>
<td>K</td>
<td>



</td>
</tr>
<tr>
<td>`humidity_profiler_stability_status`</td>
<td></td>
<td>

‘0’ = unknown, not enough data samples recorded yet, ‘1’ = stability ok, ‘2’ = not sufficiently stable

</td>
</tr>
<tr>
<td>`humidity_profiler_temperature`</td>
<td>K</td>
<td>



</td>
</tr>
<tr>
<td>`iwv_quality_level`</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>`iwv_quality_reason`</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>`lpr_quality_level`</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>`lpr_quality_reason`</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>`lwp_quality_level`</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>`lwp_quality_reason`</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>`noise_calibration_status`</td>
<td></td>
<td>

‘1’ = noise calibration running, ‘0’ = not active

</td>
</tr>
<tr>
<td>`noise_diode_status`</td>
<td></td>
<td>

‘0’ = noise diode is turned off for the current sample, ‘1’ = noise diode is turned on for the current sample.

</td>
</tr>
<tr>
<td>`power_failure_status`</td>
<td></td>
<td>

‘1’ = a power failure has occurred recently. When a new MDF has been started automatically after a power failure, the ‘1’ flag is kept for 1000 seconds and switching back to ‘0’ afterwards. ‘0’ = no power failure occurred.

</td>
</tr>
<tr>
<td>`rain_flag`</td>
<td></td>
<td>

‘1’ means raining, ‘0’ = no rain

</td>
</tr>
<tr>
<td>`remaining_flash_memory`</td>
<td>kB</td>
<td>



</td>
</tr>
<tr>
<td>`sky_tipping_calibration_status`</td>
<td></td>
<td>

‘1’ = sky tipping calibration running, ‘0’ = not active

</td>
</tr>
<tr>
<td>`sta_quality_level`</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>`sta_quality_reason`</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>`temperature_profiler_channel1_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`temperature_profiler_channel2_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`temperature_profiler_channel3_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`temperature_profiler_channel4_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`temperature_profiler_channel5_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`temperature_profiler_channel6_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`temperature_profiler_channel7_status`</td>
<td></td>
<td>

When a bit is set ‘1’, the corresponding channel is ok, otherwise the channel has a malfunction.

</td>
</tr>
<tr>
<td>`temperature_profiler_noise_diode_status`</td>
<td></td>
<td>

‘1’ = noise diode of temperature profiler ok, ‘0’ = noise diode not working

</td>
</tr>
<tr>
<td>`temperature_profiler_stability`</td>
<td>K</td>
<td>



</td>
</tr>
<tr>
<td>`temperature_profiler_stability_status`</td>
<td></td>
<td>

‘0’ = unknown, not enough data samples recorded yet, ‘1’ = stability ok, ‘2’ = not sufficiently stable

</td>
</tr>
<tr>
<td>`temperature_profiler_temperature`</td>
<td>K</td>
<td>



</td>
</tr>
<tr>
<td>`tpb_quality_level`</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>`tpb_quality_reason`</td>
<td></td>
<td>

‘0’ = unknown

‘1’ = possible external interference on a receiver channel or failure of a receiver channel that is used in the retrieval of this product.

‘2’ = LWP too high. At high rain rates the scattering on rain drops can mask the water vapour line completely and no humidity profiling or IWV determination is possible. Also the temperature profiling may be affected when the oxygen line channels are all saturated due to droplets.

‘3’ = free for future use.

</td>
</tr>
<tr>
<td>`tpc_quality_level`</td>
<td></td>
<td>

‘0’ = this level 2 product is not evaluated for quality control

‘1’ = highest quality level

‘2’ = reduced quality

‘3’ = low quality. This sample should not be used.

</td>
</tr>
<tr>
<td>`tpc_quality_reason`</td>
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
<td>`direct_detection_voltage`</td>
<td>V</td>
<td>

Direct detection channel voltage of current sample

</td>
</tr>
<tr>
<td>`environment_temperature`</td>
<td>K</td>
<td>

Environment temperature of current sample

</td>
</tr>
<tr>
<td>`pc_temperature`</td>
<td>K</td>
<td>

PC temperature of current sample

</td>
</tr>
<tr>
<td>`receiver_temperature`</td>
<td>K</td>
<td>

Receiver temperature of current sample

</td>
</tr>
<tr>
<td>`transmitter_power`</td>
<td>K</td>
<td>

Transmitter power of current sample

</td>
</tr>
<tr>
<td>`transmitter_temperature`</td>
<td>K</td>
<td>

Transmitter temperature of current sample

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
<td>`detector_quality`</td>
<td>percent</td>
<td>

Quality of detector signal

</td>
</tr>
<tr>
<td>`detector_temperature`</td>
<td>K</td>
<td>



</td>
</tr>
<tr>
<td>`external_temperature`</td>
<td>K</td>
<td>



</td>
</tr>
<tr>
<td>`internal_temperature`</td>
<td>K</td>
<td>



</td>
</tr>
<tr>
<td>`laser_optic_module_temperature`</td>
<td>K</td>
<td>



</td>
</tr>
<tr>
<td>`laser_quality`</td>
<td>percent</td>
<td>

Laser quality index

</td>
</tr>
<tr>
<td>`life_time`</td>
<td>h</td>
<td>

Laser operating hours

</td>
</tr>
<tr>
<td>`optical_quality`</td>
<td>percent</td>
<td>

Optical quality index

</td>
</tr>
</table>
