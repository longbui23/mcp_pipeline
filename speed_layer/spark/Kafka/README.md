+ checkpoint helps to backfill easily --> fault tolerance

| Topic                     | Description                   | Example Fields                                                                           |
| ------------------------- | ----------------------------- | ---------------------------------------------------------------------------------------- |
| `topic_drill_string`      | Drill string sensors          | `id`, `timestamp`, `vibration`, `torque`, `rpm`, `drag`, `hookload`                      |
| `topic_mud_logging`       | Mud logging metrics           | `id`, `timestamp`, `mud_density`, `flow_rate`, `viscosity`, `weight`                     |
| `topic_bit_data`          | Drill bit & head data         | `id`, `timestamp`, `bit_pressure`, `bit_temperature`, `wear`, `penetration_rate`         |
| `topic_well_positioning`  | Well depth and positioning    | `id`, `timestamp`, `depth`, `inclination`, `azimuth`, `deviation`                        |
| `topic_hydraulics`        | Pump and fluid data           | `id`, `timestamp`, `pump_pressure`, `pump_rate`, `fluid_volume`                          |
| `topic_casing_cementing`  | Casing and cementing          | `id`, `timestamp`, `cement_density`, `slurry_flow`, `pump_pressure`                      |
| `topic_formation_eval`    | Formation evaluation          | `id`, `timestamp`, `gamma_ray`, `resistivity`, `porosity`, `density`                     |
| `topic_surface_equipment` | Surface equipment             | `id`, `timestamp`, `hoist_status`, `drawworks_rpm`, `pump_motor_status`, `mud_motor_rpm` |
| `topic_safety_env`        | Environmental and safety      | `id`, `timestamp`, `gas_level`, `temperature`, `humidity`, `alarm`                       |
| `topic_filling_storage`   | Mud tank and cuttings storage | `id`, `timestamp`, `mud_volume`, `cuttings_volume`, `solids_content`                     |


# Real-time
- Detect overpressure/leaks (pressure sensor)
- Temperature sensors (prevent overheating)
- Flow Rate (early detection of leak or blockage)
- Vibration + mechanical failures
- Valve positions
- Drilling parameters

# No real-time
- Oil composition/ density/ gravity
- Tank level measurement
- Well production rates
- Equipment maintanence logs
- Temp history for trend analytics
- Environment sensors
- Weather/wind/humidity
