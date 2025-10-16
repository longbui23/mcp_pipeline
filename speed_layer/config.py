# Devices Random Generator
DEVICES_TYPE = {
    "thermostat": {
        "device_id": ("thermostat_1", "thermostat_100"),  
        "location_id": ("loc_1", "loc_50"),              
        "temperature": (15.0, 30.0),
        "humidity": (20.0, 60.0),
        "status": ["ON", "OFF"]
    },
    "air_quality_sensor": {
        "device_id": ("aq_1", "aq_50"),
        "location_id": ("loc_1", "loc_50"),
        "co2": (400, 2000),
        "voc": (0, 500),
        "pm2_5": (0, 150),
        "status": ["OK", "WARN", "ALERT"]
    },
    "smart_light": {
        "device_id": ("light_1", "light_100"),
        "location_id": ("loc_1", "loc_50"),
        "brightness": (0, 100),
        "color_temp": (2700, 6500),
        "status": ["ON", "OFF"]
    },
    "motion_sensor": {
        "device_id": ("motion_1", "motion_50"),
        "location_id": ("loc_1", "loc_50"),
        "motion": [True, False],
        "battery": (0, 100),
        "status": ["ACTIVE", "INACTIVE"]
    },
    "water_meter": {
        "device_id": ("water_1", "water_50"),
        "location_id": ("loc_1", "loc_50"),
        "flow_rate": (0.0, 50.0),
        "pressure": (1.0, 10.0),
        "status": ["NORMAL", "LEAK", "OFF"]
    },
}
