from enums import SensorSingleField, SensorDualField
from pathlib import Path
from acquire import acquire

fields = (
    SensorSingleField.TEMPERATURE,
    SensorSingleField.HUMIDITY,
    SensorSingleField.PRESSURE,
    SensorDualField.PM1_0,
    SensorDualField.PM2_5,
    SensorDualField.PM10,
    SensorDualField.PM2_5_ATM,
)
config_path = Path() / "config.yaml"

acquire(config_path, fields)
