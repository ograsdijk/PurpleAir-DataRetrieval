from enum import Enum


class SensorSingleField(Enum):
    TEMPERATURE = ["parent", "primary", "Temperature_F"]
    HUMIDITY = ["parent", "primary", "Humidity_%"]
    PRESSURE = ["child", "primary", "Atmospheric Pressure"]


class SensorDualField(Enum):
    PM1_0 = ["primary", "PM1.0 (CF=1) ug/m3"]
    PM2_5 = ["primary", "PM2.5 (CF=1) ug/m3"]
    PM10 = ["primary", "PM10.0 (CF=1) ug/m3"]
    PM1_0_ATM = ["secondary", "PM1.0 (CF=ATM) ug/m3"]
    PM2_5_ATM = ["primary", "PM2.5 (CF=ATM) ug/m3"]
    PM10_ATM = ["secondary", "PM10.0 (CF=ATM) ug/m3"]
