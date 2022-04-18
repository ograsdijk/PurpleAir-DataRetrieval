import datetime
import logging
from dataclasses import dataclass
from typing import Optional

from pandas import DataFrame
from purpleair.channel import Channel
from purpleair.network import Sensor


@dataclass
class SensorData:
    id: int
    is_parent: bool
    parent: Optional[int]
    primary: DataFrame
    secondary: DataFrame
    lat: float
    lon: float
    location_type: str
    name: str
    hardware: str
    model: str


@dataclass
class SensorGroupData:
    parent: SensorData
    child: SensorData


def create_sensordata(
    sensor: Sensor, df_primary: DataFrame, df_secondary: DataFrame
) -> SensorData:
    is_parent = True if sensor.parent is None else False
    parent = sensor.parent if not is_parent else None
    return SensorData(
        id=sensor.identifier,
        is_parent=is_parent,
        parent=parent,
        primary=df_primary,
        secondary=df_secondary,
        lat=sensor.lat,
        lon=sensor.lon,
        location_type=sensor.location_type,
        name=sensor.name,
        hardware=sensor.hardware,
        model=sensor.model,
    )


def get_historical_data_sensor(
    sensor: Channel, weeks_to_get: int, start_date: Optional[datetime.datetime] = None
) -> SensorData:
    if start_date is None:
        start_date = datetime.datetime.now()
    df_primary = sensor.get_historical(
        weeks_to_get=weeks_to_get, thingspeak_field="primary", start_date=start_date
    )
    df_secondary = sensor.get_historical(
        weeks_to_get=weeks_to_get, thingspeak_field="secondary", start_date=start_date
    )
    return create_sensordata(sensor, df_primary, df_secondary)


def get_historical_data(
    sensor: Sensor, weeks_to_get: int, start_date: Optional[datetime.datetime] = None
) -> SensorGroupData:
    logging.info(f"Retrieving {sensor} data for the past {weeks_to_get} weeks")
    parent = get_historical_data_sensor(
        sensor.parent, weeks_to_get, start_date=start_date
    )
    child = get_historical_data_sensor(
        sensor.child, weeks_to_get, start_date=start_date
    )
    return SensorGroupData(parent, child)
