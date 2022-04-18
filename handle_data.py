import logging
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from pandas import DataFrame
from purpleair.network import Sensor  # type: ignore

from enums import SensorDualField, SensorSingleField
from get_data import SensorGroupData


@dataclass
class ChannelData:
    primary: DataFrame
    secondary: DataFrame


@dataclass
class DataToSave:
    id: int
    latitude: float
    longitude: float
    location_type: str
    name: str
    hardware: str
    model: str
    channel_A: ChannelData
    channel_B: ChannelData


# sometimes the data types arent correct, not sure why yet but checking and converting
# them here
def check_dataframe_types(sensor_id: int, data: DataFrame) -> DataFrame:
    if np.dtype("object") in data.dtypes.values:
        logging.info(f"Type converting data from Sensor({sensor_id})")
        data = DataFrame(
            data.apply(lambda x: pd.to_datetime(x) if x.name == "created_at" else x)
        )

        data = DataFrame(
            data.apply(
                lambda x: pd.to_numeric(x)
                if x.name in [c for c in data.columns if c != "created_at"]
                else x
            )
        )

    return data


def handle_historical_data(
    sensor: Sensor, data: SensorGroupData, fields: tuple
) -> DataToSave:
    channel_A: dict[str, Any] = {
        "primary": {"created_at": data.parent.primary.created_at},
        "secondary": {"created_at": data.parent.secondary.created_at},
    }
    channel_B: dict[str, Any] = {
        "primary": {"created_at": data.child.primary.created_at},
        "secondary": {"created_at": data.child.secondary.created_at},
    }
    try:
        for sensor_field in fields:
            if isinstance(sensor_field, SensorSingleField):
                channel, lineage, field = sensor_field.value
                if channel == "parent":
                    channel_A[lineage][field] = getattr(
                        getattr(data, channel), lineage
                    )[field]
                elif channel == "child":
                    channel_B[lineage][field] = getattr(
                        getattr(data, channel), lineage
                    )[field]
            elif isinstance(sensor_field, SensorDualField):
                lineage, field = sensor_field.value
                for channel in ["parent", "child"]:
                    if channel == "parent":
                        channel_A[lineage][field] = getattr(
                            getattr(data, channel), lineage
                        )[field]
                    elif channel == "child":
                        channel_B[lineage][field] = getattr(
                            getattr(data, channel), lineage
                        )[field]
    except Exception as e:
        logging.error("Error in handle_historical_data()")
        raise e
    channel_A["primary"] = check_dataframe_types(
        sensor.identifier, DataFrame(channel_A["primary"])
    )
    channel_A["secondary"] = check_dataframe_types(
        sensor.identifier, DataFrame(channel_A["secondary"])
    )
    channel_B["primary"] = check_dataframe_types(
        sensor.identifier, DataFrame(channel_B["primary"])
    )
    channel_B["secondary"] = check_dataframe_types(
        sensor.identifier, DataFrame(channel_B["secondary"])
    )

    return DataToSave(
        id=sensor.identifier,
        latitude=sensor.parent.lat,
        longitude=sensor.parent.lon,
        location_type=sensor.location_type,
        name=sensor.parent.name,
        hardware=sensor.parent.hardware,
        model=sensor.parent.model,
        channel_A=ChannelData(channel_A["primary"], channel_A["secondary"]),
        channel_B=ChannelData(channel_B["primary"], channel_B["secondary"]),
    )
