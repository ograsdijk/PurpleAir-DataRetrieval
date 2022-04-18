from pathlib import Path

import h5py  # type: ignore
import pandas as pd

from handle_data import ChannelData, DataToSave


def read_sensor_hdf(sensor_id: int, path: Path) -> DataToSave:
    dataclass_fields = {}

    with h5py.File(path, "r") as f:
        group = f[f"sensor_{sensor_id}"]
        for attr in DataToSave.__dataclass_fields__.keys():
            if attr in ["channel_A", "channel_B"]:
                continue
            else:
                dataclass_fields[attr] = group.attrs[attr]
    with pd.HDFStore(path, mode="r") as store:
        for channel in ["channel_A", "channel_B"]:
            _ = {}
            for lineage in ["primary", "secondary"]:
                _[lineage] = pd.read_hdf(
                    store, f"sensor_{sensor_id}/{channel}/{lineage}"
                )
            dataclass_fields[channel] = ChannelData(**_)
    data = DataToSave(**dataclass_fields)
    return data
