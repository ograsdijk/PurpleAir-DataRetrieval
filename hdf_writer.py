import logging
import time
from pathlib import Path
from queue import Queue
from threading import Event, Thread

import h5py  # type: ignore

from handle_data import DataToSave
import pandas as pd


def write_to_hdf(path: Path, data: DataToSave) -> None:
    id = data.id

    with pd.HDFStore(path) as store:

        d = data.channel_A.primary
        key = f"sensor_{id}/channel_A/primary"
        d.to_hdf(store, key, mode="a", append=True, format="table")
        # store.append(key, d, data_columns=True)

        d = data.channel_A.secondary
        key = f"sensor_{id}/channel_A/secondary"
        d.to_hdf(store, key, mode="a", append=True, format="table")
        # store.append(key, d, data_columns=True)

        d = data.channel_B.primary
        key = f"sensor_{id}/channel_B/primary"
        d.to_hdf(store, key, mode="a", append=True, format="table")
        # store.append(key, d, data_columns=True)

        d = data.channel_B.secondary
        key = f"sensor_{id}/channel_B/secondary"
        d.to_hdf(store, key, mode="a", append=True, format="table")
        # store.append(key, d, data_columns=True, )

    with h5py.File(path, "a") as f:
        group = f[f"sensor_{id}"]
        for field in data.__dataclass_fields__.keys():
            if field in ["channel_A", "channel_B"]:
                continue
            try:
                attribute = getattr(data, field)
                if field == "hardware":
                    attribute = str(attribute)
                group.attrs[field] = attribute
            except Exception:
                logging.error(
                    f"Cannot add field {field}, type is {type(getattr(data, field))}"
                )


class HDFWriter(Thread):
    def __init__(self, path: Path):
        super(HDFWriter, self).__init__()
        self.path = path

        self.queue: Queue[DataToSave] = Queue()
        self.active = Event()

    def run(self):
        while self.active.is_set():
            while not self.queue.empty():
                data = self.queue.get()
                write_to_hdf(self.path, data)
            time.sleep(1e-3)
