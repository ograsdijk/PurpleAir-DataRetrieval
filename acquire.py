import datetime
import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import reverse_geocoder as rg
import yaml
from purpleair.network import Sensor, SensorList
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from enums import SensorDualField, SensorSingleField
from get_data import get_historical_data
from handle_data import DataToSave, handle_historical_data
from hdf_writer import HDFWriter

FORMAT = "%(message)s"
logging.basicConfig(
    level="WARNING", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)


def get_data(
    sensor_id: int,
    weeks_to_get: int,
    fields: tuple,
    start_date: Optional[datetime.datetime] = None,
) -> DataToSave:
    sensor = Sensor(sensor_id)
    data = get_historical_data(sensor, weeks_to_get, start_date=start_date)
    data = handle_historical_data(sensor, data, fields)
    if data.channel_A.primary.shape[0] == 0:
        return None
    return data


def acquire(config: Path, fields: tuple) -> None:
    # loading the configuration
    with open(config, "r") as f:
        config = yaml.safe_load(f)

        state = config["State"]
        workers = config["PurpleAir"]["workers"]
        weeks_to_get_bunches = config["PurpleAir"]["weeks_to_get_bunches"]
        fname = config["HDF"]["filename"]
        start_date = config["PurpleAir"]["start_date"]
        stop_date = config["PurpleAir"]["stop_date"]
        sensors_to_get = config["PurpleAir"].get("sensors_to_get")

    # grabbing all sensors
    p = SensorList()

    # filtering to only grab sensors which match the state given in config.yaml
    df = p.to_dataframe("useful", "parent")
    df = df[["lat", "lon"]]
    df = df.dropna()
    lats, lons = df.lat.values, df.lon.values
    locations = rg.search([(lat, lon) for lat, lon in zip(lats, lons)])
    indices = [idx for idx, loc in enumerate(locations) if loc["admin1"] == state]
    sensor_ids = [int(idx) for idx in df.index.values[indices]]

    if sensors_to_get is None:
        sensors_to_get = len(sensor_ids)

    # delete unused data to save memory
    del df, lats, lons, indices

    # start the HDF writer
    path = Path() / f"{fname}"
    writer = HDFWriter(path)
    writer.active.set()
    writer.start()

    # calculate how many segments of weeks_to_get_bunches are needed
    # larger weeks_to_get_bunches would take longer to retrieve, spreading it out to
    # prevent overloading the API
    dt = stop_date - start_date
    nr_segments = math.ceil(dt / (datetime.timedelta(days=7) * weeks_to_get_bunches))

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        "•",
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        TimeElapsedColumn(),
        "•",
        TimeRemainingColumn(),
    )

    sensors_to_get = (
        len(sensor_ids) if len(sensor_ids) < sensors_to_get else sensors_to_get
    )

    # iterate through the segments and sensors in a threadpool
    with progress:
        main_task = progress.add_task(
            f"[bold red]Retrieving PurpleAir data for {state}", total=nr_segments
        )
        for segment in range(nr_segments):
            date = (
                stop_date - datetime.timedelta(days=7) * weeks_to_get_bunches * segment
            )
            date_prev = date - datetime.timedelta(days=7) * weeks_to_get_bunches

            try:
                threadpool = ThreadPoolExecutor(max_workers=workers)
                task = progress.add_task(
                    f"[bold blue]Retrieving sensor data from "
                    f"{date_prev.strftime('%Y-%m-%d')} to {date.strftime('%Y-%m-%d')}",
                    total=sensors_to_get,
                )
                with threadpool as executor:
                    future_to_data = {
                        executor.submit(
                            get_data, sensor, weeks_to_get_bunches, fields, date
                        ): sensor
                        for sensor in sensor_ids[:sensors_to_get]
                    }
                    for future in as_completed(future_to_data):
                        sensor = future_to_data[future]
                        try:
                            result = future.result()
                            if result is not None:
                                writer.queue.put(result)
                        except Exception as exc:
                            print("%r generated an exception: %s" % (sensor, exc))
                        else:
                            progress.update(task, advance=1)
                            if progress.tasks[1].total == progress.tasks[1].completed:
                                progress.remove_task(task)
            except KeyboardInterrupt:
                logging.warning("KeyboardInterrupt, stopping data retrieval")
                writer.active.clear()
            progress.update(main_task, advance=1)
    writer.active.clear()


if __name__ == "__main__":
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
