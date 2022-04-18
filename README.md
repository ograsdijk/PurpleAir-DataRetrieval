# PurpleAir-DataRetrieval
 Get historical data for purple air sensors, using the [purpleair python package](https://github.com/ReagentX/purple_air_api).

API calls are split up between sensors and timespans, to prevent overloading the API.

## Dependencies
* [purpleapi](https://github.com/thampiman/reverse-geocoder)
* [rich](https://rich.readthedocs.io/en/stable/introduction.html#:~:text=Rich%20is%20a%20Python%20library,in%20a%20more%20readable%20way.)
* [pandas](https://pandas.pydata.org/)
* [numpy](https://numpy.org/)
* [pyyaml](https://pyyaml.org/)
* [h5py](https://www.h5py.org/)

## Usage
Specify the fields to load (see in `enums.py`). `SensorSingleField` is a sensor value that does not have redundancy, `SensorDualField` has redundant sensors to check the confidence of the measurement.
```Python
fields = (
    SensorSingleField.TEMPERATURE,
    SensorSingleField.HUMIDITY,
    SensorSingleField.PRESSURE,
    SensorDualField.PM1_0,
    SensorDualField.PM2_5,
    SensorDualField.PM10,
    SensorDualField.PM2_5_ATM,
)
```
The code requires a config file:
```Yaml
# which state to look in
State: "California"

PurpleAir:
  # number of threads simultanously calling the API
  workers: 10
  # nr of weeks to get at once from the API
  weeks_to_get_bunches: 5
  # date range to get data in
  start_date: 2021-01-01T00:00:00
  stop_date: 2022-01-01T00:00:00
  # how many sensors to grab, comment line to grab all sensors
  sensors_to_get: 100

HDF:
  # HDF filename where data is saved
  filename: "purple_air_data.h5"
```

The latitude and longitude data from the sensor is used with [reverse-geocoder](https://github.com/thampiman/reverse-geocoder) to select only sensors which reside in the specified `State`.

With `weeks_to_get_bunches`, `start_date` and `stop_date` data for bunches of weeks are retrieved for each sensor from the API. In principle you could retrieve all data over the required time period at once for each sensor, but for longer time periods this might take a long time and overload the API.

To run the data acquisition use ` acquire(config: Path, fields: tuple) -> None` in `acquire.py`:
```Python
acquire(config_path, fields)
```

All steps together are shown in `acquire.py`, specifically:
```Python
if __name__ == "__main__"
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
```

## Data Format
Data is stored in an `HDF` container with `pandas`. The data structure is as follows:
```
HDF container
    sensor_{id}
        channel_A
            primary
            secondary
        channel_B
            primary
            secondary
```
Each channel has primary and secondary sensors, and all have different acquisition timestamps, so they cannot be saved in one large array.
The PurpleAir sensors have several redundant sensors, such as the particulate sensors, in order to compare both and give some degree of confidence on the measurements. These are saved both in `channel_A` and `channel_B`. `primary` and `secondary` are the designations used by `PurpleAir` for different sensor types. 

Attributes (`id`, `latitude`, `location_type` `longitude`, `name`, `hardware`, `model`) are saved as attributes of the `sensor_{id}` group.

A convencience HDF reader function is located in `hdf_reader.py`:
```Python
read_sensor_hdf(sensor_id: int, path: Path) -> DataToSave:
```