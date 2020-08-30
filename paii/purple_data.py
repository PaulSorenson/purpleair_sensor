#!/usr/bin/env python3


from collections import OrderedDict
from typing import Any, Callable, Dict, Iterable, List, NamedTuple, Optional, Sequence

DB = "timeseries"
TABLE_RAW = "paii_raw"
TIME_FIELD = "paii_time"


class Field(NamedTuple):
    json_key: str
    store_flag: bool
    db_name: str
    data_type: str
    convert: Optional[Callable[[Any], Any]] = None


def fahrenheit2celsius(f: float) -> float:
    return round((f - 32) * 5 / 9, 2)


fields = [
    # inserted by the sceduler
    Field(TIME_FIELD, True, TIME_FIELD, "TIMESTAMPTZ NOT NULL"),
    # from the device
    Field("SensorId", False, "SensorId", "VARCHAR"),  # eg "84:f3:eb:7b:c8:ee"
    Field("DateTime", False, "DateTime", "VARCHAR"),  # eg "2020/08/23T10:44:39z"
    Field("Geo", True, "geo", "VARCHAR"),  # eg "PurpleAir-c8ee"
    Field("Mem", False, "Mem", "VARCHAR"),  # eg 18936
    Field("memfrag", False, "memfrag", "VARCHAR"),  # eg 8
    Field("memfb", False, "memfb", "VARCHAR"),  # eg 17568
    Field("memcs", False, "memcs", "VARCHAR"),  # eg 896
    Field("Id", False, "Id", "VARCHAR"),  # eg 4177
    Field("lat", True, "lat", "DOUBLE PRECISION"),  # eg -37.8484
    Field("lon", True, "lon", "DOUBLE PRECISION"),  # eg 145.177399
    Field("Adc", True, "adc", "DOUBLE PRECISION"),  # eg 0.05
    Field("loggingrate", False, "loggingrate", "VARCHAR"),  # eg 15
    Field("place", True, "place", "VARCHAR"),  # eg "outside"
    Field("version", False, "version", "VARCHAR"),  # eg "6.01"
    Field("uptime", False, "uptime", "VARCHAR"),  # eg 242296
    Field("rssi", False, "rssi", "VARCHAR"),  # eg -59
    Field("period", True, "period", "INTEGER"),  # eg 120
    Field("httpsuccess", False, "httpsuccess", "VARCHAR"),  # eg 12169
    Field("httpsends", False, "httpsends", "VARCHAR"),  # eg 12182
    Field("hardwareversion", True, "hardwareversion", "VARCHAR"),  # eg "2.0"
    Field(
        "hardwarediscovered", False, "hardwarediscovered", "VARCHAR"
    ),  # eg "2.0+BME280+PMSX003-B+PMSX003-A"
    Field(
        "current_temp_f", True, "current_temp_c", "DOUBLE PRECISION", fahrenheit2celsius
    ),  # eg 52
    Field("current_humidity", True, "current_humidity", "DOUBLE PRECISION"),  # eg 55
    Field(
        "current_dewpoint_f",
        True,
        "current_dewpoint_c",
        "DOUBLE PRECISION",
        fahrenheit2celsius,
    ),  # eg 36
    Field("pressure", True, "pressure", "DOUBLE PRECISION"),  # eg 1005.28
    Field("p25aqic_b", False, "p25aqic_b", "VARCHAR"),  # eg "rgb(0,228,0)"
    Field("pm2.5_aqi_b", False, "pm2.5_aqi_b", "VARCHAR"),  # eg_aqi_b": 5
    Field("pm1_0_cf_1_b", True, "pm1_0_cf_1_b", "DOUBLE PRECISION"),  # eg 0.39
    Field("p_0_3_um_b", False, "p_0_3_um_b", "VARCHAR"),  # eg 261.79
    Field(
        "pm2_5_cf_1_b", True, "pm2_5_cf_1_b", "DOUBLE PRECISION"
    ),  # eg 1.3 **** µg/m3
    Field("p_0_5_um_b", False, "p_0_5_um_b", "VARCHAR"),  # eg 72.35
    Field("pm10_0_cf_1_b", True, "pm10_0_cf_1_b", "DOUBLE PRECISION"),  # eg 1.72
    Field("p_1_0_um_b", False, "p_1_0_um_b", "VARCHAR"),  # eg 13.05
    Field("pm1_0_atm_b", False, "pm1_0_atm_b", "VARCHAR"),  # eg 0.39
    Field("p_2_5_um_b", False, "p_2_5_um_b", "VARCHAR"),  # eg 2.42
    Field("pm2_5_atm_b", False, "pm2_5_atm_b", "VARCHAR"),  # eg 1.3
    Field("p_5_0_um_b", False, "p_5_0_um_b", "VARCHAR"),  # eg 0.7
    Field("pm10_0_atm_b", False, "pm10_0_atm_b", "VARCHAR"),  # eg 1.72
    Field("p_10_0_um_b", False, "p_10_0_um_b", "VARCHAR"),  # eg 0.0
    Field("p25aqic", False, "p25aqic", "VARCHAR"),  # eg "rgb(0,228,0)"
    Field("pm2.5_aqi", False, "pm2.5_aqi", "VARCHAR"),  # eg_aqi": 1
    Field("pm1_0_cf_1", True, "pm1_0_cf_1", "DOUBLE PRECISION"),  # eg 0.14
    Field("p_0_3_um", False, "p_0_3_um", "VARCHAR"),  # eg 163.63
    Field("pm2_5_cf_1", True, "pm2_5_cf_1", "DOUBLE PRECISION"),  # eg 0.33  **** µg/m3
    Field("p_0_5_um", False, "p_0_5_um", "VARCHAR"),  # eg 45.77
    Field("pm10_0_cf_1", True, "pm10_0_cf_1", "DOUBLE PRECISION"),  # eg 0.42
    Field("p_1_0_um", False, "p_1_0_um", "VARCHAR"),  # eg 7.79
    Field("pm1_0_atm", False, "pm1_0_atm", "VARCHAR"),  # eg 0.14
    Field("p_2_5_um", False, "p_2_5_um", "VARCHAR"),  # eg 0.56
    Field("pm2_5_atm", False, "pm2_5_atm", "VARCHAR"),  # eg 0.33
    Field("p_5_0_um", False, "p_5_0_um", "VARCHAR"),  # eg 0.18
    Field("pm10_0_atm", False, "pm10_0_atm", "VARCHAR"),  # eg 0.42
    Field("p_10_0_um", False, "p_10_0_um", "VARCHAR"),  # eg 0.0
    Field("pa_latency", False, "pa_latency", "VARCHAR"),  # eg 631
    Field("response", False, "response", "VARCHAR"),  # eg 201
    Field("response_date", True, "response_date", "INTEGER"),  # eg 1598179477
    Field("latency", True, "latency", "INTEGER"),  # eg 1459
    Field("key1_response", False, "key1_response", "VARCHAR"),  # eg 200
    Field(
        "key1_response_date", False, "key1_response_date", "VARCHAR"
    ),  # eg 1598179467
    Field("key1_count", False, "key1_count", "VARCHAR"),  # eg 79205
    Field("ts_latency", False, "ts_latency", "VARCHAR"),  # eg 1198
    Field("key2_response", False, "key2_response", "VARCHAR"),  # eg 200
    Field(
        "key2_response_date", False, "key2_response_date", "VARCHAR"
    ),  # eg 1598179470
    Field("key2_count", False, "key2_count", "VARCHAR"),  # eg 79212
    Field("ts_s_latency", False, "ts_s_latency", "VARCHAR"),  # eg 1141
    Field("key1_response_b", False, "key1_response_b", "VARCHAR"),  # eg 200
    Field(
        "key1_response_date_b", False, "key1_response_date_b", "VARCHAR"
    ),  # eg 1598179472
    Field("key1_count_b", False, "key1_count_b", "VARCHAR"),  # eg 79213
    Field("ts_latency_b", False, "ts_latency_b", "VARCHAR"),  # eg 1133
    Field("key2_response_b", False, "key2_response_b", "VARCHAR"),  # eg 200
    Field(
        "key2_response_date_b", False, "key2_response_date_b", "VARCHAR"
    ),  # eg 1598179474
    Field("key2_count_b", False, "key2_count_b", "VARCHAR"),  # eg 79217
    Field("ts_s_latency_b", False, "ts_s_latency_b", "VARCHAR"),  # eg 1136
    Field("wlstate", False, "wlstate", "VARCHAR"),  # eg "Connected"
    Field("status_0", True, "status_0", "INTEGER"),  # eg 2
    Field("status_1", True, "status_1", "INTEGER"),  # eg 2
    Field("status_2", True, "status_2", "INTEGER"),  # eg 2
    Field("status_3", True, "status_3", "INTEGER"),  # eg 2
    Field("status_4", True, "status_4", "INTEGER"),  # eg 2
    Field("status_5", True, "status_5", "INTEGER"),  # eg 2
    Field("status_6", True, "status_6", "INTEGER"),  # eg 2
    Field("status_7", True, "status_7", "INTEGER"),  # eg 0
    Field("status_8", True, "status_8", "INTEGER"),  # eg 2
    Field("status_9", True, "status_9", "INTEGER"),  # eg 2
    Field("ssid", False, "ssid", "VARCHAR"),  # eg "apocalypse
]


def gen_stored(fields: List[Field] = fields):
    yield from (f for f in fields if f.store_flag)


def compose_create(
    table_name: str, time_field: str, fields: List[Field] = fields
) -> str:
    fdesc = ",\n".join([f"{f.db_name} {f.data_type}" for f in gen_stored()])
    sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{fdesc},
PRIMARY KEY({time_field})
);
    """
    return sql


def compose_insert(field_names: Sequence, table_name: str) -> str:
    """compose parameterized insert SQL

    Args:
        field_names (Sequence): database table field names
        table_name (str): database table name.

    Returns:
        str: insert SQL.
    """
    fields = ", ".join(field_names)
    placeholders = ", ".join([f"${i+1}" for i in range(len(field_names))])
    sql = f"INSERT INTO {table_name} ({fields}) values ({placeholders})"
    return sql


def convert_data(data: Dict[str, Any], fields: List[Field] = fields) -> OrderedDict:
    """return filtered and ordered device data

    Args:
        data (Dict[str, Any]): raw dictionary directly from device.
        fields (List[Field], optional): fields specification

    Returns:
        OrderedDict[str, Any]: {db_key: converted_value, ...} items() will return
        in the same order as SQL commands assuming they are all based on the same
        field list.
    """
    return OrderedDict(
        {
            f.db_name: (f.convert(data[f.json_key]) if f.convert else data[f.json_key])
            for f in gen_stored()
        }
    )
