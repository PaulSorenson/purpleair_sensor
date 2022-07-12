#!/usr/bin/env python3

import asyncio
import configparser
import json
import logging
import site
import sys
from argparse import ArgumentParser, Namespace
from datetime import datetime
from functools import partial
from getpass import getpass, getuser
from logging.handlers import SysLogHandler
from pathlib import Path
from typing import Dict, Optional, Sequence

import aiohttp
import asyncio_mqtt as mq
import asyncpg as apg
import keyring
from aioconveyor.aioconveyor2 import AioGenConveyor, Event
from .purple_data import (
    DB,
    TABLE_RAW,
    TIME_FIELD,
    compose_create,
    compose_insert,
    convert_data,
    gen_stored,
)
from utils.syslog_handler import get_syslog_handler_args

"""Poll Purple Air PAII sensor and write records to data base

Note this is not for polling the cloud data - it requires a URL that is directly
to a paii device on your network.

This is an asyncio implementation which could potentiall be a bit simpler
using threading.Timer but since I was already using `asyncpg` ascync was a
natural fit.

The code has been developed in a postgresql/timescaledb environment. You may
need to review:
- the CREATE syntax
- differences in DATETIME/TIMESTAMP types for other DBs.


# MQTT HA configuration
- [see MQTT Discovery](https://www.home-assistant.io/docs/mqtt/discovery/)
"""

logging.basicConfig(format="%(asctime)-15s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("paii_poll")
log.setLevel(logging.INFO)

MIN_WINDOW = 1  # will skip first cycle if Tn - MIN_WINDOW > t > Tn

# 30 sec averaging with live=true param, 120 sec without.
LOOP_INTERVAL = 30

CONFIG_FILENAME = "purple_air.ini"

CELSIUS_ADJ = -4
PRESSURE_ADJ = 5


async def get_connection(
    user: Optional[str] = None,
    database: str = DB,
    host: Optional[str] = None,
    password: Optional[str] = None,
) -> apg.connection.Connection:
    user = getuser() if user is None else user
    log.info(f"user: {user} db: {database} host: {host}")
    return await apg.connect(user=user, database=database, host=host, password=password)


def get_db_args(defaults: Optional[Dict[str, str]] = None) -> ArgumentParser:
    user = getuser()
    ap = ArgumentParser(add_help=False)
    ap.add_argument(
        "--host",
        default=None,
        help="Postgres remote host. "
        "Provide an empty string to override any config file setting if you want to test "
        "without writing to a database. In this case the data will be printed to stdout",
    )
    ap.add_argument("--user", default=user, help="Override unix username.")
    ap.add_argument(
        "--password",
        help="Supply database password on command line. This is not recommended.",
    )
    ap.add_argument(
        "--database",
        default=DB,
        help="Override default postgres database name, normally only for test purposes.",
    )
    ap.add_argument(
        "--paii-table",
        default=TABLE_RAW,
        help="Override table name, normally only for test purposes.",
    )
    ap.add_argument(
        "--time-field",
        default=TIME_FIELD,
        help="Override time field name, normally only for test purposes.",
    )
    ap.add_argument(
        "--no-timescaledb",
        action="store_true",
        help="don't call create_hypertable() when the table is created."
        "See https://www.timescale.com/ for how this can help timeseries apps scale.",
    )
    if defaults is not None:
        ap.set_defaults(**defaults)
    return ap


def get_common_args(defaults: Optional[Dict[str, str]] = None) -> ArgumentParser:
    ap = ArgumentParser(add_help=False)
    ap.add_argument("--log-level", default="INFO", help="Override log level")
    if defaults is not None:
        ap.set_defaults(**defaults)
    return ap


def find_config_file(config_filename: Path) -> Path:
    locations: Sequence[str] = [
        p for p in (site.USER_BASE, sys.prefix, "/etc") if p is not None
    ]
    for loc in locations:
        config_path = Path(loc) / CONFIG_FILENAME
        if config_path.exists():
            return config_path
    raise FileNotFoundError(f"{config_filename} not found in {', '.join(locations)}")


def get_args() -> Namespace:
    config = configparser.ConfigParser()
    try:
        config_path = find_config_file(Path(CONFIG_FILENAME))
        with open(config_path, "r") as cf:
            config.read_file(cf)
        log.info(f"using config from: '{config_path}'")
    except FileNotFoundError as e:
        log.info(f"{str(e)},\nusing defaults, see --help for more info.")
        config = {"db": {}, "common": {}}

    ap = ArgumentParser(
        parents=[
            get_db_args(defaults=dict(config["db"])),
            get_common_args(defaults=dict(config["common"])),
        ],
        epilog=(
            f"If '{CONFIG_FILENAME}' is present, it will be used for defaults. "
            f"See https://github.com/PaulSorenson/purpleair_sensor/tree/master/scripts/ "
            f"for a template."
        ),
    )
    ap.add_argument(
        "--url", default="http://purpleair-c8ee.home.metrak.com/json", help="PAII url."
    )
    ap.add_argument(
        "--loop-interval",
        default=LOOP_INTERVAL,
        type=int,
        help="Device read interval. " "This is rounded to the nearest second.",
    )
    ap.add_argument("--mq-host", help="turns on mqtt writer.")
    ap.add_argument(
        "--mq-topic",
        default="purple_air/outside",
        help="MQTT topic, also needs --mq-host.",
    )
    ap.add_argument(
        "--mq-downsample",
        default=1,
        type=int,
        help="Downsample MQTT pub to 1 per downsample reads. "
        "One (default) means no downsampling",
    )
    ap.add_argument(
        "--syslog-host",
        help="Set this string to a syslog host to turn on syslog. "
        "Set it to the empty string to disable any setting in the config file.",
    )
    ap.add_argument(
        "--syslog-port",
        default="514/udp",
        help="Set this string to a syslog host to turn on syslog (%(default)s). "
        "Can also specify protocol, eg 514/tcp",
    )
    opt = ap.parse_args()
    # log.debug(opt)  # don't want password in log file
    return opt


def get_password(user: str, password: Optional[str], account: str = "postgres") -> str:
    """try to pull password from keyring or prompt

    The python-keyring package supplies this and is dependent on a compatible
    backend. For desktop use gnome has it covered.

    Args:
        user (str): name of account user
        password (Optional[str]): optional, although if you provide one it
            gets returned as is.
        account: this is an arbitrary string used in context like
            `keyring get <user> <account>`

    Returns:
        str: password for account
    """
    if not password:
        try:
            password = keyring.get_password(user, account)
        except Exception:
            password = None
        if not password:
            password = getpass("enter password: ")
    return password


async def produce(
    event: Event,
    url: str,
    query_parms: Dict,
    time_field: str = TIME_FIELD,
    http_timeout: int = 30,
) -> Dict:
    log.info("producer: started")
    timeout = aiohttp.ClientTimeout(total=http_timeout)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        updates: Dict[str, datetime] = {time_field: event.event_time}
        try:
            async with session.get(
                url,
                timeout=timeout,
                params=query_parms,
            ) as response:
                jresp = await response.json()
                log.debug(f"PAII response: `{jresp}``")
                jresp.update(updates)
                return jresp
        except Exception:
            log.exception("produce: exception - exiting")


async def consume_postgres(
    event: Event,
    payload: Dict,
    user: str,
    password: str,
    host: str,
    insert_sql: str,
    database: str = DB,
) -> int:
    conn: apg.connection.Connection = await get_connection(
        user=user, database=database, host=host, password=password
    )
    paii_data = convert_data(payload)
    log.debug(f"data for '{host}:{database}': '{paii_data}'")
    paii_values = tuple(paii_data.values())
    try:
        await conn.execute(insert_sql, *paii_values)
    except apg.exceptions.DataError:
        log.exception(
            f'paii_data: "{paii_data}"\npaii_values: "{paii_values}"'
            '\ninsert_sql: "{insert_sql}"'
        )
        raise
    return 0


class MqttConsumer:
    def __init__(self, host: str, topic: str, downsample: int = 0) -> None:
        self.host = host
        self.topic = topic
        self.downsample = downsample

    async def start_client(self) -> mq.Client:
        self.client = mq.Client(self.host)
        await self.client.connect()
        return self.client

    async def consume(self, event: Event, payload: Dict):
        if event.loop_counter < 1:
            await self.start_client()

        # this will bail if user sets downsample to zero ;)
        mod = event.loop_counter % self.downsample
        if mod:
            return mod

        keys = [
            "pm2_5_cf_1",
            "pm2_5_cf_1_b",
            "current_temp_f",
            "current_humidity",
            "pressure",
        ]
        payload = {k: payload[k] for k in keys}
        # make adjustments
        # temp inside paii case is higher than ampient
        payload["current_temp_c_adj"] = (
            payload["current_temp_f"] - 32
        ) * 5 / 9 + CELSIUS_ADJ
        payload["pressure_adj"] = payload["pressure"] + PRESSURE_ADJ

        msg = json.dumps(payload)
        try:
            await self.client.publish(self.topic, msg, qos=1)
        except Exception:
            log.exception("mqtt publish failed")
            raise
        return 0


async def consume_terminal(event: Event, payload: Dict):
    print(event, payload)
    return 0


async def amain() -> None:
    opt = get_args()
    log.debug(f"{opt}")
    log.setLevel(opt.log_level)
    if opt.syslog_host:
        log_handler_args = get_syslog_handler_args(opt.syslog_host, opt.syslog_port)
        try:
            if log_handler_args:
                log_handler = SysLogHandler(**log_handler_args)
                log_fmt = logging.Formatter(
                    "%(asctime)-15s %(name)s %(levelname)s %(message)s"
                )
                log_handler.setFormatter(log_fmt)
                log.addHandler(log_handler)
                log.info(f"added syslog handler: '{log_handler_args}'")
        except ConnectionRefusedError:
            log.error(
                f"syslog couldn't open '{log_handler_args}'. Logs will go to console."
            )
    log.info("starting paii_poll.py")
    password = get_password(user=opt.user, password=opt.password)
    consumers = []
    if opt.host:
        conn: apg.connection.Connection = await get_connection(
            user=opt.user, database=opt.database, host=opt.host, password=password
        )
        create_sql = compose_create(table_name=opt.paii_table, time_field=opt.time_field)
        log.debug(f'create sql: "{create_sql}"')
        log.info(f"creating table: '{TABLE_RAW}'")
        await conn.execute(create_sql)
        if not opt.no_timescaledb:
            log.info(
                "calling create_hypertable() on table. "
                "This requires at least the community edition of timescaledb. "
                "See https://www.timescale.com/ for details. This code has been tested "
                "with timescaledb and *may* work with native postgresql."
            )
            await conn.execute(
                f"SELECT create_hypertable('{opt.paii_table}',"
                " '{opt.time_field}', if_not_exists => TRUE);"
            )

        insert_sql = compose_insert(
            field_names=[f.db_name for f in gen_stored()], table_name=opt.paii_table
        )
        log.debug(f'insert sql: "{insert_sql}"')

        _consume_postgres = partial(
            consume_postgres,
            user=opt.user,
            password=password,
            host=opt.host,
            database=opt.database,
            insert_sql=insert_sql,
        )
        consumers.append(_consume_postgres)
    else:
        log.warning("no host not provided, data will not be written to DB")
        conn, insert_sql = None, None
        consumers.append(consume_terminal)

    if opt.mq_host:
        mq_writer = MqttConsumer(
            host=opt.mq_host, topic=opt.mq_topic, downsample=opt.mq_downsample
        )
        consumers.append(mq_writer.consume)

    query_parms = {"live": "true"}
    _produce = partial(
        produce,
        url=opt.url,
        query_parms=query_parms,
        time_field=TIME_FIELD,
        http_timeout=opt.loop_interval,
    )

    conv = AioGenConveyor(
        produce=_produce,
        consumers=consumers,
        loop_interval=opt.loop_interval,
        loop_offset=5,
    )
    conv.start()
    log.info("main: thread started")
    while conv.running:
        log.debug("main loop")
        await asyncio.sleep(2)
    log.info("main: conveyor thread no longer running, terminating")


def main():
    try:
        # asyncio.run(main(), debug=True)
        asyncio.run(amain(), debug=True)
    except KeyboardInterrupt:
        log.error("aioproc exiting")


if __name__ == "__main__":
    main()
