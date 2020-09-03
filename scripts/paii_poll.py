#!/usr/bin/env python3

import asyncio
import configparser
import json
import logging
import site
import socket
import sys
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from enum import Enum
from functools import partial
from getpass import getpass, getuser
from pathlib import Path
from signal import signal
from logging.handlers import SysLogHandler
from time import time
from typing import Any, AsyncGenerator, Dict, NamedTuple, Optional, Sequence

import aiohttp
import asyncpg as apg
import keyring
from aioscheduler import TimedScheduler

from paii.purple_data import (
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
"""

logging.basicConfig(format="%(asctime)-15s %(name)s %(levelname)s %(message)s")
LOG = logging.getLogger("paii_poll")
LOG.setLevel(logging.INFO)

MIN_WINDOW = 1  # will skip first cycle if Tn - MIN_WINDOW > t > Tn

# 30 sec averaging with live=true param, 120 sec without.
LOOP_INTERVAL = 30

CONFIG_FILENAME = "purple_air.ini"

MessageType = Enum("MessageType", "DATA MSG SIGNAL")


class Msg(NamedTuple):
    """Sttructure returned on result queue

    The poller pushes a DATA message type along with the decoded dictionary.
    The MSG and SIGNAL types are for control. Some attempt is made to clean up
    the app, eg cancelling and outstanding tasks. This does make the main loop a 
    little more convoluted.
    """

    msg_type: MessageType
    payload: Dict[str, Any]


async def get_connection(
    user: Optional[str] = None,
    database: str = DB,
    host: Optional[str] = None,
    password: Optional[str] = None,
) -> apg.connection.Connection:
    user = getuser() if user is None else user
    LOG.info(f"user: {user} db: {database} host: {host}")
    return await apg.connect(user=user, database=database, host=host, password=password)


def get_db_args(defaults: Optional[Dict[str, str]] = None) -> ArgumentParser:
    user = getuser()
    ap = ArgumentParser(add_help=False)
    ap.add_argument("--db", default=DB, help="Name of database.")
    ap.add_argument("--host", help="Postgres remote host.")
    ap.add_argument("--user", default=user, help="Override unix username.")
    ap.add_argument(
        "--password",
        help="Supply database password on command line. This is not recommended.",
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
        l for l in (site.USER_BASE, sys.prefix, "/etc") if l is not None
    ]
    for l in locations:
        config_path = Path(l) / CONFIG_FILENAME
        if config_path.exists():
            return config_path
    raise FileNotFoundError(f"{config_filename} not found in {', '.join(locations)}")


def get_args() -> Namespace:
    config = configparser.ConfigParser()
    try:
        config_path = find_config_file(Path(CONFIG_FILENAME))
        with open(config_path, "r") as cf:
            config.read_file(cf)
        LOG.info(f"using config from: '{config_path}'")
    except FileNotFoundError as e:
        LOG.info(f"{str(e)},\nusing defaults, see --help for more info.")
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
        "--loop-interval", default=LOOP_INTERVAL, help="Device read interval"
    )
    ap.add_argument(
        "--min-window",
        default=MIN_WINDOW,
        help="Guard time before next event. "
        "If for some reason your loop processing is very slow "
        "you might want to increase this (and/or increase --loop-interval).",
    )
    ap.add_argument(
        "--syslog-host", help="Set this string to a syslog host to turn on syslog."
    )
    ap.add_argument(
        "--syslog-port",
        default="514/udp",
        help="Set this string to a syslog host to turn on syslog (%(default)s). "
        "Can also specify protocol, eg 514/tcp",
    )
    opt = ap.parse_args()
    # LOG.debug(opt)  # don't want password in log file
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
        except:
            password = None
        if not password:
            password = getpass("enter password: ")
    return password


def loop_time(
    loop_interval: int, min_window: float, t_now: Optional[float] = None
) -> float:
    """calculate next absolute time for a loop timer.

    The time is in the future wrt to t_now and is quantized to loop_interval seconds.

    Args:
        loop_interval (int): seconds between timer events.
        min_window (float): seconds. If t_now > next_t - min_windows, add an additional
            loop_interval.
        t_now (float, optional): If you want to start at some time in the future (or past) then
            override this. Defaults to time().

    Returns:
        float: time for next timer event
    """
    if t_now is None:
        t_now = time()
    t0: float = (t_now // loop_interval) * loop_interval
    if t_now > t0 - min_window:
        t0 += loop_interval
    return t0


def sig_handler(result_queue, signum, frame) -> None:
    """Async signal handling
    
    Send a signal to the process with eg: `# pkill -f '.*python.*purple_air'`
    """
    LOG.info("interrupt received, draining queue.")
    result_queue.put_nowait(Msg(MessageType.SIGNAL, {"signal": signum}))


async def device_reader(
    session: aiohttp.ClientSession,
    url: str,
    parms: Optional[Dict] = None,
    result_queue: Optional[asyncio.Queue] = None,
    input_queue: Optional[asyncio.Queue] = None,
) -> Dict[str, Any]:
    """poll paii and push decoded json onto result queue
    
    The values are updated a dictionary read off the input_queue (if present).
    This is currently used for the nominal poll time. Calling time.time() would
    of course be possible but the wallclock time will have jitter and a clean
    timeseries is wanted.

    aioscheduler does not have a mechanism for passing variables to the scheduled
    task (unlike threading.Timer) so any inputs from the scheduling code should push
    a dictionary on here.

    Args:
        session (aiohttp.ClientSession): requests are made against this session.
        url (str): URL to paii sensor.
        parms (Optional[Dict], optional): [description]. Defaults to None.
        result_queue (Optional[asyncio.Queue], optional): [description]. Defaults to None.
        input_queue (Optional[asyncio.Queue], optional): [description]. Defaults to None.

    Returns:
        Dict[str, Any]: [description]
    """

    dargs = {}
    if input_queue:
        dargs = input_queue.get_nowait()
    async with session.get(url) as response:
        jresp = await response.json()
        LOG.debug(f"device response length: {len(jresp)}")
        jresp.update(dargs)
        if result_queue:
            result_queue.put_nowait(Msg(MessageType.DATA, jresp))
            LOG.debug("device response queued")
        return jresp


async def main() -> None:
    opt = get_args()
    numeric_level = getattr(logging, opt.log_level.upper(), None)
    if isinstance(numeric_level, int):
        LOG.setLevel(numeric_level)
    log_handler_args = get_syslog_handler_args(opt.syslog_host, opt.syslog_port)
    try:
        if log_handler_args:
            log_handler = SysLogHandler(**log_handler_args)
            log_fmt = logging.Formatter(
                "%(asctime)-15s %(name)s %(levelname)s %(message)s"
            )
            log_handler.setFormatter(log_fmt)
            LOG.addHandler(log_handler)
            LOG.info(f"added syslog handler: '{log_handler_args}'")
    except ConnectionRefusedError:
        LOG.error(
            f"syslog couldn't open '{log_handler_args}'. Logs will go to console."
        )
    LOG.info("starting paii_poll.py")
    rest_parms = {"live": "true"}
    # scheduler uses datetime, compares with utc, naive times.
    scheduler = TimedScheduler()
    scheduler.start()
    input_queue: asyncio.Queue = asyncio.Queue()
    result_queue: asyncio.Queue = asyncio.Queue()
    password = get_password(user=opt.user, password=opt.password)
    t_next = loop_time(loop_interval=int(opt.loop_interval), min_window=opt.min_window)
    # naive, zero UTC offset datetime value required by aioscheduler
    dt_next: datetime = datetime.fromtimestamp(t_next, tz=timezone.utc).replace(
        tzinfo=None
    )
    async with aiohttp.ClientSession() as session:
        worker = partial(
            device_reader, session, opt.url, rest_parms, result_queue, input_queue
        )
        # push non-naive datetime for ultimate storage in DB
        input_queue.put_nowait({opt.time_field: dt_next.replace(tzinfo=timezone.utc)})
        task_next = scheduler.schedule(worker(), dt_next)
        # now we have scheduled our first job, do database once off prep here asynchronously
        conn: apg.connection.Connection = await get_connection(
            user=opt.user, database=DB, host=opt.host, password=password
        )
        create_sql = compose_create(
            table_name=opt.paii_table, time_field=opt.time_field
        )
        LOG.debug(f'create sql: "{create_sql}"')
        LOG.info(f"creating table: '{TABLE_RAW}'")
        await conn.execute(create_sql)
        if not opt.no_timescaledb:
            LOG.info(
                "calling create_hypertable() on table. "
                "This requires at least the community edition of timescaledb. "
                "See https://www.timescale.com/ for details. This code has been tested "
                "with timescaledb and *may* work with native postgresql."
            )
            await conn.execute(
                f"SELECT create_hypertable('{opt.paii_table}', '{opt.time_field}', if_not_exists => TRUE);"
            )

        insert_sql = compose_insert(
            field_names=[f.db_name for f in gen_stored()], table_name=opt.paii_table
        )
        LOG.debug(f'insert sql: "{insert_sql}"')
        drain = False
        while True:
            LOG.info(f"next poll event scheduled at utc: {dt_next}")
            msg = await result_queue.get()
            LOG.debug(f"msg received from queue '{msg}'")
            if msg.msg_type == MessageType.SIGNAL:
                LOG.info(
                    f"received signal: {msg.payload['signal']} exiting, be patient."
                )
                drain = True
                scheduler.cancel(task_next)
                continue
            else:
                paii_data = convert_data(msg.payload)
                LOG.debug(paii_data)
                paii_values = tuple(paii_data.values())
                try:
                    await conn.execute(insert_sql, *paii_values)
                except apg.exceptions.DataError:
                    LOG.exception(
                        f'paii_data: "{paii_data}"\npaii_values: "{paii_values}"\ninsert_sql: "{insert_sql}"'
                    )
                    raise
            if drain:
                break
            # Don't assume there are never any delays - calculate the next increment
            # fully, not simply add LOOP_INTERVAL
            t_next = loop_time(
                loop_interval=int(opt.loop_interval), min_window=opt.min_window
            )
            dt_next = datetime.fromtimestamp(t_next, tz=timezone.utc).replace(
                tzinfo=None
            )
            LOG.debug(f"scheduling event for: {dt_next}")
            input_queue.put_nowait(
                {opt.time_field: dt_next.replace(tzinfo=timezone.utc)}
            )
            task_next = scheduler.schedule(worker(), dt_next)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        LOG.warning("purple_device was interrupted, exiting")
