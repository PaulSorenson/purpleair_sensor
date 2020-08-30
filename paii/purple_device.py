#!/usr/bin/env python3

import asyncio
import json
import logging
import signal
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from enum import Enum
from functools import partial
from getpass import getpass, getuser
from time import time
from typing import Any, AsyncGenerator, Dict, NamedTuple, Optional

import aiohttp
import asyncpg as apg
import keyring
from aioscheduler import TimedScheduler
from purple_data import (
    compose_create,
    compose_insert,
    convert_data,
    DB,
    gen_stored,
    TABLE_RAW,
    TIME_FIELD,
)

"""Purple Air PAII sensor reader

Directly poll the device (cf pulling data from the API)
"""

logging.basicConfig(format="%(asctime)-15s %(levelname)s %(message)s")
LOG = logging.getLogger()

MIN_WINDOW = 1  # will skip first cycle if Tn - MIN_WINDOW > t > Tn

# 30 sec averaging with live=true param, 120 sec without.
LOOP_INTERVAL = 30


MessageType = Enum("MessageType", "DATA MSG SIGNAL")


class Msg(NamedTuple):
    msg_type: MessageType
    payload: Dict[str, Any]


async def get_connection(
    user: Optional[str] = None,
    database: str = DB,
    host: Optional[str] = None,
    password: Optional[str] = None,
):
    user = getuser() if user is None else user
    LOG.info(f"user: {user} db: {database} host: {host}")
    return await apg.connect(user=user, database=database, host=host, password=password)


def get_common_args() -> ArgumentParser:
    user = getuser()
    ap = ArgumentParser()
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
    ap.add_argument("--log-level", default="INFO", help="Override log level")
    return ap


def get_args() -> Namespace:
    ap = get_common_args()
    ap.add_argument(
        "--url", default="http://purpleair-c8ee.home.metrak.com/json", help="PAII url."
    )
    ap.add_argument(
        "--loop-interval", default=LOOP_INTERVAL, help="Device read interval"
    )
    opt = ap.parse_args()
    LOG.debug(opt)
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
    LOG.debug(opt)
    parms = {"live": "true"}
    # scheduler uses datetime, compares with utc, naive times.
    scheduler = TimedScheduler()
    scheduler.start()
    input_queue: asyncio.Queue = asyncio.Queue()
    result_queue: asyncio.Queue = asyncio.Queue()
    password = get_password(user=opt.user, password=opt.password)
    t_next = loop_time(loop_interval=LOOP_INTERVAL, min_window=MIN_WINDOW)
    # naive, zero UTC offset datetime value required by aioscheduler
    dt_next: datetime = datetime.fromtimestamp(t_next, tz=timezone.utc).replace(
        tzinfo=None
    )
    async with aiohttp.ClientSession() as session:
        worker = partial(
            device_reader, session, opt.url, parms, result_queue, input_queue
        )
        # push non-naive datetime for ultimate storage in DB
        input_queue.put_nowait({TIME_FIELD: dt_next.replace(tzinfo=timezone.utc)})
        task_next = scheduler.schedule(worker(), dt_next)
        # now we have scheduled our first job, do database once off prep here asynchronously
        conn = await get_connection(
            user=opt.user, database=DB, host=opt.host, password=password
        )
        create_sql = compose_create(
            table_name=opt.paii_table, time_field=opt.time_field
        )
        LOG.debug(f'create sql: "{create_sql}"')
        await conn.execute(create_sql)
        await conn.execute(
            f"SELECT create_hypertable('{opt.paii_table}', '{opt.time_field}', if_not_exists => TRUE);"
        )
        insert_sql = compose_insert(
            field_names=[f.db_name for f in gen_stored()], table_name=opt.paii_table
        )
        LOG.debug(f'insert sql: "{insert_sql}"')
        drain = False
        while True:
            LOG.debug(f"event scheduled at utc: {dt_next}")
            msg = await result_queue.get()
            LOG.debug(f">>> '{msg}'")
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
            t_next = loop_time(loop_interval=LOOP_INTERVAL, min_window=MIN_WINDOW)
            dt_next = datetime.fromtimestamp(t_next, tz=timezone.utc).replace(
                tzinfo=None
            )
            LOG.debug(f"scheduling event for: {dt_next}")
            input_queue.put_nowait({TIME_FIELD: dt_next.replace(tzinfo=timezone.utc)})
            task_next = scheduler.schedule(worker(), dt_next)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        LOG.warning("purple_device was interrupted, exiting")
