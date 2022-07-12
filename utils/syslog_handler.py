#!/usr/bin/env python3


import logging
import socket
from typing import Any

LOG = logging.getLogger(__name__)


def get_syslog_handler_args(
    host: str | None = None,
    port: str = "514",
) -> dict[str, Any] | None:
    if not host:
        return None
    if host.startswith("/dev"):
        # unix socket
        LOG.info(f"syslog address: {host}")
        return {"address": host}
    SOCKET_DICT = {"udp": socket.SOCK_DGRAM, "tcp": socket.SOCK_STREAM}
    pp = port.split("/")
    port = pp[0]
    protocol = pp[-1] if len(pp) > 1 else "udp"
    socktype = SOCKET_DICT.get(protocol, socket.SOCK_DGRAM)
    LOG.info(f"syslog address: ({host}, {port}/{protocol})")
    return {"address": (host, int(port)), "socktype": socktype}
