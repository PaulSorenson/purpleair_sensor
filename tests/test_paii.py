#!/usr/bin/env python3

import logging
from socket import SOCK_DGRAM, SOCK_STREAM

# from logging.handlers import SysLogHandler
from unittest import mock

import pytest

from utils.syslog_handler import get_syslog_handler_args

UNIX_DOMAIN = "/dev/log"


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ((None, 514), None),
        ((UNIX_DOMAIN, None), {"address": UNIX_DOMAIN}),
        (("localhost",), {"address": ("localhost", 514), "socktype": SOCK_DGRAM}),
        (("localhost", "520"), {"address": ("localhost", 520), "socktype": SOCK_DGRAM}),
        (
            ("localhost", "520/tcp"),
            {"address": ("localhost", 520), "socktype": SOCK_STREAM},
        ),
        ((UNIX_DOMAIN, "520/tcp"), {"address": UNIX_DOMAIN}),
        (
            ("localhost", "520/udp"),
            {"address": ("localhost", 520), "socktype": SOCK_DGRAM},
        ),
        (
            ("logger.example.com", "520/tcp"),
            {"address": ("logger.example.com", 520), "socktype": SOCK_STREAM},
        ),
        (
            ("logger.example.com",),
            {"address": ("logger.example.com", 514), "socktype": SOCK_DGRAM},
        ),
    ],
)
def test_get_syslog_handler_args(test_input, expected):
    assert get_syslog_handler_args(*test_input) == expected
