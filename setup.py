#!/usr/bin/env python3

from setuptools import find_packages, setup

with open("README.md", "r") as doc:
    long_description = doc.read()

setup(
    long_description=long_description,
    long_description_content_type="text/markdown",
    data_files=[
        (
            "config",
            [
                "config/purple_air.ini.template",
                "config/paii_poll_supervisor.ini.template",
            ],
        )
    ],
)
