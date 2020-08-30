#!/usr/bin/env python3

from setuptools import find_packages, setup

with open("README.md", "r") as doc:
    long_description = doc.read()

setup(
    name="paiitools",
    version="0.1",
    description="purple PAII device polling",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="PurpleAir PAII postgresql timescaledb",
    author="paul sorenson",
    author_email="news02@metrak.com",
    packages=find_packages(),
    package_data={"": ["*.ini"]},
    # packages=["paii"],
    scripts=["scripts/paii_poll.py", "scripts/config.ini"],
    requires=["aiohttp", "asyncscheduler", "ayncpg", "keyring"],
    install_requires=["setuptools"],
)