# Purple Air PAII sensor data collection

[PurpleAir](https://www.purpleair.com): Real-time Air Quality Monitoring

Makes a number of devices for measuring airborne pollution. When installed there
is a registration process which then adds the device to the [purple air map](https://www.purpleair.com/map). There are also APIs for downloading historic data from one or more sensors.

The device has its own REST API facilitating direct polling - all you need to know is the hostname/ip-address of your sensor(s) and you can point a browser at it. This is where this project has started.

The `paii_poll.py` script is intended to run continuously, polling the device at regular intervals and storing the data in a (postgresql+timescaledb) database.

"""
