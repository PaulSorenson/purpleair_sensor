# Purple Air PAII sensor data collection

[PurpleAir](https://www.purpleair.com): **Real-time Air Quality Monitoring** make a number of devices for measuring airborne pollution. When registered the device data can be viewed on the [purple air map](https://www.purpleair.com/map). There are also APIs for downloading historic sensor data from the cloud.

The device has its own REST API for direct polling - all you need to know is the hostname/ip-address of your sensor(s) and you can point a browser at it. The `paii_poll.py` script is intended to run continuously, polling the device at regular intervals and storing the data in a (postgresql+timescaledb) database.

Pull requests are welcome.

## INSTALLATION

As a prerequisite to run the code you should have:

- a working postgres server
- with a database named "timeseries" (unless you override the name in options)
- a role/user with CREATE TABLE and write access to the "timeseries"
- network setup to access allow the user to access it.
- installation of timescaledb. Note it probably will work without this at a small scale, you would need to supply the `--no-timescaledb` option.

I have only tested this on fedora/centos. Get the code:

```bash
git clone https://github.com/PaulSorenson/purpleair_sensor.git
cd purpleair_sensor
```

Run directly from source:

```bash
PYTHONPATH=. python3.8 scripts/paii_poll.py
```

Install:

```bash
python3.8 setup.py install --user
# or
sudo python3.8 setup.py install
```

Although `scripts/purple_air.ini` is not essential it will make running the `paii_poll.py` script more convenient. To do that make a copy of `scripts/purple_air.ini.template` alongside `paii_poll.py` and adjust the entries as necessary.

To manage passwords to your postgres server, there are a few choices:

- If you have installed python `keyring` and a working back end (eg gnome) then `keyring set <user> postgres` should stash your postgres password away safely and `paii_poll.py` will try to retrieve it from there.
- Can add it to the command line - it will be in your bash history so be careful.
- add it to `purple_air.ini`. At least you can set the permissions so no one else can read it.
- you can be prompted for it.
