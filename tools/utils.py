import datetime
import subprocess

def utc_last_tick(interval, offset_minutes=0, offset_seconds=0, delay_minutes=0):
    """Return latest time of given interval and offset.

    For example, assume that `datetime.datetime.utcnow()` gives
    `datetime.datetime(2019, 3, 22, 11, 26, 35)`.

    Then:
    utc_last_tick(5) -> datetime.datetime(2019, 3, 22, 11, 25)
    utc_last_tick(10) -> datetime.datetime(2019, 3, 22, 11, 20)
    utc_last_tick(10, delay_minutes=10) -> datetime.datetime(2019, 3, 22, 11, 10)
    utc_last_tick(10, offset_minutes=5) -> datetime.datetime(2019, 3, 22, 11, 25)
    utc_last_tick(10, offset_minutes=8) -> datetime.datetime(2019, 3, 22, 11, 18)
    utc_last_tick(10, offset_minutes=5, delay_minutes=10) ->
        datetime.datetime(2019, 3, 22, 11, 15)
    """
    offset = datetime.timedelta(minutes=offset_minutes, seconds=offset_seconds)
    time = datetime.datetime.utcnow().timestamp() - offset.seconds
    interval_seconds = interval * 60
    last_tick = datetime.datetime.fromtimestamp(time // interval_seconds * interval_seconds +\
        offset.seconds - delay_minutes * 60)
    return last_tick

def execute(command, check=True):
    return subprocess.run(command, check=check, shell=True)
