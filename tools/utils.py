import datetime
import subprocess

from django.utils.timezone import now


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
    time = now().timestamp() - offset.seconds
    interval_seconds = interval * 60
    last_tick = datetime.datetime.utcfromtimestamp(time // interval_seconds * \
        interval_seconds + offset.seconds - delay_minutes * 60)
    return last_tick

def execute(command, check=True):
    return subprocess.run(command, check=check, shell=True)

def geoscale(latmin, latmax, lonmin, lonmax, scale=0.8, pad=0.):
    latmin -= pad
    latmax += pad
    lonmin -= pad
    lonmax += pad
    latmid = latmax/2 + latmin/2
    lonmid = lonmax/2 + lonmin/2
    deltalat = latmax - latmin
    deltalon = lonmax - lonmin
    if deltalat / deltalon > scale:
        deltalon = deltalat / scale
        lonmax = lonmid + deltalon / 2
        lonmin = lonmid - deltalon / 2
    elif deltalat / deltalon < scale:
        deltalat = deltalon * scale
        latmax = latmid + deltalat / 2
        latmin = latmid - deltalat / 2
    return latmin, latmax, lonmin, lonmax

def aria2_download(url, filedir=None, threads=16):
    try:
        s = '-d ' + filedir if filedir else ''
        result = execute('aria2c -x {} {} {}'.format(threads, s, url))
    except FileNotFoundError:
        raise IOError('aria2c is not installed on this machine.')
    return result

def get_climatological_data(dataset, var, time, georange, step=1):
    import datetime
    import numpy as np
    import os
    import xarray as xr
    from django.conf import settings
    dataset = xr.open_dataset(os.path.join(settings.CLIMATE_DATA_ROOT, dataset+'.nc'))
    timeidx = (time.replace(year=2000) - datetime.datetime(2000, 1, 1)) //\
        datetime.timedelta(hours=6)
    resolution = 0.25
    latmin, latmax, lonmin, lonmax = georange
    xmin = int(lonmin / resolution)
    xmax = int(lonmax / resolution)
    ymin = int((90 - latmax) / resolution)
    ymax = int((90 - latmin) / resolution)
    data = np.flipud(dataset.get(var)[timeidx, ymin:ymax+1:step, xmin:xmax+1:step])
    dataset.close()
    return data

