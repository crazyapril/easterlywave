import datetime
import logging
import os

import numpy as np
import xarray as xr
from celery import shared_task
from django.conf import settings

from tools.cache import Key
from tools.mapstore import MAP_EASTASIA, MAP_MICRONESIA, MapArea, tropical_maps
from tools.metplot.plotplus import Plot
from tools.utils import aria2_download

logger = logging.getLogger(__name__)

HISTORY_DAYS = 15
RTOFS_DOWNLOAD_URL = 'https://www.ftp.ncep.noaa.gov/data/nccf/com/rtofs/prod/rtofs.{date}/rtofs_glo_2ds_n000_daily_prog.nc'


class RTOFSRoutine:

    def go(self):
        self.ticker()
        self.download()
        self.open_dataset()
        self.plot()
        self.close_dataset()
        self.finish()

    def ticker(self):
        nowtime = datetime.datetime.utcnow()
        self.time = nowtime.replace(hour=0, minute=0, second=0)
        if  nowtime.hour < 3:
            self.time = self.time - datetime.timedelta(days=1)
        logger.info('RTOFS SST task started. Runtime: {}'.format(
            self.time.strftime('%Y%m%d')))

    def download(self):
        url = RTOFS_DOWNLOAD_URL.format(date=self.time.strftime('%Y%m%d'))
        target_dir = os.path.join(settings.TMP_ROOT, 'typhoon/')
        filename = url.split('/')[-1]
        # Aria2c is a good tool to download large files!
        logger.info('Starting to download RTOFS model data... It could took long time.')
        aria2_download(url, filedir=target_dir, threads=16)
        logger.info('Download finished!')
        self.filepath = os.path.join(target_dir, filename)

    def open_dataset(self):
        self.ds = xr.open_dataset(self.filepath)

    def get_slice(self, georange):
        """Get slice of RTOFS data (lat, lon, data) efficiently.

        The RTOFS model has a unregular grid. Below about 47N, model coordinates
        is a cylindrical one, in which longitudes are evenly spaced. Above 47N,
        data is highly distorted. As I am focusing on tropical seas where
        latitudes are well below 47N, instead of searching latitudes and
        longitudes in a large array (3298 x 4500), I could slice the whole data
        into a small one first, then search the wanted bounding box. For a
        standard area like Western Pacific, this approach would only cost <17%
        of memory used in previous method, and for smaller area it's even more
        efficient.
        """
        STARTING_LON = 74.16
        REGULAR_LON_STEP = 0.08
        REGULAR_Y_LIMIT = 2170
        REGULAR_LAT_LIMIT = 46.0
        X_SHAPE = 4500
        latmin, latmax, lonmin, lonmax = georange
        if latmax > REGULAR_LAT_LIMIT:
            raise NotImplementedError('This method is not ready for area above 46N')
        if lonmin < STARTING_LON:
            lonmin = 360 + lonmin
        if lonmax < STARTING_LON:
            lonmax = 360 + lonmax
        x0 = int((lonmin - STARTING_LON) / REGULAR_LON_STEP)
        x1 = int((lonmax - STARTING_LON) / REGULAR_LON_STEP)
        if STARTING_LON < lonmin < lonmax:
            lat_slice = self.ds.Latitude[:REGULAR_Y_LIMIT, x0:x1+1]
            y_window, x_window = np.where(np.logical_and(lat_slice >= latmin,
                lat_slice <= latmax))
            y_start = y_window.min()
            y_end = y_window.max()
            x_start = x_window.min() + x0
            x_end = x_window.max() + x0
            lats = self.ds.Latitude[y_start:y_end+1, x_start:x_end+1]
            lons = self.ds.Longitude[y_start:y_end+1, x_start:x_end+1]
            sst = self.ds.sst[0, y_start:y_end+1, x_start:x_end+1]
        elif STARTING_LON < lonmax < lonmin:
            lat_slice = np.hstack((self.ds.Latitude[:REGULAR_Y_LIMIT, x0:],
                self.ds.Latitude[:REGULAR_Y_LIMIT, :x1+1]))
            y_window, x_window = np.where(np.logical_and(lat_slice >= latmin,
                lat_slice <= latmax))
            y_start = y_window.min()
            y_end = y_window.max()
            x_start = x_window.min() + x0
            x_end = x_window.max() + x0 - X_SHAPE
            lats = np.hstack((self.ds.Latitude[y_start:y_end+1, x_start:],
                self.ds.Latitude[y_start:y_end+1, :x_end+1]))
            lons = np.hstack((self.ds.Longitude[y_start:y_end+1, x_start:],
                self.ds.Longitude[y_start:y_end+1, :x_end+1]))
            lons[lons > 360] -= 360
            sst = np.hstack((self.ds.sst[0, y_start:y_end+1, x_start:],
                self.ds.sst[0, y_start:y_end+1, :x_end+1]))
        sst = np.ma.masked_invalid(sst)
        return lats, lons, sst

    def plot(self):
        regions = tropical_maps.copy()
        regions.append(MAP_EASTASIA)
        regions.append(MAP_MICRONESIA)
        for region in regions:
            self.plot_region(region)

    def plot_region(self, region_map):
        p = Plot(boundary='rect')
        p.usemapset(region_map.load())
        p.drawparameri(lw=0.3)
        lats, lons, sst = self.get_slice(region_map.georange)
        p.ax.add_feature(p.mapset.land, color='#E8E1C4')
        p.drawcoastline()
        p._setxy(lons, lats)
        cbarlevels = [0, 6, 12, 16, 20, 22, 24, 26, 27, 28, 29, 30, 31, 32]
        p.contourf(sst, gpfcmap='sst', cbar=True, cbardict=dict(ticks=cbarlevels))
        p.contour(sst, levels=cbarlevels, color='none', clabeldict={'colors': 'k',
            'fontsize': 4})
        p.title('RTOFS Sea Surface Temperature')
        p._timestamp(self.time.strftime('Time: %Y/%m/%d'))
        target_path = os.path.join(settings.MEDIA_ROOT, 'typhoon/sst/{}/{}.png'
            ''.format(self.time.strftime('%Y%m%d'), region_map.key))
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        p.save(target_path)
        logger.debug('RTOFS SST plot has been saved to {}'.format(target_path))
        p.clear()

    def close_dataset(self):
        self.ds.close()

    def finish(self):
        os.remove(self.filepath)
        days = Key.get(Key.RTOFS_SST_DAYS) or []
        days.insert(0, self.time.strftime('%Y%m%d'))
        days = days[:HISTORY_DAYS]
        Key.set(Key.RTOFS_SST_DAYS, days, Key.DAY * HISTORY_DAYS)
        logger.info('Finished RTOFS task. Runtime: {}'.format(self.time.strftime('%Y%m%d')))


@shared_task
def plot_rtofs_sst():
    try:
        RTOFSRoutine().go()
    except Exception as exp:
        logger.exception('A fatal error happened.')


def _debug():
    filepath = '/root/temp/rtofs/rtofs_glo_2ds_n000_daily_prog.nc'
    r = RTOFSRoutine()
    r.ticker()
    r.filepath = filepath
    r.open_dataset()
    r.plot()
    r.close_dataset()
