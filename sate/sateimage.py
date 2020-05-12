import datetime
import logging
import os
import shutil

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from django.conf import settings
from mpl_toolkits.basemap import Basemap
from pykdtree.kdtree import KDTree
from pyorbital.astronomy import cos_zen
from pyproj import Proj

from sate.colormap import get_colormap
from sate.format import HimawariFormat, MutilSegmentHimawariFormat
from sate.satefile import SateFile
from tools.cache import Key
from tools.utils import is_file_valid

matplotlib.use('agg')
matplotlib.rc('font', family='HelveticaNeue')
logger = logging.getLogger(__name__)

np.seterr(invalid='ignore')

IMAGE_LON_RANGE_LIMIT = 11.89
IMAGE_LON_RANGE_MERC_LIMIT = 1320200
MAX_LOOP_IMAGES = 30

#TODO: Mercator


class MidpointManagement:

    MAX_HISTORY_POINTS = 24
    MAX_ALLOWED_FLUTTER = 3
    MAX_ALLOWED_TIMEGAP = datetime.timedelta(minutes=60)

    def __init__(self):
        self.last_updated = None
        self.history = []
        self.latest_midpoint = None

    @classmethod
    def get(cls):
        instance = Key.get(Key.TA_MIDPOINT_HISTORY)
        if instance is None:
            instance = cls()
        return instance

    def update(self, midpoint, time):
        if self.last_updated == time:
            return self.latest_midpoint
        if self.last_updated is None:
            self.history = [midpoint]
            avgpoint = midpoint
        elif time - self.last_updated > self.MAX_ALLOWED_TIMEGAP:
            self.history = [midpoint]
            avgpoint = midpoint
        else:
            avgpoint = tuple(np.array(self.history).mean(axis=0))
            movement = (midpoint[0] - avgpoint[0]) ** 2 + \
                (midpoint[1] - avgpoint[1]) ** 2
            if movement > self.MAX_ALLOWED_FLUTTER ** 2:
                self.history = [midpoint]
                avgpoint = midpoint
            else:
                self.history.append(midpoint)
                if len(self.history) > self.MAX_HISTORY_POINTS:
                    self.history = self.history[-self.MAX_HISTORY_POINTS:]
                avgpoint = tuple(np.array(self.history).mean(axis=0))
        self.latest_midpoint = avgpoint
        logger.info('Target area midpoint: {}'.format(avgpoint))
        self.last_updated = time
        self.set_midpoint(avgpoint, time)
        Key.set(Key.TA_MIDPOINT_HISTORY, self, 3600)
        return avgpoint

    def set_midpoint(self, midpoint, time):
        Key.set(Key.TARGET_AREA_MIDPOINT, midpoint, 3600)
        if 10 <= time.hour < 20:
            Key.set(Key.SUN_ZENITH_FLAG, False, 3600)
            return
        midlon, midlat = midpoint
        cos_zenith = cos_zen(time, midlon, midlat)
        COS_88DEG = 0.0349
        if cos_zenith > COS_88DEG:
            Key.set(Key.SUN_ZENITH_FLAG, True, 3600)
        else:
            Key.set(Key.SUN_ZENITH_FLAG, False, 3600)


class SateImage:

    def __init__(self, satefile):
        self.satefile = satefile
        if satefile.area == 'target':
            self.figwidth = 1025
            self.figheight = 1000
            self.use_mercator = True
        else:
            self.figwidth = 1000
            self.figheight = self.figwidth * settings.FD_IMAGE_RANGE[1] / \
                settings.FD_IMAGE_RANGE[0]
            self.use_mercator = False
        self.figaspect = self.figwidth / self.figheight
        self.dpi = 200
        self.bgcolor = '#121212'

    def load_colormap(self, name):
        return get_colormap(name)

    def set_target_area_midpoint(self, georange):
        """To keep image range from flutter, and determine VIS flags."""
        time = self.satefile.time
        midlat = (georange[0] + georange[1]) / 2
        midlon = (georange[2] + georange[3]) / 2
        mm = MidpointManagement.get()
        midpoint = mm.update((midlon, midlat), time)
        return midpoint

    def _align_window(self, georange):
        """Align images to center on 1025 x 1000 canvas."""
        midlon, midlat = self.set_target_area_midpoint(georange)
        deltalon = georange[3] - georange[2]
        deltalat = georange[1] - georange[0]
        georange = (midlat - deltalat / 2,
            midlat + deltalat / 2,
            midlon - deltalon / 2,
            midlon + deltalon / 2)
        if self.use_mercator:
            IMAGE_LON_RANGE_LIMIT = IMAGE_LON_RANGE_MERC_LIMIT
            self.merc_proj = Proj(proj='merc', ellps='WGS84')
            coord_tuple = self.merc_proj(georange[2:], georange[:2])
            georange = coord_tuple[1] + coord_tuple[0]
        imaspect = (georange[3] - georange[2]) / (georange[1] - georange[0])
        if imaspect > self.figaspect:
            # Image is wider than canvas, pad upper and lower edges
            lon1 = georange[2]
            lon2 = georange[3]
            if lon2 - lon1 > IMAGE_LON_RANGE_LIMIT:
                lonmid = (lon1 + lon2) / 2
                lon1 = lonmid - IMAGE_LON_RANGE_LIMIT / 2
                lon2 = lonmid + IMAGE_LON_RANGE_LIMIT / 2
            lmid = (georange[0] + georange[1]) / 2
            ldelta = (lon2 - lon1) / self.figaspect
            lat1 = lmid - ldelta / 2
            lat2 = lmid + ldelta / 2
        else:
            # Image is taller than canvas, pad left and right edges
            IMAGE_LAT_RANGE_LIMIT = IMAGE_LON_RANGE_LIMIT / self.figaspect
            lat1 = georange[0]
            lat2 = georange[1]
            if lat2 - lat1 > IMAGE_LAT_RANGE_LIMIT:
                latmid = (lat1 + lat2) / 2
                lat1 = latmid - IMAGE_LAT_RANGE_LIMIT / 2
                lat2 = latmid + IMAGE_LAT_RANGE_LIMIT / 2
            lmid = (georange[2] + georange[3]) / 2
            ldelta = (lat2 - lat1) * self.figaspect
            lon1 = lmid - ldelta / 2
            lon2 = lmid + ldelta / 2
        return lat1, lat2, lon1, lon2

    def imager(self):
        if self.satefile.area == 'target':
            # Check if hsd file is successfully downloaded, if not, quit
            if not is_file_valid(self.satefile.target_path):
                logger.warning('Empty file: {}'.format(self.satefile.target_path))
                return
            # Extract data and coordinates
            hf = HimawariFormat(self.satefile.target_path)
            data = hf.extract()
            lons, lats = hf.get_geocoord()
            georange = lats.min(), lats.max(), lons.min(), lons.max()
            lat1, lat2, lon1, lon2 = self._align_window(georange)
        elif self.satefile.area == 'fulldisk':
            # for filepath in self.satefile.target_path:
            #     if os.path.getsize(filepath) < 100:
            #         logger.warning('Empty file: {}'.format(self.satefile.target_path))
            #         return
            hf = MutilSegmentHimawariFormat(self.satefile.target_path)
            data = hf.extract(vline=self.satefile.vline, vcol=self.satefile.vcol,
                decompress=self.satefile.band <= 3)
            lons, lats = hf.get_geocoord(vline=self.satefile.vline, vcol=self.satefile.vcol)
            lat1, lat2, lon1, lon2 = self.satefile.georange
        # Gather enhancement and band info
        if not isinstance(self.satefile.enhance, tuple):
            enhances = [self.satefile.enhance]
        else:
            enhances = self.satefile.enhance
        band = self.satefile.band
        # VIS doesn't have enhancement
        if band <= 3:
            enhances = [None]
        # PLOT
        if self.use_mercator:
            clon1, clat1 = self.merc_proj(lon1, lat1, inverse=True)
            clon2, clat2 = self.merc_proj(lon2, lat2, inverse=True)
            _map = Basemap(projection='merc', llcrnrlat=clat1, urcrnrlat=clat2,
                llcrnrlon=clon1, urcrnrlon=clon2, resolution='i')
        else:
            _map = Basemap(projection='cyl', llcrnrlat=lat1, urcrnrlat=lat2,
                llcrnrlon=lon1, urcrnrlon=lon2, resolution='i')
        # Plot data
        target_xy, extent = KDResampler.make_target_coords((lat1, lat2, lon1, lon2),
            self.figwidth, self.figheight)
        if self.use_mercator:
            target_xy = self.merc_proj(*target_xy, inverse=True)
        resampler = KDResampler()
        resampler.build_tree(lons, lats)
        data = resampler.resample(data, target_xy[0], target_xy[1])
        for enh in enhances:
            fig = plt.figure(figsize=(self.figwidth / self.dpi, self.figheight / self.dpi))
            ax = fig.add_axes([0, 0, 1, 1])
            if band <= 3:
                cos_zenith = cos_zen(self.satefile.time, target_xy[0], target_xy[1])
                data = sun_zenith_correction(data, cos_zenith)
                if band == 1:
                    data *= 0.92
                data = np.power(data, 0.8)
                cmap = 'gray'
                vmin = 0
                vmax = 1
            elif enh is None:
                cmap = 'gray_r'
                vmin = -80
                vmax = 50
            else:
                cmap = self.load_colormap(enh)
                vmin = -100
                vmax = 50
            _map.imshow(data, extent=extent, cmap=cmap, vmin=vmin, vmax=vmax)
            _map.drawcoastlines(linewidth=0.4, color='w')
            if enh:
                xoffset = (lon2 - lon1) / 30
                _map.drawparallels(np.arange(-90,90,1), linewidth=0.2, dashes=(None, None),
                    color='w', xoffset=-xoffset, labels=(1,0,0,0), textcolor='w', fontsize=5,
                    zorder=3)
                yoffset = (lat2 - lat1) / 20
                _map.drawmeridians(np.arange(0,360,1), linewidth=0.2, dashes=(None, None),
                    color='w', yoffset=-yoffset, labels=(0,0,0,1), textcolor='w', fontsize=5,
                    zorder=3)
            enh_str = enh or ''
            enh_disp = '-' + enh_str if enh else ''
            cap = '{} HIMAWARI-8 BAND{:02d}{}'.format(self.satefile.time.strftime('%Y/%m/%d %H%MZ'),
                band, enh_disp)
            ax.text(0.5, 0.003, cap.upper(), va='bottom', ha='center', transform=ax.transAxes,
                bbox=dict(boxstyle='round', facecolor=self.bgcolor, pad=0.3, edgecolor='none'),
                color='w', zorder=3, fontsize=6)
            ax.axis('off')
            export_path = self.satefile.export_path.format(enh=enh_str)
            os.makedirs(os.path.dirname(export_path), exist_ok=True)
            plt.savefig(export_path, dpi=self.dpi, facecolor=self.bgcolor)
            logger.info('Export to {}'.format(export_path))
            plt.clf()
            plt.close()
            # copy to latest dir
            latest_path = self.satefile.latest_path.format(enh=enh_str)
            shutil.copyfile(export_path, latest_path)
            self._write_cache(enh_str, export_path)

    def _write_cache(self, enh_str, export_path):
        name = self.satefile.name or 'TARGET'
        keyname = Key.SATE_LOOP_IMAGES.format(storm=name)
        images_dict = Key.get(keyname)
        if images_dict is None:
            images_dict = {}
        if self.satefile.band in (1, 3):
            bandname = 'VIS'
        elif self.satefile.band == 8:
            bandname = 'WV'
        elif self.satefile.band == 13:
            bandname = 'IR'
        enh_str = bandname + '-' + enh_str.upper()
        enh_str = enh_str.rstrip('-')
        if enh_str not in images_dict:
            images_dict[enh_str] = []
        images = images_dict[enh_str]
        images.append('/'.join(export_path.split('/')[-3:]))
        if len(images) > MAX_LOOP_IMAGES:
            images_dict[enh_str] = images[-MAX_LOOP_IMAGES:]
        Key.set(keyname, images_dict, Key.HOUR * 6)


def sun_zenith_correction(data, cos_zen, limit=88., max_sza=95.):
    """Perform Sun zenith angle correction for VIS.
    Refer: https://github.com/pytroll/satpy/blob/40a0dcd91544a0785c47fd2c025dac8892718800/satpy/utils.py#L215"""

    # Convert the zenith angle limit to cosine of zenith angle
    limit_rad = np.deg2rad(limit)
    limit_cos = np.cos(limit_rad)
    max_sza_rad = np.deg2rad(max_sza) if max_sza is not None else max_sza

    # Cosine correction
    corr = 1. / cos_zen
    if max_sza is not None:
        # gradually fall off for larger zenith angle
        grad_factor = (np.arccos(cos_zen) - limit_rad) / (max_sza_rad - limit_rad)
        # invert the factor so maximum correction is done at `limit` and falls off later
        grad_factor = 1. - np.log(grad_factor + 1) / np.log(2)
        # make sure we don't make anything negative
        grad_factor = grad_factor.clip(0.)
    else:
        # Use constant value (the limit) for larger zenith angles
        grad_factor = 1.

    grad_area = cos_zen < limit_cos
    corr[grad_area] = (grad_factor / limit_cos)[grad_area]

    return data * corr


class KDResampler:

    def __init__(self, distance_limit=0.05, leafsize=32):
        self.distance_limit = distance_limit
        self.leafsize = leafsize

    @staticmethod
    def make_target_coords(georange, width, height, pad=0., ratio=1.02):
        latmin, latmax, lonmin, lonmax = georange
        image_width = int(width * ratio)
        image_height = int(height * ratio)
        ix = np.linspace(lonmin-pad, lonmax+pad, image_width)
        iy = np.linspace(latmin-pad, latmax+pad, image_height)
        return np.meshgrid(ix, iy), (lonmin-pad, lonmax+pad, latmin-pad, latmax+pad)

    def build_tree(self, lons, lats):
        self.tree = KDTree(np.dstack((lons.ravel(), lats.ravel()))[0], leafsize=self.leafsize)

    def resample(self, data, target_x, target_y):
        target_coords = np.dstack((target_x.ravel(), target_y.ravel()))[0]
        _, indices = self.tree.query(target_coords, distance_upper_bound=self.distance_limit)
        invalid_mask = indices == self.tree.n # beyond distance limit
        indices[invalid_mask] = 0
        remapped = np.ma.masked_array(data.ravel()[indices], mask=invalid_mask)
        remapped = remapped.reshape(target_x.shape)
        return remapped

