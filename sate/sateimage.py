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

from sate.colormap import get_colormap
from sate.format import HimawariFormat, MutilSegmentHimawariFormat
from sate.satefile import SateFile
from tools.cache import Key

matplotlib.use('agg')
matplotlib.rc('font', family='HelveticaNeue')
logger = logging.getLogger(__name__)

np.seterr(invalid='ignore')

IMAGE_LON_RANGE_LIMIT = 11.89


class SateImage:

    def __init__(self, satefile):
        self.satefile = satefile
        self.figwidth = 1025 if satefile.area == 'target' else 1000
        self.figheight = 1000
        self.figaspect = self.figwidth / self.figheight
        self.dpi = 200
        self.bgcolor = '#121212'

    def load_colormap(self, name):
        return get_colormap(name)

    def set_sun_zenith_flag(self, georange):
        time = self.satefile.time
        if time.minute % 10 != 7:
            # Target area only
            return
        midlat = (georange[0] + georange[1]) / 2
        midlon = (georange[2] + georange[3]) / 2
        Key.set(Key.TARGET_AREA_MIDPOINT, (midlon, midlat), 3600)
        if 10 <= time.hour < 20:
            Key.set(Key.SUN_ZENITH_FLAG, False, 3600)
            return
        cos_zenith = cos_zen(time, midlon, midlat)
        COS_88DEG = 0.0349
        if cos_zenith > COS_88DEG:
            Key.set(Key.SUN_ZENITH_FLAG, True, 3600)
        else:
            Key.set(Key.SUN_ZENITH_FLAG, False, 3600)

    def _align_window(self, georange):
        """Align images to center on 1025 x 1000 canvas."""
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
            if os.path.getsize(self.satefile.target_path) < 100:
                logger.warning('Empty file: {}'.format(self.satefile.target_path))
                return
            # Extract data and coordinates
            hf = HimawariFormat(self.satefile.target_path)
            data = hf.extract()
            lons, lats = hf.get_geocoord()
            georange = lats.min(), lats.max(), lons.min(), lons.max()
            self.set_sun_zenith_flag(georange)
            lat1, lat2, lon1, lon2 = self._align_window(georange)
        elif self.satefile.area == 'fulldisk':
            for filepath in self.satefile.target_path:
                if os.path.getsize(filepath) < 100:
                    logger.warning('Empty file: {}'.format(self.satefile.target_path))
                    return
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
        _map = Basemap(projection='cyl', llcrnrlat=lat1, urcrnrlat=lat2, llcrnrlon=lon1,
            urcrnrlon=lon2, resolution='i')
        # Plot data
        target_xy, extent = KDResampler.make_target_coords((lat1, lat2, lon1, lon2),
            self.figwidth, self.figheight)
        resampler = KDResampler()
        resampler.build_tree(lons, lats)
        data = resampler.resample(data, target_xy[0], target_xy[1])
        for enh in enhances:
            fig = plt.figure(figsize=(self.figwidth / self.dpi, self.figheight / self.dpi))
            ax = fig.add_axes([0, 0, 1, 1])
            if band <= 3:
                cos_zenith = cos_zen(self.satefile.time, target_xy[0], target_xy[1])
                data = sun_zenith_correction(data, cos_zenith)
                # data = np.sqrt(data)
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
            ax.text(0.997, 0.997, 'Commercial Use PROHIBITED', va='top', ha='right',
                bbox=dict(boxstyle='round', facecolor=self.bgcolor, pad=0.3, edgecolor='none'),
                color='w', zorder=3, fontsize=6, transform=ax.transAxes)
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

