import datetime
import logging
import os
import shutil

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from django.conf import settings
from matplotlib.lines import Line2D
from mpl_toolkits.basemap import Basemap
from pykdtree.kdtree import KDTree
from pyorbital.astronomy import cos_zen
from pyproj import Proj

from sate.colormap import get_colormap
from sate.format import HimawariFormat, MutilSegmentHimawariFormat
from sate.satefile import SateFile
from tools.cache import Key
from tools.diagnosis.manager import DiagnosisSourceManager
from tools.utils import is_file_valid

matplotlib.use('agg')
matplotlib.rc('font', family='HelveticaNeue')
logger = logging.getLogger(__name__)

np.seterr(invalid='ignore')

IMAGE_LON_RANGE_LIMIT = 11.89
IMAGE_LON_RANGE_MERC_LIMIT = 1320200
MAX_LOOP_IMAGES = 30

DIAGTEXT_YINIT = 0.97
DIAGTEXT_YEND = 0.08
DIAGTEXT_LEFTX = 0.055
DIAGTEXT_YSTEP = 0.018

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
        self.enhances = self.get_enhances()
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

    def extract(self):
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
        georange = lat1, lat2, lon1, lon2
        return georange, lons, lats, data

    def get_enhances(self):
        # Gather enhancement and band info
        if not isinstance(self.satefile.enhance, tuple):
            enhances = [self.satefile.enhance]
        else:
            enhances = self.satefile.enhance
        # VIS doesn't have enhancement
        if self.satefile.band <= 3:
            enhances = [None]
        return enhances

    def make_map(self):
        lat1, lat2, lon1, lon2 = self.georange
        if self.use_mercator:
            clon1, clat1 = self.merc_proj(lon1, lat1, inverse=True)
            clon2, clat2 = self.merc_proj(lon2, lat2, inverse=True)
            _map = Basemap(projection='merc', llcrnrlat=clat1, urcrnrlat=clat2,
                llcrnrlon=clon1, urcrnrlon=clon2, resolution='i')
        else:
            _map = Basemap(projection='cyl', llcrnrlat=lat1, urcrnrlat=lat2,
                llcrnrlon=lon1, urcrnrlon=lon2, resolution='i')
        return _map

    def remap_data(self):
        lat1, lat2, lon1, lon2 = self.georange
        target_xy, extent = KDResampler.make_target_coords((lat1, lat2, lon1, lon2),
            self.figwidth, self.figheight)
        if self.use_mercator:
            target_xy = self.merc_proj(*target_xy, inverse=True)
        resampler = KDResampler()
        resampler.build_tree(self.lons, self.lats)
        self.data = resampler.resample(self.data, target_xy[0], target_xy[1])
        return extent, target_xy

    def imager(self):
        self.georange, self.lons, self.lats, self.data = self.extract()
        lat1, lat2, lon1, lon2 = self.georange
        # PLOT
        self.map = self.make_map()
        # Plot data
        extent, target_xy = self.remap_data()
        for enh in self.enhances:
            self.fig = plt.figure(figsize=(self.figwidth / self.dpi, self.figheight / self.dpi))
            self.ax = self.fig.add_axes([0, 0, 1, 1])
            if self.satefile.band <= 3:
                cos_zenith = cos_zen(self.satefile.time, target_xy[0], target_xy[1])
                data = sun_zenith_correction(self.data, cos_zenith)
                if self.satefile.band == 1:
                    data *= 0.92
                data = np.power(data, 0.8)
                cmap = 'gray'
                vmin = 0
                vmax = 1
            elif enh is None or enh == 'diagnosis':
                cmap = 'gray_r'
                vmin = -80
                vmax = 50
            else:
                cmap = self.load_colormap(enh)
                vmin = -100
                vmax = 50
            self.map.imshow(self.data, extent=extent, cmap=cmap, vmin=vmin, vmax=vmax)
            self.map.drawcoastlines(linewidth=0.4, color='w')
            self.map.readshapefile('/root/web/windygram/tools/metplot/shapefile/CP/ChinaProvince', 'Province', linewidth=0.2, color='w', ax=self.ax)
            if enh:
                xoffset = (lon2 - lon1) / 30
                self.map.drawparallels(np.arange(-90,90,1), linewidth=0.2, dashes=(None, None),
                    color='w', xoffset=-xoffset, labels=(1,0,0,0), textcolor='w', fontsize=5,
                    zorder=3)
                yoffset = (lat2 - lat1) / 20
                self.map.drawmeridians(np.arange(0,360,1), linewidth=0.2, dashes=(None, None),
                    color='w', yoffset=-yoffset, labels=(0,0,0,1), textcolor='w', fontsize=5,
                    zorder=3)
            enh_str = enh or ''
            enh_disp = '-' + enh_str if enh else ''
            cap = '{} HIMAWARI-8 BAND{:02d}{}'.format(self.satefile.time.strftime('%Y/%m/%d %H%MZ'),
                self.satefile.band, enh_disp)
            self.ax.text(0.5, 0.003, cap.upper(), va='bottom', ha='center', transform=self.ax.transAxes,
                bbox=dict(boxstyle='round', facecolor=self.bgcolor, pad=0.3, edgecolor='none'),
                color='w', zorder=3, fontsize=6)
            if enh == 'diagnosis':
                self.add_diagnosis()
            self.ax.axis('off')
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

    def add_diagnosis(self):
        storm = self.satefile.storm
        manager = DiagnosisSourceManager.get_or_create(storm.code, storm)
        textdict = {
            'va': 'center',
            'ha': 'left',
            'color': 'w',
            'zorder': 4,
            'fontsize': 6
        }
        x = DIAGTEXT_LEFTX
        y = DIAGTEXT_YINIT
        handlers = []
        self.ax.text(x, y, f'{storm.code}  {storm.name}  DIAGNOSIS',
            transform=self.ax.transAxes, **textdict)
        source = manager.get_source('JTWC')
        y -= DIAGTEXT_YSTEP * 2
        self.add_diagnosis_source(x, y, source, textdict)
        #
        source = manager.get_source('ADT')
        y -= DIAGTEXT_YSTEP * 2
        self.add_diagnosis_source(x, y, source, textdict)
        #
        source = manager.get_source('AMSU')
        y -= DIAGTEXT_YSTEP
        self.add_diagnosis_source(x, y, source, textdict)
        #
        source = manager.get_source('SATCON')
        y -= DIAGTEXT_YSTEP
        self.add_diagnosis_source(x, y, source, textdict)
        #
        source = manager.get_source('RIPA')
        y -= DIAGTEXT_YSTEP * 2
        self.add_diagnosis_source(x, y, source, textdict)
        #
        source = manager.get_source('ADT')
        if source is not None and source.loaded:
            lons, lats = self.map(np.array(source.lons), np.array(source.lats))
            self.ax.plot(lons, lats, color='#0077ed', lw=0.6, zorder=4)
            label = 'ADT [{}Z]'.format(source.data_time.strftime('%H%M'))
            handlers.append(Line2D([], [], color='#0077ed', lw=0.6, label=label))
        #
        source = manager.get_source('Archer')
        if source is not None and source.loaded:
            for lon, lat, time in zip(source.lons, source.lats, source.times):
                if not (lon or lat):
                    continue
                if self.satefile.time - time > datetime.timedelta(hours=24):
                    continue
                lon, lat = self.map(lon, lat)
                mark = self.ax.annotate('×', xy=(lon, lat), va='center', ha='center',
                    xycoords='data', fontsize=4, color=get_time_color(self.satefile.time, time),
                    zorder=5)
                self.ax.annotate(time.strftime('%H%MZ'), xy=(1, 0.5), xycoords=mark,
                    xytext=(1, 0), textcoords='offset points', va='center', ha='left',
                    fontsize=4, color=get_time_color(self.satefile.time, time), zorder=5)
            latest_color = get_time_color(self.satefile.time, source.data_time)
            label = 'Archer [{}Z]'.format(source.data_time.strftime('%H%M'))
            handlers.append(Line2D([], [], color=latest_color, lw=0, label=label, marker='x'))
        #
        source = manager.get_source('JTWC-Forecast')
        if source is not None and source.loaded:
            lons, lats = self.map(np.array(source.lons), np.array(source.lats))
            self.ax.plot(lons, lats, color='#0077ed', lw=0.6, linestyle='--',
                zorder=4)
            label = 'JTWC Forecast [{}Z]'.format(source.data_time.strftime('%H%M'))
            handlers.append(Line2D([], [], color='#0077ed', lw=0.6, linestyle='--', label=label))
        #
        source = manager.get_source('ECMWF-Forecast')
        if source is not None and source.loaded:
            lons, lats = self.map(np.array(source.lons), np.array(source.lats))
            self.ax.plot(lons, lats, color='#0077ed', lw=1, linestyle=':',
                zorder=4)
        #
        legend = self.ax.legend(handles=handlers, loc='upper right', framealpha=0., prop=dict(size=6))
        for label in legend.get_texts():
            label.set_color('w')
        #
        y = DIAGTEXT_YEND
        self.ax.text(DIAGTEXT_LEFTX, y, 'RED: 12 ~ 24 hours old', transform=self.ax.transAxes,
            color='#ff4271', ha='left', va='center', zorder=4, fontsize=6)
        y += DIAGTEXT_YSTEP
        self.ax.text(DIAGTEXT_LEFTX, y, 'YELLOW: 6 ~ 12 hours old', transform=self.ax.transAxes,
            color='#edca00', ha='left', va='center', zorder=4, fontsize=6)
        y += DIAGTEXT_YSTEP
        self.ax.text(DIAGTEXT_LEFTX, y, 'GREEN: < 6 hours old', transform=self.ax.transAxes,
            color='#00bf60', ha='left', va='center', zorder=4, fontsize=6)

    def add_diagnosis_source(self, x, y, source, textdict):
        if source is None or not source.loaded:
            return
        texts = source.represent()
        if isinstance(texts, str):
            texts = [texts]
        for text in texts:
            real_text = '{}  {}'.format(source.name, text)
            mark = self.ax.annotate(real_text, xy=(x,y), xycoords='axes fraction', **textdict)
            time_text = '[{}Z]'.format(source.data_time.strftime('%H%M'))
            if textdict['ha'] == 'left':
                xyan = (1, 0.5)
                xytext = (2, 0)
            else:
                xyan = (0, 0.5)
                xytext = (-2, 0)
            self.ax.annotate(time_text, xy=xyan, xytext=xytext, xycoords=mark, textcoords='offset points',
                va=textdict['va'], ha=textdict['ha'], color=get_time_color(self.satefile.time, source.data_time),
                fontsize=5, zorder=4)
            y -= DIAGTEXT_YSTEP


def get_time_color(image_time, data_time):
    delta = image_time - data_time
    if delta < datetime.timedelta(hours=6):
        color = '#00bf60'
    elif delta < datetime.timedelta(hours=12):
        color = '#edca00'
    else:
        color = '#ff4271'
    return color


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

