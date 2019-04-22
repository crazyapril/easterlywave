import datetime
import ftplib
import logging
import os

import matplotlib
matplotlib.use('agg')
import numpy as np
from cartopy.mpl.patch import geos_to_path
from celery import shared_task
from django.conf import settings
from matplotlib.collections import PatchCollection
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.path import Path
from matplotlib.pyplot import cm
from pybufrkit.dataquery import DataQuerent, NodePathParser
from pybufrkit.decoder import Decoder
from shapely.geometry import LineString, MultiLineString, Point, box

from tools.cache import Key
from tools.fastdown import FTPFastDown
from tools.mapstore import MapArea, tropical_mapkeys
from tools.metplot.plotplus import Plot
from tools.utils import geoscale

RES = 0.1 #spatial resolution of probabilities
RADIUS = 1 #size of strike swath, unit in lat/lon
THRESHOLD = 10 #zoom in to only show area where probabilities are greater than THRESHOLD
SHOW_PROB = 20 #only show cities where probabilities are greater than SHOW_PROB
NUM_OF_MEMS = 50 #num of ensemble members
DAYS_AHEAD = None #only draw tracks during next few days
NO_TRACK = False #no ensemble track display
NO_SUBPLOT = True #no subplot
TOP_CITIES = 9 #Maxmimum num of cities to be shown
ECMWF_FTP_ADDRESS = 'data-portal.ecmwf.int'
ECMWF_FTP_USERNAME = 'wmo'
ECMWF_FTP_PASSWORD = 'essential'
HISTORY_DAYS = 3
MOVEMENT_LIMIT_IN_6_HOURS = 4

logger = logging.getLogger(__name__)


class ECEnsembleRoutine:

    def go(self, time=None):
        if time is None:
            self.ticker()
        else:
            self.basetime = time
        downer = ECMWFDown()
        downer.set_time(self.basetime)
        downer.search_from_ftp()
        downer.download()
        downer.disconnect()
        rp = RegionPlot(self.basetime, downer.storms)
        rp.plot_all()
        sp = StormPlot(self.basetime, downer.named_storms)
        sp.plot_all()
        self.save_cache(sp.plotted)
        logger.info('EC ensemble task finished.')

    def ticker(self):
        nowtime = datetime.datetime.utcnow()
        if  7 <= nowtime.hour < 19:
            self.time = nowtime.replace(hour=0)
        elif nowtime.hour >= 19:
            self.time = nowtime.replace(hour=12)
        else:
            self.time = nowtime.replace(hour=12)
            self.time = self.time - datetime.timedelta(days=1)
        self.basetime = self.time.strftime('%Y%m%d%H')
        logger.info('EC ensemble task started. Runtime: {}'.format(self.basetime))

    def save_cache(self, storms):
        cache_entry = Key.get(Key.ECMWF_ENSEMBLE_STORMS)
        if cache_entry is None:
            cache_entry = []
        cache_entry = cache_entry[:HISTORY_DAYS*2]
        writed = False
        for entry in cache_entry:
            if entry['basetime'] == self.basetime:
                entry['storms'] = [storm.codename for storm in storms]
                writed = True
                break
        if not writed:
            cache_entry.insert(0, {
                'basetime': self.basetime,
                'storms': [storm.codename for storm in storms]
            })
        Key.set(Key.ECMWF_ENSEMBLE_STORMS, cache_entry, 3 * Key.DAY)
        logger.info('Save cache: {}'.format(cache_entry))


class ECMWFDown:

    def __init__(self):
        self.connected = False
        self.storms = []

    def connect(self):
        self.ftp = ftplib.FTP(ECMWF_FTP_ADDRESS, user=ECMWF_FTP_USERNAME,
            passwd=ECMWF_FTP_PASSWORD)
        self.connected = True

    def disconnect(self):
        self.ftp.close()
        self.connected = False

    def set_time(self, time):
        self.basetime = time

    def search_from_ftp(self):
        self.connect()
        self.ftp.cwd(self.basetime + '0000')
        filenames = self.ftp.nlst()
        for fname in filenames:
            if 'tropical_cyclone_track' in fname and 'ECEP' in fname:
                bf = BufrFile(fname)
                self.storms.append(bf)
        self.named_storms = [storm for storm in self.storms \
            if storm.codename[0] not in '789']

    def download(self, storms=None):
        if storms is None:
            storms = self.storms
        downlist = [storm for storm in storms if not storm.file_exists()]
        if not downlist:
            return storms
        if not self.connected:
            self.connect()
        dirpath = '/' + downlist[0].basetime+'0000'
        if self.ftp.pwd() != dirpath:
            self.ftp.cwd(dirpath)
        downer = FTPFastDown(file_parallel=1)
        downer.set_ftp(self.ftp)
        downer.set_task([(s.filename, s.filepath) for s in downlist])
        logger.info('Download bufr files from ECMWF ftp server. '
            'Total files: {}'.format(len(downlist)))
        downer.download()
        logger.info('Download finished.')
        return storms


class BufrFile:

    CODE_LAT = '005002'
    CODE_LON = '006002'
    CODE_WIND = '011012'
    CODE_PRES = '010051'
    CODE_TIME = '004024'

    def __init__(self, filename):
        self.filename = filename
        self.loaded = False
        self._analyze_filename()

    def __repr__(self):
        return '<{}>'.format(self.codename)

    def file_exists(self):
        return os.path.exists(self.filepath)

    def _analyze_filename(self):
        segs = self.filename.split('_')
        self.emx_flag = 'X' if 'ECEP' not in segs[1] else 'E'
        self.num = int(segs[1][4:6])
        self.basetime = segs[4][:10]
        self.codename = segs[8]
        self.atcfname = None
        self.slon = float(segs[9][:-4].replace('p', '.'))
        self.slat = float(segs[10][:-4].replace('p', '.'))
        self.filepath = os.path.join(settings.TMP_ROOT, 'ecens/{}/{}.bufr'.format(
            self.basetime, self.codename))
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

    def load(self, qc=True, force_reload=False):
        if self.loaded and not force_reload:
            return
        with open(self.filepath, 'rb') as f:
            message = Decoder().process(f.read())
        queryer = DataQuerent(NodePathParser())
        self._lons = []
        self._lats = []
        self._wind = []
        self._pres = []
        for subset in range(52):
            # lat
            try:
                values = queryer.query(message, '@[{}] > {}'.format(subset,
                    self.CODE_LAT)).all_values()
            except IndexError:
                raw_lats = np.empty(41)
                raw_lats[:] = np.nan
            else:
                raw_lats = np.array(values[0][3], dtype='float')[:,0]
                raw_lats = np.insert(raw_lats, 0, values[0][1])
                raw_lats = self.length_control(raw_lats)
            self._lats.append(raw_lats)
            # lon
            try:
                values = queryer.query(message, '@[{}] > {}'.format(subset,
                    self.CODE_LON)).all_values()
            except IndexError:
                raw_lons = np.empty(41)
                raw_lons[:] = np.nan
            else:
                raw_lons = np.array(values[0][3], dtype='float')[:,0]
                raw_lons = np.insert(raw_lons, 0, values[0][1])
                raw_lons = self.length_control(raw_lons)
            raw_lons[raw_lons<0] = raw_lons[raw_lons<0] + 360
            self._lons.append(raw_lons)
            # wind
            try:
                values = queryer.query(message, '@[{}] > {}'.format(subset,
                    self.CODE_WIND)).all_values(flat=True)
            except IndexError:
                raw_wind = np.empty(41)
                raw_wind[:] = np.nan
            else:
                raw_wind = np.array(values[0], dtype='float') * 1.94 # to kt
                raw_wind = self.length_control(raw_wind)
            self._wind.append(raw_wind)
            # pres
            try:
                values = queryer.query(message, '@[{}] > {}'.format(subset,
                    self.CODE_PRES)).all_values(flat=True)
            except IndexError:
                raw_pres = np.empty(41)
                raw_pres[:] = np.nan
            else:
                raw_pres = np.array(values[0], dtype='float') / 100 # to hPa
                raw_pres = self.length_control(raw_pres)
            self._pres.append(raw_pres)
        self._lats = np.vstack(self._lats)
        self._lons = np.vstack(self._lons)
        self._wind = np.vstack(self._wind)
        self._pres = np.vstack(self._pres)
        mask = self.movement_control(self._lats, self._lons)
        self._lats[mask] = np.nan
        self._lons[mask] = np.nan
        self._wind[mask] = np.nan
        self._pres[mask] = np.nan
        self.loaded = True

    def movement_control(self, lats, lons):
        '''Quality control and filter messy ECMWF raw bufr data.'''
        # First step: calculate movement distances between points, if the distance
        # is beyond a reasonable range, then all subsequent points are labelled as
        # invalid.
        distance = np.diff(lats) ** 2 + np.diff(lons) ** 2
        distance_illegal = distance > MOVEMENT_LIMIT_IN_6_HOURS ** 2
        distance_mask = np.cumsum(distance_illegal, axis=1).astype(bool)
        # ----
        # Second step: mark nan values as invalid. Note: the starting values can be
        # nan as the storm may have not formed yet. So different from first step,
        # we should find first nonnan values stretch, then label all remaining data
        # as invalid.
        nan_data_legal = np.pad(np.isnan(distance), ((0,0),(1,0)), 'constant',
            constant_values=True)
        nan_diff = np.diff(nan_data_legal.astype(int), axis=1)
        on_range = np.cumsum(nan_diff < 0, axis=1).astype(bool)
        off_range = np.cumsum(nan_diff > 0, axis=1).astype(bool)
        nan_data_mask = ~on_range | off_range
        # ----
        # temporary solution
        # nan_data_illegal = np.isnan(distance)
        # nan_data_mask = np.cumsum(nan_data_illegal, axis=1).astype(bool)
        # Combine two masks to create final mask.
        total_mask = np.pad(distance_mask | nan_data_mask, ((0,0),(1,0)), 'edge')
        return total_mask

    def length_control(self, arr, fixed_length=41):
        if arr.shape[0] < fixed_length:
            arr = np.pad(arr, (0, fixed_length - arr.shape[0]), 'constant',
                constant_values=np.nan)
        elif arr.shape[0] > fixed_length:
            arr = arr[:fixed_length]
        return arr

    def set_hour_range(self, hours):
        index = hours // 6 + 1
        self._lats[:, index:] = np.nan
        self._lons[:, index:] = np.nan
        self._wind[:, index:] = np.nan
        self._pres[:, index:] = np.nan
        self._maxwind = np.nanmax(self._wind, axis=1)
        self._minpres = np.nanmin(self._pres, axis=1)

    def iter_members(self):
        for i in range(50):
            mask = np.isnan(self._lats[i, :]) | np.isnan(self._lons[i, :]) | \
                np.isnan(self._wind[i, :]) | np.isnan(self._pres[i, :])
            self.lats = self._lats[i, :][~mask]
            self.lons = self._lons[i, :][~mask]
            self.wind = self._wind[i, :][~mask]
            self.pres = self._pres[i, :][~mask]
            if np.all(np.isnan(self.lats)):
                continue
            try:
                self.maxwind = self.wind.max()
            except ValueError:
                self.maxwind = None
            try:
                self.minpres = self.pres.min()
            except ValueError:
                self.minpres = None
            if i < 25:
                code = 'EN{:02d}'.format(i + 1)
            else:
                code = 'EP{:02d}'.format(i - 24)
            yield code

    def set_data_pointer(self, code):
        if isinstance(code, int):
            i = code
        elif code == 'EC00':
            i = 50
        elif code == 'EMX':
            i = 51
        elif code == 'EEMN':
            i = 52
        elif code.startswith('EN'):
            i = int(code[2:]) - 1
        elif code.startswith('EP'):
            i = int(code[2:]) + 24
        mask = np.isnan(self._lats[i, :]) | np.isnan(self._lons[i, :]) | \
                np.isnan(self._wind[i, :]) | np.isnan(self._pres[i, :])
        self.lats = self._lats[i, :][~mask]
        self.lons = self._lons[i, :][~mask]
        self.wind = self._wind[i, :][~mask]
        self.pres = self._pres[i, :][~mask]
        try:
            self.maxwind = self.wind.max()
        except ValueError:
            self.maxwind = None
        try:
            self.minpres = self.pres.min()
        except ValueError:
            self.minpres = None

    def get_georange(self):
        latmax = np.nanmax(self._lats)
        latmin = np.nanmin(self._lats)
        lonmax = np.nanmax(self._lons)
        lonmin = np.nanmin(self._lons)
        return latmin, latmax, lonmin, lonmax


def getcolor(i):
    if i == None:
        return 'X', '#AAAAAA'
    s = '%d hPa' % i
    if i > 1000:
        return s, '#444444'
    if i > 990:
        return s, '#2288FF'
    if i > 970:
        return s, 'orange'
    if i > 950:
        return s, '#FF2288'
    return s, '#800000'

def a_color(p):
    txtcolor = 'k' if p < 35 else 'w'
    p = 100 - p
    p /= 100
    bgcolor = cm.hot(p)
    return bgcolor, txtcolor


class RegionPlot:

    def __init__(self, basetime, storms):
        self.basetime = basetime
        self.storms = storms
        for storm in self.storms:
            storm.load(qc=True)

    def plot_all(self):
        for basin in tropical_mapkeys:
            try:
                self.plot_region(basin)
            except Exception as exc:
                logger.exception('Error while plotting {}.'.format(basin))

    def plot_region(self, mapkey):
        logger.info('Plot region: {}'.format(mapkey))
        mapset = MapArea.get(mapkey).load()
        p = Plot(inside_axis=True, figsize=(6, 3))
        p.usemapset(mapset)
        p.style('bom')
        p.draw('coastline')
        p.drawparameri(lw=0.3)
        for storm in self.storms:
            if mapkey in ('sio', 'aus') and storm.slat >= 0:
                continue
            if mapkey not in ('sio', 'aus') and storm.slat <= 0:
                continue
            for _ in storm.iter_members():
                p.plot(storm.lons, storm.lats, marker='None', color='#444444', lw=0.3)
                colors = [getcolor(pres)[1] for pres in storm.pres]
                p.scatter(storm.lons, storm.lats, s=6, facecolors='none',
                    linewidths=0.4, edgecolors=colors, zorder=3)
        p.ax.text(0.99, 0.98, 'ECMWF Cyclone Ensemble\nRuntime: '+self.basetime,
            ha='right', va='top', fontsize=5, family=p.family,
            transform=p.ax.transAxes, color=p.style_colors[2])
        # legends
        legend_data = [('#444444', '>1000 hPa'), ('#2288FF', '990-1000 hPa'),
            ('orange', '970-990 hPa'), ('#FF2288', '950-970 hPa'), ('#800000', '<950 hPa')]
        legend_handles = []
        for mc, desc in legend_data:
            legend_handles.append(Line2D([], [], linewidth=0.3, color='#444444',
                marker='o', markerfacecolor='none', markeredgecolor=mc, label=desc,
                markersize=3))
        p.legend(handles=legend_handles, loc='upper left', framealpha=0)
        filepath = os.path.join(settings.MEDIA_ROOT, 'typhoon/ecens/{}/{}.png'.format(
            self.basetime, mapkey))
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        p.save(filepath)
        logger.info('Export to {}'.format(filepath))
        p.clear()


def roundit(georange):
    latmin, latmax, lonmin, lonmax = georange
    latmin = round(latmin / RES) * RES
    latmax = round(latmax / RES) * RES
    lonmin = round(lonmin / RES) * RES
    lonmax = round(lonmax / RES) * RES
    georange = latmin, latmax, lonmin, lonmax
    return georange

def get_grids(georange):
    latmin, latmax, lonmin, lonmax = georange
    x = np.arange(lonmin, lonmax+RES, RES)
    y = np.arange(latmin, latmax+RES, RES)
    grid = np.zeros((y.shape[0], x.shape[0]))
    return x, y, grid


class StormPlot:

    code_eps = ['ep%02d' % i for i in range(1, 26)]
    code_ens = ['en%02d' % i for i in range(1, 26)]
    code_sps = ['emx', 'eemn', 'ec00']

    def __init__(self, basetime, storms):
        self.basetime = basetime
        self.storms = storms
        for storm in self.storms:
            storm.load()

    def plot_all(self):
        self.plotted = []
        for storm in self.storms:
            try:
                self.plot_storm(storm)
            except Exception as exc:
                logger.exception('Error while plotting {}'.format(storm.codename))
            else:
                self.plotted.append(storm)

    def plot_storm(self, storm):
        logger.info('Plot storm for {}'.format(storm.codename))
        self.storm = storm
        self.analyze()
        self.set_map()
        self.plot_probs()
        self.plot_tracks()
        self.plot_legend()
        self.plot_infos()
        self.plot_columns()
        self.plot_city_probs()
        if not NO_SUBPLOT:
            self.plot_subplot()
        self.save()

    def analyze(self):
        self.georange = roundit(geoscale(*self.storm.get_georange(), pad=1.5))
        x, y, self.prob_grid = get_grids(self.georange)
        self.xshape = x.shape[0]
        self.yshape = y.shape[0]
        xx, yy = np.meshgrid(x, y)
        xy = np.dstack((xx, yy)).reshape((self.xshape * self.yshape, 2))
        for code in self.storm.iter_members():
            linestring = list(zip(self.storm.lons, self.storm.lats))
            if len(linestring) < 2:
                continue
            path = geos_to_path(LineString(linestring).buffer(RADIUS))[0]
            boolarr = path.contains_points(xy).reshape(
                (self.yshape, self.xshape)).astype(np.uint8)
            self.prob_grid += boolarr
        self.cities_probs = self.calc_city_probs()
        self.ngeorange = self.calc_new_georange()

    def calc_city_probs(self):
        cities = list()
        latmin, latmax, lonmin, lonmax = self.georange
        path = os.path.join(os.path.dirname(__file__), 'cities.txt')
        f = open(path, 'r', encoding='utf-8')
        for line in f:
            data = line.split()
            name = data[1]
            lat = round(float(data[2][:-1]) / RES) * RES
            if lat > latmax or lat < latmin:
                continue
            lon = round(float(data[3][:-1]) / RES) * RES
            if lon > lonmax or lon < lonmin:
                continue
            x = int((lon - lonmin) / RES)
            y = int((lat - latmin) / RES)
            p = self.prob_grid[y, x]
            if p > SHOW_PROB:
                cities.append([p, name])
        cities.sort(reverse=True)
        f.close()
        if len(cities) > TOP_CITIES:
            cities = cities[:TOP_CITIES]
        if len(cities) > 0:
            logger.info('List of cities under threat: {}'.format(cities))
        else:
            logger.info('No cities under threat.')
        return cities

    def calc_new_georange(self, pad=2):
        y, x = np.where(self.prob_grid > THRESHOLD)
        latmin, latmax, lonmin, lonmax = self.georange
        nlatmin = latmin + y.min() * RES - pad
        nlatmax = latmin + y.max() * RES + pad
        nlonmin = lonmin + x.min() * RES - pad
        nlonmax = lonmin + x.max() * RES + pad
        return geoscale(nlatmin, nlatmax, nlonmin, nlonmax)

    def set_map(self):
        logger.debug('Setting maps...')
        ###PLOT: SET MAP
        self.p = Plot(dpi=200, figsize=(6,6))
        #self.p.setfamily('Segoe UI Emoji')
        self.p.setmap(proj='P', georange=self.ngeorange, resolution='i')
        self.p.setxy(self.georange, RES)

    def plot_probs(self):
        logger.debug('Plotting probabilities...')
        ###PLOT: PLOT PROBABILITIES & COLORBAR
        self.p.contourf(self.prob_grid, gpfcmap='strikeprob',
            levels=np.arange(0, 101, 2), cbar=True, cbardict=dict(sidebar=True))

    def plot_tracks(self):
        logger.debug('Plotting tracks...')
        ###PLOT: PLOT LINES
        self.intens = []
        # Deterministic
        self.storm.set_data_pointer('EMX')
        self.p.plot(self.storm.lons, self.storm.lats, marker='o', markersize=2,
            mec='none', linestyle='-', lw=0.5, color='#8877CC')
        self.intens.append(('DET', self.storm.minpres))
        # Mean
        # storm.set_data_pointer('EEMN')
        # self.p.plot(storm.lons, storm.lats, marker='o', markersize=2, mec='none',
        #             linestyle='-', lw=0.5, color='#99DD22')
        # self.intens.append(('MEAN', storm.minpres))
        # Control
        self.storm.set_data_pointer('EC00')
        self.p.plot(self.storm.lons, self.storm.lats, marker='o', markersize=2,
            mec='none', linestyle='-', lw=0.5, color='#AAAAAA')
        self.intens.append(('CTRL', self.storm.minpres))
        for code in self.storm.iter_members():
            if not NO_TRACK:
                self.p.plot(self.storm.lons, self.storm.lats, marker=None,
                    linestyle='-', lw=0.3, color='#CCCCCC')
            self.intens.append((code, self.storm.minpres))

    def plot_legend(self):
        logger.debug('Plotting legends...')
        ###PLOT: PLOT LEGEND
        h_e = Line2D([], [], color='#CCCCCC', lw=0.3, marker=None,
            label='Ensemble Cluster')
        h_x = Line2D([], [], color='#8877CC', lw=0.5, marker='o', ms=2,
            mec='none', label='Deterministic')
        # h_n = Line2D([], [], color='#99DD22', lw=0.5, marker='o', ms=2,
        #     mec='none', label='Ensemble Mean')
        h_c = Line2D([], [], color='#AAAAAA', lw=0.5, marker='o', ms=2,
            mec='none', label='Ensemble Control')
        handles = [h_e, h_x, h_c]
        self.p.legend(handles=handles, loc='upper right', framealpha=0.8)

    def plot_infos(self):
        logger.debug('Plotting infos...')
        ###PLOT: PLOT INFORMATION
        namestr = '[{}]'.format(self.storm.codename)
        self.time = self.storms[0].basetime
        hourstr = '(Within {:d} hours)'.format(int(DAYS_AHEAD * 24)) \
            if DAYS_AHEAD else ''
        self.p.title('Strike Probabilites* of %s Based on ECMWF Ensemble %s'
            '' % (namestr, hourstr))
        self.p._timestamp('Init Time: {:s}/{:s}/{:s} {:s}Z'.format(
            self.time[:4], self.time[4:6], self.time[6:8], self.time[8:]))
        self.p.draw('meripara country province coastline')
        self.p._maxminnote('*probability that the center of the tropical '
            'cyclone will pass within 1 lat/lon (approx. 100~110km) of a '
            'location')

    def plot_city_probs(self):
        logger.debug('Plotting city probs...')
        ###PLOT: PLOT CITY PROBABILITIES
        x = 0.02
        for item in self.cities_probs:
            prob, name = tuple(item)
            bgcolor, txtcolor = a_color(prob)
            y = -0.04
            s = '{:s}  {:.0f}%'.format(name, prob)
            a = self.p.ax.annotate(s, xy=(x, y), va='top', ha='left',
                xycoords='axes fraction', fontsize=6,
                family='Source Han Sans CN', color=txtcolor,
                bbox=dict(facecolor=bgcolor, edgecolor='none',
                    boxstyle='square', alpha=0.6))
            self.p.ax.figure.canvas.draw()
            x = self.p.ax.transAxes.inverted().transform(
                a.get_window_extent())[1, 0] + 0.02

    def plot_columns(self):
        logger.debug('Plotting intensity columns...')
        ###PLOT: PLOT MEMBER PRESSURE
        for i, e in enumerate(self.intens[::-1]):
            code, inten = e
            s, c = getcolor(inten)
            s = code + ' ' + s
            self.p.ax.text(1.01, i * 0.02, s, color=c, fontsize=5,
                family='Lato', transform=self.p.ax.transAxes)

    def plot_subplot(self):
        logger.debug('Plotting subplots...')
        if len(self.cities_probs) == 0:
            return
        ###SUBPLOT: GET HIGHLIGHT COASTLINES
        highlights, cgeorange = self.get_highlight_coastline()
        if len(highlights[1]) == 0:
            return
        self.buffersize = (cgeorange[3] - cgeorange[2]) / 100
        cgeorange = geoscale(*cgeorange, scale=0.43)
        self.set_subplot_map(highlights, cgeorange)
        self.plot_subplot_track(highlights)

    def save(self):
        filepath = os.path.join(settings.MEDIA_ROOT,
            'typhoon/ecens/{}/{}.png'.format(self.basetime, self.storm.codename))
        self.p.save(filepath)
        logger.info('Export to {}'.format(filepath))
        self.p.clear()

    def set_subplot_map(self, highlights, cgeorange):
        ###SUBPLOT: SET MAP
        self.oldax = self.p.ax
        self.p.ax = self.p.fig.add_axes([0.02,-0.39,1.04,0.44])
        self.p.setmap(proj='P', georange=cgeorange, resolution='i')
        self.p.draw('country coastline province city')
        ###SUBPLOT: HIGHLIGHT COASTLINES & PLOT LEGEND
        colors = ['#AAFFAA', '#FFFF44', '#FF3333', '#BB0044']
        descr = ['10~25%', '25~50%', '50~75%', '>75%']
        handles = []
        for i, clr in enumerate(colors, 0):
            patch = PathCollection(geos_to_path(MultiLineString(
                highlights[i]).buffer(self.buffersize)), facecolor=clr)
            self.p.ax.add_collection(patch)
            handles.append(Patch(color=clr, label=descr[i]))
        self.p.ax.text(0.98, 0.27, '中心经过1经纬度\n范围内的几率',
            transform=self.p.ax.transAxes, va='bottom', ha='right',
            fontsize=6, family='Source Han Sans CN')
        self.p.legend(handles=handles, loc='lower right', framealpha=0.8)

    def plot_subplot_track(self, highlights):
        ###SUBPLOT: PLOT DETERMINISTIC TRACK
        self.storm.set_data_pointer('EMX')
        self.p.plot(self.storm.lons, self.storm.lats, marker='o', markersize=2,
            mec='none', linestyle='-', lw=0.5, color='#CCCCCC')
        self.p.ax = self.oldax

    def get_highlight_coastline(self):
        lines = self.p.getfeature('physical', 'coastline', self.p.scale).geometries()
        latmin, latmax, lonmin, lonmax = self.georange
        highlights = [[], [], [], []] #10~25% 25~50% 50~75% 75~100%
        latmins, latmaxs, lonmins, lonmaxs = [], [], [], [] #Coastline georange
        if lonmin > 180:
            lonmin = lonmin - 360
        if lonmax > 180:
            lonmax = lonmax - 360
        boundbox = box(lonmin, latmin, lonmax, latmax)
        segs = []
        for seg in lines:
            ls = LineString(seg).intersection(boundbox)
            if isinstance(ls, LineString):
                segs.append(np.array(ls))
            elif isinstance(ls, MultiLineString):
                segs.extend([np.array(s) for s in ls.geoms])
        for s in segs:
            sr = np.round(s / RES) * RES
            xi = ((sr[:,0] - lonmin) / RES).astype(np.int)
            yi = ((sr[:,1] - latmin) / RES).astype(np.int)
            p = self.prob_grid[yi, xi].astype(np.uint8)
            if p.max() < 10:
                continue
            p[p < 10] = 0
            p[(p >= 10) & (p < 25)] = 1 # Green
            p[(p >= 25) & (p < 50)] = 2 # Yellow
            p[(p >= 50) & (p < 75)] = 3 # Red
            p[p >= 75] = 4 # Purple
            cutindex = np.where(np.diff(p))[0] + 1
            cutseg = np.split(s, cutindex)
            cutindex = np.insert(cutindex, 0, 0)
            for cseg, cindex in zip(cutseg, cutindex):
                if p[cindex] > 0 and len(cseg) > 1:
                    if p[cindex] > 1:
                        latmins.append(cseg[:,1].min())
                        latmaxs.append(cseg[:,1].max())
                        lonmins.append(cseg[:,0].min())
                        lonmaxs.append(cseg[:,0].max())
                    highlights[p[cindex]-1].append(tuple(map(tuple, cseg)))
        if len(latmins) == 0:
            return None, None
        cgeorange = min(latmins), max(latmaxs), min(lonmins), max(lonmaxs)
        return highlights, cgeorange


def _debug_plot_ec_ens(time=None):
    ECEnsembleRoutine().go(time=time)

@shared_task(ignore_result=True)
def plot_ec_ensemble():
    try:
        ECEnsembleRoutine().go()
    except Exception as exp:
        logger.exception('A fatal error happened.')
