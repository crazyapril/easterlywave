import logging
import os

import matplotlib
import numpy as np
from cartopy.mpl.patch import geos_to_path
from django.conf import settings
from matplotlib.collections import PathCollection
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.path import Path
from matplotlib.pyplot import cm
from shapely.geometry import LineString, MultiLineString, Point, box

from tools.metplot.plotplus import Plot
from tools.utils import geoscale

matplotlib.use('agg')

logger = logging.getLogger(__name__)


RES = 0.1 #spatial resolution of probabilities
RADIUS = 1 #size of strike swath, unit in lat/lon
THRESHOLD = 10 #zoom in to only show area where probabilities are greater than THRESHOLD
SHOW_PROB = 20 #only show cities where probabilities are greater than SHOW_PROB
NUM_OF_MEMS = 50 #num of ensemble members
DAYS_AHEAD = None #only draw tracks during next few days
NO_TRACK = False #no ensemble track display
NO_SUBPLOT = True #no subplot
TOP_CITIES = 9 #Maxmimum num of cities to be shown
LON_RANGE_LIMIT = 80 #maximum lon range allowed for storm plot or it will abort
VALID_POINTS_THRESHOLD = 320 #320/2132, threshold to make storm plot regardless of its name


def a_color(p):
    txtcolor = 'k' if p < 35 else 'w'
    p = 100 - p
    p /= 100
    bgcolor = cm.hot(p)
    return bgcolor, txtcolor

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


class StormPlot:

    code_eps = ['ep%02d' % i for i in range(1, 26)]
    code_ens = ['en%02d' % i for i in range(1, 26)]
    code_sps = ['emx', 'eemn', 'ec00']

    def __init__(self, basetime, storms):
        self.basetime = basetime
        self.storms = storms

    def plot_all(self):
        self.plotted = []
        for storm in self.storms:
            storm.load(qc_method='strict')
            if storm.valid_points < VALID_POINTS_THRESHOLD:
                continue
            try:
                self.plot_storm(storm)
            except Exception as exc:
                logger.exception('Error while plotting {}'.format(storm.codename))
            else:
                self.plotted.append(storm)

    def plot_storm(self, storm):
        logger.info('Plot storm for {}'.format(storm.codename))
        self.storm = storm
        self.georange = roundit(geoscale(*self.storm.get_georange(), pad=1.5))
        if (self.georange[3] - self.georange[2]) >= LON_RANGE_LIMIT:
            raise ValueError('Image range too large. Georange: {}'.format(self.georange))
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
        self.prob_grid = self.prob_grid / NUM_OF_MEMS * 100
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
        for code in self.storm.iter_members():
            if not NO_TRACK:
                self.p.plot(self.storm.lons, self.storm.lats, marker=None,
                    linestyle='-', lw=0.3, color='#CCCCCC')
            self.intens.append((code, self.storm.minpres))
        # Deterministic
        self.storm.set_data_pointer('EMX')
        if not np.all(np.isnan(self.storm.lats)):
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
        if not np.all(np.isnan(self.storm.lats)):
            self.p.plot(self.storm.lons, self.storm.lats, marker='o', markersize=2,
                mec='none', linestyle='-', lw=0.5, color='#AAAAAA')
            self.intens.append(('CTRL', self.storm.minpres))

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
        self.p.title('Strike Probabilities* of %s Based on ECMWF Ensemble %s'
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
                family='Source Han Sans SC', color=txtcolor,
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
            fontsize=6, family='Source Han Sans SC')
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
