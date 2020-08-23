import logging
import os

import numpy as np
from cartopy.mpl.patch import geos_to_path
from django.conf import settings
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import cascaded_union

from tools.metplot.plotplus import Plot

logger = logging.getLogger(__name__)

RES = 0.2 #spatial resolution of probabilities
RADII_CIRCLE_DEG_RES = 15 #resolution of wind radii directions (in degree)
NUM_OF_MEMS = 50 #num of ensemble members


class WindRadiiPlot:

    def __init__(self, storm, georange):
        self.storm = storm
        self.georange = georange
        self.rgeorange = None
        self.grid0 = None
        self.grid = None
        self.xy = None
        self.xshape = None
        self.yshape = None
        self.wind_repr = None
        self.plot = None

    def make_plots(self):
        logger.info('Make wind radii plot for %s', self.storm)
        self.storm.load_radii()
        self.make_grid()
        self.wind_repr = '34kt'
        self.make_plot(0)
        self.wind_repr = '50kt'
        self.make_plot(1)
        self.wind_repr = '64kt'
        self.make_plot(2)

    def make_plot(self, radii_num):
        logger.info('Plot %s radii probs for %s', self.wind_repr, self.storm)
        self.create_plot()
        grid = self.analyze(radii_num)
        self.plot_grid(grid)
        self.plot_emx_track()
        self.plot_infos()
        self.save()

    def make_grid(self):
        logger.info('Make grid for %s', self.storm)
        latmin, latmax, lonmin, lonmax = self.georange
        latmin = np.floor(latmin / RES) * RES
        latmax = np.ceil(latmax / RES) * RES
        lonmin = np.floor(lonmin / RES) * RES
        lonmax = np.ceil(lonmax / RES) * RES
        self.rgeorange = latmin, latmax, lonmin, lonmax
        x = np.arange(lonmin, lonmax + RES, RES)
        self.xshape = x.shape[0]
        y = np.arange(latmin, latmax + RES, RES)
        self.yshape = y.shape[0]
        xx, yy = np.meshgrid(x, y)
        self.xy = np.dstack((xx, yy)).reshape((self.xshape * self.yshape, 2))
        self.grid0 = np.zeros((self.yshape, self.xshape))

    def make_radii_coords(self, lats, lons, radii):
        # sampling directions, +4 because 0/90/180/270 are repeated
        SAMPLING_DIRS = 360 // RADII_CIRCLE_DEG_RES + 4
        HOURS_NUM = lats.shape[0]
        R = 6371000
        # convert to rads and make a new axis
        lats = np.deg2rad(lats)[:, None]
        lons = np.deg2rad(lons)[:, None]
        # repeat radii data to sampling directions and calculate angular
        # distances for latter use
        angular_radii = np.repeat(radii, SAMPLING_DIRS // 4, axis=1) / R
        # calculate bearings at sampling directions, in rads
        bearings = np.concatenate(tuple(
            np.linspace(i * np.pi/2, (i+1) * np.pi/2, SAMPLING_DIRS // 4)
            for i in range(4)
        ))
        # repeat bearings by number of hours
        bearings = np.broadcast_to(bearings, (HOURS_NUM, SAMPLING_DIRS))
        # calculate latlon of destination point with bearing and distance
        # source: https://www.movable-type.co.uk/scripts/latlong.html
        clats = np.arcsin(np.sin(lats)*np.cos(angular_radii) + \
            np.cos(lats)*np.sin(angular_radii)*np.cos(bearings))
        clons = lons + np.arctan2(np.sin(bearings)*np.sin(angular_radii)*np.cos(lats),
            np.cos(angular_radii)-np.sin(lats)*np.sin(clats))
        # convert back to degrees
        clats = np.rad2deg(clats)
        clons = np.rad2deg(clons)
        return clats, clons

    def create_plot(self):
        self.plot = Plot(dpi=200, figsize=(6,6))
        self.plot.setmap(proj='P', georange=self.georange, resolution='i')
        self.plot.setxy(self.rgeorange, RES)

    def analyze(self, radii_num):
        grid = self.grid0.copy()
        for code in self.storm.iter_members():
            radii = self.storm.radii[:, radii_num, :]
            if radii.max() == 0:
                continue
            clats, clons = self.make_radii_coords(self.storm.lats,
                self.storm.lons, radii)
            polygons = [Polygon(i) for i in np.dstack((clons, clats))]
            convexes = [
                MultiPolygon((polygons[i], polygons[i+1])).convex_hull
                for i in range(len(polygons)-1)
            ]
            union_convex = cascaded_union(convexes).buffer(0)
            union_path = geos_to_path(union_convex)[0]
            mask = union_path.contains_points(self.xy)\
                .reshape((self.yshape, self.xshape)).astype(np.uint8)
            grid += mask
        grid = grid * 100 / NUM_OF_MEMS
        return grid

    def plot_grid(self, grid):
        logger.debug('Plotting probabilities of %s...', self.wind_repr)
        self.plot.pcolormesh(grid, gpfcmap='strikeprob', cbar=True,
            cbardict=dict(sidebar=True))

    def plot_emx_track(self):
        logger.debug('Plotting EMX track...')
        self.storm.set_data_pointer('EMX')
        self.plot.plot(self.storm.lons, self.storm.lats, marker='o', markersize=2,
            mec='none', linestyle='-', lw=0.5, color='#8877CC', zorder=4)

    def plot_infos(self):
        logger.debug('Plotting infos...')
        time = self.storm.basetime
        self.plot.title('Experimental {} Wind Speed Probabilities of [{}] Based on '
            'ECMWF Ensemble'.format(self.wind_repr, self.storm.codename))
        self.plot._timestamp('Init Time: {:s}/{:s}/{:s} {:s}Z'.format(
            time[:4], time[4:6], time[6:8], time[8:]))
        self.plot.draw('meripara country province coastline')

    def save(self):
        filename = 'typhoon/ensemble/{}/{}{}.png'.format(self.storm.basetime,
            self.storm.codename, self.wind_repr)
        filepath = os.path.join(settings.MEDIA_ROOT, filename)
        self.plot.save(filepath, facecolor='#F8F8F8')
        logger.info('Export to {}'.format(filepath))
        self.plot.clear()

