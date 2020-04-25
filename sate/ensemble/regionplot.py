import logging
import matplotlib
matplotlib.use('agg')
import os

from django.conf import settings
from matplotlib.lines import Line2D

from tools.mapstore import MapArea, tropical_mapkeys
from tools.metplot.plotplus import Plot


logger = logging.getLogger(__name__)


class RegionPlot:

    def __init__(self, basetime, storms):
        self.basetime = basetime
        self.storms = storms
        for storm in self.storms:
            storm.load(qc_method='breakpoint')

    def plot_all(self):
        mapkeys = tropical_mapkeys + ['eastasia'] # Add east asia region
        for basin in mapkeys:
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
                for i in range(len(storm.lats)):
                    p.plot(storm.lons[i], storm.lats[i], marker='None',
                        color='#444444', lw=0.3)
                    colors = [getcolor(pres)[1] for pres in storm.pres[i]]
                    p.scatter(storm.lons[i], storm.lats[i], s=6, facecolors='none',
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
