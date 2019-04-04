import datetime
import logging
import os

from django.conf import settings

from tools.mapstore import MapArea, tropical_mapkeys
from tools.metplot.plotplus import Plot
from tools.typhoon import get_sshws_category

logger = logging.getLogger(__name__)


_intensity_colors = {
    'DB': '#AAAAAA',
    'TD': '#0060FF',
    'TS': '#4060CC',
    'C1': '#FFFF65',
    'C2': '#FFC80F',
    'C3': '#F08066',
    'C4': '#E50060',
    'C5': '#A048A0',
    'EX': '#B4E61E',
    'SD': '#B4E61E',
    'SS': '#B4E61E',
}

def _get_color(c, w):
    if c == 'TY' or c == 'ST':
        c = get_sshws_category(w)
    return _intensity_colors.get(c, '#AAAAAA')

class PlotTrackRoutine:

    def __init__(self, sector):
        self.sector = sector

    def run(self):
        nowtime = datetime.datetime.utcnow().strftime('%Y/%m/%d %HZ')
        for mapkey in tropical_mapkeys:
            logger.info('Make sector map for {}...'.format(mapkey))
            p = Plot(figsize=(5, 2.5), inside_axis=True)
            mapset = MapArea.get(mapkey).load()
            p.usemapset(mapset)
            p.style('bom')
            p.draw('coastline')
            p.drawparameri(lw=0.3)
            for storm in self.sector.storms.values():
                try:
                    self.plot_single(p, storm)
                except Exception as exp:
                    logger.exception('Fatal error.')
            p.ax.text(0.99, 0.98, 'Last Updated: '+nowtime, ha='right',
                va='top', fontsize=5, family=p.family, transform=p.ax.transAxes,
                color=p.style_colors[2])
            output_path = os.path.join(settings.MEDIA_ROOT, 'latest/typhoon',
                'sector_{}.png'.format(mapkey))
            p.save(output_path)
            p.clear()
            logger.info('Sector map outputed to {}.'.format(output_path))

    def plot_single(self, p, storm):
        data = storm.bdeck
        if len(data) == 0:
            return
        for p1, p2 in zip(data[:-1], data[1:]):
            color = _get_color(p1['category'], p1['wind'])
            p.plot([p1['lon'], p2['lon']], [p1['lat'], p2['lat']],
                color=color, lw=0.8)
        text = '{}.{}\n{}kt'.format(storm.code, storm.name, storm.wind)
        p.marktext(storm.lon, storm.lat, text, textpos='left', fontsize=6)

