import logging
import os
import shutil

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from django.conf import settings
from mpl_toolkits.basemap import Basemap

from sate.colormap import get_colormap
from sate.format import HimawariFormat
from sate.satefile import SateFile

matplotlib.use('agg')
matplotlib.rc('font', family='HelveticaNeue')
logger = logging.getLogger(__name__)


class SateImage:

    def __init__(self, satefile:SateFile):
        self.satefile = satefile
        self.figwidth = 1025
        self.figheight = 1000
        self.figaspect = self.figwidth / self.figheight
        self.dpi = 200
        self.bgcolor = '#121212'
        time = self.satefile.time
        self.output_path = os.path.join(settings.MEDIA_ROOT, 'sate/{}/B{}{{enh}}/{}.png'.format(
            time.strftime('%Y%m%d'), self.satefile.band, time.strftime('%H%M')))

    def load_colormap(self, name):
        return get_colormap(name)
        
    def imager(self):
        # Check if hsd file is successfully downloaded, if not, quit
        if os.path.getsize(self.satefile.target_path) < 100:
            logger.warning('Empty file: {}'.format(self.satefile.target_path))
            return
        # Extract data and coordinates
        hf = HimawariFormat(self.satefile.target_path)
        data = hf.extract()
        lons, lats = hf.get_geocoord()
        georange = lats.min(), lats.max(), lons.min(), lons.max()
        # Align images to center on 1020 x 1000 canvas
        imaspect = (georange[3] - georange[2]) / (georange[1] - georange[0])
        if imaspect > self.figaspect:
            # Image is wider than canvas, pad upper and lower edges
            lon1 = georange[2]
            lon2 = georange[3]
            lmid = (georange[0] + georange[1]) / 2
            ldelta = (lon2 - lon1) / self.figaspect
            lat1 = lmid - ldelta / 2
            lat2 = lmid + ldelta / 2
        else:
            # Image is taller than canvas, pad left and right edges
            lat1 = georange[0]
            lat2 = georange[1]
            lmid = (georange[2] + georange[3]) / 2
            ldelta = (lat2 - lat1) * self.figaspect
            lon1 = lmid - ldelta / 2
            lon2 = lmid + ldelta / 2
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
        for enh in enhances:
            fig = plt.figure(figsize=(self.figwidth / self.dpi, self.figheight / self.dpi))
            ax = fig.add_axes([0, 0, 1, 1])
            if band <= 3:
                data = np.sqrt(np.clip(data, 0, 1))
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
            _map.pcolormesh(lons, lats, data, latlon=True, cmap=cmap, vmin=vmin, vmax=vmax)
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
            enh_str = '' if enh is None else enh
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
            output_image_path = self.output_path.format(enh=enh_str)
            os.makedirs(os.path.dirname(output_image_path), exist_ok=True)
            plt.savefig(output_image_path, dpi=self.dpi, facecolor=self.bgcolor)
            plt.clf()
            plt.close()
            # copy to latest dir
            latest_image_path = os.path.join(settings.MEDIA_ROOT, 'latest/sate/b{}{}.png'.format(
                band, enh_str))
            shutil.copyfile(output_image_path, latest_image_path)
