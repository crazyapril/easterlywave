import json
from datetime import datetime, timedelta
import os

import matplotlib
matplotlib.use('Agg')

import matplotlib.dates as mdt
import matplotlib.font_manager as mfm
import matplotlib.patches as mpatch
import matplotlib.path as mpath
import matplotlib.pyplot as plt
import matplotlib.transforms as mtrans
import numpy as np
from matplotlib.patheffects import Normal, Stroke

from .cell import Cell, CellRow
from .gpf import pure_cmap
from .weathercode import WeatherCode
from .wxsymbols import wx_symbol_font

STROKE = [Stroke(linewidth=0.8, foreground='w'), Normal()]

ARROW_VER = [(0, -0.1), (0.66, -0.8), (0, 1), (-0.66, -0.8), (0, -0.1)]
ARROW_CODE = [mpath.Path.MOVETO, mpath.Path.LINETO,
              mpath.Path.LINETO, mpath.Path.LINETO, mpath.Path.CLOSEPOLY]
ARROW = mpath.Path(ARROW_VER, ARROW_CODE)

TIMELIMIT = 48 # 6 days, 3 hours interval

temp_cmap = pure_cmap('temp')

fontpath = os.path.join(os.path.dirname(__file__), 'source.ttf')
source_font = mfm.FontProperties(fname=fontpath, size=6)

matplotlib.rc('font', family='HelveticaNeue', size=6)


def arrow_factory(x_center=0, y_center=0, size=1, bgcolor='w', edgecolor='w',
                  alpha=1, angle=0, transform=None, figsize=None):
    width, height = figsize
    _arrow = ARROW.transformed(mtrans.Affine2D().rotate(angle))
    _arrow.vertices[:,0] = _arrow.vertices[:,0] * size * 0.8 * height / width  + x_center
    _arrow.vertices[:,1] = _arrow.vertices[:,1] * size * 0.8 + y_center
    patch = mpatch.PathPatch(_arrow, clip_on=False, ec=edgecolor, fc=bgcolor,
                             alpha=alpha, transform=transform, linewidth=0.2)
    return patch


class Windygram:

    def __init__(self):
        self.name = None

    def set_location(self, lon, lat, altitude=None):
        self.lon = lon
        self.lat = lat
        self.altitude = altitude

    def set_data_location(self):
        self.lon = self.d_detail['header']['origLon']
        self.lat = self.d_detail['header']['origLat']
        self.altitude = self.d_detail['header']['origElevation']

    def set_name(self, name, sname=None):
        self.name = name
        self.sname = sname

    def set_data(self, d_detail, d_meteo):
        self.d_detail = d_detail
        self.d_meteo = d_meteo

    def set_time(self):
        utc_offset = timedelta(hours=self.d_detail['header']['utcOffset'])
        self._times = [datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S') -
            utc_offset for ts in self.d_detail['data']['origDate']]
        self.times = self._times[:TIMELIMIT]
        self.basetime = datetime.strptime(
            self.d_detail['header']['refTime'], '%Y-%m-%dT%H:%M:%SZ')
        self.endtime = self.times[-1] + timedelta(hours=3)
        self.is12z = self.basetime.hour == 12
        self.unit = 1 / (len(self.times) + 1)

    def plot_and_save(self, target):
        self.init_plot()
        self.plot_perci()
        self.plot_temp()
        self.plot_daily()
        self.plot_extended()
        self.plot_wind()
        self.plot_rh()
        self.plot_sounding()
        self.plot_weathercode()
        self.plot_name()
        self.save_plot(target)

    def init_plot(self, figsize=None, dpi=None):
        if figsize is None:
            figsize = (8, 3)
        if dpi is None:
            dpi = 250
        self.dpi = dpi
        self.fig = plt.figure(figsize=figsize)
        self.ax = plt.gca()
        self.ax.xaxis.set_major_locator(mdt.HourLocator(byhour=(0,12)))
        self.ax.xaxis.set_major_formatter(mdt.DateFormatter('%d/%HZ'))
        self.ax.xaxis.set_minor_locator(mdt.HourLocator(interval=3))
        self.ax.xaxis.set_minor_formatter(mdt.DateFormatter(''))
        self.init_grid()

    def init_grid(self):
        plt.grid(True, axis='x', which='both', color='#666666', linestyle=':', linewidth=0.1)
        HALF_DAY = timedelta(hours = 12)
        # Color nights
        if self.times[0].hour > 12:
            first_time = self.times[0].replace(hour=12)
            self.ax.axvspan(first_time, first_time + HALF_DAY, alpha=0.5, color='#E5E5E5', zorder=0)
        for i, t in enumerate(self.times):
            if t.hour == 12:
                self.ax.axvspan(t, t + HALF_DAY, alpha=0.5, color='#E5E5E5', zorder=0)

    def _iter(self):
        return range(len(self.times))

    def _iterday(self):
        start_day = self.basetime.replace(hour=0)
        for i in range(5):
            start_day += timedelta(days=1)
            yield start_day

    def _plot_cellrow(self, cellrow):
        for cell in cellrow.cells:
            rect = mpatch.Rectangle(xy=(cell.x, cell.y),
                                    width=cell.width,
                                    height=cell.height,
                                    transform=self.ax.transAxes,
                                    ec='k',
                                    fc=cell.bgcolor,
                                    lw=0.1,
                                    clip_on=False,
                                    alpha=cell.alpha)
            self.ax.add_patch(rect)
            self.ax.text(cell.x + cell.width / 2,
                         cell.y + cell.height / 2,
                         cell.text,
                         size=4,
                         transform=self.ax.transAxes,
                         ha='center',
                         va='center')
        self.ax.text(-0.01,
                     cellrow.y + cellrow.height / 2,
                     cellrow.name,
                     size=4,
                     #family='Source Han Sans CN',
                     transform=self.ax.transAxes,
                     ha='right',
                     va='center')

    def _plot_vector_row(self, cellrow):
        for cell in cellrow.cells:
            rect = mpatch.Rectangle(xy=(cell.x, cell.y),
                                    width=cell.width,
                                    height=cell.height,
                                    transform=self.ax.transAxes,
                                    ec='k',
                                    fc=cell.bgcolor,
                                    lw=0.1,
                                    clip_on=False,
                                    alpha=1)
            self.ax.add_patch(rect)
            if cell.alpha > 0.75:
                edgecolor = 'w'
                bgcolor = 'w'
            elif cell.alpha > 0.5:
                edgecolor = 'w'
                bgcolor = cell.bgcolor
            elif cell.alpha > 0.25:
                edgecolor = '#8888C8'
                bgcolor = cell.bgcolor
            else:
                edgecolor = '#8888C8'
                bgcolor = '#8888C8'
            arrow = arrow_factory(x_center=cell.x + cell.width / 2,
                                  y_center=cell.y + cell.height / 2,
                                  size=cell.width,
                                  edgecolor=edgecolor,
                                  bgcolor=bgcolor,
                                  angle=cell.theta,
                                  transform=self.ax.transAxes,
                                  figsize=tuple(self.fig.get_size_inches()))
            self.ax.add_patch(arrow)
        self.ax.text(-0.01,
                     cellrow.y + cellrow.height / 2,
                     cellrow.name,
                     size=4,
                     #family='Source Han Sans CN',
                     transform=self.ax.transAxes,
                     ha='right',
                     va='center')

    def _plot_coderow(self, cellrow):
        for cell in cellrow.cells:
            if cell.time.hour >= 12:
                bgcolor = '#E5E5E5'
            else:
                bgcolor = 'w'
            rect = mpatch.Rectangle(xy=(cell.x, cell.y),
                                    width=cell.width,
                                    height=cell.height,
                                    transform=self.ax.transAxes,
                                    ec='k',
                                    fc=bgcolor,
                                    lw=0.1,
                                    clip_on=False,
                                    alpha=1)
            self.ax.add_patch(rect)
            if cell.code.char is None:
                continue
            self.ax.text(cell.x + cell.width / 2,
                         cell.y + cell.height / 2,
                         cell.code.char,
                         ha='center',
                         va='center',
                         fontproperties=wx_symbol_font,
                         size=cell.code.size,
                         color=cell.code.color,
                         transform=self.ax.transAxes)
        self.ax.text(-0.01,
                     cellrow.y + cellrow.height / 2,
                     cellrow.name,
                     size=4,
                     #family='Source Han Sans CN',
                     transform=self.ax.transAxes,
                     ha='right',
                     va='center')

    def plot_temp(self):
        TEMP = np.round(np.array(self.d_detail['data']['temp']) - 273.1, 1)
        TEMP_COLOR = '#5524C0'
        TEMP_MARKER = 'o'
        self.temp = TEMP
        # Plot
        TEMP = TEMP[:TIMELIMIT]
        self.ax2 = self.ax.twinx()
        self.ax2.plot_date(self.times,
                          TEMP,
                          linestyle='-',
                          linewidth=0.4,
                          marker=TEMP_MARKER,
                          color=TEMP_COLOR,
                          mec=TEMP_COLOR,
                          markersize=2,
                          zorder=5)
        for i in self._iter():
            self.ax2.annotate(str(TEMP[i]),
                             xy=(self.times[i], TEMP[i]),
                             xytext=(0, 7),
                             textcoords='offset pixels',
                             fontsize=5,
                             ha='center',
                             va='bottom',
                             color=TEMP_COLOR,
                             path_effects=STROKE)
        # Change Y-axis range
        pad = 1
        ymax = np.ceil((np.amax(TEMP) + pad) / 5) * 5
        ymin = np.floor((np.amin(TEMP) - pad) / 5) * 5 - 10
        self.ax2.set_ylim([ymin, ymax])
        self.ax2.set_yticks(np.arange(ymin + 10, ymax + 1, 5))
        self.ax2.set_ylabel('TEMPERATURE / degC')

    def plot_perci(self):
        PERCI = np.array(self.d_detail['data']['mm'])
        SNOWP = np.array(self.d_detail['data']['snowPrecip'])
        RAINFLAG = np.array(self.d_detail['data']['rain'])
        SNOWFLAG = np.array(self.d_detail['data']['snow'])
        RAIN_COLOR = '#55CC33'
        SNOW_COLOR = '#AAAAAA'
        ICE_COLOR = '#FF9090'
        BAR_WIDTH = 0.11
        TEXT_OFFSET = 18
        # Adjust percip data1
        _snowp = np.round(SNOWP * SNOWFLAG, 1)
        _icep = np.round(SNOWP * (1 - SNOWFLAG), 1)
        ADJ_PERCI = np.amax((SNOWP, PERCI), axis=0)
        _rainp = np.round(ADJ_PERCI - _snowp - _icep, 1)
        self.percip = np.round(ADJ_PERCI, 1) # Save it!
        # plot
        self.ax.bar(self.times, _rainp[:TIMELIMIT], -BAR_WIDTH, color=RAIN_COLOR,
            zorder=2, align='edge')
        self.ax.bar(self.times, _icep[:TIMELIMIT], -BAR_WIDTH, color=ICE_COLOR,
            bottom=_rainp[:TIMELIMIT], zorder=2, align='edge')
        self.ax.bar(self.times, _snowp[:TIMELIMIT], -BAR_WIDTH, color=SNOW_COLOR,
            bottom=(_rainp+_icep)[:TIMELIMIT], zorder=2, align='edge')
        OFFSET = timedelta(hours=BAR_WIDTH/2*24)
        for i in self._iter():
            rain = str(_rainp[i]) if _rainp[i] else ''
            base_pixel = -TEXT_OFFSET
            if rain:
                base_pixel += TEXT_OFFSET
                self.ax.annotate(rain,
                                 xy=(self.times[i] - OFFSET, ADJ_PERCI[i]),
                                 xytext=(0, base_pixel),
                                 textcoords='offset pixels',
                                 fontsize=5,
                                 ha='center',
                                 va='bottom',
                                 color=RAIN_COLOR,
                                 path_effects=STROKE)
            ice = str(_icep[i]) if _icep[i] else ''
            if ice:
                base_pixel += TEXT_OFFSET
                self.ax.annotate(ice,
                                 xy=(self.times[i] - OFFSET, ADJ_PERCI[i]),
                                 xytext=(0, base_pixel),
                                 textcoords='offset pixels',
                                 fontsize=5,
                                 ha='center',
                                 va='bottom',
                                 color=ICE_COLOR,
                                 path_effects=STROKE)
            snow = str(_snowp[i]) if _snowp[i] else ''
            if snow:
                base_pixel += TEXT_OFFSET
                self.ax.annotate(snow,
                                 xy=(self.times[i] - OFFSET, ADJ_PERCI[i]),
                                 xytext=(0, base_pixel),
                                 textcoords='offset pixels',
                                 fontsize=5,
                                 ha='center',
                                 va='bottom',
                                 color=SNOW_COLOR,
                                 path_effects=STROKE)
        # Change Y-axis range
        ymax = np.ceil(np.amax(ADJ_PERCI) / 2) * 3
        if ymax < 3:
            ymax = 3
        self.ax.set_ylim([0, ymax])
        self.ax.set_yticks(np.linspace(0, ymax / 3 * 2, 5))
        self.ax.set_ylabel('PRECIPITATION / mm')

    def plot_daily(self):
        plt.sca(self.ax)
        ybase = -0.135
        maxrow = CellRow(y=ybase, name='MAX TEMP', width=self.unit, height=self.unit * 2)
        minrow = CellRow(y=ybase-self.unit*2, name='MIN TEMP', width=self.unit, height=self.unit*2)
        pcprow = CellRow(y=ybase-self.unit*4, name='PRECIP', width=self.unit, height=self.unit*2)
        previous_day = ''
        day_counts_total = 0
        for day in self.d_detail['data']['day']:
            if day == previous_day or day_counts_total == TIMELIMIT:
                continue
            day_count = self.d_detail['data']['day'].count(day)
            if day_counts_total + day_count > TIMELIMIT:
                span = TIMELIMIT - day_counts_total
            else:
                span = day_count
            c_max = Cell(span=span)
            max_temp = round(np.amax(self.temp[day_counts_total:day_counts_total+span]), 1)
            c_max.set_text(str(max_temp))
            c_max.set_bgcolor(temp_cmap((max_temp + 45) / 90))
            maxrow.add_cell(c_max)
            #
            c_min = c_max.copy()
            min_temp = round(np.amin(self.temp[day_counts_total:day_counts_total+span]), 1)
            c_min.set_text(str(min_temp))
            c_min.set_bgcolor(temp_cmap((min_temp + 45) / 90))
            minrow.add_cell(c_min)
            #
            c_pcp = c_max.copy()
            daily_pcp = round(np.sum(self.percip[day_counts_total:day_counts_total+span]), 1)
            c_pcp.set_text(str(daily_pcp))
            c_pcp.set_bgcolor('w')
            pcprow.add_cell(c_pcp)
            #
            previous_day = day
            day_counts_total += span
        self._plot_cellrow(maxrow)
        self._plot_cellrow(minrow)
        self._plot_cellrow(pcprow)

    def plot_extended(self):
        ybase = -0.27
        unit = 1 / 16 * (47 / 48)
        dayrow = CellRow(y=ybase, width=unit, height=self.unit * 2, name='10-DAY\nEXTENDED')
        temprow = CellRow(y=ybase-self.unit*2, width=unit, height=self.unit*2, name='TEMP')
        perciprow = CellRow(y=ybase-self.unit*4, width=unit, height=self.unit*2, name='PRECIP')
        weatherrow = CellRow(y=ybase-self.unit*6, width=unit, height=self.unit*2, name='WEATHER')
        previous_day = 0
        for i in range(TIMELIMIT, len(self._times)):
            if self._times[i].day != previous_day:
                daystr = self._times[i].strftime('%d/%HZ')
            else:
                daystr = self._times[i].strftime('%HZ')
            c = Cell()
            c.set_text(daystr)
            c.set_bgcolor('w')
            dayrow.add_cell(c)
            previous_day = self._times[i].day
            c_temp = Cell()
            c_temp.set_text(str(self.temp[i]))
            c_temp.set_bgcolor(temp_cmap((self.temp[i] + 45) / 90))
            temprow.add_cell(c_temp)
            c_percip = Cell()
            c_percip.set_text(str(self.percip[i]))
            c_percip.set_bgcolor('w')
            perciprow.add_cell(c_percip)
            c_weather = Cell()
            code = WeatherCode()
            code.set_code(self.d_detail['data']['weathercode'][i])
            c_weather.set_code(code)
            c_weather.set_time(self._times[i])
            c_weather.set_bgcolor('w')
            weatherrow.add_cell(c_weather)
        self._plot_cellrow(dayrow)
        self._plot_cellrow(temprow)
        self._plot_cellrow(perciprow)
        self._plot_coderow(weatherrow)

    def plot_wind(self):
        WIND = np.array(self.d_detail['data']['wind']) * 1.94
        WINDDIR = np.array(self.d_detail['data']['windDir'])
        WIND_COLOR = '#59AA00'
        #
        X = np.arange(self.unit, 1, self.unit)
        Y = np.ones(X.shape)
        U = (- WIND * np.sin(WINDDIR / 180 * np.pi))[:TIMELIMIT]
        V = (- WIND * np.cos(WINDDIR / 180 * np.pi))[:TIMELIMIT]
        self.ax.barbs(X, Y, U, V, color=WIND_COLOR, transform=self.ax.transAxes, linewidth=0.2, length=4, clip_on=False)

    def plot_rh(self):
        RH = np.round(np.array(self.d_detail['data']['rh']), 1)
        #
        ybase = 1.06
        gist_cmap = plt.get_cmap('terrain')
        rhrow = CellRow(x=self.unit/2, y=ybase, name='RH', width=self.unit, height=self.unit * 2)
        for i in self._iter():
            c = Cell()
            c.set_text(str(RH[i]))
            c.set_bgcolor(gist_cmap(1 - RH[i] / 100))
            c.set_alpha(0.6)
            rhrow.add_cell(c)
        self._plot_cellrow(rhrow)

    def plot_sounding(self):
        LEVELS = ['1000h', '925h', '850h', '700h']
        #
        cellrows = []
        ybase = 1.18
        for level in LEVELS:
            cellrow = CellRow(x=self.unit/2, y=ybase, name=level, width=self.unit, height=self.unit*2)
            TEMP = self.d_meteo['data']['temp-'+level]
            RH = self.d_meteo['data']['rh-'+level]
            U = self.d_meteo['data']['wind_u-'+level]
            V = self.d_meteo['data']['wind_v-'+level]
            for i in self._iter():
                temp = TEMP[i] - 273.15
                rh = RH[i]
                c = Cell()
                c.set_theta(- np.arctan2(U[i], V[i]))
                c.set_bgcolor(temp_cmap((temp + 45) / 90))
                c.set_alpha(rh / 100)
                cellrow.add_cell(c)
            cellrows.append(cellrow)
            ybase += self.unit * 2
        for cellrow in cellrows:
            self._plot_vector_row(cellrow)

    def plot_weathercode(self):
        CODES = self.d_detail['data']['weathercode']
        #
        ybase = 1.12
        coderow = CellRow(x=self.unit/2, y=ybase, name='WEATHER', width=self.unit, height=self.unit*2)
        for i in self._iter():
            code = CODES[i]
            c = Cell()
            weather_code = WeatherCode()
            weather_code.set_code(code)
            c.set_code(weather_code)
            c.set_time(self.times[i])
            coderow.add_cell(c)
        self._plot_coderow(coderow)

    def plot_name(self):
        text_kwargs = {'va':'bottom', 'color':'#666666', 'size':6, 'transform':self.ax.transAxes}
        ybase = 1.37
        self.ax.text(self.unit, ybase+self.unit*2, 'WINDYGRAM', ha='left', **text_kwargs)
        self.ax.text(self.unit, ybase, 'ECMWF HRES 13km', ha='left', **text_kwargs)
        lat_char = 'N' if self.lat >= 0 else 'S'
        lon_char = 'E' if self.lon >= 0 else 'W'
        position_str = '{}{} {}{} {}m'.format(abs(self.lat), lat_char, abs(self.lon), lon_char, self.altitude)
        self.ax.text(0.5, ybase, position_str, ha='center', **text_kwargs)
        self.ax.text(1-self.unit, ybase, self.basetime.strftime('%Y/%m/%d %HZ'), ha='right', **text_kwargs)
        self.ax.text(1-self.unit, ybase+self.unit*2, '@NASDAQ', ha='right', **text_kwargs)
        if self.name:
            text_kwargs.update(color='k', fontproperties=source_font)
            self.ax.text(0.5, ybase+self.unit*2, self.name, ha='center', **text_kwargs)

    def save_plot(self, target):
        THREEHOUR = timedelta(hours=3)
        self.ax.xaxis.set_major_locator(mdt.HourLocator(byhour=(0, 12)))
        self.ax.xaxis.set_major_formatter(mdt.DateFormatter('%d/%HZ'))
        plt.xlim([self.times[0] - THREEHOUR, self.times[-1] + THREEHOUR])
        self.filename = target
        self.fig.savefig(target, dpi=self.dpi, bbox_inches='tight', edgecolor='none',
                         pad_inches=0.05)
