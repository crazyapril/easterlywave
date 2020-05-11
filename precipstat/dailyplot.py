import os

import matplotlib
matplotlib.use('agg')
import matplotlib.font_manager as mfm
import matplotlib.pyplot as plt
import matplotlib.patches as mpatch
from mpl_toolkits.axes_grid1.anchored_artists import AnchoredDrawingArea

fontpath = os.path.join(os.path.dirname(__file__), 'source.ttf')
source_font = mfm.FontProperties(fname=fontpath, size=6)
matplotlib.rc('font', family='HelveticaNeue')
colormap = plt.get_cmap('gist_ncar')

def custom(d, **kwargs):
    new_dict = dict()
    new_dict.update(d)
    new_dict.update(kwargs)
    return new_dict

YELLOW = '#FFB527'
RED = '#EE174B'
BLUE = '#0075C0'
PURPLE = '#404095'
GREEN = '#487530'

class DailyPlot:

    def __init__(self):
        self.fig = plt.figure(figsize=(4.5,3))
        self.ax = plt.axes([0, 0, 1, 1])
        self.dpi = 200
        plt.xlim([0, 1])
        plt.ylim([0, 1])
        self.trans = self.ax.transAxes
        self.textp = {
            'transform': self.trans,
            'ha': 'left',
            'va': 'bottom',
            'fontproperties': source_font
        }
        self.colors = [YELLOW, RED, BLUE, PURPLE, GREEN]
        self.today_list = []
        self.record_list = []
        self.month_list = []
        #for i in range(16):
        #    self.month_list.append(('广州', i*150/15, i*150/15))
        self.annual_list = []

    def set_date(self, date):
        self.date = date

    def set_title(self, title):
        self.title = title

    def plot_title(self):
        TITLE_Y_BOTTOM = 0.94
        TITLE_X_LEFT = 0.02
        self.ax.text(TITLE_X_LEFT+0.17, TITLE_Y_BOTTOM,
                     self.date.strftime('%Y{}%m{}%d{}').format(*'年月日'), color='#777777',
                     **self.textp)
        self.ax.text(TITLE_X_LEFT, TITLE_Y_BOTTOM, self.title, color='#222222', **self.textp)

    def plot_ribbon(self):
        for i, color in enumerate(self.colors):
            self.ax.axhline(y=0.93, xmin=i*0.2, xmax=i+0.2+0.2, linewidth=2, color=color)

    def plot_list_bg(self):
        self.ax.axhspan(0, 0.6, facecolor='#F3F3F3')

    def get_rained_cities(self):
        return [c[0] for c in self.today_list]

    def plot_month_list(self):
        FIRST_LINE_MAX_PERCENT = 100
        SECOND_LINE_MAX_PERCENT = 200
        COLOR_REP_MAX_PERCENT = 150
        self.ax.axvline(x=0.05, ymin=0.54, ymax=0.56, linewidth=4, color=YELLOW)
        self.ax.text(0.065, 0.548, '{:02d}月累计雨量'.format(self.date.month), color='#444444',
                     size=5, **custom(self.textp, va='center'))
        listlen = len(self.month_list)
        Y = 0.45
        rained_cities = self.get_rained_cities()
        for i, c in enumerate(self.month_list):
            city, percip, percent = c
            city_color = BLUE if city in rained_cities else '#606060'
            if i < listlen // 2:
                x = 0.12
                y = Y - 0.05 * i
            else:
                x = 0.3
                y = Y - 0.05 * (i - listlen // 2)
            self.ax.text(x, y, city, color=city_color, size=5, **custom(self.textp, ha='right'))
            self.ax.text(x+0.1, y+0.001, '{:.1f}mm'.format(percip), color='#606060', size=6,
                         ha='right', va='bottom')
            # color
            _perc = min(percent, COLOR_REP_MAX_PERCENT)
            line_color = colormap(1 - _perc / COLOR_REP_MAX_PERCENT * 0.8)
            # first line
            x_base = x - 0.035
            _perc = min(percent, FIRST_LINE_MAX_PERCENT)
            x_extent = 0.135 * _perc / FIRST_LINE_MAX_PERCENT
            self.ax.axhline(y=y-0.002, xmin=x_base, xmax=x_base+x_extent, linewidth=0.6, color=line_color)
            if percent <= FIRST_LINE_MAX_PERCENT:
                continue
            # second line
            _perc = min(percent, SECOND_LINE_MAX_PERCENT)
            x_extent = 0.135 * (_perc - FIRST_LINE_MAX_PERCENT) / \
                (SECOND_LINE_MAX_PERCENT - FIRST_LINE_MAX_PERCENT)
            self.ax.axhline(y=y-0.008, xmin=x_base, xmax=x_base+x_extent, linewidth=0.6, color=line_color)

    def plot_annual_list(self):
        self.ax.axvline(x=0.55, ymin=0.54, ymax=0.56, linewidth=4, color=PURPLE)
        self.ax.text(0.565, 0.548, '本年累计雨量', color='#444444', size=5,
                     **custom(self.textp, va='center'))
        listlen = len(self.annual_list)
        Y = 0.45
        rained_cities = self.get_rained_cities()
        for i, c in enumerate(self.annual_list):
            city, percip = c
            city_color = BLUE if city in rained_cities else '#606060'
            if i < listlen // 2:
                x = 0.62
                y = Y - 0.05 * i
            else:
                x = 0.8
                y = Y - 0.05 * (i - listlen // 2)
            self.ax.text(x, y, city, color=city_color, size=5, **custom(self.textp, ha='right'))
            self.ax.text(x+0.1, y+0.001, '{:.1f}mm'.format(percip), color='#606060', size=6,
                         ha='right', va='bottom')

    def plot_record(self):
        Y_BOTTOM = 0.6
        listlen = len(self.record_list)
        if listlen == 1:
            rows = 1
            rowgap = 0.04
        elif listlen in (2, 4):
            rows = 2
            rowgap = 0.03
        else:
            rows = 3
            rowgap = 0.02
        for i, r in enumerate(self.record_list):
            if i < rows:
                row = i
                self.ax.text(0.04, Y_BOTTOM + rowgap * (i+1), r, color=RED, **custom(self.textp, size=4))
            else:
                self.ax.text(0.54, Y_BOTTOM + rowgap * (i+1-rows), r, color=RED, **custom(self.textp, size=4))

    def plot_today(self):
        listlen = len(self.today_list)
        if listlen <= 5:
            rows = 1
            Y1 = 0.8
        elif listlen <= 10:
            rows = 2
            Y1 = 0.82
            Y2 = 0.75
        else:
            rows = 3
            Y1 = 0.84
            Y2 = 0.78
            Y3 = 0.72
        for i, c in enumerate(self.today_list):
            city, percip = c
            color = RED if percip >= 50 else '#555555'
            if i < 5:
                x = 0.1 + 0.18 * i
                y = Y1
            elif i < 10:
                x = 0.1 + 0.18 * (i - 5)
                y = Y2
            else:
                x = 0.1 + 0.18 * (i - 10)
                y = Y3
            self.ax.text(x, y, city, color=color, **custom(self.textp, ha='right'))
            self.ax.text(x+0.01, y+0.0025, '{:.1f}mm'.format(percip), color=color, ha='left',
                         va='bottom', size=6)

    def plot_footer(self):
        text = 'PRESENTED BY NASDAQ\n数据收集自国际交换站报文，时间范围为08-08，每日10点更新，蓝色代表本日有降水量更新'
        self.ax.text(0.98, 0.05, text, color='#AAAAAA', ha='right', va='top',
                     fontproperties=source_font, size=4)

    def plot_dots(self):
        ada = AnchoredDrawingArea(10, 5, 0, 0, loc='lower left', pad=0., frameon=False)
        for i, c in enumerate(self.colors):
            dot = mpatch.Circle((5+4.5*i, 2), radius=1.5, facecolor=c, edgecolor='none')
            ada.drawing_area.add_artist(dot)
        self.ax.add_artist(ada)

    def plot(self):
        self.plot_title()
        self.plot_ribbon()
        self.plot_list_bg()
        self.plot_month_list()
        self.plot_annual_list()
        self.plot_record()
        self.plot_today()
        self.plot_footer()
        self.plot_dots()

    def add_today_info(self, city, percip):
        self.today_list.append((city, percip))

    def add_record_text(self, text):
        self.record_list.append(text)

    def add_month_info(self, city, percip, percent):
        self.month_list.append((city, percip, percent))

    def add_annual_info(self, city, percip):
        self.annual_list.append((city, percip))

    def save(self, target):
        self.ax.axis('off')
        #self.fig.tight_layout(pad=0)
        self.fig.savefig(target, dpi=self.dpi, edgecolor='w')
