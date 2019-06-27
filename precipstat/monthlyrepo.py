import os
import sys
import django

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(root)
sys.path.append(root)
os.environ['DJANGO_SETTINGS_MODULE'] = 'windygram.settings'

django.setup()

import datetime
import decimal
import logging
import json
import matplotlib
import random
matplotlib.use('agg')
import matplotlib.font_manager as mfm
import matplotlib.pyplot as plt
import matplotlib.patches as mpatch
import matplotlib.path as mpath

from django.db.models import Max, Sum

from precipstat.newbot import RichText, TybbsBot
from precipstat.models import DailyStat
from precipstat.pstat import get_mean_value, get_month_percent

logger = logging.getLogger('precipstat.monthlyrepo')
DEBUG = False

fontpath = os.path.join(os.path.dirname(__file__), 'source.ttf')
source_font = mfm.FontProperties(fname=fontpath, size=5)
# matplotlib.rc('font', family='HelveticaNeue')
colormap = plt.get_cmap('gist_ncar')
MAX_PERCENT = 150
DEFAULT_FONT = 'Arial'

YELLOW = '#FFB527'
RED = '#EE174B'
BLUE = '#0075C0'
PURPLE = '#404095'
GREEN = '#487530'
COLORS = [YELLOW, RED, BLUE, PURPLE, GREEN]


class MonthlyUpdate:

    def __init__(self):
        self.data = {
            'month': None, # month num
            'month_days': [], # day list of the month
            'ranking': [], # monthly total and ranking
            'prev': {}, # previous total
            'mean': {}, # mean value of the month
            'daily': {} # daily value
        }
        if DEBUG:
            self.target_thread = 77749
            self.target_forum = 65
        else:
            self.target_thread = 78671
            self.target_forum = 70

    def collect(self, year=None, month=None):
        if year and month:
            self.set_month(year, month)
        else:
            self.set_last_month()
        self.collect_daily()
        self.collect_ranking()
        self.collect_others()
        return self.data

    def set_last_month(self):
        self.day = datetime.date.today() - datetime.timedelta(days=25)
        self._collect_days()

    def set_month(self, year, month):
        self.day = datetime.date(year, month, 15)
        self._collect_days()

    def _collect_days(self):
        self.month = self.day.month
        first_day = self.day.replace(day=1)
        next_month_day = (self.day + datetime.timedelta(days=25)).replace(day=1)
        days_diff = (next_month_day - first_day).days
        self.dayrange = [str(first_day + datetime.timedelta(days=i)) for i in range(days_diff)]
        self.data['month'] = self.month
        self.data['month_days'] = self.dayrange

    def collect_daily(self):
        month_days = len(self.dayrange)
        entries = DailyStat.objects.filter(date__month=self.month, date__year=self.day.year)
        daily_data = self.data['daily']
        for entry in entries:
            if entry.name not in daily_data:
                daily_data[entry.name] = [None for i in range(month_days)]
            day_index = entry.date.day - 1
            daily_data[entry.name][day_index] = str(entry.percip) # Decimal is not JSON-compactible

    def collect_ranking(self):
        entries = DailyStat.objects.filter(date__month=self.month,
            date__year=self.day.year).values('name').annotate(sum=Sum('percip')).order_by('-sum')
        monthly_data = self.data['ranking']
        mean_data = self.data['mean']
        for i, entry in enumerate(entries):
            entry['ranking'] = i + 1
            city_name = entry['name']
            mean_data[city_name] = {
                'mean': get_mean_value(city_name, self.month),
                'percent': get_month_percent(city_name, self.month, entry['sum'])
            }
            entry['sum'] = str(entry['sum'])
            monthly_data.append(entry)

    def collect_others(self):
        if self.month != 1:
            last_day = self.day.replace(day=1) - datetime.timedelta(days=1)
            entries = DailyStat.objects.filter(date__lte=last_day,
                date__year=self.day.year).values('name').annotate(sum=Sum('percip'))
            for entry in entries:
                self.data['prev'][entry['name']] = str(entry['sum'])

    def export_data(self):
        # For test only
        json.dump(self.data, open('monthlyrepo.json', 'w'), ensure_ascii=False)

    def update(self):
        self.bot = TybbsBot()
        self.plot = MonthlyPlot()
        self.real_text = RichText()
        self.plot.set_data(self.collect())
        path = 'precipstat/plots/m{}_{}.png'.format(self.month,
            datetime.date.today().strftime('%Y%m%d'))
        self.plot.plot()
        self.plot.save(path)
        self.bot.load_cookie()
        self.bot.login_with_retry()
        aid, fhash = self.bot.upload_image(self.target_thread, self.target_forum, path)
        self.real_text.insert_image(aid)
        self.bot.reply(self.target_thread, self.target_forum, self.real_text, form_hash=fhash)
        self.bot.write_cookie()


class MonthlyPlot:

    def __init__(self):
        self.figsize = (4.8, 7.5)
        self.fig = plt.figure(figsize=self.figsize)
        self.aspect = self.figsize[1] / self.figsize[0]
        self.ax = plt.axes([0, 0, 1, 1])
        self.dpi = 200
        plt.xlim([0, 1])
        plt.ylim([0, 1])
        self.trans = self.ax.transAxes

    def set_data(self, data):
        self.data = data
        self.month = self.data['month']
        self.month_days = [datetime.datetime.strptime(s, '%Y-%m-%d') for s in self.data['month_days']]
        self.cities = [c['name'] for c in self.data['ranking']]
        self.cities_len = len(self.cities)

    def plot(self):
        self.plot_heading()
        self.plot_table_heading()
        self.plot_table_daily()
        self.plot_table_footer()

    def plot_heading(self):
        ROW_NUM = 3
        ROW_GAP = 0.012
        ROW_LENGTH = 0.007
        COLUMN_GAP = 0.032
        STARTING_X = 0.0
        ymax = 1
        last_color = color = None
        for row in range(ROW_NUM):
            x = STARTING_X
            while x < 1.05:
                while color == last_color:
                    color = random.choice(COLORS)
                last_color = color
                self.ax.axvline(x=x, ymin=ymax - ROW_LENGTH, ymax=ymax,
                    linewidth=6, color=color)
                x += COLUMN_GAP
            ymax -= (ROW_GAP + ROW_LENGTH)
        self.ax.text(0.5, 0.925, '南方省会降雨量统计·月度总结', color='k', transform=self.trans,
            fontproperties=source_font, va='bottom', ha='center', fontsize=6)
        for i in range(12):
            xmin = 0.35 + 0.025 * i
            color = COLORS[i % 5]
            self.ax.axhline(y=0.923, xmin=xmin, xmax=xmin+0.02, linewidth=0.4, color=color)
        self.ax.text(0.5, 0.918, '{}月'.format(self.month), color='k', transform=self.trans,
            fontproperties=source_font, va='top', ha='center', fontsize=6)

    def plot_table_heading(self):
        X = 0.89
        self.ax.text(0.045, X, '前期累计', color=BLUE, transform=self.trans,
            fontproperties=source_font, va='center', ha='center')
        for i, city in enumerate(self.cities):
            if self.month == 1:
                val = '0.0'
            else:
                val = self.data['prev'][city][:-1]
            self.ax.text(0.11+i*0.055, X, val, color='k', transform=self.trans,
                family=DEFAULT_FONT, fontsize=5, va='center', ha='center')
        RoundedRect(0.01, 0.037, 0.98, 0.843, radius=0.035, facecolor='#F3F3F3',
            transform=self.trans, aspect=self.aspect).apply_to(self.ax)

    def plot_table_daily(self):
        DAY_Y = 0.87
        Y_GAP = 0.02
        ys = []
        for d in self.month_days:
            daynum = d.day
            y = DAY_Y - daynum * Y_GAP - min((daynum-1) // 5, 5) * Y_GAP / 2 - \
                min((daynum-1) // 15, 1) * Y_GAP
            ys.append(y)
            self.ax.text(0.055, y, str(daynum), ha='center', family=DEFAULT_FONT, va='center',
                fontsize=5, color=BLUE)
        for i, city in enumerate(self.cities):
            self.ax.text(0.11+i*0.055, DAY_Y, city, color='k', transform=self.trans,
                fontproperties=source_font, va='center', ha='center')
            self.ax.text(0.11+i*0.055, ys[15] + Y_GAP * 1.25, city, color='k', transform=self.trans,
                fontproperties=source_font, va='center', ha='center')
            self.ax.text(0.11+i*0.055, ys[-1] - Y_GAP * 1.25, city, color='k', transform=self.trans,
                fontproperties=source_font, va='center', ha='center')
            for dayidx, val in enumerate(self.data['daily'][city]):
                if val is None:
                    RoundedRect(0.11+i*0.055-0.025, ys[dayidx]-0.007, 0.05, 0.014, radius='full', 
                        transform=self.trans, aspect=self.aspect, facecolor='#444444',
                        text='缺载').apply_to(self.ax, color='w', fontsize=5, 
                        fontproperties=source_font)
                    continue
                val = val[:-1]
                if val == '0.0':
                    continue
                elif decimal.Decimal(val) >= 50:
                    RoundedRect(0.11+i*0.055-0.025, ys[dayidx]-0.007, 0.05, 0.014, radius='full', 
                        transform=self.trans, aspect=self.aspect, facecolor=RED,
                        text=val).apply_to(self.ax, color='w', fontsize=5, family=DEFAULT_FONT)
                else:
                    self.ax.text(0.11+i*0.055, ys[dayidx], val, ha='center', va='center',
                        family=DEFAULT_FONT, fontsize=5, color='#444444')
        Y_LATTER = ys[-1] - Y_GAP * 2.5
        self.ax.text(0.045, Y_LATTER, '本月雨日', color=BLUE, transform=self.trans, va='center',
            ha='center', fontproperties=source_font)
        self.ax.text(0.045, Y_LATTER - Y_GAP, '本月累计', color=BLUE, transform=self.trans,
            va='center', ha='center', fontproperties=source_font)
        self.ax.text(0.045, Y_LATTER - Y_GAP*3, '常年均值', color=BLUE, transform=self.trans,
            va='center', ha='center', fontproperties=source_font)
        self.ax.text(0.045, Y_LATTER - Y_GAP*4, '距平', color=BLUE, transform=self.trans,
            va='center', ha='center', fontproperties=source_font)
        for i, entry in enumerate(self.data['ranking']):
            x = 0.11 + i * 0.055
            city = entry['name']
            rainy_day_count = sum(i != '0.00' and i is not None for i in self.data['daily'][city])
            self.ax.text(x, Y_LATTER , str(rainy_day_count), color='k', transform=self.trans,
                family=DEFAULT_FONT, fontsize=5, va='center', ha='center')
            self.ax.text(x, Y_LATTER - Y_GAP , entry['sum'][:-1], color='k', transform=self.trans,
                family=DEFAULT_FONT, fontsize=5, va='center', ha='center')
            self.ax.text(x, Y_LATTER - Y_GAP*2, str(entry['ranking']), color='w', 
                transform=self.trans, family=DEFAULT_FONT, fontsize=5, va='center', ha='center',
                bbox=dict(boxstyle='circle', fc=PURPLE, ec='none', lw=0.4))
            self.ax.text(x, Y_LATTER - Y_GAP*3, '{:.1f}'.format(self.data['mean'][city]['mean']),
                color='k', transform=self.trans, family=DEFAULT_FONT, fontsize=5, va='center', ha='center')
            percent = self.data['mean'][city]['percent']
            anomaly_color = colormap(1 - min(percent, MAX_PERCENT) / MAX_PERCENT * 0.8)
            length_ratio = min(percent, 100) / 100
            rect_x = x - 0.025
            rect_y = Y_LATTER - Y_GAP*4 - 0.007
            rcolor = RoundedRect(rect_x, rect_y, 0.05*length_ratio, 0.014, radius='full', 
                facecolor=anomaly_color, transform=self.trans, aspect=self.aspect).apply_to(self.ax,
                color='k', family=DEFAULT_FONT, fontsize=5)
            rtext = RoundedRect(rect_x, rect_y, 0.05, 0.014, radius='full', edgecolor='#444444',
                text='{:d}%'.format(round(percent)), transform=self.trans, aspect=self.aspect,
                linewidth=0.1).apply_to(self.ax, color='k', family=DEFAULT_FONT, fontsize=5)
            rcolor.set_clip_path(rtext)

    def plot_table_footer(self):
        for i, entry in enumerate(self.data['ranking']):
            x = 0.11 + i * 0.055
            city = entry['name']
            total = decimal.Decimal(entry['sum'])
            if self.month > 1:
                total += decimal.Decimal(self.data['prev'][city])
            total = str(total)[:-1]
            self.ax.text(x, 0.028, total, ha='center', va='center', color='k', transform=self.trans,
                family=DEFAULT_FONT, fontsize=5)
        self.ax.text(0.045, 0.028, '本年累计', ha='center', va='center', color=BLUE,
            transform=self.trans, fontproperties=source_font)
        self.ax.text(0.5, 0.007, ' '.join('- PRESENTED BY NASDAQ -'), ha='center', va='bottom', 
            color='#444444', transform=self.trans, family=DEFAULT_FONT, fontsize=3)

    def save(self, target):
        self.ax.axis('off')
        #self.fig.tight_layout(pad=0)
        self.fig.savefig(target, dpi=self.dpi, edgecolor='w')


class RoundedRect:

    def __init__(self, llx, lly, width, height, radius=None, text=None, facecolor=None, 
    edgecolor=None, transform=None, aspect=None, **kwargs):
        self.llx = llx
        self.lly = lly
        self.width = width
        self.height = height
        if radius is None:
            self.radius = 0
        elif radius == 'full' or radius > height / 2:
            self.radius = height / 2
        else:
            self.radius = radius
        self.text = text
        self.facecolor = facecolor or 'none'
        self.edgecolor = edgecolor or 'none'
        self.transform = transform
        self.aspect = aspect
        self.kwargs = kwargs

    def to_patch(self):
        x = self.llx
        y = self.lly
        rw = rh = self.radius
        if self.aspect:
            rw = rh * self.aspect
        w = self.width
        h = self.height + 0.002
        self.verts = [
            (x + rw, y),
            (x + w - rw, y),
            (x + w, y),
            (x + w, y + rh),
            (x + w, y + h - rh),
            (x + w, y + h),
            (x + w - rw, y + h),
            (x + rw, y + h),
            (x, y + h),
            (x, y + h - rh),
            (x, y + rh),
            (x, y),
            (x + rw, y)
        ]
        self.codes = [
            mpath.Path.MOVETO,
            mpath.Path.LINETO,
            mpath.Path.CURVE3,
            mpath.Path.CURVE3,
            mpath.Path.LINETO,
            mpath.Path.CURVE3,
            mpath.Path.CURVE3,
            mpath.Path.LINETO,
            mpath.Path.CURVE3,
            mpath.Path.CURVE3,
            mpath.Path.LINETO,
            mpath.Path.CURVE3,
            mpath.Path.CURVE3
        ]
        return mpatch.PathPatch(mpath.Path(self.verts, self.codes), fc=self.facecolor,
            ec=self.edgecolor, transform=self.transform, **self.kwargs)

    def apply_to(self, ax, **text_kwargs):
        patch = self.to_patch()
        ax.add_patch(patch)
        if self.text:
            ax.text(self.llx + self.width / 2, self.lly + self.height / 2, 
                self.text, va='center', ha='center', **text_kwargs)
        return patch


if __name__ == '__main__':
    if DEBUG:
        p = MonthlyPlot()
        data = json.load(open('precipstat/monthlyrepo.json', encoding='utf8'))
        p.set_data(data)
        p.plot()
        p.save('precipstat/test.png')
    else:
        MonthlyUpdate().update()
