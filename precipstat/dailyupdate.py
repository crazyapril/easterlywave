import logging
import os
import random
import shutil
import sys
from datetime import date, datetime, timedelta

import django

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(root)
sys.path.append(root)
os.environ['DJANGO_SETTINGS_MODULE'] = 'windygram.settings'

django.setup()

from django.conf import settings
from django.db.models import Max, Sum

from precipstat.dailyplot import DailyPlot
from precipstat.models import AnnualStat, DailyStat
from precipstat.newbot import RichText, TybbsBot
from precipstat.psche import search_missing_list, update_today
from precipstat.pstat import get_month_percent


logger = logging.getLogger('precipstat.dailyupdate')
DEBUG = False

class DailyUpdate:

    def __init__(self, dry=False, testdate=None):
        self.bot = TybbsBot()
        self.plot = DailyPlot()
        if DEBUG:
            self.target_thread = 77749
            self.target_forum = 65
        else:
            self.target_thread = 78671
            self.target_forum = 70
        if testdate:
            self.today = testdate
        else:
            self.today = date.today() - timedelta(days=1)
        self.test = bool(testdate)
        self.dry = dry
        if dry:
            logger.warn('Dry TURNED ON')

    def auto(self):
        try:
            self.get_record()
            if not self.test:
                self.update_data()
            self.prepare_data()
            self.prepare_text()
            self.write_today()
            self.write_record()
            self.write_month()
            self.write_annual()
            self.send()
        except Exception as exp:
            logger.exception('Unknown error happened.')

    def set_date(self, _date):
        logger.info('Using date {}'.format(_date.strftime('%Y/%m/%d')))
        self.today = _date

    def get_record(self):
        current_year_records = DailyStat.objects.filter(date__year=self.today.year,
            date__lt=self.today)
        qs_record = current_year_records.values('name').annotate(max=Max('percip'))
        self.record_data = {}
        for query in qs_record:
            name = query['name']
            record = query['max']
            date = current_year_records.filter(name=name, percip=record).latest().date
            self.record_data[name] = {'date': date, 'record': record}

    def update_data(self):
        logger.info("Begin daily update! Now time: {}".format(datetime.now()))
        update_today()
        search_missing_list()
        logger.info("End daily update! Now: {}".format(datetime.now()))

    def prepare_data(self):
        self.today_data = DailyStat.objects.filter(date=self.today).order_by('-percip')
        self.month_data = DailyStat.objects.filter(date__month=self.today.month,
            date__year=self.today.year).values('name').annotate(sum=Sum('percip')).order_by('-sum')
        self.annual_data = AnnualStat.objects.filter(year=self.today.year).order_by('-percip')

    def prepare_text(self):
        self.real_text = RichText()
        self.city_list = []
        filepath = os.path.join(root, 'precipstat', 'stations.txt')
        with open(filepath) as f:
            for line in f:
                code, name = tuple(line.split())
                self.city_list.append(name)

    def write_today(self):
        self.has_percip = [] # If no major city has any percip, go for vacation today!
        for city_data in self.today_data:
            percip = city_data.percip
            name = city_data.name
            if percip == 0:
                continue
            self.has_percip.append(name)
            self.plot.add_today_info(name, percip)
            text = '{} {:.1f}mm'.format(name, percip)
        if len(self.has_percip) == 0:
            logger.info("Uh-oh, no city has any percip today! Exit happily!")
            exit(0)

    def write_record(self):
        if self.today.month == 1 and self.today.day == 1:
            return
        has_record = False
        for city_data in self.today_data:
            percip = city_data.percip
            name = city_data.name
            prev_record = self.record_data[name]['record']
            if percip > prev_record and percip >= 20:
                has_record = True
                prev_date = self.record_data[name]['date']
                self.plot.add_record_text('{}刷新了今年的日雨量纪录 （原纪录：{:.1f}mm {}）'
                              ''.format(name, prev_record, prev_date.strftime('%Y/%m/%d')))

    def write_month(self):
        for i, city_data in enumerate(self.month_data, 2):
            month_percent = get_month_percent(city_data['name'], self.today.month, city_data['sum'])
            self.plot.add_month_info(city_data['name'], city_data['sum'], month_percent)

    def write_annual(self):
        for i, city_data in enumerate(self.annual_data, 2):
            self.plot.add_annual_info(city_data.name, city_data.percip)

    def send(self):
        logger.info("===TODAY'S POST===")
        self.plot.set_date(self.today)
        self.plot.set_title('南方省会降水量统计')
        logger.info('Plotting...')
        self.plot.plot()
        path = 'precipstat/plots/{}.png'.format(self.today.strftime('%Y%m%d'))
        self.plot.save(path)
        latest_path = os.path.join(settings.MEDIA_ROOT, 'latest/precip.png')
        shutil.copyfile(path, latest_path)
        logger.info('Export to {}'.format(path))
        # As of December 2019, I do not plan a new bot to break through CAPTCHA.
        return
        self.bot.load_cookie()
        self.bot.login_with_retry()
        self.bot.write_cookie()
        aid, fhash = self.bot.upload_image(self.target_thread, self.target_forum, path)
        self.real_text.insert_image(aid)
        self.bot.reply(self.target_thread, self.target_forum, self.real_text,
            visit=False, form_hash=fhash)
        self.bot.write_cookie()
        logger.info("===POST FINISHED===")

if __name__ == '__main__':
    if len(sys.argv) == 2:
        cmd = sys.argv[1]
        if cmd == 'dry':
            DailyUpdate(dry=True).auto()
    else:
        DailyUpdate().auto()
