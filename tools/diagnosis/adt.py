import datetime
import logging
from io import StringIO
from typing import Optional, Type

import pandas
import re
import requests

from tools.diagnosis import DiagnosisSource
from tools.typhoon import Storm

logger = logging.getLogger(__name__)


class ADTSource(DiagnosisSource):

    name = 'ADT'
    full_name = 'ADT'
    values = {}

    URL = 'http://tropic.ssec.wisc.edu/real-time/adt/{code}-list.txt'
    ADT_FWF_WIDTHS = [17, 5, 7, 6, 5, 4, 4, 10, 5, 5, 5, 5, 7, 7, 8, 7, 7, 7,
        8, 7, 8, 5, 1]

    # @classmethod
    # def prefetch(cls):
    #     url = 'http://tropic.ssec.wisc.edu/real-time/adt/adt.html'
    #     try:
    #         page = requests.get(url, timeout=10)
    #     except:
    #         logger.exception('Fail to update ADT')
    #         return
    #     regex = r'"odt([0-5]\d[LWCEABSP])\.html'
    #     codes = re.findall(regex, page.text)
    #     for code in codes:
    #         source = ADTSource(code)
    #         source.fetch()
    #         cls.values[code] = source
    #     logger.info('Successfully update ADT: %s', codes)

    def fetch(self):
        try:
            res = requests.get(self.URL.format(code=self.code))
            self.content = pandas.read_fwf(StringIO(res.text), skiprows=4,
                skipfooter=3, widths=self.ADT_FWF_WIDTHS)
            self.content.rename(columns={
                'Date    (UTC)': 'Time',
                '(CKZ)/': 'Pres',
                '(kts)': 'Wind'
            }, inplace=True)
        except:
            logger.exception('Failed to fetch %s', self.code)
            return
        self.wind = self.content.iloc[-1]['Wind']
        self.pres = self.content.iloc[-1]['Pres']
        self.lats = self.content['Lat']
        self.lons = self.content['Lon']
        self.lons = (360 - self.lons) % 360
        self.last_updated = datetime.datetime.now()
        data_time = self.content.iloc[-1]['Time']
        self.data_time = datetime.datetime.strptime(data_time, '%Y%b%d %H%M%S')
        self.loaded = True

    def represent(self):
        return '{} kt  {} hPa'.format(self.wind, self.pres)

