import datetime
import logging
import re

import requests

from tools.diagnosis import DiagnosisSource

logger = logging.getLogger(__name__)


class SATCONSource(DiagnosisSource):

    name = 'SATCON'
    full_name = 'SATCON'
    values = {}

    URL = 'http://tropic.ssec.wisc.edu/real-time/satcon/{year}{code}.html'
    REGEX = r'SATCON:\s+MSLP = (\d+) hPa[\s&nbsp;]+MSW = (\d+) knots'

    def fetch(self):
        url = self.URL.format(year=self.storm.guess_year, code=self.code)
        try:
            page = requests.get(url, timeout=5)
            html = page.text
        except:
            logger.exception('Fail to fetch SATCON for %s', self.code)
            return
        match = re.search(self.REGEX, html)
        if match is None:
            return
        self.pres = int(match.group(1))
        self.wind = int(match.group(2))
        match = re.search(r'mmddhhmm\): (\d{8})', html)
        self.data_time = datetime.datetime.strptime(match.group(1), '%m%d%H%M')
        self.data_time = self.data_time.replace(year=self.storm.guess_year)
        self.last_updated = datetime.datetime.now()
        self.loaded = True

    def represent(self) -> str:
        return '{} kt  {} hPa'.format(self.wind, self.pres)

