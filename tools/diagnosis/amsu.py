import datetime
import logging
import re

import requests

from tools.diagnosis import DiagnosisSource

logger = logging.getLogger(__name__)


class AMSUSource(DiagnosisSource):

    name = 'AMSU'
    full_name = 'AMSU'
    values = {}

    URL = 'http://tropic.ssec.wisc.edu/real-time/amsu/archive/{year}/{year}{code}/intensity.txt'

    def fetch(self):
        url = self.URL.format(year=self.storm.guess_year, code=self.code)
        try:
            page = requests.get(url, timeout=10)
        except:
            logger.exception('No AMSU intensity estimate for %s', self.code)
            return
        txt = page.text
        match = re.search(r'MSLP\:\s+(\d+) hPa', txt)
        self.pres = match and int(match.group(1))
        match = re.search(r'Wind\:\s+(\d+) kts', txt)
        self.wind = match and int(match.group(1))
        match = re.search(r'Confidence\:\s+(\w+) \(', txt)
        self.confidence = match and match.group(1)
        match = re.search(r'(\d{2}[a-z]+\d{2})\sTime: (\d+) UTC', txt)
        if match:
            time = match.group(1) + ' ' + match.group(2)
            self.data_time = datetime.datetime.strptime(time, '%d%b%y %H%M')
        self.last_updated = datetime.datetime.now()
        self.loaded = any((self.pres, self.wind, self.confidence))

    def represent(self) -> str:
        return '{} kt  {} hPa  [{}]'.format(self.wind, self.pres, self.confidence)

