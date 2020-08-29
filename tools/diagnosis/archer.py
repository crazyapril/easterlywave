import datetime
from datetime import date
import logging
from io import StringIO
from re import S

import pandas
import requests

from tools.diagnosis import DiagnosisSource

logger = logging.getLogger(__name__)


class ArcherSource(DiagnosisSource):

    name = 'Archer'
    full_name = 'Archer'
    values = {}

    URL = 'http://tropic.ssec.wisc.edu/real-time/archerOnline/cyclones/{year}_{code}/web/archer_fdeck.txt'

    def fetch(self):
        url = self.URL.format(year=self.storm.guess_year, code=self.code)
        try:
            page = requests.get(url, timeout=10)
        except:
            logger.exception('Fail to get Archer source for %s', self.code)
            return
        self.content = pandas.read_csv(
            StringIO(page.text),
            header=None,
            skipinitialspace=True,
            converters={
                2:lambda x: datetime.datetime.strptime(x, '%Y%m%d%H%M'),
                7:lambda x: (x or None) and int(x[:-1]) / 100 * (1 if x[-1] == 'N' else -1),
                8:lambda x: (x or None) and int(x[:-1]) / 100 * (1 if x[-1] == 'E' else -1)
            }
        )
        self.last_updated = datetime.datetime.now()
        self.data_time = self.content.iloc[-1][2]
        self.lats = self.content[7]
        self.lons = self.content[8]
        self.times = self.content[2]
        self.loaded = True

