import datetime
import logging
import re
from typing import List

import requests
from tools.diagnosis import DiagnosisSource

logger = logging.getLogger(__name__)


class RIPASource(DiagnosisSource):

    name = 'RIPA'
    full_name = 'RIPA'
    values = {}

    INDEX_URL = 'https://rammb-data.cira.colostate.edu/tc_realtime/archive_text.asp?product=ripastbl&storm_identifier={id}'
    INDEX_REGEX = r'<a href="(.*)">Text File'
    RAMMB_URL = 'https://rammb-data.cira.colostate.edu/'

    CONS_PROB_REGEX = r'\s+[0-9.%]+\s+[0-9.%]+\s+([0-9.%]+)'
    P25_24H = '25kt \/ 24h'
    P40_24H = '40kt \/ 24h'
    P55_36H = '55kt \/ 36h'
    P70_48H = '70kt \/ 48h'

    def fetch(self):
        url = self.INDEX_URL.format(id=self.storm.code_full.lower())
        try:
            page = requests.get(url, timeout=5)
        except:
            logger.exception('Fail to get RIPA main page for %s', self.code)
            return
        text_urls = re.findall(self.INDEX_REGEX, page.text)
        if len(text_urls) == 0:
            return
        latest_text_url = self.RAMMB_URL + text_urls[-1]
        try:
            page = requests.get(latest_text_url, timeout=5)
            html = page.text
        except:
            logger.exception('Fail to get RIPA text for %s', self.code)
            return
        if 'NOT Run' in html:
            return
        match = re.search(self.P25_24H + self.CONS_PROB_REGEX, html)
        if match is None:
            return
        self.prob25_24 = match.group(1)
        logger.info('%s RIPA +25kt/24h %s', self.code, self.prob25_24)
        match = re.search(self.P40_24H + self.CONS_PROB_REGEX, html)
        self.prob40_24 = match.group(1)
        logger.info('%s RIPA +40kt/24h %s', self.code, self.prob40_24)
        match = re.search(self.P55_36H + self.CONS_PROB_REGEX, html)
        self.prob55_36 = match.group(1)
        logger.info('%s RIPA +55kt/36h %s', self.code, self.prob55_36)
        match = re.search(self.P70_48H + self.CONS_PROB_REGEX, html)
        self.prob70_48 = match.group(1)
        logger.info('%s RIPA +70kt/48h %s', self.code, self.prob70_48)
        match = re.search(r'(\d{6} \d{2})UTC', html)
        self.data_time = datetime.datetime.strptime(match.group(1), '%y%m%d %H')
        self.last_updated = datetime.datetime.now()
        self.loaded = True

    def represent(self) -> List[str]:
        return [
            '+25kt/24h {}'.format(self.prob25_24),
            '+40kt/24h {}'.format(self.prob40_24),
            '+55kt/36h {}'.format(self.prob55_36),
        ]
