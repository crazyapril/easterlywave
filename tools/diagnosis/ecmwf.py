import datetime
import logging
import os

from django.conf import settings

from sate.ensemble.bufrfile import BufrFile
from tools.diagnosis import DiagnosisSource
from tools.utils import utc_last_tick

logger = logging.getLogger(__name__)


class ECMWFSource(DiagnosisSource):

    name = 'ECMWF-Forecast'
    full_name = 'ECMWF Forecast'
    values = {}

    def should_update(cls, nowtime: datetime.datetime) -> bool:
        #return nowtime.hour in (4, 16) and nowtime.minute == 0
        return True

    def fetch(self):
        self.data_time = utc_last_tick(12*60)
        filepath = os.path.join(
            settings.TMP_ROOT,
            'ecens',
            self.data_time.strftime('%Y%m%d%H%M'),
            '{}.bufr'.format(self.code)
        )
        logger.info('Try to load bufr file from %s', filepath)
        if not os.path.exists(filepath):
            logger.info('File not exists. Quit.')
            return
        bufr = BufrFile(filepath=filepath)
        bufr.load()
        bufr.set_data_pointer('EMX')
        if len(bufr.lons) == 0:
            logger.info('Empty bufr file. Quit.')
            return
        self.lons = bufr.lons
        self.lats = bufr.lats
        self.last_updated = datetime.datetime.now()
        self.loaded = True
