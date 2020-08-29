import datetime
import logging
from typing import List, Type

from django.core.cache import cache

from tools.diagnosis import DiagnosisSource
from tools.diagnosis.adt import ADTSource
from tools.diagnosis.amsu import AMSUSource
from tools.diagnosis.archer import ArcherSource
from tools.diagnosis.ecmwf import ECMWFSource
from tools.diagnosis.jtwc import JTWCSource
from tools.diagnosis.jtwcforecast import JTWCForecastSource
from tools.diagnosis.ripa import RIPASource
from tools.diagnosis.satcon import SATCONSource
from tools.typhoon import StormSector

logger = logging.getLogger(__name__)


class DiagnosisSourceManager:

    sources_list = [
        ADTSource,
        AMSUSource,
        ArcherSource,
        ECMWFSource,
        JTWCSource,
        JTWCForecastSource,
        RIPASource,
        SATCONSource
    ] # type: List[Type[DiagnosisSource]]

    def __init__(self, code, storm) -> None:
        self.code = code
        self.key = self.get_key(code)
        self.storm = storm
        self.sources = {}

    @classmethod
    def get_key(cls, code):
        return 'DiagnosisSourceManager.{}'.format(code)

    @classmethod
    def get_or_create(cls, code, storm):
        instance = cache.get(cls.get_key(code))
        if instance is None:
            instance = cls(code, storm)
            instance.save()
        return instance

    def save(self):
        cache.set(self.key, self, 86400)

    def get_source(self, name):
        return self.sources.get(name)

    def update(self, nowtime=None):
        for source_class in self.sources_list:
            source = source_class(self.code, self.storm)
            try:
                if nowtime is None or source.should_update(nowtime):
                    source.fetch()
            except:
                logger.exception('Unknown error happened while fetch %s for %s',
                                 source_class.name, self.code)
            if source is None or not source.loaded:
                logger.info('No output for source %s of %s', source_class.name, self.code)
                continue
            if (nowtime - source.data_time) > datetime.timedelta(hours=24):
                logger.info('Too old for source %s of %s (%s)', source_class.name, self.code, source.data_time)
            self.sources[source_class.name] = source


def debug_manager():
    sector = StormSector.get_or_create()
    maysak = sector.storms['10W']
    manager = DiagnosisSourceManager.get_or_create(maysak.code, maysak)
    nowtime = datetime.datetime(2020, 8, 29, 19, 20)
    manager.update(nowtime)
    return manager