import datetime

from tools.diagnosis import DiagnosisSource
from tools.typhoon import StormSector


class JTWCSource(DiagnosisSource):

    name = 'JTWC'
    full_name = 'JTWC'
    values = {}

    def fetch(self):
        sector = StormSector.get_or_create()
        self.storm = sector.storms[self.code]
        self.wind = self.storm.wind
        self.pres = self.storm.pressure
        self.last_updated = datetime.datetime.now()
        self.data_time = self.storm.time
        self.loaded = True

    def represent(self) -> str:
        return '{} kt  {} hPa'.format(self.wind, self.pres)

