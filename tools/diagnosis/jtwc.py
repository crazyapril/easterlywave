import datetime

from tools.diagnosis import DiagnosisSource


class JTWCSource(DiagnosisSource):

    name = 'JTWC'
    full_name = 'JTWC'
    values = {}

    def fetch(self):
        self.wind = self.storm.wind
        self.pres = self.storm.pressure
        self.last_updated = datetime.datetime.now()
        self.data_time = self.storm.time
        self.loaded = True

    def represent(self) -> str:
        return '{} kt  {} hPa'.format(self.wind, self.pres)

