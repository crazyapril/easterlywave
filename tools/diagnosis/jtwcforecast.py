import datetime

from tools.diagnosis import DiagnosisSource


class JTWCForecastSource(DiagnosisSource):

    name = 'JTWC-Forecast'
    full_name = 'JTWC Forecast'
    values = {}

    def fetch(self):
        self.storm.update_jtwc_forecast()
        self.lats = self.storm.jtwc_forecast['lats']
        self.lons = self.storm.jtwc_forecast['lons']
        self.last_updated = datetime.datetime.now()
        self.data_time = self.storm.jtwc_forecast['time']
        self.loaded = self.data_time is not None

