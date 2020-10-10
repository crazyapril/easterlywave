import datetime
import ftplib
import logging

from celery import shared_task
from django.conf import settings

from sate.ensemble.bufrfile import BufrFile
from sate.ensemble.regionplot import RegionPlot
from sate.ensemble.stormplot import StormPlot
from tools.cache import Key
from tools.fastdown import FTPFastDown


ECMWF_FTP_ADDRESS = 'data-portal.ecmwf.int'
ECMWF_FTP_USERNAME = 'wmo'
ECMWF_FTP_PASSWORD = 'essential'
HISTORY_DAYS = 3

logger = logging.getLogger(__name__)


class ECEnsembleRoutine:

    def go(self, time:str=None, plot_region=True):
        if time is None:
            self.ticker()
        else:
            self.basetime = time
        downer = ECMWFDown()
        downer.set_time(self.basetime)
        downer.search_from_ftp()
        downer.download()
        downer.disconnect()
        if plot_region:
            rp = RegionPlot(self.basetime, downer.storms)
            rp.plot_all()
        storms = [storm for storm in downer.storms \
            if storm.codename[0] not in '789']
        sp = StormPlot(self.basetime, storms)
        sp.plot_all()
        self.save_cache(sp.plotted)
        logger.info('EC ensemble task finished.')

    def ticker(self):
        nowtime = datetime.datetime.utcnow()
        if  7 <= nowtime.hour < 19:
            self.time = nowtime.replace(hour=0)
        elif nowtime.hour >= 19:
            self.time = nowtime.replace(hour=12)
        else:
            self.time = nowtime.replace(hour=12)
            self.time = self.time - datetime.timedelta(days=1)
        self.basetime = self.time.strftime('%Y%m%d%H')
        logger.info('EC ensemble task started. Runtime: {}'.format(self.basetime))

    def save_cache(self, storms):
        cache_entry = Key.get(Key.ECMWF_ENSEMBLE_STORMS)
        if cache_entry is None:
            cache_entry = []
        cache_entry = cache_entry[:HISTORY_DAYS*2]
        writed = False
        for entry in cache_entry:
            if entry['basetime'] == self.basetime:
                entry['storms'] = [storm.codename for storm in storms]
                writed = True
                break
        if not writed:
            cache_entry.insert(0, {
                'basetime': self.basetime,
                'storms': [storm.codename for storm in storms]
            })
        Key.set(Key.ECMWF_ENSEMBLE_STORMS, cache_entry, 3 * Key.DAY)
        logger.info('Save cache: {}'.format(cache_entry))


class ECMWFDown:

    def __init__(self):
        self.connected = False
        self.storms = []

    def connect(self):
        self.ftp = ftplib.FTP(ECMWF_FTP_ADDRESS, user=ECMWF_FTP_USERNAME,
            passwd=ECMWF_FTP_PASSWORD)
        self.connected = True

    def disconnect(self):
        self.ftp.close()
        self.connected = False

    def set_time(self, time):
        self.basetime = time

    @property
    def ftp_dir(self):
        if self.basetime[-2:] in ('00', '12'):
            return self.basetime + '0000'
        else:
            return 'test/' + self.basetime + '0000'

    def search_from_ftp(self):
        self.connect()
        self.ftp.cwd(self.ftp_dir)
        filenames = self.ftp.nlst()
        for fname in filenames:
            if 'tropical_cyclone_track' in fname and 'ECEP' in fname:
                bf = BufrFile(fname)
                self.storms.append(bf)

    def download(self, storms=None):
        if storms is None:
            storms = self.storms
        downlist = [storm for storm in storms if not storm.file_exists()]
        if not downlist:
            return storms
        if not self.connected:
            self.connect()
        dirpath = '/' + self.ftp_dir
        if self.ftp.pwd() != dirpath:
            self.ftp.cwd(dirpath)
        downer = FTPFastDown(file_parallel=1)
        downer.set_ftp(self.ftp)
        downer.set_task([(s.filename, s.filepath) for s in downlist])
        logger.info('Download bufr files from ECMWF ftp server. '
            'Total files: {}'.format(len(downlist)))
        downer.download()
        logger.info('Download finished.')
        return storms


def _debug_plot_ec_ens(time=None):
    ECEnsembleRoutine().go(time=time)

@shared_task(ignore_result=True)
def plot_ec_ensemble():
    try:
        ECEnsembleRoutine().go()
    except Exception as exp:
        logger.exception('A fatal error happened.')
