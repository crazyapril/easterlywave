import datetime
import ftplib
import logging
import os
import shutil

from celery import shared_task
from django.conf import settings

from sate.satefile import SateFile
from sate.sateimage import SateImage
from tools.fastdown import FTPFastDown
from viewer.models import get_switch_status_by_name

PTREE_ADDR = settings.PTREE_FTP
PTREE_UID = settings.PTREE_UID
PTREE_PWD = settings.PTREE_PWD

TASKS = [
    #(3, None),  # VIS
    (8, 'nrl'),
    (13, (None, 'bd', 'rbtop', 'ca'))
]

MONITOR_DIRS = [
    os.path.join(settings.MEDIA_ROOT, 'sate'),
    os.path.join(settings.TMP_ROOT, 'sate'),
]

logger = logging.getLogger(__name__)

'''
R301 -- 0m0s -- 6m10s -- 6m45s / 8 -> 405
R302 -- 2m30s -- 8m10s -- 8m45s / 0 -> 525
R303 -- 5m0s -- 10m10s -- 10m45s / 2 -> 45
R304 -- 7m30s -- 11m50s -- 12m30s / 4 -> 150
<0 -(2)- 45 -(3)- 150 -(4)- 405 -(1)- 525 -(2)- 600>
'''


class TaskMaster:

    def go(self):
        logging.info('Sate service task started.')
        status = get_switch_status_by_name(settings.SWITCH_SATE_SERVICE)
        if status != 'ON':
            return
        logger.info('Sate service ON.')
        # full process
        self.ticker()
        self.prepare_tasks()
        self.download()
        self.export_image()

    def ticker(self):
        nowtime = datetime.datetime.utcnow()
        nt_m = nowtime.minute % 10
        nt_s = nowtime.second
        seconds = nt_m * 60 + nt_s
        if 405 < seconds <= 525:
            # R301
            self.time = nowtime.replace(minute=nowtime.minute // 10 * 10, second=0, microsecond=0)
        elif 525 < seconds < 600 or 0 <= seconds <= 45:
            # R302
            time = nowtime - datetime.timedelta(seconds=90)
            self.time = time.replace(minute=time.minute // 10 * 10 + 2, second=30, microsecond=0)
        elif 45 < seconds <= 150:
            # R303
            time = nowtime - datetime.timedelta(minutes=10)
            self.time = time.replace(minute=time.minute // 10 * 10 + 5, second=0, microsecond=0)
        elif 150 < seconds <= 405:
            # R304
            time = nowtime - datetime.timedelta(minutes=10)
            self.time = time.replace(minute=time.minute // 10 * 10 + 7, second=30, microsecond=0)
        logging.info('Ticker time: {}'.format(self.time))

    def prepare_tasks(self):
        self.task_files = []
        for band, enhance in TASKS:
            sf = SateFile(self.time, band=band, enhance=enhance)
            self.task_files.append(sf)

    def download(self):
        ftp = ftplib.FTP(PTREE_ADDR, PTREE_UID, PTREE_PWD)
        downer = FTPFastDown(file_parallel=1)
        downer.set_ftp(ftp)
        downer.set_task([(s.source_path, s.target_path) for s in self.task_files])
        downer.download()
        ftp.close()
        logging.info('Download finished.')

    def export_image(self):
        for sf in self.task_files:
            SateImage(sf).imager()
            logging.debug('Band{:02d} image exported.'.format(sf.band))
        logging.info('All images exported.')


@shared_task
def plotter():
    try:
        TaskMaster().go()
    except Exception as exp:
        logger.exception('A fatal error happened.')

@shared_task
def cleaner():
    for d in MONITOR_DIRS:
        subdirs = [o for o in os.listdir(d) if os.path.isdir(os.path.join(d, o))]
        subdirs.sort()
        for sd in subdirs[:-1]:
            shutil.rmtree(os.path.join(d, sd))


if __name__ == '__main__':
    TaskMaster().ticker()
