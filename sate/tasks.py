import datetime
import ftplib
import logging
import os
import shutil

from celery import shared_task
from django.conf import settings

from sate.format import get_segno
from sate.routines import PlotTrackRoutine
from sate.satefile import SateFile, combine_satefile_paths
from sate.sateimage import SateImage
from tools.cache import Key
from tools.fastdown import FTPFastDown
from tools.typhoon import StormSector
from tools.utils import utc_last_tick
from viewer.models import get_switch_status_by_name

PTREE_ADDR = settings.PTREE_FTP
PTREE_UID = settings.PTREE_UID
PTREE_PWD = settings.PTREE_PWD

TASKS = [
    (8, 'nrl'),
    (13, (None, 'bd', 'rbtop', 'ca'))
]

DAY_TASKS = [
    (3, None)
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


class TargetAreaTask:

    def go(self):
        logging.info('Sate service (target area) task started.')
        status = get_switch_status_by_name(settings.SWITCH_SATE_SERVICE)
        if status not in ('ALL', 'TANV', 'TA'):
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
        tasks = TASKS
        if self.check_sun_zenith_flag():
            tasks = tasks + DAY_TASKS
        for band, enhance in tasks:
            sf = SateFile(self.time, band=band, enhance=enhance)
            self.task_files.append(sf)

    def check_sun_zenith_flag(self):
        if 10 <= self.time.hour < 20:
            return False
        return Key.get(Key.SUN_ZENITH_FLAG)

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
        TargetAreaTask().go()
    except Exception as exp:
        logger.exception('A fatal error happened.')

@shared_task
def cleaner():
    for d in MONITOR_DIRS:
        subdirs = [o for o in os.listdir(d) if os.path.isdir(os.path.join(d, o))]
        subdirs.sort()
        for sd in subdirs[:-1]:
            shutil.rmtree(os.path.join(d, sd))

DATE_MONITOR_DIRS = [
    (os.path.join(settings.MEDIA_ROOT, 'typhoon/ecens'), 5)
]

@shared_task
def date_cleaner():
    nowtime = datetime.datetime.utcnow()
    for dirs, days in DATE_MONITOR_DIRS:
        subdirs = [o for o in os.listdir(d) if os.path.isdir(os.path.join(d, o))]
        for sd in subdirs:
            if len(sd) == 8:
                sd_time = datetime.datetime.strptime('%Y%m%d')
            elif len(sd) == 10:
                sd_time = datetime.datetime.strptime('%Y%m%d%H')
            if nowtime - sd_time >= datetime.timedelta(days=days):
                shutil.rmtree(os.path.join(d, sd))


FD_IMAGE_RANGE = 10

class FullDiskTask:

    def go(self):
        logging.info('Sate service (full disk) task started.')
        self.ticker()
        runnable = self.check_runnable()
        if not runnable:
            return
        logger.info('Sate service ON.')
        storms = self.prepare_tasks()
        if not storms:
            return
        self.download()
        self.export_image()

    def check_runnable(self):
        status = get_switch_status_by_name(settings.SWITCH_SATE_SERVICE)
        if status == 'ALL' or status == 'FD':
            self.enable_vis = True
            return True
        if status == 'TANV' or status == 'NV':
            self.enable_vis = False
            return True
        return False

    def ticker(self):
        self.time = utc_last_tick(10, delay_minutes=10)
        self.sector = StormSector.get_or_create()
        if self.time.minute == 0:
            logger.info('Update storm sector.')
            self.sector.update()
            self.sector.match_target()
            logger.info('{} is the target now.'.format(self.sector.target))
            if self.time.hour % 3 == 1:
                logger.info('Update storm tracks.')
                for storm in self.sector.storms.values():
                    storm.update_tracks()
                routine = PlotTrackRoutine(self.sector)
                routine.run()
            self.sector.save()

    def prepare_tasks(self):
        storms = self.sector.fulldisk_service_storms()
        logger.info('Full disk service for {}'.format(storms))
        tasks = TASKS
        if self.enable_vis and not 9 <= self.time.hour < 22:
            tasks += DAY_TASKS
        self.task_files = []
        for storm in storms:
            georange = (storm.lat - FD_IMAGE_RANGE / 2, storm.lat + FD_IMAGE_RANGE / 2,
                storm.lon - FD_IMAGE_RANGE / 2, storm.lon + FD_IMAGE_RANGE / 2)
            segno, vline, vcol = get_segno(georange)
            if len(segno) >= 3:
                logger.info('Range too large. Storm: {} Position: {},{}'.format(
                    storm.code, storm.lat, storm.lon))
                continue
            for band, enhance in tasks:
                sf = SateFile(self.time, area='fulldisk', band=band, segno=segno, enhance=enhance,
                    name=storm.code, vline=vline, vcol=vcol, georange=georange)
                self.task_files.append(sf)
        return storms

    def download(self):
        ftp = ftplib.FTP(PTREE_ADDR, PTREE_UID, PTREE_PWD)
        downer = FTPFastDown(file_parallel=1)
        downer.set_ftp(ftp)
        downer.set_task(combine_satefile_paths(self.task_files))
        downer.download()
        ftp.close()
        logging.info('Download finished.')

    def export_image(self):
        for sf in self.task_files:
            SateImage(sf).imager()
            logging.debug('Band{:02d} image exported.'.format(sf.band))
        logging.info('All images exported.')
        self.sector.save()


@shared_task
def fulldisk_plotter():
    try:
        FullDiskTask().go()
    except Exception as exp:
        logger.exception('A fatal error happened.')


def _debug_plot_sector_map():
    sector = StormSector.get_or_create()
    sector.update()
    print(sector.storms)
    sector.match_target()
    for storm in sector.storms.values():
        storm.update_tracks()
    routine = PlotTrackRoutine(sector)
    routine.run()
    return sector
