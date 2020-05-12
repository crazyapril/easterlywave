import datetime
import ftplib
import json
import logging
import os
import shutil

from celery import shared_task
from django.conf import settings
from pyorbital.astronomy import cos_zen

from sate.format import get_segno, HimawariFormat
from sate.makegif import MakeGifRoutine
from sate.routines import PlotTrackRoutine
from sate.satefile import SateFile, combine_satefile_paths
from sate.sateimage import SateImage
from tools.cache import Key
from tools.fastdown import S3FastDown
from tools.typhoon import StormSector
from tools.utils import utc_last_tick, is_file_valid
from viewer.models import get_switch_status_by_name

TASKS = [
    (8, ('nrl', 'ssd')),
    (13, (None, 'bd', 'rbtop', 'ca'))
]

DAY_TASKS = [
    (3, None)
]

DAY_TASKS_FOR_FLOATER = [
    (1, None)
]

logger = logging.getLogger(__name__)

S3_BUCKET_NAME = 'noaa-himawari8'

'''
R301 -- 0m0s -- 6m10s -- 7m30s / 9 -> 450
R302 -- 2m30s -- 8m10s -- 9m30s / 1 -> 570
R303 -- 5m0s -- 10m10s -- 11m30s / 3 -> 90
R304 -- 7m30s -- 11m50s -- 13m15s / 5 -> 195
<0 -(2)- 45 -(3)- 150 -(4)- 405 -(1)- 525 -(2)- 600>
'''


def is_daytime(utc_time, lat, lon, threshold=0.0349):
    return cos_zen(utc_time, lon, lat) > threshold


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


class SateServiceConfig:
    """flags: MASTER: OFF/ON
            BASIN: WPAC/ALL
            INTENSITY: TD/ALL
            AREA: TA/ALL
            VIS: OFF/ON
    areadef: +:
            -:
    """

    def __init__(self):
        self.flags = None
        self.areadef = None
        self.status = {'target':False, 'target_storm':None, 'fulldisk':False,
            'fulldisk_areas':[]}

    def _parse_latlon(self, lstr, mode='lat'):
        l = float(lstr[:-1])
        if (mode == 'lat' and lstr[-1] == 'S') or (mode == 'lon' and lstr[-1] == 'W'):
            l = -l
        return l

    def read(self):
        status = get_switch_status_by_name(settings.SWITCH_SATE_SERVICE)
        self.flags = json.loads(status.upper())
        self.areadef = {'positive': [], 'negative': []}
        status = get_switch_status_by_name(settings.SWITCH_SATE_AREADEF)
        if status == 'N/A':
            return
        for adef in status.upper().split('\n'):
            adef_seg = adef.split()
            if adef_seg[0] == '+':
                info = AttrDict()
                info.update(code=adef_seg[1], name=adef_seg[2],
                    lat=self._parse_latlon(adef_seg[3]),
                    lon=self._parse_latlon(adef_seg[4], mode='lon'))
                self.areadef['positive'].append(info)
            elif adef_seg[0] == '-':
                self.areadef['negative'].append(adef_seg[1])

    @classmethod
    def load(cls):
        instance = Key.get(Key.SATE_SERVICE_CONFIG)
        if instance is None:
            instance = cls()
            instance.read()
        return instance

    def save(self):
        Key.set(Key.SATE_SERVICE_CONFIG, self, 3600 * 6)

    def update_area(self, sector):
        """Update areas to be shown."""
        self.status = {
            'target': False,
            'target_storm': None,
            'fulldisk': False,
            'fulldisk_areas': []
        }
        if self.flags['MASTER'] != 'ON':
            return
        if sector.target:
            target_storm = sector.storms[sector.target]
            negative_conditions = (
                target_storm.is_invest and self.flags['INTENSITY'] != 'ALL',
                target_storm.basin != 'WPAC' and self.flags['BASIN'] != 'ALL',
                target_storm.code in self.areadef['negative']
            )
            if not any(negative_conditions):
                self.status['target'] = True
                self.status['target_storm'] = target_storm
        for storm in sector.storms.values():
            if self.flags['AREA'] != 'ALL':
                # Whether fulldisk service is on
                continue
            elif not storm.in_scope:
                # Whether the storm is covered by the satellite
                continue
            elif storm.is_invest and self.flags['INTENSITY'] != 'ALL':
                # Whether invests are included
                continue
            elif storm.basin != 'WPAC' and self.flags['BASIN'] != 'ALL':
                # Whether non-WPAC storms are included
                continue
            elif storm.code in self.areadef['negative']:
                # Whether the storm is explicitly excluded
                continue
            elif storm.is_target:
                # Whether the storm is in the target area:
                continue
            storm.in_service = True
            self.status['fulldisk_areas'].append(storm)
        self.status['fulldisk_areas'].extend(self.areadef['positive'])
        if len(self.status['fulldisk_areas']) > 0:
            self.status['fulldisk'] = True
        logger.info('Current status: {}'.format(self.status))
        logger.info('Active areas: {}'.format(self.active_areas))

    @property
    def active_areas(self):
        areas = []
        if self.status['target']:
            areas.append({
                'code': self.status['target_storm'].code,
                'name': self.status['target_storm'].name,
                'target': True
            })
        for area in self.status['fulldisk_areas']:
            areas.append({'code': area.code, 'name': area.name, 'target': False})
        return areas


class TargetAreaTask:

    def go(self, from_task=None, runtime=None):
        logger.info('Sate service (target area) task started.')
        self.config = SateServiceConfig.load()
        if self.config.flags['MASTER'] != 'ON' or not self.config.status['target']:
            return
        logger.info('Sate service ON.')
        # full process
        self._task = from_task
        if self._task is None:
            self.ticker(runtime=runtime)
        else:
            logger.info('Retry failed task: {}'.format(self._task))
            self.time = self._task.time
        self.prepare_tasks()
        if not self.download():
            return
        self.export_image()
        if self._task:
            failed_tasks = FailedSatelliteTasks.get_or_create()
            failed_tasks.remove(self._task)
        if self.time.minute % 10 == 0:
            logger.info('Make optimized gif.')
            MakeGifRoutine().go(mode='target')

    def ticker(self, runtime=None):
        if runtime:
            nowtime = runtime
        else:
            nowtime = datetime.datetime.utcnow()
        nt_m = nowtime.minute % 10
        nt_s = nowtime.second
        seconds = nt_m * 60 + nt_s
        if 450 < seconds <= 570:
            # R301
            self.time = nowtime.replace(minute=nowtime.minute // 10 * 10,
                second=0, microsecond=0)
        elif 570 < seconds < 600 or 0 <= seconds <= 90:
            # R302
            time = nowtime - datetime.timedelta(seconds=90)
            self.time = time.replace(minute=time.minute // 10 * 10 + 2,
                second=30, microsecond=0)
        elif 90 < seconds <= 195:
            # R303
            time = nowtime - datetime.timedelta(minutes=10)
            self.time = time.replace(minute=time.minute // 10 * 10 + 5,
                second=0, microsecond=0)
        elif 195 < seconds <= 450:
            # R304
            time = nowtime - datetime.timedelta(minutes=10)
            self.time = time.replace(minute=time.minute // 10 * 10 + 7,
                second=30, microsecond=0)
        logger.info('Ticker time: {}'.format(self.time))

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
        downer = S3FastDown()
        downer.set_bucket(S3_BUCKET_NAME)
        downer.set_task([(s.source_path, s.target_path) for s in self.task_files \
            if not is_file_valid(s.target_path)])
        try:
            downer.download()
        except OSError:
            logger.info('Fail to download.')
            failed_tasks = FailedSatelliteTasks.get_or_create()
            if self._task is None:
                failed_tasks.add(FailedSatelliteTask('target', self.time))
            else:
                failed_tasks.fail(self._task)
            return False
        logger.info('Download finished.')
        return True

    def export_image(self):
        for sf in self.task_files:
            SateImage(sf).imager()
            logger.debug('Band{:02d} image exported.'.format(sf.band))
        logger.info('All images exported.')


@shared_task(ignore_result=True, expires=30)
def plotter():
    try:
        failed_tasks = FailedSatelliteTasks.get_or_create()
        runtime = datetime.datetime.utcnow()
        for task in failed_tasks.get_tasks():
            if task.type == 'target':
                TargetAreaTask().go(from_task=task)
        TargetAreaTask().go(runtime=runtime)
    except Exception as exp:
        logger.exception('A fatal error happened.')


FD_IMAGE_RANGE = 10, 8

class FullDiskTask:

    def go(self, from_task=None):
        logger.info('Sate service (full disk) task started.')
        self.config = SateServiceConfig.load()
        self.enable_vis = self.config.flags['VIS'] == 'ON'
        self._task = from_task
        if self._task is None:
            self.ticker()
        else:
            logger.info('Retry failed task: {}'.format(self._task))
            self.time = self._task.time
            self.sector = StormSector.get_or_create()
        if not self.config.status['fulldisk']:
            return
        logger.info('Sate service ON.')
        storms = self.prepare_tasks()
        if not storms:
            return
        if not self.download():
            return
        self.export_image()
        if self._task:
            failed_tasks = FailedSatelliteTasks.get_or_create()
            failed_tasks.remove(self._task)
        logger.info('Make optimized gif.')
        MakeGifRoutine().go(mode='fulldisk')

    def ticker(self):
        self.time = utc_last_tick(10, delay_minutes=10)
        self.sector = StormSector.get_or_create()
        if self.time.minute == 10:
            logger.info('Update storm sector.')
            self.sector.update()
            self.sector.match_target()
            self.sector.save()
            logger.info('{} is the target now.'.format(self.sector.target))
            if self.time.hour % 3 == 1:
                self.update_storm_tracks()
                if self.sector.has_storm_in_scope():
                    self.update_target_area_location()
                    self.sector.match_target()
        else:
            self.sector.match_target()
        self.config.read()
        self.config.update_area(self.sector)
        if not self.config.status['target'] and self.sector.target:
            self.config.status['target'] = True
            self.config.status['target_storm'] = self.sector.target
        self.config.save()
        self.sector.save()

    def update_storm_tracks(self):
        logger.info('Update storm tracks.')
        for storm in self.sector.storms.values():
            storm.update_tracks()
            storm.update_jtwc_forecast()
        routine = PlotTrackRoutine(self.sector)
        routine.run()

    def update_target_area_location(self):
        if Key.get(Key.TARGET_AREA_MIDPOINT):
            # This entry is only cached for 1 hour, so if it exists,
            # target area service is currently on.
            return
        logger.info('Update target area location...')
        sf = SateFile(self.time, band=13, enhance=None) # Sample file
        downer = S3FastDown()
        downer.set_bucket(S3_BUCKET_NAME)
        downer.set_task([(sf.source_path, sf.target_path)])
        downer.download()
        hf = HimawariFormat(sf.target_path)
        hf.load()
        lons, lats = hf.get_geocoord()
        midlon = (lons.min() + lons.max()) / 2
        midlat = (lats.min() + lats.max()) / 2
        Key.set(Key.TARGET_AREA_MIDPOINT, (midlon, midlat), 3600)
        logger.info('The midpoint of target point is {}, {}'.format(
            midlon, midlat))

    def prepare_tasks(self):
        storms = self.config.status['fulldisk_areas']
        # self.sector.rank_storms() ???
        logger.info('Full disk service for {}'.format(storms))
        self.task_files = []
        for storm in storms:
            georange = (storm.lat - settings.FD_IMAGE_RANGE[1] / 2,
                storm.lat + settings.FD_IMAGE_RANGE[1] / 2,
                storm.lon - settings.FD_IMAGE_RANGE[0] / 2,
                storm.lon + settings.FD_IMAGE_RANGE[0] / 2)
            segno, vline, vcol = get_segno(georange)
            logger.info('Floater for storm <{}> need segs: {}'.format(
                storm.code, segno))
            if len(segno) >= 3:
                logger.info('Range too large. Storm: {} Position: {},{}'.format(
                    storm.code, storm.lat, storm.lon))
                continue
            if self.enable_vis and is_daytime(self.time, storm.lat, storm.lon):
                storm_tasks = TASKS + DAY_TASKS_FOR_FLOATER
            else:
                storm_tasks = TASKS
            for band, enhance in storm_tasks:
                sf = SateFile(self.time, area='fulldisk', band=band,
                    segno=segno, enhance=enhance, name=storm.code,
                    vline=vline, vcol=vcol, georange=georange)
                self.task_files.append(sf)
        return storms

    def download(self):
        downer = S3FastDown()
        downer.set_bucket(S3_BUCKET_NAME)
        needed_files = combine_satefile_paths(self.task_files)
        # Filter files not downloaded yet
        needed_files = [(source, target) for source, target in needed_files \
            if not is_file_valid(target)]
        downer.set_task(needed_files)
        try:
            downer.download()
        except OSError:
            logger.info('Fail to download.')
            failed_tasks = FailedSatelliteTasks.get_or_create()
            if self._task is None:
                failed_tasks.add(FailedSatelliteTask('fulldisk', self.time))
            else:
                failed_tasks.fail(self._task)
            return False
        logger.info('Download finished.')
        return True

    def export_image(self):
        for sf in self.task_files:
            SateImage(sf).imager()
            logger.debug('Band{:02d} image exported.'.format(sf.band))
        logger.info('All images exported.')
        self.sector.save()


class FailedSatelliteTasks:

    persist_hours = 12

    @classmethod
    def get_or_create(cls):
        instance = Key.get(Key.SATE_FAILED_TASKS)
        if instance:
            return instance
        instance = cls()
        instance.save()
        return instance

    def save(self):
        Key.set(Key.SATE_FAILED_TASKS, self, self.persist_hours * 3600)
        logger.info('[Failed task] Saved: {}'.format(self.tasks))

    def __init__(self):
        self.tasks = []

    def get_tasks(self):
        return self.tasks

    def add(self, task):
        logger.info('[Failed task] Add new: {}'.format(task))
        self.tasks.append(task)
        self.save()

    def remove(self, task):
        try:
            self.tasks.remove(task)
        except ValueError:
            logger.info('[Failed task] No task <{}> found in task list.'.format(task))
        self.save()

    def fail(self, task):
        # Due to redis, param task is not the same one task in the list!!
        real_task = self.tasks[self.tasks.index(task)]
        real_task.failed += 1
        logger.info('[Failed task] Fail once more: {}'.format(real_task))
        if real_task.failed >= 3:
            logger.info('[Failed task] Failed too many times. Removed.')
            self.remove(real_task)
        else:
            self.save()


class FailedSatelliteTask:

    def __init__(self, tasktype, time):
        self.type = tasktype
        self.time = time
        self.failed = 1

    def __str__(self):
        return '<{} {} Failed: {}>'.format(self.type, self.time, self.failed)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, task):
        return self.type == task.type and self.time == task.time


@shared_task(ignore_result=True)
def fulldisk_plotter():
    try:
        failed_tasks = FailedSatelliteTasks.get_or_create()
        for task in failed_tasks.get_tasks():
            if task.type == 'fulldisk':
                FullDiskTask().go(from_task=task)
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
        storm.update_jtwc_forecast()
    routine = PlotTrackRoutine(sector)
    routine.run()
    return sector
