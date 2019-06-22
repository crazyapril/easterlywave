import datetime
import ftplib
import logging
import os
import time

import numpy as np
import xarray as xr
from celery import shared_task
from django.conf import settings
from django.core.cache import cache

from model.param import Param
from model.registry import PlotTask, registry_center
from tools.fastdown import SerialFTPFastDown
from tools.mapstore import MapArea, get_areas
from tools.utils import utc_last_tick

logger = logging.getLogger(__name__)


models_runtime = {'ecmwf': list(range(0, 241, 24))}
MODEL_HISTORY_RUNS = 5


class ModelException(Exception):

    pass


def plot_ecmwf():
    try:
        ECMWFKicker().kick()
    except Exception as exp:
        logger.error('fatal error in ecmwf model run.')


class Kicker:

    clocks = iter(range(0, 241, 24))
    min_clock = 0
    max_clock = 240
    modelkey = None

    def __init__(self):
        self.time = None
        self.try_round = True
        self.next_round = True
        self.tick = None
        self.failed = 0
        self.codes = None

    def kick(self, time=None, codes=None):
        if codes is not None:
            self.codes = codes
        if time is None:
            self.get_time()
        else:
            self.time = datetime.datetime.strptime(time, '%Y%m%d%H')
        self.establish()
        while True:
            try:
                self.clock()
            except StopIteration:
                break
            self.check_and_download()
            self.wait()
        self.destroy()
        # except Exception as exp:
        #     logger.exception('Unknown error')

    def get_time(self):
        pass

    def set_time(self, time):
        self.time = time

    def establish(self):
        pass

    def clock(self):
        pass

    def check_and_download(self):
        pass

    def wait(self):
        pass

    def destroy(self):
        pass



ECMWF_FTP_ADDRESS = 'data-portal.ecmwf.int'
ECMWF_FTP_USERNAME = 'wmo'
ECMWF_FTP_PASSWORD = 'essential'

class ECMWFKicker(Kicker):

    modelkey = 'ecmwf'
    _productdict_ = {
        '500:h':('H', '50', 'gh_500hPa'),
        'msl:p':('P', '89', 'msl'),
        '850:t':('T', '85', 't_850hPa'),
        '850:u':('U', '85', 'u_850hPa'),
        '850:v':('V', '85', 'v_850hPa'),
        '850:w':('W', '85', 'ws_850hPa')
    }
    _timeseq_ = 'AEIKMOQSWYT'
    resolution = 0.5

    def get_time(self):
        self.time = utc_last_tick(12 * 60) # 12 hour interval
        logger.info('ECMWF Kicker task: {}'.format(self.time.strftime('%Y%m%d%H')))

    def establish(self):
        self.ftp = ftplib.FTP(ECMWF_FTP_ADDRESS, ECMWF_FTP_USERNAME,
            ECMWF_FTP_PASSWORD)
        self.params = registry_center.get_params(self.modelkey)
        self.downer = SerialFTPFastDown(retry=2)
        self.downer.set_ftp(self.ftp)
        self.downer.set_success_callback(self.callback)
        tmp_path = os.path.join(settings.TMP_ROOT, self.time.strftime('model/%Y%m%d%H'),
            self.modelkey)
        os.makedirs(tmp_path, exist_ok=True)
        logger.info('ECMWF ftp connection established.')

    def clock(self):
        if self.next_round:
            self.tick = next(self.clocks)
            logger.info('Clock: {}'.format(self.tick))
            self.failed = 0
            self.try_round = True
            self.next_round = False

    def wait(self):
        if not (self.try_round or self.next_round):
            return
        if self.failed == 0:
            time.sleep(3)
        elif self.failed < 10:
            logger.debug('Wait for 1 min. Failed: {}'.format(self.failed))
            time.sleep(60)
        elif self.failed < 15:
            logger.debug('Wait for 3 min. Failed: {}'.format(self.failed))
            time.sleep(180)
        else:
            raise ModelException('Too much failed times.')

    def param_to_link(self, paramkey, tick):
        parameter, level, suffix = self._productdict_[paramkey]
        timechar = self._timeseq_[tick // 24]
        if tick == 0:
            fcststr = 'an'
        else:
            fcststr = str(self.tick) + 'h'
        ftpfile = '{0}/A_H{1}X{2}{3}ECMF{4}_C_ECMF_{5}_{6}_{7}_global_0p5deg_grib2.bin'\
            ''.format(self.time.strftime('%Y%m%d%H0000'), parameter, timechar, level,
                self.time.strftime('%d%H00'), self.time.strftime('%Y%m%d%H0000'),
                fcststr, suffix)
        return ftpfile

    def param_to_path(self, paramkey, tick):
        path = '{}_{}.grib2'.format(tick, paramkey.replace(':', '-'))
        path = os.path.join(settings.TMP_ROOT, self.time.strftime('model/%Y%m%d%H'),
            self.modelkey, path)
        return path

    def check_and_download(self):
        if self.try_round:
            logger.info('Try round.')
            test_param = self.params[0]
            source = self.param_to_link(test_param, self.tick)
            target = self.param_to_path(test_param, self.tick)
            callback_args = self, test_param
            self.downer.set_task([(source, target, callback_args)])
            try:
                self.downer.download()
            except Exception as exp:
                self.failed += 1
            else:
                logger.info('Try round succeeded!')
                self.try_round = False
        else:
            logger.info('Fire round.')
            params = self.params[1:]
            tasks = [(self.param_to_link(p, self.tick), self.param_to_path(p, self.tick),
                (self, p)) for p in params]
            self.downer.set_task(tasks)
            try:
                self.downer.download()
            except Exception as exp:
                self.failed += 1
                logger.exception('Fire round failed. Failed: {}'.format(self.failed))
            else:
                logger.info('Fire round succeeded!')
                self.next_round = True

    def callback(_, filename, callback_args):
        kicker, paramkey = callback_args
        logger.debug('{} downloaded.'.format(filename))
        plot_tasks = registry_center.get_tasks(kicker.modelkey, paramkey)
        for task in plot_tasks:
            if kicker.codes is not None and task.code not in kicker.codes:
                continue
            if not kicker.validate(task):
                continue
            if kicker.tick in task.requested_ticks:
                continue
            if task.plevel is None:
                plevel = [0 for i in range(len(task.regions))]
            elif isinstance(task.plevel, int):
                plevel = [task.plevel for i in range(len(task.regions))]
            elif isinstance(task.plevel, (list, tuple)):
                assert len(task.plevel) == len(task.regions)
                plevel = task.plevel
            else:
                raise ModelException('Invalid plevel configuration.')
            task.requested_ticks.append(kicker.tick)
            logger.info('A set of plot task prepared: Code: {}'.format(task.code))
            for pl, re in zip(plevel, task.regions):
                regions = get_areas(re)
                for region in regions:
                    session = Session(resolution=kicker.resolution, region=region,
                        basetime=kicker.time, fcsthour=kicker.tick, plevel=pl)
                    session.set_plot_task(task)
                    common_plot.apply_async(args=(task.to_json(), session.to_json()),
                        retry=True, ignore_result=True, priority=task.priority)
                    time.sleep(0.1)

    def validate(self, task):
        flag = all(os.path.exists(self.param_to_path(paramkey, self.tick)) \
            for paramkey in task.params)
        return flag

    def destroy(self):
        close_all_datasets()
        self.ftp.close()
        for task in registry_center.iter_tasks(self.modelkey):
            task.requested_ticks = []
        logger.info('Kicker task finished.')


@shared_task(bind=True, ignore_result=True)
def common_plot(self, plot_task_json, session_json):
    try:
        plot_task = PlotTask.from_json(plot_task_json)
        session = Session.from_json(session_json)
        session.make()
    except Exception as exp:
        logger.exception('error')
        raise exp
    logger.info('Starting a common plot task. Model: {} Code: {} '
        'Time: {} Region: {} ID: {}'.format(plot_task.model, plot_task.code,
            session.fcsthour, session.region.key, self.request.id))
    try:
        plot_task.plotfunc(session)
    except Exception as exp:
        logger.exception('Fatal error during plotting.')
        raise exp
    else:
        label_finished(plot_task.model, session.region.pkey, plot_task.code,
            session.basetime, session.fcsthour)
        logger.info('Plot task finished. ID: {}'.format(self.request.id))


def label_finished(model, region, code, basetime, tick=None):
    """If tick is None, it will create a empty status list."""
    key = 'STATUS_{}_{}_{}'.format(model, region, code)
    basetime_str = basetime.strftime('%Y%m%d%H')
    status = cache.get(key) or []
    if not status or status[0]['time'] != basetime_str:
        status.insert(0, {'time': basetime_str, 'ticks': [], 'pending': [], 'updating': True})
        if len(status) > MODEL_HISTORY_RUNS:
            status = status[:MODEL_HISTORY_RUNS]
    runtime = models_runtime.get(model)
    if tick is not None and tick not in status[0]['ticks']:
        status[0]['ticks'].append(tick)
        status[0]['ticks'].sort()
        status[0]['pending'] = [r for r in runtime if r not in status[0]['ticks']]
    if tick == max(runtime):
        status[0]['updating'] = False
    cache.set(key, status, 30 * 86400)


class Session:

    def __init__(self, model=None, resolution=None, georange=None, region=None,
            basetime=None, fcsthour=None, params=None, code=None, plevel=None):
        self.model = model
        self.resolution = resolution
        self.georange = georange
        self.region = region
        self.basetime = basetime
        self.fcsthour = fcsthour
        self.params = params or []
        self.code = code
        self.plevel = plevel

    def slice_indices(self):
        latmin, latmax, lonmin, lonmax = self.georange
        self.xmin = int(lonmin / self.resolution)
        self.xmax = int(lonmax / self.resolution)
        if lonmax == 360:
            self.xmax -= 1
        self.ymax = int((90 - latmin) / self.resolution)
        self.ymin = int((90 - latmax) / self.resolution)

    def set_plot_task(self, task):
        self.model = task.model
        self.params = task.params
        self.code = task.code

    def to_json(self):
        json = {
            'model': self.model,
            'resolution': self.resolution,
            'georange': self.georange,
            'region': self.region.key,
            'basetime': self.basetime.strftime('%Y%m%d%H'),
            'fcsthour': self.fcsthour,
            'params': self.params,
            'code': self.code,
            'plevel': self.plevel
        }
        return json

    @classmethod
    def from_json(cls, json):
        instance = cls(
            model=json['model'],
            resolution=json['resolution'],
            georange=json['georange'],
            region=MapArea.get(json['region']),
            basetime=datetime.datetime.strptime(json['basetime'], '%Y%m%d%H'),
            fcsthour=json['fcsthour'],
            params=json['params'],
            code=json['code'],
            plevel=json['plevel'])
        return instance

    def make(self):
        if self.georange is None and self.region is not None:
            self.georange = self.region.georange
        if self.plevel and self.plevel > 0:
            root = settings.PROTECTED_ROOT
        else:
            root = settings.MEDIA_ROOT
        self.target_path = os.path.join(root,
            'model/{}/{}/{}_{}_{}.png'.format(self.model,
                self.basetime.strftime('%Y%m%d%H'), self.code.lower(),
                self.region.pkey, self.fcsthour))
        os.makedirs(os.path.dirname(self.target_path), exist_ok=True)
        self.slice_indices()

    def get(self, paramkey):
        if paramkey not in self.params:
            raise ValueError('Session has no param named ' + paramkey)
        param = Param.from_str(paramkey)
        if param.time is not None:
            tick = self.fcsthour + param.time
            if tick < self.min_clock or tick > self.max_clock:
                return None
        else:
            tick = self.fcsthour
        ds = get_dataset(self.model, self.basetime, param.purekey, tick)
        latmin, latmax, lonmin, lonmax = self.georange
        default_key = list(ds.data_vars)[0]
        raw = ds.get(default_key)
        if lonmin > lonmax:
            first_half = np.flipud(raw[self.ymin:self.ymax+1, self.xmax:])
            second_half = np.flipud(raw[self.ymin:self.ymax+1, :self.xmin+1])
            return np.hstack((first_half, second_half))
        else:
            data = np.flipud(np.asanyarray(raw[self.ymin:self.ymax+1,
                self.xmin:self.xmax+1]))
            # if self.xmin == 0 and self.xmax == raw.shape[1] - 1:
            #     data = np.c_[data, data[:, 0]]
            return data

    def get_mapset(self):
        return self.region.load()



_opened_datasets_ = {}
_recent_used_datasets_ = []
MAX_CACHED_DATASETS = 5

def get_dataset(model, basetime, paramkey, tick):
    id_key = model, basetime, paramkey, tick
    if id_key in _recent_used_datasets_:
        ds = _opened_datasets_[id_key]
        _recent_used_datasets_.insert(0, _recent_used_datasets_.pop(
            _recent_used_datasets_.index(id_key)))
        return ds
    path = '{}_{}.grib2'.format(tick, paramkey.replace(':', '-'))
    path = os.path.join(settings.TMP_ROOT, 'model',
        '{}/{}'.format(basetime.strftime('%Y%m%d%H'), model), path)
    ds = xr.open_dataset(path, engine='cfgrib')
    _opened_datasets_[id_key] = ds
    _recent_used_datasets_.insert(0, id_key)
    if len(_recent_used_datasets_) > MAX_CACHED_DATASETS:
        purged_id_key = _recent_used_datasets_.pop(-1)
        purged_ds = _opened_datasets_.pop(purged_id_key)
        purged_ds.close()
    return ds

def close_all_datasets():
    for ds in _opened_datasets_.values():
        ds.close()
    _opened_datasets_.clear()
    _recent_used_datasets_.clear()

def _debug_ec(time, codes=None):
    logger.info('Debug: {} Codes: {}'.format(time, codes))
    ECMWFKicker().kick(time, codes=codes)

def _debug_async():
    from model.registry import PlotTask
    from model.tasksets.ecmwf import plot_gpa
    from tools.mapstore import MapArea
    pt = PlotTask('ecmwf', ['500:h'], regions=['Asia'],
        plotfunc=plot_gpa, code='GPA', scope='model.tasksets.ecmwf')
    session = Session(resolution=0.5, region=MapArea.get('Asia'),
        basetime=datetime.datetime(2019,6,21,12), fcsthour=0, plevel=1)
    session.set_plot_task(pt)
    common_plot.apply_async(args=(pt.to_json(), session.to_json()),
        retry=True, ignore_result=True, priority=pt.priority)
