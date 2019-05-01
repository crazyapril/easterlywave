import datetime
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import dask.array as da
import numpy as np
import requests
from celery import shared_task
from django.conf import settings
from matplotlib.patches import PathPatch
from scipy.interpolate.rbf import Rbf

from viewer.models import Station
from tools.mapstore import load_china_polygon
from tools.metplot.plotplus import Plot, MapSet
from tools.utils import utc_last_tick

logger = logging.getLogger(__name__)
NMC_REALTIME_INTERFACE = 'http://www.nmc.cn/f/rest/real/{station_id}'
CWB_REALTIME_INTERFACE = 'https://opendata.cwb.gov.tw/fileapi/v1/opendataapi/O-A0003-001?Authorization={authorization}&downloadType=WEB&format=JSON'
CWB_AWS_REALTIME_INTERFACE = 'https://opendata.cwb.gov.tw/fileapi/v1/opendataapi/O-A0001-001?Authorization={authorization}&downloadType=WEB&format=JSON'
ALLOWED_TIME_VARIANCE = datetime.timedelta(minutes=10)

RESOLUTION = 0.25
REGIONS = {
    'china': (16, 55, 72.5, 137.5),
    'south': (17.5, 27, 103.5, 118.5),
    'southwest': (20.5, 30, 96, 111),
    'midwest': (25.5, 35, 96.5, 111.5),
    'midsouth': (23, 30.5, 108.25, 120.75),
    'mideast': (27.5, 35, 111, 123.5),
    'central': (29, 36.5, 105.5, 118),
    'north': (34.5, 42, 111, 123.5),
    'northeast': (38.5, 54, 111, 139),
    'shaanxi': (33, 42.5, 101, 116),
    'qinghai': (31, 42.5, 87.5, 106.5),
    'xinjiang': (34.5, 49.5, 73, 97.5),
    'tibet': (26, 39, 78, 100)
}
RESOLUTION_TAIWAN = 0.05
REGION_TAIWAN = 21.5, 25.75, 117.5, 124.25


class RealTimeData:

    def __init__(self, _debug=False):
        self.data = []
        self.time = None
        self._debug = _debug

    def set_time(self, time):
        self.time = time
        logger.info('Time set: {}'.format(time.strftime('%Y/%m/%d %H:%M')))

    def fetch_all(self):
        if self._debug:
            tmpfile = os.path.join(settings.TMP_ROOT, 'tmp.json')
            if os.path.exists(tmpfile):
                return self.read_from_file(source_file=tmpfile)
        stations = list(Station.objects.filter(code__startswith='5').\
            values('code', 'lat', 'lon'))
        logger.info('Start data fetching.')
        with ThreadPoolExecutor(max_workers=35) as executor:
            executor.map(self, stations, timeout=5)
        logger.info('Done data fetching. Stations: {}'.format(len(self.data)))
        if self._debug:
            self.write_to_file(tmpfile)

    def __call__(self, station_info):
        url = NMC_REALTIME_INTERFACE.format(station_id=station_info['code'])
        try:
            query = requests.get(url, timeout=3)
        except (requests.ConnectionError, requests.HTTPError):
            pass
        else:
            query_json = query.json()
            time = datetime.datetime.strptime(query_json['publish_time'],
                '%Y-%m-%d %H:%M')
            if not self._debug and abs(time - self.time) > ALLOWED_TIME_VARIANCE:
                return
            if not -75 < query_json['weather']['temperature'] < 75:
                return
            # shorten province to reduce size
            province = query_json['station']['province']
            if province[0] in '黑内':
                province = province[:3] #黑龙江/内蒙古
            else:
                province = province[:2]
            self.data.append({
                'id': station_info['code'],
                'la': float(station_info['lat']),
                'lo': float(station_info['lon']),
                't': query_json['weather']['temperature'],
                'n': query_json['station']['city'],
                'pv': province,
                'rh': query_json['weather']['humidity'],
                'r': query_json['weather']['rain'],
                'w': query_json['wind']['speed'],
                'tm': time.strftime('%Y/%m/%d %H:%M')
            })

    def write_to_file(self, target_file=None):
        if target_file is None:
            target_file = os.path.join(settings.TMP_ROOT, 'weather',
                self.time.strftime('%Y%m%d%H.json'))
        json_data = {
            'meta': {
                'updated': datetime.datetime.now().strftime('%Y/%m/%d %H:%M'),
                'stations': len(self.data),
                'keys': {
                    'id': 'WMO ID',
                    'la': 'latitude',
                    'lo': 'longitude',
                    't': 'temperature',
                    'n': 'name',
                    'pv': 'province',
                    'rh': 'humidity',
                    'r': 'rain',
                    'w': 'wind',
                    'tm': 'time',
                    'rg': 'region'
                }
            },
            'data': self.data
        }
        json.dump(json_data, open(target_file, 'w', encoding='utf8'),
            ensure_ascii=False, separators=(',', ':'))

    def read_from_file(self, source_file=None, time=None):
        if time is not None:
            source_file = os.path.join(settings.TMP_ROOT, 'weather',
                time.strftime('%Y%m%d%H.json'))
        self.data = json.load(open(source_file, encoding='utf8'))
        if not self._debug:
            self.data = self.data['data']

    def export_key(self, key):
        return [d[key] for d in self.data]


class RealTimeDataForTaiwan:

    accepted_elements = {
        'TEMP': 't',
        'WDSD': 'w',
        'HUMD': 'rh',
    }

    def __init__(self, _debug=False):
        self.data = []
        self.time = None
        self._debug = _debug
        self.time = utc_last_tick(60) + datetime.timedelta(hours=8)

    def fetch(self, url):
        try:
            query = requests.get(url, timeout=3)
        except (requests.ConnectionError, requests.HTTPError):
            return
        query_json = query.json()['cwbopendata']
        for location in query_json['location']:
            time = datetime.datetime.strptime(location['time']['obsTime'][:16],
                '%Y-%m-%dT%H:%M')
            if not self._debug and abs(time - self.time) > ALLOWED_TIME_VARIANCE:
                continue
            location_json = {
                'id': location['stationId'][:-1],
                'la': float(location['lat_wgs84']),
                'lo': float(location['lon_wgs84']),
                'n': location['locationName'],
                'rg': '台灣',
                'tm': time.strftime('%Y/%m/%d %H:%M')
            }
            for element in location['weatherElement']:
                key = self.accepted_elements.get(element['elementName'], None)
                if key:
                    location_json[key] = float(element['elementValue']['value'])
            if not -75 < location_json['t'] < 75:
                continue
            location_json['rh'] = 100 * location_json['rh']
            self.data.append(location_json)


class RealTimeMapRoutine:

    def __init__(self, _debug=False):
        self.realtime_data = RealTimeData(_debug=_debug)
        self._debug = _debug

    def go(self):
        self.set_time()
        self.download()
        self.make_coordinates(REGIONS['china'], RESOLUTION)
        self.interpolate()
        self.load_china()
        for region in REGIONS:
            self.plot_region(region)
        self.realtime_data.write_to_file(os.path.join(settings.MEDIA_ROOT,
            'latest/weather/realtime.json'))
        self.plot_taiwan()

    def set_time(self):
        self.time = utc_last_tick(60) + datetime.timedelta(hours=8)
        self.realtime_data.set_time(self.time)
        logger.info('RT Map runtime: {}'.format(self.time.strftime('%Y/%m/%d %H:%M')))

    def download(self):
        cwb_manned_url = CWB_REALTIME_INTERFACE.format(
            authorization=settings.CWB_AUTHORIZATION)
        taiwan_data = RealTimeDataForTaiwan(_debug=self._debug)
        taiwan_data.fetch(cwb_manned_url)
        self.realtime_data.fetch_all()
        self.realtime_data.data.extend(taiwan_data.data)

    def load_china(self):
        self.china_polygon = load_china_polygon()

    def make_coordinates(self, georange, resolution):
        x = np.arange(georange[2], georange[3]+resolution, resolution)
        y = np.arange(georange[0], georange[1]+resolution, resolution)
        self.xx, self.yy = np.meshgrid(x, y)

    def interpolate(self, georange=None):
        if georange is None:
            xp = np.array(self.realtime_data.export_key('lo'))
            yp = np.array(self.realtime_data.export_key('la'))
            tp = np.array(self.realtime_data.export_key('t'))
        else:
            latmin, latmax, lonmin, lonmax = georange
            pts = [(d['lo'], d['la'], d['t']) for d in self.realtime_data.data \
                if latmin <= d['la'] <= latmax and lonmin <= d['lo'] <= lonmax]
            pts = list(zip(*pts))
            xp = list(pts[0])
            yp = list(pts[1])
            tp = list(pts[2])
        # Slice arrays by chunks, otherwise memory would explode. Thank you Dask!
        chunks = 1, self.xx.shape[1]
        # Use RBF interpolation
        logger.info('Start data interpolation.')
        rbf = Rbf(xp, yp, tp, function='linear')
        x_dask = da.from_array(self.xx, chunks=chunks)
        y_dask = da.from_array(self.yy, chunks=chunks)
        t_dask = da.map_blocks(rbf, x_dask, y_dask)
        self.data = t_dask.compute()
        logger.info('Done data interpolation.')
        self.xp = xp
        self.yp = yp

    def plot_taiwan(self):
        logger.info('Special process for taiwan. More stations  and higher resolution.')
        cwb_aws_url = CWB_AWS_REALTIME_INTERFACE.format(
            authorization=settings.CWB_AUTHORIZATION)
        taiwan_aws_data = RealTimeDataForTaiwan(_debug=self._debug)
        taiwan_aws_data.fetch(cwb_aws_url)
        self.realtime_data.data.extend(taiwan_aws_data.data)
        self.make_coordinates(REGION_TAIWAN, RESOLUTION_TAIWAN)
        self.interpolate(georange=REGION_TAIWAN)
        self.plot_region('taiwan', georange=REGION_TAIWAN)

    def plot_region(self, region, georange=None):
        if georange is None:
            georange = REGIONS[region]
        p = Plot(figsize=(8,6), aspect='cos', inside_axis=True)
        mapset = MapSet.from_natural_earth(georange=georange, country=False)
        p.usemapset(mapset)
        p.draw('coastline province')
        if region != 'china':
            p.draw('city')
        p._setxy(self.xx, self.yy)
        cs = p.contourf(self.data, gpfcmap='temp')
        p.scatter(self.xp, self.yp, c='k', s=0.2, zorder=4)
        if region != 'china':
            latmin, latmax, lonmin, lonmax = georange
            for point in self.realtime_data.data:
                if latmin <= point['la'] <= latmax and lonmin <= point['lo'] <= lonmax \
                        and point['id'][0] in '45':
                    p.marktext(point['lo'], point['la'],
                        '{}\n{}℃'.format(point['n'], point['t']), mark='',
                        family='Source Han Sans SC', fontsize=5, weight='medium')
        if region != 'taiwan':
            boxtext = '{}年{}月{}日{}时'
        else:
            boxtext = '{}年{}月{}日{}時'
        p.boxtext(boxtext.format(self.time.year, self.time.month, self.time.day,
            self.time.hour), family='Source Han Sans SC', zorder=5,
            fontsize=7, weight='medium')
        patch = PathPatch(self.china_polygon, transform=p.ax.transData)
        for col in cs.collections:
            col.set_clip_path(patch)
        if self._debug:
            filename = 'temp_{}_debug.png'.format(region)
        else:
            filename = 'temp_{}.png'.format(region)
        output_path = os.path.join(settings.MEDIA_ROOT, 'latest/weather/realtime',
            filename)
        p.save(output_path)
        p.clear()
        logger.info('Region plotted: {}'.format(region))


@shared_task(ignore_result=True)
def plot_realtime_map():
    RealTimeMapRoutine().go()

def _debug_plot():
    RealTimeMapRoutine(_debug=True).go()

def _debug_taiwan():
    routine = RealTimeMapRoutine(_debug=True)
    routine.set_time()
    routine.load_china()
    routine.plot_taiwan()
