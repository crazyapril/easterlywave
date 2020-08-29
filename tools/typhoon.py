import datetime
import logging
import re
from io import StringIO

import numpy as np
import requests

from tools.cache import Key

logger = logging.getLogger(__name__)
__NRLSECTOR__ = 'https://www.nrlmry.navy.mil/tcdat/sectors/atcf_sector_file'
#__NRLSECTOR__ = 'http://tropic.ssec.wisc.edu/real-time/amsu/herndon/new_sector_file'
__JTWCFILES__ = 'https://www.metoc.navy.mil/jtwc/products/'

bdeck_sources = [
    'https://www.ssd.noaa.gov/PS/TROP/DATA/ATCF/JTWC/',
    'http://hurricanes.ral.ucar.edu/repository/data/bdecks_open/'
]

_basin_codes = {
    'W': ('WP', 'WPAC'),
    'S': ('SH', 'SHEM'),
    'L': ('AL', 'ATL'),
    'E': ('EP', 'EPAC'),
    'C': ('CP', 'CPAC'),
    'P': ('SH', 'SHEM'),
    'B': ('IO', 'NIO'),
    'A': ('IO', 'NIO'),
    'Q': ('AL', 'ATL')
}


def get_sshws_category(w):
    if w > 137:
        c = 'C5'
    elif w > 114:
        c = 'C4'
    elif w > 96:
        c = 'C3'
    elif w > 83:
        c = 'C2'
    elif w > 64:
        c = 'C1'
    elif w > 34:
        c = 'TS'
    elif w > 24:
        c = 'TD'
    else:
        c = 'DB'
    return c

latcvt = lambda x: -float(x[:-1]) if x[-1] != 'N' else float(x[:-1])
loncvt = lambda x: 360 - float(x[:-1]) if x[-1] != 'E' else float(x[:-1])

class Storm:

    @classmethod
    def from_dict(cls, d):
        ins = cls()
        ins.__dict__.update(d)
        return ins

    def __repr__(self):
        return '<{}>'.format(self.code)

    def __lt__(self, value):
        return False

    def __le__(self, value):
        return True

    def __gt__(self, value):
        return False

    def __ge__(self, value):
        return True

    def __eq__(self, value):
        return True

    def __ne__(self, value):
        return False

    @property
    def basin2(self):
        return _basin_codes[self.basin_short][0].lower()

    @property
    def guess_year(self):
        year = self.time.year
        if self.basin2 == 'sh' and self.time.month >= 8:
            # For southern hemisphere, a tropical cyclone season begins at Aug 1st
            year += 1
        return year

    @property
    def bdeck_code(self):
        bdeck_code = 'b{}{}{}'.format(self.basin2, self.code[:2], self.guess_year).lower()
        return bdeck_code

    @property
    def code_full(self):
        try:
            full_code = '{}{}{}'.format(self.basin2, self.code[:2], self.guess_year).upper()
        except:
            full_code = ''
        return full_code

    def update_tracks(self):
        self.bdeck = None
        # if self.basin_short in 'AB':
        #     # North Indian Ocean cyclones
        #     source = __SSDDECKFILES__
        # else:
        #     source = __DECKFILES__
        updated = False
        for source in bdeck_sources:
            url = '{}{}.dat'.format(source, self.bdeck_code)
            try:
                request = requests.get(url)
            except (requests.ConnectionError, requests.HTTPError, requests.Timeout):
                continue
            if request.status_code != 200:
                continue
            updated = True
            break
        if not updated:
            return
        self.bdeck = BDeck.decode(request.text)
        self.max_wind = self.bdeck['wind'].max()
        self.min_pres = self.bdeck['pres'].min()
        logger.info('Storm {} ({}) tracks updated.'.format(self.code, self.name))

    def update_jtwc_forecast(self):
        self.jtwc_forecast = None
        url = '{}{}{}web.txt'.format(__JTWCFILES__, self.bdeck_code[1:5],
            self.bdeck_code[-2:])
        try:
            request = requests.get(url, timeout=5)
        except (requests.ConnectionError, requests.HTTPError, requests.Timeout):
            return
        full_times = [0, 12, 24, 36, 48, 72, 96, 120]
        lats, lons = [], []
        coord_strs = re.findall(r'--- (?:NEAR )?(\d+.\d[NS]) (\d+.\d[EW])', request.text)
        for latstr, lonstr in coord_strs:
            lats.append(latcvt(latstr))
            lons.append(loncvt(lonstr))
        wind_strs = re.findall(r'MAX SUSTAINED WINDS - (\d+) KT', request.text)
        timestr = re.search('PGTW (\d{6})', request.text)
        if timestr:
            time = datetime.datetime.strptime(timestr.group(1), '%d%H%M')
            utcnow = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
            time = time.replace(year=utcnow.year, month=utcnow.month)
        else:
            time = None
        self.jtwc_forecast = {
            'lats': lats,
            'lons': lons,
            'winds': list(map(int, wind_strs)),
            'times': full_times[:len(lats)],
            'time': time
        }

    def to_json(self):
        _attrs = ('code', 'name', 'latstr', 'lonstr', 'basin', 'wind', 'pressure',
            'is_target', 'in_scope', 'is_invest', 'in_service')
        json = {a:getattr(self, a) for a in _attrs}
        json['time'] = self.time.strftime('%m/%d %H%MZ')
        json['code_full'] = self.code_full
        return json


class StormSector:

    persist_hours = 12

    def __init__(self):
        self.storms = {}
        self._ranked_storms = None
        self.target = None
        self.update_time = None
        self.focus = None

    @classmethod
    def get_or_create(cls):
        instance = Key.get(Key.SECTOR_FILE)
        if instance:
            return instance
        instance = cls()
        instance.update()
        instance.save()
        return instance

    @classmethod
    def refresh(cls):
        Key.delete(Key.SECTOR_FILE)
        instance = cls()
        instance.update()
        instance.save()
        return instance

    def save(self):
        Key.set(Key.SECTOR_FILE, self, self.persist_hours * 3600)

    def update(self, raise_error=False):
        logger.info('Sector update begins.')
        url = __NRLSECTOR__
        try:
            sectors = requests.get(url, timeout=5)
        except (requests.ConnectionError, requests.HTTPError, requests.Timeout):
            return
        now_time = datetime.datetime.utcnow()
        self.storms = {}
        for storm_line in sectors.text.split('\n'):
            if not storm_line:
                continue
            data = storm_line.split()
            lat, lon = float(data[4][:-1]), float(data[5][:-1])
            if data[4][-1] == 'S':
                lat = -lat
            if data[5][-1] == 'W':
                lon = 360 - lon
            storm_dict = {
                'code': data[0], # 19S
                'name': data[1], # SAVANNAH
                'timestr': data[2] + data[3], # 1903201200
                'time': datetime.datetime.strptime(data[2] + data[3], '%y%m%d%H%M'), # datetime object
                'latstr': data[4], # 19.1S
                'lat': lat, # -19.1
                'lonstr': data[5], # 83.5E
                'lon': lon, # 83.5
                'basin': data[6], # SHEM
                'basin_short': data[0][-1], # S
                'wind': int(data[7]), # 40
                'pressure': int(data[8]), # 993
                'is_target': False, # Target Area Flag
                'in_scope': 100 <= lon <= 180,
                'is_invest': data[0].startswith('9'),
                'in_service': False
            }
            if storm_dict['basin_short'] not in 'ABCELPQSW':
                continue
            if now_time - storm_dict['time'] > datetime.timedelta(hours=22):
                # outdated entry
                continue
            self.storms[data[0]] = Storm.from_dict(storm_dict)
            logger.info('Storm {} ({}): {}kt {},{}'.format(data[0], data[1], data[7],
                data[4], data[5]))
        self.update_time = now_time
        self.rank_storms()

    def match_target(self, threshold=5):
        """Match the storm which target area is focused on."""
        refpoint = Key.get(Key.TARGET_AREA_MIDPOINT)
        if refpoint is None:
            return
        reflon, reflat = refpoint
        candidates = []
        for storm in self.storms.values():
            distance = (storm.lat - reflat) ** 2 + (storm.lon - reflon) ** 2
            if distance < threshold ** 2:
                candidates.append((distance, storm.code))
        if len(candidates) == 0:
            self.target = None
        else:
            candidates.sort()
            code = candidates[0][1]
            self.target = code
        for storm in self.storms:
            self.storms[storm].is_target = storm == self.target

    def rank_storms(self):
        """Find which storm is the most likely to be focused on. Typically, it's a
        strong typhoon near China!"""
        VERY_NEAR_CHINA_BONUS = 30
        NEAR_CONTINENT_MAX_BONUS = 50
        IN_WEST_PACIFIC_BONUS = 30
        IN_NORTH_ATLANTIC_BONUS = 20
        scores = []
        for storm in self.storms.values():
            score = storm.wind
            if storm.lat >= 15 and 100 < storm.lon <= 130:
                score += VERY_NEAR_CHINA_BONUS
            if storm.basin == 'WPAC':
                score += IN_WEST_PACIFIC_BONUS
                # The closer storm is to 120E, the higher the bonus.
                continent_lon = 120
                residual = abs(storm.lon - continent_lon)
                score += int(residual / 60 * NEAR_CONTINENT_MAX_BONUS)
            elif storm.basin == 'ATL':
                score += IN_NORTH_ATLANTIC_BONUS
            scores.append((score, storm))
        scores.sort(reverse=True)
        if len(scores) > 0:
            self._ranked_storms = [r[1] for r in scores]
            self.focus = self._ranked_storms[0].code
        else:
            self._ranked_storms = []
            self.focus = None

    @property
    def ranked_storms(self):
        return [r.to_json() for r in self._ranked_storms]

    def to_json(self):
        json = {
            'storms': self.ranked_storms,
            'target': self.target,
            'update_time': self.update_time.strftime('%Y/%m/%d %H%MZ'),
            'focus': self.focus
        }
        return json

    def fulldisk_service_storms(self, mark=True):
        storms = []
        for code, storm in self.storms.items():
            if all((storm.in_scope, not storm.is_target)):
                storms.append(storm)
                if mark:
                    storm.in_service = True
        self.rank_storms()
        return storms

    def has_storm_in_scope(self):
        for storm in self.storms.values():
            if storm.in_scope:
                return True
        return False


class BDeck:

    atcf_dtypes = ('U2', 'U2', 'U10', 'S3', 'U4', 'u1', 'f4', 'f4', 'u1', 'u2',
        'U2', 'S3', 'S3', 'S4', 'S4', 'S4', 'S4')
    atcf_names = ('basin', 'num', 'time', 'a', 'code', 'fcsthour', 'lat', 'lon',
        'wind', 'pres', 'category', 'c', 'd', 'e', 'f', 'g', 'h')
    atcf_usecols = ('time', 'lat', 'lon', 'wind', 'pres', 'category')
    latcvt = lambda x: -int(x[:-1])/10 if x[-1] != 78 else int(x[:-1])/10
    loncvt = lambda x: 360 - int(x[:-1])/10 if x[-1] != 69 else int(x[:-1])/10

    @classmethod
    def decode(cls, text):
        data = np.unique(np.genfromtxt(StringIO(text), dtype=cls.atcf_dtypes,
            names=cls.atcf_names, usecols=cls.atcf_usecols, autostrip=True,
            converters={'lat':cls.latcvt, 'lon':cls.loncvt}, delimiter=','))
        return data
