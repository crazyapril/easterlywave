import datetime
import logging

import requests

from tools.cache import Key

logger = logging.getLogger(__name__)
__NRLSECTOR__ = 'http://tropic.ssec.wisc.edu/real-time/amsu/herndon/new_sector_file'


class Storm:

    @classmethod
    def from_dict(cls, d):
        ins = cls()
        ins.__dict__.update(d)
        return ins

    def __repr__(self):
        return '<{}>'.format(self.code)


class StormSector:

    persist_hours = 12

    def __init__(self):
        self.storms = {}
        self.target = None
        self.update_time = None
        self.focus = None

    @classmethod
    def get_or_create(cls):
        instance = Key.get(Key.SECTOR_FILE)
        if instance:
            return instance
        return cls()

    def save(self):
        Key.set(Key.SECTOR_FILE, self, self.persist_hours * 3600)

    def update(self, raise_error=False):
        url = __NRLSECTOR__
        try:
            sectors = requests.get(url)
        except requests.HTTPError:
            return
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
                'timestr': data[2] + data[3], # 201903201200
                'time': datetime.datetime.strptime(data[2] + data[3], '%y%m%d%H%M'), # datetime object
                'latstr': data[4], # 19.1S
                'lat': lat, # -19.1
                'lonstr': data[5], # 83.5E
                'lon': lon, # 83.5
                'basin': data[6], # SHEM
                'wind': int(data[7]), # 40
                'pressure': int(data[8]), # 993
                'is_target': False, # Target Area Flag
                'in_scope': 100 <= lon <= 180,
                'is_invest': data[0].startswith('9'),
                'in_service': False
            }
            self.storms[data[0]] = Storm.from_dict(storm_dict)
        self.update_time = datetime.datetime.utcnow()

    def match_target(self, threshold=10):
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
            return
        if len(candidates) > 1:
            candidates.sort()
        code = candidates[0][1]
        self.storms[code].is_target = True
        self.target = code

    def get_focus(self):
        """Find which storm is the most likely to be focused on. Typically, it's a
        strong typhoon near China!"""
        NEAR_CHINA_BONUS = 50
        IN_WEST_PACIFIC_BONUS = 50
        IN_NORTH_ATLANTIC_BONUS = 20
        if not self.storms:
            return
        scores = []
        for code, storm in self.storms.items():
            score = storm['wind']
            if storm.lat >= 15 and 100 < storm.lon <= 130:
                score += NEAR_CHINA_BONUS
            if storm.basin == 'WPAC':
                score += IN_WEST_PACIFIC_BONUS
            elif storm.basin == 'ATL':
                score += IN_NORTH_ATLANTIC_BONUS
            scores.append((score, code))
        scores.sort(reverse=True)
        self.focus = scores[0][1]
        return self.focus

    def to_json(self):
        json = {
            'storms': self.storms,
            'target': self.target,
            'update_time': self.update_time.strftime('%Y%m%d%H%M'),
            'focus': self.focus
        }
        return json

    def fulldisk_service_storms(self, mark=True):
        storms = []
        for code, storm in self.storms.items():
            if all((storm.in_scope, not storm.is_invest, not storm.is_target)):
                storms.append(storm)
                if mark:
                    storm.in_service = True
        return storms
