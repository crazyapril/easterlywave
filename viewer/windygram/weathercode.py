# Weather symbol image courtesy of https://github.com/OGCMetOceanDWG/WorldWeatherSymbols
# font and code tools courtesy of MetPy

import logging
from .wxsymbols import current_weather, sky_cover, wx_code_map

logger = logging.getLogger(__name__)

class Cloud:
    picdict = {
        'SKC': 'SKC', # Sky clear (0/0)
        'CLR': 'SKC', # Clear (0/0)
        'FEW': 'FEW', # Few (1/10-2/10)
        'SCT': 'SCT', # ? (3/10-5/10)
        'BKN': 'BKN', # Broken (6/10-9/10)
        'OVC': 'OVC', # Overcast (10/10)
    }
    color = '#444444'
    level = 1
    directory = 'clouds'

    def __init__(self, cloud_group, low_group, high_group):
        self.cloud_group = cloud_group
        self.low_group = low_group
        self.high_group = high_group

    def to_code(self):
        return self.picdict.get(self.cloud_group, None)


class Obscuration:
    picdict = {
        'BR': 'BR', # Mist
        'FG': 'FG', # Fog
        'FU': 'FU', # Smoke
        'DU': 'DU', # Dust
        'SA': 'SA', # Sand
        'HZ': 'HZ', # Haze
        'PY': 'PY', # Spray
        'FZFG': 'FZFG', # Freezing Fog
    }
    level = 2
    directory = 'obscur'

    def __init__(self, group):
        self.group = group

    def to_code(self):
        if self.group in ('BR', 'FG', 'FZFG'):
            self.color = '#99D9EA'
        elif self.group in ('FU', 'HZ'):
            self.color = '#AAAAAA'
        else:
            self.color = '#FFF200'
        return self.picdict.get(self.group, None)


class Precipitation:
    directory = 'precip'
    level = 3

    def __init__(self, group, intensity, shower):
        self.group = group
        self.intensity = intensity
        self.shower = shower

    def to_code(self):
        if self.group == 'RA' or self.shower == 'TS':
            child = Rain
        elif self.group == 'SN':
            child = Snow
        elif self.group == 'DZ':
            child = Drizzle
        elif self.group in ('RASN', 'FZRA', 'PE', 'GS'):
            child = RainSnow
        else:
            return None
        self.color = child.color
        return child.from_parent(self).to_code()


class Rain(Precipitation):
    color = '#55CC33'

    @classmethod
    def from_parent(cls, parent):
        return cls(parent.group, parent.intensity, parent.shower)

    def to_code(self):
        basename = 'RA'
        if self.shower == 'SH':
            basename = 'SHRA'
        elif self.shower == 'TS':
            basename = 'TSRA'
        if self.intensity in ('+', '-'):
            basename = self.intensity + basename
        return basename


class Snow(Precipitation):
    color = '#AAAAAA'

    @classmethod
    def from_parent(cls, parent):
        return cls(parent.group, parent.intensity, parent.shower)

    def to_code(self):
        basename = 'SN'
        if self.shower == 'SH':
            basename = 'SHSN'
        elif self.shower == 'TS':
            basename = 'TSSN'
        if self.intensity in ('+', '-'):
            basename = self.intensity + basename
        return basename


class RainSnow(Precipitation):
    color = '#FF9090'

    @classmethod
    def from_parent(cls, parent):
        return cls(parent.group, parent.intensity, parent.shower)

    def to_code(self):
        basename = self.group
        return basename


class Drizzle(Precipitation):
    color = '#55CC33'

    @classmethod
    def from_parent(cls, parent):
        return cls(parent.group, parent.intensity, parent.shower)

    def to_code(self):
        basename = self.group
        if self.intensity in ('+', '-'):
            basename = self.intensity + basename
        return basename


class WeatherCode:

    def __init__(self):
        self.char = None
        self.color = None
        self.size = 5

    def set_code(self, code):
        self.code = code
        # logger.debug('CODE {}'.format(code))
        codes = self.code.split(',')
        cloud = Cloud(*codes[:3])
        obscur = Obscuration(codes[3])
        precip = Precipitation(codes[-3], codes[-4], codes[-2])
        for i in [precip, obscur, cloud]:
            code = i.to_code()
            if code is not None:
                self.color = i.color
                try:
                    real_code = wx_code_map[code]
                except KeyError:
                    logger.info('UNKNOWN CODE {}'.format(code))
                    continue
                if isinstance(i, Cloud):
                    self.char = sky_cover(real_code)
                    self.size = 5
                    return
                else:
                    self.char = current_weather(real_code)
                    self.size = 9
                    return
        self.char = None
