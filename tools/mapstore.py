import os

from django.conf import settings

from tools.metplot.plotplus import MapSet, Plot

__warehouse__ = os.path.join(os.path.dirname(__file__), 'mapstore')
__mapbooks__ = {}


class MapArea:

    maps = {}

    def __init__(self, key, **kwargs):
        self.maps[key] = self
        self.key = key
        self.kwargs = kwargs
        self.georange = kwargs['georange']

    def load(self):
        path = os.path.join(__warehouse__, self.key+'.mapset')
        return MapSet.load(path)

    def make(self):
        path = os.path.join(__warehouse__, self.key+'.mapset')
        mapset = MapSet.from_natural_earth(**self.kwargs)
        mapset.save(path)

    def thumbnail(self, target_path=None):
        if target_path is None:
            target_path = os.path.join(settings.MEDIA_ROOT,
                'maps/{}.png'.format(self.key))
        p = Plot(figsize=(2.4, 1.8), inside_axis=True, aspect='auto')
        p.usemapset(self.load())
        p.style('bom')
        p.draw('coastline country')
        p.save(target_path)
        p.clear()

    @classmethod
    def get(cls, s):
        return cls.maps.get(s.lower(), None)


def get_areas(area=None):
    if area is None:
        return list(MapArea.maps.values())
    if not isinstance(area, (tuple, list)):
        area = (area, )
    regions = []
    for region in area:
        if region in __mapbooks__:
            regions.extend(__mapbooks__[region])
        else:
            regions.append(MapArea.get(region))
    return regions

def get_area_keys(area=None):
    regions = get_areas(area)
    keys = [region.key for region in regions]
    return keys


MAP_TROPICS = MapArea('tropics', georange=(-40, 50, 30, 360), scale='110m',
    land=True, ocean=True)
MAP_WPAC = MapArea('wpac', georange=(0, 45, 100, 190), land=True, ocean=True)
MAP_EPAC = MapArea('epac', georange=(0, 45, -175, -85), land=True, ocean=True)
MAP_NATL = MapArea('natl', georange=(0, 45, -100, -10), land=True, ocean=True)
MAP_NIO = MapArea('nio', georange=(0, 35, 40, 110), land=True, ocean=True)
MAP_SIO = MapArea('sio', georange=(-35, 5, 35, 115), land=True, ocean=True)
MAP_AUS = MapArea('aus', georange=(-35, 5, 110, 190), land=True, ocean=True)

tropical_maps = [MAP_WPAC, MAP_EPAC, MAP_NATL, MAP_NIO, MAP_SIO, MAP_AUS]
tropical_mapkeys = [m.key for m in tropical_maps]

MAP_EASTASIA = MapArea('eastasia', georange=(10, 35, 105, 145), land=True)
MAP_MICRONESIA = MapArea('micronesia', georange=(5, 30, 130, 170), land=True)

MAP_ASIA = MapArea('asia', georange=(-5, 80, 0, 200), proj='L', proj_params=dict(
    central_longitude=100, central_latitude=40, standard_parallels=(40, 40),
    map_georange=(5, 75, 55, 145)))
MAP_CHINA = MapArea('china', georange=(10, 55, 70, 135))
