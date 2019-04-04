import os

from tools.metplot.plotplus import MapSet

__warehouse__ = os.path.join(os.path.dirname(__file__), 'mapstore')


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

    @classmethod
    def get(cls, s):
        return cls.maps.get(s.lower(), None)


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
