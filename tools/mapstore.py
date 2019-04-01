import os
import pickle

__warehouse__ = os.path.join(os.path.dirname(__file__), 'mapstore')


class MapArea:

    def __init__(self):
        pass

    def load(self, key):
        pass


class BasemapMapArea(MapArea):

    maps = {}

    def __init__(self, key, **kwargs):
        self.key = key
        if 'georange' in kwargs:
            georange = kwargs.pop('georange')
            kwargs.update(llcrnrlat=georange[0], urcrnrlat=georange[1], llcrnrlon=georange[2],
                urcrnrlon=georange[3])
            self.georange = georange
        self.kwargs = kwargs
        self.maps[key] = self

    @classmethod
    def _get_filename_by_key(cls, key):
        return os.path.join(__warehouse__, key + '.map')

    @classmethod
    def load_map(cls, key):
        try:
            return pickle.load(open(cls._get_filename_by_key(key), 'rb'))
        except FileNotFoundError:
            return None

    @classmethod
    def delete(cls, key):
        os.remove(cls._get_filename_by_key(key))

    def load(self):
        return self.load_map(self.key)

    def make(self):
        from mpl_toolkits.basemap import Basemap
        m = Basemap(**self.kwargs)
        pickle.dump(m, open(self._get_filename_by_key(self.key), 'wb'))

    @classmethod
    def make_all(cls):
        for instance in cls.maps.values():
            instance.make()


class CartopyMapArea(MapArea):

    maps = {}

    def __init__(self, key, **kwargs):
        if 'georange' in kwargs:
            self.georange = kwargs.pop('georange')
        self.maps[key] = self


MAP_TROPICS = BasemapMapArea('tropics', projection='cyl', resolution='l',
    georange=(-40, 50, 30, 360))
MAP_WPAC = BasemapMapArea('westpac', projection='cyl', resolution='l',
    georange=(0, 45, 100, 190))
MAP_EPAC = BasemapMapArea('eastpac', projection='cyl', resolution='l',
    georange=(0, 45, 185, 275))
MAP_NATL = BasemapMapArea('northatl', projection='cyl', resolution='l',
    georange=(0, 45, 260, 350))
MAP_NIO = BasemapMapArea('northio', projecrion='cyl', resolution='l',
    georange=(0, 35, 40, 110))
MAP_SIO = BasemapMapArea('southio', projection='cyl', resolution='l',
    georange=(-35, 5, 35, 115))
MAP_AUS = BasemapMapArea('australia', projection='cyl', resolution='l',
    georange=(-35, 5, 110, 190))
tropical_maps = [MAP_TROPICS, MAP_WPAC, MAP_EPAC, MAP_NATL, MAP_NIO, MAP_SIO, MAP_AUS]
