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

    @property
    def path(self):
        return os.path.join(__warehouse__, self.pkey + '.mapset')

    @property
    def pkey(self):
        return self.key.replace(' ', '_').replace('&', '').lower()

    def load(self):
        return MapSet.load(self.path)

    def make(self, pc=False):
        mapset = MapSet.from_natural_earth(**self.kwargs)
        mapset.save(self.path)
        if pc:
            self.political_correctness()

    def thumbnail(self, target_path=None):
        if target_path is None:
            target_path = os.path.join(settings.MEDIA_ROOT,
                'maps/{}.png'.format(self.pkey))
        p = Plot(figsize=(2.4, 1.8), inside_axis=True, aspect='auto')
        p.usemapset(self.load())
        p.style('bom')
        p.draw('coastline country')
        p.save(target_path)
        p.clear()

    def political_correctness(self):
        """Maps without showing south tibet within China would face troubles. However,
        it's not easy to find politically correct shapefiles for international
        boundaries. So we have to do some work to rewrite the boundaries, which is a
        little bit hacky. As its side effect, border between India and Myanmar may be
        problematic."""
        from shapely.geometry import GeometryCollection, Polygon, box
        import pickle
        SOUTH_TIBET_POLYGON = [
            (91.55, 27.92),
            (91.49, 27.46),
            (92.09, 26.86),
            (93.94, 26.77),
            (97.03, 27.77),
            (97.12, 27.63),
            (97.79, 28.17),
            (96.15, 29.49),
            (94.68, 29.42),
        ]
        geobox = box(self.georange[2], self.georange[0], self.georange[3], self.georange[1])
        south_tibet = Polygon(SOUTH_TIBET_POLYGON)
        if not geobox.intersects(south_tibet):
            return
        mapset = self.load()
        geoms = mapset.country._geoms
        # remove south tibet region from original shapefile
        new_geoms = []
        for g in geoms:
            g = g.difference(south_tibet)
            if isinstance(g, GeometryCollection):
                # Cartopy doesnot support GeometryCollection, expand it
                new_geoms.extend(list(g))
            else:
                new_geoms.append(g)
        # load prepared offical boundary lines
        patch = pickle.load(open(os.path.join(__warehouse__,
            'south_tibet_patch.pkl'), 'rb'))
        new_geoms += patch
        mapset.country._geoms = new_geoms
        mapset.save(self.path)

    @classmethod
    def get(cls, s):
        return cls.maps.get(s)


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

def _make_all():
    for m in MapArea.maps.values():
        print(m)
        m.make()
        m.political_correctness()
        m.thumbnail()

def load_china_polygon():
    import cartopy.io.shapereader as ciosr
    from cartopy.mpl.patch import geos_to_path
    from matplotlib.path import Path
    filepath = os.path.join(__warehouse__, 'china_poly/China_Polygon.shp')
    china_polygon = list(ciosr.Reader(filepath).geometries())[0]
    paths = geos_to_path(china_polygon)
    codes = []
    vertices = []
    for path in paths:
        codes.extend(path.codes)
        vertices.extend(path.vertices)
    return Path(vertices, codes)


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

MAP_EASTASIA = MapArea('eastasia', georange=(7.5, 37.5, 102.5, 147.5), land=True)
MAP_MICRONESIA = MapArea('micronesia', georange=(2.5, 32.5, 130, 175), land=True)

MAP_ASIA = MapArea('Asia', georange=(-5, 80, 0, 200), proj='L', proj_params=dict(
    central_longitude=100, central_latitude=40, standard_parallels=(40, 40),
    map_georange=(5, 75, 55, 145)))
MAP_CHINA = MapArea('China', georange=(15, 55, 72.5, 137.5))
MAP_SOUTHEAST_CHINA = MapArea('Southeast China', georange=(15, 35, 97.5, 127.5))
MAP_NORTHEAST_CHINA = MapArea('Northeast China', georange=(30, 55, 100, 135))
MAP_WEST_CHINA = MapArea('West China', georange=(20, 50, 72.5, 112.5))
MAP_MID_CHINA = MapArea('Mid China', georange=(22.5, 42.5, 92.5, 122.5))
MAP_JAPAN_KOREA = MapArea('Japan & Korea', georange=(22, 47, 117.5, 152.5))
MAP_EAST_ASIAN_SEAS = MapArea('East Asian Seas', georange=(7.5, 37.5, 102.5, 147.5))
MAP_MICRONESIA_ALT = MapArea('Micronesia', georange=(2.5, 32.5, 130, 175), land=True)
MAP_WPAC_ALT = MapArea('Western Pacific', georange=(0, 55, 100, 190))
MAP_EPAC_ALT = MapArea('Eastern Pacific', georange=(0, 55, -175, -85))
MAP_NATL_ALT = MapArea('Northern Atlantic', georange=(0, 55, -100, -10))
MAP_NIO_ALT = MapArea('N Indian Ocean', georange=(-5, 35, 40, 110))
MAP_SIO_ALT = MapArea('S Indian Ocean', georange=(-40, 5, 35, 115))
MAP_AUS_ALT = MapArea('Southern Pacific', georange=(-40, 5, 110, 190))
__mapbooks__['*china'] = [MAP_CHINA, MAP_SOUTHEAST_CHINA, MAP_NORTHEAST_CHINA,
    MAP_WEST_CHINA, MAP_MID_CHINA]
__mapbooks__['*tropics'] = [MAP_EAST_ASIAN_SEAS, MAP_MICRONESIA_ALT, MAP_WPAC_ALT,
    MAP_EPAC_ALT, MAP_NATL_ALT, MAP_NIO_ALT, MAP_SIO_ALT, MAP_AUS_ALT]
