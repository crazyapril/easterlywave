import os

import numpy as np
from django.conf import settings
from pandas.core.common import flatten
from pybufrkit.dataquery import DataQuerent, NodePathParser
from pybufrkit.decoder import Decoder


MOVEMENT_LIMIT_IN_6_HOURS_LOW = 4
MOVEMENT_LIMIT_IN_6_HOURS_HIGH = 6


class BufrFile:

    CODE_LAT = '005002'
    CODE_LON = '006002'
    CODE_WIND = '011012'
    CODE_PRES = '010051'
    CODE_TIME = '004024'
    CODE_RADII = '019004'

    def __init__(self, filename=None, filepath=None):
        self.filename = filename
        self.loaded = False
        if self.filename is None:
            self.filepath = filepath
        else:
            self._analyze_filename()
        self._lons = []
        self._lats = []
        self._wind = []
        self._pres = []
        self._radii = None
        self.qc_method = None
        self.valid_points = 0

    def __repr__(self):
        return '<{}>'.format(self.codename)

    def file_exists(self):
        return os.path.exists(self.filepath)

    def _analyze_filename(self):
        segs = self.filename.split('_')
        self.emx_flag = 'X' if 'ECEP' not in segs[1] else 'E'
        self.num = int(segs[1][4:6])
        self.basetime = segs[4][:10]
        self.codename = segs[8]
        self.atcfname = None
        self.slon = float(segs[9][:-4].replace('p', '.'))
        self.slat = float(segs[10][:-4].replace('p', '.'))
        self.filepath = os.path.join(settings.TMP_ROOT, 'ecens/{}/{}.bufr'.format(
            self.basetime, self.codename))
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

    def load(self, qc_method='strict', force_reload=False):
        if self.loaded and self.qc_method == qc_method and not force_reload:
            return
        if not self.loaded:
            self.load_raw()
        np.seterr(invalid='ignore')
        if qc_method == 'strict':
            mask = self.quality_control_strict(self._lats, self._lons)
            self.valid_points = np.sum(~mask)
            self._lats_qc = self._lats.copy()
            self._lons_qc = self._lons.copy()
            self._wind_qc = self._wind.copy()
            self._pres_qc = self._pres.copy()
            self._lats_qc[mask] = np.nan
            self._lons_qc[mask] = np.nan
            self._wind_qc[mask] = np.nan
            self._pres_qc[mask] = np.nan
            self.qc_method = 'strict'
        elif qc_method == 'breakpoint':
            self.breakpoints = self.quality_control_breakpoint(self._lats,
                self._lons)
            self.valid_points = 0
            self.qc_method = 'breakpoint'
        else:
            raise IOError('Unknown qc method {}'.format(qc_method))

    def load_raw(self):
        with open(self.filepath, 'rb') as f:
            message = Decoder().process(f.read())
        queryer = DataQuerent(NodePathParser())
        for subset in range(52):
            # lat
            try:
                values = queryer.query(message, '@[{}] > {}'.format(subset,
                    self.CODE_LAT)).all_values()
                assert len(values[0]) > 3
            except (IndexError, AssertionError):
                raw_lats = np.empty(41)
                raw_lats[:] = np.nan
            else:
                raw_lats = np.array(values[0][3], dtype='float')[:,0]
                raw_lats = np.insert(raw_lats, 0, values[0][1])
                raw_lats = self.length_control(raw_lats)
            self._lats.append(raw_lats)
            # lon
            try:
                values = queryer.query(message, '@[{}] > {}'.format(subset,
                    self.CODE_LON)).all_values()
                assert len(values[0]) > 3
            except (IndexError, AssertionError):
                raw_lons = np.empty(41)
                raw_lons[:] = np.nan
            else:
                raw_lons = np.array(values[0][3], dtype='float')[:,0]
                raw_lons = np.insert(raw_lons, 0, values[0][1])
                raw_lons = self.length_control(raw_lons)
            raw_lons[raw_lons<0] = raw_lons[raw_lons<0] + 360
            self._lons.append(raw_lons)
            # wind
            try:
                values = queryer.query(message, '@[{}] > {}'.format(subset,
                    self.CODE_WIND)).all_values(flat=True)
                assert len(values) > 0
            except (IndexError, AssertionError):
                raw_wind = np.empty(41)
                raw_wind[:] = np.nan
            else:
                raw_wind = np.array(values[0], dtype='float') * 1.94 # to kt
                raw_wind = self.length_control(raw_wind)
            self._wind.append(raw_wind)
            # pres
            try:
                values = queryer.query(message, '@[{}] > {}'.format(subset,
                    self.CODE_PRES)).all_values(flat=True)
                assert len(values) > 0
            except (IndexError, AssertionError):
                raw_pres = np.empty(41)
                raw_pres[:] = np.nan
            else:
                raw_pres = np.array(values[0], dtype='float') / 100 # to hPa
                raw_pres = self.length_control(raw_pres)
            self._pres.append(raw_pres)
        self._lats = np.vstack(self._lats)
        self._lons = np.vstack(self._lons)
        self._wind = np.vstack(self._wind)
        self._pres = np.vstack(self._pres)
        self.loaded = True

    def load_radii(self):
        members = 52
        hours = 41
        levels = 3
        directions = 4
        member_size = hours * levels * directions
        member_shape = hours, levels, directions
        self._radii = np.zeros((members, hours, levels, directions))
        with open(self.filepath, 'rb') as f:
            message = Decoder().process(f.read())
        queryer = DataQuerent(NodePathParser())
        for subset in range(52):
            try:
                query = '@[{}] > {}'.format(subset, self.CODE_RADII)
                values = list(flatten(queryer.query(message, query).all_values()))
                assert len(values) > 10
            except (IndexError, AssertionError):
                pass
            else:
                radius = self.length_control(np.array(values, dtype='float'),
                                             member_size).reshape(member_shape)
                radius[np.isnan(radius)] = 0
                radius[radius < 1000] = 0
                self._radii[subset, ...] = radius
        return self._radii

    def quality_control_strict(self, lats, lons):
        '''Quality control and filter messy ECMWF raw bufr data.'''
        # First step: calculate movement distances between points, if the distance
        # is beyond a reasonable range, then all subsequent points are labelled as
        # invalid.
        distance = np.diff(lats) ** 2 + np.diff(lons) ** 2
        distance_illegal = distance > MOVEMENT_LIMIT_IN_6_HOURS_LOW ** 2
        distance_mask = np.cumsum(distance_illegal, axis=1).astype(bool)
        # ----
        # Second step: mark nan values as invalid. Note: the starting values can be
        # nan as the storm may have not formed yet. So different from first step,
        # we should find first nonnan values stretch, then label all remaining data
        # as invalid.
        nan_data_legal = np.pad(np.isnan(distance), ((0,0),(1,0)), 'constant',
            constant_values=True)
        nan_diff = np.diff(nan_data_legal.astype(int), axis=1)
        on_range = np.cumsum(nan_diff < 0, axis=1).astype(bool)
        off_range = np.cumsum(nan_diff > 0, axis=1).astype(bool)
        nan_data_mask = ~on_range | off_range
        # ----
        # temporary solution
        # nan_data_illegal = np.isnan(distance)
        # nan_data_mask = np.cumsum(nan_data_illegal, axis=1).astype(bool)
        # Combine two masks to create final mask.
        total_mask = np.pad((distance_mask | nan_data_mask), ((0,0),(1,0)), 'edge')
        return total_mask

    def quality_control_breakpoint(self, lats, lons):
        latnan = np.isnan(lats[:, 1:])
        lonnan = np.isnan(lons[:, 1:])
        distance = np.diff(lats) ** 2 + np.diff(lons) ** 2
        distance_illegal = distance > MOVEMENT_LIMIT_IN_6_HOURS_HIGH ** 2
        return distance_illegal | latnan | lonnan

    def length_control(self, arr, fixed_length=41):
        if arr.shape[0] < fixed_length:
            arr = np.pad(arr, (0, fixed_length - arr.shape[0]), 'constant',
                constant_values=np.nan)
        elif arr.shape[0] > fixed_length:
            arr = arr[:fixed_length]
        return arr

    def set_hour_range(self, hours):
        index = hours // 6 + 1
        self._lats[:, index:] = np.nan
        self._lons[:, index:] = np.nan
        self._wind[:, index:] = np.nan
        self._pres[:, index:] = np.nan
        self._maxwind = np.nanmax(self._wind, axis=1)
        self._minpres = np.nanmin(self._pres, axis=1)

    def iter_members(self):
        for i in range(50):
            if self.qc_method == 'strict':
                flag = self.prepare_data_strict(i)
            elif self.qc_method == 'breakpoint':
                flag = self.prepare_data_breakpoint(i)
            if flag:
                continue
            code = 'M{:02d}'.format(i + 1)
            yield code

    def prepare_data_strict(self, i):
        mask = np.isnan(self._lats_qc[i, :]) | np.isnan(self._lons_qc[i, :]) | \
            np.isnan(self._wind_qc[i, :]) | np.isnan(self._pres_qc[i, :])
        self.lats = self._lats_qc[i, :][~mask]
        self.lons = self._lons_qc[i, :][~mask]
        self.wind = self._wind_qc[i, :][~mask]
        self.pres = self._pres_qc[i, :][~mask]
        if np.all(np.isnan(self.lats)):
            return True
        try:
            self.maxwind = self.wind.max()
        except ValueError:
            self.maxwind = None
        try:
            self.minpres = self.pres.min()
        except ValueError:
            self.minpres = None
        if self._radii is not None:
            self.radii = self._radii[i, ...][~mask]
        return False

    def prepare_data_breakpoint(self, i):
        mask = np.isnan(self._lats[i, :]) | np.isnan(self._lons[i, :]) | \
            np.isnan(self._wind[i, :]) | np.isnan(self._pres[i, :])
        self.lats = self.break_with_mask(self._lats[i, :],
            self.breakpoints[i, :], mask)
        self.lons = self.break_with_mask(self._lons[i, :],
            self.breakpoints[i, :], mask)
        self.wind = self.break_with_mask(self._wind[i, :],
            self.breakpoints[i, :], mask)
        self.pres = self.break_with_mask(self._pres[i, :],
            self.breakpoints[i, :], mask)
        self.maxwind = None
        self.minpres = None
        return len(self.lats) == 0

    def break_with_mask(self, inp, breakpoints, mask):
        bp_indices = np.where(breakpoints)[0] + 1
        inp_segs = np.split(inp, bp_indices)
        mask_segs = np.split(mask, bp_indices)
        broken_segs = [inp_seg[~mask_seg] for inp_seg, mask_seg in \
            zip(inp_segs, mask_segs) if len(inp_seg[~mask_seg]) > 1]
        return broken_segs

    def set_data_pointer(self, code):
        if self.qc_method != 'strict':
            raise NotImplementedError('Data pointer only support strict mode.')
        if isinstance(code, int):
            i = code
        elif code == 'EC00':
            i = 50
        elif code == 'EMX':
            i = 51
        elif code == 'EEMN':
            i = 52
        elif code.startswith('M'):
            i = int(code[2:]) - 1
        self.prepare_data_strict(i)

    def get_georange(self):
        latmax = np.nanmax(self._lats)
        latmin = np.nanmin(self._lats)
        lonmax = np.nanmax(self._lons)
        lonmin = np.nanmin(self._lons)
        return latmin, latmax, lonmin, lonmax
