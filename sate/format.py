import numpy as np
import bz2
import pyproj

from tools.utils import execute


GEOS_HEIGHT = 35785831
HIM8_SUBLON = 140.8
SCLUNIT = 2 ** -16
LOFF = 2750.5
LFAC = 20466275
REF_LINES = 5500
SEG_LINES = REF_LINES // 10


def get_segno(georange):
    latmin, latmax, lonmin, lonmax = georange
    if lonmin < HIM8_SUBLON < lonmax:
        lons = np.array([lonmin, HIM8_SUBLON, lonmax], dtype=float)
    else:
        lons = np.array([lonmin, lonmax], dtype=float)
    if latmin < 0 < latmax:
        lats = np.array([latmin, 0, latmax], dtype=float)
    else:
        lats = np.array([latmin, latmax], dtype=float)
    lons, lats = np.meshgrid(lons, lats)
    proj = pyproj.Proj(proj='geos', h=GEOS_HEIGHT, lon_0=HIM8_SUBLON, ellps='WGS84', sweep='y')
    xs, ys = proj(lons, lats)
    column = (np.rad2deg(xs / GEOS_HEIGHT * LFAC * SCLUNIT) + LOFF).astype(np.int)
    line = (np.rad2deg(-ys / GEOS_HEIGHT * LFAC * SCLUNIT) + LOFF).astype(np.int)
    vlines = (line.min() - 3) / REF_LINES, (line.max() + 3) / REF_LINES
    segs = list(range(int(vlines[0] * 10) + 1, int(vlines[1] * 10) + 2))
    vcols = (column.min() - 3) / REF_LINES, (column.max() + 3) / REF_LINES
    return segs, vlines, vcols


class HimawariFormat:

    def __init__(self, filename):
        self.filename = filename

    def load(self, decompress=False):
        """Load file and read meta data."""
        hsd = {}
        if decompress:
            self.decompress()
        if self.filename.endswith('.bz2'):
            self.f = bz2.open(self.filename, mode='rb')
        else:
            self.f = open(self.filename, mode='rb')
        hsd['BLOCK_01'] = np.frombuffer(self.f.read(282), dtype=self._BLOCK_01)
        hsd['BLOCK_02'] = np.frombuffer(self.f.read(50), dtype=self._BLOCK_02)
        hsd['BLOCK_03'] = np.frombuffer(self.f.read(127), dtype=self._BLOCK_03)
        self.leap_block(self.f, 1)
        hsd['BLOCK_05'] = np.frombuffer(self.f.read(35), dtype=self._BLOCK_05)
        if hsd['BLOCK_05']['BandNumber'] <= 6:
            hsd['VisibleBand'] = np.frombuffer(self.f.read(112), dtype=self._VisibleBand)
        else:
            hsd['InfraredBand'] = np.frombuffer(self.f.read(112), dtype=self._InfraredBand)
        self.leap_block(self.f, 1)
        hsd['BLOCK_07'] = np.frombuffer(self.f.read(47), dtype=self._BLOCK_07)
        self.leap_block(self.f, 4)
        self.lines = hsd['BLOCK_02']['NumberOfLines'].item()
        self.columns = hsd['BLOCK_02']['NumberOfColumns'].item()
        self.first_lineno = hsd['BLOCK_07']['FirstLineNumber'].item()
        self.loff = hsd['BLOCK_03']['LOFF'].item()
        self.coff = hsd['BLOCK_03']['COFF'].item()
        self.lfac = hsd['BLOCK_03']['LFAC'].item()
        self.cfac = hsd['BLOCK_03']['CFAC'].item()
        self.first_colno = 0
        self.hsd = hsd

    def decompress(self):
        """Decompress file to raw binary file."""
        import os
        if not self.filename.endswith('.bz2'):
            return
        filename = self.filename[:-4]
        if not os.path.exists(filename):
            execute('bzip2 -d {}'.format(self.filename))
        self.filename = filename

    def extract(self, vline=None, vcol=None):
        self.load()
        return self.calibration(self._extract(vline=vline, vcol=vcol))

    def get_geocoord(self):
        return self.get_lonlat()

    def _extract(self, vline=None, vcol=None):
        """extract raw data from file.

        `vline` and `vcol` are relative position of desired window in the raw data, both
        in a format of tuple (start_ratio, end_ratio). When they are both `None`, full
        range data is returned.
        If source file is compressed, traditional line-picking method is deployed. If not,
        we will use memory map to precisely extract desired window, consuming less memory
        than line-picking method. (Compressed file does not support memory map.)
        """
        # Get virtual line/column numbers from vline/vcol. `Virtual` means data may
        # consist of two or more segments, and the line number may be not in this
        # segment. If vline/vcol is None, it will return entire lines/columns in
        # this segment.
        if vline is None:
            virtual_first_lineno = self.first_lineno
            virtual_end_lineno = self.lines + self.first_lineno
        else:
            virtual_first_lineno = int(vline[0] * self.lines * 10) # 10 segments
            virtual_end_lineno = int(vline[1] * self.lines * 10)
        if vcol is None:
            first_column = 0
            end_column = self.columns
        else:
            first_column = int(self.columns * vcol[0])
            end_column = int(self.columns * vcol[1])
        # `self.first_lineno` and `end_lineno` is the line number of starting and
        # ending line in this segment, respectively.
        end_lineno = self.first_lineno + self.lines
        # Compare virtual and local line numbers, get actual line number of starting
        # and ending line to extract from this segment.
        if end_lineno < virtual_first_lineno or virtual_end_lineno < self.first_lineno:
            #  LFirst < LEnd < VFirst < VEnd or VFirst < VEnd < LFirst < LEnd
            return np.array([])
        if self.first_lineno >= virtual_first_lineno:
            #  VFirst < LFirst
            actual_first_lineno = 0
        else:
            #  LFirst < VFirst
            actual_first_lineno = virtual_first_lineno - self.first_lineno
        if end_lineno >= virtual_end_lineno:
            #  VEnd < LEnd
            actual_lines = virtual_end_lineno - self.first_lineno - actual_first_lineno
        else:
            # LEnd < VEnd
            actual_lines = end_lineno - self.first_lineno - actual_first_lineno
        # Extracting by memory map method or line picking method
        if not isinstance(self.f, bz2.BZ2File):
            # Memory map method does not load data until the last step, therefore we do not
            # need to read entire columns into memory.
            offset = self.f.tell() + actual_first_lineno * self.columns * 2
            mmap = np.memmap(self.f, dtype='uint16', mode='r',
                shape=(actual_lines, self.columns), offset=offset)
            data = np.array(mmap[:, first_column:end_column])
        else:
            # Jump straight to the line where desired window begins, read least lines
            # possible and then reshape and index it to get the desired window.
            self.f.seek(actual_first_lineno * self.columns * 2, 1)
            data = np.frombuffer(self.f.read(actual_lines * self.columns * 2),
                dtype='uint16').reshape((actual_lines, self.columns))[:, first_column:end_column]
        self.f.close()
        return data

    def calibration(self, raw):
        if self.hsd['BLOCK_05']['BandNumber'] <= 6:
            return self.vis_calibration(raw)
        else:
            return self.ir_calibration(raw)

    def ir_calibration(self, raw):
        hsd = self.hsd
        lam = hsd['BLOCK_05']['CentralWaveLength'] * 1e-6
        gain = hsd['BLOCK_05']['Gain']
        const = hsd['BLOCK_05']['Constant']
        c = hsd['InfraredBand']['c']
        k = hsd['InfraredBand']['k']
        h = hsd['InfraredBand']['h']
        c0 = hsd['InfraredBand']['c0']
        c1 = hsd['InfraredBand']['c1']
        c2 = hsd['InfraredBand']['c2']
        const1 = h * c / (k * lam)
        const2 = 2 * h * np.power(c, 2) * np.power(lam, -5)
        I = (gain * raw + const) * 1e6
        EBT = const1 / np.log1p(const2 / I)
        return c0 + c1 * EBT + c2 * np.power(EBT, 2) - 273.15

    def vis_calibration(self, raw):
        gain = self.hsd['BLOCK_05']['Gain']
        const = self.hsd['BLOCK_05']['Constant']
        c = self.hsd['VisibleBand']['c*']
        return c * gain * raw + c * const

    def get_lonlat(self):
        hsd = self.hsd
        DEGTORAD = np.pi / 180.
        RADTODEG = 180. / np.pi
        SCLUNIT = 2 ** -16
        HEIGHT = (hsd['BLOCK_03']['Distance'] - hsd['BLOCK_03']['EarthEquatorialRadius'])[0] * 1000
        SUBLON = hsd['BLOCK_03']['SubLon'][0]
        #Calculation
        lines = np.arange(self.first_lineno, self.first_lineno + self.lines)
        columns = np.arange(self.first_colno, self.first_colno + self.columns)
        xx, yy = np.meshgrid(columns, lines)
        x = DEGTORAD * HEIGHT * (xx - hsd['BLOCK_03']['COFF']) / \
            (SCLUNIT * hsd['BLOCK_03']['CFAC'])
        y = -DEGTORAD * HEIGHT * (yy - hsd['BLOCK_03']['LOFF']) / \
            (SCLUNIT * hsd['BLOCK_03']['LFAC'])
        projection = pyproj.Proj(proj='geos', h=HEIGHT, ellps='WGS84', lon_0=SUBLON, sweep='y')
        lons, lats = projection(x, y, inverse=True)
        lons = np.ma.masked_outside(lons, -360., 360.)
        lats = np.ma.masked_outside(lats, -90., 90.)
        return lons, lats

    def leap_block(self, f, n):
        for i in range(n):
            tmparr = np.frombuffer(f.read(3), dtype=self._Header)
            f.seek(tmparr['BlockLength'].item()-3, 1)

    _BLOCK_01 = np.dtype([('HeaderBlockNumber', 'u1'),
                     ('BlockLength', 'u2'),
                     ('TotalNumberOfHeaderBlocks', 'u2'),
                     ('ByteOrder', 'u1'),
                     ('SatelliteName', 'a16'),
                     ('ProcessingCenterName', 'a16'),
                     ('ObservationArea', 'a4'),
                     ('OtherObservationInformation', 'a2'),
                     ('ObservationTimeline', 'u2'),
                     ('ObservationStartTime', 'f8'),
                     ('ObservationEndTime', 'f8'),
                     ('FileCreationTime', 'f8'),
                     ('TotalHeaderLength', 'u4'),
                     ('TotalDataLength', 'u4'),
                     ('QualityFlag1', 'u1'),
                     ('QualityFlag2', 'u1'),
                     ('QualityFlag3', 'u1'),
                     ('QualityFlag4', 'u1'),
                     ('FileFormatVersion', 'a32'),
                     ('FileName', 'a128'),
                     ('Spare', 'a40')])

    _BLOCK_02 = np.dtype([('HeaderBlockNumber', 'u1'),
                     ('BlockLength', 'u2'),
                     ('NumberOfBitsPerPixel', 'u2'),
                     ('NumberOfColumns', 'u2'),
                     ('NumberOfLines', 'u2'),
                     ('CompressionFlag', 'u1'),
                     ('Spare', 'a40')])

    _BLOCK_03 = np.dtype([('HeaderBlockNumber', 'u1'),
                     ('BlockLength', 'u2'),
                     ('SubLon', 'f8'),
                     ('CFAC', 'u4'),
                     ('LFAC', 'u4'),
                     ('COFF', 'f4'),
                     ('LOFF', 'f4'),
                     ('Distance', 'f8'),
                     ('EarthEquatorialRadius', 'f8'),
                     ('EarthPolarRadius', 'f8'),
                     ('EarthConst1', 'f8'),
                     ('EarthConst2', 'f8'),
                     ('EarthConst3', 'f8'),
                     ('EarthConstStd', 'f8'),
                     ('ResamplingTypes', 'u2'),
                     ('ResamplingSize', 'u2'),
                     ('Spare', 'a40')])

    _BLOCK_05 = np.dtype([('HeaderBlockNumber', 'u1'),
                     ('BlockLength', 'u2'),
                     ('BandNumber', 'u2'),
                     ('CentralWaveLength', 'f8'),
                     ('ValidNumberOfBitsPerPixel', 'u2'),
                     ('CountValueOfErrorPixels', 'u2'),
                     ('CountValueOfPixelsOutsideScanArea', 'u2'),
                     ('Gain', 'f8'),
                     ('Constant', 'f8')])

    _InfraredBand = np.dtype([('c0', 'f8'),
                         ('c1', 'f8'),
                         ('c2', 'f8'),
                         ('C0', 'f8'),
                         ('C1', 'f8'),
                         ('C2', 'f8'),
                         ('c', 'f8'),
                         ('h', 'f8'),
                         ('k', 'f8'),
                         ('Spare', 'a40')])

    _VisibleBand = np.dtype([('c*', 'f8'),
                        ('Spare', 'a104')])

    _BLOCK_07 = np.dtype([('HeaderBlockNumber', 'u1'),
                     ('BlockLength', 'u2'),
                     ('TotalNumberOfSegments', 'u1'),
                     ('SegmentSequenceNumber', 'u1'),
                     ('FirstLineNumber', 'u2'),
                     ('Spare', 'a40')])

    _Header = np.dtype([('HeaderBlockNumber', 'u1'),
                ('BlockLength', 'u2')])


class MutilSegmentHimawariFormat(HimawariFormat):

    def __init__(self, filenames):
        self.filenames = filenames
        self.filename = filenames[0]

    def extract(self, vline=None, vcol=None, decompress=False):
        self.load(decompress=decompress)
        if len(self.filenames) < 2:
            raws = self._extract(vline=vline, vcol=vcol)
        else:
            raws = [self._extract(vline=vline, vcol=vcol)]
            for filename in self.filenames[1:]:
                hf = HimawariFormat(filename)
                hf.load(decompress=decompress)
                raws.append(hf._extract(vline=vline, vcol=vcol))
            raws = np.concatenate(raws)
        return self.calibration(raws)

    def get_geocoord(self, vline=None, vcol=None):
        self.modify_metadata(vline, vcol)
        return self.get_lonlat()

    def modify_metadata(self, vline, vcol):
        """Modify meta data to generate full lon/lat coordinates at one time."""
        self.first_lineno = int(self.lines * vline[0] * 10)
        self.lines = int(self.lines * 10 * vline[1]) - int(self.lines * 10 * vline[0])
        self.first_colno = int(self.columns * vcol[0])
        self.columns = int(self.columns * vcol[1]) - int(self.columns * vcol[0])
