from django.conf import settings
import os

class SateFile:

    def __init__(self, time, area='target', band=None, segno=None, enhance=None):
        self.time = time
        self.area = area
        self.band = band
        self.segno = segno
        self.enhance = enhance
        if self.time.minute % 10 == 0:
            rapid_scan_no = 1
        elif self.time.minute % 10 == 2:
            rapid_scan_no = 2
        elif self.time.minute % 10 == 5:
            rapid_scan_no = 3
        elif self.time.minute % 10 == 7:
            rapid_scan_no = 4
        ntime = self.time.replace(minute=self.time.minute // 10 * 10)
        if self.band in (1, 2):
            resolution = '10'
        elif self.band == 3:
            resolution = '05'
        else:
            resolution = '20'
        self.source_path = 'jma/hsd/{}/HS_H08_{}_B{:02d}_R30{}_R{}_S0101.DAT.bz2'.format(
            self.time.strftime('%Y%m/%d/%H'), ntime.strftime('%Y%m%d_%H%M'), self.band,
            rapid_scan_no, resolution)
        self.target_path = os.path.join(settings.TMP_ROOT, 'sate/{}/{}_B{}.bz2'.format(
            self.time.strftime('%Y%m%d%H'), self.time.minute, self.band))
        os.makedirs(os.path.dirname(self.target_path), exist_ok=True)

