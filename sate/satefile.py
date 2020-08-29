import os

from django.conf import settings


class SateFile:

    def __init__(self, time, area='target', band=None, segno=None, enhance=None,
            name=None, georange=None, vline=None, vcol=None, storm=None):
        self.time = time
        self.area = area
        self.band = band
        self.segno = segno
        self.enhance = enhance
        self.name = name
        self.georange = georange
        self.vline = vline
        self.vcol = vcol
        self.storm = storm
        if self.band in (1, 2):
            resolution = '10'
        elif self.band == 3:
            resolution = '05'
        else:
            resolution = '20'
        target_dir = os.path.join(settings.TMP_ROOT,
            'sate/{}'.format(self.time.strftime('%Y%m%d%H')))
        if area == 'target':
            if self.time.minute % 10 == 0:
                rapid_scan_no = 1
            elif self.time.minute % 10 == 2:
                rapid_scan_no = 2
            elif self.time.minute % 10 == 5:
                rapid_scan_no = 3
            elif self.time.minute % 10 == 7:
                rapid_scan_no = 4
            ntime = self.time.replace(minute=self.time.minute // 10 * 10)
            # self.source_path = 'jma/hsd/{}/HS_H08_{}_B{:02d}_R30{}_R{}_S0101.DAT.bz2'.format(
            #     self.time.strftime('%Y%m/%d/%H'), ntime.strftime('%Y%m%d_%H%M'), self.band,
            #     rapid_scan_no, resolution)
            self.source_path = 'AHI-L1b-Target/{0}/HS_H08_{1}_B{2:02d}_R30{3}_R{4}_S0101.DAT.bz2'.format(
                ntime.strftime('%Y/%m/%d/%H%M'), ntime.strftime('%Y%m%d_%H%M'),
                self.band, rapid_scan_no, resolution)
            self.target_path = os.path.join(target_dir,
                '{}_B{}.bz2'.format(self.time.minute, self.band))
            self.export_path = os.path.join(settings.MEDIA_ROOT,
                'sate/{}/B{}{{enh}}/{}.png'.format(self.time.strftime('%Y%m%d'),
                self.band, self.time.strftime('%H%M')))
            self.latest_path = os.path.join(settings.MEDIA_ROOT,
                'latest/sate/b{}{{enh}}.png'.format(self.band))
        elif area == 'fulldisk':
            if not isinstance(self.segno, list):
                self.segno = [self.segno]
            self.segno.sort()
            # self.source_path = ['jma/hsd/{}/HS_H08_{}_B{:02d}_FLDK_R{}_S{:02d}10.DAT.bz2'.format(
            #     self.time.strftime('%Y%m/%d/%H'), self.time.strftime('%Y%m%d_%H%M'), self.band,
            #     resolution, seg) for seg in self.segno]
            self.source_path = ['AHI-L1b-FLDK/{0}/HS_H08_{1}_B{2:02d}_FLDK_R{3}_S{4:02d}10.DAT.bz2'.format(
                self.time.strftime('%Y/%m/%d/%H%M'),
                self.time.strftime('%Y%m%d_%H%M'), self.band, resolution,
                seg) for seg in self.segno]
            self.target_path = [os.path.join(target_dir, '{}_B{}_S{}.bz2'.format(
                self.time.minute, self.band, seg)) for seg in self.segno]
            self.export_path = os.path.join(settings.MEDIA_ROOT,
                'sate/{}/B{}{{enh}}/{}_{}.png'.format(self.time.strftime('%Y%m%d'), self.band, self.name, self.time.strftime('%H%M')))
            self.latest_path = os.path.join(settings.MEDIA_ROOT,
                'latest/sate/{}_b{}{{enh}}.png'.format(self.name, self.band))
        os.makedirs(target_dir, exist_ok=True)


def combine_satefile_paths(satefiles):
    combines = {sf.source_path[i]:sf.target_path[i] for sf in satefiles
        for i in range(len(sf.source_path))}
    zip_paths = list(combines.items())
    return zip_paths
