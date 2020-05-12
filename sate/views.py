import os

from braces.views import JsonRequestResponseMixin
from django.conf import settings
from django.views.generic.base import View

from tools.cache import Key
from tools.typhoon import StormSector
from tools.utils import execute, is_file_valid


class TyphoonSectorView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        sector = StormSector.get_or_create()
        return self.render_json_response(sector.to_json())


class TyphoonImagesView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        images = Key.get(Key.SATE_LOOP_IMAGES.format(storm=self.request_json['storm'].upper())) or {}
        return self.render_json_response(images)


class TyphoonCreateVideoView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        if not request.user.is_authenticated:
            return self.render_json_response({'url':''})
        storm = self.request_json['storm'].upper()
        imtype = self.request_json['type'].upper()
        video_time = self.request_json.get('vtime', 'cache')
        url = make_video(storm, imtype, video_time)
        return self.render_json_response({'url':url})


def make_video(storm, imtype, video_time):
    cache_images = Key.get(Key.SATE_LOOP_IMAGES.format(storm=storm)) or {}
    if imtype not in cache_images:
        return ''
    import time, datetime
    if video_time == 'cache':
        images = cache_images[imtype]
    elif video_time == 'today':
        last_image = cache_images[imtype][-1]
        last_time = datetime.datetime.strptime(last_image[:8] +\
            last_image[-8:-4], '%Y%m%d%H%M')
        fimtype = last_image.split('/')[1]
        stormname = last_image.split('/')[-1].split('_')
        if len(stormname) == 1:
            stormname = ''
        else:
            stormname = stormname[0] + '_'
        if last_time.minute % 10 in (2, 7):
            last_time = last_time.replace(second=30)
        start_time = last_time.replace(hour=7, minute=20, second=0)
        timedelta = datetime.timedelta(seconds=150)
        image_time = start_time
        images = []
        while image_time <= last_time:
            images_path = '{}/{}/{}{}.png'.format(image_time.strftime('%Y%m%d'),
                fimtype, stormname, image_time.strftime('%H%M'))
            fullpath = os.path.join(settings.MEDIA_ROOT, 'sate', images_path)
            if is_file_valid(fullpath):
                images.append(images_path)
            image_time = image_time + timedelta
    if len(images) < 3:
        return ''
    tmp_id = '{:07x}'.format(int(time.time() % 10 * 1e7))
    tmp_input_file = os.path.join(settings.TMP_ROOT, tmp_id+'.txt')
    with open(tmp_input_file, 'w') as f:
        for image in images:
            fullpath = os.path.join(settings.MEDIA_ROOT, 'sate', image)
            f.write("file '{}'\nduration 0.1\n".format(fullpath))
        f.write("file '{0}'\nduration 0.5\nfile '{0}'".format(fullpath))
    export_uri = '/'.join(['latest', 'satevid',
        datetime.datetime.utcnow().strftime('%Y%m%d%H'), tmp_id+'.mp4'])
    export_file = os.path.join(settings.MEDIA_ROOT, export_uri)
    os.makedirs(os.path.dirname(export_file), exist_ok=True)
    execute('ffmpeg -f concat -safe 0 -i {} -vf scale=960:-2 -vsync vfr '
        '-pix_fmt yuv420p {}'.format(tmp_input_file, export_file))
    os.remove(tmp_input_file)
    return export_uri


class ECEnsembleView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        data = Key.get(Key.ECMWF_ENSEMBLE_STORMS) or []
        return self.render_json_response({'data': data})


class SSTView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        data = Key.get(Key.RTOFS_SST_DAYS) or []
        return self.render_json_response({'times': data})


class SatelliteAreaView(JsonRequestResponseMixin, View):

    def get(self, request, *args, **kwargs):
        config = Key.get(Key.SATE_SERVICE_CONFIG)
        if not config:
            data = []
        else:
            data = config.active_areas
        return self.render_json_response({'areas': data})

