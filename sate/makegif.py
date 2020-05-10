import logging
import os

from django.conf import settings
from django.core.cache import cache
from PIL import Image

from tools.cache import Key
from tools.utils import optimize_gif

logger = logging.getLogger(__name__)


def get_filename_by_channel_key(channel, target=False):
    namesegs = channel.split('-')
    if namesegs[0] == 'VIS':
        bandname = 'b3' if target else 'b1'
    elif namesegs[0] == 'WV':
        bandname = 'b8'
    elif namesegs[0] == 'IR':
        bandname = 'b13'
    if len(namesegs) == 1:
        return bandname
    return bandname + namesegs[1].lower()


class MakeGifRoutine:

    def go(self, mode='target'):
        if mode == 'target':
            self.make_gifs_by_key(Key.SATE_LOOP_IMAGES.format(storm='target'))
        else:
            image_keys = cache.keys(Key.SATE_LOOP_IMAGES.format(storm='*'))
            for key in image_keys:
                if 'target' in key:
                    continue
                self.make_gifs_by_key(key)

    def make_gifs_by_key(self, key):
        storm_name = key.split('_')[-1]
        target_flag = storm_name == 'target'
        all_images = cache.get(key)
        for channel in all_images:
            gifname = get_filename_by_channel_key(channel, target=target_flag)\
                + '.gif'
            if not target_flag:
                gifname = storm_name + '_' + gifname
            target_path = os.path.join(settings.MEDIA_ROOT, 'latest/sate',
                gifname)
            images = [os.path.join(settings.MEDIA_ROOT, 'sate', f) \
                for f in all_images[channel]]
            self.make_gif(images=images, output=target_path)

    def make_gif(self, images=None, output=None):
        if len(images) == 0:
            return
        gif = Image.open(images[0])
        # Except last frame, interval between two frames is set to 100ms.
        duration = [100] * len(images)
        duration[-1] = 700
        handles = [Image.open(f) for f in images[1:]]
        gif.save(output, save_all=True, duration=duration, loop=0,
            append_images=handles)
        optimize_gif(output)
        logger.info('Optimized gif exported to %s.', output)

