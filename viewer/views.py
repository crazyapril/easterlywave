import datetime
import json
import logging
import os
import time

import requests
from braces.views import JsonRequestResponseMixin
from django.conf import settings
from django.shortcuts import render
from django.utils import timezone
from django.views.generic.base import TemplateView, View
from hanziconv import HanziConv

from tools.cache import DAY, redis_cached, redis_cached_for_classmethod
from viewer.models import HitRecord, Notice, Station
from viewer.windygram.windygram import Windygram

PIC_DIR = os.path.join(settings.BASE_DIR, 'img')

logger = logging.getLogger(__name__)

# Create your views here.

class HomepageView(TemplateView):
    template_name = 'windygram.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['notices'] = self.get_notices()
        return context

    @redis_cached_for_classmethod(namespace='', timeout=3600, as_key='notices')
    def get_notices(self):
        current_time = timezone.now()
        notices = []
        for notice in Notice.objects.all():
            if notice.start_time + datetime.timedelta(minutes=notice.ttl) > current_time:
                notices.append(notice)
        return notices


SUGGESTION_NUM = 5
TIME_00Z = datetime.time(9, 0) # BJT 17:00
TIME_12Z = datetime.time(21, 0) # BJT 05:00

@redis_cached(timeout=7 * DAY)
def get_suggestion(content):
    if content.isdigit():
        qs = Station.objects.filter(code__contains=content).order_by('-hit')[:SUGGESTION_NUM]
    elif all(ord(c) >= 128 for c in content):
        content = HanziConv.toSimplified(content)
        qs = Station.objects.filter(name__contains=content).order_by('-hit')[:SUGGESTION_NUM]
    elif content.isalpha():
        qs = Station.objects.filter(en_name__icontains=content).order_by('-hit')[:SUGGESTION_NUM]
    else:
        qs = None
    return qs


class NoticeView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        notices = self.get_notices()
        responses = {'status':0, 'notices':[{'type': n.typ, 'content': n.content} for n in notices]}
        return self.render_json_response(responses)

    @redis_cached_for_classmethod(namespace='', timeout=3600, as_key='notices')
    def get_notices(self):
        current_time = timezone.now()
        notices = []
        for notice in Notice.objects.all():
            if notice.start_time + datetime.timedelta(minutes=notice.ttl) > current_time:
                notices.append(notice)
        return notices


class SearchSuggestionView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        content = self.request_json['content']
        response = {'status': 0, 'suggestions':[]}
        qs = get_suggestion(content)
        if qs:
            response['suggestions'] = [{'value':s.name, 'data':s.code} for s in qs]
        else:
            response['status'] = 1
        return self.render_json_response(response)


def validate_geo_position(query):
    query = query.replace(' ', '')
    if query.count(',') != 1:
        return None
    lat, lon = tuple(query.split(','))
    try:
        lat = float(lat)
        lon = float(lon)
    except:
        return None
    if lat > 85 or lat < -85:
        return None
    if lon > 180 or lon < -180:
         return None
    return lat, lon

class MakingPlotView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        post_data = self.request_json
        response = {'status': 0, 'message':''}
        query = post_data['content']
        qs = get_suggestion(query)
        if not qs or qs.count() < 1:
            result = validate_geo_position(query)
            if not result:
                response['status'] = 1
                response['message'] = 'Not found'
                return self.render_json_response(response)
            lat, lon = result
            name = None
            logger.info('({}, {}) selected'.format(lat, lon))
        else:
            chosen = qs[0]
            lat = chosen.lat
            lon = chosen.lon
            name = chosen.name
            logger.info('{} ({}, {}) selected.'.format(name, lat, lon))
            chosen.hit += 1
            chosen.save()
        if name is None:
            query_name = '0'
        else:
            query_name = name
        try:
            record = HitRecord.objects.get(name=query_name, date=datetime.date.today())
        except HitRecord.DoesNotExist:
            record = HitRecord.objects.create(name=query_name)
        else:
            record.hit += 1
            record.save()
        lat = round(lat, 3)
        lon = round(lon, 3)
        try:
            filepath = get_windygram_plot(lat, lon, name)
        except Exception as exp:
            logger.exception(exp)
            response['status'] = 2
            response['message'] = 'Internal error'
            return self.render_json_response(response)
        response['src'] = '/' + os.path.join(settings.MEDIA_URL, filepath)
        return self.render_json_response(response)

def get_nearest_run():
    now = datetime.datetime.utcnow()
    nowtime = now.time()
    if TIME_00Z <= nowtime <= TIME_12Z:
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    runtime = now.replace(hour=12, minute=0, second=0, microsecond=0)
    if nowtime < TIME_00Z:
        runtime = runtime - datetime.timedelta(days=1)
    return runtime

def get_windygram_plot(lat, lon, name):
    runtime = get_nearest_run()
    folder_name = runtime.strftime('%Y%m%d%H')
    directory = os.path.join(settings.MEDIA_ROOT, folder_name)
    os.makedirs(directory, exist_ok=True, mode=0o755)
    short_filename = '{:.3f}-{:.3f}'.format(lat, lon)
    short_filename = short_filename.replace('.', '_') + '.png'
    filename = os.path.join(directory, short_filename)
    if not os.path.exists(filename):
        make_windygram_plot(lat, lon, name, target=filename)
    return '{}/{}'.format(folder_name, short_filename)


DETAIL_ADDR = 'https://node.windy.com/forecast/v2.4/ecmwf/{:.3f}/{:.3f}'
METEO_ADDR = 'https://node.windy.com/forecast/meteogram/ecmwf/{:.3f}/{:.3f}'

def make_windygram_plot(lat, lon, name, target=None):
    detail_url = DETAIL_ADDR.format(lat, lon)
    meteo_url = METEO_ADDR.format(lat, lon)
    #
    response = requests.get(detail_url, timeout=1)
    logger.debug('Request: {}'.format(detail_url))
    logger.debug('Elapsed: {:.0f} ms'.format(response.elapsed.microseconds / 1e3))
    detail_data = response.json()
    #
    response = requests.get(meteo_url, timeout=1)
    logger.debug('Request: {}'.format(meteo_url))
    logger.debug('Elapsed: {:.0f} ms'.format(response.elapsed.microseconds / 1e3))
    meteo_data = response.json()
    #
    tic = time.time()
    logger.debug('Start plotting')
    w = Windygram()
    w.set_data(detail_data, meteo_data)
    w.set_data_location()
    w.set_time()
    w.set_name(name)
    w.plot_and_save(target)
    toc = time.time()
    logger.debug('End plotting')
    logger.debug('Plotting work elapsed: {:.1f} s'.format(toc - tic))
