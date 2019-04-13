from braces.views import JsonRequestResponseMixin
from django.views.generic.base import View

from tools.cache import Key
from tools.typhoon import StormSector


class TyphoonSectorView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        sector = StormSector.get_or_create()
        return self.render_json_response(sector.to_json())


class ECEnsembleView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        data = Key.get(Key.ECMWF_ENSEMBLE_STORMS) or []
        return self.render_json_response({'data': data})


class SSTView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        data = Key.get(Key.RTOFS_SST_DAYS) or []
        return self.render_json_response({'times': data})
