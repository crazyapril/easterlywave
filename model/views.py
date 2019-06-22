from braces.views import JsonRequestResponseMixin
from django.views.generic.base import View

from model.status import get_update_status, select_name_and_code
from tools.cache import Key


class ModelListView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        models = Key.get(Key.MODEL_MODELS)
        return self.render_json_response({'models': models})


class RegionListView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        regions = Key.get(Key.MODEL_REGIONS)
        return self.render_json_response({'regions': regions})


class CodeListView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        model = self.request_json['model']
        region = self.request_json['region']
        plevel = request.session.get('USER_PLEVEL', 0)
        codes = select_name_and_code(model, region, plevel)
        return self.render_json_response(codes)


class PlotStatusView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        model = self.request_json['model']
        region = self.request_json['region']
        code = self.request_json['code']
        status = get_update_status(model, region, code)
        return self.render_json_response(status)

