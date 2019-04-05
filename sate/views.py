from braces.views import JsonRequestResponseMixin
from django.views.generic.base import View

from tools.typhoon import StormSector


class TyphoonSectorView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        sector = StormSector.get_or_create()
        return self.render_json_response(sector.to_json())

