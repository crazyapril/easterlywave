from braces.views import JsonRequestResponseMixin
from django.conf import settings
from django.views.generic.base import TemplateView, View

from viewer.models import get_switch_status_by_name


class SatelliteImageView(TemplateView):
    template_name = 'sate.html'


class SatelliteServiceView(JsonRequestResponseMixin, View):

    def post(self, request, *args, **kwargs):
        status = get_switch_status_by_name(settings.SWITCH_SATE_SERVICE) == 'ON'
        return self.render_json_response({'status': status})
