from braces.views import AjaxResponseMixin, JSONResponseMixin
from django.conf import settings
from django.views.generic.base import TemplateView, View

from viewer.models import Switch


class SatelliteImageView(TemplateView):
    template_name = 'sate.html'


class SatelliteServiceView(AjaxResponseMixin, JSONResponseMixin, View):

    def post_ajax(self, request, *args, **kwargs):
        status = Switch.get_status_by_name(settings.SWITCH_SATE_SERVICE) == 'ON'
        return self.render_json_response({'status': status})
