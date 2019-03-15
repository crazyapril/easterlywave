from django.urls import include, path
from sate.views import SatelliteServiceView


urlpatterns = [
    path('service', SatelliteServiceView.as_view())
]
