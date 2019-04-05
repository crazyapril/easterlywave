from django.urls import include, path
from sate.views import TyphoonSectorView


urlpatterns = [
    path('sector', TyphoonSectorView.as_view())
]
