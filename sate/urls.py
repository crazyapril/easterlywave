from django.urls import include, path
from sate.views import ECEnsembleView, TyphoonSectorView


urlpatterns = [
    path('ecens', ECEnsembleView.as_view()),
    path('sector', TyphoonSectorView.as_view())
]
