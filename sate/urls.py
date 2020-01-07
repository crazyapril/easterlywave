from django.urls import include, path

from sate.views import (ECEnsembleView, SatelliteAreaView, SSTView,
                        TyphoonCreateVideoView, TyphoonImagesView,
                        TyphoonSectorView)

urlpatterns = [
    path('ecens', ECEnsembleView.as_view()),
    path('sector', TyphoonSectorView.as_view()),
    path('sst', SSTView.as_view()),
    path('images', TyphoonImagesView.as_view()),
    path('createvideo', TyphoonCreateVideoView.as_view()),
    path('areas', SatelliteAreaView.as_view()),
]
