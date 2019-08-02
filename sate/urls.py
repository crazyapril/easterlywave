from django.urls import include, path
from sate.views import ECEnsembleView, SSTView, TyphoonSectorView, TyphoonImagesView


urlpatterns = [
    path('ecens', ECEnsembleView.as_view()),
    path('sector', TyphoonSectorView.as_view()),
    path('sst', SSTView.as_view()),
    path('images', TyphoonImagesView.as_view()),
]
