from django.urls import path

from model.views import (CodeListView, ModelListView, PlotStatusView,
                         RegionListView)

urlpatterns = [
    path('codes', CodeListView.as_view()),
    path('models', ModelListView.as_view()),
    path('status', PlotStatusView.as_view()),
    path('regions', RegionListView.as_view())
]
