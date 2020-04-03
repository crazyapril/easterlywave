from django.urls import path
from viewer.views import SearchSuggestionView, MakingPlotView, StationInfoView


urlpatterns = [
    path('search', SearchSuggestionView.as_view()),
    path('plot', MakingPlotView.as_view()),
    path('stationinfo', StationInfoView.as_view()),
]
