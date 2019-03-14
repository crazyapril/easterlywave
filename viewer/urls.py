from django.urls import path
from viewer.views import SearchSuggestionView, MakingPlotView


urlpatterns = [
    path('search', SearchSuggestionView.as_view()),
    path('plot', MakingPlotView.as_view()),
]
