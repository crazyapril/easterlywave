"""windygram URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include, path, re_path
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.generic.base import TemplateView, RedirectView

from sate.views import SatelliteImageView
from viewer.views import *

coreview = ensure_csrf_cookie(TemplateView.as_view(template_name='index.html'))

urlpatterns = [
    path('admintown', admin.site.urls),
    #path('windygram', ensure_csrf_cookie(HomepageView.as_view()), name='home'),
    #path('satellite', SatelliteImageView.as_view(), name='sate'),
    # path('home', coreview),
    # #path('ajax/search', SearchSuggestionView.as_view(), name='search'),
    # #path('ajax/plot', MakingPlotView.as_view(), name='plot'),
    # path('windygram', coreview),
    # path('satellite', coreview),
    # path('about', coreview),
    # path('weather', coreview),
    # path('', RedirectView.as_view(url='home')),

    path('action/notices', NoticeView.as_view()),
    path('action/weather/', include('viewer.urls')),
    path('action/satellite/', include('sate.urls')),
    path('action/blog/', include('blog.urls')),

    re_path(r'^.*$', coreview),
]
