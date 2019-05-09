from django.urls import path

from core.views import (UserAutoLoginView, UserLoginView, UserLogoutView,
                        UserRegisterView)

urlpatterns = [
    path('check', UserAutoLoginView.as_view()),
    path('login', UserLoginView.as_view()),
    path('logout', UserLogoutView.as_view()),
    path('register', UserRegisterView.as_view())
]
