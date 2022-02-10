"""svdjango URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
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
# from django.urls import path, include
from django.conf.urls import url, include
from django.urls import path
from django.contrib import admin

from .views import IndexView
from svauth.views import UserCreateView, UserCreateDoneTV

urlpatterns = [
    # path('admin/', admin.site.urls),
    url(r'^admin/', admin.site.urls),
    path('', IndexView.as_view(), name='index'),
    path('login/login.cgi', IndexView.as_view(), name='index_alt'),  # iptime DDNS disallow access to /
    
    path('accounts/', include('django.contrib.auth.urls')),  # {% url 'login' %} 등 처리
    path('accounts/register/', UserCreateView.as_view(), name='register'),
    path('accounts/register/done', UserCreateDoneTV.as_view(), name='register_done'),

    path('extract/', include('svextract.urls')),
    path('upload/', include('svupload.urls')),
]
