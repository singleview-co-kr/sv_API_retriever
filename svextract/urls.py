# from django.urls import path
from . import views
from django.conf.urls import url

app_name = 'svextract'
urlpatterns = [
    # path('googleads/', views.GoogleAds.as_view(), name='googleads'),
    # url(r'^(?P<plugin_name>[^/]+)/$', views.room, name='room'),
    url(r'^(?P<brand_name>[^/]+)/$', views.SvPluginWebConsole.as_view(), name='plugin_console'),
]
