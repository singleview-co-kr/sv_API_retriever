# from django.urls import path
from . import views
# from django.conf.urls import url
from django.urls import path

app_name = 'svextract'
urlpatterns = [
    # url(r'^(?P<brand_name>[^/]+)/$', views.SvPluginWebConsole.as_view(), name='plugin_console'),
    path('<int:sv_brand_id>/', views.SvPluginWebConsole.as_view(), name='plugin_console'),
]
