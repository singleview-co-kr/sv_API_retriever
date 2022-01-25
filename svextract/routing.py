# svextract/routing.py
from django.conf.urls import url
from . import consumers

websocket_urlpatterns = [
    url(r'^ws/extract/(?P<sv_acct_id>[^/]+)/(?P<sv_brand_id>[^/]+)/$', consumers.PluginConsole.as_asgi()),
]