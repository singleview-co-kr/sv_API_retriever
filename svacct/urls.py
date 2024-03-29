from django.urls import path

from . import views

app_name = 'svacct'
urlpatterns = [
    path('', views.IndexView.as_view(), name='index'),
    path('<int:sv_brand_id>/', views.BrandConfView.as_view(), name='brand_conf'),
]
