from django.urls import path

from . import views

app_name = 'svacct'
urlpatterns = [
    path('', views.IndexView.as_view(), name='index'),
    path('<int:n_brand_id>/', views.BrandConfView.as_view(), name='brand_conf'),
    # path('<int:pk>/results/', views.ResultsView.as_view(), name='results'),
]
