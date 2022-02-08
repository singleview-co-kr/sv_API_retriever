from django.urls import path
from . import views

app_name = 'svupload'
urlpatterns = [
    path('<int:sv_brand_id>/', views.UploadFileListView.as_view(), name='index'),
    path('<int:sv_brand_id>/download/<int:n_file_id>/', views.DownloadFileView.as_view(), name='download'),
    path('<int:sv_brand_id>/transform/<int:n_file_id>/', views.TransformFileView.as_view(), name='transform'),
    path('ajax_handling/', views.AjaxHandling.as_view(), name='ajax_handling'),
]
