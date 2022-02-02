from django.urls import path
from . import views

app_name = 'svupload'
urlpatterns = [
    path('<int:sv_brand_id>/', views.UploadFileListView.as_view(), name='index'),
    path('<int:sv_brand_id>/extract/<int:n_file_id>/', views.DownloadFileView.as_view(), name='extract'),
    # path('<int:sv_brand_id>/delete/<int:n_file_id>/', views.DownloadFileView.as_view(), name='delete'),
    path('ajax_handling/', views.AjaxHandling.as_view(), name='ajax_handling'),
]
