from django.urls import path
from svload import views

app_name = 'svload'
urlpatterns = [
    path('today/<int:sv_brand_id>/', views.GaMedia.as_view(), name='index'),
    path('ga_media/<int:sv_brand_id>/', views.GaMedia.as_view(), name='ga_media'),
    path('ga_source_medium/<int:sv_brand_id>/', views.GaSourceMediumView.as_view(), name='ga_source'),
    path('ga_itemperf/<int:sv_brand_id>/', views.GaItemPerfView.as_view(), name='ga_item'),
    path('edi/<int:sv_brand_id>/', views.FocusTodayEdi.as_view(), name='edi_today'),
    path('edi_by_branch/<int:sv_brand_id>/<int:branch_id>/', views.ByBranchEdi.as_view(), name='edi_branch'),
    path('edi_by_sku/<int:sv_brand_id>/<int:item_id>/', views.BySkuEdi.as_view(), name='edi_sku'),
    path('morpheme/<int:sv_brand_id>/', views.Morpheme.as_view(), name='morpheme'),
    path('morpheme/<int:sv_brand_id>/chronicle/', views.MorphemeChronicle.as_view(), name='morpheme_chronicle'),
    path('budget/<int:sv_brand_id>/', views.BudgetView.as_view(), name='budget_list'),
    path('budget/<int:sv_brand_id>/<str:period_from>/<str:period_to>/', views.BudgetView.as_view(), name='budget_period'),
    path('budget_update/<int:sv_brand_id>/<int:budget_id>/', views.BudgetView.as_view(), name='budget_update'),
]
