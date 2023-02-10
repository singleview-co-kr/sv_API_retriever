from django.urls import path
from svload import views

app_name = 'svload'
urlpatterns = [
    path('today/<int:sv_brand_id>/', views.GaMedia.as_view(), name='index'),
    path('ga_media/<int:sv_brand_id>/', views.GaMedia.as_view(), name='ga_media'),
    path('agency_detail/<int:sv_brand_id>/<int:sv_agency_id>/', views.AgencyDetail.as_view(), name='agency_detail'),
    path('ga_source_medium/<int:sv_brand_id>/', views.GaSourceMediumView.as_view(), name='ga_source'),
    path('ga_itemperf/<int:sv_brand_id>/', views.GaItemPerfView.as_view(), name='ga_item'),
    path('edi/<int:sv_brand_id>/', views.FocusTodayEdi.as_view(), name='edi_today'),
    path('edi_by_branch/<int:sv_brand_id>/<int:branch_id>/', views.ByBranchEdi.as_view(), name='edi_branch'),
    path('edi_by_sku/<int:sv_brand_id>/<int:item_id>/', views.BySkuEdi.as_view(), name='edi_sku'),
    path('viral/<int:sv_brand_id>/', views.Viral.as_view(), name='viral_main'),
    path('viral/<int:sv_brand_id>/<str:sv_source_name>/', views.Viral.as_view(), name='viral_main'),
    path('morpheme/<int:sv_brand_id>/', views.Morpheme.as_view(), name='morpheme'),
    path('morpheme/<int:sv_brand_id>/chronicle/', views.MorphemeChronicle.as_view(), name='morpheme_chronicle'),
    path('budget/<int:sv_brand_id>/', views.BudgetView.as_view(), name='budget_list'),
    path('budget/<int:sv_brand_id>/<str:period_from>/<str:period_to>/', views.BudgetView.as_view(), name='budget_period'),
    path('budget_update/<int:sv_brand_id>/<int:budget_id>/', views.BudgetView.as_view(), name='budget_update'),
    path('nvr_brs_contract/<int:sv_brand_id>/', views.NvrBrsContractView.as_view(), name='nvr_brs_contract_list'),
    path('nvr_brs_contract/<int:sv_brand_id>/<str:period_from>/<str:period_to>/', views.NvrBrsContractView.as_view(), name='nvr_brs_contract_list_period'),
    path('nvr_brs_contract_update/<int:sv_brand_id>/<int:contract_srl>/', views.NvrBrsContractView.as_view(), name='nvr_brs_contract_update'),
    # path('pns_contract/<int:sv_brand_id>/', views.PnsContractView.as_view(), name='pns_contract_list'),
    # path('pns_contract/<int:sv_brand_id>/<str:period_from>/<str:period_to>/', views.PnsContractView.as_view(), name='pns_contract_list_period'),
    # path('pns_contract_update/<int:sv_brand_id>/<int:contract_id>/', views.PnsContractView.as_view(), name='pns_contract_update'),
    path('brded_term/<int:sv_brand_id>/', views.TermManagerView.as_view(), name='term_manager'),
    path('campaign_alias/<int:sv_brand_id>/', views.CampaignAliasView.as_view(), name='campaign_alias_list'),
    path('campaign_alias/<int:sv_brand_id>/<str:period_from>/<str:period_to>/', views.CampaignAliasView.as_view(), name='campaign_alias_list_period'),
    path('campaign_alias_update/<int:sv_brand_id>/<int:alias_id>/', views.CampaignAliasView.as_view(), name='campaign_alias_update'),
]
