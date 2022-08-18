import os
import configparser

# begin - django related
from django.shortcuts import render
from django.shortcuts import redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import TemplateView
from django.db.models.base import ObjectDoesNotExist
from django.conf import settings
# end - django related

# from svacct.models import Account
# from svacct.models import Brand
from svacct.models import DataSourceType
from svacct.models import DataSource
from svacct.models import DataSourceDetail
from svacct.ownership import get_owned_brand_list


# Create your views here.
class IndexView(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'
    def get(self, request, *args, **kwargs):
        lst_owned_brand = get_owned_brand_list(request)
        return render(request, 'svacct/index.html', {'lst_owned_brand': lst_owned_brand})


class BrandConfView(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'
    __g_sSvNull = '#%'

    def get(self, request, *args, **kwargs):
        # https://stackoverflow.com/questions/4148923/is-it-possible-to-create-a-custom-admin-view-without-a-model-behind-it
        dict_owned_brand = get_owned_brand_list(request, kwargs)
        b_brand_ownership = False
        for _, dict_single_acct in dict_owned_brand.items():
            for dict_single_brand in dict_single_acct['lst_brand']:
                if dict_single_brand['b_current_brand']:
                    n_sv_acct_id = dict_single_acct['n_acct_id']
                    n_sv_brand_id = dict_single_brand['n_brand_id']
                    s_brand_name = dict_single_brand['s_brand_ttl']
                    if n_sv_brand_id == kwargs['sv_brand_id']:
                        b_brand_ownership = True
                    break
        if not b_brand_ownership:
            return render(request, 'svacct/deny.html')

        s_ga_property_or_view_id, s_nvr_customer_id, lst_google_ads, \
            lst_facebook_biz, lst_kko_moment_aid = self.__get_source_info(n_sv_brand_id)

        s_api_info_ini_abs_path = os.path.join(settings.SV_STORAGE_ROOT, str(n_sv_acct_id), str(n_sv_brand_id), 'api_info.ini')
        o_ini_config = configparser.ConfigParser()
        o_ini_config.read(s_api_info_ini_abs_path)

        dict_query_google_ads = {}
        for s_single_google_ads in lst_google_ads:
            if o_ini_config['google_ads'].get(s_single_google_ads, self.__g_sSvNull) != self.__g_sSvNull:  # if value exists
                dict_query_google_ads[s_single_google_ads] = o_ini_config['google_ads'][s_single_google_ads]
            else:
                dict_query_google_ads[s_single_google_ads] = 'off'

        dict_query_fb_biz = {}
        for s_single_fb_biz_id in lst_facebook_biz:
            if o_ini_config['facebook_business'].get(s_single_fb_biz_id, self.__g_sSvNull) != self.__g_sSvNull:  # if value exists
                dict_query_fb_biz[s_single_fb_biz_id] = o_ini_config['facebook_business'][s_single_fb_biz_id]
            else:
                dict_query_fb_biz[s_single_fb_biz_id] = 'off'

        dict_query_kko_moment = {}
        for s_single_kko_moment_aid in lst_kko_moment_aid:
            print(o_ini_config['others'].get('kko_moment_aid', self.__g_sSvNull))
            s_kko_moment_val = o_ini_config['others'].get('kko_moment_aid', self.__g_sSvNull)
            if s_kko_moment_val == '' or s_kko_moment_val == self.__g_sSvNull:  # if value exists
                dict_query_kko_moment[s_single_kko_moment_aid] = 'off'
            else:
                dict_query_kko_moment[s_single_kko_moment_aid] = 'on'

        return render(request, 'svacct/brand_api_info.html', {
            's_brand_name': s_brand_name,
            'n_sv_brand_id': n_sv_brand_id,
            's_ga_property_or_view_id': s_ga_property_or_view_id,
            's_nvr_customer_id': s_nvr_customer_id,
            'dict_query_google_ads': dict_query_google_ads,
            'dict_query_fb_biz': dict_query_fb_biz,
            'dict_query_kko_moment': dict_query_kko_moment,
            'o_config': o_ini_config,
        })

    def post(self, request, *args, **kwargs):
        dict_owned_brand = get_owned_brand_list(request, kwargs)
        s_return_url = request.META.get('HTTP_REFERER')
        b_brand_ownership = False
        for _, dict_single_acct in dict_owned_brand.items():
            for dict_single_brand in dict_single_acct['lst_brand']:
                if dict_single_brand['b_current_brand']:
                    n_sv_acct_id = dict_single_acct['n_acct_id']
                    n_sv_brand_id = dict_single_brand['n_brand_id']
                    s_brand_name = dict_single_brand['s_brand_ttl']
                    if n_sv_brand_id == kwargs['sv_brand_id']:
                        b_brand_ownership = True
                    break
        if not b_brand_ownership:
            dict_context = {'err_msg': 'no permission', 's_return_url': s_return_url}
            return render(request, "svacct/deny.html", context=dict_context)

        s_act = request.POST.get('act')        
        if s_act == 'update_brd_api_info':

            s_ga_property_or_view_id, s_nvr_customer_id, lst_google_ads, \
            lst_facebook_biz, lst_kko_moment_aid = self.__get_source_info(n_sv_brand_id)

            dict_query_nvr_config = {}  # naver ads API config
            dict_query_ga_config = {}  # naver google analytics API config
            dict_query_gads_config = {}  # naver google ads API config
            dict_query_fb_biz_config = {}  # naver fb business API config
            dict_query_kko_config = {}  # naver kakao moment API config

            for s_key, s_value in request.POST.items():
                if s_key.startswith('nvr_'):
                    dict_query_nvr_config[s_key.lstrip('nvr_')] = s_value
                if s_key.startswith('ga_'):
                    if s_key == 'ga_version':  # validation
                        if s_value == '':
                            dict_context = {'err_msg': 'Google Analytics version should be choosed', 's_return_url': s_return_url}
                            return render(request, "svacct/deny.html", context=dict_context)
                    dict_query_ga_config[s_key.lstrip('ga_')] = s_value
                if s_key.startswith('gads_'):
                    dict_query_gads_config[s_key.lstrip('gads_')] = s_value
                if s_key.startswith('fb_biz_'):
                    dict_query_fb_biz_config[s_key.lstrip('fb_biz_')] = s_value
                if s_key.startswith('kko_'):
                    dict_query_kko_config[s_key.lstrip('kko_')] = s_value

            o_ini_config = configparser.ConfigParser()
            # proc nvr_ad
            # o_ini_config['DEFAULT']['DDD'] = 'EEE'  # DEFAULT 섹션은 기본적으로 생성되어 있어 생성없이 쓸 수 있다 
            o_ini_config['naver_searchad'] = {}  # naver_searchad section
            o_ini_config['naver_searchad']['manager_login_id'] = dict_query_nvr_config['manager_login_id']
            o_ini_config['naver_searchad']['api_key'] = dict_query_nvr_config['api_key']
            o_ini_config['naver_searchad']['secret_key'] = dict_query_nvr_config['secret_key']
            o_ini_config['naver_searchad']['customer_id'] = s_nvr_customer_id
            
            dict_ini_nvr_master_report = {
                'BusinessChannel': 0,
                'Campaign': 0,
                'CampaignBudget': 0,
                'Adgroup': 0,
                'AdgroupBudget': 0,
                'Keyword': 0,
                'Ad': 0,
                'AdExtension': 0,
                'Qi': 0,
            }
            for s_report_ttl in dict_ini_nvr_master_report:
                if dict_query_nvr_config.get(s_report_ttl, self.__g_sSvNull) != self.__g_sSvNull:  # if value exists
                    dict_ini_nvr_master_report[s_report_ttl] = dict_query_nvr_config[s_report_ttl]
            o_ini_config['nvr_master_report'] = dict_ini_nvr_master_report  # nvr_master_report section

            dict_ini_nvr_stat_report = {
                'AD': 0,
                'AD_DETAIL': 0,
                'AD_CONVERSION': 0,
                'AD_CONVERSION_DETAIL': 0,
                'ADEXTENSION': 0,
                'ADEXTENSION_CONVERSION': 0,
                'EXPKEYWORD': 0,
                'NAVERPAY_CONVERSION': 0,
            }
            for s_report_ttl in dict_ini_nvr_stat_report:
                if dict_query_nvr_config.get(s_report_ttl, self.__g_sSvNull) != self.__g_sSvNull:  # if value exists
                    dict_ini_nvr_stat_report[s_report_ttl] = dict_query_nvr_config[s_report_ttl]
            o_ini_config['nvr_stat_report'] = dict_ini_nvr_stat_report  # nvr_stat_report section

            # proc google analytics
            dict_ini_google_analytics = {
                'version': dict_query_ga_config['version'],
                'property_or_view_id': s_ga_property_or_view_id,
                'homepage': 'on',
                'internal_search': 'on' if dict_query_ga_config.get('internal_search', self.__g_sSvNull) != self.__g_sSvNull else 'off',
                'catalog': 'on' if dict_query_ga_config.get('catalog', self.__g_sSvNull) != self.__g_sSvNull else 'off',
                'payment': 'on' if dict_query_ga_config.get('payment', self.__g_sSvNull) != self.__g_sSvNull else 'off',
            }
            o_ini_config['google_analytics'] = dict_ini_google_analytics

            # proc google ads
            dict_ini_google_ads = {}
            for s_single_google_ads in lst_google_ads:
                if dict_query_gads_config.get(s_single_google_ads, self.__g_sSvNull) != self.__g_sSvNull:  # if value exists
                    s_toggle = 'on'
                else:
                    s_toggle = 'off'
                dict_ini_google_ads[s_single_google_ads] = s_toggle
            o_ini_config['google_ads'] = dict_ini_google_ads

            # proc facebook business
            dict_ini_fb_biz = {}
            for s_single_fb_biz_id in lst_facebook_biz:
                if dict_query_fb_biz_config.get(s_single_fb_biz_id, self.__g_sSvNull) != self.__g_sSvNull:  # if value exists
                    s_toggle = 'on'
                else:
                    s_toggle = 'off'
                dict_ini_fb_biz[s_single_fb_biz_id] = s_toggle
            o_ini_config['facebook_business'] = dict_ini_fb_biz

            # proc kakao moment
            o_ini_config['others'] = {}
            s_ini_single_kko_aid = ''
            # print(lst_kko_moment_aid)
            for s_single_kko_aid in lst_kko_moment_aid:
                print(dict_query_kko_config.get(s_single_kko_aid, self.__g_sSvNull))
                if dict_query_kko_config.get(s_single_kko_aid, self.__g_sSvNull) != self.__g_sSvNull:  # if value exists
                    s_ini_single_kko_aid = s_single_kko_aid
            # print(s_ini_single_kko_aid)
            o_ini_config['others']['kko_moment_aid'] = s_ini_single_kko_aid

            s_api_info_ini_abs_path = os.path.join(settings.SV_STORAGE_ROOT, str(n_sv_acct_id), str(n_sv_brand_id), 'api_info.ini')
            with open(s_api_info_ini_abs_path, 'w') as configfile:
                o_ini_config.write(configfile)

        o_redirect = redirect('svacct:brand_conf', sv_brand_id=n_sv_brand_id)
        return o_redirect

    def __get_source_info(self, n_sv_brand_id):
        try:
            qs_ds_owned = DataSource.objects.filter(sv_brand_id=n_sv_brand_id)
        except DataSource.DoesNotExist:
            qs_ds_owned = None

        s_ga_property_or_view_id = ''
        s_nvr_customer_id = ''
        lst_google_ads = []
        lst_facebook_biz = []
        lst_kko_moment_aid = []
        for o_single_datasource in qs_ds_owned:
            n_data_source_type = o_single_datasource.n_data_source

            if n_data_source_type == DataSourceType.GOOGLE_ANALYTICS:
                o_dsd_single = DataSourceDetail.objects.get(sv_data_source_id=o_single_datasource.pk)
                s_ga_property_or_view_id = o_dsd_single.s_data_source_serial

            if n_data_source_type == DataSourceType.NAVER_AD:
                o_dsd_single = DataSourceDetail.objects.get(sv_data_source_id=o_single_datasource.pk)
                s_nvr_customer_id = o_dsd_single.s_data_source_serial

            if n_data_source_type == DataSourceType.KAKAO:
                o_dsd_single = DataSourceDetail.objects.get(sv_data_source_id=o_single_datasource.pk)
                s_kko_moment_aid = o_dsd_single.s_data_source_serial

            if n_data_source_type == DataSourceType.ADWORDS:
                qs_dsd_single = DataSourceDetail.objects.filter(sv_data_source_id=o_single_datasource.pk)
                for o_single_datasourcedetail in qs_dsd_single:
                    lst_google_ads.append(o_single_datasourcedetail.s_data_source_serial)
            
            if n_data_source_type == DataSourceType.FB_BIZ:
                qs_dsd_single = DataSourceDetail.objects.filter(sv_data_source_id=o_single_datasource.pk)
                for o_single_datasourcedetail in qs_dsd_single:
                    lst_facebook_biz.append(o_single_datasourcedetail.s_data_source_serial)

            if n_data_source_type == DataSourceType.KAKAO:
                qs_dsd_single = DataSourceDetail.objects.filter(sv_data_source_id=o_single_datasource.pk)
                for o_single_datasourcedetail in qs_dsd_single:
                    lst_kko_moment_aid.append(o_single_datasourcedetail.s_data_source_serial)
        
        return s_ga_property_or_view_id, s_nvr_customer_id, lst_google_ads, lst_facebook_biz, lst_kko_moment_aid
