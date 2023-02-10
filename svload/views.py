import os

# begin - django related
from django.shortcuts import render, redirect
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.conf import settings
# end - django related

# begin - bokeh related
from bokeh.embed import components
# end - bokeh related

from svcommon.sv_mysql import SvMySql
from svcommon.sv_hypermart_model import BranchType
from svacct.ownership import get_owned_brand_list

from .pandas_plugins.period_window import PeriodWindow
from .pandas_plugins.edi_by_branch import EdiBranchRaw
from .pandas_plugins.edi_by_branch import EdiBranchPerformance
from .pandas_plugins.edi_by_sku import EdiSkuPerformance
from .pandas_plugins.edi_national import EdiNationalPerformance
from .pandas_plugins.edi_tools import EdiFilter
from .pandas_plugins.budget import Budget
from .pandas_plugins.ga_item import GaItem
from .pandas_plugins.contract import NvrBrsInfo
from .pandas_plugins.term_manager import BrdedTerm
from .pandas_plugins.term_manager import SeoTrackingTerm
from .pandas_plugins.campaign_alias import CampaignAliasInfo
from .pandas_plugins.search_api import SearchApiFreqTrendVisual
from .visualizer import Visualizer

# dash plotly visualiztion with AI ML
# https://www.youtube.com/watch?v=hSPmj7mK6ng

# python visualization with bokeh
# https://www.youtube.com/watch?v=2TR_6VaVSOs
# data visualization with D3.js
# https://www.youtube.com/watch?v=_8V5o2UHG0E
# Create your views here.
# https://hackernoon.com/integrating-bokeh-visualisations-into-django-projects-a1c01a16b67a

# bokeh server jinja example
# https://stackoverflow.com/questions/44878976/curdoc-add-root-causes-bokeh-plot-rendering-to-fail-silently
# jinja template tutorial
# https://www.webforefront.com/django/jinjaconfiguration.html

from bokeh import _version as bokeh_version

# for logger
import logging

logger = logging.getLogger(__name__)


class GaMedia(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'
    __g_nCntToVisitorNounRank = 100  # 추출할 순위 수
    __g_nCntToInboundKeywordRank = 10  # 추출할 순위 수
    __g_nCntToSourceMediumRank = 10  # 추출할 순위 수

    def __init__(self):
        return

    def get(self, request, *args, **kwargs):
        o_window = PeriodWindow()
        # o_window.activate_debug()
        dict_period = o_window.get_period_range(request, s_freq_mode='day')
        dict_sampling_freq_mode = o_window.get_sampling_freq_ui()  # sampling freq btn UI
        del o_window

        o_sv_db = SvMySql()
        if not o_sv_db:
            raise Exception('invalid db handler')

        dict_rst = get_brand_info(o_sv_db, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)
        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation

        from .pandas_plugins.ga_media import GmMainVisual
        lst_retrieve_attrs = ['media_ua', 'media_rst_type', 'media_source', 'media_media', 'media_imp', 'media_click',
                              'media_term', 'media_brd', 'in_site_tot_session', 'in_site_tot_bounce',
                              'in_site_tot_duration_sec', 'in_site_tot_pvs', 'in_site_tot_new',
                              'media_gross_cost_inc_vat']
        o_visualize = GmMainVisual(o_sv_db)
        o_visualize.set_period_dict(dict_period, ['tm', 'lm'])
        o_visualize.load_df(lst_retrieve_attrs)

        # get this month
        s_sort_column = 'media_gross_cost_inc_vat'  # 'gross_cpc_inc_vat'
        dict_gross_tm = o_visualize.load_source_medium_ps_only('tm', s_sort_column=s_sort_column)
        dict_mtd_tm = o_visualize.retrieve_mtd_by_column('tm')
        dict_est_tm = o_visualize.retrieve_full_by_column('tm')
        dict_mtd_by_mob, dict_mtd_by_pc = o_visualize.retrieve_period_by_ua_column('tm')
        dict_top_kws, n_other_kws_cnt = o_visualize.retrieve_gross_term(s_period_window='tm',
                                                                        s_sort_column='in_site_tot_session',
                                                                        n_th_rank=self.__g_nCntToInboundKeywordRank)
        dict_top_sources, n_other_source_cnt = \
            o_visualize.retrieve_gross_source_medium(s_period_window='tm', s_sort_column='in_site_tot_session',
                                                     n_th_rank=self.__g_nCntToSourceMediumRank)

        # plots can be a single Bokeh Model, a list/tuple, or even a dictionary
        n_brand_id = dict_rst['dict_ret']['n_brand_id']
        lst_graph_to_draw = o_visualize.get_graph_data()
        dict_plots = {}
        for lst_graph in lst_graph_to_draw:
            o_graph = Visualizer()
            o_graph.set_title(lst_graph[5])
            o_graph.set_height(lst_graph[6])
            o_graph.set_x_labels(lst_graph[2])
            for s_source_medium_name, lst_series_val in lst_graph[1].items():
                s_series_color = lst_graph[4].pop(0)
                o_graph.append_series(s_source_medium_name, s_series_color, lst_series_val)
            dict_plots[lst_graph[0]] = o_graph.draw_vertical_bar(n_max_y_axis=lst_graph[3])
            del o_graph

        # get last month
        dict_mtd_lm = o_visualize.retrieve_mtd_by_column('lm')
        script, div = components(dict(dict_plots))
        return render(request, 'svload/ga_media_main.html',
                      {'script': script, 'div': div,
                       'dict_sampling_freq_mode': dict_sampling_freq_mode,
                       's_cur_period_window': dict_period['s_cur_period_window'],
                       'dict_period_date': {'from': dict_period['dt_first_day_this_month'].strftime("%Y%m%d"),
                                            'to': dict_period['dt_today'].strftime("%Y%m%d")},
                       's_bokeh_version': bokeh_version.get_versions()['version'],
                       'n_brand_id': n_brand_id,
                       's_brand_name': s_brand_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'n_target_budget': dict_gross_tm['n_gross_budget_inc_vat'],
                       'dict_gross_tm': dict_gross_tm['dict_ps_source_medium_gross'],
                       'dict_mtd_tm': dict_mtd_tm,
                       'dict_est_tm': dict_est_tm,
                       'dict_mtd_lm': dict_mtd_lm,
                       'dict_mtd_by_mob': dict_mtd_by_mob,
                       'dict_mtd_by_pc': dict_mtd_by_pc,
                       'dict_top_kws': dict_top_kws,
                       'n_other_kws_cnt': n_other_kws_cnt,
                       'dict_top_sources': dict_top_sources,
                       'n_other_source_cnt': n_other_source_cnt,
                       'visitor_noun_n_th_rank': self.__g_nCntToVisitorNounRank,
                       'inbound_noun_n_th_rank': self.__g_nCntToInboundKeywordRank,
                       })


class GaSourceMediumView(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'

    def __init__(self):
        return

    def get(self, request, *args, **kwargs):
        o_window = PeriodWindow()
        # o_window.activate_debug()
        dict_period = o_window.get_period_range(request)
        dict_sampling_freq_mode = o_window.get_sampling_freq_ui()  # sampling freq btn UI
        del o_window

        o_sv_db = SvMySql()
        if not o_sv_db:
            raise Exception('invalid db handler')

        dict_rst = get_brand_info(o_sv_db, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)
        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation

        # from .pandas_plugins.ga_media_source_medium import Performance
        from .pandas_plugins.ga_media import SourceMediumVisual
        lst_retrieve_attrs = ['media_ua', 'media_rst_type', 'media_source', 'media_media', 'media_imp', 'media_click',
                              'media_term', 'media_brd', 'in_site_tot_session', 'in_site_tot_bounce',
                              'in_site_tot_duration_sec', 'in_site_tot_pvs', 'in_site_tot_new',
                              'media_gross_cost_inc_vat']
        o_visualize = SourceMediumVisual(o_sv_db)
        o_visualize.set_period_dict(dict_period, ['tm', 'lm', 'ly'])
        o_visualize.load_df(lst_retrieve_attrs)
        s_sort_column = 'media_gross_cost_inc_vat'  # 'gross_cpc_inc_vat'
        dict_gross_tm = o_visualize.load_source_medium_ps_only('tm', s_sort_column=s_sort_column)
        dict_gross_lm = o_visualize.load_source_medium_ps_only('lm', s_sort_column=s_sort_column)
        dict_gross_ly = o_visualize.load_source_medium_ps_only('ly', s_sort_column=s_sort_column)
        # if not b_rst_tm and not b_rst_lm and not b_rst_ly:
        #    dict_context = {'err_msg': 'period without data'}
        #    return render(request, "svload/analyze_deny.html", context=dict_context)
        lst_graph_to_draw = o_visualize.get_graph_data()

        # plots can be a single Bokeh Model, a list/tuple, or even a dictionary
        dict_plots = {}
        for lst_graph in lst_graph_to_draw:
            o_graph = Visualizer()
            o_graph.set_title(lst_graph[5])
            o_graph.set_height(lst_graph[6])
            o_graph.set_x_labels(lst_graph[2])
            for s_source_medium_name, lst_series_val in lst_graph[1].items():
                s_series_color = lst_graph[4].pop(0)
                o_graph.append_series(s_source_medium_name, s_series_color, lst_series_val)
            dict_plots[lst_graph[0]] = o_graph.draw_vertical_bar(n_max_y_axis=lst_graph[3])
            del o_graph

        script, graph = components(dict(dict_plots))
        return render(request, 'svload/ga_media_by_source_medium.html',
                      {'script': script, 'graph': graph,
                       'dict_sampling_freq_mode': dict_sampling_freq_mode,
                       's_cur_period_window': dict_period['s_cur_period_window'],
                       'dict_period_date': {'from': dict_period['dt_first_day_this_month'].strftime("%Y%m%d"),
                                            'to': dict_period['dt_today'].strftime("%Y%m%d")},
                       's_bokeh_version': bokeh_version.get_versions()['version'],
                       's_brand_name': s_brand_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'dict_gross_tm': dict_gross_tm['dict_ps_source_medium_gross'],
                       'dict_gross_lm': dict_gross_lm['dict_ps_source_medium_gross'],
                       'dict_gross_ly': dict_gross_ly['dict_ps_source_medium_gross']
                       })


class AgencyDetail(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'

    def __init__(self):
        return

    def get(self, request, *args, **kwargs):
        o_window = PeriodWindow()
        # o_window.activate_debug()
        dict_period = o_window.get_period_range(request, s_freq_mode='day')
        dict_sampling_freq_mode = o_window.get_sampling_freq_ui()  # sampling freq btn UI
        del o_window

        o_sv_db = SvMySql()
        if not o_sv_db:
            raise Exception('invalid db handler')

        dict_rst = get_brand_info(o_sv_db, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)
        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation

        from .pandas_plugins.ga_media import GmAgencyVisual
        lst_retrieve_attrs = ['media_ua', 'media_rst_type', 'media_source', 'media_media', 'media_imp', 'media_click',
                              'media_term', 'media_brd', 'in_site_tot_session', 'in_site_tot_bounce',
                              'in_site_tot_duration_sec', 'in_site_tot_pvs', 'in_site_tot_new',
                              'media_gross_cost_inc_vat']
        o_visualize = GmAgencyVisual(o_sv_db)
        o_visualize.set_period_dict(dict_period, ['tm', 'lm'])
        o_visualize.set_agency_id(kwargs['sv_agency_id'])
        o_visualize.load_df(lst_retrieve_attrs)

        # get this month
        s_sort_column = 'media_gross_cost_inc_vat'  # 'gross_cpc_inc_vat'
        dict_gross_tm = o_visualize.load_source_medium_ps_only('tm', s_sort_column=s_sort_column)
        dict_mtd_tm = o_visualize.retrieve_mtd_by_column('tm')
        dict_est_tm = o_visualize.retrieve_full_by_column('tm')
        dict_mtd_by_mob, dict_mtd_by_pc = o_visualize.retrieve_period_by_ua_column('tm')

        # plots can be a single Bokeh Model, a list/tuple, or even a dictionary
        n_brand_id = dict_rst['dict_ret']['n_brand_id']
        lst_graph_to_draw = o_visualize.get_graph_data()
        dict_plots = {}
        for lst_graph in lst_graph_to_draw:
            o_graph = Visualizer()
            o_graph.set_title(lst_graph[5])
            o_graph.set_height(lst_graph[6])
            o_graph.set_x_labels(lst_graph[2])
            for s_source_medium_name, lst_series_val in lst_graph[1].items():
                s_series_color = lst_graph[4].pop(0)
                o_graph.append_series(s_source_medium_name, s_series_color, lst_series_val)
            dict_plots[lst_graph[0]] = o_graph.draw_vertical_bar(n_max_y_axis=lst_graph[3])
            del o_graph

        # get last month
        dict_mtd_lm = o_visualize.retrieve_mtd_by_column('lm')
        script, div = components(dict(dict_plots))
        return render(request, 'svload/agency_detail.html',
                      {'script': script, 'div': div,
                       'dict_sampling_freq_mode': dict_sampling_freq_mode,
                       's_cur_period_window': dict_period['s_cur_period_window'],
                       'dict_period_date': {'from': dict_period['dt_first_day_this_month'].strftime("%Y%m%d"),
                                            'to': dict_period['dt_today'].strftime("%Y%m%d")},
                       's_bokeh_version': bokeh_version.get_versions()['version'],
                       'n_brand_id': n_brand_id,
                       's_brand_name': s_brand_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'n_target_budget': dict_gross_tm['n_gross_budget_inc_vat'],
                       'dict_gross_tm': dict_gross_tm['dict_ps_source_medium_gross'],
                       'dict_mtd_tm': dict_mtd_tm,
                       'dict_est_tm': dict_est_tm,
                       'dict_mtd_lm': dict_mtd_lm,
                       'dict_mtd_by_mob': dict_mtd_by_mob,
                       'dict_mtd_by_pc': dict_mtd_by_pc,
                       })


class GaItemPerfView(LoginRequiredMixin, TemplateView):
    __g_oSvDb = None
    __g_dictBrandInfo = {}

    def __init__(self):
        self.__g_oSvDb = SvMySql()
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')
        return

    def __del__(self):
        del self.__g_oSvDb

    def get(self, request, *args, **kwargs):
        self.__g_dictBrandInfo = get_brand_info(self.__g_oSvDb, request, kwargs)
        if self.__g_dictBrandInfo['b_error']:
            dict_context = {'err_msg': self.__g_dictBrandInfo['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        if 'item_id' in kwargs:
            return self.__item_detail(request, *args, **kwargs)
        else:
            return self.__item_list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')

        dict_rst = get_brand_info(self.__g_oSvDb, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        n_brand_id = dict_rst['dict_ret']['n_brand_id']
        s_act = request.POST.get('act')
        s_return_url = request.META.get('HTTP_REFERER')
        if s_act == 'update_item':
            n_item_srl = len(request.POST.getlist('item_srls[]'))
            if n_item_srl == 0:
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            
            n_sv_acct_id = kwargs['sv_brand_id']
            o_ga_item = GaItem(self.__g_oSvDb)
            o_ga_item.update_item(request, n_sv_acct_id, n_brand_id)
            del o_ga_item
            o_redirect = redirect('svload:ga_item', sv_brand_id=n_brand_id)
        return o_redirect

    def __item_list(self, request, *args, **kwargs):
        o_ga_item = GaItem(self.__g_oSvDb)
        dict_budget_info = o_ga_item.get_list()
        del o_ga_item

        lst_owned_brand = self.__g_dictBrandInfo['dict_ret']['lst_owned_brand']  # for global navigation
        return render(request, 'svload/item_list.html',
                      {'s_brand_name': self.__g_dictBrandInfo['dict_ret']['s_brand_name'],
                       'n_brand_id': self.__g_dictBrandInfo['dict_ret']['n_brand_id'],
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'lst_catalog': dict_budget_info['lst_catalog'],
                       })

    def __item_detail(self, request, *args, **kwargs):
        if 'item_id' not in kwargs:
            raise Exception('invalid item id')

        n_item_id = kwargs['item_id']
        n_brand_id = self.__g_dictBrandInfo['dict_ret']['n_brand_id']
        o_ga_item = GaItem(self.__g_oSvDb)
        dict_budget_info = o_ga_item.get_detail_by_id(n_brand_id, n_item_id)
        dict_budget_info['n_budget_id'] = n_item_id
        del o_ga_item
        s_brand_name = self.__g_dictBrandInfo['dict_ret']['s_brand_name']
        lst_owned_brand = self.__g_dictBrandInfo['dict_ret']['lst_owned_brand']  # for global navigation
        return render(request, 'svload/budget_detail.html',
                      {'s_brand_name': s_brand_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'dict_budget_info': dict_budget_info,
                       })


class FocusTodayEdi(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'

    def __init__(self):
        return

    def get(self, request, *args, **kwargs):
        o_window = PeriodWindow()
        dict_period = o_window.get_period_range(request)
        dict_sampling_freq_mode = o_window.get_sampling_freq_ui()  # sampling freq btn UI
        del o_window

        o_sv_db = SvMySql()
        if not o_sv_db:
            raise Exception('invalid db handler')

        dict_rst = get_brand_info(o_sv_db, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)
        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation

        # begin - retrieve emart sku info
        o_filter = EdiFilter(request)
        dict_sales_ch_info = o_filter.get_sales_ch()
        dict_branch_info = o_filter.get_branch()
        dict_sku_info = o_filter.get_sku_by_brand_id(o_sv_db)
        # print(dict_sku_info_by_id)
        del o_filter
        # end - retrieve emart sku info

        o_edi_raw = EdiBranchRaw()
        o_edi_raw.set_period_dict(dt_start=dict_period['dt_first_day_2year_ago'], dt_end=dict_period['dt_today'])
        o_edi_raw.set_freq(dict_sampling_freq_mode)
        o_edi_raw.set_sku_dict(dict_sku_info['dict_sku_info_by_id'])
        o_edi_raw.set_branch_info(dict_branch_info)
        df_edi_raw = o_edi_raw.load_national(o_sv_db)
        del o_edi_raw
        del o_sv_db

        o_perf_to_visualize = EdiNationalPerformance()
        o_perf_to_visualize.set_period_dict(dict_period)
        o_perf_to_visualize.set_sku_dict(dict_sku_info['dict_sku_info_by_id'])
        o_perf_to_visualize.set_all_branches(dict_branch_info)  # ['dict_branch_info_for_dataframe'])
        o_perf_to_visualize.load_df(df_edi_raw)

        dict_branch_gross = o_perf_to_visualize.retrieve_branch_gross_in_period()  # arrange branches by tm amnt
        lst_branch_data_table = o_perf_to_visualize.retrieve_branch_rank_in_period()  # arrange branches by tm amnt

        # begin - 당월 공급액 순위 & 당월 출고량 비교
        dict_4_h_bar_graph = o_perf_to_visualize.retrieve_sku_rank_in_period()
        dict_plots = {}
        o_graph = Visualizer()
        o_graph.set_title('품목별 총 공급액')
        o_graph.set_height(400)
        o_graph.set_x_labels(dict_4_h_bar_graph['lst_sku_name_by_tm_amnt'])
        o_graph.append_series('2LY', dict_4_h_bar_graph['lst_bar_color'][0],
                              dict_4_h_bar_graph['lst_2ly_sku_by_tm_amnt'])
        o_graph.append_series('LY', dict_4_h_bar_graph['lst_bar_color'][1], dict_4_h_bar_graph['lst_ly_sku_by_tm_amnt'])
        o_graph.append_series('LM', dict_4_h_bar_graph['lst_bar_color'][2], dict_4_h_bar_graph['lst_lm_sku_by_tm_amnt'])
        o_graph.append_series('TM', dict_4_h_bar_graph['lst_bar_color'][3], dict_4_h_bar_graph['lst_tm_sku_by_tm_amnt'])
        dict_plots['sell_in_amnt'] = o_graph.draw_horizontal_bar()
        # del dict_data
        del o_graph

        o_graph = Visualizer()
        o_graph.set_title('품목별 총 출고량')
        o_graph.set_height(400)
        o_graph.set_x_labels(dict_4_h_bar_graph['lst_sku_name_by_tm_amnt'])
        o_graph.append_series('2LY', dict_4_h_bar_graph['lst_bar_color'][0],
                              dict_4_h_bar_graph['lst_2ly_sku_by_tm_qty'])
        o_graph.append_series('LY', dict_4_h_bar_graph['lst_bar_color'][1], dict_4_h_bar_graph['lst_ly_sku_by_tm_qty'])
        o_graph.append_series('LM', dict_4_h_bar_graph['lst_bar_color'][2], dict_4_h_bar_graph['lst_lm_sku_by_tm_qty'])
        o_graph.append_series('TM', dict_4_h_bar_graph['lst_bar_color'][3], dict_4_h_bar_graph['lst_tm_sku_by_tm_qty'])
        dict_plots['sell_out_qty'] = o_graph.draw_horizontal_bar()
        # del dict_data
        del o_graph
        # end - 당월 공급액 순위 & 당월 출고량 비교

        # 퓸목별 현황 - top 10
        dict_top_5_sku_table, n_sku_dashboard_div_height_px = o_perf_to_visualize.retrieve_top_sku_chronicle(20)
        del o_perf_to_visualize

        script, div = components(dict(dict_plots))
        return render(request, 'svload/edi_main.html',
                      {'script': script, 'div': div,
                       'dict_sampling_freq_mode': dict_sampling_freq_mode,
                       's_cur_period_window': dict_period['s_cur_period_window'],
                       'dict_period_date': {'from': dict_period['dt_first_day_this_month'].strftime("%Y%m%d"),
                                            'to': dict_period['dt_today'].strftime("%Y%m%d")},
                       's_bokeh_version': bokeh_version.get_versions()['version'],
                       's_brand_name': s_brand_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'dict_sales_ch_info_by_id': dict_sales_ch_info['dict_sales_ch_info_for_ui'],
                       's_sales_ch_filter_mode': dict_sales_ch_info['s_sales_ch_filter_mode'],
                       'dict_branch_info_by_id': dict_branch_info['dict_branch_info_for_ui'],
                       's_branch_filter_mode': dict_branch_info['s_branch_filter_mode'],
                       'dict_sku_info_by_id': dict_sku_info['dict_sku_info_for_ui'],
                       's_sku_filter_mode': dict_sku_info['s_sku_filter_mode'],
                       'dict_branch_gross': dict_branch_gross,
                       'lst_branch_data_table': lst_branch_data_table,
                       'dict_top_5_sku_table': dict_top_5_sku_table,
                       'n_sku_dashboard_div_height_px': n_sku_dashboard_div_height_px
                       })


class ByBranchEdi(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'

    def __init__(self):
        return

    def get(self, request, *args, **kwargs):
        o_window = PeriodWindow()
        dict_period = o_window.get_period_range(request)
        dict_sampling_freq_mode = o_window.get_sampling_freq_ui()  # sampling freq btn UI
        del o_window

        o_sv_db = SvMySql()
        if not o_sv_db:
            raise Exception('invalid db handler')

        dict_rst = get_brand_info(o_sv_db, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)
        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        n_branch_id = kwargs['branch_id']
        lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation

        # begin - retrieve emart sku info
        o_filter = EdiFilter(request)
        dict_branch_info = o_filter.get_branch(n_branch_id)
        dict_sku_info = o_filter.get_sku_by_brand_id(o_sv_db)
        del o_filter
        # end - retrieve emart sku info

        # begin - set branch name for template
        dict_single_branch_info = list(dict_branch_info['dict_branch_info_for_ui'].values())[0]
        s_sch_name = dict_single_branch_info['hypermart_name']
        s_branch_name = dict_single_branch_info['name']
        # end - set branch name for template

        o_edi_raw = EdiBranchRaw()
        o_edi_raw.set_period_dict(dt_start=dict_period['dt_first_day_2year_ago'], dt_end=dict_period['dt_today'])
        o_edi_raw.set_freq(dict_sampling_freq_mode)
        o_edi_raw.set_branch_info(dict_branch_info)
        o_edi_raw.set_sku_dict(dict_sku_info['dict_sku_info_by_id'])
        df_edi_raw = o_edi_raw.load_branch(o_sv_db)
        del o_edi_raw

        # get whole period
        o_whole_perf_summary = EdiBranchPerformance()
        o_whole_perf_summary.set_period_dict(dict_period)
        o_whole_perf_summary.set_single_branch_info(dict_branch_info)
        o_whole_perf_summary.set_sku_dict(dict_sku_info['dict_sku_info_by_id'])
        o_whole_perf_summary.load_df(df_edi_raw)

        dict_plots = {}  # defaultdict()  # graph dict to draw
        # begin - 당월 품목별 공급액 순위, 당월 품목별 출고량 비교
        dict_4_h_bar_graph = o_whole_perf_summary.retrieve_sku_rank_in_period('amount')
        # plots can be a single Bokeh Model, a list/tuple, or even a dictionary
        o_graph = Visualizer()
        o_graph.set_title('품목별 총 공급액')
        o_graph.set_height(380)
        o_graph.set_x_labels(dict_4_h_bar_graph['lst_sku_name_by_tm_amnt'])
        o_graph.append_series('2LY', '#D6E2DF', dict_4_h_bar_graph['dict_amnt_by_sku']['2ly'])
        o_graph.append_series('LY', '#A4C8C1', dict_4_h_bar_graph['dict_amnt_by_sku']['ly'])
        o_graph.append_series('LM', '#6CBDAC', dict_4_h_bar_graph['dict_amnt_by_sku']['lm'])
        o_graph.append_series('TM', '#079476', dict_4_h_bar_graph['dict_amnt_by_sku']['tm'])
        dict_plots['sell_in_amnt'] = o_graph.draw_horizontal_bar()
        # del dict_data
        del o_graph

        o_graph = Visualizer()
        o_graph.set_title('품목별 총 출고량')
        o_graph.set_height(380)
        o_graph.set_x_labels(dict_4_h_bar_graph['lst_sku_name_by_tm_amnt'])
        o_graph.append_series('2LY', '#D6E2DF', dict_4_h_bar_graph['dict_qty_by_sku']['2ly'])
        o_graph.append_series('LY', '#A4C8C1', dict_4_h_bar_graph['dict_qty_by_sku']['ly'])
        o_graph.append_series('LM', '#6CBDAC', dict_4_h_bar_graph['dict_qty_by_sku']['lm'])
        o_graph.append_series('TM', '#079476', dict_4_h_bar_graph['dict_qty_by_sku']['tm'])
        dict_plots['sell_out_qty'] = o_graph.draw_horizontal_bar()
        # del dict_data
        del o_graph
        # end - 당월 품목별 공급액 순위, 당월 품목별 출고량 비교

        # begin - Top 4 공급액 추이 2년간
        lst_item_line_color = ['#D6E2DF', '#A4C8C1', '#6CBDAC', '#079476']  # last one is larger one
        dict_4_multi_line = o_whole_perf_summary.retrieve_monthly_chronicle_by_sku_ml(lst_item_line_color)
        # dict_plots['monthly'] = draw_multi_line_graph(lst_line_data=dict_4_multi_line['lst_series_amnt'],

        n_cnt = min([len(dict_4_multi_line['lst_line_label']), 4])
        o_graph = Visualizer()
        o_graph.set_title('Top4 품목의 공급액 추이')
        o_graph.set_height(380)
        o_graph.set_x_labels(dict_4_multi_line['lst_x_label'])
        for i in range(0, n_cnt):
            o_graph.append_series(dict_4_multi_line['lst_line_label'][i], dict_4_multi_line['lst_line_color'][i],
                                dict_4_multi_line['lst_series_amnt'][i])
        dict_plots['monthly'] = o_graph.draw_multi_line()
        del o_graph
        # end - Top 4 공급액 추이 2년간

        # begin - 매장 공급액, 전국 공급액, 매장 출고량, 전국 출고량
        dict_national_overview, dict_branch_overview = o_whole_perf_summary.retrieve_branch_level_overview(o_sv_db)
        # end - 매장 공급액, 전국 공급액, 매장 출고량, 전국 출고량
        del o_sv_db

        # begin - 퓸목별 현황
        dict_sku_perf_table = o_whole_perf_summary.retrieve_sku_chronicle()
        # end - 퓸목별 현황
        del o_whole_perf_summary

        script, div = components(dict(dict_plots))
        return render(request, 'svload/edi_by_branch.html',
                      {'script': script, 'div': div,
                       'dict_sampling_freq_mode': dict_sampling_freq_mode,
                       's_cur_period_window': dict_period['s_cur_period_window'],
                       'dict_period_date': {'from': dict_period['dt_first_day_this_month'].strftime("%Y%m%d"),
                                            'to': dict_period['dt_today'].strftime("%Y%m%d")},
                       's_bokeh_version': bokeh_version.get_versions()['version'],
                       's_brand_name': s_brand_name,
                       's_sch_name': s_sch_name,
                       's_branch_name': s_branch_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'dict_branch_info': dict_single_branch_info,
                       'dict_national_overview': dict_national_overview,
                       'dict_branch_overview': dict_branch_overview,
                       'dict_all_sku_table': dict_sku_perf_table,
                       'BranchType': BranchType,  # for naver map api permission
                       'ALLOWED_HOSTS': settings.ALLOWED_HOSTS  # for naver map api permission
                       })

    def post(self, request, *args, **kwargs):
        o_sv_db = SvMySql()
        if not o_sv_db:
            raise Exception('invalid db handler')

        dict_rst = get_brand_info(o_sv_db, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        o_branch = EdiBranchPerformance()
        dict_rst = o_branch.add_memo(o_sv_db, int(kwargs['branch_id']), n_brand_id, request)
        del o_branch
        del o_sv_db
        return
        return redirect('svload:edi_branch', owner_id=kwargs['owner_id'], ga_view_id=kwargs['ga_view_id'],
                        branch_id=kwargs['branch_id'])


class BySkuEdi(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'

    def __init__(self):
        return

    def get(self, request, *args, **kwargs):
        o_window = PeriodWindow()
        dict_period = o_window.get_period_range(request)
        dict_sampling_freq_mode = o_window.get_sampling_freq_ui()  # sampling freq btn UI
        del o_window

        o_sv_db = SvMySql()
        if not o_sv_db:
            raise Exception('invalid db handler')
        dict_rst = get_brand_info(o_sv_db, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)
        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation

        # begin - retrieve emart sku info
        o_filter = EdiFilter(request)
        dict_branch_info = o_filter.get_branch()
        dict_sku_info = o_filter.get_sku_by_brand_id(o_sv_db, n_sku_id=kwargs['item_id'])
        del o_filter
        # end - retrieve emart sku info

        # set item name for template
        s_item_id = kwargs['item_id']
        if s_item_id in dict_sku_info['dict_sku_info_by_id']:
            s_item_name = dict_sku_info['dict_sku_info_by_id'][kwargs['item_id']]['hypermart_name'] + ' ' + \
                        dict_sku_info['dict_sku_info_by_id'][kwargs['item_id']]['name']
        else:
            s_item_name = 'unknown item'

        o_edi_raw = EdiBranchRaw()
        o_edi_raw.set_period_dict(dt_start=dict_period['dt_first_day_2year_ago'], dt_end=dict_period['dt_today'])
        o_edi_raw.set_freq(dict_sampling_freq_mode)
        o_edi_raw.set_sku_dict(dict_sku_info['dict_sku_info_by_id'])
        o_edi_raw.set_branch_info(dict_branch_info)
        df_edi_raw = o_edi_raw.load_sku(o_sv_db)
        del o_edi_raw
        del o_sv_db

        # get whole period
        o_whole_perf_summary = EdiSkuPerformance()
        o_whole_perf_summary.set_period_dict(dict_period)
        o_whole_perf_summary.set_sku_dict(dict_sku_info['dict_sku_info_by_id'])
        o_whole_perf_summary.set_all_branches(dict_branch_info)
        o_whole_perf_summary.load_df(df_edi_raw)

        dict_plots = {}  # graph dict to draw
        # begin - 당월 품목별 공급액 순위, 당월 품목별 출고량 비교
        # end - 당월 품목별 공급액 순위, 당월 품목별 출고량 비교

        # begin - 전국 공급액 추이 2년간
        dict_rst_tm = o_whole_perf_summary.retrieve_monthly_gross_vb()
        o_graph = Visualizer()
        o_graph.set_title('최근 2년간 공급액')
        o_graph.set_height(170)
        o_graph.set_x_labels(dict_rst_tm['lst_x_labels'])
        # for s_scope_name in dict_rst_tm['lst_series_lbl']:
        for s_scope_name, lst_series_val in dict_rst_tm['lst_series_info'].items():
            s_series_color = dict_rst_tm['lst_palette'].pop(0)
            # lst_series_val = dict_rst_tm['lst_series_val'].pop(0)
            o_graph.append_series(s_scope_name, s_series_color, lst_series_val)
        dict_plots['monthly_national'] = o_graph.draw_vertical_bar(n_max_y_axis=None)
        del o_graph
        # end - 전국 공급액 추이 2년간

        # begin - 상위 매장 공급액 추이 2년간
        lst_item_line_color = ['#D6E2DF', '#A4C8C1', '#6CBDAC', '#079476', '#097C63',
                               '#026E57']  # last one is larger one
        s_top_branch_cnt = str(len(lst_item_line_color))
        # s_graph_title = '당월 Top ' + s_top_branch_cnt + ' 매장의 최근 2년간 공급액'
        dict_rst_tm = o_whole_perf_summary.retrieve_monthly_gross_by_branch_vb(lst_item_line_color)
        o_graph = Visualizer()
        o_graph.set_title('당월 Top ' + s_top_branch_cnt + ' 매장의 최근 2년간 공급액')
        o_graph.set_height(170)
        o_graph.set_x_labels(dict_rst_tm['lst_x_labels'])
        for s_branch_name, lst_series_val in dict_rst_tm['lst_series_info'].items():
            s_series_color = dict_rst_tm['lst_palette'].pop(0)
            o_graph.append_series(s_branch_name, s_series_color, lst_series_val)
        dict_plots['monthly_branch_top4'] = o_graph.draw_vertical_bar(n_max_y_axis=None)
        del o_graph
        # end - Top4 공급액 추이 2년간

        # begin - 매장 공급액, 전국 공급액, 매장 출고량, 전국 출고량
        lst_branch_data_table = o_whole_perf_summary.retrieve_branch_overview()
        # end - 매장 공급액, 전국 공급액, 매장 출고량, 전국 출고량

        script, div = components(dict(dict_plots))
        return render(request, 'svload/edi_by_sku.html',
                      {'script': script, 'div': div,
                       'dict_sampling_freq_mode': dict_sampling_freq_mode,
                       's_cur_period_window': dict_period['s_cur_period_window'],
                       'dict_period_date': {'from': dict_period['dt_first_day_this_month'].strftime("%Y%m%d"),
                                            'to': dict_period['dt_today'].strftime("%Y%m%d")},
                       's_bokeh_version': bokeh_version.get_versions()['version'],
                       's_brand_name': s_brand_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       's_item_name': s_item_name,
                       's_top_branch_cnt': s_top_branch_cnt,
                       'lst_branch_data_table': lst_branch_data_table
                       })


class BudgetView(LoginRequiredMixin, TemplateView):
    __g_oSvDb = None
    __g_dictBrandInfo = {}

    def __init__(self):
        self.__g_oSvDb = SvMySql()
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')
        return

    def __del__(self):
        if self.__g_oSvDb:
            del self.__g_oSvDb
        if self.__g_dictBrandInfo:
            del self.__g_dictBrandInfo

    def get(self, request, *args, **kwargs):
        self.__g_dictBrandInfo = get_brand_info(self.__g_oSvDb, request, kwargs)
        if self.__g_dictBrandInfo['b_error']:
            dict_context = {'err_msg': self.__g_dictBrandInfo['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        if 'budget_id' in kwargs:
            return self.__budget_detail(request, *args, **kwargs)
        else:
            return self.__budget_list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')

        self.__g_dictBrandInfo = get_brand_info(self.__g_oSvDb, request, kwargs)
        if self.__g_dictBrandInfo['b_error']:
            dict_context = {'err_msg': self.__g_dictBrandInfo['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        n_brand_id = self.__g_dictBrandInfo['dict_ret']['n_brand_id']
        s_act = request.POST.get('act')
        s_return_url = request.META.get('HTTP_REFERER')
        if s_act == 'add_budget':
            o_budget = Budget(self.__g_oSvDb)
            dict_rst = o_budget.add_budget(request)
            del o_budget
            if dict_rst['b_error']:
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)

            o_redirect = redirect('svload:budget_list', sv_brand_id=n_brand_id)
        elif s_act == 'update_budget':
            if request.POST['budget_id'] == '':
                dict_context = {'err_msg': 'invalid_budget_id', 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            n_budget_id = int(request.POST['budget_id'])
            o_budget = Budget(self.__g_oSvDb)
            o_budget.update_budget(n_budget_id, request)
            del o_budget
            o_redirect = redirect('svload:budget_list', sv_brand_id=n_brand_id)
        elif s_act == 'inquiry_budget':
            s_period_from = request.POST.get('budget_period_from')
            s_period_to = request.POST.get('budget_period_to')
            o_redirect = redirect('svload:budget_period',
                                  sv_brand_id=n_brand_id, period_from=s_period_from, period_to=s_period_to)
        return o_redirect

    def __budget_list(self, request, *args, **kwargs):
        if 'period_from' in kwargs:
            s_period_from = kwargs['period_from']
        else:
            s_period_from = None
        if 'period_to' in kwargs:
            s_period_to = kwargs['period_to']
        else:
            s_period_to = None

        o_budget = Budget(self.__g_oSvDb)
        dict_agency_list = o_budget.get_agency_list_for_ui()
        lst_acct_list = o_budget.get_acct_list_for_ui()
        dict_budget_info = o_budget.get_list_by_period(s_period_from, s_period_to)
        del o_budget
        lst_owned_brand = self.__g_dictBrandInfo['dict_ret']['lst_owned_brand']  # for global navigation
        return render(request, 'svload/budget_list.html',
                      {'s_brand_name': self.__g_dictBrandInfo['dict_ret']['s_brand_name'],
                       'n_brand_id': self.__g_dictBrandInfo['dict_ret']['n_brand_id'],
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'dict_budget_period': dict_budget_info['dict_budget_period'],
                       'lst_budget_table': dict_budget_info['lst_added_rst'],
                       'dict_budget_progress': dict_budget_info['dict_budget_progress'],
                       'lst_acct_list': lst_acct_list,
                       'dict_agency_list': dict_agency_list,
                       })

    def __budget_detail(self, request, *args, **kwargs):
        if 'budget_id' not in kwargs:
            raise Exception('invalid budget id')

        if 'period_from' in kwargs:
            s_period_from = kwargs['period_from']
        else:
            s_period_from = None

        if 'period_to' in kwargs:
            s_period_to = kwargs['period_to']
        else:
            s_period_to = None

        n_budget_id = kwargs['budget_id']
        o_budget = Budget(self.__g_oSvDb)
        dict_budget_info = o_budget.get_detail_by_id(n_budget_id)
        dict_budget_info['n_budget_id'] = n_budget_id
        dict_period_info = {'s_earliest_budget': s_period_from, 's_latest_budget': s_period_to}
        lst_acct_list = o_budget.get_acct_list_for_ui()
        dict_agency_list = o_budget.get_agency_list_for_ui()
        del o_budget
        s_brand_name = self.__g_dictBrandInfo['dict_ret']['s_brand_name']
        lst_owned_brand = self.__g_dictBrandInfo['dict_ret']['lst_owned_brand']  # for global navigation
        return render(request, 'svload/budget_detail.html',
                      {'s_brand_name': s_brand_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'dict_budget_info': dict_budget_info,
                       'dict_budget_period': dict_period_info,
                       'lst_acct_list': lst_acct_list,
                       'dict_agency_list': dict_agency_list,
                       })


class NvrBrsContractView(LoginRequiredMixin, TemplateView):
    __g_oSvDb = None
    __g_dictBrandInfo = {}

    def __init__(self):
        self.__g_oSvDb = SvMySql()
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')
        return

    def __del__(self):
        if self.__g_oSvDb:
            del self.__g_oSvDb
        if self.__g_dictBrandInfo:
            del self.__g_dictBrandInfo

    def get(self, request, *args, **kwargs):
        self.__g_dictBrandInfo = get_brand_info(self.__g_oSvDb, request, kwargs)
        if self.__g_dictBrandInfo['b_error']:
            dict_context = {'err_msg': self.__g_dictBrandInfo['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        if 'contract_srl' in kwargs:
            return self.__contract_detail(request, *args, **kwargs)
        else:
            return self.__contract_list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')

        self.__g_dictBrandInfo = get_brand_info(self.__g_oSvDb, request, kwargs)
        if self.__g_dictBrandInfo['b_error']:
            dict_context = {'err_msg': self.__g_dictBrandInfo['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        n_brand_id = self.__g_dictBrandInfo['dict_ret']['n_brand_id']
        s_act = request.POST.get('act')
        s_return_url = request.META.get('HTTP_REFERER')
        if s_act == 'add_contract_bulk':
            o_nvr_brs_info = NvrBrsInfo(self.__g_oSvDb)
            dict_rst = o_nvr_brs_info.add_contract_bulk(request)
            del o_nvr_brs_info
            if dict_rst['b_error']:
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            o_redirect = redirect('svload:nvr_brs_contract_list', sv_brand_id=n_brand_id)
        elif s_act == 'add_contract_barter':
            o_nvr_brs_info = NvrBrsInfo(self.__g_oSvDb)
            dict_rst = o_nvr_brs_info.add_contract_barter(request)
            del o_nvr_brs_info
            if dict_rst['b_error']:
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            o_redirect = redirect('svload:nvr_brs_contract_list', sv_brand_id=n_brand_id)
        elif s_act == 'update_contract':
            if request.POST['contract_srl'] == '':
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            o_nvr_brs_info = NvrBrsInfo(self.__g_oSvDb)
            dict_rst = o_nvr_brs_info.update_contract(request)
            del o_nvr_brs_info
            if dict_rst['b_error']:
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            o_redirect = redirect('svload:nvr_brs_contract_list', sv_brand_id=n_brand_id)
        elif s_act == 'delete_contract':
            if request.POST['contract_srl'] == '':
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            o_nvr_brs_info = NvrBrsInfo(self.__g_oSvDb)
            dict_rst = o_nvr_brs_info.delete_contract(request)
            del o_nvr_brs_info
            if dict_rst['b_error']:
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            o_redirect = redirect('svload:nvr_brs_contract_list', sv_brand_id=n_brand_id)
        elif s_act == 'inquiry_contract':
            s_period_from = request.POST.get('contract_period_from')
            s_period_to = request.POST.get('contract_period_to')
            o_redirect = redirect('svload:nvr_brs_contract_list_period',
                                  sv_brand_id=n_brand_id, period_from=s_period_from, period_to=s_period_to)
        return o_redirect

    def __contract_list(self, request, *args, **kwargs):
        if 'period_from' in kwargs:
            s_period_from = kwargs['period_from']
        else:
            s_period_from = None
        if 'period_to' in kwargs:
            s_period_to = kwargs['period_to']
        else:
            s_period_to = None
        o_nvr_brs_info = NvrBrsInfo(self.__g_oSvDb)
        dict_contract_info = o_nvr_brs_info.get_list_by_period(s_period_from, s_period_to)
        lst_ua = o_nvr_brs_info.get_ua_list()
        del o_nvr_brs_info
        lst_owned_brand = self.__g_dictBrandInfo['dict_ret']['lst_owned_brand']  # for global navigation
        return render(request, 'svload/nvr_brs_contract_list.html',
                      {'s_brand_name': self.__g_dictBrandInfo['dict_ret']['s_brand_name'],
                       'n_brand_id': self.__g_dictBrandInfo['dict_ret']['n_brand_id'],
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'lst_ua': lst_ua,
                       'dict_contract_period': dict_contract_info['dict_contract_period'],
                       'lst_contract_table': dict_contract_info['lst_contract_rst'],
                       })

    def __contract_detail(self, request, *args, **kwargs):
        if 'contract_srl' not in kwargs:
            raise Exception('invalid contract srl')

        n_contract_srl = int(kwargs['contract_srl'])
        o_nvr_brs_info = NvrBrsInfo(self.__g_oSvDb)
        dict_contract_info = o_nvr_brs_info.get_detail_by_srl(n_contract_srl)
        lst_ua = o_nvr_brs_info.get_ua_list()
        del o_nvr_brs_info
        s_brand_name = self.__g_dictBrandInfo['dict_ret']['s_brand_name']
        lst_owned_brand = self.__g_dictBrandInfo['dict_ret']['lst_owned_brand']  # for global navigation
        return render(request, 'svload/nvr_brs_contract_detail.html',
                      {'s_brand_name': s_brand_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'lst_ua': lst_ua,
                       'dict_contract_info': dict_contract_info,
                       })


class TermManagerView(LoginRequiredMixin, TemplateView):
    __g_oSvDb = None
    __g_dictBrandInfo = {}

    def __init__(self):
        self.__g_oSvDb = SvMySql()
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')
        self.__g_sBrandedTruncPath = None
        return

    def __del__(self):
        self.__g_sBrandedTruncPath = None
        if self.__g_oSvDb:
            del self.__g_oSvDb
        if self.__g_dictBrandInfo:
            del self.__g_dictBrandInfo

    def get(self, request, *args, **kwargs):
        self.__g_dictBrandInfo = get_brand_info(self.__g_oSvDb, request, kwargs)
        if self.__g_dictBrandInfo['b_error']:
            dict_context = {'err_msg': self.__g_dictBrandInfo['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        s_sv_acct_id = str(self.__g_dictBrandInfo['dict_ret']['n_acct_id'])
        s_brand_id = str(self.__g_dictBrandInfo['dict_ret']['n_brand_id'])
        self.__g_sBrandedTruncPath = os.path.join(settings.SV_STORAGE_ROOT, s_sv_acct_id, s_brand_id, 'branded_term.conf')

        # begin - retrieve brded terms
        o_brded_term = BrdedTerm(self.__g_sBrandedTruncPath)
        lst_brded_term = o_brded_term.get_list()
        del o_brded_term
        # end - retrieve brded terms

        # begin - retrieve SEO monitoring terms
        o_seo_tracking_term = SeoTrackingTerm(self.__g_oSvDb)
        lst_seo_monitoring_term = o_seo_tracking_term.get_list()
        del o_seo_tracking_term
        # end - retrieve SEO monitoring terms
        lst_owned_brand = self.__g_dictBrandInfo['dict_ret']['lst_owned_brand']  # for global navigation
        return render(request, 'svload/brded_term_list.html',
                      {'s_brand_name': self.__g_dictBrandInfo['dict_ret']['s_brand_name'],
                       'n_brand_id': self.__g_dictBrandInfo['dict_ret']['n_brand_id'],
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'lst_brded_term': lst_brded_term,
                       'lst_seo_monitoring_term': lst_seo_monitoring_term
                       })

    def post(self, request, *args, **kwargs):
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')

        self.__g_dictBrandInfo = get_brand_info(self.__g_oSvDb, request, kwargs)
        if self.__g_dictBrandInfo['b_error']:
            dict_context = {'err_msg': self.__g_dictBrandInfo['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        n_brand_id = self.__g_dictBrandInfo['dict_ret']['n_brand_id']
        s_sv_acct_id = str(self.__g_dictBrandInfo['dict_ret']['n_acct_id'])
        self.__g_sBrandedTruncPath = os.path.join(settings.SV_STORAGE_ROOT, s_sv_acct_id, str(n_brand_id), 'branded_term.conf')

        s_act = request.POST.get('act')
        if s_act == 'update_brded_term':
            s_config_loc_param = 'config_loc=' + s_sv_acct_id + '/' + str(n_brand_id)
            o_brded_term = BrdedTerm(self.__g_sBrandedTruncPath)
            o_brded_term.update_list(request, s_config_loc_param)
            del o_brded_term
            o_redirect = redirect('svload:term_manager', sv_brand_id=n_brand_id)
        elif s_act == 'add_seo_term':
            o_seo_tracking_term = SeoTrackingTerm(self.__g_oSvDb)
            o_seo_tracking_term.add_term(request)
            del o_seo_tracking_term
            o_redirect = redirect('svload:term_manager', sv_brand_id=n_brand_id)
        elif s_act == 'toggle_seo_term':
            o_seo_tracking_term = SeoTrackingTerm(self.__g_oSvDb)
            o_seo_tracking_term.toggle_term(request)
            del o_seo_tracking_term
            o_redirect = redirect('svload:term_manager', sv_brand_id=n_brand_id)
        return o_redirect


class CampaignAliasView(LoginRequiredMixin, TemplateView):
    # __g_oSvDb = None
    # __g_dictBrandInfo = {}

    def __init__(self):
        self.__g_oSvDb = SvMySql()
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')
        self.__g_dictBrandInfo = {}
        return

    def __del__(self):
        if self.__g_oSvDb:
            del self.__g_oSvDb
        if self.__g_dictBrandInfo:
            del self.__g_dictBrandInfo

    def get(self, request, *args, **kwargs):
        self.__g_dictBrandInfo = get_brand_info(self.__g_oSvDb, request, kwargs)
        if self.__g_dictBrandInfo['b_error']:
            dict_context = {'err_msg': self.__g_dictBrandInfo['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        if 'alias_id' in kwargs:
            return self.__alias_detail(request, *args, **kwargs)
        else:
            return self.__alias_list(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')

        self.__g_dictBrandInfo = get_brand_info(self.__g_oSvDb, request, kwargs)
        if self.__g_dictBrandInfo['b_error']:
            dict_context = {'err_msg': self.__g_dictBrandInfo['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        n_acct_id = self.__g_dictBrandInfo['dict_ret']['n_acct_id']
        n_brand_id = self.__g_dictBrandInfo['dict_ret']['n_brand_id']
        s_act = request.POST.get('act')
        s_return_url = request.META.get('HTTP_REFERER')
        if s_act == 'add_alias_bulk':
            o_campaign_alias_info = CampaignAliasInfo(n_acct_id, n_brand_id)
            dict_rst = o_campaign_alias_info.add_alias_bulk(request)
            del o_campaign_alias_info
            if dict_rst['b_error']:
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            o_redirect = redirect('svload:campaign_alias_list', sv_brand_id=n_brand_id)
        elif s_act == 'add_alias_single':
            o_campaign_alias_info = CampaignAliasInfo(n_acct_id, n_brand_id)
            dict_rst = o_campaign_alias_info.add_alias_single(request)
            del o_campaign_alias_info
            if dict_rst['b_error']:
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            o_redirect = redirect('svload:campaign_alias_list', sv_brand_id=n_brand_id)
        elif s_act == 'update_alias':
            if request.POST['alias_id'] == '':
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)
            o_campaign_alias_info = CampaignAliasInfo(n_acct_id, n_brand_id)
            o_campaign_alias_info.update_alias(request)
            del o_campaign_alias_info
            o_redirect = redirect('svload:campaign_alias_list', sv_brand_id=n_brand_id)
        elif s_act == 'inquiry_alias':
            s_period_from = request.POST.get('alias_period_from')
            s_period_to = request.POST.get('alias_period_to')
            o_redirect = redirect('svload:campaign_alias_list_period',
                                  sv_brand_id=n_brand_id, period_from=s_period_from, period_to=s_period_to)
        return o_redirect

    def __alias_list(self, request, *args, **kwargs):
        if 'period_from' in kwargs:
            s_period_from = kwargs['period_from']
        else:
            s_period_from = None
        if 'period_to' in kwargs:
            s_period_to = kwargs['period_to']
        else:
            s_period_to = None

        n_acct_id = self.__g_dictBrandInfo['dict_ret']['n_acct_id']
        n_brand_id = self.__g_dictBrandInfo['dict_ret']['n_brand_id']
        o_campaign_alias_info = CampaignAliasInfo(n_acct_id, n_brand_id)
        dict_alias_info = o_campaign_alias_info.get_list_by_period(s_period_from, s_period_to)
        dict_source_type = o_campaign_alias_info.get_source_type_dict()
        dict_search_rst_type = o_campaign_alias_info.get_search_rst_type_id_title_dict()
        dict_medium_type = o_campaign_alias_info.get_medium_type_id_title_dict()
        del o_campaign_alias_info
        lst_owned_brand = self.__g_dictBrandInfo['dict_ret']['lst_owned_brand']  # for global navigation
        return render(request, 'svload/campalign_alias_list.html',
                      {'s_brand_name': self.__g_dictBrandInfo['dict_ret']['s_brand_name'],
                       'n_brand_id': self.__g_dictBrandInfo['dict_ret']['n_brand_id'],
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'dict_alias_period': dict_alias_info['dict_alias_period'],
                       'dict_source_type': dict_source_type,
                       'dict_search_rst_type': dict_search_rst_type,
                       'dict_medium_type': dict_medium_type,
                       'lst_campaign_alias_table': dict_alias_info['lst_alias_rst'],
                       })

    def __alias_detail(self, request, *args, **kwargs):
        if 'alias_id' not in kwargs:
            raise Exception('invalid alias id')

        n_alias_id = int(kwargs['alias_id'])
        n_acct_id = self.__g_dictBrandInfo['dict_ret']['n_acct_id']
        n_brand_id = self.__g_dictBrandInfo['dict_ret']['n_brand_id']
        o_campaign_alias_info = CampaignAliasInfo(n_acct_id, n_brand_id)
        dict_alias_info = o_campaign_alias_info.get_detail_by_id(n_alias_id)
        dict_source_type = o_campaign_alias_info.get_source_type_dict()
        dict_search_rst_type = o_campaign_alias_info.get_search_rst_type_id_title_dict()
        dict_medium_type = o_campaign_alias_info.get_medium_type_id_title_dict()
        del o_campaign_alias_info
        s_brand_name = self.__g_dictBrandInfo['dict_ret']['s_brand_name']
        lst_owned_brand = self.__g_dictBrandInfo['dict_ret']['lst_owned_brand']  # for global navigation
        return render(request, 'svload/campaign_alias_detail.html',
                      {'s_brand_name': s_brand_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       'dict_source_type': dict_source_type, 
                       'dict_search_rst_type': dict_search_rst_type,
                       'dict_medium_type': dict_medium_type,
                       'dict_alias_info': dict_alias_info,
                       })


class Viral(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'
    __g_lstPeriod = ['ly', 'lm', 'tm']

    def __init__(self):
        self.__g_oSvDb = SvMySql()
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')
        self.__g_dictPeriod = {}
        self.__g_dictSamplingFreqMode = {}
        self.__g_sBrandName = None
        self.__g_lstOwnedBrand = []
        return

    def __del__(self):
        if self.__g_oSvDb:
            del self.__g_oSvDb
        if self.__g_dictPeriod:
            del self.__g_dictPeriod
        if self.__g_sBrandName:
            del self.__g_sBrandName
        if self.__g_lstOwnedBrand:
            del self.__g_lstOwnedBrand

    def get(self, request, *args, **kwargs):
        o_window = PeriodWindow()
        self.__g_dictPeriod = o_window.get_period_range(request)
        self.__g_dictSamplingFreqMode = o_window.get_sampling_freq_ui()  # sampling freq btn UI
        del o_window

        dict_rst = get_brand_info(self.__g_oSvDb, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)
        self.__g_sBrandName = dict_rst['dict_ret']['s_brand_name']
        self.__g_lstOwnedBrand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation

        if 'sv_source_name' in kwargs:
            return self.__viral_source_media_kw_level(request, *args, **kwargs)
        else:
            return self.__viral_source_level(request)

    def __viral_source_media_kw_level(self, request, *args, **kwargs):
        # begin - search API result
        o_search_api_freq_trend = SearchApiFreqTrendVisual(self.__g_oSvDb)
        o_search_api_freq_trend.set_period_dict(self.__g_dictPeriod, self.__g_lstPeriod)
        o_search_api_freq_trend.load_df([kwargs['sv_source_name']])
        dict_plots = {}  # graph dict to draw
        
        if kwargs['sv_source_name'] == 'naver':
            dict_stacked_bar = o_search_api_freq_trend.retrieve_source_media_kw_level_sb('lm')
            for s_media, dict_kw_daily_freq in dict_stacked_bar['data_body'].items():
                o_graph = Visualizer()
                o_graph.set_title(s_media + '의 키워드별 일별 바이럴 발생 빈도')
                o_graph.set_height(170)
                o_graph.set_x_labels(dict_stacked_bar['lst_x_label'])
                lst_palette_tmp = dict_stacked_bar['lst_palette'].copy()
                for s_morpheme, lst_series_val in dict_kw_daily_freq.items():
                    s_series_color = lst_palette_tmp.pop(0)
                    o_graph.append_series(s_morpheme, s_series_color, lst_series_val)
                dict_plots[s_media] = o_graph.draw_vertical_bar(n_max_y_axis=dict_stacked_bar['n_final_axis_max'])
                del o_graph
            s_template_name = 'viral_source_media_kw_level_nvr'
        else:
            o_graph = Visualizer()
            dict_stacked_bar = o_search_api_freq_trend.retrieve_source_kw_level_sb('lm')
            o_graph.set_title('소스별 키워드별 일별 바이럴 발생 빈도')
            o_graph.set_height(170)
            o_graph.set_x_labels(dict_stacked_bar['lst_x_label'])
            for s_morpheme, lst_series_val in dict_stacked_bar['data_body'].items():
                    s_series_color = dict_stacked_bar['lst_palette'].pop(0)
                    o_graph.append_series(s_morpheme, s_series_color, lst_series_val)
            dict_plots['viral_daily'] = o_graph.draw_vertical_bar(n_max_y_axis=dict_stacked_bar['y_max_val'])
            s_template_name = 'viral_source_media_kw_level'
            del o_graph
        del o_search_api_freq_trend
        # end - search API result
        
        script, div = components(dict(dict_plots))
        return render(request, 'svload/'+s_template_name+'.html', { 
                          'script': script, 'div': div,
                          'dict_sampling_freq_mode': self.__g_dictSamplingFreqMode,
                          's_cur_period_window': self.__g_dictPeriod['s_cur_period_window'],
                          'dict_period_date': {'from': self.__g_dictPeriod['dt_first_day_this_month'].strftime("%Y%m%d"),
                                               'to': self.__g_dictPeriod['dt_today'].strftime("%Y%m%d")},
                          's_bokeh_version': bokeh_version.get_versions()['version'],
                          's_brand_name': self.__g_sBrandName,
                          'lst_owned_brand': self.__g_lstOwnedBrand,  # for global navigation
                          's_source_lbl': kwargs['sv_source_name']
                      })
    
    def __viral_source_level(self, request):
        """ viral main UX """
        # begin - search API result
        o_search_api_freq_trend = SearchApiFreqTrendVisual(self.__g_oSvDb)
        o_search_api_freq_trend.set_period_dict(self.__g_dictPeriod, self.__g_lstPeriod)
        o_search_api_freq_trend.load_df()
        dict_plots = {}  # graph dict to draw
        o_graph = Visualizer()
        # dict_multi_line = o_search_api_freq_trend.retrieve_daily_chronicle_by_source_ml('lm')
        # del o_search_api_freq_trend
        # o_graph.set_height(170)
        # o_graph.set_x_labels(dict_multi_line['lst_x_label'])
        # n_source_cnt = len(dict_multi_line['lst_line_label'])
        # n_gross_freq = 0
        # for n_idx in range(0, n_source_cnt):
        #     o_graph.append_series(dict_multi_line['lst_line_label'][n_idx],
        #                           dict_multi_line['lst_line_color'][n_idx],
        #                           dict_multi_line['lst_series_cnt'][n_idx])
        #     n_gross_freq = n_gross_freq + sum(dict_multi_line['lst_series_cnt'][n_idx])
        # o_graph.set_title('총 ' + "{:,}".format(n_gross_freq) + '회의 바이럴 발생함')
        # dict_plots['morpheme_daily'] = o_graph.draw_multi_line()

        dict_stacked_bar = o_search_api_freq_trend.retrieve_source_level_sb('lm')
        del o_search_api_freq_trend
        
        lst_effectuve_source = []
        o_graph.set_title('소스별 일별 바이럴 발생 빈도')
        o_graph.set_height(170)
        o_graph.set_x_labels(dict_stacked_bar['lst_x_label'])
        for s_source_name, lst_series_val in dict_stacked_bar['data_body'].items():
                s_series_color = dict_stacked_bar['lst_palette'].pop(0)
                o_graph.append_series(s_source_name, s_series_color, lst_series_val)
                lst_effectuve_source.append(s_source_name)
        dict_plots['viral_daily'] = o_graph.draw_vertical_bar(n_max_y_axis=dict_stacked_bar['y_max_val'])
        del o_graph
        # end - search API result
        script, div = components(dict(dict_plots))
        return render(request, 'svload/viral_main.html', { 
                          'script': script, 'div': div,
                          'dict_sampling_freq_mode': self.__g_dictSamplingFreqMode,
                          's_cur_period_window': self.__g_dictPeriod['s_cur_period_window'],
                          'dict_period_date': {'from': self.__g_dictPeriod['dt_first_day_this_month'].strftime("%Y%m%d"),
                                               'to': self.__g_dictPeriod['dt_today'].strftime("%Y%m%d")},
                          's_bokeh_version': bokeh_version.get_versions()['version'],
                          's_brand_name': self.__g_sBrandName,
                          'lst_owned_brand': self.__g_lstOwnedBrand,  # for global navigation
                          'lst_effectuve_source': lst_effectuve_source
                      })


class Morpheme(LoginRequiredMixin, TemplateView):
    # http://192.168.0.24:8002/load/morpheme/1/
    # template_name = 'analyze/index.html'
    __g_nCntToVisitorNounRank = 100  # 추출할 순위 수
    # __g_nCntToInboundKeywordRank = 10  # 추출할 순위 수
    # __g_nCntToSourceMediumRank = 10  # 추출할 순위 수

    def __init__(self):
        return

    def get(self, request, *args, **kwargs):
        o_window = PeriodWindow()
        dict_period = o_window.get_period_range(request)
        dict_sampling_freq_mode = o_window.get_sampling_freq_ui()  # sampling freq btn UI
        del o_window

        o_sv_db = SvMySql()
        if not o_sv_db:
            raise Exception('invalid db handler')

        dict_rst = get_brand_info(o_sv_db, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)
        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation

        # begin - in-site viral result
        from .pandas_plugins.word_cloud import WcMainVisual
        o_word_cloud = WcMainVisual(o_sv_db)
        lst_period = ['ly', 'lm', 'tm']
        o_word_cloud.set_period_dict(dict_period, lst_period)
        o_word_cloud.load_df()

        dict_config = {'n_brand_id': dict_rst['dict_ret']['n_brand_id'],
                       's_static_file_path': settings.STATICFILES_DIRS[0],
                       's_media_file_path': settings.MEDIA_ROOT,
                       's_media_url_root': settings.MEDIA_URL,
                       'lst_period': lst_period, 'n_th_rank': self.__g_nCntToVisitorNounRank}
        dict_wc_rst = o_word_cloud.get_top_ranker(dict_config)
        del o_word_cloud, dict_config
        # end - in-site viral result

        return render(request, 'svload/morpheme.html', { 
                          'dict_sampling_freq_mode': dict_sampling_freq_mode,
                          's_cur_period_window': dict_period['s_cur_period_window'],
                          'dict_period_date': {'from': dict_period['dt_first_day_this_month'].strftime("%Y%m%d"),
                                               'to': dict_period['dt_today'].strftime("%Y%m%d")},
                          's_bokeh_version': bokeh_version.get_versions()['version'],
                          's_brand_name': s_brand_name,
                          'lst_owned_brand': lst_owned_brand,  # for global navigation
                          'lst_top_word_by_freq_trend': dict_wc_rst['lst_top_word_by_freq_trend'],
                          'visitor_noun_n_th_rank': self.__g_nCntToVisitorNounRank,
                          'dict_misc_word_cnt': dict_wc_rst['dict_misc_word_cnt'],
                          'dict_word_cloud_img_url': dict_wc_rst['dict_word_cloud_img_url']
                      })

    def post(self, request, *args, **kwargs):
        o_sv_db = SvMySql()
        if not o_sv_db:
            raise Exception('invalid db handler')

        dict_rst = get_brand_info(o_sv_db, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation

        s_act = request.POST.get('act')
        s_return_url = request.META.get('HTTP_REFERER')
        if s_act == 'search_morpheme':
            s_morpheme_query = request.POST.get('morpheme_query')
            from .pandas_plugins.word_cloud import MorphemeVisual
            o_morpheme = MorphemeVisual(o_sv_db)
            dict_rst = o_morpheme.get_morpheme_id_by_morpheme(s_morpheme_query)
            del o_morpheme
            if dict_rst['b_error']:
                dict_context = {'err_msg': dict_rst['s_msg'], 's_return_url': s_return_url}
                return render(request, "svload/deny.html", context=dict_context)

            return render(request, "svload/morpheme.html",
                          context={  # 'dict_sampling_freq_mode': dict_sampling_freq_mode,
                              's_bokeh_version': bokeh_version.get_versions()['version'],
                              's_brand_name': s_brand_name,
                              'lst_owned_brand': lst_owned_brand,  # for global navigation
                              's_morpheme_query': s_morpheme_query,
                              'lst_relevant_morpheme': dict_rst['lst_morpheme'],
                          })
        del o_sv_db


class MorphemeChronicle(LoginRequiredMixin, TemplateView):
    # template_name = 'analyze/index.html'

    def __init__(self):
        return

    def get(self, request, *args, **kwargs):
        o_window = PeriodWindow()
        dict_period = o_window.get_period_range(request)
        dict_sampling_freq_mode = o_window.get_sampling_freq_ui()  # sampling freq btn UI
        del o_window

        o_sv_db = SvMySql()
        if not o_sv_db:
            raise Exception('invalid db handler')

        dict_rst = get_brand_info(o_sv_db, request, kwargs)
        if dict_rst['b_error']:
            dict_context = {'err_msg': dict_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)
        s_brand_name = dict_rst['dict_ret']['s_brand_name']
        lst_owned_brand = dict_rst['dict_ret']['lst_owned_brand']  # for global navigation

        # if 'morpheme_id' in kwargs.keys():  # 7,21,758
        from .pandas_plugins.word_cloud import MorphemeVisual
        o_morpheme_chronicle = MorphemeVisual(o_sv_db)
        lst_period = ['2ly', 'ly', 'lm', 'tm']

        dict_url_rst = {'b_error': False, 's_msg': None}
        if request.method == 'GET' and 'morpheme_id' in request.GET:
            s_morpheme_ids = request.GET['morpheme_id']
            lst_morpheme_id = s_morpheme_ids.split(',')
            for x in lst_morpheme_id:
                if not x.isdigit():
                    dict_url_rst['b_error'] = True
                    dict_url_rst['s_msg'] = 'invalid morpheme id'
                    break
        else:
            dict_url_rst['b_error'] = True
            dict_url_rst['s_msg'] = 'invalid approach'

        if dict_url_rst['b_error']:
            dict_context = {'err_msg': dict_url_rst['s_msg']}
            return render(request, "svload/analyze_deny.html", context=dict_context)

        lst_morpheme_id = [int(x.strip()) for x in lst_morpheme_id]  # ?morpheme_id=7,21,758

        # begin -
        lst_item_line_color = ['#D6E2DF', '#A4C8C1', '#6CBDAC', '#079476', '#960614', '#6b0000', '#205a86', '#140696',
                               '#4d6165', '#798984', '#8db670', '#ffad60']  # last one is the largest
        n_max_morpheme_cnt = len(lst_item_line_color)
        o_morpheme_chronicle.set_period_dict(dict_period, lst_period)
        o_morpheme_chronicle.set_freq(dict_sampling_freq_mode)
        o_morpheme_chronicle.set_morpheme_lst(lst_morpheme_id[:n_max_morpheme_cnt])
        o_morpheme_chronicle.load_df()

        dict_plots = {}  # graph dict to draw
        dict_4_multi_line = o_morpheme_chronicle.retrieve_daily_chronicle_by_morpheme_ml(lst_item_line_color)
        del o_morpheme_chronicle
        
        o_graph = Visualizer()
        o_graph.set_height(170)
        o_graph.set_x_labels(dict_4_multi_line['lst_x_label'])
        n_morpheme_cnt = len(dict_4_multi_line['lst_line_label'])
        n_gross_freq = 0
        for n_idx in range(0, n_morpheme_cnt):
            o_graph.append_series(dict_4_multi_line['lst_line_label'][n_idx],
                                  dict_4_multi_line['lst_line_color'][n_idx],
                                  dict_4_multi_line['lst_series_cnt'][n_idx])
            n_gross_freq = n_gross_freq + sum(dict_4_multi_line['lst_series_cnt'][n_idx])
        o_graph.set_title('선택 단어는 총 ' + "{:,}".format(n_gross_freq) + '회 발생함')

        dict_plots['morpheme_daily'] = o_graph.draw_multi_line()
        del o_graph
        # end -
        script, div = components(dict(dict_plots))
        return render(request, 'svload/morpheme_chronicle.html',
                      {'script': script, 'div': div,
                       'dict_sampling_freq_mode': dict_sampling_freq_mode,
                       's_bokeh_version': bokeh_version.get_versions()['version'],
                       's_brand_name': s_brand_name,
                       'lst_owned_brand': lst_owned_brand,  # for global navigation
                       })


def get_brand_info(o_db, request, kwargs):
    dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
    dict_owned_brand = get_owned_brand_list(request, kwargs)
    n_acct_id = None
    n_brand_id = None
    s_brand_name = None
    lst_owned_brand = []
    for _, dict_single_acct in dict_owned_brand.items():
        lst_owned_brand += dict_single_acct['lst_brand']
        for dict_single_brand in dict_single_acct['lst_brand']:
            if dict_single_brand['b_current_brand']:
                n_acct_id = dict_single_acct['n_acct_id']
                n_brand_id = dict_single_brand['n_brand_id']
                s_brand_name = dict_single_brand['s_brand_ttl']
                break
    
    s_tbl_prefix = str(n_acct_id) + '_' + str(n_brand_id)
    if not s_tbl_prefix or not n_acct_id or not n_brand_id:
        dict_rst['b_error'] = True
        dict_rst['s_msg'] = 'not allowed brand'
        return dict_rst

    o_db.set_tbl_prefix(s_tbl_prefix)
    o_db.set_app_name(__name__)
    o_db.initialize({'n_acct_id': n_acct_id, 'n_brand_id': n_brand_id})
    # o_db.set_reserved_tag_value({'brand_id': n_brand_id})

    dict_rst['s_msg'] = 'success'
    dict_rst['dict_ret'] = {'n_acct_id': n_acct_id, 'n_brand_id': n_brand_id,
                            # 'n_owner_id': request.user.pk, 'n_ga_view_id': n_ga_view_id,
                            's_brand_name': s_brand_name, 'lst_owned_brand': lst_owned_brand}
    return dict_rst
