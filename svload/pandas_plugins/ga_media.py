from abc import ABC
from abc import abstractmethod
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from svcommon.sv_campaign_parser import SvCampaignParser
from .sv_palette import SvPalette


# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class GaMediaVisual(ABC):
    """
    https://hyunlee103.tistory.com/91  추상성 다형성
    """
    _g_oSvDb = None
    _g_dictPeriod = {'lm': [], 'tm': []}  #
    _g_dictSamplingFreq = {'qtr': 'Q', 'mon': 'M', 'day': 'D'}
    _g_sFreqMode = 'D'
    _g_dictPeriodRaw = {'lm': None, 'tm': None}
    _g_dictColumnMaxValue = {'lm': None, 'tm': None}
    _g_dictPsSourceMediumDaily = {'lm': None, 'tm': None}
    _g_lstRequestedAttrs = None
    _g_lstAllowableNumericalAttrs = ['media_imp', 'media_click', 'in_site_tot_session', 'in_site_tot_bounce',
                                     'in_site_tot_duration_sec', 'in_site_tot_pvs', 'in_site_tot_new',
                                     'media_gross_cost_inc_vat']
    _g_lstRetrievableNumericalAttrs = ['media_imp', 'media_click', 'in_site_tot_session', 'in_site_tot_bounce',
                                       'in_site_tot_new', 'media_gross_cost_inc_vat', 'avg_dur_sec', 'avg_pvs',
                                       'gross_cpc_inc_vat', 'gross_ctr', 'effective_session']
    _g_lstIgnoreTerms = ['(notset)', '(NOTSET)', '(not set)', '(notprovided)', '|@|sv', 'gdn']
    _g_dtMtdElapsedDays = 0  # 당기 기준 MTD 구하기 위한 경과 일수
    _g_dictPalletSourceMedium = None  # for source_medium line color

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        if not o_sv_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self._g_oSvDb = o_sv_db

        o_sv_pallet = SvPalette()
        self._g_dictPalletSourceMedium = o_sv_pallet.get_source_medium_color_dict()
        del o_sv_pallet
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # for s_period in list(self._g_dictPeriod.keys()):
        for s_period in self._g_dictPeriod:
            del self._g_dictPeriodRaw[s_period]
        pass

    @abstractmethod
    def load_df(self, lst_retrieve_attrs):
        pass

    @abstractmethod
    def get_graph_data(self):  # abstract
        pass

    def set_period_dict(self, dict_period, lst_switch=None):
        """
        dtDesignatedFirstDate = self._g_dictPeriod['2ly'][0]
        dtDesignatedLastDate = self._g_dictPeriod['tm'][1]
        :param dict_period:
        :param lst_switch:
        :return:
        """
        if not dict_period['dt_first_day_year_ago'] or not dict_period['dt_today']:
            raise Exception('invalid data period')
        if lst_switch is None:
            raise Exception('invalid period switch')

        dict_period_switch = {
            'tm': ['dt_first_day_this_month', 'dt_today'],
            'lm': ['dt_first_day_month_ago', 'dt_last_day_month_ago'],
            'ly': ['dt_first_day_year_ago', 'dt_last_day_year_ago'],
            '2ly': ['dt_first_day_2year_ago', 'dt_last_day_2year_ago'],
        }
        for s_period_window in lst_switch:
            self._g_dictPeriod[s_period_window] = \
                [dict_period[dict_period_switch[s_period_window][0]],
                 dict_period[dict_period_switch[s_period_window][1]]]
    
        del dict_period_switch
        self._g_dtMtdElapsedDays = dict_period['dt_today'] - dict_period['dt_first_day_this_month']

    def set_freq(self, dict_freq):  # 일치
        for s_freq, b_activated in dict_freq.items():
            if b_activated:
                self._g_sFreqMode = self._g_dictSamplingFreq[s_freq]
                break
        self._g_sFreqMode = self._g_dictSamplingFreq['day']  ###################### for daily debug  

    def set_request_attr_list(self, lst_attr_title):  # 일치
        if len(lst_attr_title) == 0:
            raise Exception('invalid lst_attr_title')
        self._g_lstRequestedAttrs = lst_attr_title

    def load_source_medium_ps_only(self, s_period_window, s_sort_column='in_site_tot_session'):
        """
        get daily trend by media_source for top n_th
        :param s_period_window:
        :param s_sort_column:
        :return:
        """
        dict_top_source_medium, n_other_source_cnt = \
            self._retrieve_gross_source_medium(s_period_window=s_period_window, s_sort_column=s_sort_column,
                                               n_th_rank=10)
        # get full month date range for JS sparkline
        idx_full_day = self._g_dictPeriodRaw[s_period_window].get_full_period_idx()
        f_full_period_est_factor = self._g_dictPeriodRaw[s_period_window].get_full_period_est_factor()
        # get budget index
        dict_budget_info = self._g_dictPeriodRaw[s_period_window].get_budget_info()
        n_gross_budget_inc_vat = 0
        dict_maximum = {'media_gross_cost_inc_vat': 0, 'media_imp': 0, 'media_click': 0, 'gross_cpc_inc_vat': 0,
                        'gross_ctr': 0, 'in_site_tot_session': 0, 'in_site_tot_new': 0, 'in_site_tot_bounce': 0,
                        'avg_dur_sec': 0, 'avg_pvs': 0}
        dict_ps_source_medium_daily = {}
        dict_ps_source_medium_gross = {}
        lst_filter = [{'col_ttl': 'media_source', 'fil_val': None}, {'col_ttl': 'media_media', 'fil_val': None}]
        for s_source_medium, dict_row in dict_top_source_medium.items():
            if dict_row['media_gross_cost_inc_vat'] > 0:
                lst_info = s_source_medium.split('_')
                lst_filter[0]['fil_val'] = lst_info[0]
                lst_filter[1]['fil_val'] = lst_info[1]
                df_by_source_medium = self._g_dictPeriodRaw[s_period_window].get_gross_filtered_by_index(lst_filter)
                # begin - collect derivative max value to determine y-axis range
                dict_maximum['media_gross_cost_inc_vat'] = dict_maximum['media_gross_cost_inc_vat'] + \
                    df_by_source_medium['media_gross_cost_inc_vat'].max()
                dict_maximum['media_imp'] = dict_maximum['media_imp'] + df_by_source_medium['media_imp'].max()
                dict_maximum['media_click'] = dict_maximum['media_click'] + df_by_source_medium['media_click'].max()
                dict_maximum['gross_cpc_inc_vat'] = dict_maximum['gross_cpc_inc_vat'] + \
                    df_by_source_medium['gross_cpc_inc_vat'].max()
                dict_maximum['gross_ctr'] = dict_maximum['gross_ctr'] + df_by_source_medium['gross_ctr'].max()
                dict_maximum['in_site_tot_session'] = dict_maximum['in_site_tot_session'] + \
                    df_by_source_medium['in_site_tot_session'].max()
                dict_maximum['in_site_tot_new'] = dict_maximum['in_site_tot_new'] + \
                    df_by_source_medium['in_site_tot_new'].max()
                dict_maximum['in_site_tot_bounce'] = dict_maximum['in_site_tot_bounce'] + \
                    df_by_source_medium['in_site_tot_bounce'].max()
                dict_maximum['avg_dur_sec'] = dict_maximum['avg_dur_sec'] + df_by_source_medium['avg_dur_sec'].max()
                dict_maximum['avg_pvs'] = dict_maximum['avg_pvs'] + df_by_source_medium['avg_pvs'].max()
                # end - collect derivative max value to determine y-axis range

                # make mtd column sum for progress bar
                dict_ttl_value_temp = {
                    'media_gross_cost_inc_vat': df_by_source_medium['media_gross_cost_inc_vat'].sum(),
                    'media_gross_cost_inc_vat_ttl': float(0.0),
                    'media_imp': df_by_source_medium['media_imp'].sum(),
                    'media_imp_ttl': 0.0,
                    'media_click': df_by_source_medium['media_click'].sum(),
                    'media_click_ttl': 0.0,
                    'gross_cpc_inc_vat': 0.0,
                    # 'gross_cpc_inc_vat_ttl': 0.0,  # same with ttl est.
                    'gross_ctr': 0.0,
                    # 'gross_ctr_ttl': float(0.0),  # same with ttl est.
                    'in_site_tot_session': df_by_source_medium['in_site_tot_session'].sum(),
                    'in_site_tot_session_ttl': 0.0,
                    'in_site_tot_new': df_by_source_medium['in_site_tot_new'].sum(),
                    'in_site_tot_new_ttl': 0.0,
                    'in_site_tot_bounce': df_by_source_medium['in_site_tot_bounce'].sum(),
                    'in_site_tot_bounce_ttl': 0.0,
                    'in_site_tot_pvs': df_by_source_medium['in_site_tot_pvs'].sum(),
                    'in_site_tot_pvs_ttl': 0.0,
                    'in_site_tot_duration_sec': df_by_source_medium['in_site_tot_duration_sec'].sum(),
                    'in_site_tot_duration_sec_ttl': 0.0,
                    'avg_dur_sec': 0.0,
                    # 'avg_dur_sec_ttl': 0.0,  # same with ttl est.
                    'avg_pvs': 0.0,
                    # 'avg_pvs_ttl': 0.0  # same with ttl est.
                }

                # get 1st degree index
                f_budget_period_est_factor = dict_budget_info[s_source_medium]['f_est_factor']
                dict_ttl_value_temp['media_gross_cost_inc_vat_ttl'] = \
                    dict_ttl_value_temp['media_gross_cost_inc_vat'] * f_budget_period_est_factor
                dict_ttl_value_temp['media_imp_ttl'] = dict_ttl_value_temp['media_imp'] * f_full_period_est_factor
                dict_ttl_value_temp['media_click_ttl'] = dict_ttl_value_temp['media_click'] * f_full_period_est_factor
                dict_ttl_value_temp['in_site_tot_session_ttl'] = \
                    dict_ttl_value_temp['in_site_tot_session'] * f_full_period_est_factor
                dict_ttl_value_temp['in_site_tot_new_ttl'] = \
                    dict_ttl_value_temp['in_site_tot_new'] * f_full_period_est_factor
                dict_ttl_value_temp['in_site_tot_bounce_ttl'] = \
                    dict_ttl_value_temp['in_site_tot_bounce'] * f_full_period_est_factor
                dict_ttl_value_temp['in_site_tot_duration_sec_ttl'] =\
                    dict_ttl_value_temp['in_site_tot_duration_sec'] * f_full_period_est_factor
                dict_ttl_value_temp['in_site_tot_pvs_ttl'] = \
                    dict_ttl_value_temp['in_site_tot_pvs'] * f_full_period_est_factor
                # get derivative index
                if dict_ttl_value_temp['media_click'] > 0:
                    dict_ttl_value_temp['gross_cpc_inc_vat'] = \
                        dict_ttl_value_temp['media_gross_cost_inc_vat'] / dict_ttl_value_temp['media_click']
                else:
                    dict_ttl_value_temp['gross_cpc_inc_vat'] = 0

                if dict_ttl_value_temp['media_click'] > 0 and dict_ttl_value_temp['media_imp'] > 0:
                    dict_ttl_value_temp['gross_ctr'] = \
                        dict_ttl_value_temp['media_click'] / dict_ttl_value_temp['media_imp'] * 100
                else:
                    dict_ttl_value_temp['gross_ctr'] = 0

                if dict_ttl_value_temp['in_site_tot_duration_sec'] > 0 and \
                        dict_ttl_value_temp['in_site_tot_session'] > 0:
                    dict_ttl_value_temp['avg_dur_sec'] = \
                        dict_ttl_value_temp['in_site_tot_duration_sec'] / dict_ttl_value_temp['in_site_tot_session']
                else:
                    dict_ttl_value_temp['avg_dur_sec'] = 0

                if dict_ttl_value_temp['in_site_tot_pvs'] > 0 and dict_ttl_value_temp['in_site_tot_session'] > 0:
                    dict_ttl_value_temp['avg_pvs'] = \
                        dict_ttl_value_temp['in_site_tot_pvs'] / dict_ttl_value_temp['in_site_tot_session']
                else:
                    dict_ttl_value_temp['avg_pvs'] = 0

                # o_keys = dict_budget_info[s_source_medium].keys()
                dict_keys = dict_budget_info[s_source_medium]
                if 'n_budget_tgt_amnt_inc_vat' in dict_keys and 'n_agency_fee_inc_vat' in dict_keys and \
                        'n_agency_fee_inc_vat_est' in dict_keys:
                    n_budget_tgt_amnt_inc_vat = dict_budget_info[s_source_medium]['n_budget_tgt_amnt_inc_vat']
                    n_agency_fee_inc_vat = dict_budget_info[s_source_medium]['n_agency_fee_inc_vat']
                    n_agency_fee_inc_vat_est = dict_budget_info[s_source_medium]['n_agency_fee_inc_vat_est']
                    n_gross_budget_inc_vat += n_budget_tgt_amnt_inc_vat
                else:
                    n_budget_tgt_amnt_inc_vat = 0
                    n_agency_fee_inc_vat = 0
                    n_agency_fee_inc_vat_est = 0
                # del o_keys
                del dict_keys
                dict_ttl_value_temp['s_media_agency_name'] = dict_budget_info[s_source_medium]['s_media_agency_name']
                dict_ttl_value_temp['n_media_agency_id'] = dict_budget_info[s_source_medium]['n_media_agency_id']
                dict_ttl_value_temp['n_budget_tgt_amnt_inc_vat'] = n_budget_tgt_amnt_inc_vat
                dict_ttl_value_temp['n_agency_fee_inc_vat'] = n_agency_fee_inc_vat
                dict_ttl_value_temp['n_agency_fee_inc_vat_est'] = n_agency_fee_inc_vat_est

                # make gross and est for js bar graph library
                dict_ps_source_medium_gross[s_source_medium] = dict_ttl_value_temp

                # begin - add a campaign level info if exists
                if dict_budget_info[s_source_medium]['b_campaign_level']:
                    dict_ps_source_medium_gross[s_source_medium]['b_campaign_level'] = True
                    dict_ps_source_medium_gross[s_source_medium]['dict_campaign'] = {}
                    for s_camp_name, dict_camp_budget in dict_budget_info[s_source_medium]['dict_campaign'].items():
                        dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][s_camp_name] =\
                            self._g_dictPeriodRaw[s_period_window].get_sv_campaign_info(s_camp_name)
                        dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][s_camp_name]['s_media_agency_name'] = \
                            dict_camp_budget['s_media_agency_name']
                        dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][s_camp_name]['n_media_agency_id'] = \
                            dict_camp_budget['n_media_agency_id']
                        dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][s_camp_name]['n_budget_tgt_amnt_inc_vat'] = \
                            dict_camp_budget['n_budget_tgt_amnt_inc_vat']
                        dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][s_camp_name]['campaign_gross_cost_inc_vat_ttl'] = \
                            dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][s_camp_name]['campaign_gross_cost_inc_vat'] * \
                            dict_camp_budget['f_est_factor']
                        dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][s_camp_name]['n_agency_fee_inc_vat'] = \
                            dict_camp_budget['n_agency_fee_inc_vat']
                        dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][s_camp_name]['n_agency_fee_inc_vat_est'] = \
                            dict_camp_budget['n_agency_fee_inc_vat'] * dict_camp_budget['f_est_factor']
                else:
                    dict_ps_source_medium_gross[s_source_medium]['b_campaign_level'] = False
                # end - add a campaign level info if exists
                # make full month for bokeh
                del df_by_source_medium['in_site_tot_duration_sec']
                del df_by_source_medium['in_site_tot_pvs']
                df_by_source_medium = df_by_source_medium.reindex(idx_full_day, fill_value=0)
                dict_ps_source_medium_daily[s_source_medium] = df_by_source_medium
        for s_sm, dict_single in dict_budget_info.items():
            if s_sm.endswith('organic'):
                dict_ps_source_medium_gross[s_sm] = {
                    's_media_agency_name': dict_single['s_media_agency_name'],
                    'n_media_agency_id': dict_single['n_media_agency_id'],
                    'n_budget_tgt_amnt_inc_vat': dict_single['n_budget_tgt_amnt_inc_vat']
                }
        # specify derivative max value sum to determine y-axis range
        dict_col_gross_max_value = {'media_gross_cost_inc_vat': dict_maximum['media_gross_cost_inc_vat'],
                                    'media_imp': dict_maximum['media_imp'],
                                    'media_click': dict_maximum['media_click'],
                                    'gross_cpc_inc_vat': dict_maximum['gross_cpc_inc_vat'],
                                    'gross_ctr': dict_maximum['gross_ctr'],
                                    'in_site_tot_session': dict_maximum['in_site_tot_session'],
                                    'in_site_tot_new': dict_maximum['in_site_tot_new'],
                                    'in_site_tot_bounce': dict_maximum['in_site_tot_bounce'],
                                    'avg_dur_sec': dict_maximum['avg_dur_sec'],
                                    'avg_pvs': dict_maximum['avg_pvs']}
        del dict_maximum
        self._g_dictColumnMaxValue[s_period_window] = dict_col_gross_max_value
        self._g_dictPsSourceMediumDaily[s_period_window] = dict_ps_source_medium_daily
        return {'dict_ps_source_medium_gross': dict_ps_source_medium_gross,
                'n_gross_budget_inc_vat': n_gross_budget_inc_vat}

    def _retrieve_x_lbl(self, s_period_window):
        """
        get x-axis list
        :return:
        """
        lst_x_axis_label = []
        if len(self._g_dictPsSourceMediumDaily[s_period_window]) == 0:
            pass
        else:
            s_source_medium, df_first = next(iter(self._g_dictPsSourceMediumDaily[s_period_window].items()))
            for n_idx in range(1, len(df_first.index.to_list()) + 1):
                lst_x_axis_label.append('{:02d}'.format(n_idx))
        return lst_x_axis_label

    def _get_source_medium_info(self, s_period_window, s_col_title):
        dict_source_medium = {}
        lst_palette = []
        for s_source_medium, df_row in self._g_dictPsSourceMediumDaily[s_period_window].items():
            dict_source_medium[s_source_medium] = df_row[s_col_title].to_list()
            # try:
            if self._g_dictPalletSourceMedium.get(s_source_medium, 0):
                lst_palette.append(self._g_dictPalletSourceMedium[s_source_medium])
            # except KeyError:
            else:
                lst_palette.append(self._g_dictPalletSourceMedium['default'])

        return {'dict_source_medium': dict_source_medium, 'lst_palette': lst_palette,
                'column_max_value': self._g_dictColumnMaxValue[s_period_window][s_col_title]}

    def _retrieve_gross_source_medium(self, s_period_window, s_sort_column, n_th_rank=5):
        """
        get summary by source; this could be called by view class directly or called by to retrieve daily index by source
        :param s_period_window:
        :param s_sort_column:
        :param n_th_rank:
        :return:
        """
        if not s_sort_column:
            raise Exception('invalid sort column')
        df_by_source = self._g_dictPeriodRaw[s_period_window].get_gross_group_by_index(['media_source', 'media_media'])

        # convert df to dict to draw table
        dict_top_source = {}
        df = df_by_source.sort_values([s_sort_column], ascending=False).head(n_th_rank)
        for i, df_row in df.iterrows():
            dict_top_source['_'.join(df_row.name)] = df_row.to_dict()
        return dict_top_source, len(df_by_source.index) - n_th_rank


# pd.set_option('display.max_columns', None) 후에 print(df)하면 모든 컬럼명이 출력됨
class GmMainVisual(GaMediaVisual):
    """
    match with ./templates/svload/ga_media_main.html
    """

    def load_df(self, lst_retrieve_attrs):
        # for s_period in list(self._g_dictPeriod.keys()):
        for s_period in self._g_dictPeriod:
            dt_first_date = self._g_dictPeriod[s_period][0]
            dt_last_date = self._g_dictPeriod[s_period][1]
            o_ga_media_raw = GaSourceMediaRaw(self._g_oSvDb)
            o_ga_media_raw.set_period_dict(dt_start=dt_first_date, dt_end=dt_last_date)
            o_ga_media_raw.set_freq(self._g_sFreqMode)
            self._g_dictPeriodRaw[s_period] = o_ga_media_raw
            self._g_dictPeriodRaw[s_period].load_period()
            self._g_dictPeriodRaw[s_period].compress_data_by_freq_mode()
            self._g_dictPeriodRaw[s_period].aggregate_data(self._g_dtMtdElapsedDays)

    def get_graph_data(self):
        """
        get daily trend by media_source for top n_th
        :return:
        """
        # retrieve x-lbl
        lst_x_lbl_tm = self._retrieve_x_lbl('tm')

        # 당월 VAT 포함 총 비용(Media)
        dict_rst_tm = self._get_source_medium_info('tm', 'media_gross_cost_inc_vat')
        n_max_y = dict_rst_tm['column_max_value']
        # lst define: 'graph_id', 'data_body', 'series label', 'y max val', 'lst_palette', 'graph_title', 'graph_height'
        lst_graph_to_draw = [
            ['media_gross_cost_inc_vat_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
             n_max_y, dict_rst_tm['lst_palette'], '당기 VAT 포함 총 비용(Media)', 210],
        ]
        return lst_graph_to_draw

    def retrieve_raw_lst_by_column(self, s_period_window, s_column_title):
        """
        provide series values for bokeh
        :param s_period_window:
        :param s_column_title:
        :return:
        """
        # 'DataFrame' object has no attribute 'tolist' could be occured if DF has single column
        # use .squeeze() to avoid the exception
        # if s_period_window in list(self._g_dictPeriod.keys()):
        if s_period_window in self._g_dictPeriod:
            return self._g_dictPeriodRaw[s_period_window].get_period_numeric_by_index(s_column_title).tolist()

    def retrieve_longest_x_lbl(self):
        """
        get period of analyzing window
        :return:
        """
        n_longest_days = 0
        # for s_period_window in list(self._g_dictPeriod.keys()):
        for s_period_window in self._g_dictPeriod:
            idx_tmp = self._g_dictPeriodRaw[s_period_window].get_period_numeric_by_index('in_site_tot_session').index
            n_idx = len(idx_tmp)
            if n_longest_days < n_idx:
                n_longest_days = n_idx
        del idx_tmp
        lst_x_axis_label = []
        for n_idx in range(1, n_longest_days + 1):
            lst_x_axis_label.append(n_idx)
        return lst_x_axis_label

    def retrieve_mtd_by_column(self, s_period_window):
        dict_rst = {}
        # if s_period_window in list(self._g_dictPeriod.keys()):
        if s_period_window in self._g_dictPeriod:
            dict_rst = self._g_dictPeriodRaw[s_period_window].get_gross_numeric_by_index_mtd()
        else:
            for s_retrieve_attrs in self._g_lstRetrievableNumericalAttrs:
                dict_rst[s_retrieve_attrs] = 0
        return dict_rst

    def retrieve_full_by_column(self, s_period_window):
        dict_rst = {}
        # if s_period_window in list(self._g_dictPeriod.keys()):
        if s_period_window in self._g_dictPeriod:
            dict_rst = self._g_dictPeriodRaw[s_period_window].get_gross_numeric_by_index_full()
        else:
            for s_retrieve_attrs in self._g_lstRetrievableNumericalAttrs:
                dict_rst[s_retrieve_attrs] = 0
        return dict_rst

    def retrieve_period_by_ua_column(self, s_period_window):
        """
        get daily trend by media_ua
        :return:
        """
        # first, get mobile index
        lst_filter = [{'col_ttl': 'media_ua', 'fil_val': 'M'}]
        df_daily_by_mob = self._g_dictPeriodRaw[s_period_window].get_gross_filtered_by_index(lst_filter)
        del lst_filter
        del df_daily_by_mob['media_brd']

        # begin - gross index by mob
        df_gross_by_mob = df_daily_by_mob.sum()
        if df_gross_by_mob['in_site_tot_duration_sec'] > 0 and df_gross_by_mob['in_site_tot_session'] > 0:
            df_gross_by_mob['avg_dur_sec'] = df_gross_by_mob['in_site_tot_duration_sec'] / \
                                             df_gross_by_mob['in_site_tot_session']
        else:
            df_gross_by_mob['avg_dur_sec'] = 0

        if df_gross_by_mob['in_site_tot_pvs'] > 0 and df_gross_by_mob['in_site_tot_session'] > 0:
            df_gross_by_mob['avg_pvs'] = df_gross_by_mob['in_site_tot_pvs'] / df_gross_by_mob['in_site_tot_session']
        else:
            df_gross_by_mob['avg_pvs'] = 0

        if df_gross_by_mob['media_gross_cost_inc_vat'] > 0 and df_gross_by_mob['media_click'] > 0:
            df_gross_by_mob['gross_cpc_inc_vat'] = df_gross_by_mob['media_gross_cost_inc_vat'] / \
                                                   df_gross_by_mob['media_click']
        else:
            df_gross_by_mob['gross_cpc_inc_vat'] = 0

        if df_gross_by_mob['media_click'] > 0 and df_gross_by_mob['media_imp'] > 0:
            df_gross_by_mob['gross_ctr'] = df_gross_by_mob['media_click'] / df_gross_by_mob['media_imp'] * 100
        else:
            df_gross_by_mob['gross_ctr'] = 0

        df_gross_by_mob[['effective_session']] = df_gross_by_mob['in_site_tot_session'] - df_gross_by_mob[
            'in_site_tot_bounce']

        del df_gross_by_mob['in_site_tot_duration_sec']
        del df_gross_by_mob['in_site_tot_pvs']
        # end - gross index by mob

        # get full month date range for JS sparkline
        idx_full_day = self._g_dictPeriodRaw[s_period_window].get_full_period_idx()

        # make full month for js graph library
        df_daily_by_mob = df_daily_by_mob.reindex(idx_full_day, fill_value=0)

        # second, get pc index; avoid complex and similar calculation
        df_period_gross = self._g_dictPeriodRaw[s_period_window].get_period_numeric_by_index()
        df_daily_by_pc = df_period_gross - df_daily_by_mob

        # begin - calculate rate over in_site_tot_session
        df_daily_by_pc[['avg_dur_sec', 'avg_pvs']] = df_daily_by_pc[['in_site_tot_duration_sec', 'in_site_tot_pvs']]. \
            div(df_daily_by_pc['in_site_tot_session'].values, axis=0)
        # calculate gross cpc
        df_daily_by_pc[['gross_cpc_inc_vat']] = df_daily_by_pc[['media_gross_cost_inc_vat']].div(
            df_daily_by_pc['media_click'].values, axis=0)
        # calculate gross ctr
        df_daily_by_pc[['gross_ctr']] = df_daily_by_pc[['media_click']].div(df_daily_by_pc['media_imp'].values, axis=0)
        # end - calculate rate over in_site_tot_session

        # begin - gross index by pc
        df_gross_by_pc = df_daily_by_pc.sum()
        if df_gross_by_pc['in_site_tot_duration_sec'] > 0 and df_gross_by_pc['in_site_tot_session'] > 0:
            df_gross_by_pc['avg_dur_sec'] = df_gross_by_pc['in_site_tot_duration_sec'] / \
                                            df_gross_by_pc['in_site_tot_session']
        else:
            df_gross_by_pc['avg_dur_sec'] = 0

        if df_gross_by_pc['in_site_tot_pvs'] > 0 and df_gross_by_pc['in_site_tot_session'] > 0:
            df_gross_by_pc['avg_pvs'] = df_gross_by_pc['in_site_tot_pvs'] / df_gross_by_pc['in_site_tot_session']
        else:
            df_gross_by_pc['avg_pvs'] = 0

        if df_gross_by_pc['media_gross_cost_inc_vat'] > 0 and df_gross_by_pc['media_click'] > 0:
            df_gross_by_pc['gross_cpc_inc_vat'] = df_gross_by_pc['media_gross_cost_inc_vat'] / \
                                                  df_gross_by_pc['media_click']
        else:
            df_gross_by_pc['gross_cpc_inc_vat'] = 0

        if df_gross_by_pc['media_click'] > 0 and df_gross_by_pc['media_imp'] > 0:
            df_gross_by_pc['gross_ctr'] = df_gross_by_pc['media_click'] / df_gross_by_pc['media_imp'] * 100
        else:
            df_gross_by_pc['gross_ctr'] = 0

        del df_gross_by_pc['in_site_tot_duration_sec']
        del df_gross_by_pc['in_site_tot_pvs']
        # end - gross index by pc

        del df_daily_by_mob['in_site_tot_duration_sec']
        del df_daily_by_mob['in_site_tot_pvs']
        del df_daily_by_pc['in_site_tot_duration_sec']
        del df_daily_by_pc['in_site_tot_pvs']

        lst_numeric_idx = []
        for dt_date in df_daily_by_mob.index.to_list():
            lst_numeric_idx.append(dt_date.strftime('%d'))
        df_daily_by_mob = df_daily_by_mob.reset_index()
        df_daily_by_mob.index = lst_numeric_idx
        del lst_numeric_idx
        dict_daily_by_mob = df_daily_by_mob.to_dict()
        dict_daily_by_mob['gross'] = df_gross_by_mob.to_dict()

        # reset pc numeric index for js graph library
        # df_daily_by_pc = df_daily_by_pc.reindex(idx_full_day, fill_value=0)
        df_daily_by_pc.replace(np.nan, 0, inplace=True)
        lst_numeric_idx = []
        for dt_date in df_daily_by_pc.index.to_list():
            lst_numeric_idx.append(dt_date.strftime('%d'))
        df_daily_by_pc = df_daily_by_pc.reset_index()
        df_daily_by_pc.index = lst_numeric_idx
        del lst_numeric_idx
        dict_daily_by_pc = df_daily_by_pc.to_dict()
        dict_daily_by_pc['gross'] = df_gross_by_pc.to_dict()
        del df_daily_by_mob
        del df_gross_by_mob
        del df_daily_by_pc
        return dict_daily_by_mob, dict_daily_by_pc

    def retrieve_gross_term(self, s_period_window, s_sort_column='in_site_tot_session', n_th_rank=5):
        """
        get summary by term
        :param s_period_window:
        :param s_sort_column:
        :param n_th_rank:
        :return:
        """
        if not s_sort_column:
            raise Exception('invalid sort column')

        df_by_term = self._g_dictPeriodRaw[s_period_window].get_gross_group_by_index(['media_term'])
        for s_term in self._g_lstIgnoreTerms:
            df_by_term.drop(df_by_term.loc[df_by_term.index == s_term].index, inplace=True)

        # convert df to dict to draw table
        dict_top_kws = {}
        df = df_by_term.sort_values([s_sort_column], ascending=False).head(n_th_rank)
        for i, row in df.iterrows():
            dict_top_kws[row.name] = row.to_dict()

        return dict_top_kws, len(df_by_term.index) - n_th_rank

    def retrieve_gross_source_medium(self, s_period_window, s_sort_column='in_site_tot_session', n_th_rank=5):
        """
        get summary by source; this could be called by view class directly or called by to retrieve daily index by source
        :param s_period_window:
        :param s_sort_column:
        :param n_th_rank:
        :return:
        """
        if not s_sort_column:
            raise Exception('invalid sort column')

        # retrieve group by & sum data
        df_by_source = self._g_dictPeriodRaw[s_period_window].get_gross_group_by_index(['media_source', 'media_media'])
        # convert df to dict to draw table
        dict_top_source = {}
        df = df_by_source.sort_values([s_sort_column], ascending=False).head(n_th_rank)
        for i, row in df.iterrows():
            dict_top_source['_'.join(row.name)] = row.to_dict()
        return dict_top_source, len(df_by_source.index) - n_th_rank


class SourceMediumVisual(GaMediaVisual):
    def load_df(self, lst_retrieve_attrs):
        # for s_period in list(self._g_dictPeriod.keys()):
        for s_period in self._g_dictPeriod:
            dt_first_date = self._g_dictPeriod[s_period][0]
            dt_last_date = self._g_dictPeriod[s_period][1]
            o_ga_media_raw = GaSourceMediaRaw(self._g_oSvDb)
            o_ga_media_raw.set_period_dict(dt_start=dt_first_date, dt_end=dt_last_date)
            o_ga_media_raw.set_freq(self._g_sFreqMode)
            self._g_dictPeriodRaw[s_period] = o_ga_media_raw
            self._g_dictPeriodRaw[s_period].load_period()

    def get_graph_data(self):
        """
        get daily trend by media_source for top n_th
        :return:
        """
        # retrieve x-lbl
        lst_x_lbl_tm = self._retrieve_x_lbl('tm')
        lst_x_lbl_lm = self._retrieve_x_lbl('lm')
        lst_x_lbl_ly = self._retrieve_x_lbl('ly')

        # 당월 VAT 포함 총 비용(Media)
        dict_rst_tm = self._get_source_medium_info('tm', 'media_gross_cost_inc_vat')
        dict_rst_lm = self._get_source_medium_info('lm', 'media_gross_cost_inc_vat')
        dict_rst_ly = self._get_source_medium_info('ly', 'media_gross_cost_inc_vat')
        n_max_y = max(dict_rst_tm['column_max_value'], dict_rst_lm['column_max_value'], dict_rst_ly['column_max_value'])
        # lst define: 'graph_id', 'data_body', 'series label', 'y max val', 'lst_palette', 'graph_title', 'graph_height'
        lst_graph_to_draw = [
            ['media_gross_cost_inc_vat_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
             n_max_y, dict_rst_tm['lst_palette'], '당기 VAT 포함 총 비용(Media)', 430],
            ['media_gross_cost_inc_vat_lm', dict_rst_lm['dict_source_medium'], lst_x_lbl_lm,
             n_max_y, dict_rst_lm['lst_palette'], '전기 VAT 포함 총 비용(Media)', 430],
            ['media_gross_cost_inc_vat_ly', dict_rst_ly['dict_source_medium'], lst_x_lbl_ly,
             n_max_y, dict_rst_ly['lst_palette'], '전년 VAT 포함 총 비용(Media)', 430],
        ]
        # 총 PS 노출수(Media)
        dict_rst_tm = self._get_source_medium_info('tm', 'media_imp')
        dict_rst_lm = self._get_source_medium_info('lm', 'media_imp')
        dict_rst_ly = self._get_source_medium_info('ly', 'media_imp')
        n_max_y = max(dict_rst_tm['column_max_value'], dict_rst_lm['column_max_value'], dict_rst_ly['column_max_value'])
        lst_graph_to_draw.append(['media_imp_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
                                  n_max_y, dict_rst_tm['lst_palette'], '총 PS 노출수(Media)', 430])
        lst_graph_to_draw.append(['media_imp_lm', dict_rst_lm['dict_source_medium'], lst_x_lbl_lm,
                                  n_max_y, dict_rst_lm['lst_palette'], '총 PS 노출수(Media)', 430])
        lst_graph_to_draw.append(['media_imp_ly', dict_rst_ly['dict_source_medium'], lst_x_lbl_ly,
                                  n_max_y, dict_rst_ly['lst_palette'], '총 PS 노출수(Media)', 430])
        # 총 PS 클릭수(Media)
        dict_rst_tm = self._get_source_medium_info('tm', 'media_click')
        dict_rst_lm = self._get_source_medium_info('lm', 'media_click')
        dict_rst_ly = self._get_source_medium_info('ly', 'media_click')
        n_max_y = max(dict_rst_tm['column_max_value'], dict_rst_lm['column_max_value'], dict_rst_ly['column_max_value'])
        lst_graph_to_draw.append(['media_click_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
                                  n_max_y, dict_rst_tm['lst_palette'], '총 PS 클릭수(Media)', 430])
        lst_graph_to_draw.append(['media_click_lm', dict_rst_lm['dict_source_medium'], lst_x_lbl_lm,
                                  n_max_y, dict_rst_lm['lst_palette'], '총 PS 클릭수(Media)', 430])
        lst_graph_to_draw.append(['media_click_ly', dict_rst_ly['dict_source_medium'], lst_x_lbl_ly,
                                  n_max_y, dict_rst_ly['lst_palette'], '총 PS 클릭수(Media)', 430])
        # VAT 포함 총 PS CPC(Media)
        dict_rst_tm = self._get_source_medium_info('tm', 'gross_cpc_inc_vat')
        dict_rst_lm = self._get_source_medium_info('lm', 'gross_cpc_inc_vat')
        dict_rst_ly = self._get_source_medium_info('ly', 'gross_cpc_inc_vat')
        n_max_y = max(dict_rst_tm['column_max_value'], dict_rst_lm['column_max_value'], dict_rst_ly['column_max_value'])
        lst_graph_to_draw.append(['gross_cpc_inc_vat_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
                                  n_max_y, dict_rst_tm['lst_palette'], 'VAT 포함 총 PS CPC(Media)', 430])
        lst_graph_to_draw.append(['gross_cpc_inc_vat_lm', dict_rst_lm['dict_source_medium'], lst_x_lbl_lm,
                                  n_max_y, dict_rst_lm['lst_palette'], 'VAT 포함 총 PS CPC(Media)', 430])
        lst_graph_to_draw.append(['gross_cpc_inc_vat_ly', dict_rst_ly['dict_source_medium'], lst_x_lbl_ly,
                                  n_max_y, dict_rst_ly['lst_palette'], 'VAT 포함 총 PS CPC(Media)', 430])
        # 총 PS CTR % (Media)
        dict_rst_tm = self._get_source_medium_info('tm', 'gross_ctr')
        dict_rst_lm = self._get_source_medium_info('lm', 'gross_ctr')
        dict_rst_ly = self._get_source_medium_info('ly', 'gross_ctr')
        n_max_y = max(dict_rst_tm['column_max_value'], dict_rst_lm['column_max_value'], dict_rst_ly['column_max_value'])
        lst_graph_to_draw.append(['gross_ctr_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
                                  n_max_y, dict_rst_tm['lst_palette'], '총 PS CTR % (Media)', 430])
        lst_graph_to_draw.append(['gross_ctr_lm', dict_rst_lm['dict_source_medium'], lst_x_lbl_lm,
                                  n_max_y, dict_rst_lm['lst_palette'], '총 PS CTR % (Media)', 430])
        lst_graph_to_draw.append(['gross_ctr_ly', dict_rst_ly['dict_source_medium'], lst_x_lbl_ly,
                                  n_max_y, dict_rst_ly['lst_palette'], '총 PS CTR % (Media)', 430])
        # 총 유입 세션수(GA)
        dict_rst_tm = self._get_source_medium_info('tm', 'in_site_tot_session')
        dict_rst_lm = self._get_source_medium_info('lm', 'in_site_tot_session')
        dict_rst_ly = self._get_source_medium_info('ly', 'in_site_tot_session')
        n_max_y = max(dict_rst_tm['column_max_value'], dict_rst_lm['column_max_value'], dict_rst_ly['column_max_value'])
        lst_graph_to_draw.append(['in_site_tot_session_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
                                  n_max_y, dict_rst_tm['lst_palette'], '총 유입 세션수(GA)', 430])
        lst_graph_to_draw.append(['in_site_tot_session_lm', dict_rst_lm['dict_source_medium'], lst_x_lbl_lm,
                                  n_max_y, dict_rst_lm['lst_palette'], '총 유입 세션수(GA)', 430])
        lst_graph_to_draw.append(['in_site_tot_session_ly', dict_rst_ly['dict_source_medium'], lst_x_lbl_ly,
                                  n_max_y, dict_rst_ly['lst_palette'], '총 유입 세션수(GA)', 430])
        # 총 신규 세션수(GA)
        dict_rst_tm = self._get_source_medium_info('tm', 'in_site_tot_new')
        dict_rst_lm = self._get_source_medium_info('lm', 'in_site_tot_new')
        dict_rst_ly = self._get_source_medium_info('ly', 'in_site_tot_new')
        n_max_y = max(dict_rst_tm['column_max_value'], dict_rst_lm['column_max_value'], dict_rst_ly['column_max_value'])
        lst_graph_to_draw.append(['in_site_tot_new_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
                                  n_max_y, dict_rst_tm['lst_palette'], '총 신규 세션수(GA)', 430])
        lst_graph_to_draw.append(['in_site_tot_new_lm', dict_rst_lm['dict_source_medium'], lst_x_lbl_lm,
                                  n_max_y, dict_rst_lm['lst_palette'], '총 신규 세션수(GA)', 430])
        lst_graph_to_draw.append(['in_site_tot_new_ly', dict_rst_ly['dict_source_medium'], lst_x_lbl_ly,
                                  n_max_y, dict_rst_ly['lst_palette'], '총 신규 세션수(GA)', 430])
        # 총 이탈 세션수(GA)
        dict_rst_tm = self._get_source_medium_info('tm', 'in_site_tot_bounce')
        dict_rst_lm = self._get_source_medium_info('lm', 'in_site_tot_bounce')
        dict_rst_ly = self._get_source_medium_info('ly', 'in_site_tot_bounce')
        n_max_y = max(dict_rst_tm['column_max_value'], dict_rst_lm['column_max_value'], dict_rst_ly['column_max_value'])
        lst_graph_to_draw.append(['in_site_tot_bounce_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
                                  n_max_y, dict_rst_tm['lst_palette'], '총 이탈 세션수(GA)', 430])
        lst_graph_to_draw.append(['in_site_tot_bounce_lm', dict_rst_lm['dict_source_medium'], lst_x_lbl_lm,
                                  n_max_y, dict_rst_lm['lst_palette'], '총 이탈 세션수(GA)', 430])
        lst_graph_to_draw.append(['in_site_tot_bounce_ly', dict_rst_ly['dict_source_medium'], lst_x_lbl_ly,
                                  n_max_y, dict_rst_ly['lst_palette'], '총 이탈 세션수(GA)', 430])
        # 평균 체류시간 sec(GA)
        dict_rst_tm = self._get_source_medium_info('tm', 'avg_dur_sec')
        dict_rst_lm = self._get_source_medium_info('lm', 'avg_dur_sec')
        dict_rst_ly = self._get_source_medium_info('ly', 'avg_dur_sec')
        n_max_y = max(dict_rst_tm['column_max_value'], dict_rst_lm['column_max_value'], dict_rst_ly['column_max_value'])
        lst_graph_to_draw.append(['avg_dur_sec_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
                                  n_max_y, dict_rst_tm['lst_palette'], '평균 체류시간 sec(GA)', 430])
        lst_graph_to_draw.append(['avg_dur_sec_lm', dict_rst_lm['dict_source_medium'], lst_x_lbl_lm,
                                  n_max_y, dict_rst_lm['lst_palette'], '평균 체류시간 sec(GA)', 430])
        lst_graph_to_draw.append(['avg_dur_sec_ly', dict_rst_ly['dict_source_medium'], lst_x_lbl_ly,
                                  n_max_y, dict_rst_ly['lst_palette'], '평균 체류시간 sec(GA)', 430])
        # 평균 PV(GA)
        dict_rst_tm = self._get_source_medium_info('tm', 'avg_pvs')
        dict_rst_lm = self._get_source_medium_info('lm', 'avg_pvs')
        dict_rst_ly = self._get_source_medium_info('ly', 'avg_pvs')
        n_max_y = max(dict_rst_tm['column_max_value'], dict_rst_lm['column_max_value'], dict_rst_ly['column_max_value'])
        lst_graph_to_draw.append(['avg_pvs_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
                                  n_max_y, dict_rst_tm['lst_palette'], '평균 PV(GA)', 430])
        lst_graph_to_draw.append(['avg_pvs_lm', dict_rst_lm['dict_source_medium'], lst_x_lbl_lm,
                                  n_max_y, dict_rst_lm['lst_palette'], '평균 PV(GA)', 430])
        lst_graph_to_draw.append(['avg_pvs_ly', dict_rst_ly['dict_source_medium'], lst_x_lbl_ly,
                                  n_max_y, dict_rst_ly['lst_palette'], '평균 PV(GA)', 430])
        return lst_graph_to_draw


# pd.set_option('display.max_columns', None) 후에 print(df)하면 모든 컬럼명이 출력됨
class GmAgencyVisual(GaMediaVisual):
    """
    match with ./templates/svload/agency_detail.html
    """
    _g_nAgencyId = None

    def set_agency_id(self, n_agency_id):
        self._g_nAgencyId = n_agency_id

    def load_df(self, lst_retrieve_attrs):
        # for s_period in list(self._g_dictPeriod.keys()):
        for s_period in self._g_dictPeriod:
            dt_first_date = self._g_dictPeriod[s_period][0]
            dt_last_date = self._g_dictPeriod[s_period][1]
            o_ga_media_raw = GaSourceMediaRaw(self._g_oSvDb)
            o_ga_media_raw.set_period_dict(dt_start=dt_first_date, dt_end=dt_last_date)
            o_ga_media_raw.set_freq(self._g_sFreqMode)
            self._g_dictPeriodRaw[s_period] = o_ga_media_raw
            self._g_dictPeriodRaw[s_period].set_agency_id(self._g_nAgencyId)
            self._g_dictPeriodRaw[s_period].load_period()
            self._g_dictPeriodRaw[s_period].compress_data_by_freq_mode()
            self._g_dictPeriodRaw[s_period].aggregate_data(self._g_dtMtdElapsedDays)

    def get_graph_data(self):
        """
        get daily trend by media_source for top n_th
        :return:
        """
        # retrieve x-lbl
        lst_x_lbl_tm = self._retrieve_x_lbl('tm')

        # 당월 VAT 포함 총 비용(Media)
        dict_rst_tm = self._get_source_medium_info('tm', 'media_gross_cost_inc_vat')
        n_max_y = dict_rst_tm['column_max_value']
        # lst define: 'graph_id', 'data_body', 'series label', 'y max val', 'lst_palette', 'graph_title', 'graph_height'
        lst_graph_to_draw = [
            ['media_gross_cost_inc_vat_tm', dict_rst_tm['dict_source_medium'], lst_x_lbl_tm,
             n_max_y, dict_rst_tm['lst_palette'], '당기 VAT 포함 총 비용(Media)', 210],
        ]
        return lst_graph_to_draw

    def retrieve_raw_lst_by_column(self, s_period_window, s_column_title):
        """
        provide series values for bokeh
        :param s_period_window:
        :param s_column_title:
        :return:
        """
        # 'DataFrame' object has no attribute 'tolist' could be occured if DF has single column
        # use .squeeze() to avoid the exception
        # if s_period_window in list(self._g_dictPeriod.keys()):
        if s_period_window in self._g_dictPeriod:
            return self._g_dictPeriodRaw[s_period_window].get_period_numeric_by_index(s_column_title).tolist()

    def retrieve_longest_x_lbl(self):
        """
        get period of analyzing window
        :return:
        """
        n_longest_days = 0
        # for s_period_window in list(self._g_dictPeriod.keys()):
        for s_period_window in self._g_dictPeriod:
            idx_tmp = self._g_dictPeriodRaw[s_period_window].get_period_numeric_by_index('in_site_tot_session').index
            n_idx = len(idx_tmp)
            if n_longest_days < n_idx:
                n_longest_days = n_idx
        del idx_tmp
        lst_x_axis_label = []
        for n_idx in range(1, n_longest_days + 1):
            lst_x_axis_label.append(n_idx)
        return lst_x_axis_label

    def retrieve_mtd_by_column(self, s_period_window):
        dict_rst = {}
        # if s_period_window in list(self._g_dictPeriod.keys()):
        if s_period_window in self._g_dictPeriod:
            dict_rst = self._g_dictPeriodRaw[s_period_window].get_gross_numeric_by_index_mtd()
        else:
            for s_retrieve_attrs in self._g_lstRetrievableNumericalAttrs:
                dict_rst[s_retrieve_attrs] = 0
        return dict_rst

    def retrieve_full_by_column(self, s_period_window):
        dict_rst = {}
        # if s_period_window in list(self._g_dictPeriod.keys()):
        if s_period_window in self._g_dictPeriod:
            dict_rst = self._g_dictPeriodRaw[s_period_window].get_gross_numeric_by_index_full()
        else:
            for s_retrieve_attrs in self._g_lstRetrievableNumericalAttrs:
                dict_rst[s_retrieve_attrs] = 0
        return dict_rst

    def retrieve_period_by_ua_column(self, s_period_window):
        """
        get daily trend by media_ua
        :return:
        """
        # first, get mobile index
        lst_filter = [{'col_ttl': 'media_ua', 'fil_val': 'M'}]
        df_daily_by_mob = self._g_dictPeriodRaw[s_period_window].get_gross_filtered_by_index(lst_filter)
        del lst_filter
        del df_daily_by_mob['media_brd']

        # begin - gross index by mob
        df_gross_by_mob = df_daily_by_mob.sum()
        if df_gross_by_mob['in_site_tot_duration_sec'] > 0 and df_gross_by_mob['in_site_tot_session'] > 0:
            df_gross_by_mob['avg_dur_sec'] = df_gross_by_mob['in_site_tot_duration_sec'] / \
                                             df_gross_by_mob['in_site_tot_session']
        else:
            df_gross_by_mob['avg_dur_sec'] = 0

        if df_gross_by_mob['in_site_tot_pvs'] > 0 and df_gross_by_mob['in_site_tot_session'] > 0:
            df_gross_by_mob['avg_pvs'] = df_gross_by_mob['in_site_tot_pvs'] / df_gross_by_mob['in_site_tot_session']
        else:
            df_gross_by_mob['avg_pvs'] = 0

        if df_gross_by_mob['media_gross_cost_inc_vat'] > 0 and df_gross_by_mob['media_click'] > 0:
            df_gross_by_mob['gross_cpc_inc_vat'] = df_gross_by_mob['media_gross_cost_inc_vat'] / \
                                                   df_gross_by_mob['media_click']
        else:
            df_gross_by_mob['gross_cpc_inc_vat'] = 0

        if df_gross_by_mob['media_click'] > 0 and df_gross_by_mob['media_imp'] > 0:
            df_gross_by_mob['gross_ctr'] = df_gross_by_mob['media_click'] / df_gross_by_mob['media_imp'] * 100
        else:
            df_gross_by_mob['gross_ctr'] = 0

        df_gross_by_mob[['effective_session']] = df_gross_by_mob['in_site_tot_session'] - df_gross_by_mob[
            'in_site_tot_bounce']

        del df_gross_by_mob['in_site_tot_duration_sec']
        del df_gross_by_mob['in_site_tot_pvs']
        # end - gross index by mob

        # get full month date range for JS sparkline
        idx_full_day = self._g_dictPeriodRaw[s_period_window].get_full_period_idx()

        # make full month for js graph library
        df_daily_by_mob = df_daily_by_mob.reindex(idx_full_day, fill_value=0)

        # second, get pc index; avoid complex and similar calculation
        df_period_gross = self._g_dictPeriodRaw[s_period_window].get_period_numeric_by_index()
        df_daily_by_pc = df_period_gross - df_daily_by_mob

        # begin - calculate rate over in_site_tot_session
        df_daily_by_pc[['avg_dur_sec', 'avg_pvs']] = df_daily_by_pc[['in_site_tot_duration_sec', 'in_site_tot_pvs']]. \
            div(df_daily_by_pc['in_site_tot_session'].values, axis=0)
        # calculate gross cpc
        df_daily_by_pc[['gross_cpc_inc_vat']] = df_daily_by_pc[['media_gross_cost_inc_vat']].div(
            df_daily_by_pc['media_click'].values, axis=0)
        # calculate gross ctr
        df_daily_by_pc[['gross_ctr']] = df_daily_by_pc[['media_click']].div(df_daily_by_pc['media_imp'].values, axis=0)
        # end - calculate rate over in_site_tot_session

        # begin - gross index by pc
        df_gross_by_pc = df_daily_by_pc.sum()
        if df_gross_by_pc['in_site_tot_duration_sec'] > 0 and df_gross_by_pc['in_site_tot_session'] > 0:
            df_gross_by_pc['avg_dur_sec'] = df_gross_by_pc['in_site_tot_duration_sec'] / \
                                            df_gross_by_pc['in_site_tot_session']
        else:
            df_gross_by_pc['avg_dur_sec'] = 0

        if df_gross_by_pc['in_site_tot_pvs'] > 0 and df_gross_by_pc['in_site_tot_session'] > 0:
            df_gross_by_pc['avg_pvs'] = df_gross_by_pc['in_site_tot_pvs'] / df_gross_by_pc['in_site_tot_session']
        else:
            df_gross_by_pc['avg_pvs'] = 0

        if df_gross_by_pc['media_gross_cost_inc_vat'] > 0 and df_gross_by_pc['media_click'] > 0:
            df_gross_by_pc['gross_cpc_inc_vat'] = df_gross_by_pc['media_gross_cost_inc_vat'] / \
                                                  df_gross_by_pc['media_click']
        else:
            df_gross_by_pc['gross_cpc_inc_vat'] = 0

        if df_gross_by_pc['media_click'] > 0 and df_gross_by_pc['media_imp'] > 0:
            df_gross_by_pc['gross_ctr'] = df_gross_by_pc['media_click'] / df_gross_by_pc['media_imp'] * 100
        else:
            df_gross_by_pc['gross_ctr'] = 0

        del df_gross_by_pc['in_site_tot_duration_sec']
        del df_gross_by_pc['in_site_tot_pvs']
        # end - gross index by pc

        del df_daily_by_mob['in_site_tot_duration_sec']
        del df_daily_by_mob['in_site_tot_pvs']
        del df_daily_by_pc['in_site_tot_duration_sec']
        del df_daily_by_pc['in_site_tot_pvs']

        lst_numeric_idx = []
        for dt_date in df_daily_by_mob.index.to_list():
            lst_numeric_idx.append(dt_date.strftime('%d'))
        df_daily_by_mob = df_daily_by_mob.reset_index()
        df_daily_by_mob.index = lst_numeric_idx
        del lst_numeric_idx
        dict_daily_by_mob = df_daily_by_mob.to_dict()
        dict_daily_by_mob['gross'] = df_gross_by_mob.to_dict()

        # reset pc numeric index for js graph library
        # df_daily_by_pc = df_daily_by_pc.reindex(idx_full_day, fill_value=0)
        df_daily_by_pc.replace(np.nan, 0, inplace=True)
        lst_numeric_idx = []
        for dt_date in df_daily_by_pc.index.to_list():
            lst_numeric_idx.append(dt_date.strftime('%d'))
        df_daily_by_pc = df_daily_by_pc.reset_index()
        df_daily_by_pc.index = lst_numeric_idx
        del lst_numeric_idx
        dict_daily_by_pc = df_daily_by_pc.to_dict()
        dict_daily_by_pc['gross'] = df_gross_by_pc.to_dict()
        del df_daily_by_mob
        del df_gross_by_mob
        del df_daily_by_pc
        return dict_daily_by_mob, dict_daily_by_pc

    # def retrieve_gross_term(self, s_period_window, s_sort_column='in_site_tot_session', n_th_rank=5):
    #     """
    #     get summary by term
    #     :param s_period_window:
    #     :param s_sort_column:
    #     :param n_th_rank:
    #     :return:
    #     """
    #     if not s_sort_column:
    #         raise Exception('invalid sort column')

    #     df_by_term = self._g_dictPeriodRaw[s_period_window].get_gross_group_by_index(['media_term'])
    #     for s_term in self._g_lstIgnoreTerms:
    #         df_by_term.drop(df_by_term.loc[df_by_term.index == s_term].index, inplace=True)

    #     # convert df to dict to draw table
    #     dict_top_kws = {}
    #     df = df_by_term.sort_values([s_sort_column], ascending=False).head(n_th_rank)
    #     for i, row in df.iterrows():
    #         dict_top_kws[row.name] = row.to_dict()

    #     return dict_top_kws, len(df_by_term.index) - n_th_rank

    # def retrieve_gross_source_medium(self, s_period_window, s_sort_column='in_site_tot_session', n_th_rank=5):
    #     """
    #     get summary by source; this could be called by view class directly or called by to retrieve daily index by source
    #     :param s_period_window:
    #     :param s_sort_column:
    #     :param n_th_rank:
    #     :return:
    #     """
    #     if not s_sort_column:
    #         raise Exception('invalid sort column')

    #     # retrieve group by & sum data
    #     df_by_source = self._g_dictPeriodRaw[s_period_window].get_gross_group_by_index(['media_source', 'media_media'])
    #     # convert df to dict to draw table
    #     dict_top_source = {}
    #     df = df_by_source.sort_values([s_sort_column], ascending=False).head(n_th_rank)
    #     for i, row in df.iterrows():
    #         dict_top_source['_'.join(row.name)] = row.to_dict()
    #     return dict_top_source, len(df_by_source.index) - n_th_rank


class GaSourceMediaRaw:
    __g_oSvCampaignParser = None
    __g_lstAllowedSamplingFreq = ['Q', 'M', 'W', 'D']
    __g_sFreqMode = 'D'  # default freq is daily
    __g_fFullPeriodEstFactor = 1  # 전기간 마감 예상 가중치
    __g_dictAggregatedDf = {}  # 지표별 합산 데이터

    # def __new__(cls):
    #    # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
    #    return super().__new__(cls)

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        if not o_sv_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self.__g_oSvDb = o_sv_db
        self.__g_oSvCampaignParser = SvCampaignParser()
        self.__g_sClassId = id(self)
        self.__g_dictAggregatedDf[self.__g_sClassId] = {'mtd': None, 'full': None}  # 지표별 합산 데이터
        self.__g_dtToday = datetime.today().date()
        self.__g_dtDesignatedFirstDate = None  # 추출 기간 시작일
        self.__g_dtDesignatedLastDate = None  # 추출 기간 종료일
        self.__g_dfPeriodDataRaw = None  # 추출한 기간 데이터 원본
        self.__g_nAgencyId = None
        self.__g_dfCompressedByFreqMode = None  # 빈도에 따라 압축된 데이터
        self.__g_dictBudgetInfo = {}  # 예산과 대행사 수수료 정보
        self.__g_idxFullPeriod = None  # 설정한 기간의 완전한 일자 목록
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # logger.debug('__del__')
        self.__g_sClassId = None
        self.__g_oSvDb = None
        self.__g_oSvCampaignParser = None
        self.__g_dtDesignatedFirstDate = None  # 추출 기간 시작일
        self.__g_dtDesignatedLastDate = None  # 추출 기간 종료일
        self.__g_dfPeriodDataRaw = None  # 추출한 기간 데이터 원본
        self.__g_nAgencyId = None
        self.__g_dfCompressedByFreqMode = None  # 빈도에 따라 압축된 데이터
        self.__g_dictAggregatedDf = {}  # 지표별 합산 데이터
        self.__g_dictBudgetInfo = {}  # 예산과 대행사 수수료 정보
        self.__g_idxFullPeriod = None  # 설정한 기간의 완전한 일자 목록
        self.__g_dtToday = None
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        # logger.debug('__exit__')
        pass

    def set_period_dict(self, dt_start, dt_end):
        self.__g_dtDesignatedFirstDate = dt_start
        self.__g_dtDesignatedLastDate = dt_end

    def set_freq(self, s_freq_mode):
        if s_freq_mode in self.__g_lstAllowedSamplingFreq:
            self.__g_sFreqMode = s_freq_mode
        else:
            pass  # default is already 'D'
        self.__g_sFreqMode = 'D'  ####################### for daily debug

    def set_agency_id(self, n_agency_id):
        self.__g_nAgencyId = n_agency_id

    def load_period(self):
        if self.__g_dtDesignatedFirstDate is None or self.__g_dtDesignatedLastDate is None:
            raise Exception('not ready to load dataframe to analyze')
        self.__construct_dataframe()  # retrieve raw data
        # self.__compress_data_by_freq_mode()  # compress by freq mode

    def get_budget_info(self, s_source_medium=None):
        if s_source_medium is None:
            return self.__g_dictBudgetInfo
        else:
            return self.__g_dictBudgetInfo[s_source_medium]

    def get_period_numeric_by_index(self, s_column_title=None):
        if s_column_title is None:
            return self.__g_dfCompressedByFreqMode
        else:
            return self.__g_dfCompressedByFreqMode[s_column_title]

    def get_gross_numeric_by_index_mtd(self, s_column_title=None):
        if s_column_title is None:
            return self.__g_dictAggregatedDf[self.__g_sClassId]['mtd']
        else:
            return self.__g_dictAggregatedDf[self.__g_sClassId]['mtd'][s_column_title]

    def get_gross_numeric_by_index_full(self, s_column_title=None):
        if s_column_title is None:
            return self.__g_dictAggregatedDf[self.__g_sClassId]['full']
        else:
            return self.__g_dictAggregatedDf[self.__g_sClassId]['full'][s_column_title]

    def get_sv_campaign_info(self, s_sv_camp_name):
        dict_camp_rst = self.__g_oSvCampaignParser.parse_campaign_code(s_sv_camp_name)
        if dict_camp_rst['source'] == 'unknown' or dict_camp_rst['rst_type'] == '' or \
                dict_camp_rst['medium'] == '':
            b_valid_sv_campaign_code = False
        else:
            b_valid_sv_campaign_code = True

        if b_valid_sv_campaign_code:
            df_campaign = self.__g_dfPeriodDataRaw.loc[
                (self.__g_dfPeriodDataRaw['media_source'] == dict_camp_rst['source']) &
                (self.__g_dfPeriodDataRaw['media_rst_type'] == dict_camp_rst['rst_type']) &
                (self.__g_dfPeriodDataRaw['media_media'] == dict_camp_rst['medium']) &
                (self.__g_dfPeriodDataRaw['media_camp1st'] == dict_camp_rst['campaign1st']) &
                (self.__g_dfPeriodDataRaw['media_camp2nd'] == dict_camp_rst['campaign2nd']) &
                (self.__g_dfPeriodDataRaw['media_camp3rd'] == dict_camp_rst['campaign3rd'])]
            del df_campaign['media_ua']
            del df_campaign['media_brd']
            del df_campaign['media_rst_type']
            del df_campaign['media_source']
            del df_campaign['media_media']
            del df_campaign['media_term']
            del df_campaign['media_camp1st']
            del df_campaign['media_camp2nd']
            del df_campaign['media_camp3rd']
            # no [inplace=True] param to avoid SettingWithCopyWarning of pandas
            df_renamed = df_campaign.rename({'media_gross_cost_inc_vat': 'campaign_gross_cost_inc_vat'}, axis=1)
            del df_campaign
            return df_renamed.sum().to_dict()
        else:
            return {'media_imp': 0, 'media_click': 0, 'in_site_tot_session': 0, 'in_site_tot_bounce': 0,
                    'in_site_tot_duration_sec': 0, 'in_site_tot_pvs': 0, 'in_site_tot_new': 0,
                    'campaign_gross_cost_inc_vat': 0}

    def get_gross_filtered_by_index(self, lst_filter):
        lst_pd_series = []
        for dict_filter in lst_filter:
            lst_pd_series.append(self.__g_dfPeriodDataRaw[dict_filter['col_ttl']] == dict_filter['fil_val'])

        b_first = True
        for pd_series in lst_pd_series:
            if b_first:
                combined_pd_series = pd_series
                b_first = False
            else:
                combined_pd_series = combined_pd_series & pd_series
        del lst_pd_series
        df_filtered = self.__g_dfPeriodDataRaw[combined_pd_series].resample(self.__g_sFreqMode).sum()
        # begin - calculate derivative index
        df_filtered['avg_dur_sec'] = df_filtered['in_site_tot_duration_sec'] / df_filtered['in_site_tot_session']
        df_filtered['avg_dur_sec'].replace(np.nan, 0, inplace=True)
        df_filtered['avg_pvs'] = df_filtered['in_site_tot_pvs'] / df_filtered['in_site_tot_session']
        df_filtered['avg_pvs'].replace(np.nan, 0, inplace=True)
        df_filtered['gross_cpc_inc_vat'] = df_filtered['media_gross_cost_inc_vat'] / df_filtered['media_click']
        df_filtered['gross_cpc_inc_vat'].replace(np.nan, 0, inplace=True)
        df_filtered['gross_cpc_inc_vat'].replace(np.inf, 0, inplace=True)
        df_filtered['gross_ctr'] = df_filtered['media_click'] / df_filtered['media_imp'] * 100
        df_filtered['gross_ctr'].replace(np.nan, 0, inplace=True)
        df_filtered['effective_session'] = df_filtered['in_site_tot_session'] - df_filtered['in_site_tot_bounce']
        # end - calculate derivative index
        return df_filtered

    def get_gross_group_by_index(self, lst_columns):
        # list에 유효 필드만 있는지 검사해야 함
        if type(lst_columns) != list:
            raise Exception('invalid groupby columns')
        df_grp = self.__g_dfPeriodDataRaw.groupby(lst_columns).sum()
        # begin - calculation
        df_grp['avg_dur_sec'] = df_grp['in_site_tot_duration_sec'] / df_grp['in_site_tot_session']
        df_grp['avg_dur_sec'].replace(np.nan, 0, inplace=True)
        df_grp['avg_pvs'] = df_grp['in_site_tot_pvs'] / df_grp['in_site_tot_session']
        df_grp['avg_pvs'].replace(np.nan, 0, inplace=True)
        df_grp['gross_cpc_inc_vat'] = df_grp['media_gross_cost_inc_vat'] / df_grp['media_click']
        df_grp['gross_cpc_inc_vat'].replace(np.nan, 0, inplace=True)
        df_grp['gross_cpc_inc_vat'].replace(np.inf, 0, inplace=True)
        df_grp['gross_ctr'] = df_grp['media_click'] / df_grp['media_imp'] * 100
        df_grp['gross_ctr'].replace(np.nan, 0, inplace=True)
        df_grp['effective_session'] = df_grp['in_site_tot_session'] - df_grp['in_site_tot_bounce']
        # del df_grp['in_site_tot_duration_sec']
        # del df_grp['in_site_tot_pvs']
        # end - calculation
        return df_grp

    def get_full_period_idx(self):
        return self.__g_idxFullPeriod

    def get_full_period_est_factor(self):
        return self.__g_fFullPeriodEstFactor

    def compress_data_by_freq_mode(self):
        """
        set self.__g_dfCompressedByFreqMode
        compress data by designated freq mode
        :return:
        """
        # remove non-quantitative columns
        df_compressed = self.__g_dfPeriodDataRaw.drop(
            ['media_ua', 'media_term', 'media_source', 'media_rst_type', 'media_media', 'media_brd'], axis=1)
        # compress data
        df_compressed = df_compressed.resample(self.__g_sFreqMode).sum()
        # begin - calculate derivative index
        df_compressed.insert(3, 'avg_dur_sec', df_compressed['in_site_tot_duration_sec'] / df_compressed['in_site_tot_session'])
        df_compressed['avg_dur_sec'].replace(np.nan, 0, inplace=True)
        df_compressed.insert(3, 'avg_pvs', df_compressed['in_site_tot_pvs'] / df_compressed['in_site_tot_session'])
        df_compressed['avg_pvs'].replace(np.nan, 0, inplace=True)
        df_compressed.insert(3, 'gross_cpc_inc_vat', df_compressed['media_gross_cost_inc_vat'] / df_compressed['media_click'])
        df_compressed['gross_cpc_inc_vat'].replace(np.nan, 0, inplace=True)
        df_compressed.insert(3, 'gross_ctr', df_compressed['media_click'] / df_compressed['media_imp'] * 100)
        df_compressed['gross_ctr'].replace(np.nan, 0, inplace=True)
        df_compressed.insert(3, 'effective_session', df_compressed['in_site_tot_session'] - df_compressed['in_site_tot_bounce'])
        # end - calculate derivative index
        self.__g_dfCompressedByFreqMode = df_compressed
        del df_compressed

    def aggregate_data(self, dt_mtd_elapsed_days):
        """
        depends on self.__g_dfCompressedByFreqMode
        summarize compressed data
        estimate index if this period
        get accumulate to-date if prev period
        :param dt_mtd_elapsed_days: 당기 기준 MTD 구하기 위한 경과일
        :return:
        """
        if self.__g_fFullPeriodEstFactor == 1:  # MTD mode; completed prev period     on-going current period
            # begin - MTD calculation
            dt_mtd_date = self.__g_dtDesignatedFirstDate + dt_mtd_elapsed_days
            s_mtd_date = dt_mtd_date.strftime("%Y-%m-%d")
            del dt_mtd_date

            try:
                df_aggregation_mtd = self.__g_dfCompressedByFreqMode[
                    (self.__g_dfCompressedByFreqMode.index <= s_mtd_date)].sum()
            except ValueError:  # 당월의 일수가 전월보다 긴 경우, 전체 월 가져오기
                df_aggregation_mtd = self.__g_dfCompressedByFreqMode.sum()

            if df_aggregation_mtd['in_site_tot_duration_sec'] > 0 and df_aggregation_mtd['in_site_tot_session'] > 0:
                df_aggregation_mtd['avg_dur_sec'] = df_aggregation_mtd['in_site_tot_duration_sec'] / \
                                                    df_aggregation_mtd['in_site_tot_session']
            else:
                df_aggregation_mtd['avg_dur_sec'] = 0

            if df_aggregation_mtd['in_site_tot_pvs'] > 0 and df_aggregation_mtd['in_site_tot_session'] > 0:
                df_aggregation_mtd['avg_pvs'] = df_aggregation_mtd['in_site_tot_pvs'] / \
                                                df_aggregation_mtd['in_site_tot_session']
            else:
                df_aggregation_mtd['avg_pvs'] = 0

            if df_aggregation_mtd['media_gross_cost_inc_vat'] > 0 and df_aggregation_mtd['media_click'] > 0:
                df_aggregation_mtd['gross_cpc_inc_vat'] = df_aggregation_mtd['media_gross_cost_inc_vat'] / \
                                                          df_aggregation_mtd['media_click']
            else:
                df_aggregation_mtd['gross_cpc_inc_vat'] = 0

            if df_aggregation_mtd['media_click'] > 0 and df_aggregation_mtd['media_imp'] > 0:
                df_aggregation_mtd['gross_ctr'] = df_aggregation_mtd['media_click'] / \
                                                  df_aggregation_mtd['media_imp'] * 100
            else:
                df_aggregation_mtd['gross_ctr'] = 0
            # end - MTD calculation

            # begin - estimation = full period raw data
            df_aggregation_est = self.__g_dfCompressedByFreqMode.sum()
            if df_aggregation_est['in_site_tot_duration_sec'] > 0 and df_aggregation_est['in_site_tot_session'] > 0:
                df_aggregation_est['avg_dur_sec'] = df_aggregation_est['in_site_tot_duration_sec'] / \
                                                    df_aggregation_est['in_site_tot_session']
            else:
                df_aggregation_est['avg_dur_sec'] = 0

            if df_aggregation_est['in_site_tot_pvs'] > 0 and df_aggregation_est['in_site_tot_session'] > 0:
                df_aggregation_est['avg_pvs'] = df_aggregation_est['in_site_tot_pvs'] / \
                                                df_aggregation_est['in_site_tot_session']
            else:
                df_aggregation_est['avg_pvs'] = 0

            if df_aggregation_est['media_gross_cost_inc_vat'] > 0 and df_aggregation_est['media_click'] > 0:
                df_aggregation_est['gross_cpc_inc_vat'] = df_aggregation_est['media_gross_cost_inc_vat'] / \
                                                          df_aggregation_est['media_click']
            else:
                df_aggregation_est['gross_cpc_inc_vat'] = 0

            if df_aggregation_est['media_click'] > 0 and df_aggregation_est['media_imp'] > 0:
                df_aggregation_est['gross_ctr'] = df_aggregation_est['media_click'] / \
                                                  df_aggregation_est['media_imp'] * 100
            else:
                df_aggregation_est['gross_ctr'] = 0
            # end - estimation = full period raw data

            del df_aggregation_mtd['in_site_tot_duration_sec']
            del df_aggregation_mtd['in_site_tot_pvs']
            self.__g_dictAggregatedDf[self.__g_sClassId]['mtd'] = df_aggregation_mtd.to_dict()
            del df_aggregation_est['in_site_tot_duration_sec']
            del df_aggregation_est['in_site_tot_pvs']
            self.__g_dictAggregatedDf[self.__g_sClassId]['full'] = df_aggregation_est.to_dict()
            del df_aggregation_mtd
            del df_aggregation_est
        else:  # Estimation mode; on-going current period
            # begin - MTD calculation
            df_aggregation_mtd = self.__g_dfCompressedByFreqMode.sum()  # MTD is raw for on-going period
            if df_aggregation_mtd['in_site_tot_duration_sec'] > 0 and df_aggregation_mtd['in_site_tot_session'] > 0:
                df_aggregation_mtd['avg_dur_sec'] = df_aggregation_mtd['in_site_tot_duration_sec'] / \
                                                    df_aggregation_mtd['in_site_tot_session']
            else:
                df_aggregation_mtd['avg_dur_sec'] = 0

            if df_aggregation_mtd['in_site_tot_pvs'] > 0 and df_aggregation_mtd['in_site_tot_session'] > 0:
                df_aggregation_mtd['avg_pvs'] = df_aggregation_mtd['in_site_tot_pvs'] / \
                                                df_aggregation_mtd['in_site_tot_session']
            else:
                df_aggregation_mtd['avg_pvs'] = 0

            if df_aggregation_mtd['media_gross_cost_inc_vat'] > 0 and df_aggregation_mtd['media_click'] > 0:
                df_aggregation_mtd['gross_cpc_inc_vat'] = df_aggregation_mtd['media_gross_cost_inc_vat'] / \
                                                          df_aggregation_mtd['media_click']
            else:
                df_aggregation_mtd['gross_cpc_inc_vat'] = 0

            if df_aggregation_mtd['media_click'] > 0 and df_aggregation_mtd['media_imp'] > 0:
                df_aggregation_mtd['gross_ctr'] = df_aggregation_mtd['media_click'] / \
                                                  df_aggregation_mtd['media_imp'] * 100
            else:
                df_aggregation_mtd['gross_ctr'] = 0
            # end - MTD calculation

            # begin - construct memory for estimation
            dict_est = {'media_imp': int(df_aggregation_mtd['media_imp'] * self.__g_fFullPeriodEstFactor),
                        'media_click': int(df_aggregation_mtd['media_click'] * self.__g_fFullPeriodEstFactor),
                        'in_site_tot_session': int(
                            df_aggregation_mtd['in_site_tot_session'] * self.__g_fFullPeriodEstFactor),
                        'in_site_tot_bounce': int(
                            df_aggregation_mtd['in_site_tot_bounce'] * self.__g_fFullPeriodEstFactor),
                        'in_site_tot_new': int(df_aggregation_mtd['in_site_tot_new'] * self.__g_fFullPeriodEstFactor),
                        'media_gross_cost_inc_vat': int(
                            df_aggregation_mtd['media_gross_cost_inc_vat'] * self.__g_fFullPeriodEstFactor),
                        'effective_session': int(
                            df_aggregation_mtd['effective_session'] * self.__g_fFullPeriodEstFactor),
                        'avg_dur_sec': df_aggregation_mtd['avg_dur_sec'],
                        'avg_pvs': df_aggregation_mtd['avg_pvs'],
                        'gross_cpc_inc_vat': df_aggregation_mtd['gross_cpc_inc_vat'],
                        'gross_ctr': df_aggregation_mtd['gross_ctr']}
            # end - construct memory for estimation
            del df_aggregation_mtd['in_site_tot_duration_sec']
            del df_aggregation_mtd['in_site_tot_pvs']
            self.__g_dictAggregatedDf[self.__g_sClassId]['mtd'] = df_aggregation_mtd.to_dict()
            self.__g_dictAggregatedDf[self.__g_sClassId]['full'] = dict_est
            del df_aggregation_mtd

    def __construct_dataframe(self):
        # begin - get period range index based on frequency mode
        dt_last_date_of_month = self.__get_last_day_of_month(self.__g_dtDesignatedFirstDate)
        self.__g_idxFullPeriod = pd.date_range(start=self.__g_dtDesignatedFirstDate, end=dt_last_date_of_month,
                                               freq=self.__g_sFreqMode)
        idx_full_day = self.__g_idxFullPeriod

        if self.__g_nAgencyId:  # agency view mode
            lst_raw_data = self.__g_oSvDb.executeQuery('getGaMediaDailyLogByPeriodAgencyId', self.__g_dtDesignatedFirstDate,
                                                        self.__g_dtDesignatedLastDate, self.__g_nAgencyId)
        else:  # ga media main view mode
            lst_raw_data = self.__g_oSvDb.executeQuery('getGaMediaDailyLogByPeriod', self.__g_dtDesignatedFirstDate,
                                                        self.__g_dtDesignatedLastDate)
        
        if lst_raw_data and 'err_code' in lst_raw_data[0].keys():  # for an initial stage; no table
            lst_raw_data = []

        lst_blank_raw = []
        if len(lst_raw_data) == 0:  # set blank dataset if no period data
            for pd_datetime in idx_full_day:
                lst_blank_raw.append({'media_ua': 'P', 'media_rst_type': 'NS', 'media_source': '(direct)',
                                      'media_media': '(none)', 'media_raw_cost': 0, 'media_agency_cost': 0,
                                      'media_cost_vat': 0, 'media_imp': 0, 'media_click': 0, 'media_term': '(notset)',
                                      'media_brd': 0, 'in_site_tot_session': 0, 'in_site_tot_bounce': 0,
                                      'in_site_tot_duration_sec': 0, 'in_site_tot_pvs': 0, 'in_site_tot_new': 0,
                                      'in_site_revenue': 0, 'in_site_trs': 0,
                                      'media_camp1st': '', 'media_camp2nd': '', 'media_camp3rd': '',
                                      'logdate': pd_datetime})
            self.__g_dfPeriodDataRaw = pd.DataFrame(lst_blank_raw)
        else:  # if period data
            self.__g_dfPeriodDataRaw = pd.DataFrame(lst_raw_data)
            # ensure logdate field to datetime format
            self.__g_dfPeriodDataRaw['logdate'] = pd.to_datetime(self.__g_dfPeriodDataRaw['logdate'])

            # begin - calculate weight factor for estimation
            lst_raw_data_date = self.__g_dfPeriodDataRaw['logdate'].unique().tolist()
            n_raw_data_first_date = lst_raw_data_date[0]  # 추출 데이터 기준 기간 시작일
            n_raw_data_last_date = lst_raw_data_date[-1]  # 추출 데이터 기준 기간 마지막일
            del lst_raw_data_date
            idx_raw_data_period = pd.date_range(start=n_raw_data_first_date, end=n_raw_data_last_date,
                                                freq=self.__g_sFreqMode)
            n_full_day_cnt = len(idx_full_day)
            n_actual_day_cnt = len(idx_raw_data_period)
            del idx_raw_data_period
            if n_full_day_cnt == n_actual_day_cnt:  # 청구일이 청구월의 마지막 날이면 예상 가중치는 1
                pass  # self.__g_fFullPeriodEstFactor = 1
            elif n_actual_day_cnt < n_full_day_cnt:  # 청구일이 청구월의 마지막 날보다 과거면 예상 가중치 계산
                dt_yesterday = datetime.today() - timedelta(1)
                self.__g_fFullPeriodEstFactor = 1 / dt_yesterday.day * dt_last_date_of_month.day
                del dt_yesterday
            # end - calculate weight factor for estimation

            # begin - 사용자 요청 기간보다 데이터 기간이 짧으면 누락일을 채움
            lst_index = []
            for pd_datetime in idx_full_day:
                if pd_datetime not in self.__g_dfPeriodDataRaw['logdate'].values:
                    lst_blank_raw.append({'media_ua': 'P', 'media_rst_type': 'NS', 'media_source': '(direct)',
                                          'media_media': '(none)', 'media_raw_cost': 0, 'media_agency_cost': 0,
                                          'media_cost_vat': 0, 'media_imp': 0, 'media_click': 0,
                                          'media_term': '(notset)',
                                          'media_brd': 0, 'in_site_tot_session': 0, 'in_site_tot_bounce': 0,
                                          'in_site_tot_duration_sec': 0, 'in_site_tot_pvs': 0, 'in_site_tot_new': 0,
                                          'in_site_revenue': 0, 'in_site_trs': 0,
                                          'media_camp1st': '', 'media_camp2nd': '', 'media_camp3rd': '',
                                          'logdate': pd_datetime})
                    lst_index.append(pd_datetime)
            if len(lst_index) > 0:
                df_null_date = pd.DataFrame(lst_blank_raw, index=lst_index)
                self.__g_dfPeriodDataRaw = pd.concat([self.__g_dfPeriodDataRaw, df_null_date])
            del lst_index
            # end - 사용자 요청 기간보다 데이터 기간이 짧으면 누락일을 0으로 채움
        del lst_blank_raw
        del idx_full_day
        self.__g_dfPeriodDataRaw = self.__g_dfPeriodDataRaw.set_index('logdate')
        self.__g_dfPeriodDataRaw['media_gross_cost_inc_vat'] = self.__g_dfPeriodDataRaw['media_raw_cost'] + \
                                                               self.__g_dfPeriodDataRaw['media_agency_cost'] + \
                                                               self.__g_dfPeriodDataRaw['media_cost_vat']
        # begin - retrieve agency fee
        # begin - construct source-medium level summary
        df_by_source_medium = self.__g_dfPeriodDataRaw.groupby(['media_source', 'media_media']).sum()
        df = df_by_source_medium.sort_values(['media_gross_cost_inc_vat'], ascending=False)
        for i, df_row_sm in df.iterrows():
            if df_row_sm['media_agency_cost'] > 0:
                n_agency_fee_inc_vat = df_row_sm['media_agency_cost'] * 1.1  # add VAT
                self.__g_dictBudgetInfo['_'.join(df_row_sm.name)] = {
                    's_media_agency_name': 'N/A',
                    'n_media_agency_id': 0,
                    'n_budget_tgt_amnt_inc_vat': 0,
                    'n_agency_fee_inc_vat': n_agency_fee_inc_vat,
                    'n_agency_fee_inc_vat_est': 0,
                    'f_est_factor': 1,
                    'b_campaign_level': False
                }
        del df_row_sm
        # end - construct source-medium level summary
        # begin - construct sv-campaign level summary
        dict_source_ttl_tag = self.__g_oSvCampaignParser.get_source_tag_title_dict(b_inverted=True)
        dict_medium_ttl_tag = self.__g_oSvCampaignParser.get_medium_type_tag_title_dict(b_inverted=True)
        df_by_sv_campaign = self.__g_dfPeriodDataRaw.groupby(
            ['media_source', 'media_rst_type', 'media_media', 'media_camp1st', 'media_camp2nd', 'media_camp3rd']).sum()
        df_camp = df_by_sv_campaign.sort_values(['media_gross_cost_inc_vat'], ascending=False)
        df_camp = df_camp[(df_camp.media_gross_cost_inc_vat > 0)]
        dict_camp_lvl_budget = {}
        for i, df_camp_row in df_camp.iterrows():
            if df_camp_row['media_agency_cost'] > 0:
                s_camp_id_uniq = dict_source_ttl_tag[df_camp_row.name[0]] + '_' + df_camp_row.name[1] + '_' + \
                                 dict_medium_ttl_tag[df_camp_row.name[2]] + '_' + df_camp_row.name[3] + '_' + \
                                 df_camp_row.name[4] + '_' + df_camp_row.name[5]
                dict_camp_lvl_budget[s_camp_id_uniq] = {
                    'n_agency_fee_inc_vat': df_camp_row['media_agency_cost'] * 1.1,  # add VAT
                }
        del df_camp
        # end - construct sv-campaign level summary
        # end - retrieve agency fee
        # begin - retrieve planned budget
        from .budget import Budget
        o_budget = Budget(self.__g_oSvDb)
        dict_budget = o_budget.get_budget_amnt_by_period(dt_start=self.__g_dtDesignatedFirstDate,
                                                         dt_end=dt_last_date_of_month)
        del o_budget
        for s_sm, dict_budget_single in dict_budget.items():  # sm means source-medium
            if s_sm in self.__g_dictBudgetInfo:  # update existing
                self.__g_dictBudgetInfo[s_sm]['s_media_agency_name'] = dict_budget_single['s_media_agency_name']
                self.__g_dictBudgetInfo[s_sm]['n_media_agency_id'] = dict_budget_single['n_media_agency_id']

                self.__g_dictBudgetInfo[s_sm]['n_budget_tgt_amnt_inc_vat'] = \
                    dict_budget_single['n_budget_tgt_amnt_inc_vat']
                f_est_factor = self.__get_estimate_factor(dict_budget_single['dt_period_start'],
                                                          dict_budget_single['dt_period_end'])
                if f_est_factor >= 1:  # f_est_factor == 1 means budget schedule has been finished
                    self.__g_dictBudgetInfo[s_sm]['n_agency_fee_inc_vat_est'] = \
                        self.__g_dictBudgetInfo[s_sm]['n_agency_fee_inc_vat'] * f_est_factor
                self.__g_dictBudgetInfo[s_sm]['f_est_factor'] = f_est_factor
            else:  # add new
                self.__g_dictBudgetInfo[s_sm] = {
                    's_media_agency_name': dict_budget_single['s_media_agency_name'],
                    'n_media_agency_id': dict_budget_single['n_media_agency_id'],
                    'n_budget_tgt_amnt_inc_vat': dict_budget_single['n_budget_tgt_amnt_inc_vat'],
                    'n_agency_fee_inc_vat': 0,
                    'n_agency_fee_inc_vat_est': 0,
                    'f_est_factor': 1,
                    'b_campaign_level': False
                }
            # begin - arrange campaign level budget if exists
            if dict_budget_single['b_campaign_level']:
                self.__g_dictBudgetInfo[s_sm]['b_campaign_level'] = True
                self.__g_dictBudgetInfo[s_sm]['dict_campaign'] = {}
                for s_camp_title, dict_single_campaign in dict_budget_single['dict_campaign'].items():
                    n_budget_tgt_amnt_inc_vat = dict_single_campaign['n_budget_tgt_amnt_inc_vat']
                    f_est_factor = self.__get_estimate_factor(dict_single_campaign['dt_period_start'],
                                                              dict_single_campaign['dt_period_end'])
                    if s_camp_title in dict_camp_lvl_budget:
                        n_agency_fee_inc_vat = dict_camp_lvl_budget[s_camp_title]['n_agency_fee_inc_vat']
                    else:
                        n_agency_fee_inc_vat = 0
                    # if f_est_factor >= 1:  # f_est_factor == 1 means budget schedule has been finished
                    #     n_agency_fee_inc_vat_est = n_agency_fee_inc_vat * f_est_factor
                    # else:
                    #     n_agency_fee_inc_vat_est = 0
                    self.__g_dictBudgetInfo[s_sm]['dict_campaign'][s_camp_title] = {
                        's_media_agency_name': dict_single_campaign['s_media_agency_name'],
                        'n_media_agency_id': dict_single_campaign['n_media_agency_id'],
                        'n_budget_tgt_amnt_inc_vat': n_budget_tgt_amnt_inc_vat,
                        'n_agency_fee_inc_vat': n_agency_fee_inc_vat,
                        'f_est_factor': f_est_factor
                    }
            # end - arrange campaign level budget if exists
        # begin - clear campaign level budget related
        del dict_camp_lvl_budget
        del dict_source_ttl_tag
        del dict_medium_ttl_tag
        # end - clear campaign level budget related
        # end - retrieve planned budget
        # remove unnecessary columns to minimize DF sum() overload
        del self.__g_dfPeriodDataRaw['media_raw_cost']
        del self.__g_dfPeriodDataRaw['media_agency_cost']
        del self.__g_dfPeriodDataRaw['media_cost_vat']

        # remove GA ecommerce data if null
        if self.__g_dfPeriodDataRaw['in_site_revenue'].sum() == 0:
            del self.__g_dfPeriodDataRaw['in_site_revenue']
        if self.__g_dfPeriodDataRaw['in_site_trs'].sum() == 0:
            del self.__g_dfPeriodDataRaw['in_site_trs']

    def __get_estimate_factor(self, dt_start, dt_end):
        if dt_end < self.__g_dtToday:  # budget completed
            f_est_factor = 1
        else:  # estimating budget
            if dt_start >= self.__g_dtToday:  # no estimation if a upcoming budget
                f_est_factor = 0
            else:
                idx_budget_entire_period = pd.date_range(start=dt_start, end=dt_end, freq=self.__g_sFreqMode)
                n_budget_entire_period_day_cnt = len(idx_budget_entire_period)
                del idx_budget_entire_period
                idx_budget_elapsed_period = pd.date_range(start=dt_start, end=self.__g_dtToday, freq=self.__g_sFreqMode)
                n_budget_elapsed_period_day_cnt = \
                    len(idx_budget_elapsed_period) - 1  # must be not today but yesterday
                del idx_budget_elapsed_period
                f_est_factor = 1 / n_budget_elapsed_period_day_cnt * n_budget_entire_period_day_cnt
        return f_est_factor

    def __get_last_day_of_month(self, dt_point):
        if dt_point.month == 12:
            return dt_point.replace(day=31)
        return dt_point.replace(month=dt_point.month + 1, day=1) - relativedelta(days=1)
