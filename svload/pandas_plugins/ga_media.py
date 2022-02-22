from abc import ABC, abstractmethod
import numpy as np
from .ga_media_raw import GaSourceMediaRaw

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class SvPallet:
    __g_dictSourceMedium = {'default': '#000000',  # https://www.color-hex.com/
                            'youtube_display': '#960614',
                            'google_cpc': '#6b0000',
                            'facebook_cpi': '#205a86',
                            'facebook_cpc': '#140696',
                            'naver_cpc': '#4d6165',
                            'naver_organic': '#798984',
                            'naver_display': '#8db670',
                            'kakao_cpc': '#ffad60'}
    __g_dictPeriodWindow = {'tm': '#2ABA9C',  # this period
                            'lm': '#A0DBCF',  # last period
                            'ly': '#D6E2DF',  # year ago period
                            '2ly': '#e7298a'}  # 2 years ago period
    __g_sDefaultColor = '#000000'

    def __new__(cls):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
        return super().__new__(cls)

    def __init__(self):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        # logger.debug('__exit__')
        pass

    def get_source_medium_color(self, s_source_medium=None):
        """
        :param s_source_medium:
        :return:
        """
        if s_source_medium is None:
            return self.__g_dictSourceMedium
        elif s_source_medium in self.__g_dictSourceMedium:  # list(self.__g_dictSourceMedium.keys()):
            return self.__g_sDefaultColor
        else:
            return self.__g_dictSourceMedium[s_source_medium]

    def get_period_window_color(self, s_period_window=None):
        """
        :param s_period_window:
        :return:
        """
        if s_period_window is None:
            return self.__g_dictPeriodWindow
        elif s_period_window in self.__g_dictPeriodWindow:  # list(self.__g_dictPeriodWindow.keys()):
            return self.__g_sDefaultColor
        else:
            return self.__g_dictPeriodWindow[s_period_window]


class GaMediaVisual(ABC):
    """
    https://hyunlee103.tistory.com/91  추상성 다형성
    """
    _g_oSvDb = None
    _g_dictPeriod = {'lm': [], 'tm': []}  #
    _g_dictSamplingFreq = {'qtr': 'Q', 'mon': 'M', 'day': 'D'}
    _g_sFreqMode = 'D'
    _g_nBrandId = -1
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

        o_sv_pallet = SvPallet()
        self._g_dictPalletSourceMedium = o_sv_pallet.get_source_medium_color()
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
        ######################
        self._g_sFreqMode = self._g_dictSamplingFreq['day']  # for daily debug
        ######################

    def set_brand_id(self, n_brand_id):  # 일치
        self._g_nBrandId = n_brand_id

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
        for s_source_medium, dict_row in dict_top_source_medium.items():
            if dict_row['media_gross_cost_inc_vat'] > 0:
                lst_info = s_source_medium.split('_')
                lst_filter = [{'col_ttl': 'media_source', 'fil_val': lst_info[0]},
                              {'col_ttl': 'media_media', 'fil_val': lst_info[1]}]
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
                        dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][
                            s_camp_name]['n_budget_tgt_amnt_inc_vat'] = dict_camp_budget['n_budget_tgt_amnt_inc_vat']
                        dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][
                            s_camp_name]['campaign_gross_cost_inc_vat_ttl'] = \
                            dict_ps_source_medium_gross[s_source_medium]['dict_campaign'][
                                s_camp_name]['campaign_gross_cost_inc_vat'] * dict_camp_budget['f_est_factor']
                else:
                    dict_ps_source_medium_gross[s_source_medium]['b_campaign_level'] = False
                # end - add a campaign level info if exists

                # make full month for bokeh
                del df_by_source_medium['in_site_tot_duration_sec']
                del df_by_source_medium['in_site_tot_pvs']
                df_by_source_medium = df_by_source_medium.reindex(idx_full_day, fill_value=0)
                dict_ps_source_medium_daily[s_source_medium] = df_by_source_medium

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
            self._g_dictPeriodRaw[s_period].set_brand_id(self._g_nBrandId)
            self._g_dictPeriodRaw[s_period].load_period(lst_retrieve_attrs)
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
            self._g_dictPeriodRaw[s_period].load_period(lst_retrieve_attrs)

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
