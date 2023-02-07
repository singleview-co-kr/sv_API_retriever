import pandas as pd
from datetime import date
from .edi_tools import EdiRanker, EdiSampler
from .sv_palette import SvPalette

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


# pd.set_option('display.max_columns', None) 후에 print(df)하면 모든 컬럼명이 출력됨
class EdiSkuPerformance:
    __g_dictPeriodRankDf = {'2ly': None, 'ly': None, 'lm': None, 'tm': None}
    __g_dictPeriod = {'2ly': [], 'ly': [], 'lm': [], 'tm': []}
    __g_dictEdiSku = {}  # SKU 정보 저장
    __g_dictEdiBranchId = {}  # 추출할 매장 고유 번호 저장
    __g_dfPeriodDataRaw = None
    __g_sSamplingFreq = None  # 사용자가 지정한 수치 합산 주기

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
        # logger.debug('__del__')
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        # logger.debug('__exit__')
        pass

    def set_period_dict(self, dict_period):
        """
        dtDesignatedFirstDate = self.__g_dictPeriod['2ly'][0]
        dtDesignatedLastDate = self.__g_dictPeriod['tm'][1]
        :param dict_period:
        :return:
        """
        if not dict_period['dt_first_day_2year_ago'] or not dict_period['dt_today']:
            raise Exception('invalid data period')
        self.__g_dictPeriod['tm'] = [dict_period['dt_first_day_this_month'], dict_period['dt_today']]
        self.__g_dictPeriod['lm'] = [dict_period['dt_first_day_month_ago'], dict_period['dt_last_day_month_ago']]
        self.__g_dictPeriod['ly'] = [dict_period['dt_first_day_year_ago'], dict_period['dt_last_day_year_ago']]
        self.__g_dictPeriod['2ly'] = [dict_period['dt_first_day_2year_ago'], dict_period['dt_last_day_2year_ago']]
        self.__g_sSamplingFreq = dict_period['s_cur_sampling_freq']

    def set_sku_dict(self, dict_sku):
        """
        :param dict_sku: must be single sku info; dict
        :return:
        """
        if len(dict_sku) == 0:
            self.__g_dictEdiSku = {1: {'hypermart_name': 'Emart', 'selected': '', 'mart_id': 3, 'name': 'none', 'first_detect_logdate': date.today()}}  # raise Exception('invalid dict_sku')
        else:
            self.__g_dictEdiSku = dict_sku
        # self.__g_dictEdiSku = dict_sku

    def set_all_branches(self, dict_branch):
        if len(dict_branch['dict_branch_info_for_ui']):
            self.__g_dictEdiBranchId = dict_branch['dict_branch_info_for_ui']
            # self.__g_dfEmartBranchId = pd.DataFrame(dict_all_branch_info_by_id).transpose()
        else:
            raise Exception('excel extraction failure - no branch info')
        # dict_all_branch_info_by_id = {}
        # if len(o_branch):
        #     for single_branch in o_branch:
        #         dict_branch = {'id': single_branch.id,
        #                        'name': single_branch.branch_name, 'branch_type': single_branch.get_branch_type_label(),
        #                        'do_name': single_branch.do_name, 'si_name': single_branch.si_name,
        #                        'gu_gun': single_branch.gu_gun, 'dong_myun_ri': single_branch.dong_myun_ri}
        #         dict_all_branch_info_by_id[single_branch.id] = dict_branch
        # else:
        #     raise Exception('excel extraction failure - no branch info')

    def load_df(self, df_edi_raw):
        if len(df_edi_raw.index) == 0:
            raise Exception('invalid data frame')

        self.__g_dfPeriodDataRaw = df_edi_raw
        o_emart_monthly_rank = EdiRanker()
        lst_emart_branch_id = list(self.__g_dictEdiBranchId.keys())
        for s_period in list(self.__g_dictPeriod.keys()):
            dt_first_date = self.__g_dictPeriod[s_period][0]
            dt_last_date = self.__g_dictPeriod[s_period][1]
            idx_mask = (self.__g_dfPeriodDataRaw['logdate'] >= dt_first_date) & (
                    self.__g_dfPeriodDataRaw['logdate'] <= dt_last_date)
            df_period_data = self.__g_dfPeriodDataRaw.loc[idx_mask]
            del idx_mask
            o_emart_monthly_rank.set_period(dt_first_date, dt_last_date)
            o_emart_monthly_rank.set_mart_info(lst_emart_branch_id)
            o_emart_monthly_rank.load_data(df_period_data)
            self.__g_dictPeriodRankDf[s_period] = o_emart_monthly_rank.get_rank('branch_id', 'amnt')
        del lst_emart_branch_id
        del o_emart_monthly_rank

    def load_period_data(self, o_db):
        if not o_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')

        if not self.__g_dictPeriod['2ly'][0] or not self.__g_dictPeriod['tm'][1]:
            raise Exception('invalid data period')

        # retrieve sku id list, convert int id to string id
        lst_raw_data = o_db.executeQuery('getEmartLogSingleItemId', str(next(iter(self.__g_dictEdiSku))),
                                         self.__g_dictPeriod['2ly'][0], self.__g_dictPeriod['tm'][1])
        # set daily raw data
        self.__g_dfPeriodDataRaw = pd.DataFrame(lst_raw_data)
        del lst_raw_data
        # unset unnecessary field
        # ensure logdate field to datetime format
        self.__g_dfPeriodDataRaw['logdate'] = pd.to_datetime(self.__g_dfPeriodDataRaw['logdate'])

        o_emart_monthly_rank = EdiRanker()
        lst_emart_branch_id = list(self.__g_dictEdiBranchId.keys())
        for s_period in list(self.__g_dictPeriod.keys()):
            dt_first_date = self.__g_dictPeriod[s_period][0]
            dt_last_date = self.__g_dictPeriod[s_period][1]
            idx_mask = (self.__g_dfPeriodDataRaw['logdate'] >= dt_first_date) & (
                        self.__g_dfPeriodDataRaw['logdate'] <= dt_last_date)
            df_period_data = self.__g_dfPeriodDataRaw.loc[idx_mask]
            del idx_mask
            o_emart_monthly_rank.set_period(dt_first_date, dt_last_date)
            o_emart_monthly_rank.set_mart_info('EM', lst_emart_branch_id)
            o_emart_monthly_rank.load_data(df_period_data)
            self.__g_dictPeriodRankDf[s_period] = o_emart_monthly_rank.get_rank('branch_id', 'amnt')
        del lst_emart_branch_id
        del o_emart_monthly_rank
        return True

    def retrieve_monthly_gross_vb(self):
        """
        retrieve gross monthly amount graph
        bokeh 그래프 데이터: 전국 공급액 추이 2년간
        _vb means vertical bar graph
        :return:
        """
        df_temp = self.__g_dfPeriodDataRaw.set_index('logdate')
        df_period_data_monthly = df_temp.resample(self.__g_sSamplingFreq).sum()  ### sampler로 이동 ################

        o_sv_palette = SvPalette()
        lst_series_color = o_sv_palette.get_single_color_lst()
        del o_sv_palette
        dict_branch_raw = {'전국': df_period_data_monthly['amnt'].tolist()}
        lst_x_labels = []
        for dt_date in df_period_data_monthly.index.to_list():
            lst_x_labels.append(dt_date.strftime("%Y%m"))
        return {'lst_series_info': dict_branch_raw, 'lst_x_labels': lst_x_labels, 'lst_palette': lst_series_color}

    def retrieve_monthly_gross_by_branch_vb(self, lst_item_line_color):
        """
        bokeh 그래프 데이터: Top N 공급액 추이 2년간
        _vb means vertical bar graph
        :param lst_item_line_color:
        :return:
        """
        n_th_to_display = len(lst_item_line_color)
        if self.__g_dictPeriodRankDf['tm']['qty'].sum() == 0 and self.__g_dictPeriodRankDf['tm']['amnt'].sum() == 0:
            lst_amnt_rank_branch_id = self.__g_dictPeriodRankDf['lm'].index[:n_th_to_display].tolist()
        else:
            lst_amnt_rank_branch_id = self.__g_dictPeriodRankDf['tm'].index[:n_th_to_display].tolist()
        dict_branch_raw = {}
        lst_line_color = []
        lst_x_labels = []

        o_emart_sampler = EdiSampler()
        o_emart_sampler.set_period(self.__g_dictPeriod['2ly'][0], self.__g_dictPeriod['tm'][1])
        for n_branch_id in lst_amnt_rank_branch_id:
            idx_mask = (self.__g_dfPeriodDataRaw['branch_id'] == n_branch_id)
            df_single_branch = self.__g_dfPeriodDataRaw.loc[idx_mask]
            del idx_mask
            o_emart_sampler.set_mart_info(list(self.__g_dictEdiBranchId.keys()))
            o_emart_sampler.load_data(df_single_branch)
            del df_single_branch
            df_sampled = o_emart_sampler.get_sampling(self.__g_sSamplingFreq)
            dict_branch_raw[self.__g_dictEdiBranchId[n_branch_id]['name']] = df_sampled['amnt'].tolist()
            lst_line_color.append(lst_item_line_color.pop(-1))
        del o_emart_sampler
        for dt_date in df_sampled.index.to_list():
            lst_x_labels.append(dt_date.strftime("%Y%m"))
        return {'lst_series_info': dict_branch_raw, 'lst_x_labels': lst_x_labels, 'lst_palette': lst_line_color}

    def retrieve_branch_overview(self):
        """
        매장별  DataTable.js 데이터 생성
        :return:
        """
        b_refer_lm_rank = False  # refer to lm rank if tm is empty
        if self.__g_dictPeriodRankDf['tm']['qty'].sum() == 0 and self.__g_dictPeriodRankDf['tm']['amnt'].sum() == 0:
            b_refer_lm_rank = True
            lst_amnt_rank_branch_id = self.__g_dictPeriodRankDf['lm'].index.tolist()
        else:
            lst_amnt_rank_branch_id = self.__g_dictPeriodRankDf['tm'].index.tolist()
        lst_branch_data_table = []
        dict_single_sku_info = list(self.__g_dictEdiSku.values())[0]
        n_mart_id = dict_single_sku_info['mart_id']
        for n_branch_id in lst_amnt_rank_branch_id:
            if b_refer_lm_rank:
                n_rank_tm = self.__g_dictPeriodRankDf['lm'].loc[n_branch_id, 'rank']
            else:
                n_rank_tm = self.__g_dictPeriodRankDf['tm'].loc[n_branch_id, 'rank']

            dict_branch_temp = self.__g_dictEdiBranchId[n_branch_id]
            if n_mart_id == dict_branch_temp['hypermart_id']:
                dict_branch_row = {'hypermart_name': dict_branch_temp['hypermart_name'],
                                   's_branch_name': dict_branch_temp['name'],
                                   'n_branch_id': n_branch_id,
                                   's_do_name': dict_branch_temp['do_name'],
                                   's_si_name': dict_branch_temp['si_name'],
                                   'n_rank_2ly': self.__g_dictPeriodRankDf['2ly'].loc[n_branch_id, 'rank'],
                                   'n_rank_ly': self.__g_dictPeriodRankDf['ly'].loc[n_branch_id, 'rank'],
                                   'n_rank_lm': self.__g_dictPeriodRankDf['lm'].loc[n_branch_id, 'rank'],
                                   'n_rank_tm': n_rank_tm,
                                   'n_amnt_2ly': self.__g_dictPeriodRankDf['2ly'].loc[n_branch_id, 'amnt'],
                                   'n_amnt_ly': self.__g_dictPeriodRankDf['ly'].loc[n_branch_id, 'amnt'],
                                   'n_amnt_lm': self.__g_dictPeriodRankDf['lm'].loc[n_branch_id, 'amnt'],
                                   'n_amnt_tm': self.__g_dictPeriodRankDf['tm'].loc[n_branch_id, 'amnt'],
                                   'n_qty_2ly': self.__g_dictPeriodRankDf['2ly'].loc[n_branch_id, 'qty'],
                                   'n_qty_ly': self.__g_dictPeriodRankDf['ly'].loc[n_branch_id, 'qty'],
                                   'n_qty_lm': self.__g_dictPeriodRankDf['lm'].loc[n_branch_id, 'qty'],
                                   'n_qty_tm': self.__g_dictPeriodRankDf['tm'].loc[n_branch_id, 'qty']}
                lst_branch_data_table.append(dict_branch_row)
        return lst_branch_data_table
