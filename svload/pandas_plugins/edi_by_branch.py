# import pandas as pd
from django.utils.html import strip_tags
from datetime import datetime
from datetime import date
from svcommon.sv_hypermart_model import SvHypermartGeoInfo
from .edi_tools import EdiRanker, EdiSampler
from .edi_raw import EdiRaw

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


# pd.set_option('display.max_columns', None) 후에 print(df)하면 모든 컬럼명이 출력됨
class Performance:
    __g_dictPeriodRankDf = {'2ly': None, 'ly': None, 'lm': None, 'tm': None}
    __g_dictPeriod = {'2ly': [], 'ly': [], 'lm': [], 'tm': []}
    __g_dictEdiSku = {}  # SKU 정보 저장
    __g_dictSingleBranch = None  # 단일 매장 모드에서 단일 매장 정보
    __g_dfPeriodDataRaw = None
    __g_sSamplingFreq = None

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
        if len(dict_sku) == 0:
            self.__g_dictEdiSku = {1: {'hypermart_name': 'Emart', 'selected': '', 'mart_id': 3, 'name': 'none', 'first_detect_logdate': date.today()}}  # raise Exception('invalid dict_sku')
        else:
            self.__g_dictEdiSku = dict_sku

    def set_single_branch_info(self, o_branch_info):
        """
        set single branches info
        :param o_branch_info:
        :return:
        """
        if type(o_branch_info) == dict:
            if len(o_branch_info['dict_branch_info_for_ui']):
                dict_single_branch = list(o_branch_info['dict_branch_info_for_ui'].values())[0]
                self.__g_dictSingleBranch = {'n_branch_id': dict_single_branch['id'],
                                             'hypermart_name': dict_single_branch['hypermart_name'],
                                             'n_hypermart_id': dict_single_branch['hypermart_id'],
                                             's_branch_name': dict_single_branch['name'],
                                             # 's_branch_type': dict_single_branch['hypermart_name'],
                                             's_do_name': dict_single_branch['do_name'],
                                             's_si_name': dict_single_branch['si_name'],
                                             's_gu_gun_name': dict_single_branch['gu_gun'],
                                             's_dong_myun_ri_name': dict_single_branch['dong_myun_ri']
                                             }
            else:
                raise Exception('excel extraction failure - no branch info')
        elif type(o_branch_info) == SvHypermartGeoInfo:
            self.__g_dictSingleBranch = {'n_branch_id': o_branch_info.id,
                                         'hypermart_name': o_branch_info.get_branch_type_label(),
                                         'n_hypermart_id': o_branch_info['hypermart_type'],  # should validate
                                         's_branch_name': o_branch_info.branch_name,
                                         # 's_branch_type': o_branch_info.get_branch_type_label(),
                                         's_do_name': o_branch_info.do_name,
                                         's_si_name': o_branch_info.si_name,
                                         's_gu_gun_name': o_branch_info.gu_gun,
                                         's_dong_myun_ri_name': o_branch_info.dong_myun_ri}
        else:
            raise Exception('invalid o_branch')

    def load_df(self, df_edi_raw):
        if len(df_edi_raw.index) == 0:
            return # raise Exception('invalid data frame')

        self.__g_dfPeriodDataRaw = df_edi_raw
        lst_edi_sku_id = list(self.__g_dictEdiSku.keys())
        o_emart_monthly_rank = EdiRanker()
        for s_period in self.__g_dictPeriod:
            dt_first_date = self.__g_dictPeriod[s_period][0]
            dt_last_date = self.__g_dictPeriod[s_period][1]
            idx_mask = (self.__g_dfPeriodDataRaw['logdate'] >= dt_first_date) & (
                    self.__g_dfPeriodDataRaw['logdate'] <= dt_last_date)
            df_period_data = self.__g_dfPeriodDataRaw.loc[idx_mask]
            del idx_mask
            o_emart_monthly_rank.set_period(dt_first_date, dt_last_date)
            o_emart_monthly_rank.set_sku_info(lst_edi_sku_id)

            o_emart_monthly_rank.load_data(df_period_data)
            self.__g_dictPeriodRankDf[s_period] = o_emart_monthly_rank.get_rank('item_id', 'amnt')
        del o_emart_monthly_rank
        del lst_edi_sku_id

    def retrieve_sku_rank_in_period(self, s_sort_column):
        """
        bokeh 그래프 데이터: 당월 품목별 공급액 순위, 당월 품목별 출고량 비교
        data for horizontal bar graph
        refer to https://stackoverflow.com/questions/28797330/infinite-horizontal-line-in-bokeh
        :param s_sort_column:
        :return:
        """
        # refer to lm rank if tm is empty
        if self.__g_dictPeriodRankDf['tm']['qty'].sum() == 0 and self.__g_dictPeriodRankDf['tm']['amnt'].sum() == 0:
            lst_amnt_rank_item_id = self.__g_dictPeriodRankDf['lm'].index.tolist()
        else:
            lst_amnt_rank_item_id = self.__g_dictPeriodRankDf['tm'].index.tolist()
        lst_sku_name_by_tm_amnt = []
        dict_amnt_by_sku = {'2ly': [], 'ly': [], 'lm': [], 'tm': []}
        dict_qty_by_sku = {'2ly': [], 'ly': [], 'lm': [], 'tm': []}

        for n_sku_id in lst_amnt_rank_item_id:
            if self.__g_dictPeriodRankDf['lm'].loc[n_sku_id, 'qty'] > 0 or \
                    self.__g_dictPeriodRankDf['tm'].loc[n_sku_id, 'qty'] > 0:
                lst_sku_name_by_tm_amnt.append(self.__g_dictEdiSku[n_sku_id]['name'])
                dict_amnt_by_sku['2ly'].append(self.__g_dictPeriodRankDf['2ly'].loc[n_sku_id, 'amnt'])
                dict_amnt_by_sku['ly'].append(self.__g_dictPeriodRankDf['ly'].loc[n_sku_id, 'amnt'])
                dict_amnt_by_sku['lm'].append(self.__g_dictPeriodRankDf['lm'].loc[n_sku_id, 'amnt'])
                dict_amnt_by_sku['tm'].append(self.__g_dictPeriodRankDf['tm'].loc[n_sku_id, 'amnt'])

                dict_qty_by_sku['2ly'].append(self.__g_dictPeriodRankDf['2ly'].loc[n_sku_id, 'qty'])
                dict_qty_by_sku['ly'].append(self.__g_dictPeriodRankDf['ly'].loc[n_sku_id, 'qty'])
                dict_qty_by_sku['lm'].append(self.__g_dictPeriodRankDf['lm'].loc[n_sku_id, 'qty'])
                dict_qty_by_sku['tm'].append(self.__g_dictPeriodRankDf['tm'].loc[n_sku_id, 'qty'])
        return {'lst_sku_name_by_tm_amnt': lst_sku_name_by_tm_amnt,
                'dict_amnt_by_sku': dict_amnt_by_sku,
                'dict_qty_by_sku': dict_qty_by_sku}

    def retrieve_monthly_chronicle_by_sku_ml(self, lst_item_line_color):
        """
        bokeh 그래프 데이터: Top N 공급액 추이 2년간
        data for multi line graph; _ml means multi-line
        self.__g_dfItemRankAmntTm 때문에 retrieve_sku_rank_in_period() 호출 후에 사용해야 함
        :param lst_item_line_color:
        :return:
        """
        n_th_to_display = len(lst_item_line_color)
        # refer to lm rank if tm is empty
        if self.__g_dictPeriodRankDf['tm']['qty'].sum() == 0 and self.__g_dictPeriodRankDf['tm']['amnt'].sum() == 0:
            lst_amnt_rank_item_id = self.__g_dictPeriodRankDf['lm'].index[:n_th_to_display].tolist()
        else:
            lst_amnt_rank_item_id = self.__g_dictPeriodRankDf['tm'].index[:n_th_to_display].tolist()

        lst_line_label = []
        lst_line_color = []
        lst_series_amnt = []
        # lst_x_labels = []
        lst_x_label = None
        o_emart_sampler = EdiSampler()
        o_emart_sampler.set_period(self.__g_dictPeriod['2ly'][0], self.__g_dictPeriod['tm'][1])
        for n_item_id in lst_amnt_rank_item_id:
            idx_mask = (self.__g_dfPeriodDataRaw['item_id'] == n_item_id)
            df_single_item = self.__g_dfPeriodDataRaw.loc[idx_mask]
            del idx_mask
            o_emart_sampler.load_data(df_single_item)
            del df_single_item
            df_sampled = o_emart_sampler.get_sampling(self.__g_sSamplingFreq)
            lst_line_label.append(self.__g_dictEdiSku[n_item_id]['name'])
            lst_line_color.append(lst_item_line_color.pop(-1))
            lst_series_amnt.append(df_sampled['amnt'].tolist())
            # lst_x_labels.append(df_sampled.index.unique().tolist())
            lst_x_label = df_sampled.index.unique().tolist()
            del df_sampled
        del o_emart_sampler
        return {'lst_line_label': lst_line_label, 'lst_line_color': lst_line_color,
                'lst_series_amnt': lst_series_amnt, 'lst_x_label': lst_x_label}

    def retrieve_branch_level_overview(self, o_db):
        """
        공급액(매장/전국), 출고량(매장/전국) chart.js 그래프 데이터 생성
        전국 매장 성과를 집계해야 해서 self.__g_dfPeriodDataRaw를 사용하지 않음
        :return:
        """
        dict_national_rst = {'amnt': [], 'qty': []}
        dict_branch_rst = {'amnt': [], 'qty': []}
        o_edi_raw = EdiRaw()
        o_edi_raw.set_freq({'qtr': 0, 'mon': 1, 'day': 0})
        o_edi_raw.set_sku_dict(self.__g_dictEdiSku)
        dict_branch_info = {'dict_branch_info_for_ui': {self.__g_dictSingleBranch['n_branch_id']: self.__g_dictSingleBranch}}
        o_edi_raw.set_branch_info(dict_branch_info)
        for s_period_window in list(self.__g_dictPeriod.keys()):  # self.__g_lstAllowedPeriod:
            dt_first_date = self.__g_dictPeriod[s_period_window][0]
            dt_last_date = self.__g_dictPeriod[s_period_window][1]
            o_edi_raw.set_period_dict(dt_start=dt_first_date, dt_end=dt_last_date)
            df_national_gross = o_edi_raw.load_sch_gross_by_item_id_list(o_db,
                                                                         self.__g_dictSingleBranch['n_hypermart_id'])
            # dict_param_tmp['s_period_start'] = dt_first_date
            # dict_param_tmp['s_period_end'] = dt_last_date
            # lst_raw_data = o_db.executeDynamicQuery('getEmartLogByItemId', dict_param_tmp)
            # if len(lst_raw_data) == 0:  # eg., first day of month before latest data appended
            #     lst_raw_data = [{'id': 0, 'item_id': 0, 'branch_id': self.__g_dictSingleBranch['n_branch_id'],
            #                      'qty': 0, 'amnt': 0, 'logdate': dt_first_date}]
            # # set daily raw data
            # df_national_gross = pd.DataFrame(lst_raw_data)
            # # print(df_national_gross)
            # del lst_raw_data

            # del df_national_gross['id']
            del df_national_gross['item_id']
            del df_national_gross['branch_id']
            df_national_gross_sum = df_national_gross.sum()
            del df_national_gross
            dict_national_rst['amnt'].append(df_national_gross_sum['amnt'])
            dict_national_rst['qty'].append(df_national_gross_sum['qty'])

            # greater than the start date and smaller than the end date
            idx_mask = (self.__g_dfPeriodDataRaw['logdate'] >= dt_first_date) & (
                        self.__g_dfPeriodDataRaw['logdate'] <= dt_last_date)
            df_period_data = self.__g_dfPeriodDataRaw.loc[idx_mask]
            del idx_mask
            del df_period_data['item_id']
            df_gross_sum = df_period_data.sum()
            del df_period_data
            dict_branch_rst['amnt'].append(df_gross_sum['amnt'])
            dict_branch_rst['qty'].append(df_gross_sum['qty'])
        # del dict_param_tmp
        del o_edi_raw
        return dict_national_rst, dict_branch_rst

    def retrieve_sku_chronicle(self):
        """
        품목별 현황 테이블 작성
        :return:
        """
        # refer to lm rank if tm is empty
        if self.__g_dictPeriodRankDf['tm']['qty'].sum() == 0 and \
                self.__g_dictPeriodRankDf['tm']['amnt'].sum() == 0:
            df_to_refer = self.__g_dictPeriodRankDf['lm']
        else:
            df_to_refer = self.__g_dictPeriodRankDf['tm']

        dict_top_n = {}
        n_gross_amnt = df_to_refer['amnt'].sum()
        n_gross_qty = df_to_refer['qty'].sum()
        lst_amnt_rank_sku_id = df_to_refer.index.tolist()

        o_emart_sampler = EdiSampler()
        o_emart_sampler.set_period(self.__g_dictPeriod['2ly'][0], self.__g_dictPeriod['tm'][1])
        o_emart_sampler.set_mart_info([self.__g_dictSingleBranch['n_branch_id']])
        for n_sku_id in lst_amnt_rank_sku_id:
            idx_mask = (self.__g_dfPeriodDataRaw['item_id'] == n_sku_id)
            df_single_item = self.__g_dfPeriodDataRaw.loc[idx_mask]
            del idx_mask
            # del df_single_item['branch_id']
            o_emart_sampler.load_data(df_single_item)
            df_sampled = o_emart_sampler.get_sampling('M')
            n_qty = df_to_refer.loc[n_sku_id, 'qty']
            n_amnt = df_to_refer.loc[n_sku_id, 'amnt']
            f_shr_amnt = 0
            f_shr_qty = 0
            if n_gross_amnt > 0:
                f_shr_amnt = int(n_amnt / n_gross_amnt * 100)
            if n_gross_qty > 0:
                f_shr_qty = int(n_qty / n_gross_qty * 100)

            dict_cur_sku_info = self.__g_dictEdiSku[n_sku_id]
            dict_sku = {'hypermart_name': dict_cur_sku_info['hypermart_name'],
                        'item_name': dict_cur_sku_info['name'],
                        's_detected_date': dict_cur_sku_info['first_detect_logdate'].strftime("%Y-%m-%d"),
                        'rank': df_to_refer.loc[n_sku_id, 'rank'],
                        'qty': n_qty, 'amnt': n_amnt,
                        'shr_qty': f_shr_qty,
                        'shr_amnt': f_shr_amnt,
                        'lst_monthly_amnt': df_sampled['amnt'].tolist(),
                        'lst_monthly_qty': df_sampled['qty'].tolist()}
            dict_top_n[n_sku_id] = dict_sku
        del o_emart_sampler
        return dict_top_n

    def add_memo(self, o_sv_db, n_branch_id, n_brand_id, request):
        a_period = request.POST['period_date'].split(' - ')
        s_memo = request.POST['memo']

        # begin - validation
        # try:
        #    dict_budget_info = self.__g_dictBudgetType[s_budget_title]
        # except KeyError:
        #    return False
        # <script>console.log('dd')</script>
        s_memo = strip_tags(s_memo).strip()
        if len(s_memo) == 0:
            return False
        try:
            dt_memo_date_begin = datetime.strptime(a_period[0], '%m/%d/%Y')
        except ValueError:
            return False

        try:
            dt_memo_date_end = datetime.strptime(a_period[1], '%m/%d/%Y')
        except ValueError:
            return False
        # end - validation
        o_sv_db.executeQuery('insertBranchMemo', request.user.pk, n_brand_id, n_branch_id, s_memo,
                             dt_memo_date_begin, dt_memo_date_end)
        return {}
