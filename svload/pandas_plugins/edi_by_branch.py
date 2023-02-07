# import pandas as pd
import pandas as pd
from django.utils.html import strip_tags
from datetime import datetime
from datetime import date
from svcommon.sv_hypermart_model import SvHypermartGeoInfo
from svcommon.sv_hypermart_model import SvHyperMartType
from .edi_tools import EdiRanker
from .edi_tools import EdiSampler

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


# pd.set_option('display.max_columns', None) 후에 print(df)하면 모든 컬럼명이 출력됨
class EdiBranchPerformance:
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
        # execute this method just after set_branch_info
        if len(dict_sku) == 0:
            s_mart_name = self.__g_dictSingleBranch['hypermart_name']
            n_mart_id = self.__g_dictSingleBranch['n_hypermart_id']
            self.__g_dictEdiSku = {1: {'hypermart_name': s_mart_name, 'selected': '', 'mart_id': n_mart_id, 'name': 'none', 'first_detect_logdate': date.today()}}  # raise Exception('invalid dict_sku')
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
        o_edi_raw = EdiBranchRaw()
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

    def add_memo(self, o_sv_db, n_branch_id, request):
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
        o_sv_db.executeQuery('insertBranchMemo', request.user.pk, n_branch_id, s_memo,
                                dt_memo_date_begin, dt_memo_date_end)
        return {}


class EdiBranchRaw:
    __g_dtDesignatedFirstDate = None  # 추출 기간 시작일
    __g_dtDesignatedLastDate = None  # 추출 기간 종료일
    __g_sFreqMode = None
    __g_dictEdiBranchId = None
    __g_dictEdiSku = None
    __g_oSvDb = None

    # def __new__(cls):
    #    # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
    #    return super().__new__(cls)

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

    def set_period_dict(self, dt_start, dt_end):
        # if not dict_period['dt_first_day_2year_ago'] or not dict_period['dt_today']:
        #    raise Exception('invalid data period')
        self.__g_dtDesignatedFirstDate = dt_start  # dict_period['dt_first_day_2year_ago']
        self.__g_dtDesignatedLastDate = dt_end  # dict_period['dt_today']

    def set_freq(self, dict_freq):
        for s_freq, b_activated in dict_freq.items():
            if b_activated:
                self.__g_sFreqMode = s_freq
                break

    def set_branch_info(self, o_branch_info):
        if type(o_branch_info) == dict:
            # print('multi branch')
            if len(o_branch_info['dict_branch_info_for_ui']):
                self.__g_dictEdiBranchId = o_branch_info['dict_branch_info_for_ui']
            else:
                raise Exception('excel extraction failure - no branch info')
        # elif type(o_branch_info) == SvHypermartGeoInfo:
        #     # print('single branch')
        #     self.__g_dictEdiBranchId = {o_branch_info.id: {'id': o_branch_info.id,
        #                                                    'selected': '',
        #                                                    'hypermart_name': o_branch_info.get_hypermart_type_label(),
        #                                                    'name': o_branch_info.branch_name,
        #                                                    'branch_type': o_branch_info.get_branch_type_label(),
        #                                                    'do_name': o_branch_info.do_name,
        #                                                    'si_name': o_branch_info.si_name,
        #                                                    'gu_gun': o_branch_info.gu_gun,
        #                                                    'dong_myun_ri': o_branch_info.dong_myun_ri}
        #                                 }

    def set_sku_dict(self, dict_sku):
        # execute this method just after set_branch_info
        if len(dict_sku) == 0:
            if self.__g_dictEdiBranchId is not None:
                dict_single_branch = list(self.__g_dictEdiBranchId.values())[0]
                s_mart_name = dict_single_branch['hypermart_name']
                n_mart_id = dict_single_branch['hypermart_id']
                del dict_single_branch
            else:
                s_mart_name = 'hypermart_name'
                n_mart_id = 'hypermart_id'
            self.__g_dictEdiSku = {1: {'hypermart_name': s_mart_name, 'selected': '', 'mart_id': n_mart_id, 'name': 'none', 'first_detect_logdate': date.today()}}
        else:
            self.__g_dictEdiSku = dict_sku
        # self.__g_dfEdiSku = pd.DataFrame(dict_sku).transpose()

    def load_sch_gross_by_item_id_list(self, o_db, n_sch_id):
        if not o_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self.__g_oSvDb = o_db

        # retrieve sku id list, convert int id to string id
        dict_param_tmp = {'s_period_start': self.__g_dtDesignatedFirstDate,
                          's_period_end': self.__g_dtDesignatedLastDate}

        if n_sch_id == SvHyperMartType.EMART.value:
            lst_sch_raw_data = self.__load_emart_edi(dict_param_tmp)
        elif n_sch_id == SvHyperMartType.LOTTEMART.value:
            lst_sch_raw_data = self.__load_ltmart_edi(dict_param_tmp)
        else:
            lst_sch_raw_data = []

        if len(lst_sch_raw_data) == 0:
            df_period_data_raw = self.__set_nullify_dataframe()
        else:
            df_period_data_raw = pd.DataFrame(lst_sch_raw_data)
        del lst_sch_raw_data

        # ensure logdate field to datetime format
        df_period_data_raw['logdate'] = pd.to_datetime(df_period_data_raw['logdate'])
        return df_period_data_raw

    def load_national(self, o_db):
        if not o_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self.__g_oSvDb = o_db

        if self.__g_dtDesignatedFirstDate is None or self.__g_dtDesignatedLastDate is None:
            raise Exception('not ready to load dataframe to analyze')

        # retrieve sku id list, convert int id to string id
        # lst_edi_sku_id = [str(n_sku_id) for n_sku_id in list(self.__g_dictEdiSku.keys())]
        # dict_param_tmp = {'s_req_sku_set': ','.join(lst_edi_sku_id), 's_period_start': self.__g_dtDesignatedFirstDate,
        #                   's_period_end': self.__g_dtDesignatedLastDate}
        dict_param_tmp = {'s_period_start': self.__g_dtDesignatedFirstDate,
                          's_period_end': self.__g_dtDesignatedLastDate}

        dict_hypermart_type = SvHyperMartType.get_dict_by_idx()
        del dict_hypermart_type[1]  # remove ESTIMATION
        del dict_hypermart_type[2]  # remove NOT_SURE
        lst_extract_hypermart_type = []
        for n_hypermart_id in dict_hypermart_type:  # .keys():
            lst_raw_data = self.__g_oSvDb.executeQuery('getEdiSkuCountByMartId', n_hypermart_id)
            if lst_raw_data and 'err_code' in lst_raw_data[0].keys():  # for an initial stage; no table
                lst_raw_data = []
            if len(lst_raw_data):
                if lst_raw_data[0]['count(*)'] > 0:
                    lst_extract_hypermart_type.append(n_hypermart_id)

        # begin - Emart
        if SvHyperMartType.EMART.value in lst_extract_hypermart_type:
            lst_raw_data_emart = self.__load_emart_edi(dict_param_tmp)
        else:
            lst_raw_data_emart = []
        # lst_raw_data_emart = []
        # if SvHyperMartType.EMART.value in lst_extract_hypermart_type:
        #    lst_raw_data_emart = o_db.executeDynamicQuery('getEmartLogByItemId', dict_param_tmp)
        # end - Emart

        # begin - Lotte mart
        if SvHyperMartType.LOTTEMART.value in lst_extract_hypermart_type:
            lst_raw_data_ltmart = self.__load_ltmart_edi(dict_param_tmp)
        else:
            lst_raw_data_ltmart = []
        # lst_raw_data_ltmart = []
        # if SvHyperMartType.LOTTEMART.value in lst_extract_hypermart_type:
        #    if self.__g_sFreqMode == 'day':
        #        lst_raw_data_ltmart = o_db.executeDynamicQuery('getLtmartLogByItemIdLogdateSince', dict_param_tmp)
        #        lst_raw_data_ltmart_daily_allocated = []
        #        for dict_single_row in lst_raw_data_ltmart:
        #            lst_date_range = list(pd.date_range(dict_single_row['logdate_since'], dict_single_row['logdate']))
        #            n_date_cnt = len(lst_date_range)
        #            if dict_single_row['qty'] >= n_date_cnt:  # 기간일수로 분할한 수량 결과가 1 이상이면
        #                n_allocated_qty = int(dict_single_row['qty'] / n_date_cnt)
        #                n_allocated_amnt = int(dict_single_row['amnt'] / n_date_cnt)
        #                for log_date in lst_date_range:
        #                    lst_raw_data_ltmart_daily_allocated.append({'item_id': dict_single_row['item_id'],
        #                                                                'branch_id': dict_single_row['branch_id'],
        #                                                                'qty': n_allocated_qty,
        #                                                                'amnt': n_allocated_amnt,
        #                                                                'logdate': log_date.to_pydatetime()})
        #            else:  # 기간일수로 분할한 수량 결과가 1 이하이면 분해 하지 않고 마지막 로그일에 배정
        #                lst_raw_data_ltmart_daily_allocated.append({'item_id': dict_single_row['item_id'],
        #                                                            'branch_id': dict_single_row['branch_id'],
        #                                                            'qty': dict_single_row['qty'],
        #                                                            'amnt': dict_single_row['amnt'],
        #                                                            'logdate': dict_single_row['logdate']})
        #        lst_raw_data_ltmart = lst_raw_data_ltmart_daily_allocated
        #        del lst_raw_data_ltmart_daily_allocated
        #    else:
        #        lst_raw_data_ltmart = o_db.executeDynamicQuery('getLtmartLogByItemId', dict_param_tmp)
        # end - Lotte mart
        national_raw_data = lst_raw_data_emart + lst_raw_data_ltmart
        del lst_raw_data_emart
        del lst_raw_data_ltmart
        if len(national_raw_data) == 0:
            df_period_data_raw = self.__set_nullify_dataframe()
        else:
            df_period_data_raw = pd.DataFrame(national_raw_data)
        # ensure logdate field to datetime format
        if not df_period_data_raw.empty:  # for an initial stage; no table
            df_period_data_raw['logdate'] = pd.to_datetime(df_period_data_raw['logdate'])
        return df_period_data_raw

    def load_branch(self, o_db):
        """
        assume single branch mode
        :param o_db:
        :return:
        """
        if not o_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        if self.__g_dtDesignatedFirstDate is None or self.__g_dtDesignatedLastDate is None:
            raise Exception('not ready to load dataframe to analyze')

        self.__g_oSvDb = o_db
        dict_single_branch_info = list(self.__g_dictEdiBranchId.values())[0]
        # retrieve sku id list, convert int id to string id
        n_hypermart_id = dict_single_branch_info['hypermart_id']
        if n_hypermart_id == SvHyperMartType.EMART.value:
            lst_branch_raw_data = self.__g_oSvDb.executeQuery('getEmartLogByBranchId', self.__g_dtDesignatedFirstDate,
                                                                self.__g_dtDesignatedLastDate, dict_single_branch_info['id'])
        elif n_hypermart_id == SvHyperMartType.LOTTEMART.value:
            lst_branch_raw_data = self.__g_oSvDb.executeQuery('getLtmartLogByBranchId', self.__g_dtDesignatedFirstDate,
                                                                self.__g_dtDesignatedLastDate, dict_single_branch_info['id'])
            if self.__g_sFreqMode == 'day':
                print('regnerate daily data')

        if len(lst_branch_raw_data) == 0:
            df_period_data_raw = self.__set_nullify_dataframe()
        else:
            df_period_data_raw = pd.DataFrame(lst_branch_raw_data)

        # unset unnecessary field
        if 'branch_id' in df_period_data_raw:
            del df_period_data_raw['branch_id']
        # ensure logdate field to datetime format
        if 'logdate' in df_period_data_raw:
            df_period_data_raw['logdate'] = pd.to_datetime(df_period_data_raw['logdate'])
        return df_period_data_raw

    def load_sku(self, o_db):
        """
        assume single sku mode
        :param o_db:
        :return:
        """
        if not o_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self.__g_oSvDb = o_db

        if self.__g_dtDesignatedFirstDate is None or self.__g_dtDesignatedLastDate is None:
            raise Exception('not ready to load dataframe to analyze')

        dict_single_sku_info = list(self.__g_dictEdiSku.values())[0]
        n_hypermart_id = dict_single_sku_info['mart_id']
        if n_hypermart_id == SvHyperMartType.EMART.value:
            lst_raw_data = self.__g_oSvDb.executeQuery('getEmartLogSingleItemId', str(next(iter(self.__g_dictEdiSku))),
                                                       self.__g_dtDesignatedFirstDate, self.__g_dtDesignatedLastDate)
        elif n_hypermart_id == SvHyperMartType.LOTTEMART.value:
            lst_raw_data = self.__g_oSvDb.executeQuery('getLtmartLogSingleItemId', str(next(iter(self.__g_dictEdiSku))),
                                                       self.__g_dtDesignatedFirstDate, self.__g_dtDesignatedLastDate)
            if self.__g_sFreqMode == 'day':
                print('regnerate daily data')

        if len(lst_raw_data) == 0:
            df_period_data_raw = self.__set_nullify_dataframe()
        else:
            df_period_data_raw = pd.DataFrame(lst_raw_data)

        # ensure logdate field to datetime format
        df_period_data_raw['logdate'] = pd.to_datetime(df_period_data_raw['logdate'])
        return df_period_data_raw

    def __set_nullify_dataframe(self):
        # divide branch by mart
        lst_emart_branch_id = []
        lst_ltmart_branch_id = []
        for n_branch_id, dict_branch in self.__g_dictEdiBranchId.items():
            if dict_branch['hypermart_name'] == SvHyperMartType.EMART.name.title():
                lst_emart_branch_id.append(n_branch_id)
            elif dict_branch['hypermart_name'] == SvHyperMartType.LOTTEMART.name.title():
                lst_ltmart_branch_id.append(n_branch_id)
        # divide sku by mart
        lst_emart_sku_id = []
        lst_ltmart_sku_id = []
        for n_sku_id, dict_sku in self.__g_dictEdiSku.items():
            if dict_sku['mart_id'] == SvHyperMartType.EMART.value:
                lst_emart_sku_id.append(n_sku_id)
            elif dict_sku['mart_id'] == SvHyperMartType.LOTTEMART.value:
                lst_ltmart_sku_id.append(n_sku_id)
        lst_blank_edi = []
        # set emart null
        for n_branch_id in lst_emart_branch_id:
            for n_sku_id in lst_emart_sku_id:
                lst_blank_edi.append([n_sku_id, n_branch_id, 0, 0, self.__g_dtDesignatedFirstDate])
        # set lottemart null
        for n_branch_id in lst_ltmart_branch_id:
            for n_sku_id in lst_ltmart_sku_id:
                lst_blank_edi.append([n_sku_id, n_branch_id, 0, 0, self.__g_dtDesignatedFirstDate])

        df_blank = pd.DataFrame(lst_blank_edi)
        df_blank.rename(columns={0: 'item_id', 1: 'branch_id', 2: 'qty', 3: 'amnt', 4: 'logdate'}, inplace=True)
        del lst_blank_edi
        return df_blank

    def __load_emart_edi(self, dict_param):
        """
        이마트 EDI Raw 가져오기
        :param dict_param:
        :return:
        """
        if 's_period_start' not in dict_param and 's_period_end' not in dict_param:
            return []
        return self.__g_oSvDb.executeQuery('getEmartLogByPeriod', dict_param['s_period_start'], dict_param['s_period_end'])

    def __load_ltmart_edi(self, dict_param):
        """
        롯데마트 EDI Raw 가져오기
        :param dict_param:
        :return:
        """
        if 's_period_start' not in dict_param and 's_period_end' not in dict_param:
            return []

        if self.__g_sFreqMode == 'day':
            lst_raw_data_ltmart = self.__g_oSvDb.executeQuery('getLtmartLogWithLogdateSince', dict_param['s_period_start'], dict_param['s_period_end'])
            lst_raw_data_ltmart_daily_allocated = []
            for dict_single_row in lst_raw_data_ltmart:
                lst_date_range = list(pd.date_range(dict_single_row['logdate_since'], dict_single_row['logdate']))
                n_date_cnt = len(lst_date_range)
                if dict_single_row['qty'] >= n_date_cnt:  # 기간일수로 분할한 수량 결과가 1 이상이면
                    n_allocated_qty = int(dict_single_row['qty'] / n_date_cnt)
                    n_allocated_amnt = int(dict_single_row['amnt'] / n_date_cnt)
                    for log_date in lst_date_range:
                        lst_raw_data_ltmart_daily_allocated.append({'item_id': dict_single_row['item_id'],
                                                                    'branch_id': dict_single_row['branch_id'],
                                                                    'qty': n_allocated_qty,
                                                                    'amnt': n_allocated_amnt,
                                                                    'logdate': log_date.to_pydatetime()})
                else:  # 기간일수로 분할한 수량 결과가 1 이하이면 분해 하지 않고 마지막 로그일에 배정
                    lst_raw_data_ltmart_daily_allocated.append({'item_id': dict_single_row['item_id'],
                                                                'branch_id': dict_single_row['branch_id'],
                                                                'qty': dict_single_row['qty'],
                                                                'amnt': dict_single_row['amnt'],
                                                                'logdate': dict_single_row['logdate']})
            return lst_raw_data_ltmart_daily_allocated
        else:
            return self.__g_oSvDb.executeQuery('getLtmartLogByPeriod', dict_param['s_period_start'], dict_param['s_period_end'])
