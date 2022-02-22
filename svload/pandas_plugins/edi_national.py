from datetime import date
from .edi_tools import EdiRanker, EdiSampler

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


# pd.set_option('display.max_columns', None) 후에 print(df)하면 모든 컬럼명이 출력됨
class Performance:
    __g_dictPeriod = {'2ly': [], 'ly': [], 'lm': [], 'tm': []}
    __g_dictEdiSku = {}  # SKU 정보 저장
    __g_dictEdiBranchId = []  # 추출할 매장 고유 번호 저장
    __g_sBranchFilterMode = None
    __g_lstSelectedBranch = []
    __g_dfPeriodDataRaw = None
    __g_dictPeriodBranchRankDf = {'2ly': None, 'ly': None, 'lm': None, 'tm': None}  # 매장 순위
    __g_dictPeriodSkuRankDf = {'2ly': None, 'ly': None, 'lm': None, 'tm': None}  # 품목 순위

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

    def set_sku_dict(self, dict_sku):
        if len(dict_sku) == 0:
            self.__g_dictEdiSku = {1: {'hypermart_name': 'Emart', 'selected': '', 'mart_id': 3, 'name': 'none', 'first_detect_logdate': date.today()}}  # raise Exception('invalid dict_sku')
        else:
            self.__g_dictEdiSku = dict_sku

    def set_all_branches(self, dict_branch_info):
        if len(dict_branch_info['dict_branch_info_for_ui']):
            self.__g_dictEdiBranchId = dict_branch_info['dict_branch_info_for_ui']
        else:
            raise Exception('excel extraction failure - no branch info')

        self.__g_sBranchFilterMode = dict_branch_info['s_branch_filter_mode']
        if self.__g_sBranchFilterMode == 'exc' or self.__g_sBranchFilterMode == 'inc':
            self.__g_lstSelectedBranch.clear()
            for n_branch_id, dict_branch in self.__g_dictEdiBranchId.items():
                if dict_branch['selected'] == 'selected':
                    self.__g_lstSelectedBranch.append(dict_branch['id'])
        # self.__g_dfEdiBranchId = pd.DataFrame(dict_all_branch_info_by_id).transpose()

    def load_df(self, df_edi_raw):
        if len(df_edi_raw.index) == 0:
            raise Exception('invalid data frame')

        self.__g_dfPeriodDataRaw = df_edi_raw
        o_monthly_rank_branch = EdiRanker()
        lst_edi_branch_id = list(self.__g_dictEdiBranchId.keys())
        for s_period in self.__g_dictPeriod:
            dt_first_date = self.__g_dictPeriod[s_period][0]
            dt_last_date = self.__g_dictPeriod[s_period][1]
            idx_mask = (self.__g_dfPeriodDataRaw['logdate'] >= dt_first_date) & (
                    self.__g_dfPeriodDataRaw['logdate'] <= dt_last_date)
            df_period_data = self.__g_dfPeriodDataRaw.loc[idx_mask]
            del idx_mask
            o_monthly_rank_branch.set_period(dt_first_date, dt_last_date)
            o_monthly_rank_branch.set_mart_info(lst_edi_branch_id)
            o_monthly_rank_branch.load_data(df_period_data)
            self.__g_dictPeriodBranchRankDf[s_period] = o_monthly_rank_branch.get_rank('branch_id', 'amnt')
        del lst_edi_branch_id
        del o_monthly_rank_branch

        o_monthly_rank_sku = EdiRanker()
        lst_edi_sku_id = list(self.__g_dictEdiSku.keys())
        for s_period in self.__g_dictPeriod:
            dt_first_date = self.__g_dictPeriod[s_period][0]
            dt_last_date = self.__g_dictPeriod[s_period][1]
            idx_mask = (self.__g_dfPeriodDataRaw['logdate'] >= dt_first_date) & (
                    self.__g_dfPeriodDataRaw['logdate'] <= dt_last_date)
            df_period_data = self.__g_dfPeriodDataRaw.loc[idx_mask]
            del idx_mask
            # exc inc filter 적용 https://stackoverflow.com/questions/23745677/filtering-pandas-data-frame-by-a-list-of-ids
            if self.__g_sBranchFilterMode == 'exc':
                idx_mask = df_period_data['branch_id'].isin(self.__g_lstSelectedBranch)
                df_period_data = df_period_data.loc[~idx_mask]
                del idx_mask
            elif self.__g_sBranchFilterMode == 'inc':
                idx_mask = df_period_data['branch_id'].isin(self.__g_lstSelectedBranch)
                df_period_data = df_period_data.loc[idx_mask]
                del idx_mask
            elif self.__g_sBranchFilterMode == '':
                pass
            o_monthly_rank_sku.set_period(dt_first_date, dt_last_date)
            o_monthly_rank_sku.set_sku_info(lst_edi_sku_id)
            o_monthly_rank_sku.load_data(df_period_data)
            self.__g_dictPeriodSkuRankDf[s_period] = o_monthly_rank_sku.get_rank('item_id', 'amnt')
        del lst_edi_sku_id
        del o_monthly_rank_sku

    def retrieve_branch_gross_in_period(self):
        """
        표시된 매장 총공급액 & 총출고량 추이 js 막대 그래프
        :return:
        """
        lst_gross_amnt = []  # 2ly, ly, lm, tm
        lst_gross_amnt_ratio = []  # 2ly, ly, lm, tm
        lst_gross_qty = []  # 2ly, ly, lm, tm
        lst_gross_qty_ratio = []  # 2ly, ly, lm, tm
        dict_final = {}
        # exc inc filter 적용 https://stackoverflow.com/questions/23745677/filtering-pandas-data-frame-by-a-list-of-ids
        if self.__g_sBranchFilterMode == 'exc':
            for s_period, df_rank in self.__g_dictPeriodBranchRankDf.items():
                idx_mask = df_rank.index.isin(self.__g_lstSelectedBranch)
                dict_final[s_period] = df_rank.loc[~idx_mask]
                del idx_mask
        elif self.__g_sBranchFilterMode == 'inc':
            for s_period, df_rank in self.__g_dictPeriodBranchRankDf.items():
                idx_mask = df_rank.index.isin(self.__g_lstSelectedBranch)
                dict_final[s_period] = df_rank.loc[idx_mask]
                del idx_mask
        elif self.__g_sBranchFilterMode == '':
            dict_final = self.__g_dictPeriodBranchRankDf

        # construct final data to display
        for s_period, df_rank in dict_final.items():
            lst_gross_amnt.append(df_rank['amnt'].sum())
            lst_gross_qty.append(df_rank['qty'].sum())

        lst_gross_amnt_ratio.append(0)  # 2ly는 변화율 계산 불가능
        for n in range(1, len(lst_gross_amnt)):
            f_gross_amnt_ratio = 0
            if lst_gross_amnt[n - 1] > 0:
                f_gross_amnt_ratio = (lst_gross_amnt[n] - lst_gross_amnt[n - 1]) / lst_gross_amnt[n - 1] * 100
            lst_gross_amnt_ratio.append(f_gross_amnt_ratio)

        lst_gross_qty_ratio.append(0)  # 2ly는 변화율 계산 불가능
        for n in range(1, len(lst_gross_qty)):
            f_gross_qty_ratio = 0
            if lst_gross_qty[n - 1] > 0:
                f_gross_qty_ratio = (lst_gross_qty[n] - lst_gross_qty[n - 1]) / lst_gross_qty[n - 1] * 100
            lst_gross_qty_ratio.append(f_gross_qty_ratio)

        # lst_gross_amnt_ratio = [(lst_gross_amnt[n] - lst_gross_amnt[n - 1]) / lst_gross_amnt[n - 1] * 100 for n in
        #                        range(1, len(lst_gross_amnt))]
        # lst_gross_amnt_ratio.insert(0, 0)   # 2ly는 변화율 계산 불가능
        # lst_gross_qty_ratio = [(lst_gross_qty[n] - lst_gross_qty[n - 1]) / lst_gross_qty[n - 1] * 100 for n in
        #                       range(1, len(lst_gross_qty))]
        # lst_gross_qty_ratio.insert(0, 0)  # 2ly는 변화율 계산 불가능
        return {'amnt': lst_gross_amnt, 'amnt_ratio': lst_gross_amnt_ratio, 'qty': lst_gross_qty, 'qty_ratio': lst_gross_qty_ratio}

    def retrieve_branch_rank_in_period(self):
        b_refer_lm_rank = False  # refer to lm rank if tm is empty
        if self.__g_dictPeriodBranchRankDf['tm']['qty'].sum() == 0 and self.__g_dictPeriodBranchRankDf['tm'][
            'amnt'].sum() == 0:
            b_refer_lm_rank = True
            lst_amnt_rank_branch_id = self.__g_dictPeriodBranchRankDf['lm'].index.tolist()
        else:
            lst_amnt_rank_branch_id = self.__g_dictPeriodBranchRankDf['tm'].index.tolist()
        lst_branch_data_table = []

        # begin - apply branch filter
        if self.__g_sBranchFilterMode == 'exc':
            lst_amnt_rank_branch_id = [item for item in lst_amnt_rank_branch_id if
                                       item not in self.__g_lstSelectedBranch]
        elif self.__g_sBranchFilterMode == 'inc':
            lst_amnt_rank_branch_id = self.__g_lstSelectedBranch
        # end - apply branch filter

        for n_branch_id in lst_amnt_rank_branch_id:
            if b_refer_lm_rank:
                n_rank_tm = self.__g_dictPeriodBranchRankDf['lm'].loc[n_branch_id, 'rank']
            else:
                n_rank_tm = self.__g_dictPeriodBranchRankDf['tm'].loc[n_branch_id, 'rank']
            dict_branch_row = {'s_hypermart_name': self.__g_dictEdiBranchId[n_branch_id]['hypermart_name'],
                               's_branch_name': self.__g_dictEdiBranchId[n_branch_id]['name'],
                               's_do_name': self.__g_dictEdiBranchId[n_branch_id]['do_name'],
                               's_si_name': self.__g_dictEdiBranchId[n_branch_id]['si_name'],
                               'n_branch_id': n_branch_id,
                               'n_rank_2ly': self.__g_dictPeriodBranchRankDf['2ly'].loc[n_branch_id, 'rank'],
                               'n_rank_ly': self.__g_dictPeriodBranchRankDf['ly'].loc[n_branch_id, 'rank'],
                               'n_rank_lm': self.__g_dictPeriodBranchRankDf['lm'].loc[n_branch_id, 'rank'],
                               'n_rank_tm': n_rank_tm,
                               'n_amnt_2ly': self.__g_dictPeriodBranchRankDf['2ly'].loc[n_branch_id, 'amnt'],
                               'n_amnt_ly': self.__g_dictPeriodBranchRankDf['ly'].loc[n_branch_id, 'amnt'],
                               'n_amnt_lm': self.__g_dictPeriodBranchRankDf['lm'].loc[n_branch_id, 'amnt'],
                               'n_amnt_tm': self.__g_dictPeriodBranchRankDf['tm'].loc[n_branch_id, 'amnt'],
                               'n_qty_2ly': self.__g_dictPeriodBranchRankDf['2ly'].loc[n_branch_id, 'qty'],
                               'n_qty_ly': self.__g_dictPeriodBranchRankDf['ly'].loc[n_branch_id, 'qty'],
                               'n_qty_lm': self.__g_dictPeriodBranchRankDf['lm'].loc[n_branch_id, 'qty'],
                               'n_qty_tm': self.__g_dictPeriodBranchRankDf['tm'].loc[n_branch_id, 'qty']}
            lst_branch_data_table.append(dict_branch_row)
        return lst_branch_data_table

    def retrieve_sku_rank_in_period(self):
        """
        당월 공급액 순위, 당월 출고량 비교 그래프 데이터
        :return:
        """
        lst_bar_color = ['#D6E2DF', '#A4C8C1', '#6CBDAC', '#079476']
        # refer to lm rank if tm is empty
        if self.__g_dictPeriodSkuRankDf['tm']['qty'].sum() == 0 and self.__g_dictPeriodSkuRankDf['tm'][
            'amnt'].sum() == 0:
            lst_amnt_rank_sku_id = self.__g_dictPeriodSkuRankDf['lm'].index.tolist()
        else:
            lst_amnt_rank_sku_id = self.__g_dictPeriodSkuRankDf['tm'].index.tolist()

        lst_sku_name = []
        lst_2ly_sku_by_tm_amnt = []
        lst_lm_sku_by_tm_amnt = []
        lst_ly_sku_by_tm_amnt = []
        lst_tm_sku_by_tm_amnt = []
        lst_2ly_sku_by_tm_qty = []
        lst_ly_sku_by_tm_qty = []
        lst_lm_sku_by_tm_qty = []
        lst_tm_sku_by_tm_qty = []
        for n_sku_id in lst_amnt_rank_sku_id:
            lst_sku_name.append(self.__g_dictEdiSku[n_sku_id]['name'])
            lst_2ly_sku_by_tm_amnt.append(self.__g_dictPeriodSkuRankDf['2ly'].loc[n_sku_id, 'amnt'])
            lst_ly_sku_by_tm_amnt.append(self.__g_dictPeriodSkuRankDf['ly'].loc[n_sku_id, 'amnt'])
            lst_lm_sku_by_tm_amnt.append(self.__g_dictPeriodSkuRankDf['lm'].loc[n_sku_id, 'amnt'])
            lst_tm_sku_by_tm_amnt.append(self.__g_dictPeriodSkuRankDf['tm'].loc[n_sku_id, 'amnt'])
            lst_2ly_sku_by_tm_qty.append(self.__g_dictPeriodSkuRankDf['2ly'].loc[n_sku_id, 'qty'])
            lst_ly_sku_by_tm_qty.append(self.__g_dictPeriodSkuRankDf['ly'].loc[n_sku_id, 'qty'])
            lst_lm_sku_by_tm_qty.append(self.__g_dictPeriodSkuRankDf['lm'].loc[n_sku_id, 'qty'])
            lst_tm_sku_by_tm_qty.append(self.__g_dictPeriodSkuRankDf['tm'].loc[n_sku_id, 'qty'])
        return {'lst_bar_color': lst_bar_color,
                'lst_sku_name_by_tm_amnt': lst_sku_name,
                'lst_2ly_sku_by_tm_amnt': lst_2ly_sku_by_tm_amnt,
                'lst_ly_sku_by_tm_amnt': lst_ly_sku_by_tm_amnt,
                'lst_lm_sku_by_tm_amnt': lst_lm_sku_by_tm_amnt,
                'lst_tm_sku_by_tm_amnt': lst_tm_sku_by_tm_amnt,
                'lst_2ly_sku_by_tm_qty': lst_2ly_sku_by_tm_qty,
                'lst_ly_sku_by_tm_qty': lst_ly_sku_by_tm_qty,
                'lst_lm_sku_by_tm_qty': lst_lm_sku_by_tm_qty,
                'lst_tm_sku_by_tm_qty': lst_tm_sku_by_tm_qty}

    def retrieve_top_sku_chronicle(self, n_th_rank=10):
        """
        품목별 현황 테이블 작성
        :param n_th_rank:
        :return:
        """
        # refer to lm rank if tm is empty
        if self.__g_dictPeriodSkuRankDf['tm']['qty'].sum() == 0 and \
                self.__g_dictPeriodSkuRankDf['tm']['amnt'].sum() == 0:
            df_to_refer = self.__g_dictPeriodSkuRankDf['lm']
        else:
            df_to_refer = self.__g_dictPeriodSkuRankDf['tm']

        dict_top_n = {}
        n_gross_amnt = df_to_refer['amnt'].sum()
        n_gross_qty = df_to_refer['qty'].sum()
        lst_amnt_rank_sku_id = df_to_refer.index[:n_th_rank].tolist()

        o_edi_sampler = EdiSampler()
        lst_edi_branch_id = list(self.__g_dictEdiBranchId.keys())

        o_edi_sampler.set_period(self.__g_dictPeriod['2ly'][0], self.__g_dictPeriod['tm'][1])
        o_edi_sampler.set_mart_info(lst_edi_branch_id)
        del lst_edi_branch_id
        for n_sku_id in lst_amnt_rank_sku_id:
            idx_mask = (self.__g_dfPeriodDataRaw['item_id'] == n_sku_id)
            df_single_item = self.__g_dfPeriodDataRaw.loc[idx_mask]
            del idx_mask
            del df_single_item['branch_id']
            o_edi_sampler.load_data(df_single_item)
            df_sampled = o_edi_sampler.get_sampling('M')
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
        del o_edi_sampler

        n_sku_dashboard_div_height_px = 90 + len(dict_top_n) * 53  # div px height for table
        return dict_top_n, n_sku_dashboard_div_height_px
