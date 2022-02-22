from datetime import datetime
import pandas as pd

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class EdiSampler:
    __g_dtDesignatedFirstDate = None  # 사용자가 지정한 기간 시작일
    __g_dtDesignatedLastDate = None  # 사용자가 지정한 기간 종료일
    __g_dfAnalyze = None
    __g_lstBranchId = None
    __g_lstSkuId = None

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

    def set_period(self, dt_start, dt_end):
        if isinstance(dt_start, datetime) and isinstance(dt_end, datetime):
            self.__g_dtDesignatedFirstDate = dt_start
            self.__g_dtDesignatedLastDate = dt_end
        else:
            raise Exception('invalid date')

    def set_mart_info(self, lst_branch_id=None):
        """
        :param s_mart_type:
        :param lst_branch_id: load_data()에 empty df가 입력될 경우 적절하게 변환된 df를 반환하기 위해 필요함
        :return:
        """
        if lst_branch_id:
            if len(lst_branch_id) > 0:
                self.__g_lstBranchId = lst_branch_id
        else:
            pass  # item-wise  # raise Exception('invalid mart branch id list')

    def set_sku_info(self, lst_sku_id):
        if len(lst_sku_id) > 0:
            self.__g_lstSkuId = lst_sku_id
            # self.__g_lstBranchId = None
        else:
            raise Exception('invalid sku list')

    def load_data(self, df_raw):
        if len(df_raw.index) == 0:
            self.__g_dfAnalyze = self.__set_nullify_dataframe()
        else:
            self.__g_dfAnalyze = df_raw

        # reset index for resampling
        self.__g_dfAnalyze.set_index('logdate', inplace=True)

    def get_sampling(self, s_freq='D'):
        # to fill out empty element
        idx_full_period = pd.date_range(start=self.__g_dtDesignatedFirstDate, end=self.__g_dtDesignatedLastDate,
                                        freq=s_freq)
        df_resample = self.__g_dfAnalyze.resample(s_freq).sum()
        return df_resample.reindex(idx_full_period, fill_value=0)

    def __set_nullify_dataframe(self):  # should be global method
        lst_qty = []
        lst_amnt = []
        lst_logdate = []
        if self.__g_lstBranchId:
            lst_to_iter = self.__g_lstBranchId
            s_anal_wise = 'branch_id'
        elif self.__g_lstSkuId:
            lst_to_iter = self.__g_lstSkuId
            s_anal_wise = 'item_id'
        else:
            raise Exception('invalid analytical wise')
        # if len(lst_raw_data) == 0:  # eg., first day of month before latest data appended
        #                     lst_raw_data = []
        #                     for n_branch_id, dict_branch in self.__g_dictEmartBranchId.items():
        #                         lst_raw_data.append({'id': 0, 'item_id': 0, 'branch_id': n_branch_id,
        #                                              'qty': 0, 'amnt': 0, 'logdate': self.__g_dictPeriod['tm'][0]}
        for n_dummy in lst_to_iter:
            lst_qty.append(0)
            lst_amnt.append(0)
            lst_logdate.append(self.__g_dtDesignatedFirstDate)
        df = pd.DataFrame([x for x in zip(lst_to_iter, lst_qty, lst_amnt, lst_logdate)])
        df.rename(columns={0: s_anal_wise, 1: 'qty', 2: 'amnt', 3: 'logdate'}, inplace=True)
        return df


class EdiRanker:
    __g_dtDesignatedFirstDate = None  # 사용자가 지정한 기간 시작일
    __g_dtDesignatedLastDate = None  # 사용자가 지정한 기간 종료일
    __g_lstBranchId = None
    __g_lstSkuId = None
    __g_dfAnalyze = None

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

    def set_period(self, dt_start, dt_end):
        if isinstance(dt_start, datetime) and isinstance(dt_end, datetime):
            self.__g_dtDesignatedFirstDate = dt_start
            self.__g_dtDesignatedLastDate = dt_end
        else:
            raise Exception('invalid date')

    def set_mart_info(self, lst_branch_id=None):
        """
        :param lst_branch_id: load_data()에 empty df가 입력될 경우 적절하게 변환된 df를 반환하기 위해 필요함
        :return:
        """
        if lst_branch_id:
            if len(lst_branch_id) > 0:
                self.__g_lstBranchId = lst_branch_id
        else:
            pass  # item-wise  # raise Exception('invalid mart branch id list')

    def set_sku_info(self, lst_sku_id):
        if len(lst_sku_id) > 0:
            self.__g_lstSkuId = lst_sku_id
        else:
            raise Exception('invalid sku list')

    def load_data(self, df_raw):
        if self.__g_dtDesignatedFirstDate is None or self.__g_dtDesignatedLastDate is None:  # or self.__g_nHyperMartTypeIdx == -1:
            raise Exception('not ready to load dataframe to analyze')
        if len(df_raw.index) == 0:
            self.__g_dfAnalyze = self.__set_nullify_dataframe()
        else:
            self.__g_dfAnalyze = df_raw

    def get_rank(self, s_view_point, s_sort_column):
        """
        적재된 EDI 데이터에서 순위 추출
        :param s_view_point: item vs. branch
        :param s_sort_column: amnt vs. qty
        :return:
        """
        tup_view_point = ('branch_id', 'item_id')
        tup_sort_column = ('amnt', 'qty')
        try:
            tup_view_point.index(s_view_point)
        except ValueError:
            raise Exception('invalid view point')
        try:
            tup_sort_column.index(s_sort_column)
        except ValueError:
            raise Exception('invalid sort column')

        try:  # df w/o item_id or df w/o branch_id
            df_sum_by_branch = self.__g_dfAnalyze.groupby([s_view_point]).sum()
        except KeyError:
            raise Exception('requested view point - ' + s_view_point + ' not exist')
        # 매장 정보 vlookup 위해서 item_id를 인덱스에서 컬럼으로 변경
        df_sum_by_branch.reset_index(inplace=True)

        # 기본: 오름차순(ascending. 내림차순 정렬: ascending 옵션을 False
        df_rank = df_sum_by_branch.sort_values(by=s_sort_column, ascending=False)
        # df_branch_rank.index = pd.RangeIndex(start=1, step=1)
        df_rank.reset_index(inplace=True)
        df_rank.index += 1
        del df_rank['index']
        # 매장 순위 vlookup 위해서 인덱스를 컬럼으로 변경
        df_rank.reset_index(inplace=True)
        df_rank.rename(columns={'index': 'rank'}, inplace=True)
        df_rank.set_index(s_view_point, inplace=True)
        if s_view_point == 'branch_id':  # 기간별 존재하지 않는 매장 정보를 채움
            df_rank = df_rank.reindex(self.__g_lstBranchId, fill_value=0)
            n_lowest_rank = len(self.__g_lstBranchId) + 1
        if s_view_point == 'item_id':  # 기간별 존재하지 않는 품목 정보를 채움
            df_rank = df_rank.reindex(self.__g_lstSkuId, fill_value=0)
            n_lowest_rank = len(self.__g_lstSkuId) + 1

        # 기간별 존재하지 않는 품목과 매장 순위를 최하위로 설정
        idx_mask = (df_rank['qty'] == 0) & (df_rank['amnt'] == 0)
        df_rank['rank'][idx_mask] = n_lowest_rank
        df_rank = df_rank.sort_values(by=s_sort_column, ascending=False)
        return df_rank

    def __set_nullify_dataframe(self):
        lst_qty = []
        lst_amnt = []
        lst_logdate = []
        if self.__g_lstBranchId:
            lst_to_iter = self.__g_lstBranchId
            s_anal_wise = 'branch_id'
        elif self.__g_lstSkuId:
            lst_to_iter = self.__g_lstSkuId
            s_anal_wise = 'item_id'
        else:
            raise Exception('invalid analytical wise')

        for n_dummy in lst_to_iter:
            lst_qty.append(0)
            lst_amnt.append(0)
            lst_logdate.append(self.__g_dtDesignatedFirstDate)
        df = pd.DataFrame([x for x in zip(lst_to_iter, lst_qty, lst_amnt, lst_logdate)])
        df.rename(columns={0: s_anal_wise, 1: 'qty', 2: 'amnt', 3: 'logdate'}, inplace=True)
        return df

    # def __estimate_perf(self):
    #     """ get full month estimation regarding present period """
    #     if not self.__g_dtDesignatedLastDate:
    #         raise Exception('invalid last date')
    #     return None
