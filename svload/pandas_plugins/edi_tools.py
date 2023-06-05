from datetime import datetime
import pandas as pd
from svcommon.sv_hypermart_model import BranchType
from svcommon.sv_hypermart_model import SvHyperMartType
from svcommon.sv_hypermart_model import SvHypermartGeoInfo

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


class EdiFilter:
    __g_oHttpRequest = None
    __g_dictBranchInfo = {}
    __g_dictSalesChInfo = None
    __g_dictFilter = {'s_sales_ch_mode': None, 'lst_sales_ch': [], 's_branch_mode': None, 'lst_branch': [],
                      's_sku_mode': None, 'lst_sku': []}

    # def __new__(cls, request):
    #    # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
    #    return super().__new__(cls)

    def __init__(self, request):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        self.__g_oHttpRequest = request

        # begin - set effective hyper mart type dict
        dict_hyper_mart_type = SvHyperMartType.get_dict_by_idx()
        del dict_hyper_mart_type[1]  # remove ESTIMATION
        del dict_hyper_mart_type[2]  # remove NOT_SURE
        del dict_hyper_mart_type[5]  # remove HOMEPLUS
        self.__g_dictSalesChInfo = dict_hyper_mart_type

        # begin - hyper mart branch object
        dict_branch_by_title = SvHyperMartType.get_dict_by_title()
        dict_branch_type = BranchType.get_dict_by_title()
        o_mart_geo_info = SvHypermartGeoInfo()
        for dict_single_branch in o_mart_geo_info.lst_hypermart_geo_info:
            n_hypermart_id = dict_branch_by_title[dict_single_branch['hypermart_name']]
            n_branch_type_id = dict_branch_type[dict_single_branch['branch_type']]
            dict_branch = {'id': dict_single_branch['id'], 'selected': '', 'hypermart_id': n_hypermart_id,
                           'hypermart_name': dict_single_branch['hypermart_name'],
                           'name': dict_single_branch['name'],
                           'branch_type_id': n_branch_type_id,
                           'branch_type': dict_single_branch['branch_type'],
                           'do_name': dict_single_branch['do_name'], 'si_name': dict_single_branch['si_name'],
                           'gu_gun': dict_single_branch['gu_gun'], 'dong_myun_ri': dict_single_branch['dong_myun_ri'],
                           'latitude': dict_single_branch['latitude'], 'longitude': dict_single_branch['longitude']}
            self.__g_dictBranchInfo[dict_single_branch['id']] = dict_branch
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # logger.debug('__del__')
        pass

    def get_sales_ch(self):
        # sch means sales_ch
        s_sch_filter_mode = ''
        lst_selected_sch = []
        try:
            s_include_sch_id = self.__g_oHttpRequest.GET['sales_ch_inc']
            lst_selected_sch = s_include_sch_id.split(',')
            s_sch_filter_mode = s_sch_filter_mode + 'inc'
        except KeyError:
            pass
        try:
            s_exclude_sch_id = self.__g_oHttpRequest.GET['sales_ch_exc']
            lst_selected_sch = s_exclude_sch_id.split(',')
            s_sch_filter_mode = s_sch_filter_mode + 'exc'
        except KeyError:
            pass

        if s_sch_filter_mode.find('exc') > -1 and s_sch_filter_mode.find('inc') > -1:
            raise Exception('weird sales ch filter')

        # 영업 채널 일련번호를 문자열에서 정수로 변경
        lst_selected_sch = [int(item) for item in lst_selected_sch]
        # begin - set sales ch info for filter UI
        dict_sch_info_for_ui = {}
        for n_idx, single_sch_title in self.__g_dictSalesChInfo.items():
            dict_sch = {'id': n_idx, 'selected': '', 'sales_ch_name': single_sch_title}
            try:
                lst_selected_sch.index(n_idx)
                dict_sch['selected'] = 'selected'
            except ValueError:
                pass
            dict_sch_info_for_ui[n_idx] = dict_sch
        # end - set sales h info for filter UI
        self.__g_dictFilter['s_sales_ch_mode'] = s_sch_filter_mode
        self.__g_dictFilter['lst_sales_ch'] = lst_selected_sch
        return {'dict_sales_ch_info_for_ui': dict_sch_info_for_ui, 's_sales_ch_filter_mode': s_sch_filter_mode}

    def get_branch(self, n_branch_id=None):
        # self.__g_dfEmartBranchId = pd.DataFrame(dict_all_branch_info_by_id).transpose()
        s_branch_filter_mode = ''
        lst_selected_branch = []
        dict_branch_info_for_ui = {}
        if n_branch_id:  # for ByBranchEdi.view()
            dict_single_branch = self.__g_dictBranchInfo[n_branch_id]
            dict_branch_info_for_ui[n_branch_id] = dict_single_branch
            s_branch_filter_mode = 'inc'
            lst_selected_branch.append(n_branch_id)
        else:  # for other view()
            try:
                s_include_branch_id = self.__g_oHttpRequest.GET['branch_inc']
                lst_selected_branch = s_include_branch_id.split(',')
                s_branch_filter_mode = s_branch_filter_mode + 'inc'
            except KeyError:
                pass
            try:
                s_exclude_branch_id = self.__g_oHttpRequest.GET['branch_exc']
                lst_selected_branch = s_exclude_branch_id.split(',')
                s_branch_filter_mode = s_branch_filter_mode + 'exc'
            except KeyError:
                pass

            if s_branch_filter_mode.find('exc') > -1 and s_branch_filter_mode.find('inc') > -1:
                raise Exception('weird branch filter')

            # 매장 일련번호를 문자열에서 정수로 변경
            lst_selected_branch = [int(item) for item in lst_selected_branch]
            # begin - set edi branch info for filter UI
            if self.__g_dictFilter['s_sales_ch_mode'] == 'inc':
                for n_branch_id, dict_single_branch in self.__g_dictBranchInfo.items():
                    if dict_single_branch['hypermart_id'] in self.__g_dictFilter['lst_sales_ch']:
                        try:
                            lst_selected_branch.index(n_branch_id)
                            dict_single_branch['selected'] = 'selected'
                        except ValueError:
                            pass
                        dict_branch_info_for_ui[n_branch_id] = dict_single_branch
            elif self.__g_dictFilter['s_sales_ch_mode'] == 'exc':
                for n_branch_id, dict_single_branch in self.__g_dictBranchInfo.items():
                    if dict_single_branch['hypermart_id'] not in self.__g_dictFilter['lst_sales_ch']:
                        try:
                            lst_selected_branch.index(n_branch_id)
                            dict_single_branch['selected'] = 'selected'
                        except ValueError:
                            pass
                        dict_branch_info_for_ui[n_branch_id] = dict_single_branch
            else:
                for n_branch_id, dict_single_branch in self.__g_dictBranchInfo.items():
                    try:
                        lst_selected_branch.index(n_branch_id)
                        dict_single_branch['selected'] = 'selected'
                    except ValueError:
                        pass
                    dict_branch_info_for_ui[n_branch_id] = dict_single_branch
            # end - set edi branch info for filter UI
        self.__g_dictFilter['s_branch_mode'] = s_branch_filter_mode
        self.__g_dictFilter['lst_branch'] = lst_selected_branch
        return {'dict_branch_info_for_ui': dict_branch_info_for_ui, 's_branch_filter_mode': s_branch_filter_mode}

    def get_sku_by_brand_id(self, o_sv_db, n_sku_id=None):
        """
        :param o_sv_db:
        :param n_sku_id: set only if sku-detail-view
        :return:
        """
        s_sku_filter_mode = ''
        lst_selected_sku = []
        try:
            s_include_sku_id = self.__g_oHttpRequest.GET['sku_inc']
            lst_selected_sku = s_include_sku_id.split(',')
            s_sku_filter_mode = s_sku_filter_mode + 'inc'
        except KeyError:
            pass
        try:
            s_exclude_sku_id = self.__g_oHttpRequest.GET['sku_exc']
            lst_selected_sku = s_exclude_sku_id.split(',')
            s_sku_filter_mode = s_sku_filter_mode + 'exc'
        except KeyError:
            pass

        if s_sku_filter_mode.find('exc') > -1 and s_sku_filter_mode.find('inc') > -1:
            raise Exception('weird sku filter')

        lst_selected_sku_for_ui = []
        if s_sku_filter_mode != '':  # sku filter designated
            lst_selected_sku_for_ui = [int(item) for item in lst_selected_sku]  # SKU 일련번호를 문자열에서 정수로 변경

        lst_sku_rst = o_sv_db.execute_query('getEdiSkuInfoByAccept', 1)
        if lst_sku_rst and 'err_code' in lst_sku_rst[0].keys():  # for an initial stage; no table
            lst_sku_rst = []
        lst_selected_sku = []
        # 좁은 범위의 필터부터 살펴봄
        if n_sku_id:  # sku designated
            for dict_single_sku in lst_sku_rst:
                if dict_single_sku['id'] == n_sku_id:
                    lst_selected_sku.append(dict_single_sku)
                    break
        elif self.__g_dictFilter['s_branch_mode']:  # branch designated
            lst_mart_id = []
            for n_branch_id in self.__g_dictFilter['lst_branch']:
                dict_single_branch = self.__g_dictBranchInfo[n_branch_id]
                lst_mart_id.append(dict_single_branch['hypermart_id'])

            if self.__g_dictFilter['s_branch_mode'] == 'inc':
                for dict_single_sku in lst_sku_rst:
                    if dict_single_sku['mart_id'] in lst_mart_id:
                        lst_selected_sku.append(dict_single_sku)
            elif self.__g_dictFilter['s_branch_mode'] == 'exc':
                for dict_single_sku in lst_sku_rst:
                    if dict_single_sku['mart_id'] not in lst_mart_id:
                        lst_selected_sku.append(dict_single_sku)
        elif self.__g_dictFilter['s_sales_ch_mode']:  # sales ch designated
            if self.__g_dictFilter['s_sales_ch_mode'] == 'inc':
                for dict_single_sku in lst_sku_rst:
                    if dict_single_sku['mart_id'] in self.__g_dictFilter['lst_sales_ch']:
                        lst_selected_sku.append(dict_single_sku)
            elif self.__g_dictFilter['s_sales_ch_mode'] == 'exc':
                for dict_single_sku in lst_sku_rst:
                    if dict_single_sku['mart_id'] not in self.__g_dictFilter['lst_sales_ch']:
                        lst_selected_sku.append(dict_single_sku)
        else:  # no filter
            lst_selected_sku = lst_sku_rst
        del lst_sku_rst
        # if self.__g_dictFilter['s_branch_mode'] == 'inc':  # branch designated
        #     lst_mart_id = []
        #     for n_branch_id in self.__g_dictFilter['lst_branch']:
        #         dict_single_branch = self.__g_dictBranchInfo[n_branch_id]
        #         lst_mart_id.append(dict_single_branch['hypermart_id'])
        #     lst_unique_mart_id = [str(n_mart_id) for n_mart_id in list(set(lst_mart_id))]  # for uniqueness
        #     dict_param_tmp = {'n_brand_id': n_brand_id, 's_req_mart_id_set': ','.join(lst_unique_mart_id)}
        #     print(dict_param_tmp)
        #     lst_sku_rst = o_sv_db.execute_dynamic_query('getEdiSkuInfoByBrandBranchId', dict_param_tmp)
        #     del dict_param_tmp
        # elif n_sku_id:  # sku designated
        #     lst_sku_rst = o_sv_db.execute_query('getEdiSkuInfoByBrandSkuId', n_sku_id)
        #     # {19: {'mart_id': 3, 'mart_name': 'Emart', 'name': '유한락스 욕실청소 900ML*2', 'first_detect_logdate': datetime.date(2019, 1, 1)}}
        # else:  # all branch & all sku mode
        #     lst_sku_rst = o_sv_db.execute_query('getEdiSkuInfoByBrandId', n_brand_id)
        dict_hyper_mart_type = SvHyperMartType.get_dict_by_idx()
        dict_sku_info_by_id = {}
        if len(lst_selected_sku):
            for dict_sku in lst_selected_sku:
                dict_sku_info_by_id[dict_sku['id']] = {'hypermart_name': self.__g_dictSalesChInfo[dict_sku['mart_id']],
                                                       'selected': '',
                                                       'mart_id': dict_sku['mart_id'],
                                                       # 'mart_name': dict_hyper_mart_type[dict_sku['mart_id']],
                                                       'name': dict_sku['item_name'],
                                                       'first_detect_logdate': dict_sku['first_detect_logdate']}
        del dict_hyper_mart_type

        # begin - set sku info for filter UI
        dict_sku_info_for_ui = {}
        for n_sku_id, dict_single_sku in dict_sku_info_by_id.items():
            # print(dict_single_sku)
            dict_sku = {'id': n_sku_id, 'selected': '', 'sch_name': dict_single_sku['hypermart_name'],
                        'name': dict_single_sku['name']}
            try:
                lst_selected_sku_for_ui.index(n_sku_id)
                dict_sku['selected'] = 'selected'
            except ValueError:
                pass
            dict_sku_info_for_ui[n_sku_id] = dict_sku
        # end - set sku info for filter UI
        self.__g_dictFilter['s_sku_mode'] = s_sku_filter_mode
        self.__g_dictFilter['lst_sku'] = lst_selected_sku

        if s_sku_filter_mode != '':  # sku filter designated
            dict_sku_filtered = {}
            for n_sku_id in lst_selected_sku_for_ui:
                dict_sku_filtered[n_sku_id] = dict_sku_info_by_id[n_sku_id]
            dict_sku_info_by_id = dict_sku_filtered

        return {'dict_sku_info_by_id': dict_sku_info_by_id, 'dict_sku_info_for_ui': dict_sku_info_for_ui,
                's_sku_filter_mode': s_sku_filter_mode}
