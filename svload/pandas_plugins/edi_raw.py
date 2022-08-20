import pandas as pd
from datetime import date

from svcommon.sv_hypermart_model import SvHyperMartType, SvHypermartGeoInfo

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class EdiRaw:
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
            if lst_raw_data and 'err_code' in lst_raw_data.pop().keys():  # for an initial stage; no table
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
