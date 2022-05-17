from dateutil.relativedelta import relativedelta
from datetime import datetime
import pandas as pd

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class NvrBrsInfo:
    """ depends on svplugins.ga_register_db.item_performance """
    # __g_bPeriodDebugMode = False
    __g_oSvDb = None

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        self.__g_oSvDb = o_sv_db
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # logger.debug('__del__')
        del self.__g_oSvDb

    # def activate_debug(self):
    #     self.__g_bPeriodDebugMode = True

    def get_list_by_period(self, s_period_from, s_period_to):
        """
        data for brs contract list screen
        :param s_period_from:
        :param s_period_to:
        :return:
        """
        lst_contract_earliest = self.__g_oSvDb.executeQuery('getNvrBrsContractEarliest')
        lst_contract_latest = self.__g_oSvDb.executeQuery('getNvrBrsContractLatest')
        if lst_contract_earliest[0]['min_date'] is None or lst_contract_latest[0]['max_date'] is None:
            dt_latest_contract = datetime.today()
            dt_earliest_contract = dt_latest_contract - relativedelta(months=6)
            dt_earliest_contract = dt_earliest_contract.replace(day=1)
        else:
            dt_earliest_contract = lst_contract_earliest[0]['min_date']
            dt_latest_contract = lst_contract_latest[0]['max_date']
        del lst_contract_earliest, lst_contract_latest

        if s_period_from is not None and s_period_to is not None:
            dt_earliest_req = datetime.strptime(s_period_from, '%Y%m%d')
            dt_latest_req = datetime.strptime(s_period_to, '%Y%m%d')
        else:
            dt_latest_req = dt_latest_contract
            dt_earliest_req = dt_latest_req - relativedelta(months=6)
            dt_earliest_req = dt_earliest_req.replace(day=1)

        lst_contract_rst = self.__g_oSvDb.executeQuery('getNvrBrsContractDetailByPeriod', dt_earliest_req, dt_latest_req)
        dict_budget_period = {'s_earliest_contract': dt_earliest_contract.strftime("%Y%m%d"),
                              's_latest_contract': dt_latest_contract.strftime("%Y%m%d"),
                              's_earliest_req': dt_earliest_req.strftime("%Y%m%d"),
                              's_latest_req': dt_latest_req.strftime("%Y%m%d")}
        del dt_earliest_contract, dt_latest_contract
        return {'dict_contract_period': dict_budget_period, 'lst_contract_rst': lst_contract_rst}

    def add_contract(self, request):
        """ 
        :param n_sv_acct_id: is to execute the client_serve plugin
        """
        # begin - construct contract info list
        s_multiple_contract = request.POST.get('multiple_contract')
        lst_line = s_multiple_contract.splitlines()
        # [0] 계약 ID [1] 계약 상태 [2] 등록 일시 [3] 계약 이름	[4]현재 연결 광고 그룹 [5] 템플릿 이름 
        # [6] 계약 가능 검색수 [7] 계약 기간 [8] 계약 광고비 [9] 환급액 [10] 노출수 [11] 클릭수 [12] 클릭률(%)
        for s_line in lst_line:
            lst_single_line = s_line.split('\t')
            if lst_single_line[0] == '계약 ID' and lst_single_line[1] == '계약 상태':
                continue
            dt_regdate = datetime.strptime(lst_single_line[2], '%Y.%m.%d.')
            lst_contract_period = lst_single_line[7].split('~')
            dt_contract_begin = datetime.strptime(lst_contract_period[0], '%Y.%m.%d.')
            dt_contract_end = datetime.strptime(lst_contract_period[1], '%Y.%m.%d.')
            s_conntected_ad_group = lst_single_line[4]
            if s_conntected_ad_group.find('NV_PS_DISP_BRS') != -1:
                if s_conntected_ad_group.find('MOB') != -1:
                    s_ua = 'M'
                elif s_conntected_ad_group.find('PC') != -1:
                    s_ua = 'P'
            else:
                s_ua = ''
            self.__g_oSvDb.executeQuery('insertNvrBrsContract', lst_single_line[0], lst_single_line[1], dt_regdate,
                                        lst_single_line[3], lst_single_line[4], lst_single_line[5], lst_single_line[6],
                                        dt_contract_begin, dt_contract_end, lst_single_line[8], lst_single_line[9], s_ua)
        del lst_line
        # end - construct contract info list
        return {'b_error': False, 's_msg': None, 'dict_ret': None}
