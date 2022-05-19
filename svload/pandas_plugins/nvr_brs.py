from dateutil.relativedelta import relativedelta
from datetime import datetime
import random

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class NvrBrsInfo:
    """ depends on svplugins.ga_register_db.item_performance """
    # __g_bPeriodDebugMode = False
    __g_oSvDb = None
    __g_lstUa = ['M', 'P']

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
    def get_ua_list(self):
        return self.__g_lstUa

    def get_list_by_period(self, s_period_from, s_period_to):
        """
        data for brs contract list screen
        :param s_period_from:
        :param s_period_to:
        :return:
        """
        lst_contract_earliest = self.__g_oSvDb.executeQuery('getNvrBrsContractEarliest')
        if 'err_code' in lst_contract_earliest[0]:  # if table not exists
            dict_budget_period = {'s_earliest_contract': '',
                                    's_latest_contract': '',
                                    's_earliest_req': '',
                                    's_latest_req': ''}
            return {'dict_contract_period': dict_budget_period, 'lst_contract_rst': []}

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

    def get_detail_by_srl(self, n_contract_srl):
        """
        data for contract detail screen
        :param s_budget_id:
        :return:
        """
        lst_contract_detail = self.__g_oSvDb.executeQuery('getNvrBrsContractDetailByBudgetSrl', n_contract_srl)
        return lst_contract_detail[0]

    def update_contract(self, request):
        """
        data for contract detail screen
        :param s_budget_id:
        :return:
        """
        n_contract_srl = int(request.POST['contract_srl'])
        s_ua = request.POST['ua'].strip()
        if s_ua in self.__g_lstUa:
            self.__g_oSvDb.executeQuery('updateNvrBrsContractUaBySrl', s_ua, n_contract_srl)
        return

    def add_contract_barter(self, request):
        """ 
        add barter contract info
        :param 
        """
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        lst_query_title = ['contract_name', 'connected_ad_group', 'template_name', 'available_queries', 
                            'contract_amnt', 'contract_date_begin', 'contract_date_end', 'ua']
        lst_query_value = []
        for s_ttl in lst_query_title:
            lst_query_value.append(request.POST.get(s_ttl))

        s_available_queries = lst_query_value[3].replace(',', '')
        if not str.isdigit(s_available_queries):
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = lst_query_title[3] + ' should be digit'
            return dict_rst
        s_contract_amnt = lst_query_value[4].replace(',', '')
        if not str.isdigit(s_contract_amnt):
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = lst_query_title[4] + ' should be digit'
            return dict_rst
        
        try:
            dt_contract_begin = datetime.strptime(lst_query_value[5], '%Y-%m-%d')
        except ValueError:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = lst_query_title[5] + ' is invalid date'
            return dict_rst

        try:
            dt_contract_end = datetime.strptime(lst_query_value[6], '%Y-%m-%d')
        except ValueError:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = lst_query_title[5] + ' is invalid date'
            return dict_rst
        
        if lst_query_value[7] not in self.__g_lstUa:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = lst_query_title[7] + ' is invalid.'
            return dict_rst

        s_unique_contract_id = 'svmanual-' + self.__get_unique_namespace(10)
        self.__g_oSvDb.executeQuery('insertNvrBrsContract', s_unique_contract_id, '집행 중', datetime.today(),
                                        lst_query_value[0].strip(), lst_query_value[1].strip(),
                                        lst_query_value[2].strip(), int(s_available_queries), 
                                        dt_contract_begin, dt_contract_end, int(s_contract_amnt), 0, lst_query_value[7])
        return dict_rst


    def add_contract_bulk(self, request):
        """ 
        copy & paste from excel file downloaded from NVR admin
        :param 
        """
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        # begin - construct contract info list
        s_multiple_contract = request.POST.get('multiple_contract')
        lst_line = s_multiple_contract.splitlines()
        # [0] 계약 ID [1] 계약 상태 [2] 등록 일시 [3] 계약 이름	[4]현재 연결 광고 그룹 [5] 템플릿 이름 
        # [6] 계약 가능 검색수 [7] 계약 기간 [8] 계약 광고비 [9] 환급액 [10] 노출수 [11] 클릭수 [12] 클릭률(%)
        for s_line in lst_line:
            lst_single_line = s_line.split('\t')
            if len(lst_single_line) < 10:
                dict_rst['b_error'] = True
                dict_rst['s_msg'] = 'weird nvr brs contract info'
                return dict_rst

            if lst_single_line[0] == '계약 ID' and lst_single_line[1] == '계약 상태':
                continue
            if lst_single_line[7] == '-':  # means 집행 전 취소
                continue
            dt_regdate = datetime.strptime(lst_single_line[2], '%Y.%m.%d.')
            lst_contract_period = lst_single_line[7].split('~')
            # print(lst_single_line)
            dt_contract_begin = datetime.strptime(lst_contract_period[0], '%Y.%m.%d.')
            dt_contract_end = datetime.strptime(lst_contract_period[1], '%Y.%m.%d.')
            s_conntected_ad_group = lst_single_line[4]
            if s_conntected_ad_group.find('NV_PS_DISP_BRS') != -1:
                if s_conntected_ad_group.find('MOB') != -1:
                    s_ua = self.__g_lstUa[0]
                elif s_conntected_ad_group.find('PC') != -1:
                    s_ua = self.__g_lstUa[1]
            else:
                s_ua = 'e'  # means error
            
            s_available_queries = lst_single_line[6].replace(',', '')
            if not str.isdigit(s_available_queries):
                s_available_queries = 0
            s_contract_amnt = lst_single_line[8].replace(',', '')
            if not str.isdigit(s_contract_amnt):
               s_contract_amnt = 0
            self.__g_oSvDb.executeQuery('insertNvrBrsContract', lst_single_line[0], lst_single_line[1], dt_regdate,
                                        lst_single_line[3], lst_single_line[4], lst_single_line[5], s_available_queries,
                                        dt_contract_begin, dt_contract_end, s_contract_amnt, lst_single_line[9], s_ua)
        del lst_line
        # end - construct contract info list
        return dict_rst

    def __get_unique_namespace(self, n_namespace_len=8):
        # set tbl prefix for each account
        if n_namespace_len > 10:
            n_namespace_len = 10  # refer to column max_length
        s_allowed_chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghjklmnopqrstuvwxyz_'
        # mysql tbl name does not differ upper and lower case
        s_u_namespace = ''.join(random.sample(s_allowed_chars, len(s_allowed_chars)))
        return s_u_namespace[:n_namespace_len]
