import sys
from dateutil.relativedelta import relativedelta
from datetime import datetime
import random
import re

# for logger
import logging

if __name__ == 'contract': # for calling from svplugins.integrate_db.task
    sys.path.append('../../svcommon')
    from sv_campaign_parser import SvCampaignParser
else: # for platform running
    from svcommon.sv_campaign_parser import SvCampaignParser

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class PnsInfo:
    """ depends on svplugins.ga_register_db.item_performance """
    # __g_bPeriodDebugMode = False
    __g_oSvDb = None
    __g_dictSource = None
    __g_dictSourceInverted = None
    __g_dictContractType = None
    __g_dictContractTypeInverted = None

    def __init__(self, o_sv_db=None):
        """ o_sv_db=None for calling from svplugins.integrate_db """
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        if o_sv_db:
            self.__g_oSvDb = o_sv_db
        o_sv_campaign_parser = SvCampaignParser()
        self.__g_dictSource = o_sv_campaign_parser.get_source_id_title_dict()
        self.__g_dictContractType = o_sv_campaign_parser.get_pns_contract_type_dict()
        self.__g_dictSourceInverted = o_sv_campaign_parser.get_source_id_title_dict(True)
        self.__g_dictContractTypeInverted = o_sv_campaign_parser.get_pns_contract_type_dict(True)
        del o_sv_campaign_parser
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # logger.debug('__del__')
        if self.__g_oSvDb:
            del self.__g_oSvDb
    
    # def activate_debug(self):
    #     self.__g_bPeriodDebugMode = True

    def get_source_type_dict(self):
        return self.__g_dictSource
    
    def get_inverted_source_type_dict(self):
        return self.__g_dictSourceInverted

    def get_contract_type_dict(self):
        return self.__g_dictContractType
    
    def get_list_by_period(self, s_period_from, s_period_to):
        """
        data for brs contract list screen
        :param s_period_from:
        :param s_period_to:
        :return:
        """
        lst_contract_earliest = self.__g_oSvDb.execute_query('getPnsContractEarliest')
        if 'err_code' in lst_contract_earliest[0]:  # if table not exists
            dict_budget_period = {'s_earliest_contract': '',
                                    's_latest_contract': '',
                                    's_earliest_req': '',
                                    's_latest_req': ''}
            return {'dict_contract_period': dict_budget_period, 'lst_contract_rst': []}

        lst_contract_latest = self.__g_oSvDb.execute_query('getPnsContractLatest')
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
        
        lst_contract_rst = self.__g_oSvDb.execute_query('getPnsContractDetailByPeriod', dt_earliest_req, dt_latest_req)
        for dict_single_contract in lst_contract_rst:
            dict_single_contract['source_name'] = self.__g_dictSource[dict_single_contract['source_id']]
            del dict_single_contract['source_id']
            dict_single_contract['contract_type'] = self.__g_dictContractType[dict_single_contract['contract_type']]
            # print(dict_single_contract)
        dict_budget_period = {'s_earliest_contract': dt_earliest_contract.strftime("%Y%m%d"),
                              's_latest_contract': dt_latest_contract.strftime("%Y%m%d"),
                              's_earliest_req': dt_earliest_req.strftime("%Y%m%d"),
                              's_latest_req': dt_latest_req.strftime("%Y%m%d")}
        del dt_earliest_contract, dt_latest_contract
        return {'dict_contract_period': dict_budget_period, 'lst_contract_rst': lst_contract_rst}

    def get_detail_by_id(self, n_contract_id):
        """
        data for contract detail screen
        :param n_contract_id:
        :return:
        """
        lst_contract_detail = self.__g_oSvDb.execute_query('getPnsContractDetailBySrl', n_contract_id)
        return lst_contract_detail[0]

    def add_contract_single(self, request):
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        lst_query_title = ['source_id', 'contract_type', 'media_term', 'contractor_id',
                            'cost_incl_vat', 'agency_rate_percent', 'execute_date_begin', 
                            'execute_date_end', 'regdate']
        lst_query_value = []
        for s_ttl in lst_query_title:
            lst_query_value.append(request.POST.get(s_ttl))
        if int(lst_query_value[0]) not in self.__g_dictSource:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'invaid source'
            return dict_rst
        if int(lst_query_value[1]) not in self.__g_dictContractType:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'invaid contract type'
            return dict_rst
        
        s_cost_incl_vat = lst_query_value[4].replace(',', '')
        if not str.isdigit(s_cost_incl_vat):
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = lst_query_title[4] + ' should be digit'
            return dict_rst

        o_reg_ex = re.compile(r"\d+%$") # pattern ex) 2% 23%
        s_agency_rate_percent = lst_query_value[5].strip()
        m = o_reg_ex.search(s_agency_rate_percent) # match() vs search()
        if not m: # if valid percent string
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = lst_query_title[5] + ' should be 00%'
            return dict_rst
        del o_reg_ex
        del m

        try:
            dt_execute_date_begin = datetime.strptime(lst_query_value[6], '%Y-%m-%d')
        except ValueError:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = lst_query_title[6] + ' is invalid date'
            return dict_rst
        try:
            dt_execute_date_end = datetime.strptime(lst_query_value[7], '%Y-%m-%d')
        except ValueError:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = lst_query_title[7] + ' is invalid date'
            return dict_rst

        if len(lst_query_value[8]) > 0:
            try:
                dt_regdate = datetime.strptime(lst_query_value[8], '%Y-%m-%d')
            except ValueError:
                dict_rst['b_error'] = True
                dict_rst['s_msg'] = lst_query_title[8] + ' is invalid date'
                return dict_rst
        else:
            dt_regdate = datetime.today()
        self.__g_oSvDb.execute_query('insertPnsContract', lst_query_value[0], lst_query_value[1],
                                     lst_query_value[2].strip(), lst_query_value[3].strip(),
                                     s_cost_incl_vat, s_agency_rate_percent,
                                     dt_execute_date_begin, dt_execute_date_end, dt_regdate)

    def add_contract_bulk(self, request):
        """ 
        copy & paste from SV CMS web admin
        :param 
        """
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        # begin - construct contract info list
        s_multiple_contract = request.POST.get('multiple_contract')
        lst_line = s_multiple_contract.splitlines()
        # [0] 번호 [1] Query ID [2] GA인식 소스 [3] 서비스명 [4] utm_term [5] 고정 비용 VAT포함 [6] 클릭수 [7] 클릭단가 [8] 비용 배분 기간 [9] 등록일
        for s_line in lst_line:
            lst_single_line = s_line.split('\t')
            if len(lst_single_line) != 10:
                dict_rst['b_error'] = True
                dict_rst['s_msg'] = 'weird pns contract info'
                return dict_rst
            n_source_id = self.__g_dictSourceInverted[lst_single_line[2]]
            lst_temp = lst_single_line[4].split('_')
            n_lst_temp = len(lst_temp)
            if n_lst_temp == 3:
                s_targeted_term = lst_temp[0]
                s_contractor_id = lst_temp[2]
                n_contract_type_id = self.__g_dictContractTypeInverted[lst_temp[1]]
            elif n_lst_temp == 4:  # double targeted keyword case
                s_targeted_term = lst_temp[0] + '_' + lst_temp[1]
                s_contractor_id = lst_temp[3]
                n_contract_type_id = self.__g_dictContractTypeInverted[lst_temp[2]]
            
            del lst_temp
            s_contract_amnt_incl_vat = lst_single_line[5].replace('₩', '').replace(',', '')
            if not str.isdigit(s_contract_amnt_incl_vat):
                s_contract_amnt_incl_vat = 0
            lst_contract_period = lst_single_line[8].split('~')
            dt_execute_begin = datetime.strptime(lst_contract_period[0], '%Y.%m.%d')
            dt_execute_end = datetime.strptime(lst_contract_period[1], '%Y.%m.%d')
            del lst_contract_period
            dt_regdate = datetime.strptime(lst_single_line[9], '%Y-%m-%d')
            self.__g_oSvDb.execute_query('insertPnsContract', n_source_id, n_contract_type_id, s_targeted_term,
                                         s_contractor_id, s_contract_amnt_incl_vat, '50%',
                                         dt_execute_begin, dt_execute_end, dt_regdate)
            del dt_execute_begin
            del dt_execute_end
            del dt_regdate
        del lst_line
        # end - construct contract info list
        return dict_rst
    
    def update_contract(self, request):
        """
        data for contract detail screen
        :param s_budget_id:
        :return:
        """
        # n_contract_srl = int(request.POST['contract_srl'])
        # dict_contract = self.get_detail_by_srl(n_contract_srl)
        # s_ua = request.POST['ua'].strip()
        # if s_ua not in self.__g_lstUa:
        #     s_ua = dict_contract['ua']

        # s_refund_amnt = request.POST['refund_amnt'].replace(',', '')
        # if not str.isdigit(s_refund_amnt):
        #     s_refund_amnt = dict_contract['refund_amnt']
        
        # if int(s_refund_amnt) == 0:
        #     s_contract_status = dict_contract['contract_status']
        # else:
        #     s_contract_status = '집행 중 취소'
        # self.__g_oSvDb.execute_query('updatePnsContractUaBySrl', s_contract_status, s_refund_amnt, s_ua, n_contract_srl)
        return
