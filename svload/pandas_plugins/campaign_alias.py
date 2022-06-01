# from dateutil.relativedelta import relativedelta
from datetime import datetime
# import re

# for logger
import logging

from svcommon.sv_mysql import SvMySql
from svcommon.sv_campaign_parser import SvCampaignParser

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class CampaignAliasInfo:
    """ depends on svplugins.ga_register_db.item_performance """
    # __g_bPeriodDebugMode = False
    __g_oSvDb = None
    __g_dictSourceIdTitle = None
    __g_dictSourceIdTag = None
    __g_dictSearchRstTypeIdTitle = None
    __g_dictSearchRstTypeIdTag = None
    __g_dictMediumTypeIdTitle = None
    __g_dictMediumTypeIdTag = None

    def __init__(self, n_acct_id, n_brand_id):
        """ """
        # print(__file__ + ':' + sys._getframe().f_code.co_name)

        s_tbl_prefix = str(n_acct_id) + '_' + str(n_brand_id)
        self.__g_oSvDb = SvMySql()
        self.__g_oSvDb.set_tbl_prefix(s_tbl_prefix)
        self.__g_oSvDb.set_app_name('svload.views')
        self.__g_oSvDb.initialize({'n_acct_id': n_acct_id, 'n_brand_id': n_brand_id})

        o_sv_campaign_parser = SvCampaignParser()
        self.__g_dictSourceIdTitle = o_sv_campaign_parser.get_source_id_title_dict()
        self.__g_dictSourceIdTag = o_sv_campaign_parser.get_source_id_tag_dict()
        self.__g_dictSearchRstTypeIdTitle = o_sv_campaign_parser.get_search_rst_type_id_title_dict()
        self.__g_dictSearchRstTypeIdTag = o_sv_campaign_parser.get_search_rst_type_id_tag_dict()
        self.__g_dictMediumTypeIdTitle = o_sv_campaign_parser.get_medium_type_id_title_dict()
        self.__g_dictMediumTypeIdTag = o_sv_campaign_parser.get_medium_type_id_tag_dict()
        
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
        return self.__g_dictSourceIdTitle
    
    def get_search_rst_type_id_title_dict(self):
        return self.__g_dictSearchRstTypeIdTitle

    def get_medium_type_id_title_dict(self):
        return self.__g_dictMediumTypeIdTitle
    
    def get_list(self):
        """
        data for campaign alias list screen
        :return:
        """
        lst_alias_rst = self.__g_oSvDb.executeQuery('getCampaignAliasList')
        for dict_single_alias in lst_alias_rst:
            dict_single_alias['source_name'] = self.__g_dictSourceIdTitle[dict_single_alias['source_id']]
            s_sv_campaign_convention = self.__get_source_tag_by_id(dict_single_alias['source_id']) + '_' + \
                self.__get_search_rst_tag_by_id(dict_single_alias['search_rst_id']) + '_' + \
                self.__get_medium_tag_by_id(dict_single_alias['medium_id']) + '_' + \
                dict_single_alias['sv_lvl_1'] + '_' + dict_single_alias['sv_lvl_2'] + '_' + \
                dict_single_alias['sv_lvl_3']
            dict_single_alias['sv_conventional_alias'] = s_sv_campaign_convention
            del dict_single_alias['source_id']
        return {'lst_alias_rst': lst_alias_rst}

    def get_detail_by_id(self, n_alias_id):
        """
        data for campaign alias detail screen
        :param n_alias_id:
        :return:
        """
        lst_alias_detail = self.__g_oSvDb.executeQuery('getCampaignAliasDetailById', n_alias_id)
        s_sv_campaign_convention = self.__get_source_tag_by_id(lst_alias_detail[0]['source_id']) + '_' + \
            self.__get_search_rst_tag_by_id(lst_alias_detail[0]['search_rst_id']) + '_' + \
            self.__get_medium_tag_by_id(lst_alias_detail[0]['medium_id']) + '_' + \
            lst_alias_detail[0]['sv_lvl_1'] + '_' + lst_alias_detail[0]['sv_lvl_2'] + '_' + \
            lst_alias_detail[0]['sv_lvl_3']
        lst_alias_detail[0]['sv_conventional_alias'] = s_sv_campaign_convention
        return lst_alias_detail[0]

    def add_alias_single(self, request):
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        lst_query_title = ['source_id', 'contract_type', 'media_term', 'contractor_id',
                            'cost_incl_vat', 'agency_rate_percent', 'execute_date_begin', 
                            'execute_date_end', 'regdate']
        lst_query_value = []
        for s_ttl in lst_query_title:
            lst_query_value.append(request.POST.get(s_ttl))
        if int(lst_query_value[0]) not in self.__g_dictSourceIdTitle:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'invaid source'
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
        self.__g_oSvDb.executeQuery('insertPnsContract', lst_query_value[0], lst_query_value[1], 
                                    lst_query_value[2].strip(), lst_query_value[3].strip(), dt_regdate)

    def add_alias_bulk(self, request):
        """ 
        copy & paste multiple campaign alias
        :param 
        """
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        # begin - construct campaign alias info list
        s_multiple_campaign_alias = request.POST.get('multiple_campaign_alias')
        lst_line = s_multiple_campaign_alias.splitlines()
        # [0] 번호 [1] Query ID [2] GA인식 소스 [3] 서비스명 [4] utm_term [5] 고정 비용 VAT포함 [6] 클릭수 [7] 클릭단가 [8] 비용 배분 기간 [9] 등록일
        for s_line in lst_line:
            lst_single_line = s_line.split('\t')
            if len(lst_single_line) != 10:
                dict_rst['b_error'] = True
                dict_rst['s_msg'] = 'weird campaign alias info'
                return dict_rst
            # n_source_id = self.__g_dictSourceInverted[lst_single_line[2]]
            
            dt_regdate = datetime.strptime(lst_single_line[9], '%Y-%m-%d')
            self.__g_oSvDb.executeQuery('insertCampaignAlias', n_source_id, s_targeted_term,
                                        s_contractor_id, s_contract_amnt_incl_vat, '50%', 
                                        dt_execute_begin, dt_execute_end, dt_regdate)
            del dt_execute_begin
            del dt_execute_end
            del dt_regdate
        del lst_line
        # end - construct campaign alias info list
        return dict_rst
    
    def update_contract(self, request):
        """
        data for campaign alias detail screen
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
        # self.__g_oSvDb.executeQuery('updatePnsContractUaBySrl', s_contract_status, s_refund_amnt, s_ua, n_contract_srl)
        return
    
    def __get_source_tag_by_id(self, n_source_id):
        return self.__g_dictSourceIdTag[n_source_id]

    def __get_search_rst_tag_by_id(self, n_search_rst_id):
        return self.__g_dictSearchRstTypeIdTag[n_search_rst_id]
    
    def __get_medium_tag_by_id(self, n_medium_id):
        return self.__g_dictMediumTypeIdTag[n_medium_id]
