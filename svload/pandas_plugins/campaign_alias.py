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
    __g_dictSourceTagId = None
    __g_dictSearchRstTypeIdTitle = None
    __g_dictSearchRstTypeIdTag = None
    __g_dictSearchRstTypeTagId = None
    __g_dictMediumTypeIdTitle = None
    __g_dictMediumTypeIdTag = None
    __g_dictMediumTypeTagId = None

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
        self.__g_dictSourceTagId = o_sv_campaign_parser.get_source_id_tag_dict(True)
        self.__g_dictSearchRstTypeIdTitle = o_sv_campaign_parser.get_search_rst_type_id_title_dict()
        self.__g_dictSearchRstTypeIdTag = o_sv_campaign_parser.get_search_rst_type_id_tag_dict()
        self.__g_dictSearchRstTypeTagId = o_sv_campaign_parser.get_search_rst_type_id_tag_dict(True)
        self.__g_dictMediumTypeIdTitle = o_sv_campaign_parser.get_medium_type_id_title_dict()
        self.__g_dictMediumTypeIdTag = o_sv_campaign_parser.get_medium_type_id_tag_dict()
        self.__g_dictMediumTypeTagId = o_sv_campaign_parser.get_medium_type_id_tag_dict(True)
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
        lst_query_title = ['media_campaign_title', 'source_id', 'search_rst_type_id', 
                            'media_type_id', 'sv_lvl_1', 'sv_lvl_2', 'sv_lvl_3', 'regdate']

        lst_query_value = []
        for s_ttl in lst_query_title:
            lst_query_value.append(request.POST.get(s_ttl))
        
        s_media_campaign_title = lst_query_value[0].strip()
        n_source_id = int(lst_query_value[1])
        if n_source_id not in self.__g_dictSourceIdTitle:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'invaid source'
            return dict_rst
        n_search_rst_id = int(lst_query_value[2])
        if n_search_rst_id not in self.__g_dictSearchRstTypeIdTitle:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'invaid search rst'
            return dict_rst
        n_medium_id = int(lst_query_value[3])
        if n_medium_id not in self.__g_dictMediumTypeIdTitle:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'invaid medium'
            return dict_rst

        o_sv_campaign_parser = SvCampaignParser()
        s_sv_lvl_1 = o_sv_campaign_parser.validate_sv_campaign_level_tag(lst_query_value[4])['dict_ret']
        s_sv_lvl_2 = o_sv_campaign_parser.validate_sv_campaign_level_tag(lst_query_value[5])['dict_ret']
        s_sv_lvl_3 = o_sv_campaign_parser.validate_sv_campaign_level_tag(lst_query_value[6])['dict_ret']
        s_sv_lvl_4 = None
        del o_sv_campaign_parser

        if len(lst_query_value[7]) > 0:
            try:
                dt_regdate = datetime.strptime(lst_query_value[8], '%Y-%m-%d')
            except ValueError:
                dict_rst['b_error'] = True
                dict_rst['s_msg'] = lst_query_title[8] + ' is invalid date'
                return dict_rst
        else:
            dt_regdate = datetime.today()

        self.__g_oSvDb.executeQuery('insertCampaignAlias', n_source_id, s_media_campaign_title,
                                        n_search_rst_id, n_medium_id, s_sv_lvl_1, s_sv_lvl_2, s_sv_lvl_3, s_sv_lvl_4, dt_regdate)
        return dict_rst
        
    def add_alias_bulk(self, request):
        """ 
        copy & paste multiple campaign alias
        :param 
        """
        o_sv_campaign_parser = SvCampaignParser()
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        # begin - construct campaign alias info list
        s_multiple_campaign_alias = request.POST.get('multiple_campaign_alias')
        lst_line = s_multiple_campaign_alias.splitlines()
        # ['범퍼애드_테이블청소', 'YT', 'PS', 'DISP', 'WIPES', 'TABLE', '20190402', '2019-04-03']
        for s_line in lst_line:
            lst_single_line = s_line.split('\t')
            if len(lst_single_line) != 8:
                dict_rst['b_error'] = True
                dict_rst['s_msg'] = 'weird campaign alias info'
                return dict_rst

            s_media_campaign_title = lst_single_line[0].strip()
            n_source_id = self.__g_dictSourceTagId[lst_single_line[1].strip()]
            n_search_rst_id = self.__g_dictSearchRstTypeTagId[lst_single_line[2].strip()]
            n_medium_id = self.__g_dictMediumTypeTagId[lst_single_line[3].strip()]
            s_sv_lvl_1 = o_sv_campaign_parser.validate_sv_campaign_level_tag(lst_single_line[4])['dict_ret']
            s_sv_lvl_2 = o_sv_campaign_parser.validate_sv_campaign_level_tag(lst_single_line[5])['dict_ret']
            s_sv_lvl_3 = o_sv_campaign_parser.validate_sv_campaign_level_tag(lst_single_line[6])['dict_ret']
            s_sv_lvl_4 = None
            dt_regdate = datetime.strptime(lst_single_line[7], '%Y-%m-%d')
            self.__g_oSvDb.executeQuery('insertCampaignAlias', n_source_id, s_media_campaign_title,
                                        n_search_rst_id, n_medium_id, s_sv_lvl_1, s_sv_lvl_2, s_sv_lvl_3, s_sv_lvl_4, dt_regdate)

            del dt_regdate
        del lst_line
        # end - construct campaign alias info list
        del o_sv_campaign_parser
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
