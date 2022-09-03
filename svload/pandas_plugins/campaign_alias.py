import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta

if __name__ == 'campaign_alias':  # for calling from svplugins.aw_register_db.task
    sys.path.append('../../svcommon')
    from sv_mysql import SvMySql
    from sv_campaign_parser import SvCampaignParser
else:  # for platform running
    from svcommon.sv_mysql import SvMySql
    from svcommon.sv_campaign_parser import SvCampaignParser

# for logger
import logging
logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class CampaignAliasInfo:
    """ depends on svplugins.ga_register_db.item_performance """
    # __g_bPeriodDebugMode = False
    __g_oSvDb = None
    __g_dictSourceIdTitle = None
    # __g_dictSourceTitleId = None
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
        # self.__g_dictSourceTitleId = o_sv_campaign_parser.get_source_id_title_dict(True)
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
    
    def get_list_by_period(self, s_period_from, s_period_to):
        """
        data for campaign alias list screen
        :param s_period_from:
        :param s_period_to:
        :return:
        """
        lst_alias_earliest = self.__g_oSvDb.executeQuery('getCampaignAliasEarliest')
        if 'err_code' in lst_alias_earliest[0]:  # if table not exists
            dict_alias_period = {'s_earliest_alias': '',
                                    's_latest_alias': '',
                                    's_earliest_req': '',
                                    's_latest_req': ''}
            return {'dict_alias_period': dict_alias_period, 'lst_alias_rst': []}

        lst_alias_latest = self.__g_oSvDb.executeQuery('getCampaignAliasLatest')
        if lst_alias_earliest[0]['min_date'] is None or lst_alias_latest[0]['max_date'] is None:
            dt_latest_alias = datetime.today()
            dt_earliest_alias = dt_latest_alias - relativedelta(months=6)
            dt_earliest_alias = dt_earliest_alias.replace(day=1)
        else:
            dt_earliest_alias = lst_alias_earliest[0]['min_date']
            dt_latest_alias = lst_alias_latest[0]['max_date']
        del lst_alias_earliest, lst_alias_latest

        if s_period_from is not None and s_period_to is not None:
            dt_earliest_req = datetime.strptime(s_period_from, '%Y%m%d')
            dt_latest_req = datetime.strptime(s_period_to, '%Y%m%d')
        else:
            dt_latest_req = dt_latest_alias
            dt_earliest_req = dt_latest_req - relativedelta(months=6)
            dt_earliest_req = dt_earliest_req.replace(day=1)

        lst_alias_rst = self.__g_oSvDb.executeQuery('getCampaignAliasListByPeriod', dt_earliest_req, dt_latest_req) #self.__g_oSvDb.executeQuery('getCampaignAliasList')
        for dict_single_alias in lst_alias_rst:
            dict_single_alias['source_name'] = self.__g_dictSourceIdTitle[dict_single_alias['source_id']]
            dict_sv_convention = self.__construct_sv_campaign_convention(dict_single_alias)
            s_sv_convention = dict_sv_convention['s_source_tag'] + '_' + \
                                dict_sv_convention['s_search_rst_id_tag'] + '_' + \
                                dict_sv_convention['s_medium_id_tag'] + '_' + \
                                dict_sv_convention['s_sv_lvl_1'] + '_' + \
                                dict_sv_convention['s_sv_lvl_2'] + '_' + \
                                dict_sv_convention['s_sv_lvl_3']
            dict_single_alias['sv_conventional_alias'] = s_sv_convention
            del dict_single_alias['source_id']
            del dict_sv_convention
        
        dict_alias_period = {'s_earliest_alias': dt_earliest_alias.strftime("%Y%m%d"),
                              's_latest_alias': dt_latest_alias.strftime("%Y%m%d"),
                              's_earliest_req': dt_earliest_req.strftime("%Y%m%d"),
                              's_latest_req': dt_latest_req.strftime("%Y%m%d")}
        del dt_earliest_alias, dt_latest_alias
        return {'dict_alias_period': dict_alias_period, 'lst_alias_rst': lst_alias_rst}

    def get_detail_by_media_campaign_name(self, s_media_campaign_title):
        """
        called from svplugins.aw_register_db::__validate_campaign_code()
        :param s_source_name:
        :param s_media_campaign_title:
        :return:
        """
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        # dict should be streamlined with svcommon.sv_campaign_parser
        dict_rst['dict_ret'] = {'source': 'unknown', 'source_code': '', 'rst_type': '',
                                'medium': '', 'medium_code': '', 'brd': 0,
                                'campaign1st': '00', 'campaign2nd': '00', 'campaign3rd': '00',
                                'detected': False}
        lst_alias_detail = self.__g_oSvDb.executeQuery('getCampaignAliasDetailByMediaCampaign', 
                                                        s_media_campaign_title)
        if len(lst_alias_detail) == 0:  # add new
            b_rst = self.__reserve_alias_single(s_media_campaign_title)
            if b_rst:
                dict_rst['s_msg'] = 'new_appending'
            else:
                dict_rst['b_error'] = True
                dict_rst['s_msg'] = 'failed_appending'
        else:  # retrieve old one
            if lst_alias_detail[0]['source_id'] and \
                    lst_alias_detail[0]['search_rst_id'] and \
                    lst_alias_detail[0]['medium_id']:
                dict_sv_convention = self.__construct_sv_campaign_convention(lst_alias_detail[0])
                dict_rst['dict_ret']['source'] = dict_sv_convention['s_source_title']
                dict_rst['dict_ret']['source_code'] = dict_sv_convention['s_source_tag']
                dict_rst['dict_ret']['rst_type'] = dict_sv_convention['s_search_rst_id_tag']
                dict_rst['dict_ret']['medium'] = dict_sv_convention['s_medium_title']
                dict_rst['dict_ret']['medium_code'] = dict_sv_convention['s_medium_id_tag']
                dict_rst['dict_ret']['campaign1st'] = dict_sv_convention['s_sv_lvl_1']
                dict_rst['dict_ret']['campaign2nd'] = dict_sv_convention['s_sv_lvl_2']
                dict_rst['dict_ret']['campaign3rd'] = dict_sv_convention['s_sv_lvl_3']
                dict_rst['dict_ret']['detected'] = True
            else:
                dict_rst['s_msg'] = 'new_appending'
        return dict_rst

    def get_detail_by_id(self, n_alias_id):
        """
        data for campaign alias detail screen
        :param n_alias_id:
        :return:
        """
        lst_alias_detail = self.__g_oSvDb.executeQuery('getCampaignAliasDetailById', n_alias_id)
        dict_sv_convention = self.__construct_sv_campaign_convention(lst_alias_detail[0])
        s_sv_convention = dict_sv_convention['s_source_tag'] + '_' + \
                            dict_sv_convention['s_search_rst_id_tag'] + '_' + \
                            dict_sv_convention['s_medium_id_tag'] + '_' + \
                            dict_sv_convention['s_sv_lvl_1'] + '_' + \
                            dict_sv_convention['s_sv_lvl_2'] + '_' + \
                            dict_sv_convention['s_sv_lvl_3']
        lst_alias_detail[0]['sv_conventional_alias'] = s_sv_convention
        return lst_alias_detail[0]

    def add_alias_single(self, request):
        dict_rst = self.__validate_alias_single(request)
        if dict_rst['b_error']:
            return dict_rst
        self.__g_oSvDb.executeQuery('insertCampaignAlias', 
                                        dict_rst['dict_ret']['n_source_id'],
                                        dict_rst['dict_ret']['s_media_campaign_title'], 
                                        dict_rst['dict_ret']['n_search_rst_id'],
                                        dict_rst['dict_ret']['n_medium_id'], 
                                        dict_rst['dict_ret']['s_sv_lvl_1'],
                                        dict_rst['dict_ret']['s_sv_lvl_2'], 
                                        dict_rst['dict_ret']['s_sv_lvl_3'],
                                        dict_rst['dict_ret']['s_sv_lvl_4'], 
                                        dict_rst['dict_ret']['dt_regdate'])
        return dict_rst
    
    def update_alias(self, request):
        """
        data for campaign alias detail screen
        :return:
        """
        n_alias_id = int(request.POST['alias_id'])
        dict_rst = self.__validate_alias_single(request)
        if dict_rst['b_error']:
            return dict_rst
        self.__g_oSvDb.executeQuery('updateCampaignAliasById', 
                                        dict_rst['dict_ret']['n_source_id'],
                                        dict_rst['dict_ret']['n_search_rst_id'],
                                        dict_rst['dict_ret']['n_medium_id'], 
                                        dict_rst['dict_ret']['s_sv_lvl_1'],
                                        dict_rst['dict_ret']['s_sv_lvl_2'], 
                                        dict_rst['dict_ret']['s_sv_lvl_3'],
                                        dict_rst['dict_ret']['s_sv_lvl_4'],
                                        n_alias_id)
        return

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

            if lst_single_line[0] == 'campaign_name' and lst_single_line[1] == 'source':  # mean header
                continue

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
    
    def __reserve_alias_single(self, s_media_campaign_title):
        """ reserve alias for newly found non-sv-conventional campaign title """
        lst_rst = self.__g_oSvDb.executeQuery('insertCampaignAlias', 
                                        0, # ['n_source_id'],
                                        s_media_campaign_title, 
                                        0, # ['n_search_rst_id'],
                                        0, # ['n_medium_id'], 
                                        None, # ['s_sv_lvl_1'],
                                        None, # ['s_sv_lvl_2'], 
                                        None, # ['s_sv_lvl_3'],
                                        None, # ['s_sv_lvl_4'], 
                                        datetime.today())
        if 'id' in lst_rst[0]:
            return True
        else:  # if sql insert failed
            return False
            
    def __validate_alias_single(self, request):
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

        if request.POST['act'] != 'update_alias' and len(lst_query_value[7]) > 0:
            try:
                dt_regdate = datetime.strptime(lst_query_value[7], '%Y-%m-%d')
            except ValueError:
                dict_rst['b_error'] = True
                dict_rst['s_msg'] = lst_query_title[7] + ' is invalid date'
                return dict_rst
        else:
            dt_regdate = datetime.today()

        dict_rst['dict_ret'] = {'n_source_id': n_source_id, 
                                's_media_campaign_title': s_media_campaign_title, 
                                'n_search_rst_id': n_search_rst_id, 
                                'n_medium_id': n_medium_id, 
                                's_sv_lvl_1': s_sv_lvl_1, 
                                's_sv_lvl_2': s_sv_lvl_2, 
                                's_sv_lvl_3': s_sv_lvl_3, 
                                's_sv_lvl_4': s_sv_lvl_4, 
                                'dt_regdate': dt_regdate}
        return dict_rst

    def __construct_sv_campaign_convention(self, dict_single_alias):
        s_source_title = self.__g_dictSourceIdTitle[dict_single_alias['source_id']]
        s_source_tag = self.__get_source_tag_by_id(dict_single_alias['source_id'])
        s_search_rst_id_tag = self.__get_search_rst_tag_by_id(dict_single_alias['search_rst_id'])
        s_medium_title = self.__g_dictMediumTypeIdTitle[dict_single_alias['medium_id']]
        s_medium_id_tag = self.__get_medium_tag_by_id(dict_single_alias['medium_id'])
        s_sv_lvl_1 = dict_single_alias['sv_lvl_1'] if dict_single_alias['sv_lvl_1'] else 'None'
        s_sv_lvl_2 = dict_single_alias['sv_lvl_2'] if dict_single_alias['sv_lvl_2'] else 'None'
        s_sv_lvl_3 = dict_single_alias['sv_lvl_3'] if dict_single_alias['sv_lvl_3'] else 'None'
        return {'s_source_title': s_source_title, 's_source_tag': s_source_tag, 
                's_search_rst_id_tag': s_search_rst_id_tag, 
                's_medium_title': s_medium_title, 's_medium_id_tag': s_medium_id_tag, 
                's_sv_lvl_1': s_sv_lvl_1, 's_sv_lvl_2': s_sv_lvl_2, 's_sv_lvl_3': s_sv_lvl_3}

    def __get_source_tag_by_id(self, n_source_id):
        return self.__g_dictSourceIdTag[n_source_id]

    def __get_search_rst_tag_by_id(self, n_search_rst_id):
        return self.__g_dictSearchRstTypeIdTag[n_search_rst_id]
    
    def __get_medium_tag_by_id(self, n_medium_id):
        return self.__g_dictMediumTypeIdTag[n_medium_id]
