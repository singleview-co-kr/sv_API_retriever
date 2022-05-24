from datetime import datetime

from svcommon.sv_campaign_parser import SvCampaignParser


# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class BrdedTerm:
    """ depends on svplugins.ga_register_db.item_performance """
    # __g_bPeriodDebugMode = False
    __g_oSvDb = None
    __g_lstBrdedTerm = []
    
    def __init__(self, s_branded_trunc_path):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        super().__init__()
        self.__g_sBrandedTruncPath = s_branded_trunc_path

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # logger.debug('__del__')
        self.__g_sBrandedTruncPath = None
    
    # def activate_debug(self):
    #     self.__g_bPeriodDebugMode = True
    
    def get_list(self):
        """
        data for brded term list screen
        :return:
        """
        o_sv_campaign_parser = SvCampaignParser()
        lst_brded_term_list = o_sv_campaign_parser.get_branded_trunc(self.__g_sBrandedTruncPath)
        del o_sv_campaign_parser
        return lst_brded_term_list

    def update_list(self, request):
        """ 
        copy & paste from SV CMS web admin
        :param 
        """
        # import csv
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        # begin - construct contract info list
        s_multiple_term = request.POST.get('multiple_term')
        lst_line = s_multiple_term.splitlines()
        lst_line = list(set(lst_line))
        lst_line.sort()
        o_sv_campaign_parser = SvCampaignParser()
        o_sv_campaign_parser.set_branded_trunc(self.__g_sBrandedTruncPath, lst_line)
        del o_sv_campaign_parser
        del lst_line
        return dict_rst
