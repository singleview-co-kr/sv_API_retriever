from svcommon.sv_campaign_parser import SvCampaignParser
from svcommon.sv_plugin import svPluginDaemonJob


# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class SeoTrackingTerm:
    # __g_bPeriodDebugMode = False

    def __init__(self, o_sv_db):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        if not o_sv_db:  # refer to an external db instance to minimize data class
            raise Exception('invalid db handler')
        self.__g_oSvDb = o_sv_db
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        # logger.debug('__del__')
        self.__g_oSvDb = None

    # def activate_debug(self):
    #     self.__g_bPeriodDebugMode = True

    def get_list(self):
        """
        data for SEO term list screen
        :return:
        """
        return self.__g_oSvDb.executeQuery('getSeoTrackingTermList')

    def add_term(self, request):
        s_new_seo_tracking_term = request.POST.get('seo_tracking_term')
        self.__g_oSvDb.executeQuery('insertSeoTrackingTerm', s_new_seo_tracking_term)
        return

    def toggle_term(self, request):
        n_toggle_seo_tracking_term_srl = request.POST.get('toggle_morpheme_srl')
        lst_rst = self.__g_oSvDb.executeQuery('getSeoTrackingTermBySrl', n_toggle_seo_tracking_term_srl)
        s_new_toggle = 1 if lst_rst[0]['b_toggle'] == '0' else '0'
        self.__g_oSvDb.executeQuery('updateSeoTrackingTermToggleBySrl', s_new_toggle, n_toggle_seo_tracking_term_srl)
        return


class BrdedTerm:
    # __g_bPeriodDebugMode = False
    
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

    def update_list(self, request, s_config_loc_param):
        """ 
        update brded term list
        :param 
        """
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        s_multiple_term = request.POST.get('multiple_term')
        lst_line = s_multiple_term.splitlines()
        o_sv_campaign_parser = SvCampaignParser()
        dict_update_rst = o_sv_campaign_parser.set_branded_trunc(self.__g_sBrandedTruncPath, lst_line)
        del o_sv_campaign_parser
        del lst_line

        if dict_update_rst['updated']:  # initialize a tbl compiled_ga_media_daily_log
            o_sv_plugin_daemon = svPluginDaemonJob('integrate_db', s_config_loc_param, 'mode=clear')
            del o_sv_plugin_daemon
        return dict_rst
