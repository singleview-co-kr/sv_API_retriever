from dateutil.relativedelta import relativedelta
from django.utils.html import strip_tags
from datetime import datetime, date
import pandas as pd

# for logger
import logging

logger = logging.getLogger(__name__)  # __file__ # logger.debug('debug msg')


class GaItem:
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
        pass

    # def activate_debug(self):
    #     self.__g_bPeriodDebugMode = True

    def get_list(self):
        """
        data for item list screen
        """
        lst_rst = self.__g_oSvDb.executeQuery('getGaItemList')
        # for dict_single_ga_item in lst_rst:
        #     print(dict_single_ga_item)
        # del lst_rst
        return {'lst_catalog': lst_rst}

    def update_budget(self, n_budget_id, request):
        dict_rst = self.__validate_budget_info(request)
        if not dict_rst['b_error']:
            dict_budget = dict_rst['dict_ret']
            self.__g_oSvDb.executeQuery('updateBudgetByBudgetId', dict_budget['n_acct_id'],
                                        dict_budget['dt_budget_alloc_yr_mo'].year, dict_budget['dt_budget_alloc_yr_mo'].month,
                                        dict_budget['s_budget_memo'], dict_budget['n_budget_target_amnt_inc_vat'],
                                        dict_budget['dt_budget_date_begin'], dict_budget['dt_budget_date_end'], n_budget_id)
            del dict_budget
        return dict_rst

    def add_budget(self, n_brand_id, request):
        """
        add budget
        :param n_brand_id:
        :param request:
        :return:
        """
        dict_rst = self.__validate_budget_info(request)
        if not dict_rst['b_error']:
            dict_budget = dict_rst['dict_ret']
            self.__g_oSvDb.executeQuery('insertBudget', request.user.pk, dict_budget['n_acct_id'],
                                        dict_budget['dt_budget_alloc_yr_mo'].year, dict_budget['dt_budget_alloc_yr_mo'].month,
                                        dict_budget['s_budget_memo'], dict_budget['n_budget_target_amnt_inc_vat'],
                                        dict_budget['dt_budget_date_begin'], dict_budget['dt_budget_date_end'])
            del dict_budget
        return dict_rst
