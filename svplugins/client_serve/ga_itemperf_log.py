# UTF-8 테스트

# Copyright 2021 singleview.co.kr, Inc.

# You are hereby granted a non-exclusive, worldwide, royalty-free license to
# use, copy, modify, and distribute this software in source code or binary
# form for use in connection with the web services and APIs provided by
# singleview.co.kr.

# As with any software that integrates with the singleview.co.kr platform, 
# your use of this software is subject to the Facebook Developer Principles 
# and Policies [http://singleview.co.kr/api_policy/]. This copyright 
# notice shall be included in all copies or substantial portions of the 
# software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

# standard library
import sys
import logging
from datetime import datetime
from datetime import timedelta

# singleview library
if __name__ == 'ga_itemperf_log': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql
else: # for platform running
    from svcommon import sv_mysql


class SvGaItemPerfLog():
    
    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        # print('item:__init__')
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        self.__g_sTblPrefix = None
        self.__g_sYesterday = None
        self.__g_dictSvAcctInfo = None
        self.__g_dictDateRange = None
        self._g_oLogger = logging.getLogger(__name__)

    def __del__(self):
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        self.__g_sTblPrefix = None
        self.__g_sYesterday = None
        self.__g_dictSvAcctInfo = None
        self.__g_dictDateRange = None

    def init_var(self, dict_sv_acct_info, s_tbl_prefix, 
                    f_print_debug, f_print_progress_bar, f_continue_iteration):
        self.__g_dictSvAcctInfo = dict_sv_acct_info
        self.__continue_iteration = f_continue_iteration
        self.__print_debug = f_print_debug
        self.__print_progress_bar = f_print_progress_bar
        self.__g_sTblPrefix = s_tbl_prefix

    def proc(self, s_mode):
        # python3.7 task.py config_loc=1/1 mode=add_ga_itemperf_sql
        dt_yesterday = datetime.now() - timedelta(1)
        self.__g_sYesterday = datetime.strftime(dt_yesterday, '%Y%m%d')
        del dt_yesterday
        self.__g_dictDateRange = {'s_start_date': 'na', 's_end_date': self.__g_sYesterday}
        
        if s_mode == 'add_ga_itemperf_sql':
            self.__add_ga_itemperf_sql()
        elif s_mode == 'clear_ga_itemperf_sql':
            self.__clear_ga_itemperf_sql()

    def __clear_ga_itemperf_sql(self):
        """
        clear transferred compiled_ga_intsearch_daily table on BI db
        """
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
            o_sv_mysql.create_table_on_demand('_ga_itemperf_log_denorm')  # for google data studio
            o_sv_mysql.executeQuery('deleteGaItemPerfDenormAll')
        self.__print_debug('cleared')

    def __add_ga_itemperf_sql(self):
        """
        transfer compiled_ga_intsearch_daily table to BI db
        """
        # begin - ext bi denorm ga item perf date range
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
            o_sv_mysql.create_table_on_demand('_ga_itemperf_log_denorm')  # for google data studio
            lst_catalog_date_range = o_sv_mysql.executeQuery('getGaItemPerfDenormDateRange')
        
        if lst_catalog_date_range[0]['maxdate']:
            dt_maxdate = lst_catalog_date_range[0]['maxdate']
            dt_startdate = dt_maxdate + timedelta(1)
            s_startdate = dt_startdate.strftime('%Y%m%d')
            if int(s_startdate) <= int(self.__g_sYesterday):
                self.__g_dictDateRange['s_start_date'] = s_startdate
        del lst_catalog_date_range
        # end - ext bi denorm ga item perf date range

        # begin - get ga item perf daily log
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            if not self.__continue_iteration():
                return
            if self.__g_dictDateRange['s_start_date'] != 'na':
                s_start_date = self.__g_dictDateRange['s_start_date']
                s_start_date = datetime.strptime(s_start_date, '%Y%m%d').strftime('%Y-%m-%d')
                self.__print_debug('get from ' + s_start_date)
                lst_daily_log = o_sv_mysql.executeQuery('getGaItemPerfLogFrom', s_start_date)
            else:
                self.__print_debug('get whole')
                lst_daily_log = o_sv_mysql.executeQuery('getGaItemPerfLogGross')

            if len(lst_daily_log):
                # retrieve catalog info if item log exists
                self.__print_debug('get whole catalog info')
                dict_item_info = {}
                lst_item_info = o_sv_mysql.executeQuery('getAllGaItemInfo')
                for dict_single_word in lst_item_info:
                    if not self.__continue_iteration():
                        return
                    dict_item_info[dict_single_word['item_srl']] = {'item_title': dict_single_word['item_title'],
                                                                    'b_ignore': dict_single_word['b_ignore']}
                del lst_item_info
        # end - get ga item perf daily log

        # begin - set catalog hierarch
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            if not self.__continue_iteration():
                return
            dict_arranged_catalog_depth = self.__get_cat_depth_dictionary(o_sv_mysql)
        # end - set catalog hierarch
        n_idx = 0
        n_sentinel = len(lst_daily_log)
        if n_sentinel:
            self.__print_debug('transfer ga item performance via SQL')
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.client_serve')
                o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
                for dict_single_item_log in lst_daily_log:
                    if not self.__continue_iteration():
                        return
                    if dict_item_info[dict_single_item_log['item_srl']]['b_ignore'] == '0':
                        if dict_single_item_log['item_srl'] in dict_arranged_catalog_depth:
                            lst_cat_depth_info = dict_arranged_catalog_depth[dict_single_item_log['item_srl']]
                            s_cat1 = lst_cat_depth_info[0] if self.__is_index_in_list(lst_cat_depth_info, 0) else ''
                            s_cat2 = lst_cat_depth_info[1] if self.__is_index_in_list(lst_cat_depth_info, 1) else ''
                            s_cat3 = lst_cat_depth_info[2] if self.__is_index_in_list(lst_cat_depth_info, 2) else ''
                            o_sv_mysql.executeQuery('insertGaItemPerfDenormDailyLog', 
                                                dict_item_info[dict_single_item_log['item_srl']]['item_title'],
                                                s_cat1, s_cat2, s_cat3,
                                                dict_single_item_log['ua'], 
                                                dict_single_item_log['imp_list'],
                                                dict_single_item_log['click_list'],
                                                dict_single_item_log['imp_detail'],
                                                dict_single_item_log['freq_cart'],
                                                dict_single_item_log['qty_cart'],
                                                dict_single_item_log['qty_cart_remove'],
                                                dict_single_item_log['amnt_pur'],
                                                dict_single_item_log['freq_pur'],
                                                dict_single_item_log['freq_cko'],
                                                dict_single_item_log['qty_cko'],
                                                dict_single_item_log['logdate'])
                    self.__print_progress_bar(n_idx+1, n_sentinel, prefix = 'transfer ga item performance:', suffix = 'Complete', length = 50)
                    n_idx += 1
        elif n_sentinel == 0:
            self.__print_debug('stop transferring - no more data to update')
        del lst_daily_log
        del dict_item_info
        del dict_arranged_catalog_depth
    
    def __is_index_in_list(self, lst_to_check, n_idx):
        return n_idx < len(lst_to_check)

    def __get_cat_depth_dictionary(self, o_sv_mysql):
        """ 
        construct cat depth dictionary 
        this method should be streamlined with svload.pandas_plugins.ga_item.__get_cat_depth_dictionary()
        """
        dict_max_depth = {}
        dict_arranged_catalog_depth = {}
        lst_cat_depth_rst = o_sv_mysql.executeQuery('getGaItemDepthAll')
        for dict_single_cat in lst_cat_depth_rst:
            n_item_srl = dict_single_cat['item_srl']
            if n_item_srl not in dict_arranged_catalog_depth:
                dict_arranged_catalog_depth[n_item_srl] = []
            dict_arranged_catalog_depth[n_item_srl].append(dict_single_cat)
            if n_item_srl not in dict_max_depth:
                dict_max_depth[n_item_srl] = 0
            dict_max_depth[n_item_srl] += 1
        del lst_cat_depth_rst
        n_max_depth = max(dict_max_depth.values())

        dict_cat_info_by_item_srl = {}
        for n_item_srl in dict_max_depth:
            if n_item_srl not in dict_cat_info_by_item_srl:
                dict_cat_info_by_item_srl[n_item_srl] = []
            for i in range(0,n_max_depth):
                dict_cat_info_by_item_srl[n_item_srl].append('')
        del dict_max_depth

        for n_item_srl, lst_cat_depth in dict_arranged_catalog_depth.items():
            for dict_single_cat_depth in lst_cat_depth:
                n_nth_depth = dict_single_cat_depth['cat_depth'] - 1
                dict_cat_info_by_item_srl[n_item_srl][n_nth_depth] = dict_single_cat_depth['cat_title']
        del dict_arranged_catalog_depth
        return dict_cat_info_by_item_srl
