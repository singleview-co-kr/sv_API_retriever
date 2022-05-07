# -*- coding: UTF-8 -*-
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
import pandas as pd

# singleview library
if __name__ == 'ga_int_search_log': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql
else: # for platform running
    from svcommon import sv_mysql


class SvGaIntSearchLog():
    
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
        self.__g_sTopNcnt = 100  # default word cnt rank to transmit
        self._g_oLogger = logging.getLogger(__name__)

    def __del__(self):
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        self.__g_sTblPrefix = None
        self.__g_sYesterday = None
        self.__g_dictSvAcctInfo = None
        self.__g_dictDateRange = None
        self.__g_sTopNcnt = None

    def init_var(self, dict_sv_acct_info, s_tbl_prefix, 
                    f_print_debug, f_print_progress_bar, f_continue_iteration):
        self.__g_dictSvAcctInfo = dict_sv_acct_info
        self.__continue_iteration = f_continue_iteration
        self.__print_debug = f_print_debug
        self.__print_progress_bar = f_print_progress_bar
        self.__g_sTblPrefix = s_tbl_prefix

    def proc(self, s_mode):
        # python3.7 task.py config_loc=1/1 mode=add_ga_int_searcj_sql
        dt_yesterday = datetime.now() - timedelta(1)
        self.__g_sYesterday = datetime.strftime(dt_yesterday, '%Y%m%d')
        del dt_yesterday
        self.__g_dictDateRange = {'s_start_date': 'na', 's_end_date': self.__g_sYesterday}
        
        if s_mode == 'add_ga_intsearch_sql':
            self.__add_ga_int_search_sql()

    def __add_ga_int_search_sql(self):
        """
        transfer compiled_ga_intsearch_daily table to BI db
        """
        # begin - ext bi denorm ga media date range
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
            o_sv_mysql.create_table_on_demand('_ga_intsearch_log_denorm')  # for google data studio
            lst_wc_date_range = o_sv_mysql.executeQuery('getGaIntSearchDenormDateRange')
        if lst_wc_date_range[0]['maxdate']:
            dt_maxdate = lst_wc_date_range[0]['maxdate']
            dt_startdate = dt_maxdate + timedelta(1)
            s_startdate = dt_startdate.strftime('%Y%m%d')
            if int(s_startdate) <= int(self.__g_sYesterday):
                self.__g_dictDateRange['s_start_date'] = s_startdate
        del lst_wc_date_range
        # end - ext bi denorm ga media date range

        # begin - get ga internal search daily log
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
                lst_daily_log = o_sv_mysql.executeQuery('getGaIntSearchLogFrom', s_start_date)
            else:
                self.__print_debug('get whole')
                lst_daily_log = o_sv_mysql.executeQuery('getGaIntSearchLogGross')

            if len(lst_daily_log):
                # retrieve dictionary if word count log exists
                self.__print_debug('get whole dictionary')
                dict_dictionary = {}
                lst_dictionary = o_sv_mysql.executeQuery('getAllGaIntSearchDictionary')
                for dict_single_word in lst_dictionary:
                    if not self.__continue_iteration():
                        return
                    dict_dictionary[dict_single_word['word_srl']] = {'word': dict_single_word['word'],
                                                                    'b_ignore': dict_single_word['b_ignore']}
                del lst_dictionary
        # end - get ga internal search daily log
        
        # print(dict_dictionary)
        # return

        n_idx = 0
        n_sentinel = len(lst_daily_log)

        # regarding ignored word, retrieve doubled rank than requested 
        if n_sentinel:
            self.__print_debug('retrieve top ' + str(self.__g_sTopNcnt) + ' words for the period')
            df_word_cnt = pd.DataFrame(lst_daily_log)
            df_sum_by_word_srl = df_word_cnt.groupby(['word_srl']).sum()
            del df_word_cnt
            df_word_rank = df_sum_by_word_srl.sort_values(by='cnt', ascending=False)
            lst_word_srl_to_trans = df_word_rank.index[:self.__g_sTopNcnt*2].tolist()
            del df_word_rank
        n_idx = 0
        if n_sentinel:
            self.__print_debug('transfer ga intsearch via SQL')
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.client_serve')
                o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
                for dict_single_int_search in lst_daily_log:
                    if not self.__continue_iteration():
                        return
                    if dict_dictionary[dict_single_int_search['word_srl']]['b_ignore'] == '0':
                        if dict_single_int_search['word_srl'] in lst_word_srl_to_trans:
                            # print(dict_dictionary[dict_single_int_search['word_srl']]['word'])
                            # print(dict_single_int_search['ua'])
                            # print(dict_single_int_search['cnt'])
                            # print(dict_single_int_search['logdate'])
                            o_sv_mysql.executeQuery('insertGaIntSearchDenormDailyLog', 
                                                dict_dictionary[dict_single_int_search['word_srl']]['word'],
                                                dict_single_int_search['ua'], dict_single_int_search['cnt'], 
                                                dict_single_int_search['logdate'])
                    self.__print_progress_bar(n_idx+1, n_sentinel, prefix = 'transfer ga intsearch data:', suffix = 'Complete', length = 50)
                    n_idx += 1
        elif n_sentinel == 0:
            self.__print_debug('stop transferring - no more data to update')
        del lst_word_srl_to_trans
        del lst_daily_log
        del dict_dictionary
