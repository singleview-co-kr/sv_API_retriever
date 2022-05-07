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
import calendar
import re

# singleview library
if __name__ == 'ga_media_log': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql
else: # for platform running
    from svcommon import sv_mysql


class SvGaMediaLog():
    
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
        self.__g_sReplaceYearMonth = None
        self.__g_dictSvAcctInfo = None
        self.__g_dictDateRange = None
        self._g_oLogger = logging.getLogger(__name__)

    def __del__(self):
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        self.__g_sTblPrefix = None
        self.__g_sYesterday = None
        self.__g_sReplaceYearMonth = None
        self.__g_dictSvAcctInfo = None
        self.__g_dictDateRange = None

    def init_var(self, dict_sv_acct_info, s_tbl_prefix, 
                    f_print_debug, f_print_progress_bar, f_continue_iteration,
                    s_replace_year_month):
        self.__g_dictSvAcctInfo = dict_sv_acct_info
        self.__continue_iteration = f_continue_iteration
        self.__print_debug = f_print_debug
        self.__print_progress_bar = f_print_progress_bar
        self.__g_sTblPrefix = s_tbl_prefix
        self.__g_sReplaceYearMonth = s_replace_year_month

    def proc(self, s_mode):
        # python3.7 task.py config_loc=1/1 mode=add_ga_media_sql
        # python3.7 task.py config_loc=1/1 mode=update_ga_media_sql yyyymm=202201
        dt_yesterday = datetime.now() - timedelta(1)
        self.__g_sYesterday = datetime.strftime(dt_yesterday, '%Y%m%d')
        self.__g_dictDateRange = {'s_start_date': 'na', 's_end_date': self.__g_sYesterday}
        del dt_yesterday
        if s_mode == 'add_ga_media_sql':
            self.__add_ga_media_sql()
        elif s_mode == 'update_ga_media_sql':
            self.__update_period_ga_media_sql()

    def __update_period_ga_media_sql(self):
        """
        transfer and update compiled_ga_media_daily table to BI db for designated period
        """
        if self.__g_sReplaceYearMonth == None:
            self.__print_debug('stop -> invalid yyyymm0')
            return
        s_pattern = r"^[0-9]{4}(0[1-9]|1[0-2])$"
        o_found_month = re.search(s_pattern, self.__g_sReplaceYearMonth)
        if not o_found_month:
            self.__print_debug('stop -> invalid yyyymm1')
            return
        n_yr = int(self.__g_sReplaceYearMonth[:4])
        n_mo = int(self.__g_sReplaceYearMonth[4:None])
        try:
            lstMonthRange = calendar.monthrange(n_yr, n_mo)
        except calendar.IllegalMonthError:
            self.__print_debug( 'stop -> invalid yyyymm2' )
            return
        
        self.__print_debug('delete replacing period data')
        s_data_year = self.__g_sReplaceYearMonth[:4]  # get year 4 digit
        s_data_mo = str(int(self.__g_sReplaceYearMonth[-2:]))  # get month 2 digit and remove 0
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
            o_sv_mysql.executeQuery('deleteCompiledLogByPeriod', s_data_year, s_data_mo)

        self.__print_debug('add period data')
        s_start_date = self.__g_sReplaceYearMonth[:4] + '-' + self.__g_sReplaceYearMonth[4:None] + '-01'
        s_end_date = self.__g_sReplaceYearMonth[:4] + '-' + self.__g_sReplaceYearMonth[4:None] + '-' + str(lstMonthRange[1])
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            lst_compiled_log = o_sv_mysql.executeQuery('getCompiledGaMediaLogPeriod', s_start_date, s_end_date)
        n_idx = 0
        n_sentinel = len(lst_compiled_log)
        if n_sentinel:
            self.__print_debug('transfer ga media via SQL')
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.client_serve')
                o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
                for dict_single_log in lst_compiled_log:
                    if not self.__continue_iteration():
                        return
                    o_sv_mysql.executeQuery('insertCompiledGaMediaDailyLog', 
                                            dict_single_log['log_srl'], dict_single_log['media_ua'],
                                            dict_single_log['media_term'], dict_single_log['media_source'],
                                            dict_single_log['media_rst_type'], dict_single_log['media_media'],
                                            dict_single_log['media_brd'], dict_single_log['media_camp1st'],
                                            dict_single_log['media_camp2nd'], dict_single_log['media_camp3rd'],
                                            dict_single_log['media_raw_cost'], dict_single_log['media_agency_cost'],
                                            dict_single_log['media_cost_vat'], dict_single_log['media_imp'],
                                            dict_single_log['media_click'], dict_single_log['media_conv_cnt'],
                                            dict_single_log['media_conv_amnt'], dict_single_log['in_site_tot_session'],
                                            dict_single_log['in_site_tot_new'], dict_single_log['in_site_tot_bounce'],
                                            dict_single_log['in_site_tot_duration_sec'], dict_single_log['in_site_tot_pvs'],
                                            dict_single_log['in_site_trs'], dict_single_log['in_site_revenue'],
                                            dict_single_log['in_site_registrations'], dict_single_log['logdate'])
                    self.__print_progress_bar(n_idx+1, n_sentinel, prefix = 'transfer wc data:', suffix = 'Complete', length = 50)
                    n_idx += 1
        elif n_sentinel == 0:
            self.__print_debug('stop transferring - no more data to update')
        del lst_compiled_log
        return

    def __add_ga_media_sql(self):
        """
        transfer compiled_ga_media_daily table to BI db
        """
        # begin - ext bi denorm ga media date range
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
            o_sv_mysql.create_table_on_demand('_compiled_ga_media_daily_log')  # for google data studio
            lst_wc_date_range = o_sv_mysql.executeQuery('getCompiledGaMediaDateRange')
        if lst_wc_date_range[0]['maxdate']:
            dt_maxdate = lst_wc_date_range[0]['maxdate']
            dt_startdate = dt_maxdate + timedelta(1)
            s_startdate = dt_startdate.strftime('%Y%m%d')
            if int(s_startdate) <= int(self.__g_sYesterday):
                self.__g_dictDateRange['s_start_date'] = s_startdate
        del lst_wc_date_range
        # end - ext bi denorm ga media date range

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
                lst_compiled_log = o_sv_mysql.executeQuery('getCompiledGaMediaLogFrom', s_start_date)
            else:
                self.__print_debug('get whole')
                lst_compiled_log = o_sv_mysql.executeQuery('getCompiledGaMediaLogGross')
        n_idx = 0
        n_sentinel = len(lst_compiled_log)
        if n_sentinel:
            self.__print_debug('transfer ga media via SQL')
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.client_serve')
                o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
                for dict_single_log in lst_compiled_log:
                    if not self.__continue_iteration():
                        return
                    o_sv_mysql.executeQuery('insertCompiledGaMediaDailyLog', 
                                            dict_single_log['log_srl'], dict_single_log['media_ua'],
                                            dict_single_log['media_term'], dict_single_log['media_source'],
                                            dict_single_log['media_rst_type'], dict_single_log['media_media'],
                                            dict_single_log['media_brd'], dict_single_log['media_camp1st'],
                                            dict_single_log['media_camp2nd'], dict_single_log['media_camp3rd'],
                                            dict_single_log['media_raw_cost'], dict_single_log['media_agency_cost'],
                                            dict_single_log['media_cost_vat'], dict_single_log['media_imp'],
                                            dict_single_log['media_click'], dict_single_log['media_conv_cnt'],
                                            dict_single_log['media_conv_amnt'], dict_single_log['in_site_tot_session'],
                                            dict_single_log['in_site_tot_new'], dict_single_log['in_site_tot_bounce'],
                                            dict_single_log['in_site_tot_duration_sec'], dict_single_log['in_site_tot_pvs'],
                                            dict_single_log['in_site_trs'], dict_single_log['in_site_revenue'],
                                            dict_single_log['in_site_registrations'], dict_single_log['logdate'])
                    self.__print_progress_bar(n_idx+1, n_sentinel, prefix = 'transfer ga media data:', suffix = 'Complete', length = 50)
                    n_idx += 1
        elif n_sentinel == 0:
            self.__print_debug('stop transferring - no more data to update')
        return
