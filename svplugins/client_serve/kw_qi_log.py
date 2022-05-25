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

# singleview library
if __name__ == 'kw_qi_log': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql
    import sv_campaign_parser
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser


class SvKeywordQi():
    __g_dictSourceInverted = None

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
        # python3.7 task.py config_loc=1/1 mode=add_nvr_qi_sql

        o_sv_campaign_parser = sv_campaign_parser.SvCampaignParser()
        self.__g_dictSourceInverted = o_sv_campaign_parser.get_source_id_dict(True)
        del o_sv_campaign_parser

        # print(self.__g_dictSourceInverted)
        # return

        dt_yesterday = datetime.now() - timedelta(1)
        self.__g_sYesterday = datetime.strftime(dt_yesterday, '%Y%m%d')
        self.__g_dictDateRange = {'s_start_date': 'na', 's_end_date': self.__g_sYesterday}
        del dt_yesterday
        # if s_mode == 'add_ga_media_sql':
        #     self._add_ga_media_sql()
        self.__add_new_2_bi_db()

    def __add_new_2_bi_db(self):
        """
        transfer de-normalized table to BI db
        """
        lst_nvad_master_qi = None
        # begin - ext bi denorm word count date range
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
            o_sv_mysql.create_table_on_demand('_sv_qi_log_denorm')  # for google data studio
            lst_wc_date_range = o_sv_mysql.executeQuery('getSvQiDenormDateRange')
        if lst_wc_date_range[0]['maxdate']:
            dt_maxdate = lst_wc_date_range[0]['maxdate']
            dt_startdate = dt_maxdate + timedelta(1)
            s_startdate = dt_startdate.strftime('%Y%m%d')
            if int(s_startdate) <= int(self.__g_sYesterday):
                self.__g_dictDateRange['s_start_date'] = s_startdate
        del lst_wc_date_range
        # end - ext bi denorm naver master QI date range

        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            if not self.__continue_iteration():
                return
            # begin - retrieve naver master QI
            s_end_date = datetime.strptime(self.__g_dictDateRange['s_end_date'], '%Y%m%d').strftime('%Y-%m-%d')
            if self.__g_dictDateRange['s_start_date'] == 'na':  # get whole wc
                self.__print_debug('get whole naver master QI')
                lst_nvad_master_qi = o_sv_mysql.executeQuery('getAllNvadMasterQiTo', s_end_date)
            else:
                s_start_date = datetime.strptime(self.__g_dictDateRange['s_start_date'], '%Y%m%d').strftime('%Y-%m-%d')
                self.__print_debug('naver master QI get from ' + s_start_date + ' to ' + s_end_date)
                lst_nvad_master_qi = o_sv_mysql.executeQuery('getNvadMasterQiFromTo', self.__g_dictDateRange['s_start_date'], s_end_date)
            # end - retrieve naver master QI

            # begin - retrieve naver master ad grp
            lst_nvad_ad_grp = o_sv_mysql.executeQuery('getAllNvadAdGrp')
            # end - retrieve naver master ad grp

        dict_naver_ad_grp = {}
        for dict_single_ad_grp in lst_nvad_ad_grp:
            dict_naver_ad_grp[dict_single_ad_grp['ad_group_id']] = dict_single_ad_grp['ad_group_name']
        del lst_nvad_ad_grp

        for dict_single_qi in lst_nvad_master_qi:
            dict_single_qi['ad_group_name'] = dict_naver_ad_grp[dict_single_qi['ad_group_id']]
            del dict_single_qi['ad_group_id']
        
        # print(lst_nvad_master_qi)
        
        n_idx = 0
        n_sentinel = len(lst_nvad_master_qi)
        if n_sentinel:
            self.__print_debug('transfer naver master QI via SQL')
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.client_serve')
                o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
                for dict_single_qi in lst_nvad_master_qi:
                    if not self.__continue_iteration():
                        return
                    o_sv_mysql.executeQuery('insertNvadMasterQiDenorm', self.__g_dictSourceInverted['naver'], 
                                                dict_single_qi['ad_group_name'], dict_single_qi['ad_keyword'], 
                                                dict_single_qi['quality_index'], dict_single_qi['check_date'])
                    self.__print_progress_bar(n_idx+1, n_sentinel, prefix = 'transfer naver master QI data:', suffix = 'Complete', length = 50)
                    n_idx += 1
        del lst_nvad_master_qi
