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
if __name__ == 'edi_log': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql

    sys.path.append('../edi_register_db')
    import sv_hypermart_model
else: # for platform running
    from svcommon import sv_mysql


class SvEdiLog():
    
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

        self.__g_dictSkuInfoById = {}
        self.__g_dictBranchInfoById = {}
        self.__g_nLimitToSingleQuery = 100000  # prevent memory dump, when loads big data

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

    def proc_edi_log(self, s_mode):
        # python3.7 task.py config_loc=1/1 mode=add_edi_sql
        dt_yesterday = datetime.now() - timedelta(1)
        self.__g_sYesterday = datetime.strftime(dt_yesterday, '%Y%m%d')
        self.__g_dictDateRange = {'s_start_date': 'na', 's_end_date': self.__g_sYesterday}
        del dt_yesterday
        self.__extract_branch_and_skus()
        # if s_mode == 'add_ga_media_sql':
        #     self._add_ga_media_sql()
        self.__extract_4_google_data_studio()
    
    def __extract_branch_and_skus(self):
        """
        retrieve branches and skus info
        :param dict_param:
        :return:
        """
        dict_branch_by_title = sv_hypermart_model.SvHyperMartType.get_dict_by_title()
        dict_branch_type = sv_hypermart_model.BranchType.get_dict_by_title()
        o_mart_geo_info = sv_hypermart_model.SvHypermartGeoInfo()

        lst_hypermart_geo_info = o_mart_geo_info.lst_hypermart_geo_info
        if len(lst_hypermart_geo_info):
            for dict_single_branch in o_mart_geo_info.lst_hypermart_geo_info:
                n_hypermart_id = dict_branch_by_title[dict_single_branch['hypermart_name']]
                n_branch_type_id = dict_branch_type[dict_single_branch['branch_type']]
                dict_branch = {'mart': dict_single_branch['hypermart_name'],  # emart, lottemart
                               'type': dict_single_branch['branch_type'],  # on & off
                               'name': dict_single_branch['name'],
                               'do': dict_single_branch['do_name'],
                               'si': dict_single_branch['si_name'],
                               'gu': dict_single_branch['gu_gun'],
                               'dong': dict_single_branch['dong_myun_ri'],
                               'longi': dict_single_branch['longitude'],
                               'lati': dict_single_branch['latitude']}
                self.__g_dictBranchInfoById[dict_single_branch['id']] = dict_branch
        else:
            del dict_branch_by_title
            del dict_branch_type
            del o_mart_geo_info
            print('excel extraction failure - no branch info')
            return
        del dict_branch_by_title
        del dict_branch_type
        del o_mart_geo_info
        # write selected sku info csv
        # retrieve account specific SKU info dictionary from account dependent table
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            if not self.__continue_iteration():
                return
            lst_sku_info_rst = o_sv_mysql.executeQuery('getEdiSkuAccepted', 1)
        
        if len(lst_sku_info_rst):
            for dict_single_sku in lst_sku_info_rst:
                self.__g_dictSkuInfoById[dict_single_sku['id']] = {
                    'name': dict_single_sku['item_name']
                }
        del lst_sku_info_rst
        print(self.__g_dictSkuInfoById)

    def __extract_4_google_data_studio(self):
        """
        retrieve specific period for Google Data Studio
        """
        self.__print_debug('start edi extraction')

        # begin - ext bi denorm word count date range
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
            o_sv_mysql.create_table_on_demand('_edi_daily_log_denorm')  # for google data studio
            lst_wc_date_range = o_sv_mysql.executeQuery('getEdiDenormDateRange')
        if lst_wc_date_range[0]['maxdate']:
            dt_maxdate = lst_wc_date_range[0]['maxdate']
            dt_startdate = dt_maxdate + timedelta(1)
            s_startdate = dt_startdate.strftime('%Y%m%d')
            if int(s_startdate) <= int(self.__g_sYesterday):
                self.__g_dictDateRange['s_start_date'] = s_startdate
        del lst_wc_date_range
        # end - ext bi denorm word count date range

        # print(self.__g_dictDateRange)
        # return
        # if not dict_param['s_start_date'] or not dict_param['s_end_date']:
        #     print('excel extraction failure - no period selected')
        #     return

        lst_mart = ['Emart', 'Ltmart']
        for s_mart_title in lst_mart:
            s_log_cnt_query = 'get{s_mart_title}LogCountByPeriod'.format(s_mart_title=s_mart_title)
            lst_log_count = self.__g_oSvDb.executeQuery(s_log_cnt_query, self.__g_dictDateRange['s_start_date'], self.__g_dictDateRange['s_start_date'])
            n_edi_log_count = lst_log_count[0]['count(*)']
            del lst_log_count

            s_performance_log_query = 'get{s_mart_title}LogByPeriod'.format(s_mart_title=s_mart_title)
            # begin - mart extraction by mart
            n_limit = self.__g_nLimitToSingleQuery
            n_offset = 0
            dict_param_tmp = {'s_period_start': self.__g_dictDateRange['s_start_date'],
                              's_period_end': self.__g_dictDateRange['s_end_date'],
                              'n_offset': n_offset, 'n_limit': n_limit}

            # very big data causes memory dump, if retrieve at single access
            while n_edi_log_count > 0:
                dict_param_tmp['n_offset'] = n_offset
                dict_param_tmp['n_limit'] = n_limit
                n_offset = n_offset + n_limit
                n_edi_log_count = n_edi_log_count - n_limit
                if n_limit >= n_edi_log_count:
                    n_limit = n_edi_log_count

                lst_log_period = self.__g_oSvDb.executeDynamicQuery(s_performance_log_query, dict_param_tmp)
                # write csv body
                for dict_single_log in lst_log_period:
                    b_refund = 0
                    if int(dict_single_log['qty']) < 0:
                        b_refund = 1
                        n_amnt = dict_single_log['amnt'] * -1
                    else:
                        n_amnt = dict_single_log['amnt']

                    if self.__g_dictBranchInfoById[dict_single_log['branch_id']]['lati']:
                        s_latitude_longitude = self.__g_dictBranchInfoById[dict_single_log['branch_id']]['lati'] + ',' + \
                                                self.__g_dictBranchInfoById[dict_single_log['branch_id']]['longi']
                    else:
                        s_latitude_longitude = None
                    self.__g_oSvDb.executeQuery('insertEdiDailyLogDenorm',
                                                self.__g_dictBranchInfoById[dict_single_log['branch_id']]['mart'],
                                                self.__g_dictBranchInfoById[dict_single_log['branch_id']]['type'],
                                                self.__g_dictBranchInfoById[dict_single_log['branch_id']]['name'],
                                                self.__g_dictSkuInfoById[dict_single_log['item_id']]['name'],
                                                dict_single_log['qty'], n_amnt,
                                                self.__g_dictBranchInfoById[dict_single_log['branch_id']]['do'],
                                                self.__g_dictBranchInfoById[dict_single_log['branch_id']]['si'],
                                                self.__g_dictBranchInfoById[dict_single_log['branch_id']]['gu'],
                                                self.__g_dictBranchInfoById[dict_single_log['branch_id']]['dong'],
                                                s_latitude_longitude,
                                                dict_single_log['logdate'])
                del lst_log_period
        # end - extraction by mart
        # specific period for excel pivoting end
        print('excel extraction succeed')

    def __add_new_2_bi_db(self):
        """
        transfer de-normalized table to BI db
        """
        # begin - ext bi denorm word count date range
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
            o_sv_mysql.create_table_on_demand('_wc_word_cnt_denorm')  # for google data studio
            lst_wc_date_range = o_sv_mysql.executeQuery('getWordCountDenormDateRange')
        if lst_wc_date_range[0]['maxdate']:
            dt_maxdate = lst_wc_date_range[0]['maxdate']
            dt_startdate = dt_maxdate + timedelta(1)
            s_startdate = dt_startdate.strftime('%Y%m%d')
            if int(s_startdate) <= int(self.__g_sYesterday):
                self.__g_dictDateRange['s_start_date'] = s_startdate
        del lst_wc_date_range
        # end - ext bi denorm word count date range

        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            if not self.__continue_iteration():
                return
            # retrieve word count
            s_end_date = datetime.strptime(self.__g_dictDateRange['s_end_date'], '%Y%m%d').strftime('%Y-%m-%d')
            if self.__g_dictDateRange['s_start_date'] == 'na':  # get whole wc
                self.__print_debug('get whole wc')
                lst_word_cnt = o_sv_mysql.executeQuery('getAllWordCountTo', s_end_date)
            else:
                s_start_date = datetime.strptime(self.__g_dictDateRange['s_start_date'], '%Y%m%d').strftime('%Y-%m-%d')
                self.__print_debug('wc get from ' + s_start_date + ' to ' + s_end_date)
                lst_word_cnt = o_sv_mysql.executeQuery('getWordCountFromTo', self.__g_dictDateRange['s_start_date'], s_end_date)

            if len(lst_word_cnt):
                # retrieve dictionary if word count log exists
                self.__print_debug('get whole dictionary')
                dict_dictionary = {}
                lst_dictionary = o_sv_mysql.executeQuery('getAllDictionaryCompact')
                for dict_single_word in lst_dictionary:
                    if not self.__continue_iteration():
                        return
                    dict_dictionary[dict_single_word['word_srl']] = {'word': dict_single_word['word'],
                                                                    'b_ignore': dict_single_word['b_ignore']}
                del lst_dictionary
        nIdx = 0
        nSentinel = len(lst_word_cnt)
        if nSentinel:
            self.__print_debug('transfer word count via SQL')
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.client_serve')
                o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
                # lst_wc_date_range = o_sv_mysql.executeQuery('getWordCountDenormDateRange')
                for dict_single_wc in lst_word_cnt:
                    if not self.__continue_iteration():
                        return
                    if dict_dictionary[dict_single_wc['word_srl']]['b_ignore'] == '0':
                        o_sv_mysql.executeQuery('insertWordCountDenorm', dict_single_wc['log_srl'],
                                               dict_dictionary[dict_single_wc['word_srl']]['word'],
                                               dict_single_wc['cnt'], dict_single_wc['logdate'])
                    self.__print_progress_bar(nIdx, nSentinel, prefix = 'transfer wc data:', suffix = 'Complete', length = 50)
                    nIdx += 1
        del lst_word_cnt
        del dict_dictionary
