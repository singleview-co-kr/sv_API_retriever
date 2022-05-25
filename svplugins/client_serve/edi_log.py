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
    import sv_hypermart_model
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_hypermart_model


class SvEdiLog():
    __g_sBirthOftheEdiWorld = '20100101'
    
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

    def proc(self, s_mode):
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
        self.__print_debug('retrieve branches and skus info')
        dict_branch_by_title = sv_hypermart_model.SvHyperMartType.get_dict_by_title()
        dict_branch_type = sv_hypermart_model.BranchType.get_dict_by_title()
        o_mart_geo_info = sv_hypermart_model.SvHypermartGeoInfo()
        lst_hypermart_geo_info = o_mart_geo_info.lst_hypermart_geo_info
        if len(lst_hypermart_geo_info):
            for dict_single_branch in o_mart_geo_info.lst_hypermart_geo_info:
                # n_hypermart_id = dict_branch_by_title[dict_single_branch['hypermart_name']]
                # n_branch_type_id = dict_branch_type[dict_single_branch['branch_type']]
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
            self.__print_debug('get whole dictionary')('excel extraction failure - no branch info')
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

    def __extract_4_google_data_studio(self):
        """
        retrieve specific period for Google Data Studio
        """
        self.__print_debug('start EDI extraction')
        # begin - ext bi denorm EDI date range
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.client_serve')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
            o_sv_mysql.create_table_on_demand('_edi_daily_log_denorm')  # for google data studio
            lst_wc_date_range = o_sv_mysql.executeQuery('getEdiDenormDateRange')
        if lst_wc_date_range[0]['mindate'] is None and lst_wc_date_range[0]['maxdate'] is None:
            self.__print_debug('init mode')
            self.__g_dictDateRange['s_start_date'] = self.__g_sBirthOftheEdiWorld

        if lst_wc_date_range[0]['maxdate']:
            self.__print_debug('appending mode')
            dt_maxdate = lst_wc_date_range[0]['maxdate']
            dt_startdate = dt_maxdate + timedelta(1)
            s_startdate = dt_startdate.strftime('%Y%m%d')
            if int(s_startdate) <= int(self.__g_sYesterday):
                self.__g_dictDateRange['s_start_date'] = s_startdate
        del lst_wc_date_range
        # end - ext bi denorm EDI date range
        dict_date_range = {'s_start_date': datetime.strptime(self.__g_dictDateRange['s_start_date'], '%Y%m%d').strftime('%Y-%m-%d'),
                           's_end_date':  datetime.strptime(self.__g_dictDateRange['s_end_date'], '%Y%m%d').strftime('%Y-%m-%d')}
        
        lst_mart = ['Emart', 'Ltmart']
        for s_mart_title in lst_mart:
            self.__print_debug('transfer ' + s_mart_title + ' EDI from ' + dict_date_range['s_start_date'] + 
                                ' to ' + dict_date_range['s_end_date'] + ' via SQL')
            s_log_cnt_query = 'getEdi{s_mart_title}LogCountByPeriod'.format(s_mart_title=s_mart_title)
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.client_serve')
                o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
                if not self.__continue_iteration():
                    return
                lst_log_count = o_sv_mysql.executeQuery(s_log_cnt_query, 
                                    dict_date_range['s_start_date'], dict_date_range['s_end_date'])
            
            n_edi_log_count = lst_log_count[0]['count(*)']
            del lst_log_count
            n_idx = 0
            n_sentinel = n_edi_log_count

            s_performance_log_query = 'getEdi{s_mart_title}LogByPeriod'.format(s_mart_title=s_mart_title)
            # begin - mart extraction by mart
            n_limit = self.__g_nLimitToSingleQuery
            n_offset = 0
            dict_param_tmp = {'s_period_start': dict_date_range['s_start_date'],
                              's_period_end': dict_date_range['s_end_date'],
                              'n_offset': n_offset, 'n_limit': n_limit}

            # very big data causes memory dump, if retrieve at single access
            while n_edi_log_count > 0:
                dict_param_tmp['n_offset'] = n_offset
                dict_param_tmp['n_limit'] = n_limit
                n_offset = n_offset + n_limit
                n_edi_log_count = n_edi_log_count - n_limit
                if n_limit >= n_edi_log_count:
                    n_limit = n_edi_log_count

                with sv_mysql.SvMySql() as o_sv_mysql:
                    o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                    o_sv_mysql.set_app_name('svplugins.client_serve')
                    o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
                    if not self.__continue_iteration():
                        return
                    lst_log_period = o_sv_mysql.executeDynamicQuery(s_performance_log_query, dict_param_tmp)
                # insert de-norm table
                with sv_mysql.SvMySql() as o_sv_mysql:
                    o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                    o_sv_mysql.set_app_name('svplugins.client_serve')
                    o_sv_mysql.initialize(self.__g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
                    o_sv_mysql.create_table_on_demand('_edi_daily_log_denorm')  # for google data studio
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
                        o_sv_mysql.executeQuery('insertEdiDailyLogDenorm',
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
                        self.__print_progress_bar(n_idx+1, n_sentinel, prefix = 'transfer EDI data:', suffix = 'Complete', length = 50)
                        n_idx += 1
                del lst_log_period
        del dict_date_range
        # end - extraction by mart
        # specific period for excel pivoting end
        print('de-norm extraction succeed')
