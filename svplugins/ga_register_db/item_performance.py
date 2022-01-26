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
import os
import shutil
import csv

# singleview library
if __name__ == 'item_performance': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql
else: # for platform running
    from svcommon import sv_mysql


class svItemPerformance():
    __g_sSvNull = '$%'
    
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
        self.__g_oSvCampaignParser = None
        self.__g_sTblPrefix = None
        self.__g_sDataPath = None
        self.__g_dictGaItemPerfRaw = {}
        self.__g_dictItemSrl = {}
        self.__g_dictSvAcctInfo = None
        self._g_oLogger = logging.getLogger(__name__)

    def __del__(self):
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        self.__g_oSvCampaignParser = None
        self.__g_sTblPrefix = None
        self.__g_sDataPath = None
        self.__g_dictGaItemPerfRaw = {}
        self.__g_dictItemSrl = {}
        self.__g_dictSvAcctInfo = None

    def init_var(self, dict_sv_acct_onfo, s_tbl_prefix, s_ga_data_path, o_sv_campaign_parser, 
         f_print_debug, f_print_progress_bar, f_continue_iteration):
        self.__g_dictSvAcctInfo = dict_sv_acct_onfo
        self.__continue_iteration = f_continue_iteration
        self.__print_debug = f_print_debug
        self.__print_progress_bar = f_print_progress_bar
        self.__g_sTblPrefix = s_tbl_prefix
        self.__g_sDataPath = s_ga_data_path
        self.__g_oSvCampaignParser = o_sv_campaign_parser

    def proc_item_perf_log(self):
        # traverse directory and categorize data files
        lst_data_file = os.listdir(self.__g_sDataPath)
        lst_data_file.sort()
        n_idx = 0
        n_sentinel = len(lst_data_file)
        dict_query = {'productListViews.tsv': 'imp_list', 
            'productListClicks.tsv': 'click_list', 
            'productDetailViews.tsv': 'imp_detail',
            # 'buyToDetailRate.tsv': 'rate_detail_pur',  # to be deprecated - Unique purchases divided by views of product detail pages (Enhanced Ecommerce)
            # 'cartToDetailRate.tsv': 'rate_detail_cart',   # to be deprecated - Product adds divided by views of product details (Enhanced Ecommerce).
            'productAddsToCart.tsv': 'freq_cart',  # Number of times the product was added to the shopping cart
            'quantityAddedToCart.tsv': 'qty_cart',  # Number of product units added to the shopping cart
            'productRemovesFromCart.tsv': 'qty_cart_remove',
            'productRevenuePerPurchase.tsv': 'amnt_pur',
            'itemQuantity.tsv': 'freq_pur',  # Total number of items purchased. For example, if users purchase 2 frisbees and 5 tennis balls, this will be 7.
            'productCheckouts.tsv': 'freq_cko',  # Number of times the product was included in the check-out process (Enhanced Ecommerce).
            'quantityCheckedOut.tsv': 'qty_cko'  # Number of product units included in check out (Enhanced Ecommerce).
        }
        lst_analyzing_filename = dict_query.keys()
        for s_filename in lst_data_file:
            s_data_file_fullname = os.path.join(self.__g_sDataPath, s_filename)
            if s_filename == 'archive':
                continue
            lst_file_info = s_filename.split('_')
            s_data_date = lst_file_info[0]
            n_first_detected_date = int(s_data_date)
            s_ua_type = self.__g_oSvCampaignParser.getUa(lst_file_info[1])
            s_specifier = lst_file_info[2]
            if s_specifier in lst_analyzing_filename:
                s_attr_name = dict_query[s_specifier]
            else:
                continue
            
            if os.path.isfile(s_data_file_fullname):
                with open(s_data_file_fullname, 'r') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t', skipinitialspace=True)
                    for row in reader:
                        if not self.__continue_iteration():
                            break

                        s_item_title = row[0]
                        if self.__g_dictItemSrl.get(s_item_title, self.__g_sSvNull) == self.__g_sSvNull:  # if new item
                            self.__g_dictItemSrl[s_item_title] = {'n_item_srl':0, 'n_first_detected_date': n_first_detected_date}
                        elif self.__g_dictItemSrl[s_item_title]['n_first_detected_date'] > n_first_detected_date:
                            self.__g_dictItemSrl[s_item_title]['n_first_detected_date'] = n_first_detected_date

                        # lst_item_title.append(s_item_title)
                        s_rpt_id = s_data_date+'|@|'+s_ua_type+'|@|'+s_item_title
                        if not self.__g_dictGaItemPerfRaw.get(s_rpt_id, 0):  # if designated log not existed
                            self.__g_dictGaItemPerfRaw[s_rpt_id] = {
                                'imp_list':0, 'click_list':0, 'imp_detail':0,
                                # 'rate_detail_pur':0, 'rate_detail_cart':0,
                                'freq_cart':0, 'qty_cart':0, 'qty_cart_remove':0,
                                'amnt_pur':0, 'freq_pur':0, 'freq_cko':0, 'qty_cko':0
                            }
                        self.__g_dictGaItemPerfRaw[s_rpt_id][s_attr_name] = float(row[1])
                self.__archive_ga_data_file(s_filename)
            else:
                self.__print_debug('pass ' + s_data_file_fullname + ' does not exist')

            self.__print_progress_bar(n_idx + 1, n_sentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
            n_idx += 1
        self.__get_item_srl()
        self.__register_item_perf_log()

    def __get_item_srl(self):
        with sv_mysql.SvMySql('svplugins.ga_register_db', self.__g_dictSvAcctInfo) as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for s_item_title, dict_item_info in self.__g_dictItemSrl.items():
                lst_rst = oSvMysql.executeQuery('getItemTitle', s_item_title)
                if len(lst_rst):
                    n_item_srl = lst_rst[0]['item_srl']
                else:
                    lst_insert = oSvMysql.executeQuery('insertItemTitle', s_item_title,
                        str(dict_item_info['n_first_detected_date']))
                    n_item_srl = lst_insert[0]['id']
                self.__g_dictItemSrl[s_item_title]['n_item_srl'] = n_item_srl

    def __register_item_perf_log(self):
        n_idx = 0
        n_sentinel = len(self.__g_dictGaItemPerfRaw)
        with sv_mysql.SvMySql('svplugins.ga_register_db', self.__g_dictSvAcctInfo) as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for s_rpt_id, dict_single_raw in self.__g_dictGaItemPerfRaw.items():
                if not self.__continue_iteration():
                    break
                lst_rpt_type = s_rpt_id.split('|@|')
                s_data_date = datetime.strptime(lst_rpt_type[0], "%Y%m%d")
                s_ua_type = lst_rpt_type[1]
                s_item_title = lst_rpt_type[2]
                # # should check if there is duplicated date + SM log
                # # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                oSvMysql.executeQuery('insertItemPerfLog', self.__g_dictItemSrl[s_item_title]['n_item_srl'], s_ua_type, 
                    str(dict_single_raw['imp_list']), str(dict_single_raw['click_list']),
                    str(dict_single_raw['imp_detail']), 
                    # str(dict_single_raw['rate_detail_pur']), str(dict_single_raw['rate_detail_cart']), 
                    str(dict_single_raw['freq_cart']),
                    str(dict_single_raw['qty_cart']), str(dict_single_raw['qty_cart_remove']),
                    str(dict_single_raw['amnt_pur']), str(dict_single_raw['freq_pur']),
                    str(dict_single_raw['freq_cko']), str(dict_single_raw['qty_cko']), s_data_date)

                self.__print_progress_bar(n_idx + 1, n_sentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                n_idx += 1

    def __archive_ga_data_file(self, s_cur_filename):
        if not os.path.exists(self.__g_sDataPath):
            self._printDebug( 'error: google analytics source directory does not exist!' )
            return
        s_archive_data_path = os.path.join(self.__g_sDataPath, 'archive')
        if not os.path.exists(s_archive_data_path):
            os.makedirs(s_archive_data_path)
        
        s_source_file_path = os.path.join(self.__g_sDataPath, s_cur_filename)
        s_archive_data_file_path = os.path.join(s_archive_data_path, s_cur_filename)
        shutil.move(s_source_file_path, s_archive_data_file_path)
