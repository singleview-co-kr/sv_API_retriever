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
from collections import defaultdict

# singleview library
if __name__ == 'internal_search': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql
else: # for platform running
    from svcommon import sv_mysql


class svInternalSearch():
    
    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        # print('search:__init__')
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
        self.__g_dictGaIntSearchRaw = {}
        self.__g_dictWordSrl = defaultdict(int)
        self.__g_dictSvAcctInfo = None
        self._g_oLogger = logging.getLogger(__name__)

    def __del__(self):
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        self.__g_oSvCampaignParser = None
        self.__g_sTblPrefix = None
        self.__g_sDataPath = None
        self.__g_dictGaIntSearchRaw = {}
        self.__g_dictWordSrl = defaultdict(int)
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

    def proc_internal_search_log(self):
        # traverse directory and categorize data files
        lst_search_term = []
        lst_data_file = os.listdir(self.__g_sDataPath)
        lst_data_file.sort()
        n_idx = 0
        n_sentinel = len(lst_data_file)
        dict_query = {'searchUniques.tsv': 'search_term'}
        # lst_analyzing_filename = dict_query.keys()
        for s_filename in lst_data_file:
            s_data_file_fullname = os.path.join(self.__g_sDataPath, s_filename)
            if s_filename == 'archive':
                continue
            lst_file_info = s_filename.split('_')
            s_data_date = lst_file_info[0]
            s_ua_type = self.__g_oSvCampaignParser.get_ua(lst_file_info[1])
            s_specifier = lst_file_info[2]
            if s_specifier in dict_query:  #lst_analyzing_filename:
                s_attr_name = dict_query[s_specifier]
            else:
                continue
            
            if os.path.isfile(s_data_file_fullname):
                with open(s_data_file_fullname, 'r') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t', skipinitialspace=True)
                    for row in reader:
                        if not self.__continue_iteration():
                            break
                        s_search_term = row[0].lower()
                        s_rpt_id = s_data_date+'|@|'+s_ua_type+'|@|'+s_search_term  # row[0] is search_term
                        lst_search_term.append(s_search_term)
                        if s_rpt_id not in self.__g_dictGaIntSearchRaw:  #.keys():  # if designated log already created
                            self.__g_dictGaIntSearchRaw[s_rpt_id] = {'search_term': 0}
                        self.__g_dictGaIntSearchRaw[s_rpt_id][s_attr_name] = row[1]
                self.__archive_ga_data_file(s_filename)
            else:
                self.__print_debug('pass ' + s_data_file_fullname + ' does not exist')

            self.__print_progress_bar(n_idx + 1, n_sentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
            n_idx += 1
        for s_search_term in set(lst_search_term):
            self.__g_dictWordSrl[s_search_term]  # s_search_term: 0 automatically sets by defaultdict(int)
        del lst_search_term
        self.__get_term_srl()
        self.__register_int_search_log()

    def __get_term_srl(self):
        with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.set_tbl_prefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.ga_register_db')
            oSvMysql.initialize(self.__g_dictSvAcctInfo)
            for s_term, _ in self.__g_dictWordSrl.items():
                lst_rst = oSvMysql.executeQuery('getIntSearchTermInfo', s_term)
                if len(lst_rst):
                    n_word_srl = lst_rst[0]['word_srl']
                else:
                    lst_insert = oSvMysql.executeQuery('insertIntSearchTerm', s_term)
                    n_word_srl = lst_insert[0]['id']
                self.__g_dictWordSrl[s_term] = n_word_srl

    def __register_int_search_log(self):
        n_idx = 0
        n_sentinel = len(self.__g_dictGaIntSearchRaw)
        with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.set_tbl_prefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.ga_register_db')
            oSvMysql.initialize(self.__g_dictSvAcctInfo)
            for s_rpt_id, dict_single_raw in self.__g_dictGaIntSearchRaw.items():
                if not self.__continue_iteration():
                    break
                
                lst_rpt_type = s_rpt_id.split('|@|')
                s_data_date = datetime.strptime(lst_rpt_type[0], "%Y%m%d")
                s_ua_type = lst_rpt_type[1]
                s_search_term = lst_rpt_type[2]
                # # should check if there is duplicated date + SM log
                # # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                oSvMysql.executeQuery('insertIntSearchLog', self.__g_dictWordSrl[s_search_term], s_ua_type,
                                      dict_single_raw['search_term'], s_data_date)

                self.__print_progress_bar(n_idx + 1, n_sentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                n_idx += 1

    def __archive_ga_data_file(self, s_cur_filename):
        if not os.path.exists(self.__g_sDataPath):
            self.__print_debug('error: google analytics source directory does not exist!' )
            return
        s_archive_data_path = os.path.join(self.__g_sDataPath, 'archive')
        if not os.path.exists(s_archive_data_path):
            os.makedirs(s_archive_data_path)
        
        s_source_file_path = os.path.join(self.__g_sDataPath, s_cur_filename)
        s_archive_data_file_path = os.path.join(s_archive_data_path, s_cur_filename)
        shutil.move(s_source_file_path, s_archive_data_file_path)
