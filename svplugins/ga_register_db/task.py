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

# refer to https://github.com/google/google-api-python-client/tree/master/samples/analytics
# you firstly need to install by cmd "pip3.6 install --upgrade google-api-python-client"
# refer to https://developers.google.com/analytics/devguides/config/mgmt/v3/quickstart/installed-py
# to create desinated credential refer to https://console.developers.google.com/apis/credentials
# to get console credential you firstly need to run with the option --noauth_local_webserver
# to monitor API traffic refer to https://console.developers.google.com/apis/api/analytics.googleapis.com/quotas?project=svgastudio

# standard library
import sys
import logging
from datetime import datetime
import os
import shutil
import csv
import re
import codecs
import decimal  # to resolve round(0.5) is 0

# singleview library
if __name__ == '__main__':  # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_campaign_parser
    import sv_object
    import sv_plugin
    import settings
    import internal_search
    import item_performance
else:  # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_object
    from svcommon import sv_plugin
    from django.conf import settings
    from svplugins.ga_register_db import internal_search
    from svplugins.ga_register_db import item_performance


class SvJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oSvCampaignParser = sv_campaign_parser.SvCampaignParser()
    __g_sSvNull = '#$'
    __g_oDecimalContext = None
    # __g_dictGaVersion = {'ua': 3, 'ga4': 4}
    __g_dictUaMediaQuery = {'sessions.tsv': 'sess',
                            'percentNewSessions.tsv': 'new_per',   # tsv 파일명이 다르고 UA는 % GA4는 절대값
                            'bounceRate.tsv': 'b_per',  # UA 100이 최대치 GA4는 1이 최대치
                            'avgSessionDuration.tsv': 'dur_sec',
                            'pageviewsPerSession.tsv': 'pvs'}
    __g_dictGa4MediaQuery = {'sessions.tsv': 'sess',
                             'newUsers.tsv': 'new_per',   # tsv 파일명이 다르고 UA는 % GA4는 절대값
                             'bounceRate.tsv': 'b_per',  # UA 100이 최대치 GA4는 1이 최대치
                             'avgSessionDuration.tsv': 'dur_sec',
                             'pageviewsPerSession.tsv': 'pvs'}
    __g_dictGaTransactionQuery = {'transactionRevenueByTrId.tsv': 'rev',
                                  'transactions.tsv': 'trs'}

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name + '(20230612)')
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first,
        # whenever you create new objects it will reuse this same dict.
        # To create instance variables, you declare them with self in __init__.
        self.__g_sBrandedTruncPath = None
        self.__g_sTblPrefix = None
        self.__g_sGaVersion = None
        self.__g_lstErroneousMedia = []
        self.__g_dictGaRaw = None  # prevent duplication on a web console
        self.__g_dictSourceMediaAliasInfo = {}
        self.__g_dictGoogleAdsCampaignAlias = {}
        self.__g_dictNvrPowerlinkCampaignAlias = {}

    def __del__(self):
        """ never place self._task_post_proc() here
            __del__() is not executed if try except occurred """
        self.__g_sBrandedTruncPath = None
        self.__g_oSvCampaignParser = None
        self.__g_sTblPrefix = None
        self.__g_sGaVersion = None
        self.__g_lstErroneousMedia = []
        self.__g_dictGaRaw = None  # prevent duplication on a web console
        self.__g_dictSourceMediaAliasInfo = {}
        self.__g_dictGoogleAdsCampaignAlias = {}
        self.__g_dictNvrPowerlinkCampaignAlias = {}

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_dictGaRaw = {}  # prevent duplication on a web console

        # begin - make round(0.5) to not 0 but 1
        self.__g_oDecimalContext = decimal.getcontext()
        self.__g_oDecimalContext.rounding = decimal.ROUND_HALF_UP
        # end - make round(0.5) to not 0 but 1
        
        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
                'google_analytics' not in dict_acct_info:
            self._print_debug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        s_version = dict_acct_info['google_analytics']['s_version']
        s_property_or_view_id = dict_acct_info['google_analytics']['s_property_or_view_id']
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']
        self.__g_sBrandedTruncPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id,
                                                  s_brand_id, 'branded_term.conf')
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.ga_register_db')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
        # self.__g_nGaVersion = self.__g_dictGaVersion[s_version]  # 3 for universal analytics, 4 for google analytics 4 
        self.__g_sGaVersion = s_version
        self._print_debug('-> register ga raw data')
        self.__parse_ga_data_file(s_sv_acct_id, s_brand_id, s_property_or_view_id)

        self._task_post_proc(self._g_oCallback)

    def __parse_ga_data_file(self, s_sv_acct_id, s_acct_title, s_ua_view_id):
        self._print_debug('-> ' + s_ua_view_id + ' is registering GA data files')
        s_data_path = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_acct_title,
                                   'google_analytics', s_ua_view_id, 'data')
        s_conf_path = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_acct_title,
                                   'google_analytics', s_ua_view_id, 'conf')
        # try internal search log
        self._print_debug('UA internal search log has been started\n')
        o_internal_search = internal_search.SvInternalSearch()
        o_internal_search.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix, s_data_path, self.__g_oSvCampaignParser,
                                   self._print_debug, self._print_progress_bar, self._continue_iteration)
        o_internal_search.proc_internal_search_log()
        del o_internal_search
        self._print_debug('UA internal search log has been finished\n')

        # try item performance log
        self._print_debug('UA item performance log has been started\n')
        o_item_perf = item_performance.SvItemPerformance()
        o_item_perf.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix, s_data_path, self.__g_oSvCampaignParser,
                             self._print_debug, self._print_progress_bar, self._continue_iteration)
        o_item_perf.proc_item_perf_log()
        del o_item_perf
        self._print_debug('UA item performance log has been finished\n')
        # try transaction referral log
        self.__proc_transaction_log(s_data_path)
        self.__get_source_meedium_alias(s_conf_path)
        # retrieve google ads campaign name alias info
        s_googeads_data_path = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT,
                                            s_sv_acct_id, s_acct_title, 'adwords')
        self.__g_dictGoogleAdsCampaignAlias = self.__get_campaign_alias(s_googeads_data_path)
        # retrieve naver powerlink campaign name alias info
        s_nvr_powerlink_data_path = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id,
                                                 s_acct_title, 'naver_ad')
        self.__g_dictNvrPowerlinkCampaignAlias = self.__get_campaign_alias(s_nvr_powerlink_data_path)
        self.__proc_media_perf_log(s_data_path)
        # stop if erroneous source medium list not empty
        if len(self.__g_lstErroneousMedia) > 0:
            self.__g_lstErroneousMedia = sorted(set(self.__g_lstErroneousMedia))
            self._print_debug('erroneous media names has been detected!')
            for sMedia in self.__g_lstErroneousMedia:
                self._print_debug(sMedia)
        self.__register_source_medium_term()

    def __get_source_meedium_alias(self, s_parent_data_path):
        s_alias_filepath = os.path.join(s_parent_data_path, 'alias_info_source_media.tsv')
        if os.path.isfile(s_alias_filepath):
            with codecs.open(s_alias_filepath, 'r', encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                n_row_cnt = 0
                for row in reader:
                    if n_row_cnt > 0:
                        self.__g_dictSourceMediaAliasInfo[row[0]] = {'alias': row[1]}
                    n_row_cnt += 1
        return

    def __proc_transaction_log(self, s_data_path):
        self._print_debug('UA Transaction log has been started\n')
        # traverse directory and categorize data files
        lst_transaction_log = []
        lst_data_file = os.listdir(s_data_path)
        lst_data_file.sort()
        n_idx = 0
        n_sentinel = len(lst_data_file)
        try:
            for s_filename in lst_data_file:
                s_data_file_fullname = os.path.join(s_data_path, s_filename)
                if s_filename == 'archive':
                    continue
                lst_file_info = s_filename.split('_')
                s_data_date = lst_file_info[0]
                s_ua_type = self.__g_oSvCampaignParser.get_ua(lst_file_info[1])
                s_specifier = lst_file_info[2]
                if s_specifier not in self.__g_dictGaTransactionQuery:
                    continue
                if os.path.isfile(s_data_file_fullname):
                    with open(s_data_file_fullname, 'r') as tsvfile:
                        o_reader = csv.reader(tsvfile, delimiter='\t', skipinitialspace=True)
                        lst_row = None
                        for lst_row in o_reader:
                            if not self._continue_iteration():
                                break
                            if s_specifier == 'transactionRevenueByTrId.tsv':
                                # row[1:] is to remove transaction ID
                                dict_rst = self.__parse_ga_row(lst_row[1:], s_data_file_fullname)
                                lst_transaction_log.append([lst_row[0], s_ua_type, dict_rst['source'], 
                                                            dict_rst['rst_type'], dict_rst['medium'], 
                                                            str(dict_rst['brd']), dict_rst['campaign1st'], 
                                                            dict_rst['campaign2nd'], dict_rst['campaign3rd'], 
                                                            lst_row[2], str(lst_row[4]), s_data_date])
                                del dict_rst
                            elif s_specifier == 'transactions.tsv':
                                # self._print_debug('transactions.tsv proc needs to be developed')
                                # return
                                pass
                        if lst_row:
                            del lst_row
                    self.__archive_ga_data_file(s_data_path, s_filename)
                else:
                    self._print_debug('pass ' + s_data_file_fullname + ' does not exist')
                self._print_progress_bar(n_idx + 1, n_sentinel, prefix='Arrange data file:', suffix='Complete',
                                         length=50)
                n_idx += 1
            n_idx = 0
            n_sentinel = len(lst_transaction_log)
            with sv_mysql.SvMySql() as o_sv_mysql:  # to enforce follow strict mysql connection mgmt
                o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.ga_register_db')
                o_sv_mysql.initialize(self._g_dictSvAcctInfo)
                for lst_single_row in lst_transaction_log:
                    if not self._continue_iteration():
                        break
                    # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                    o_sv_mysql.execute_query('insertGaTransactionDailyLog',
                                             lst_single_row[0], lst_single_row[1], lst_single_row[2], lst_single_row[3],
                                             lst_single_row[4], lst_single_row[5], lst_single_row[6], lst_single_row[7],
                                             lst_single_row[8], lst_single_row[9], str(lst_single_row[10]),
                                             lst_single_row[11])
                    self._print_progress_bar(n_idx + 1, n_sentinel, prefix='Register DB:', suffix='Complete', length=50)
                    n_idx += 1
            del lst_transaction_log
        except Exception as err:
            self._print_debug(err)
        self._print_debug('UA Transaction log has been finished\n')

    def __proc_media_perf_log(self, s_data_path):
        """ process homepage level data """
        self._print_debug('UA media performance log arranging has been started\n')
        if self.__g_sGaVersion == 'ga4':
            dict_media_qry = self.__g_dictGa4MediaQuery
        elif self.__g_sGaVersion == 'ua':
            dict_media_qry = self.__g_dictUaMediaQuery

        # traverse directory and categorize data files
        lst_data_files = os.listdir(s_data_path)
        lst_data_files.sort()
        n_idx = 0
        n_sentinel = len(lst_data_files)
        for s_filename in lst_data_files:
            s_data_file_fullname = os.path.join(s_data_path, s_filename)
            if s_filename == 'archive':
                continue
            lst_file = s_filename.split('_')
            s_data_date = lst_file[0]
            s_ua_type = self.__g_oSvCampaignParser.get_ua(lst_file[1])
            s_specifier = lst_file[2]
            # if s_specifier in dict_query:
            #     s_idx_name = dict_query[s_specifier]
            if s_specifier in dict_media_qry:
                s_idx_name = dict_media_qry[s_specifier]
            else:
                continue

            if os.path.isfile(s_data_file_fullname):
                with open(s_data_file_fullname, 'r') as tsvfile:
                    o_reader = csv.reader(tsvfile, delimiter='\t', skipinitialspace=True)
                    lst_row = None
                    for lst_row in o_reader:
                        if not self._continue_iteration():
                            break
                        dict_rst = self.__parse_ga_row(lst_row, s_data_file_fullname)
                        s_term = lst_row[2]
                        s_rpt_id = s_data_date + '|@|' + s_ua_type + '|@|' + dict_rst['source'] + '|@|' + \
                                   dict_rst['rst_type'] + '|@|' + dict_rst['medium'] + '|@|' + \
                                   str(dict_rst['brd']) + '|@|' + dict_rst['campaign1st'] + '|@|' + \
                                   dict_rst['campaign2nd'] + '|@|' + dict_rst['campaign3rd'] + '|@|' + s_term
                        if self.__g_dictGaRaw.get(s_rpt_id, self.__g_sSvNull) == self.__g_sSvNull:  # if not exists
                            self.__g_dictGaRaw[s_rpt_id] = {
                                # new_per is absolute number for GA4, percent for UA
                                # b_per is 1 at max for GA4, 100 at max for UA
                                'sess': 0, 'new_per': 0, 'b_per': 0, 'dur_sec': 0, 'pvs': 0  #, 'trs': 0, 'rev': 0
                            }
                        del dict_rst
                        self.__g_dictGaRaw[s_rpt_id][s_idx_name] = float(lst_row[3])
                    if lst_row:
                        del lst_row
                self.__archive_ga_data_file(s_data_path, s_filename)
            else:
                self._print_debug('pass ' + s_data_file_fullname + ' does not exist')
            self._print_progress_bar(n_idx + 1, n_sentinel, prefix='Arrange data file:', suffix='Complete', length=50)
            n_idx += 1
        self._print_debug('UA media performance log arranging has been finished\n')

    def __get_campaign_alias(self, s_parent_data_path):
        dict_campaign_alias = {}
        s_alias_filepath = os.path.join(s_parent_data_path, 'alias_info_campaign.tsv')
        if os.path.isfile(s_alias_filepath):
            with codecs.open(s_alias_filepath, 'r', encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                n_row_cnt = 0
                for row in reader:
                    if n_row_cnt > 0:
                        dict_campaign_alias[row[0]] = {'source': row[1], 'rst_type': row[2], 'medium': row[3],
                                                       'camp1st': row[4], 'camp2nd': row[5], 'camp3rd': row[6]}
                    n_row_cnt += 1
        return dict_campaign_alias

    def __register_source_medium_term(self):
        self._print_debug('UA media performance log registration has been started\n')
        n_idx = 0
        n_sentinel = len(self.__g_dictGaRaw)
        with sv_mysql.SvMySql() as o_sv_mysql:  # to enforce follow strict mysql connection mgmt
            o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.ga_register_db')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            for s_rpt_id, dict_single_raw in self.__g_dictGaRaw.items():
                if not self._continue_iteration():
                    break
                lst_rpt_type = s_rpt_id.split('|@|')
                s_data_date = datetime.strptime(lst_rpt_type[0], "%Y%m%d")
                s_ua_type = lst_rpt_type[1]
                s_source = lst_rpt_type[2]
                s_rst_type = lst_rpt_type[3]
                s_medium = lst_rpt_type[4]
                b_brd = lst_rpt_type[5]
                s_campaign1st = lst_rpt_type[6]
                s_campaign2nd = lst_rpt_type[7]
                s_campaign3rd = lst_rpt_type[8]
                s_term = lst_rpt_type[9]

                if self.__g_sGaVersion == 'ga4':
                    n_new = int(dict_single_raw['new_per'])
                    n_bounce = round(decimal.Decimal(dict_single_raw['sess'] * dict_single_raw['b_per']), 0)
                elif self.__g_sGaVersion == 'ua':
                    n_new = round(decimal.Decimal(dict_single_raw['sess'] * dict_single_raw['new_per'] / 100), 0)
                    n_bounce = round(decimal.Decimal(dict_single_raw['sess'] * dict_single_raw['b_per'] / 100), 0)

                # should check if there is duplicated date + SM log
                # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                o_sv_mysql.execute_query('insertGaCompiledDailyLog', s_ua_type, s_source, 
                                         s_rst_type, s_medium, b_brd, s_campaign1st, s_campaign2nd, 
                                         s_campaign3rd, s_term, dict_single_raw['sess'], 
                                         str(n_new),  # str(dict_single_raw['new_per']), 
                                         str(n_bounce),  # str(dict_single_raw['b_per']), 
                                         str(dict_single_raw['dur_sec']), str(dict_single_raw['pvs']),  
                                         # dict_single_raw['trs'], dict_single_raw['rev'], 0,
                                         s_data_date)
                self._print_progress_bar(n_idx + 1, n_sentinel, prefix='Register DB:', suffix='Complete', length=50)
                n_idx += 1
        self._print_debug('UA media performance log registration has been finished\n')

    def __parse_ga_row(self, lst_row, s_datafile_fullname):
        try:
            if self.__g_dictSourceMediaAliasInfo.get(lst_row[0], self.__g_sSvNull) != self.__g_sSvNull:  # if exists
                s_source_medium_alias = str(self.__g_dictSourceMediaAliasInfo[lst_row[0]]['alias'])
                lst_source_medium = s_source_medium_alias.split(' / ')
            else:
                lst_source_medium = lst_row[0].split(' / ')
        except Exception as err:
            self._print_debug('3333')

        try:
            sSource = lst_source_medium[0]
            sMedium = lst_source_medium[1]
            sCampaignCode = lst_row[1]
        except Exception as err:
            self._print_debug('4444')
        sRstType = ''
        sCampaign1st = ''
        sCampaign2nd = ''
        sCampaign3rd = ''
        sTerm = lst_row[2]
        # "skin-skin14--shop3.ratestw.cafe24.com" like source string occurs confusion
        m = re.search(r"[-\w.]+", sSource)
        nHttpPos = m.group(0).find('http')
        if nHttpPos > -1:
            if len(sSource) > 30:  # remedy erronous UTM parameter
                m = re.search(r"https?://(\w*:\w*@)?[-\w.]+(:\d+)?(/([\w/_.]*(\?\S+)?)?)?", sSource)
                try:
                    if len(m.group(0)):
                        nPos = sSource.find('utm_source')
                        if nPos > -1:
                            sRightPart = sSource[nPos:]
                            aRightPart = sRightPart.split('=')
                            sSource = aRightPart[1]
                        else:
                            m1 = re.search(r"[^https?://](\w*:\w*@)?[-\w.]+(:\d+)?", sSource)
                            sSource = m1.group(0)
                except AttributeError:  # retry to handle very weird source tagging
                    # this block handles '＆' which is not & that naver shopping foolishly occurs
                    self._print_debug(
                        'weird source found on ' + s_datafile_fullname + ' -> unicode ampersand which is not &')
                    # same source code needs to be method - begin
                    sEncodedSource = sSource.encode('UTF-8')
                    sEncodedSource = str(sEncodedSource)
                    aWeirdSource = sEncodedSource.split("\\xef\\xbc\\x86")  # utf-8 encoded unicode ampersand ��
                    for sQueryElement in aWeirdSource:
                        aQuery = sQueryElement.split('=')
                        if aQuery[0] == 'utm_campaign':
                            dictSmRst = self.__g_oSvCampaignParser.parse_campaign_code(s_sv_campaign_code=aQuery[1])
                            sSource = dictSmRst['source']
                            sMedium = dictSmRst['medium']
                            sCampaignCode = aQuery[1]
                            sRstType = dictSmRst['rst_type']
                            sCampaign1st = dictSmRst['campaign1st']
                            sCampaign2nd = dictSmRst['campaign2nd']
                            sCampaign3rd = dictSmRst['campaign3rd']

                        if aQuery[0] == 'utm_term':
                            sTerm = aQuery[1]
                    # same source code needs to be method - end
                except Exception as inst:
                    self._print_debug(s_datafile_fullname)
                    self._print_debug(lst_row)
                    # self._print_debug(inst.args)     # arguments stored in .args
                    self._print_debug(
                        inst)  # __str__ allows args to be printed directly, but may be overridden in exception subclasses
        else:  # ex) naver��utm_medium=cpc��utm_campaign=NV_PS_CPC_NVSHOP_00_00��utm_term=NVSHOP_4741��n_media=33421��n_query=��ɼ��漮��n_rank=1��n_ad_group=grp-a001-02-000000002830061��n_ad=nad-a001-02-000000011190197��n_campaign_type=2��n_
            # same source code needs to be method - begin
            sEncodedSource = sSource.encode('UTF-8')
            sEncodedSource = str(sEncodedSource)
            nUnicodeAmpersandPos = sEncodedSource.find("\\xef\\xbc\\x86")
            if nUnicodeAmpersandPos > -1:
                try:
                    aWeirdSource = sEncodedSource.split("\\xef\\xbc\\x86")  # utf-8 encoded unicode ampersand ��
                    for sQueryElement in aWeirdSource:
                        aQuery = sQueryElement.split('=')
                        if aQuery[0] == 'utm_campaign':
                            dictSmRst = self.__g_oSvCampaignParser.parse_campaign_code(s_sv_campaign_code=aQuery[1])
                            sSource = dictSmRst['source']
                            sMedium = dictSmRst['medium']
                            sCampaignCode = aQuery[1]
                            sRstType = dictSmRst['rst_type']
                            sCampaign1st = dictSmRst['campaign1st']
                            sCampaign2nd = dictSmRst['campaign2nd']
                            sCampaign3rd = dictSmRst['campaign3rd']
                        if aQuery[0] == 'utm_term':
                            sTerm = aQuery[1]
                except Exception as err:
                    self._print_debug('7777')
            # same source code needs to be method - end
        dictValidMedium = self.__g_oSvCampaignParser.validate_ga_medium_tag(sMedium)
        if dictValidMedium['medium'] != 'weird':
            sMedium = dictValidMedium['medium']
            if dictValidMedium['found_pos'] > -1:
                nPos = dictValidMedium['found_pos'] + len(dictValidMedium['medium'])
                sRightPart = sMedium[nPos:]
                aRightPart = sRightPart.split('=')
                if aRightPart[0] == 'utm_campaign':
                    sCampaignCode = aRightPart[1]
        else:
            self.__g_lstErroneousMedia.append(s_datafile_fullname + ' -> ' + sSource + ' / ' + sMedium)

        if sSource == 'google' and sMedium == 'cpc':
            # lookup alias db, if non sv standard code
            if self.__g_dictGoogleAdsCampaignAlias.get(sCampaignCode,
                                                       self.__g_sSvNull) != self.__g_sSvNull:  # if exists
                if self.__g_dictGoogleAdsCampaignAlias[sCampaignCode]['source'] == 'YT':
                    sSource = 'youtube'
                if self.__g_dictGoogleAdsCampaignAlias[sCampaignCode]['medium'] == 'DISP':
                    sMedium = 'display'
                sCampaignCode = self.__g_dictGoogleAdsCampaignAlias[sCampaignCode]['source'] + '_' + \
                                self.__g_dictGoogleAdsCampaignAlias[sCampaignCode]['rst_type'] + '_' + \
                                self.__g_dictGoogleAdsCampaignAlias[sCampaignCode]['medium'] + '_' + \
                                self.__g_dictGoogleAdsCampaignAlias[sCampaignCode]['camp1st'] + '_' + \
                                self.__g_dictGoogleAdsCampaignAlias[sCampaignCode]['camp2nd'] + '_' + \
                                self.__g_dictGoogleAdsCampaignAlias[sCampaignCode]['camp3rd']
        elif sSource == 'naver' and sMedium == 'cpc':
            # lookup alias db, if non sv standard code
            if self.__g_dictNvrPowerlinkCampaignAlias.get(sCampaignCode,
                                                            self.__g_sSvNull) != self.__g_sSvNull:  # if exists
                sCampaignCode = self.__g_dictNvrPowerlinkCampaignAlias[sCampaignCode]['source'] + '_' + \
                                self.__g_dictNvrPowerlinkCampaignAlias[sCampaignCode]['rst_type'] + '_' + \
                                self.__g_dictNvrPowerlinkCampaignAlias[sCampaignCode]['medium'] + '_' + \
                                self.__g_dictNvrPowerlinkCampaignAlias[sCampaignCode]['camp1st'] + '_' + \
                                self.__g_dictNvrPowerlinkCampaignAlias[sCampaignCode]['camp2nd'] + '_' + \
                                self.__g_dictNvrPowerlinkCampaignAlias[sCampaignCode]['camp3rd']
        dictCampaignRst = self.__g_oSvCampaignParser.parse_campaign_code(s_sv_campaign_code=sCampaignCode)
        if dictCampaignRst['source'] == 'unknown':  # handle no sv campaign code data
            if sMedium == 'cpc' or sMedium == 'display':
                dictCampaignRst['rst_type'] = 'PS'
            else:
                dictCampaignRst['rst_type'] = 'NS'

        bBrd = dictCampaignRst['brd']
        sRstType = dictCampaignRst['rst_type']
        if len(dictCampaignRst['medium']) > 0:  # sv campaign criteria first, GA auto categorize later
            sMedium = dictCampaignRst['medium']
        sCampaign1st = dictCampaignRst['campaign1st']
        sCampaign2nd = dictCampaignRst['campaign2nd']
        sCampaign3rd = dictCampaignRst['campaign3rd']
        # finally determine branded by term
        dict_brded_rst = self.__g_oSvCampaignParser.decide_brded_by_term(self.__g_sBrandedTruncPath, sTerm)
        if dict_brded_rst['b_error'] == True:
            self._print_debug(dict_brded_rst['s_err_msg'])
        elif dict_brded_rst['b_brded']:
            bBrd = 1

        # monitor weird source name - begin
        if len(sSource) > 50:
            self._print_debug(s_datafile_fullname)
            self._print_debug(lst_row)
            raise Exception('stop')
        # monitor weird source name - begin
        return {'source': sSource, 'rst_type': sRstType, 'medium': sMedium, 'brd': bBrd,
                'campaign1st': sCampaign1st, 'campaign2nd': sCampaign2nd, 'campaign3rd': sCampaign3rd}

    def __archive_ga_data_file(self, s_data_path, s_cur_filename):
        if not os.path.exists(s_data_path):
            self._print_debug('error: google analytics source directory does not exist!')
            return
        s_data_archive_path = os.path.join(s_data_path, 'archive')
        if not os.path.exists(s_data_archive_path):
            os.makedirs(s_data_archive_path)
        s_source_filepath = os.path.join(s_data_path, s_cur_filename)
        s_archive_data_file_path = os.path.join(s_data_archive_path, s_cur_filename)
        shutil.move(s_source_filepath, s_archive_data_file_path)


if __name__ == '__main__':  # for console debugging
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with SvJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('ga_register_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params are required for console execution.')
