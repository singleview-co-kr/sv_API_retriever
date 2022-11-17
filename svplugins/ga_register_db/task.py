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

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_campaign_parser
    import sv_object
    import sv_plugin
    import settings
    import internal_search
    import item_performance
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_object
    from svcommon import sv_plugin
    from django.conf import settings
    from svplugins.ga_register_db import internal_search
    from svplugins.ga_register_db import item_performance


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oSvCampaignParser = sv_campaign_parser.SvCampaignParser()
    __g_sSvNull = '#$'

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20221117)')
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sBrandedTruncPath = None
        self.__g_sTblPrefix = None
        self.__g_lstErrornousMedia = []
        self.__g_dictGaRaw = None  # prevent duplication on a web console
        self.__g_dictSourceMediaNameAliasInfo = {}
        self.__g_dictGoogleAdsCampaignNameAlias = {}
        self.__g_dictNaverPowerlinkCampaignNameAlias = {}

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sBrandedTruncPath = None
        self.__g_oSvCampaignParser = None
        self.__g_sTblPrefix = None
        self.__g_lstErrornousMedia = []
        self.__g_dictGaRaw = None  # prevent duplication on a web console
        self.__g_dictSourceMediaNameAliasInfo = {}
        self.__g_dictGoogleAdsCampaignNameAlias = {}
        self.__g_dictNaverPowerlinkCampaignNameAlias = {}

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_dictGaRaw = {}  # prevent duplication on a web console
        
        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
          'google_analytics' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
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
        self.__g_sBrandedTruncPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_brand_id, 'branded_term.conf')
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.ga_register_db')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
        
        self._printDebug('-> register ga raw data')
        if s_version == 'ua':  # universal analytics
            self.__parseGaDataFile(s_sv_acct_id, s_brand_id, s_property_or_view_id)
        elif s_version == 'ga4':
            self._printDebug('plugin is developing')
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        self._task_post_proc(self._g_oCallback)
        
    def __parseGaDataFile(self, sSvAcctId, sAcctTitle, sGaViewId):
        self._printDebug('-> '+ sGaViewId +' is registering GA data files')
        sDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'google_analytics', sGaViewId, 'data')
        sConfPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'google_analytics', sGaViewId, 'conf')

        # try internal search log
        self._printDebug('UA internal search log has been started\n')
        o_internal_search = internal_search.svInternalSearch()
        o_internal_search.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix, sDataPath, self.__g_oSvCampaignParser, self._printDebug, self._printProgressBar, self._continue_iteration)
        o_internal_search.proc_internal_search_log()
        del o_internal_search
        self._printDebug('UA internal search log has been finished\n')

        # try item performance log
        self._printDebug('UA item performance log has been started\n')
        o_item_perf = item_performance.svItemPerformance()
        o_item_perf.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix, sDataPath, self.__g_oSvCampaignParser, self._printDebug, self._printProgressBar, self._continue_iteration)
        o_item_perf.proc_item_perf_log()
        del o_item_perf
        self._printDebug('UA item performance log has been finished\n')

        # try transaction referral log
        self.__proc_transaction_log(sDataPath)

        self.__getSourceMediaNameAlias(sConfPath)
        
        # retrieve google ads campaign name alias info
        sGoogeAdsDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'adwords')
        self.__g_dictGoogleAdsCampaignNameAlias = self.__getCampaignNameAlias(sGoogeAdsDataPath)

        # retrieve naver powerlink campaign name alias info
        sNaverPowerlinkDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'naver_ad')
        self.__g_dictNaverPowerlinkCampaignNameAlias = self.__getCampaignNameAlias(sNaverPowerlinkDataPath)
        self.__proc_media_perf_log(sDataPath)
        
        # stop if errornous source medium list not empty
        if len(self.__g_lstErrornousMedia) > 0:
            self.__g_lstErrornousMedia = sorted(set(self.__g_lstErrornousMedia))
            self._printDebug('errornous media names has been detected!')
            for sMedia in self.__g_lstErrornousMedia:
                self._printDebug(sMedia)
        
        self.__registerSourceMediumTerm()

    def __getSourceMediaNameAlias(self, sParentDataPath):
        s_alias_filepath = os.path.join(sParentDataPath, 'alias_info_source_media.tsv')
        if os.path.isfile(s_alias_filepath):
            with codecs.open(s_alias_filepath, 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if nRowCnt > 0:
                        self.__g_dictSourceMediaNameAliasInfo[row[0]] = {'alias':row[1]}

                    nRowCnt = nRowCnt + 1
        return

    def __proc_transaction_log(self, s_data_path):
        # print('proc_transaction_log')
        self._printDebug('UA Transaction log has been started\n')
        # traverse directory and categorize data files
        lst_transaction_log = []
        lst_data_file = os.listdir(s_data_path)
        lst_data_file.sort()
        n_idx = 0
        n_sentinel = len(lst_data_file)
        dict_query = {'transactionRevenueByTrId.tsv': 'rev'}
        
        try:
            for s_filename in lst_data_file:
                s_data_file_fullname = os.path.join(s_data_path, s_filename)
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
                            if not self._continue_iteration():
                                break
                            lst_tmp_row = row[1:]  # remove transaction ID
                            dictRst = self.__parseGaRow(lst_tmp_row, s_data_file_fullname)
                            del lst_tmp_row
                            lst_transaction_log.append([row[0], s_ua_type, dictRst['source'], dictRst['rst_type'], 
                                dictRst['medium'], str(dictRst['brd']), dictRst['campaign1st'], dictRst['campaign2nd'], 
                                dictRst['campaign3rd'], row[2], str(row[4]), s_data_date])
                else:
                    self._printDebug('pass ' + s_data_file_fullname + ' does not exist')

                self._printProgressBar(n_idx + 1, n_sentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
                n_idx += 1
            
            n_idx = 0
            n_sentinel = len(lst_transaction_log)
            with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
                oSvMysql.setTablePrefix(self.__g_sTblPrefix)
                oSvMysql.set_app_name('svplugins.ga_register_db')
                oSvMysql.initialize(self._g_dictSvAcctInfo)
                for lst_single_row in lst_transaction_log:
                    if not self._continue_iteration():
                        break
                    # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                    oSvMysql.executeQuery('insertGaTransactionDailyLog', 
                        lst_single_row[0], lst_single_row[1], lst_single_row[2], lst_single_row[3], lst_single_row[4], 
                        lst_single_row[5], lst_single_row[6], lst_single_row[7], lst_single_row[8], lst_single_row[9], 
                        str(lst_single_row[10]), lst_single_row[11])

                    self._printProgressBar(n_idx + 1, n_sentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                    n_idx += 1
            del lst_transaction_log
        except Exception as err:
            self._printDebug(err)
            
        self._printDebug('UA Transaction log has been finished\n')

    def __proc_media_perf_log(self, sDataPath):
        """ process homepage level data """
        self._printDebug('UA media performance log has been started\n')
        # traverse directory and categorize data files
        lstDataFiles = os.listdir(sDataPath)
        lstDataFiles.sort()
        
        nIdx = 0
        nSentinel = len(lstDataFiles)
        dictQuery = {'avgSessionDuration.tsv':'dur_sec', 'bounceRate.tsv':'b_per', 'pageviewsPerSession.tsv':'pvs', 
            'percentNewSessions.tsv':'new_per', 'sessions.tsv':'sess', 
            # transactions.tsv is for old version; transactionRevenueByTrId.tsv is for a log after 2021-Nov
            'transactions.tsv':'trs', 'transactionRevenueByTrId.tsv':'rev'}

        for sFilename in lstDataFiles:
            sDataFileFullname = os.path.join(sDataPath, sFilename)
            if sFilename == 'archive':
                continue
            
            aFile = sFilename.split('_')
            sDataDate = aFile[0]
            sUaType = self.__g_oSvCampaignParser.get_ua(aFile[1])
            sSpecifier = aFile[2]
            if sSpecifier in dictQuery:  #lst_analyzing_filename:
                sIdxName = dictQuery[sSpecifier]
            else:
                continue
            
            if os.path.isfile(sDataFileFullname):
                with open(sDataFileFullname, 'r') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t', skipinitialspace=True)
                    for row in reader:
                        if not self._continue_iteration():
                            break
                        try:
                            if sSpecifier == 'transactionRevenueByTrId.tsv':
                                row = row[1:]  # remove transaction ID
                        except Exception as err:
                            self._printDebug('1111')
                            self._printDebug(err)
                            self._printDebug(row)
                        
                        try:
                            dictRst = self.__parseGaRow(row, sDataFileFullname)
                        except Exception as err:
                            self._printDebug('2222')
                            self._printDebug(err)
                            self._printDebug(dictRst['source'])
                            self._printDebug(dictRst['rst_type'])
                            self._printDebug(dictRst['medium'])
                            self._printDebug(dictRst['brd'])
                            self._printDebug(dictRst['campaign1st'])
                            self._printDebug(dictRst['campaign2nd'])
                            self._printDebug(dictRst['campaign3rd'])
                            self._printDebug(sDataFileFullname)

                        try:
                            sTerm = row[2]
                        except Exception as err:
                            self._printDebug('3333')
                            self._printDebug(err)
                            self._printDebug(sTerm)
                        
                        try:
                            sReportId = sDataDate+'|@|'+sUaType+'|@|'+dictRst['source']+'|@|'+dictRst['rst_type']+'|@|'+ \
                                dictRst['medium']+'|@|'+str(dictRst['brd'])+'|@|'+dictRst['campaign1st']+'|@|'+dictRst['campaign2nd']+'|@|'+\
                                dictRst['campaign3rd']+'|@|'+sTerm
                        except Exception as err:
                            self._printDebug('4444')
                            self._printDebug(err)
                            self._printDebug(dictRst['source'])
                            self._printDebug(dictRst['rst_type'])
                            self._printDebug(dictRst['medium'])
                            self._printDebug(dictRst['brd'])
                            self._printDebug(dictRst['campaign1st'])
                            self._printDebug(dictRst['campaign2nd'])
                            self._printDebug(dictRst['campaign3rd'])
                            self._printDebug(sReportId)
                            
                            if self.__g_dictGaRaw.get(sReportId, self.__g_sSvNull) == self.__g_sSvNull:  # if not exists
                                self.__g_dictGaRaw[sReportId] = {
                                    'sess':0,'new_per':0,'b_per':0,'dur_sec':0,'pvs':0,'trs':0, 'rev':0  # , 'ua':sUaType
                                }
                            self.__g_dictGaRaw[sReportId][sIdxName] = float(row[3])
                        
                self.__archive_ga_data_file(sDataPath, sFilename)
            else:
                self._printDebug('pass ' + sDataFileFullname + ' does not exist')

            self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
            nIdx += 1
       

        self._printDebug('UA media performance log has been finished\n')
        
    def __getCampaignNameAlias(self, sParentDataPath):
        dictCampaignNameAliasInfo = {}
        s_alias_filepath = os.path.join(sParentDataPath, 'alias_info_campaign.tsv')
        if os.path.isfile(s_alias_filepath):
            with codecs.open(s_alias_filepath, 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if nRowCnt > 0:
                        dictCampaignNameAliasInfo[row[0]] = {'source':row[1], 'rst_type':row[2], 
                            'medium':row[3], 'camp1st':row[4], 'camp2nd':row[5], 'camp3rd':row[6] }

                    nRowCnt = nRowCnt + 1
        return dictCampaignNameAliasInfo

    def __registerSourceMediumTerm(self):
        nIdx = 0
        nSentinel = len(self.__g_dictGaRaw)
        with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.ga_register_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            for sReportId, dict_single_raw in self.__g_dictGaRaw.items():
                if not self._continue_iteration():
                    break
                
                aReportType = sReportId.split('|@|')
                sDataDate = datetime.strptime(aReportType[0], "%Y%m%d")
                sUaType = aReportType[1]
                sSource = aReportType[2]
                sRstType = aReportType[3]
                sMedium = aReportType[4]
                bBrd = aReportType[5]
                sCampaign1st = aReportType[6]
                sCampaign2nd = aReportType[7]
                sCampaign3rd = aReportType[8]
                sTerm = aReportType[9]
                # should check if there is duplicated date + SM log
                # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                oSvMysql.executeQuery('insertGaCompiledDailyLog', sUaType, sSource, sRstType, sMedium, 
                    bBrd, sCampaign1st, sCampaign2nd, sCampaign3rd, sTerm, 
                    dict_single_raw['sess'], str(dict_single_raw['new_per']), str(dict_single_raw['b_per']), 
                    str(dict_single_raw['dur_sec']), str(dict_single_raw['pvs']), dict_single_raw['trs'], 
                    dict_single_raw['rev'], 0, sDataDate)

                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __parseGaRow(self, lstRow, sDataFileFullname):
        if self.__g_dictSourceMediaNameAliasInfo.get(lstRow[0], self.__g_sSvNull) != self.__g_sSvNull:  # if exists
            sSourceMediumAlias = str(self.__g_dictSourceMediaNameAliasInfo[lstRow[0]]['alias'])
            aSourceMedium = sSourceMediumAlias.split(' / ')
        else:
            aSourceMedium = lstRow[0].split(' / ')

        sSource = aSourceMedium[0]
        sMedium = aSourceMedium[1]
        sCampaignCode = lstRow[1]
        sRstType = ''
        sCampaign1st = ''
        sCampaign2nd = ''
        sCampaign3rd = ''
        sTerm = lstRow[2]
        # "skin-skin14--shop3.ratestw.cafe24.com" like source string occurs confusion
        m = re.search(r"[-\w.]+", sSource)
        nHttpPos = m.group(0).find('http')
        if nHttpPos > -1:
            if len(sSource) > 30: # remedy erronous UTM parameter
                m = re.search(r"https?://(\w*:\w*@)?[-\w.]+(:\d+)?(/([\w/_.]*(\?\S+)?)?)?", sSource)
                try:
                    if len( m.group(0) ):
                        nPos = sSource.find('utm_source')
                        if nPos > -1:
                            sRightPart = sSource[nPos:]
                            aRightPart = sRightPart.split('=')
                            sSource = aRightPart[1]
                        else:
                            m1 = re.search(r"[^https?://](\w*:\w*@)?[-\w.]+(:\d+)?", sSource)
                            sSource = m1.group(0)
                except AttributeError: # retry to handle very weird source tagging
                    # this block handles '＆' which is not & that naver shopping foolishly occurs
                    self._printDebug('weird source found on ' + sDataFileFullname + ' -> unicode ampersand which is not &')
                    # same source code needs to be method - begin
                    sEncodedSource = sSource.encode('UTF-8')
                    sEncodedSource = str(sEncodedSource)
                    aWeirdSource = sEncodedSource.split("\\xef\\xbc\\x86") # utf-8 encoded unicode ampersand ��
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
                    self._printDebug(sDataFileFullname)
                    self._printDebug(lstRow)
                    #self._printDebug(inst.args)     # arguments stored in .args
                    self._printDebug(inst)			# __str__ allows args to be printed directly, but may be overridden in exception subclasses
        else: # ex) naver��utm_medium=cpc��utm_campaign=NV_PS_CPC_NVSHOP_00_00��utm_term=NVSHOP_4741��n_media=33421��n_query=��ɼ��漮��n_rank=1��n_ad_group=grp-a001-02-000000002830061��n_ad=nad-a001-02-000000011190197��n_campaign_type=2��n_
            # same source code needs to be method - begin
            sEncodedSource = sSource.encode('UTF-8')
            sEncodedSource = str(sEncodedSource)
            nUnicodeAmpersandPos = sEncodedSource.find("\\xef\\xbc\\x86")
            if nUnicodeAmpersandPos > -1:
                aWeirdSource = sEncodedSource.split("\\xef\\xbc\\x86") # utf-8 encoded unicode ampersand ��
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
        dictValidMedium = self.__g_oSvCampaignParser.validate_ga_medium_tag(sMedium)
        if dictValidMedium['medium'] != 'weird':
            sMedium = dictValidMedium['medium']
            if dictValidMedium['found_pos'] > -1:
                nPos = dictValidMedium['found_pos'] + len( dictValidMedium['medium'])
                sRightPart = sMedium[nPos:]
                aRightPart = sRightPart.split('=')
                if aRightPart[0] == 'utm_campaign':
                    sCampaignCode = aRightPart[1]
        else:
            self.__g_lstErrornousMedia.append(sDataFileFullname + ' -> ' + sSource+' / ' + sMedium)

        if sSource == 'google' and sMedium == 'cpc':
            # lookup alias db, if non sv standard code
            if self.__g_dictGoogleAdsCampaignNameAlias.get(sCampaignCode, self.__g_sSvNull) != self.__g_sSvNull:  # if exists
                if self.__g_dictGoogleAdsCampaignNameAlias[sCampaignCode]['source'] == 'YT':
                    sSource = 'youtube'
                if self.__g_dictGoogleAdsCampaignNameAlias[sCampaignCode]['medium'] == 'DISP':
                    sMedium = 'display'
                sCampaignCode = self.__g_dictGoogleAdsCampaignNameAlias[sCampaignCode]['source'] + '_' + \
                                self.__g_dictGoogleAdsCampaignNameAlias[sCampaignCode]['rst_type'] + '_' + \
                                self.__g_dictGoogleAdsCampaignNameAlias[sCampaignCode]['medium'] + '_' + \
                                self.__g_dictGoogleAdsCampaignNameAlias[sCampaignCode]['camp1st'] + '_' + \
                                self.__g_dictGoogleAdsCampaignNameAlias[sCampaignCode]['camp2nd'] + '_' + \
                                self.__g_dictGoogleAdsCampaignNameAlias[sCampaignCode]['camp3rd']
        elif sSource == 'naver' and sMedium == 'cpc':
            # lookup alias db, if non sv standard code
            if self.__g_dictNaverPowerlinkCampaignNameAlias.get(sCampaignCode, self.__g_sSvNull) != self.__g_sSvNull:  # if exists
                sCampaignCode = self.__g_dictNaverPowerlinkCampaignNameAlias[sCampaignCode]['source'] + '_' + \
                                self.__g_dictNaverPowerlinkCampaignNameAlias[sCampaignCode]['rst_type'] + '_' + \
                                self.__g_dictNaverPowerlinkCampaignNameAlias[sCampaignCode]['medium'] + '_' + \
                                self.__g_dictNaverPowerlinkCampaignNameAlias[sCampaignCode]['camp1st'] + '_' + \
                                self.__g_dictNaverPowerlinkCampaignNameAlias[sCampaignCode]['camp2nd'] + '_' + \
                                self.__g_dictNaverPowerlinkCampaignNameAlias[sCampaignCode]['camp3rd']
        
        dictCampaignRst = self.__g_oSvCampaignParser.parse_campaign_code(s_sv_campaign_code=sCampaignCode)
        if dictCampaignRst['source'] == 'unknown': # handle no sv campaign code data
            if sMedium == 'cpc' or sMedium == 'display':
                dictCampaignRst['rst_type'] = 'PS'
            else:
                dictCampaignRst['rst_type'] = 'NS'

        bBrd = dictCampaignRst['brd']
        sRstType = dictCampaignRst['rst_type']
        if len(dictCampaignRst['medium']) > 0: # sv campaign criteria first, GA auto categorize later
            sMedium = dictCampaignRst['medium']
        sCampaign1st = dictCampaignRst['campaign1st']
        sCampaign2nd = dictCampaignRst['campaign2nd']
        sCampaign3rd = dictCampaignRst['campaign3rd']
        # finally determine branded by term
        dict_brded_rst = self.__g_oSvCampaignParser.decide_brded_by_term(self.__g_sBrandedTruncPath, sTerm)
        if dict_brded_rst['b_error'] == True:
            self._printDebug(dict_brded_rst['s_err_msg'])
        elif dict_brded_rst['b_brded']:
            bBrd = 1
        # monitor weird source name - begin
        if len(sSource) > 50: 
            self._printDebug(sDataFileFullname)
            self._printDebug(lstRow)
            raise Exception('stop')
        # monitor weird source name - begin
        return {'source':sSource,'rst_type':sRstType,'medium':sMedium,'brd':bBrd,'campaign1st':sCampaign1st,'campaign2nd':sCampaign2nd,'campaign3rd':sCampaign3rd}

    def __archive_ga_data_file(self, s_data_path, s_cur_filename):
        if not os.path.exists(s_data_path):
            self._printDebug('error: google analytics source directory does not exist!' )
            return
        s_data_archive_path = os.path.join(s_data_path, 'archive')
        if not os.path.exists(s_data_archive_path):
            os.makedirs(s_data_archive_path)
        s_source_filepath = os.path.join(s_data_path, s_cur_filename)
        sArchiveDataFilePath = os.path.join(s_data_archive_path, s_cur_filename)
        shutil.move(s_source_filepath, sArchiveDataFilePath)


if __name__ == '__main__': # for console debugging
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('ga_register_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params are required for console execution.')
