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
import logging
from datetime import datetime
import sys
import os
import shutil
import csv
import calendar
import codecs

# 3rd party library

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svload/pandas_plugins')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_campaign_parser
    import sv_object
    import sv_plugin
    import campaign_alias
    import settings
else:
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svload.pandas_plugins import campaign_alias
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oSvCampaignParser = sv_campaign_parser.SvCampaignParser()  # None
    __g_lstIgnoreText = ['CRITERIA', # for old adwords API report; ignore report title row: CRITERIA_PERFORMANCE_REPORT (Nov 14, 2015)
                        'google', # for new google ads API report; ignore report title row: google_ads_api (v6)
                        'Campaign', # ignore column title row
                        'Total'] # for old adwords API report; ignore gross sum row

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_oLogger = logging.getLogger(__name__ + ' modified at 6th, Jun 2022')
        self._g_dictParam.update({'yyyymm':None})
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sBrandedTruncPath = None
        self.__g_sReplaceMonth = None
        self.__g_sTblPrefix = None
        self.__g_dictAdwRaw = None  # prevent duplication on a web console

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sBrandedTruncPath = None
        self.__g_sReplaceMonth = None
        self.__g_sTblPrefix = None
        self.__g_dictAdwRaw = None  # prevent duplication on a web console

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_sReplaceMonth = self._g_dictParam['yyyymm']
        self.__g_dictAdwRaw = {}  # prevent duplication on a web console

        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
          'adw_cid' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        lst_google_ads = dict_acct_info['adw_cid']
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.aw_register_db')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
        
        self.__g_sBrandedTruncPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_brand_id, 'branded_term.conf')
        if self.__g_sReplaceMonth != None:
            self._printDebug('-> replace aw raw data')
            self.__delete_certain_month()
        else:
            self._printDebug('-> register aw raw data')
        
        # begin - referring to raw_data_file, validate raw data file without registration
        lst_non_sv_convention_campaign_title = self.__validate_campaign_code(s_sv_acct_id, s_brand_id, lst_google_ads)
        if len(lst_non_sv_convention_campaign_title):
            for s_single_campaign in lst_non_sv_convention_campaign_title:
                self._printDebug('[' + s_single_campaign + '] should be filled!')
            self._task_post_proc(self._g_oCallback)
            return
        # end - referring to raw_data_file, validate raw data file without registration
        self.__arrange_gad_raw_data_file(s_sv_acct_id, s_brand_id, lst_google_ads)
        self.__register_db()
        self._task_post_proc(self._g_oCallback)

    def __delete_certain_month(self):
        nYr = int(self.__g_sReplaceMonth[:4])
        nMo = int(self.__g_sReplaceMonth[4:None])
        try:
            lstMonthRange = calendar.monthrange(nYr, nMo)
        except calendar.IllegalMonthError:
            self._printDebug('invalid yyyymm')
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        
        sStartDateRetrieval = self.__g_sReplaceMonth[:4] + '-' + self.__g_sReplaceMonth[4:None] + '-01'
        sEndDateRetrieval = self.__g_sReplaceMonth[:4] + '-' + self.__g_sReplaceMonth[4:None] + '-' + str(lstMonthRange[1])

        with sv_mysql.SvMySql() as oSvMysql:
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.aw_register_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            oSvMysql.executeQuery('deleteCompiledLogByPeriod', sStartDateRetrieval, sEndDateRetrieval)

    def __validate_campaign_code(self, sSvAcctId, s_brand_id, lstGoogleads):
        """ referring to raw_data_file, validate raw data file without registration """
        lst_non_sv_convention_campaign_title = []
        lstMergedDataFiles = []
        sParentDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, s_brand_id, 'adwords')
        for sGoogleadsCid in lstGoogleads:
            if self.__g_sReplaceMonth == None:
                self._printDebug('-> '+ sGoogleadsCid +' is validating AW data files')
                sDataPath = os.path.join(sParentDataPath, sGoogleadsCid, 'data')
            else:
                self._printDebug('-> '+ sGoogleadsCid +' is validating AW data files')
                sDataPath = os.path.join(sParentDataPath, sGoogleadsCid, 'data', 'closing')
            
            # traverse directory and categorize data files
            lstDataFiles = os.listdir(sDataPath)
            for nIdx, sDatafileName in enumerate(lstDataFiles):
                aFileExt = os.path.splitext(sDatafileName)
                if aFileExt[1] == '':
                    continue
                if self.__g_sReplaceMonth == None:
                    sMode = 'r' # means regular daily
                else:
                    sMode = 'c' # means closing monthly
                lstMergedDataFiles.append(sDatafileName + '|@|' + sGoogleadsCid + '|@|' + sMode)
        o_campaign_alias = campaign_alias.CampaignAliasInfo(sSvAcctId, s_brand_id)
        lstMergedDataFiles.sort()
        nIdx = 0
        nSentinel = len(lstMergedDataFiles)
        for sDataFileInfo in lstMergedDataFiles:
            if not self._continue_iteration():
                break

            aDataFileInfo = sDataFileInfo.split('|@|')
            sFilename = aDataFileInfo[0]
            sCid = aDataFileInfo[1]
            sMode = aDataFileInfo[2]
            if sMode == 'r':
                sDataFileFullname = os.path.join(sParentDataPath, sCid, 'data', sFilename)
            elif sMode == 'c':
                sDataFileFullname = os.path.join(sParentDataPath, sCid, 'data', 'closing', sFilename)
            
            if not os.path.isfile(sDataFileFullname):
                self._printDebug('pass ' + sDataFileFullname + ' does not exist')
                continue
            
            with open(sDataFileFullname, 'r') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                for row in reader:
                    lst_campaign_code = row[0].split('_')
                    if lst_campaign_code[0] in self.__g_lstIgnoreText:  # ignore TSV file header and tail
                        continue
                    dict_rst = self.__g_oSvCampaignParser.parse_campaign_code(row[0])
                    if not dict_rst['detected']:
                        dict_rst = self.__g_oSvCampaignParser.parse_campaign_code(row[1])
                        if not dict_rst['detected']:
                            dict_campaign_alias_rst = o_campaign_alias.get_detail_by_media_campaign_name(row[0])
                            dict_rst = dict_campaign_alias_rst['dict_ret']
                            if not dict_rst['detected']:  # retrieve campaign name alias info
                                lst_non_sv_convention_campaign_title.append(row[0])
                                continue
                    if dict_rst['source_code'] not in ['GG', 'YT']:  # if unacceptable googleads campaign name
                        sCampaignName = row[0]
                        self._printDebug('  ' + sCampaignName + '  ' + sDataFileFullname)
                        self._printDebug('weird googleads log!')
                        if self._g_bDaemonEnv:  # for running on dbs.py only
                            raise Exception('remove')
                        else:
                            return
            self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Validate data file:', suffix = 'Complete', length = 50)
            nIdx += 1
        del o_campaign_alias
        return list(set(lst_non_sv_convention_campaign_title))  # unique list

    def __arrange_gad_raw_data_file(self, sSvAcctId, sAcctTitle, lstGoogleads):
        """ referring to raw_data_file, arrange raw data file to register """
        lstMergedDataFiles = []
        sParentDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'adwords')
        for sGoogleadsCid in lstGoogleads:
            if self.__g_sReplaceMonth == None:
                self._printDebug('-> '+ sGoogleadsCid +' is registering AW data files')
                sDataPath = os.path.join(sParentDataPath, sGoogleadsCid, 'data')
            else:
                self._printDebug('-> '+ sGoogleadsCid +' is replacing AW data files')
                sDataPath = os.path.join(sParentDataPath, sGoogleadsCid, 'data', 'closing')
            
            # traverse directory and categorize data files
            lstDataFiles = os.listdir(sDataPath)
            for nIdx, sDatafileName in enumerate(lstDataFiles):
                aFileExt = os.path.splitext(sDatafileName)
                if aFileExt[1] == '':
                    continue
                if self.__g_sReplaceMonth == None:
                    sMode = 'r' # means regular daily
                else:
                    sMode = 'c' # means closing monthly
                lstMergedDataFiles.append(sDatafileName + '|@|' + sGoogleadsCid + '|@|' + sMode)
        
        o_campaign_alias = campaign_alias.CampaignAliasInfo(sSvAcctId, sAcctTitle)
        lstMergedDataFiles.sort()
        nIdx = 0
        nSentinel = len(lstMergedDataFiles)
        for sDataFileInfo in lstMergedDataFiles:
            if not self._continue_iteration():
                break

            aDataFileInfo = sDataFileInfo.split('|@|')
            sFilename = aDataFileInfo[0]
            sCid = aDataFileInfo[1]
            sMode = aDataFileInfo[2]
            if sMode == 'r':
                sSourceDataPath = os.path.join(sParentDataPath, sCid, 'data') # to archive data file
                sDataFileFullname = os.path.join(sParentDataPath, sCid, 'data', sFilename)
            elif sMode == 'c':
                sSourceDataPath = os.path.join(sParentDataPath, sCid, 'data', 'closing') # to archive data file
                sDataFileFullname = os.path.join(sParentDataPath, sCid, 'data', 'closing', sFilename)
            
            if not os.path.isfile(sDataFileFullname):
                self._printDebug('pass ' + sDataFileFullname + ' does not exist')
                continue
            
            with open(sDataFileFullname, 'r') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                for row in reader:
                    # bBrd = 0
                    lst_campaign_code = row[0].split('_')
                    if lst_campaign_code[0] in self.__g_lstIgnoreText:  # ignore TSV file header and tail
                        continue
                    
                    dict_rst = self.__g_oSvCampaignParser.parse_campaign_code(row[0])
                    if not dict_rst['detected']:
                        dict_rst = self.__g_oSvCampaignParser.parse_campaign_code(row[1])
                        if not dict_rst['detected']:
                            dict_campaign_alias_rst = o_campaign_alias.get_detail_by_media_campaign_name(row[0])
                            dict_rst = dict_campaign_alias_rst['dict_ret']
                            if not dict_rst['detected']:  # if unacceptable googleads campaign name
                                sCampaignName = row[0]
                                self._printDebug('  ' + sCampaignName + '  ' + sDataFileFullname)
                                self._printDebug('weird googleads log!')
                                if self._g_bDaemonEnv:  # for running on dbs.py only
                                    raise Exception('remove')
                                else:
                                    return

                    if dict_rst['source_code'] in ['GG', 'YT']:
                        sSource = dict_rst['source_code']
                        sRstType = dict_rst['rst_type']
                        sMedium = dict_rst['medium_code']
                        sCampaign1st = dict_rst['campaign1st']
                        sCampaign2nd = dict_rst['campaign2nd']
                        sCampaign3rd = dict_rst['campaign3rd']
                    else:  # if unacceptable googleads campaign name
                        sCampaignName = row[0]
                        self._printDebug('  ' + sCampaignName + '  ' + sDataFileFullname)
                        self._printDebug('weird googleads log!')
                        if self._g_bDaemonEnv:  # for running on dbs.py only
                            raise Exception('remove')
                        else:
                            return
                    
                    sUa = self.__g_oSvCampaignParser.get_ua(row[6])
                    sTerm = row[2]
                    sPlacement = None
                    # diff between "adwords term" and "adwords placement"
                    if self.__g_oSvCampaignParser.get_gad_placement_tag_by_term(sTerm) == True:
                        sPlacement = sTerm
                        sTerm = None
                    # finally determine branded by term
                    bBrd = 0
                    dict_brded_rst = self.__g_oSvCampaignParser.decide_brded_by_term(self.__g_sBrandedTruncPath, sTerm)
                    if dict_brded_rst['b_error'] == True:
                        self._printDebug(dict_brded_rst['s_err_msg'])
                    elif dict_brded_rst['b_brded']:
                        bBrd = 1
                    del dict_brded_rst

                    nImpression = int(row[3])
                    nClick = int(row[4])
                    nCost = int(int(row[5]) / 1000000)
                    nConvCnt = int(float(row[7]))
                    nConvAmnt = int(float(row[8]))
                    sDatadate = row[9]
                    sReportId = sCid+'|@|'+sDatadate+'|@|'+sUa+'|@|'+sSource+'|@|'+sRstType+'|@|'+ sMedium+'|@|'+str(bBrd)+'|@|'+\
                        sCampaign1st+'|@|'+\
                        sCampaign2nd+'|@|'+\
                        sCampaign3rd+'|@|'+\
                        str(sTerm)+'|@|'+\
                        str(sPlacement)

                    if self.__g_dictAdwRaw.get(sReportId, 0):  # returns 0 if sRowId does not exist
                        self.__g_dictAdwRaw[sReportId]['imp'] += nImpression
                        self.__g_dictAdwRaw[sReportId]['clk'] += nClick
                        self.__g_dictAdwRaw[sReportId]['cost'] += nCost
                        self.__g_dictAdwRaw[sReportId]['conv_cnt'] += nConvCnt
                        self.__g_dictAdwRaw[sReportId]['conv_amnt'] += nConvAmnt
                    else:  # if new log requested
                        self.__g_dictAdwRaw[sReportId] = {
                            'imp':nImpression,'clk':nClick,'cost':nCost,'conv_cnt':nConvCnt,'conv_amnt':nConvAmnt
                        }
            self.__archive_data_file(sSourceDataPath, sFilename)
            self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
            nIdx += 1
        del o_campaign_alias

    def __register_db(self):
        nIdx = 0
        nSentinel = len(self.__g_dictAdwRaw)
        with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.aw_register_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)

            for sReportId, dict_row in self.__g_dictAdwRaw.items():
                if not self._continue_iteration():
                    break
                aReportType = sReportId.split('|@|')
                sAdwordsCid = aReportType[0]
                sDataDate = datetime.strptime(aReportType[1], "%Y-%m-%d")
                sUaType = aReportType[2]
                sSource = aReportType[3].strip()
                sRstType = aReportType[4].strip()
                sMedium = self.__g_oSvCampaignParser.get_sv_medium_tag(aReportType[5])
                bBrd = aReportType[6]
                sCampaign1st = aReportType[7].strip()
                sCampaign2nd = aReportType[8].strip()
                sCampaign3rd = aReportType[9].strip()
                sTerm = aReportType[10].strip()
                sPlacement = aReportType[11].strip()
                # should check if there is duplicated date + SM log
                # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                oSvMysql.executeQuery('insertAwCompiledDailyLog', sAdwordsCid, sUaType, sSource, sRstType, sMedium, 
                    bBrd, sCampaign1st, sCampaign2nd, sCampaign3rd, sTerm, sPlacement, 
                    str(dict_row['cost']), dict_row['imp'], str(dict_row['clk']),
                    str(dict_row['conv_cnt']), str(dict_row['conv_amnt']), sDataDate)

                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __archive_data_file(self, sDataPath, sCurrentFileName):
        sSourcePath = sDataPath
        if not os.path.exists(sSourcePath):
            self._printDebug('error: adw source directory does not exist!')
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        
        if self.__g_sReplaceMonth == None:
            sArchiveDataPath = sDataPath +'/archive'
        else:
            sArchiveDataPath = sDataPath +'/../archive'

        if not os.path.exists(sArchiveDataPath):
            os.makedirs(sArchiveDataPath)

        sSourceFilePath = os.path.join(sDataPath, sCurrentFileName)
        sArchiveDataFilePath = os.path.join(sArchiveDataPath, sCurrentFileName)		
        shutil.move(sSourceFilePath, sArchiveDataFilePath)


if __name__ == '__main__': # for console debugging
    # python task.py config_loc=1/1 yyyymm=201811
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('aw_register_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params are required for console execution.')
