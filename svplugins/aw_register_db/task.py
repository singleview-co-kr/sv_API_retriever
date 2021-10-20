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
    import sv_mysql
    import sv_campaign_parser
    import sv_object, sv_plugin
else:
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_object, sv_plugin


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sBrandedTruncPath = None
    __g_oSvCampaignParser = None
    __g_sReplaceMonth = None
    __g_sTblPrefix = None
    __g_lstIgnoreText = ['CRITERIA', # for old adwords API report; ignore report title row: CRITERIA_PERFORMANCE_REPORT (Nov 14, 2015)
                        'google', # for new google ads API report; ignore report title row: google_ads_api (v6)
                        'Campaign', # ignore column title row
                        'Total'] # for old adwords API report; ignore gross sum row
    __g_dictAdwRaw = None  # prevent duplication on a web console

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sVersion = '1.0.3'
        self._g_sLastModifiedDate = '19th, Oct 2021'
        self._g_oLogger = logging.getLogger(__name__ + ' v'+self._g_sVersion)
        self._g_dictParam.update({'yyyymm':None})

    def do_task(self, o_callback):
        self.__g_sReplaceMonth = self._g_dictParam['yyyymm']
        self.__g_dictAdwRaw = {}  # prevent duplication on a web console

        oResp = self._task_pre_proc(o_callback)
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self._printDebug('stop -> invalid config_loc')
            return
        
        self.__g_oSvCampaignParser = sv_campaign_parser.svCampaignParser()
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        lst_google_ads = dict_acct_info[s_sv_acct_id]['adw_cid']
        self.__g_sTblPrefix = dict_acct_info[s_sv_acct_id]['tbl_prefix']
        
        with sv_mysql.SvMySql('svplugins.aw_register_db') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.initialize()

        self.__g_sBrandedTruncPath = os.path.join(self._g_sAbsRootPath, 'files', s_sv_acct_id, s_acct_title, 'branded_term.conf')
        if self.__g_sReplaceMonth != None:
            self._printDebug('-> replace aw raw data')
            self.__deleteCertainMonth()
        else:
            self._printDebug('-> register aw raw data')

        self.__arrangeAwRawDataFile(s_sv_acct_id, s_acct_title, lst_google_ads)
        self.__registerDb()

        self._task_post_proc(o_callback)

    def __deleteCertainMonth(self):
        nYr = int(self.__g_sReplaceMonth[:4])
        nMo = int(self.__g_sReplaceMonth[4:None])
        try:
            lstMonthRange = calendar.monthrange(nYr, nMo)
        except calendar.IllegalMonthError:
            self._printDebug('invalid yyyymm')
            return
        
        sStartDateRetrieval = self.__g_sReplaceMonth[:4] + '-' + self.__g_sReplaceMonth[4:None] + '-01'
        sEndDateRetrieval = self.__g_sReplaceMonth[:4] + '-' + self.__g_sReplaceMonth[4:None] + '-' + str(lstMonthRange[1])
        with sv_mysql.SvMySql('svplugins.aw_register_db') as oSvMysql:
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            lstRst = oSvMysql.executeQuery('deleteCompiledLogByPeriod', sStartDateRetrieval, sEndDateRetrieval)

    def __getCampaignNameAlias(self, sParentDataPath):
        dictCampaignNameAliasInfo = {}
        try:
            with codecs.open(os.path.join(sParentDataPath, 'alias_info_campaign.tsv'), 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if nRowCnt > 0:
                        dictCampaignNameAliasInfo[row[0]] = {'source':row[1], 'rst_type':row[2], 'medium':row[3], 'camp1st':row[4], 'camp2nd':row[5], 'camp3rd':row[6] }

                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            pass
        
        return dictCampaignNameAliasInfo

    def __arrangeAwRawDataFile(self, sSvAcctId, sAcctTitle, lstGoogleads):
        lstMergedDataFiles = []
        sParentDataPath = os.path.join(self._g_sAbsRootPath, 'files', sSvAcctId, sAcctTitle, 'adwords')
        # retrieve campaign name alias info
        dictCampaignNameAlias = self.__getCampaignNameAlias(sParentDataPath)

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
            
            try:
                with open(sDataFileFullname, 'r') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    for row in reader:
                        bBrd = 0
                        lstCampaignCode = row[0].split('_')
                        if lstCampaignCode[0] in self.__g_lstIgnoreText:  # ignore TSV file header and tail
                            continue
                        
                        # process body
                        if len(lstCampaignCode) > 3: # adwords campaign name follows singleview campaign code
                            if lstCampaignCode[0] == 'OLD': # OLD campaign
                                lstCampaignCode.pop(0)
                                nLastIdx = len(lstCampaignCode) - 1
                                sTempCampaign3rd = lstCampaignCode[nLastIdx]
                                lstCampaignCode[nLastIdx] = sTempCampaign3rd + '_OLD'
                        else: # adwords group name follows singleview campaign code
                            self._printDebug(lstCampaignCode)
                            lstCampaignCode = row[1].split('_')
                        
                        if len(lstCampaignCode) == 6 and (lstCampaignCode[0] == 'GG' or lstCampaignCode[0] == 'YT'):
                                sSource = lstCampaignCode[0]
                                sRstType = lstCampaignCode[1]
                                sMedium = lstCampaignCode[2]
                                sCampaign1st = lstCampaignCode[3]
                                sCampaign2nd = lstCampaignCode[4]
                                sCampaign3rd = lstCampaignCode[5]
                        else:
                            try:
                                sCampaignName = row[0]
                                dictCampaignNameAlias[ sCampaignName ]
                                sSource = dictCampaignNameAlias[ sCampaignName ]['source']
                                sRstType = dictCampaignNameAlias[ sCampaignName ]['rst_type']
                                sMedium = dictCampaignNameAlias[ sCampaignName ]['medium']
                                sCampaign1st = dictCampaignNameAlias[ sCampaignName ]['camp1st']
                                sCampaign2nd = dictCampaignNameAlias[ sCampaignName ]['camp2nd']
                                sCampaign3rd = dictCampaignNameAlias[ sCampaignName ]['camp3rd']
                            except KeyError: # if unacceptable googleads campaign name
                                self._printDebug('  ' + sCampaignName + '  ' + sDataFileFullname)
                                self._printDebug('weird googleads log!')
                                return
                        
                        sUa = self.__g_oSvCampaignParser.getUa(row[6])
                        sTerm = row[2]
                        sPlacement = None
                        # diff between "adwords term" and "adwords placement"
                        if self.__g_oSvCampaignParser.decideAwPlacementTagByTerm(sTerm) == True:
                            sPlacement = sTerm
                            sTerm = None
                        # finally determine branded by term
                        if self.__g_oSvCampaignParser.decideBrandedByTerm(self.__g_sBrandedTruncPath, sTerm) == True:
                            bBrd = 1

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

                        try: # if designated log already created
                            self.__g_dictAdwRaw[sReportId]
                            self.__g_dictAdwRaw[sReportId]['imp'] += nImpression
                            self.__g_dictAdwRaw[sReportId]['clk'] += nClick
                            self.__g_dictAdwRaw[sReportId]['cost'] += nCost
                            self.__g_dictAdwRaw[sReportId]['conv_cnt'] += nConvCnt
                            self.__g_dictAdwRaw[sReportId]['conv_amnt'] += nConvAmnt
                        except KeyError: # if new log requested
                            self.__g_dictAdwRaw[sReportId] = {
                                'imp':nImpression,'clk':nClick,'cost':nCost,'conv_cnt':nConvCnt,'conv_amnt':nConvAmnt
                            }
                self.__archiveGaDataFile(sSourceDataPath, sFilename)
            except FileNotFoundError:
                self._printDebug('pass ' + sDataFileFullname + ' does not exist')
            
            self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
            nIdx += 1

    def __registerDb(self):
        nIdx = 0
        nSentinel = len(self.__g_dictAdwRaw)
        with sv_mysql.SvMySql('svplugins.aw_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            for sReportId in self.__g_dictAdwRaw:
                if not self._continue_iteration():
                    break

                aReportType = sReportId.split('|@|')
                sAdwordsCid = aReportType[0]
                sDataDate = datetime.strptime(aReportType[1], "%Y-%m-%d")
                sUaType = aReportType[2]
                sSource = aReportType[3].strip()
                sRstType = aReportType[4].strip()
                sMedium = self.__g_oSvCampaignParser.getSvMediumTag(aReportType[5]).strip()
                bBrd = aReportType[6]
                sCampaign1st = aReportType[7].strip()
                sCampaign2nd = aReportType[8].strip()
                sCampaign3rd = aReportType[9].strip()
                sTerm = aReportType[10].strip()
                sPlacement = aReportType[11].strip()
                    
                # should check if there is duplicated date + SM log
                # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                lstFetchRst = oSvMysql.executeQuery('insertAwCompiledDailyLog', sAdwordsCid, sUaType, sSource, sRstType, sMedium, bBrd, sCampaign1st, sCampaign2nd, sCampaign3rd,
                    sTerm, sPlacement, str(self.__g_dictAdwRaw[sReportId]['cost']), self.__g_dictAdwRaw[sReportId]['imp'], str(self.__g_dictAdwRaw[sReportId]['clk']),
                    str(self.__g_dictAdwRaw[sReportId]['conv_cnt']), str(self.__g_dictAdwRaw[sReportId]['conv_amnt']), sDataDate)

                self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __archiveGaDataFile(self, sDataPath, sCurrentFileName):
        sSourcePath = sDataPath
        if not os.path.exists(sSourcePath):
            self._printDebug( 'error: adw source directory does not exist!' )
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
    # python task.py analytical_namespace=test config_loc=1/ynox yyyymm=201811
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('aw_register_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [analytical_namespace] [config_loc] params are required for console execution.')
