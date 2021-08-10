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
from datetime import datetime, timedelta
import os
import shutil
import csv
import re
import calendar
import csv
import codecs

# singleview library
if __name__ == '__main__': # for console debugging
    import sys
    sys.path.append('../../classes')
    import sv_http
    import sv_mysql
    import sv_campaign_parser
    import sv_api_config_parser
    sys.path.append('../../conf') # singleview config
    import basic_config
else: # for platform running
    from classes import sv_http
    from classes import sv_mysql
    from classes import sv_campaign_parser
    from classes import sv_api_config_parser
    # singleview config
    from conf import basic_config # singleview config

class svJobPlugin():
    __g_sVersion = '1.0.1'
    __g_sLastModifiedDate = '6th, Aug 2021'
    __g_sBrandedTruncPath = None
    __g_sConfigLoc = None
    __g_lstBrandedTrunc = None
    __g_oLogger = None
    __g_oSvCampaignParser = None
    __g_dictAdwRaw = dict()
    __g_sRetrieveMonth = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_sConfigLoc = dictParams['config_loc']
        self.__g_sRetrieveMonth = dictParams['yyyymm']

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __printDebug( self, sMsg ):
        if __name__ == '__main__': # for console debugging
            print( sMsg )
        else: # for platform running
            if( self.__g_oLogger is not None ):
                self.__g_oLogger.debug( sMsg )

    def __getHttpResponse(self, sTargetUrl ):
        oSvHttp = sv_http.svHttpCom(sTargetUrl)
        oResp = oSvHttp.getUrl()
        oSvHttp.close()
        if( oResp['error'] == -1 ):
            if( oResp['variables'] ): # oResp['variables'] list has items
                try:
                   oResp['variables']['todo']
                except KeyError: # if ['variables']['todo'] is not defined
                    self.__printDebug( '__checkHttpResp error occured but todo is not defined -> continue')
                else: # if ['variables']['todo'] is defined
                    sTodo = oResp['variables']['todo']
                    if( sTodo == 'stop' ):
                        self.__printDebug('HTTP response raised exception!!')
                        raise Exception(sTodo)
        return oResp

    def procTask(self):
        # oRegEx = re.compile(r"https?:\/\/[\w\-.]+\/\w+\/\w+\w/?$") # host_url pattern ex) /aaa/bbb or /aaa/bbb/
        # m = oRegEx.search(self.__g_sConfigLoc) # match() vs search()
        # if( m ): # if arg matches desinated host_url
        #    sTargetUrl = self.__g_sConfigLoc + '?mode=check_status'
        #    oResp = self.__getHttpResponse( sTargetUrl )
        # else:
        #    oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        #    oResp = oSvApiConfigParser.getConfig()
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        oResp = oSvApiConfigParser.getConfig()

        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self.__printDebug('invalid config_loc')
            raise Exception('stop')
        
        self.__g_oSvCampaignParser = sv_campaign_parser.svCampaignParser()
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        lst_google_ads = dict_acct_info[s_sv_acct_id]['adw_cid']
        
        with sv_mysql.SvMySql('job_plugins.aw_register_db') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(s_acct_title)
            o_sv_mysql.initialize()

        self.__g_sBrandedTruncPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + s_sv_acct_id +'/' + s_acct_title + '/branded_term.conf'
        
        if( self.__g_sRetrieveMonth != None ):
            self.__printDebug( '-> replace aw raw data' )
            self.__deleteCertainMonth(s_acct_title)
        else:
            self.__printDebug( '-> register aw raw data' )

        self.__arrangeAwRawDataFile(s_sv_acct_id, s_acct_title, lst_google_ads)
        self.__registerDb( s_sv_acct_id, s_acct_title)
        
        """
        aAcctInfo = oResp['variables']['acct_info']
        if aAcctInfo is not None:
            self.__g_oSvCampaignParser = sv_campaign_parser.svCampaignParser()

            for sSvAcctId in aAcctInfo:
                sAcctTitle = aAcctInfo[sSvAcctId]['account_title']
                with sv_mysql.SvMySql('job_plugins.aw_register_db') as oSvMysql:
                    oSvMysql.setTablePrefix(sAcctTitle)
                    oSvMysql.initialize()

                self.__g_sBrandedTruncPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/branded_term.conf'

                lstGoogleads = aAcctInfo[sSvAcctId]['adw_cid']
                if( self.__g_sRetrieveMonth != None ):
                    self.__printDebug( '-> replace aw raw data' )
                    self.__deleteCertainMonth(sAcctTitle)
                else:
                    self.__printDebug( '-> register aw raw data' )

                self.__arrangeAwRawDataFile(sSvAcctId, sAcctTitle, lstGoogleads)
                self.__registerDb( sSvAcctId, sAcctTitle)
        """ 

    def __deleteCertainMonth(self, sAcctTitle):
        nYr = int(self.__g_sRetrieveMonth[:4])
        nMo = int(self.__g_sRetrieveMonth[4:None])
        try:
            lstMonthRange = calendar.monthrange(nYr, nMo)
        except calendar.IllegalMonthError:
            self.__printDebug( 'invalid yyyymm' )
            raise Exception('remove' )
            return
        
        sStartDateRetrieval = self.__g_sRetrieveMonth[:4] + '-' + self.__g_sRetrieveMonth[4:None] + '-01'
        sEndDateRetrieval = self.__g_sRetrieveMonth[:4] + '-' + self.__g_sRetrieveMonth[4:None] + '-' + str(lstMonthRange[1])
        with sv_mysql.SvMySql('job_plugins.aw_register_db') as oSvMysql:
            oSvMysql.setTablePrefix(sAcctTitle)
            lstRst = oSvMysql.executeQuery('deleteCompiledLogByPeriod', sStartDateRetrieval, sEndDateRetrieval)

    def __getCampaignNameAlias(self, sParentDataPath):
        dictCampaignNameAliasInfo = {}
        try:
            with codecs.open(sParentDataPath+'alias_info_campaign.tsv', 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if( nRowCnt > 0 ):
                        dictCampaignNameAliasInfo[row[0]] = {'source':row[1], 'rst_type':row[2], 'medium':row[3], 'camp1st':row[4], 'camp2nd':row[5], 'camp3rd':row[6] }

                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            pass
        
        return dictCampaignNameAliasInfo

    def __arrangeAwRawDataFile(self, sSvAcctId, sAcctTitle, lstGoogleads):
        lstMergedDataFiles = []
        sParentDataPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/adwords/'
        # retrieve campaign name alias info
        dictCampaignNameAlias = self.__getCampaignNameAlias( sParentDataPath )

        for sGoogleadsCid in lstGoogleads:
            sDataPath = sParentDataPath + sGoogleadsCid
            if( self.__g_sRetrieveMonth == None ):
                self.__printDebug( '-> '+ sGoogleadsCid +' is registering AW data files' )
            else:
                self.__printDebug( '-> '+ sGoogleadsCid +' is replacing AW data files' )
                sDataPath = sParentDataPath + sGoogleadsCid + '/closing'
            
            # traverse directory and categorize data files
            lstDataFiles = os.listdir(sDataPath)
            for nIdx, sDatafileName in enumerate(lstDataFiles):
                aFileExt = os.path.splitext(sDatafileName)
                if( aFileExt[0] == 'agency_info' ):
                    continue
                if( aFileExt[1] == '' or aFileExt[1] == '.latest' or aFileExt[1] == '.earliest' ): # pass if extension is .earliest or .latest or directory
                    continue
                
                if( self.__g_sRetrieveMonth == None ):
                    sMode = 'r' # means regular daily
                else:
                    sMode = 'c' # means closing monthly
                lstMergedDataFiles.append( sDatafileName + '|@|' + sGoogleadsCid + '|@|' + sMode )
        
        lstMergedDataFiles.sort()

        nIdx = 0
        nSentinel = len(lstMergedDataFiles)
        for sDataFileInfo in lstMergedDataFiles:
            aDataFileInfo = sDataFileInfo.split('|@|')
            sFilename = aDataFileInfo[0]
            sCid = aDataFileInfo[1]
            sMode = aDataFileInfo[2]
            if sMode == 'r':
                sSourceDataPath = os.path.join(sParentDataPath, sCid) # to archive data file
                sDataFileFullname = os.path.join(sParentDataPath, sCid, sFilename)
            elif sMode == 'c':
                sSourceDataPath = os.path.join(sParentDataPath, sCid, 'closing') # to archive data file
                sDataFileFullname = os.path.join(sParentDataPath, sCid, 'closing', sFilename)
            
            try:
                with open(sDataFileFullname, 'r') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    for row in reader:
                        bBrd = 0

                        lstCampaignCode = row[0].split('_')
                        # ignore TSV file header and tail
                        if( lstCampaignCode[0] == 'CRITERIA' ): # for old adwords API report; ignore report title row: CRITERIA_PERFORMANCE_REPORT (Nov 14, 2015)
                            continue
                        if( lstCampaignCode[0] == 'google' ): # for new google ads API report; ignore report title row: google_ads_api (v6)
                            continue
                        elif( lstCampaignCode[0] == 'Campaign'): # ignore column title row
                            continue
                        elif( lstCampaignCode[0] == 'Total'): # for old adwords API report; ignore gross sum row
                            continue
                        
                        # process body
                        if( len( lstCampaignCode ) > 3 ): # adwords campaign name follows singleview campaign code
                            if( lstCampaignCode[0] == 'OLD' ): # OLD campaign
                                lstCampaignCode.pop(0)
                                nLastIdx = len(lstCampaignCode) - 1
                                sTempCampaign3rd = lstCampaignCode[nLastIdx]
                                lstCampaignCode[nLastIdx] = sTempCampaign3rd + '_OLD'
                        else: # adwords group name follows singleview campaign code
                            print(lstCampaignCode )
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
                                self.__printDebug( '  ' + sCampaignName + '  ' + sDataFileFullname )
                                self.__printDebug( 'weird googleads log!' )
                                return
                        
                        sUa = self.__g_oSvCampaignParser.getUa( row[6] )
                        sTerm = row[2]
                        sPlacement = None
                        # diff between "adwords term" and "adwords placement"
                        if self.__g_oSvCampaignParser.decideAwPlacementTagByTerm( sTerm ) == True:
                            sPlacement = sTerm
                            sTerm = None

                        # finally determine branded by term
                        if self.__g_oSvCampaignParser.decideBrandedByTerm( self.__g_sBrandedTruncPath, sTerm ) == True:
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
                self.__printDebug( 'pass ' + sDataFileFullname + ' does not exist')
            
            self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
            nIdx += 1

    def __registerDb(self, sSvAcctId, sAcctTitle):
        nIdx = 0
        nSentinel = len(self.__g_dictAdwRaw)
        with sv_mysql.SvMySql('job_plugins.aw_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sReportId in self.__g_dictAdwRaw:
                aReportType = sReportId.split('|@|')
                sAdwordsCid = aReportType[0]
                sDataDate = datetime.strptime( aReportType[1], "%Y-%m-%d" )
                sUaType = aReportType[2]
                sSource = aReportType[3].strip()
                sRstType = aReportType[4].strip()
                sMedium = self.__g_oSvCampaignParser.getSvMediumTag( aReportType[5] ).strip()
                bBrd = aReportType[6]
                sCampaign1st = aReportType[7].strip()
                sCampaign2nd = aReportType[8].strip()
                sCampaign3rd = aReportType[9].strip()
                sTerm = aReportType[10].strip()
                sPlacement = aReportType[11].strip()
                    
                # should check if there is duplicated date + SM log
                # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                lstFetchRst = oSvMysql.executeQuery('insertAwCompiledDailyLog', sAdwordsCid, sUaType, sSource, sRstType, sMedium, bBrd, sCampaign1st, sCampaign2nd, sCampaign3rd,
                    sTerm, sPlacement,	str(self.__g_dictAdwRaw[sReportId]['cost']), self.__g_dictAdwRaw[sReportId]['imp'], str(self.__g_dictAdwRaw[sReportId]['clk']),
                    str(self.__g_dictAdwRaw[sReportId]['conv_cnt']), str(self.__g_dictAdwRaw[sReportId]['conv_amnt']), sDataDate )

                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __archiveGaDataFile(self, sDataPath, sCurrentFileName):
        sSourcePath = sDataPath
        if not os.path.exists(sSourcePath):
            self.__printDebug( 'error: adw source directory does not exist!' )
            return
        
        if( self.__g_sRetrieveMonth == None ):
            sArchiveDataPath = sDataPath +'/archive'
        else:
            sArchiveDataPath = sDataPath +'/../archive'

        if not os.path.exists(sArchiveDataPath):
            os.makedirs(sArchiveDataPath)

        sSourceFilePath = os.path.join(sDataPath, sCurrentFileName)
        sArchiveDataFilePath = os.path.join(sArchiveDataPath, sCurrentFileName)		
        shutil.move(sSourceFilePath, sArchiveDataFilePath)

    def __printProgressBar(self, iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '='):
        """
        Print iterations progress
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
        """
        if __name__ == '__main__': # for console debugging
            percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
            filledLength = int(length * iteration // total)
            bar = fill * filledLength + '-' * (length - filledLength)
            print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
            # Print New Line on Complete
            if iteration == total: 
                print()

    '''
    #def __parseAwDataFile(self, sSvAcctId, sAcctTitle, sAdwordsCid ):
    def __parseAwDataFile(self, sSvAcctId, sAcctTitle, lstGoogleads ):
        #if( self.__g_sRetrieveMonth == None ):
        #	self.__printDebug( '-> '+ sAdwordsCid +' is registering AW data files' )
        #	sDataPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/adwords/' + sAdwordsCid
        #else:
        #	self.__printDebug( '-> '+ sAdwordsCid +' is replacing AW data files' )
        #	sDataPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/adwords/' + sAdwordsCid + '/closing'
        
        #self.__arrangeAwRawDataFile(sDataPath)
        self.__arrangeAwRawDataFile(sSvAcctId, sAcctTitle, lstGoogleads)
        #self.__registerDb( sSvAcctId, sAcctTitle, sAdwordsCid)
        self.__registerDb( sSvAcctId, sAcctTitle)
    def __getBrandedTrunc(self, sSvAcctId, sAcctTitle):
        sBrandedTermsPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/branded_term.conf'		
        lstBrandedTrunc = []
        try:
            with open(sBrandedTermsPath, 'r') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                for term in reader:
                    lstBrandedTrunc.append(term[0])
        except FileNotFoundError:
            pass
        return lstBrandedTrunc
    '''

if __name__ == '__main__': # for console debugging
    dictPluginParams = {'config_loc':None, 'yyyymm':None} # {'config_loc':'1/test_acct', 'yyyymm':'201811'}
    nCliParams = len(sys.argv)
    if( nCliParams > 1 ):
        for i in range(nCliParams):
            if i is 0:
                continue

            sArg = sys.argv[i]
            for sParamName in dictPluginParams:
                nIdx = sArg.find( sParamName + '=' )
                if( nIdx > -1 ):
                    aModeParam = sArg.split('=')
                    dictPluginParams[sParamName] = aModeParam[1]
                
        #print( dictPluginParams )
        with svJobPlugin(dictPluginParams) as oJob: # to enforce to call plugin destructor
            oJob.procTask()
    else:
        print( 'warning! [config_loc] params are required for console execution.' )