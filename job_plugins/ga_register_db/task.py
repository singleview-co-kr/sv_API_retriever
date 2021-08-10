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
    __g_sVersion = '1.0.0'
    __g_sLastModifiedDate = '4th, Jul 2021'
    __g_sBrandedTruncPath = None
    __g_sConfigLoc = None
    __g_lstErrornousMedia = []
    __g_dictGaRaw = {}
    __g_dictSourceMediaNameAliasInfo = {}
    __g_dictGoogleAdsCampaignNameAlias = {}
    __g_dictNaverPowerlinkCampaignNameAlias = {}
    __g_oLogger = None	
    __g_oSvCampaignParser = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_sConfigLoc = dictParams['config_loc']

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
        s_ga_view_id = dict_acct_info[s_sv_acct_id]['ga_view_id']
        
        self.__g_sBrandedTruncPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + s_sv_acct_id +'/' + s_acct_title + '/branded_term.conf'
        with sv_mysql.SvMySql('job_plugins.ga_register_db') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(s_acct_title)
            o_sv_mysql.initialize()

        self.__printDebug( '-> register ga raw data' )
        self.__parseGaDataFile( s_sv_acct_id, s_acct_title, s_ga_view_id )

        """
        aAcctInfo = oResp['variables']['acct_info']
        if aAcctInfo is not None:
            self.__g_oSvCampaignParser = sv_campaign_parser.svCampaignParser()
            
            for sSvAcctId in aAcctInfo:
                sAcctTitle = aAcctInfo[sSvAcctId]['account_title']
                sGaViewId = aAcctInfo[sSvAcctId]['ga_view_id']
                self.__g_sBrandedTruncPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/branded_term.conf'
                with sv_mysql.SvMySql('job_plugins.ga_register_db') as oSvMysql:
                    oSvMysql.setTablePrefix(sAcctTitle)
                    oSvMysql.initialize()

                self.__printDebug( '-> register ga raw data' )
                self.__parseGaDataFile( sSvAcctId, sAcctTitle, sGaViewId )
        """

    def __parseGaDataFile(self, sSvAcctId, sAcctTitle, sGaViewId ):
        self.__printDebug( '-> '+ sGaViewId +' is registering GA data files' )
        # sDataPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/google_analytics/' + sGaViewId
        # self.__getSourceMediaNameAlias(sDataPath)
        sDataPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/google_analytics/' + sGaViewId + '/data'
        sConfPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/google_analytics/' + sGaViewId + '/conf'
        self.__getSourceMediaNameAlias(sConfPath)
        
        # retrieve google ads campaign name alias info
        sGoogeAdsDataPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/adwords/'
        self.__g_dictGoogleAdsCampaignNameAlias = self.__getCampaignNameAlias( sGoogeAdsDataPath )

        # retrieve naver powerlink campaign name alias info
        sNaverPowerlinkDataPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/naver_ad/'
        self.__g_dictNaverPowerlinkCampaignNameAlias = self.__getCampaignNameAlias( sNaverPowerlinkDataPath )
        
        self.__arrangeGaRawDataFile(sDataPath)
        
        # stop if errornous source medium list not empty
        if len( self.__g_lstErrornousMedia ) > 0:
            self.__g_lstErrornousMedia = sorted(set(self.__g_lstErrornousMedia))
            self.__printDebug( 'errornous media names has been detected!' )
            for sMedia in self.__g_lstErrornousMedia:
                self.__printDebug( sMedia )
        
        self.__registerSourceMediumTerm( sSvAcctId, sAcctTitle)

    def __getSourceMediaNameAlias(self, sParentDataPath):
        try:
            with codecs.open(sParentDataPath+'/alias_info_source_media.tsv', 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                nPnsInfoIdx = 0
                for row in reader:
                    if( nRowCnt > 0 ):
                        self.__g_dictSourceMediaNameAliasInfo[row[0]] = {'alias':row[1]}

                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            pass
        return

    def __parseGaRow(self, lstRow,sDataFileFullname):
        try:
            sSourceMediumAlias = str(self.__g_dictSourceMediaNameAliasInfo[lstRow[0]]['alias'])
            aSourceMedium = sSourceMediumAlias.split(' / ')
        except KeyError:
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
            if len( sSource ) > 30: # remedy erronous UTM parameter
                m = re.search(r"https?://(\w*:\w*@)?[-\w.]+(:\d+)?(/([\w/_.]*(\?\S+)?)?)?", sSource)
                try:
                    if( len( m.group(0) ) ):
                        nPos = sSource.find('utm_source')
                        if( nPos > -1 ):
                            sRightPart = sSource[nPos:]
                            aRightPart = sRightPart.split('=')
                            sSource = aRightPart[1]
                        else:
                            m1 = re.search(r"[^https?://](\w*:\w*@)?[-\w.]+(:\d+)?", sSource)
                            sSource = m1.group(0)
                except AttributeError: # retry to handle very weird source tagging
                    # this block handles '＆' which is not & that naver shopping foolishly occurs
                    self.__printDebug( 'weird source found on ' + sDataFileFullname + ' -> unicode ampersand which is not &' )
                    
                    # same source code needs to be method - begin
                    sEncodedSource = sSource.encode('UTF-8')
                    sEncodedSource = str( sEncodedSource )
                    aWeirdSource = sEncodedSource.split("\\xef\\xbc\\x86") # utf-8 encoded unicode ampersand ��
                    
                    for sQueryElement in aWeirdSource:
                        aQuery = sQueryElement.split('=')
                        if( aQuery[0] == 'utm_campaign' ):
                            dictSmRst = self.__g_oSvCampaignParser.parseCampaignCode(sSvCampaignCode=aQuery[1])
                            sSource = dictSmRst['source']
                            sMedium = dictSmRst['medium']
                            sCampaignCode = aQuery[1]
                            sRstType = dictSmRst['rst_type']
                            sCampaign1st = dictSmRst['campaign1st']
                            sCampaign2nd = dictSmRst['campaign2nd']
                            sCampaign3rd = dictSmRst['campaign3rd']

                        if( aQuery[0] == 'utm_term' ):
                            sTerm = aQuery[1]
                    # same source code needs to be method - end
                except Exception as inst:
                    self.__printDebug( sDataFileFullname )
                    self.__printDebug( lstRow )
                    #self.__printDebug(type(inst))    # the exception instance
                    #self.__printDebug(inst.args)     # arguments stored in .args
                    self.__printDebug(inst)			# __str__ allows args to be printed directly, but may be overridden in exception subclasses
        else: # ex) naver��utm_medium=cpc��utm_campaign=NV_PS_CPC_NVSHOP_00_00��utm_term=NVSHOP_4741��n_media=33421��n_query=��ɼ��漮��n_rank=1��n_ad_group=grp-a001-02-000000002830061��n_ad=nad-a001-02-000000011190197��n_campaign_type=2��n_
            # same source code needs to be method - begin
            sEncodedSource = sSource.encode('UTF-8')
            sEncodedSource = str( sEncodedSource )
            nUnicodeAmpersandPos = sEncodedSource.find("\\xef\\xbc\\x86")
            if nUnicodeAmpersandPos > -1:
                aWeirdSource = sEncodedSource.split("\\xef\\xbc\\x86") # utf-8 encoded unicode ampersand ��
                for sQueryElement in aWeirdSource:
                    aQuery = sQueryElement.split('=')
                    if( aQuery[0] == 'utm_campaign' ):
                        dictSmRst = self.__g_oSvCampaignParser.parseCampaignCode(sSvCampaignCode=aQuery[1])
                        sSource = dictSmRst['source']
                        sMedium = dictSmRst['medium']
                        sCampaignCode = aQuery[1]
                        sRstType = dictSmRst['rst_type']
                        sCampaign1st = dictSmRst['campaign1st']
                        sCampaign2nd = dictSmRst['campaign2nd']
                        sCampaign3rd = dictSmRst['campaign3rd']

                    if( aQuery[0] == 'utm_term' ):
                            sTerm = aQuery[1]
            # same source code needs to be method - end
        dictValidMedium = self.__g_oSvCampaignParser.validateGaMediumTag( sMedium )
        
        if dictValidMedium['medium'] != 'weird':
            sMedium = dictValidMedium['medium']
            if dictValidMedium['found_pos'] > -1:
                #nPos = nPos + len(sKnownMediaCode)
                nPos = dictValidMedium['found_pos'] + len( dictValidMedium['medium'])
                sRightPart = sMedium[nPos:]
                aRightPart = sRightPart.split('=')
                if( aRightPart[0] == 'utm_campaign' ):
                    sCampaignCode = aRightPart[1]
        else:
            self.__g_lstErrornousMedia.append( sDataFileFullname + ' -> ' + sSource+' / ' + sMedium )

        if sSource == 'google' and sMedium == 'cpc':
            try:
                self.__g_dictGoogleAdsCampaignNameAlias[ sCampaignCode ]
                if self.__g_dictGoogleAdsCampaignNameAlias[ sCampaignCode ]['source'] == 'YT':
                    sSource = 'youtube'
                if self.__g_dictGoogleAdsCampaignNameAlias[ sCampaignCode ]['medium'] == 'DISP':
                    sMedium = 'display'

                sCampaignCode = self.__g_dictGoogleAdsCampaignNameAlias[ sCampaignCode ]['source'] + '_' + \
                                self.__g_dictGoogleAdsCampaignNameAlias[ sCampaignCode ]['rst_type'] + '_' + \
                                self.__g_dictGoogleAdsCampaignNameAlias[ sCampaignCode ]['medium'] + '_' + \
                                self.__g_dictGoogleAdsCampaignNameAlias[ sCampaignCode ]['camp1st'] + '_' + \
                                self.__g_dictGoogleAdsCampaignNameAlias[ sCampaignCode ]['camp2nd'] + '_' + \
                                self.__g_dictGoogleAdsCampaignNameAlias[ sCampaignCode ]['camp3rd']
            except KeyError: # if sv standard campaign code
                pass
        elif sSource == 'naver' and sMedium == 'cpc':
            try:
                self.__g_dictNaverPowerlinkCampaignNameAlias[ sCampaignCode ]
                sCampaignCode = self.__g_dictNaverPowerlinkCampaignNameAlias[ sCampaignCode ]['source'] + '_' + \
                                self.__g_dictNaverPowerlinkCampaignNameAlias[ sCampaignCode ]['rst_type'] + '_' + \
                                self.__g_dictNaverPowerlinkCampaignNameAlias[ sCampaignCode ]['medium'] + '_' + \
                                self.__g_dictNaverPowerlinkCampaignNameAlias[ sCampaignCode ]['camp1st'] + '_' + \
                                self.__g_dictNaverPowerlinkCampaignNameAlias[ sCampaignCode ]['camp2nd'] + '_' + \
                                self.__g_dictNaverPowerlinkCampaignNameAlias[ sCampaignCode ]['camp3rd']
            except KeyError: # if sv standard campaign code
                pass
        
        dictCampaignRst = self.__g_oSvCampaignParser.parseCampaignCode(sSvCampaignCode=sCampaignCode)
        if( dictCampaignRst['source'] == 'unknown' ): # handle no sv campaign code data
            if( sMedium == 'cpc' or sMedium == 'display' ):
                dictCampaignRst['rst_type'] = 'PS'
            else:
                dictCampaignRst['rst_type'] = 'NS'

        bBrd = dictCampaignRst['brd']
        sRstType = dictCampaignRst['rst_type']
        if( len( dictCampaignRst['medium'] ) > 0 ): # sv campaign criteria first, GA auto categorize later
            sMedium = dictCampaignRst['medium']
        sCampaign1st = dictCampaignRst['campaign1st']
        sCampaign2nd = dictCampaignRst['campaign2nd']
        sCampaign3rd = dictCampaignRst['campaign3rd']

        # finally determine branded by term
        if self.__g_oSvCampaignParser.decideBrandedByTerm( self.__g_sBrandedTruncPath, sTerm ) == True:
            bBrd = 1
        
        # monitor weird source name - begin
        if len(sSource) > 50: 
            self.__printDebug( sDataFileFullname )
            self.__printDebug( lstRow )
            raise Exception('stop')
        # monitor weird source name - begin
        return {'source':sSource,'rst_type':sRstType,'medium':sMedium,'brd':bBrd,'campaign1st':sCampaign1st,'campaign2nd':sCampaign2nd,'campaign3rd':sCampaign3rd}

    def __arrangeGaRawDataFile(self, sDataPath):
        # traverse directory and categorize data files
        lstDataFiles = os.listdir(sDataPath)
        lstDataFiles.sort()
        
        nIdx = 0
        nSentinel = len(lstDataFiles)
        dictQuery = { 'avgSessionDuration.tsv':'dur_sec', 'bounceRate.tsv':'b_per', 'pageviewsPerSession.tsv':'pvs', 'percentNewSessions.tsv':'new_per', 'sessions.tsv':'sess', 'transactionRevenue.tsv':'rev', 'transactions.tsv':'trs' }

        for sFilename in lstDataFiles:
            sDataFileFullname = os.path.join(sDataPath, sFilename)
            if sFilename == 'archive':
                continue
            
            aFile = sFilename.split('_')
            sDataDate = aFile[0]
            sUaType = self.__g_oSvCampaignParser.getUa( aFile[1] )
            sSpecifier = aFile[2]

            try: # proc media related data file only
                sIdxName = dictQuery[sSpecifier]

                try:
                    with open(sDataFileFullname, 'r') as tsvfile:
                        reader = csv.reader(tsvfile, delimiter='\t', skipinitialspace=True)
                        for row in reader:
                            dictRst = self.__parseGaRow(row,sDataFileFullname)
                            sTerm = row[2]
                            sReportId = sDataDate+'|@|'+sUaType+'|@|'+dictRst['source']+'|@|'+dictRst['rst_type']+'|@|'+ \
                                dictRst['medium']+'|@|'+str(dictRst['brd'])+'|@|'+dictRst['campaign1st']+'|@|'+dictRst['campaign2nd']+'|@|'+\
                                dictRst['campaign3rd']+'|@|'+sTerm
                            
                            try: # if designated log already created
                                self.__g_dictGaRaw[sReportId]
                            except KeyError: # if new log requested
                                self.__g_dictGaRaw[sReportId] = {
                                    'sess':0,'new_per':0,'b_per':0,'dur_sec':0,'pvs':0,'trs':0, 'rev':0, 'ua':sUaType
                                }
                            self.__g_dictGaRaw[sReportId][sIdxName] = float(row[3])
                            
                    self.__archiveGaDataFile(sDataPath, sFilename)
                except FileNotFoundError:
                    self.__printDebug( 'pass ' + sDataFileFullname + ' does not exist')

                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
                nIdx += 1
            except KeyError:
                self.__printDebug( sSpecifier +' is not relevant' )
                continue

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

    def __registerSourceMediumTerm(self, sSvAcctId, sAcctTitle):
        nIdx = 0
        nSentinel = len(self.__g_dictGaRaw)
        with sv_mysql.SvMySql('job_plugins.ga_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sReportId in self.__g_dictGaRaw:
                aReportType = sReportId.split('|@|')
                sDataDate = datetime.strptime( aReportType[0], "%Y%m%d" )
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
                lstFetchRst = oSvMysql.executeQuery('insertGaCompiledDailyLog', sUaType, sSource, sRstType, sMedium, bBrd, sCampaign1st, sCampaign2nd, sCampaign3rd, sTerm, 
                    self.__g_dictGaRaw[sReportId]['sess'], str(self.__g_dictGaRaw[sReportId]['new_per']), str(self.__g_dictGaRaw[sReportId]['b_per']), 
                    str(self.__g_dictGaRaw[sReportId]['dur_sec']), str(self.__g_dictGaRaw[sReportId]['pvs']), self.__g_dictGaRaw[sReportId]['trs'], self.__g_dictGaRaw[sReportId]['rev'], 0, 
                    sDataDate )

                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __archiveGaDataFile(self, sDataPath, sCurrentFileName):
        #self.__printDebug( '-> archives registered data files' )
        sSourcePath = sDataPath

        if not os.path.exists(sSourcePath):
            self.__printDebug( 'error: google analytics source directory does not exist!' )
            return
        
        sArchiveDataPath = sDataPath +'/archive'

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

if __name__ == '__main__': # for console debugging
    dictPluginParams = {'config_loc':None} # {'config_loc':'1/test_acct'}
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