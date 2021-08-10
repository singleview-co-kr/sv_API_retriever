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
    __g_sVersion = '0.0.5'
    __g_sLastModifiedDate = '4th, Jul 2021'
    __g_sBrandedTruncPath = None
    __g_sConfigLoc = None
    __g_lstBrandedTrunc = None
    __g_oLogger = None
    __g_oSvCampaignParser = None
    __g_dictKkoRaw = dict()

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
        # 	sTargetUrl = self.__g_sConfigLoc + '?mode=check_status'
        #	oResp = self.__getHttpResponse( sTargetUrl )
        # else:
        #    oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        #    oResp = oSvApiConfigParser.getConfig()
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        oResp = oSvApiConfigParser.getConfig()
        
        aAcctInfo = oResp['variables']['acct_info']
        if aAcctInfo is not None:
            self.__g_oSvCampaignParser = sv_campaign_parser.svCampaignParser()

            for sSvAcctId in aAcctInfo:
                sAcctTitle = aAcctInfo[sSvAcctId]['account_title']
                with sv_mysql.SvMySql('job_plugins.kko_register_db') as oSvMysql:
                    oSvMysql.setTablePrefix(sAcctTitle)
                    oSvMysql.initialize()
                
                sKakaoAcctId = aAcctInfo[sSvAcctId]['kko_moment_aid']
                self.__g_sBrandedTruncPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/branded_term.conf'
                self.__printDebug( '-> register kko raw data' )

                self.__arrangeKkoRawDataFile(sSvAcctId, sAcctTitle, sKakaoAcctId)
                self.__registerDb( sSvAcctId, sAcctTitle)

    def __getCampaignNameAlias(self, sParentDataPath):
        dictCampaignNameAliasInfo = {}
        try:
            with codecs.open(sParentDataPath+'alias_info_campaign.tsv', 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                for row in reader:
                    if( nRowCnt > 0 ):
                        sCampaignUniqueKey = row[0] + '|@|' + row[1] + '|@|' + row[2]
                        dictCampaignNameAliasInfo[sCampaignUniqueKey] = {'source':row[3], 'rst_type':row[4], 'medium':row[5], 'camp1st':row[6], 'camp2nd':row[7], 'camp3rd':row[8] }

                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            pass
        
        return dictCampaignNameAliasInfo

    def __arrangeKkoRawDataFile(self, sSvAcctId, sAcctTitle, sKakaoAcctId):
        lstMergedDataFiles = []
        sParentDataPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/kakao/'
        # retrieve campaign name alias info
        dictCampaignNameAlias = self.__getCampaignNameAlias( sParentDataPath )
        sDataPath = sParentDataPath + sKakaoAcctId
        # traverse directory and categorize data files
        lstDataFiles = os.listdir(sDataPath)
        for nIdx, sDatafileName in enumerate(lstDataFiles):
            aFileExt = os.path.splitext(sDatafileName)
            if aFileExt[0] == 'agency_info' or aFileExt[0] == 'archive':
                continue
            
            sMode = 'r' # means regular daily
            lstMergedDataFiles.append( sDatafileName + '|@|' + sKakaoAcctId + '|@|' + sMode )

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
            
            if int(sFilename.split('.')[0]) < 20200420:
                self.__procKkoRawDataFileV1(sCid, sSourceDataPath, sDataFileFullname, sFilename)
            else:
                self.__procKkoRawDataFileV2(sCid, sSourceDataPath, sDataFileFullname, sFilename)
            
            self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data file:', suffix = 'Complete', length = 50)
            nIdx += 1

    def __procKkoRawDataFileV2(self, sCid, sSourceDataPath, sDataFileFullname, sFilename):
        self.__printDebug( 'v2 format')
        # 유형x목표	row[0]
        # 캠페인 row[1]
        # 광고그룹 row[2]
        # 소재 row[3]
        # 시작일 row[4]
        # 종료일 row[5]
        # 디바이스 row[6]
        # 비용 row[7]
        # 노출수 row[8]
        # 클릭수 row[9]
        # 클릭률 row[10]
        # 도달수 row[11]
        # 노출당 비용 row[12]
        # 클릭당 비용 row[13]
        # 도달당 비용 row[14]
        # 가입완료(직접) row[15]
        # 장바구니 보기(직접) row[16]
        # 구매(직접) row[17]
        # 구매금액(직접) row[18]
        # 가입완료(간접) row[19]
        # 장바구니 보기(간접) row[20]
        # 구매(간접) row[21]
        # 구매금액(간접) row[22]
        try:
            with open(sDataFileFullname, 'r') as tsvfile:
                nRowCnt = 0
                reader = csv.reader(tsvfile, delimiter='\t')
                for row in reader:
                    if nRowCnt == 0: # ignore TSV file header
                        nRowCnt = nRowCnt + 1
                        continue
                        
                    nRowCnt = nRowCnt + 1
                    
                    if int(row[8]) == 0: # if impression is zero
                        continue
                    
                    bBrd = 0
                    sSource = None
                    sRstType = None
                    sMedium = None
                    sCampaign1st = None
                    sCampaign2nd = None
                    sCampaign3rd = None
                    lstCampaignCode = row[3].split('_')

                    # process body
                    if len( lstCampaignCode ) == 6: # adwords campaign name follows singleview campaign code
                        if lstCampaignCode[0] == 'KKO':
                            sSource = lstCampaignCode[0]
                            sRstType = lstCampaignCode[1]
                            sMedium = lstCampaignCode[2]
                            sCampaign1st = lstCampaignCode[3]
                            sCampaign2nd = lstCampaignCode[4]
                            sCampaign3rd = lstCampaignCode[5]
                    
                    # if non singleview standard ad name
                    if sSource == None or sRstType == None or sMedium == None or sCampaign1st == None or sCampaign2nd == None or sCampaign3rd == None:
                        try:
                            sCampaignName = row[1] + '|@|' + row[2] + '|@|' + row[3]
                            sSource = dictCampaignNameAlias[ sCampaignName ]['source']
                            sRstType = dictCampaignNameAlias[ sCampaignName ]['rst_type']
                            sMedium = dictCampaignNameAlias[ sCampaignName ]['medium']
                            sCampaign1st = dictCampaignNameAlias[ sCampaignName ]['camp1st']
                            sCampaign2nd = dictCampaignNameAlias[ sCampaignName ]['camp2nd']
                            sCampaign3rd = dictCampaignNameAlias[ sCampaignName ]['camp3rd']
                        except KeyError: # if unacceptable googleads campaign name
                            self.__printDebug( '  ' + sCampaignName + '  ' + sDataFileFullname )
                            self.__printDebug( 'weird kakao moment log!' )
                            return
                    
                    sUa = self.__g_oSvCampaignParser.getUa( row[6] )
    #						sTerm = row[2]
    #						# finally determine branded by term
    #						if self.__g_oSvCampaignParser.decideBrandedByTerm( self.__g_sBrandedTruncPath, sTerm ) == True:
    #							bBrd = 1
    #
                    nImpression = int(row[8])
                    nClick = int(row[9])
                    nCost = int(row[7])
                    nConvCntDirect = int(float(row[17]))
                    nConvAmntDirect = int(float(row[18]))
                    nConvCntIndirect = int(float(row[21]))
                    nConvAmntIndirect = int(float(row[22]))

                    sDatadate = row[4]

                    sReportId = sCid+'|@|'+sDatadate+'|@|'+sUa+'|@|'+sSource+'|@|'+sRstType+'|@|'+ sMedium+'|@|'+ str(bBrd)+'|@|'+\
                        sCampaign1st+'|@|'+	sCampaign2nd+'|@|'+	sCampaign3rd # +'|@|'+ str(sTerm)
    #
                    try: # if designated log already created
                        self.__g_dictKkoRaw[sReportId]
                        self.__g_dictKkoRaw[sReportId]['imp'] += nImpression
                        self.__g_dictKkoRaw[sReportId]['clk'] += nClick
                        self.__g_dictKkoRaw[sReportId]['cost_inc_vat'] += nCost
                        self.__g_dictKkoRaw[sReportId]['conv_cnt_direct'] += nConvCntDirect
                        self.__g_dictKkoRaw[sReportId]['conv_amnt_direct'] += nConvAmntDirect
                        self.__g_dictKkoRaw[sReportId]['conv_cnt_indirect'] += nConvCntIndirect
                        self.__g_dictKkoRaw[sReportId]['conv_amnt_indirect'] += nConvAmntIndirect
                    except KeyError: # if new log requested
                        self.__g_dictKkoRaw[sReportId] = {
                            'imp':nImpression,'clk':nClick,'cost_inc_vat':nCost,'conv_cnt_direct':nConvCntDirect,'conv_amnt_direct':nConvAmntDirect,'conv_cnt_indirect':nConvCntIndirect,'conv_amnt_indirect':nConvAmntIndirect
                        }
            self.__archiveGaDataFile(sSourceDataPath, sFilename)
        except FileNotFoundError:
            self.__printDebug( 'pass ' + sDataFileFullname + ' does not exist')

    def __procKkoRawDataFileV1(self, sCid, sSourceDataPath, sDataFileFullname, sFilename):
        self.__printDebug( 'v1 format')
        
        # 디스플레이 캠페인 row[0]
        # 광고그룹 row[1]
        # 소재 row[2]
        # 시작일 row[3]
        # 종료일 row[4]
        # 디바이스 row[5]
        # 노출수 row[6]
        # 클릭수 row[7]
        # 클릭률 row[8]
        # 비용 row[9]
        # 가입완료(직접) row[10]
        # 장바구니 보기(직접) row[11]
        # 구매(직접) row[12]
        # 구매금액(직접) row[13]
        # 가입완료(간접) row[14]
        # 장바구니 보기(간접) row[15]
        # 구매(간접) row[16]
        # 구매금액(간접) row[17]

        try:
            with open(sDataFileFullname, 'r') as tsvfile:
                nRowCnt = 0
                reader = csv.reader(tsvfile, delimiter='\t')
                for row in reader:
                    if nRowCnt == 0: # ignore TSV file header
                        nRowCnt = nRowCnt + 1
                        continue
                        
                    nRowCnt = nRowCnt + 1
                    
                    if int(row[6]) == 0: # if impression is zero
                        continue
                    
                    bBrd = 0
                    sSource = None
                    sRstType = None
                    sMedium = None
                    sCampaign1st = None
                    sCampaign2nd = None
                    sCampaign3rd = None
                    lstCampaignCode = row[2].split('_')

                    # process body
                    if len( lstCampaignCode ) == 6: # adwords campaign name follows singleview campaign code
                        if lstCampaignCode[0] == 'KKO':
                            sSource = lstCampaignCode[0]
                            sRstType = lstCampaignCode[1]
                            sMedium = lstCampaignCode[2]
                            sCampaign1st = lstCampaignCode[3]
                            sCampaign2nd = lstCampaignCode[4]
                            sCampaign3rd = lstCampaignCode[5]
                    
                    # if non singleview standard ad name
                    if sSource == None or sRstType == None or sMedium == None or sCampaign1st == None or sCampaign2nd == None or sCampaign3rd == None:
                        try:
                            sCampaignName = row[0] + '|@|' + row[1] + '|@|' + row[2]
                            sSource = dictCampaignNameAlias[ sCampaignName ]['source']
                            sRstType = dictCampaignNameAlias[ sCampaignName ]['rst_type']
                            sMedium = dictCampaignNameAlias[ sCampaignName ]['medium']
                            sCampaign1st = dictCampaignNameAlias[ sCampaignName ]['camp1st']
                            sCampaign2nd = dictCampaignNameAlias[ sCampaignName ]['camp2nd']
                            sCampaign3rd = dictCampaignNameAlias[ sCampaignName ]['camp3rd']
                        except KeyError: # if unacceptable googleads campaign name
                            self.__printDebug( '  ' + sCampaignName + '  ' + sDataFileFullname )
                            self.__printDebug( 'weird kakao moment log!' )
                            return
                    
                    sUa = self.__g_oSvCampaignParser.getUa( row[5] )
    #						sTerm = row[2]
    #						# finally determine branded by term
    #						if self.__g_oSvCampaignParser.decideBrandedByTerm( self.__g_sBrandedTruncPath, sTerm ) == True:
    #							bBrd = 1
    #
                    nImpression = int(row[6])
                    nClick = int(row[7])
                    nCost = int(row[9])
                    nConvCntDirect = int(float(row[12]))
                    nConvAmntDirect = int(float(row[13]))
                    nConvCntIndirect = int(float(row[16]))
                    nConvAmntIndirect = int(float(row[17]))
                    sDatadate = row[3]

                    sReportId = sCid+'|@|'+sDatadate+'|@|'+sUa+'|@|'+sSource+'|@|'+sRstType+'|@|'+ sMedium+'|@|'+ str(bBrd)+'|@|'+\
                        sCampaign1st+'|@|'+	sCampaign2nd+'|@|'+	sCampaign3rd # +'|@|'+ str(sTerm)
    #
                    try: # if designated log already created
                        self.__g_dictKkoRaw[sReportId]
                        self.__g_dictKkoRaw[sReportId]['imp'] += nImpression
                        self.__g_dictKkoRaw[sReportId]['clk'] += nClick
                        self.__g_dictKkoRaw[sReportId]['cost_inc_vat'] += nCost
                        self.__g_dictKkoRaw[sReportId]['conv_cnt_direct'] += nConvCntDirect
                        self.__g_dictKkoRaw[sReportId]['conv_amnt_direct'] += nConvAmntDirect
                        self.__g_dictKkoRaw[sReportId]['conv_cnt_indirect'] += nConvCntIndirect
                        self.__g_dictKkoRaw[sReportId]['conv_amnt_indirect'] += nConvAmntIndirect
                    except KeyError: # if new log requested
                        self.__g_dictKkoRaw[sReportId] = {
                            'imp':nImpression,'clk':nClick,'cost_inc_vat':nCost,'conv_cnt_direct':nConvCntDirect,'conv_amnt_direct':nConvAmntDirect,'conv_cnt_indirect':nConvCntIndirect,'conv_amnt_indirect':nConvAmntIndirect
                        }
            self.__archiveGaDataFile(sSourceDataPath, sFilename)
        except FileNotFoundError:
            self.__printDebug( 'pass ' + sDataFileFullname + ' does not exist')

    def __registerDb(self, sSvAcctId, sAcctTitle):
        nIdx = 0
        nSentinel = len(self.__g_dictKkoRaw)
        with sv_mysql.SvMySql('job_plugins.kko_register_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            for sReportId in self.__g_dictKkoRaw:
                aReportType = sReportId.split('|@|')
                sKkoCid = aReportType[0]
                sDataDate = datetime.strptime( aReportType[1], "%Y-%m-%d" )
                sUaType = aReportType[2]
                sSource = aReportType[3]
                sRstType = aReportType[4]
                sMedium = self.__g_oSvCampaignParser.getSvMediumTag( aReportType[5] )
                bBrd = aReportType[6]
                sCampaign1st = aReportType[7]
                sCampaign2nd = aReportType[8]
                sCampaign3rd = aReportType[9]
                sTerm = '' #aReportType[10]
                    
                # should check if there is duplicated date + SM log
                # strict str() formatting prevents that pymysql automatically rounding up tiny decimal
                lstFetchRst = oSvMysql.executeQuery('insertKkoCompiledDailyLog', sKkoCid, sUaType, sSource, sRstType, sMedium, bBrd, sCampaign1st, sCampaign2nd, sCampaign3rd,sTerm,
                    str(self.__g_dictKkoRaw[sReportId]['cost_inc_vat']), self.__g_dictKkoRaw[sReportId]['imp'], str(self.__g_dictKkoRaw[sReportId]['clk']),
                    str(self.__g_dictKkoRaw[sReportId]['conv_cnt_direct']), str(self.__g_dictKkoRaw[sReportId]['conv_amnt_direct']),
                    str(self.__g_dictKkoRaw[sReportId]['conv_cnt_indirect']), str(self.__g_dictKkoRaw[sReportId]['conv_amnt_indirect']), sDataDate )
                self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                nIdx += 1

    def __archiveGaDataFile(self, sDataPath, sCurrentFileName):
        sSourcePath = sDataPath
        if not os.path.exists(sSourcePath):
            self.__printDebug( 'error: adw source directory does not exist!' )
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
                
        with svJobPlugin(dictPluginParams) as oJob: # to enforce to call plugin destructor
            oJob.procTask()
    else:
        print( 'warning! [config_loc] params are required for console execution.' )