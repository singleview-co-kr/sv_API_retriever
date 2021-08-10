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
import time
import os 
import re # https://docs.python.org/3/library/re.html

# singleview library
if __name__ == '__main__': # for console debugging
    import sys
    sys.path.append('../../classes')
    import sv_http
    import sv_api_config_parser
    sys.path.append('../../conf')
    import basic_config
else: # for platform running
    from classes import sv_http
    from classes import sv_api_config_parser
    from conf import basic_config

class svJobPlugin():
    __g_sVersion = '1.0.11'
    __g_sLastModifiedDate = '11th, Apr 2019'
    __g_oLogger = None
    __g_oSvHttp = None
    __g_sEncodedApiKey = None
    __g_sEncodedSecretKey = None
    __g_sUrl = None
    __g_sNvrAdManagerLoginId = None
    __g_sConfigLoc = None
    __g_sRetrieveInfoPath = None
    __g_sDownloadPath = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_sConfigLoc = dictParams['config_loc']
        self.__g_sUrl = dictParams['host_url']

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
        if( oResp['error'] == -1 ): # if error occured
            if( oResp['variables'] ): # oResp['variables'] list has items
                try:
                   oResp['variables']['todo']
                except KeyError: # if ['variables']['todo'] is not defined
                    self.__printDebug( '__checkHttpResp error occured but todo is not defined -> continue')
                else: # if ['variables']['todo'] is defined
                    sTodo = oResp['variables']['todo']
                    if( sTodo == 'stop' ):
                        self.__printDebug('HTTP response raised exception!! with message: ' + oResp['message'] + ' - ' + oResp['variables']['message'] )
                        raise Exception(sTodo)
        return oResp

    def procTask(self):
        """oRegEx = re.compile(r"https?:\/\/[\w\-.]+\/\w+\/\w+\w/?$") # host_url pattern ex) /aaa/bbb or /aaa/bbb/
        m = oRegEx.search(self.__g_sConfigLoc) # match() vs search()
        if( m ): # if arg matches desinated host_url
            sTargetUrl = self.__g_sConfigLoc + '?mode=check_status'
            oResp = self.__getHttpResponse( sTargetUrl )
        else:
            oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
            oResp = oSvApiConfigParser.getConfig()"""
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        oResp = oSvApiConfigParser.getConfig()
        
        """
        dictErrornousSpecialChars = {'#':'_SVHASH_', '%':'_SVPERCENT_', '&':'_SVAMPERSAND_', '+':'_SVPLUS_'}
        aAcctInfo = oResp['variables']['acct_info']
        if aAcctInfo is not None:
            for sSvAcctId in aAcctInfo:
                aNvrAdAcct = aAcctInfo[sSvAcctId]['nvr_ad_acct']
                for cid in aNvrAdAcct:
                    if( cid == 'api_key' or cid == 'secret_key' or cid == 'manager_login_id'):
                        continue

                    self.__g_sNvrAdManagerLoginId = aNvrAdAcct['manager_login_id']
                    self.__g_sEncodedApiKey = aNvrAdAcct['api_key']
                    self.__g_sEncodedSecretKey = aNvrAdAcct['secret_key']
                    # replace any # % & + to safe word
                    for sErrornousChar in dictErrornousSpecialChars:
                        self.__g_sEncodedApiKey = self.__g_sEncodedApiKey.replace(sErrornousChar, dictErrornousSpecialChars[sErrornousChar])
                        self.__g_sEncodedSecretKey = self.__g_sEncodedSecretKey.replace(sErrornousChar, dictErrornousSpecialChars[sErrornousChar])

                    self.__printDebug( '-> '+ cid +' delete master reports' )
                    sTargetUrl = self.__g_sUrl + '?mode=clear&cid=' + cid + '&a=' + self.__g_sEncodedApiKey + '&s=' + self.__g_sEncodedSecretKey + '&f=' + self.__g_sConfigLoc
                    oResp = self.__getHttpResponse( sTargetUrl )
                
                    self.__g_sRetrieveInfoPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId + '/' + aAcctInfo[sSvAcctId]['account_title'] + '/naver_ad/' + cid  
                    if( os.path.isdir(self.__g_sRetrieveInfoPath) is False ):
                        os.makedirs(self.__g_sRetrieveInfoPath)
                    
                    self.__g_sDownloadPath = basic_config.ABSOLUTE_PATH_XE + '/files/svnvcrawl/' + sSvAcctId + '/' + aAcctInfo[sSvAcctId]['account_title'] + '/naver_ad/' + cid
                    self.__reirieveNvMasterReport( sSvAcctId, cid, aNvrAdAcct[cid]['nvr_master_report'] )
                    self.__retrieveNvStatReport( sSvAcctId, cid, aNvrAdAcct[cid]['nvr_stat_report'] ) #statdate arg should be defined

        """
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self.__printDebug('invalid config_loc')
            raise Exception('stop')

        s_sv_acct_id = list(dict_acct_info.keys())[0]
        dict_nvr_ad_acct = dict_acct_info[s_sv_acct_id]['nvr_ad_acct']
        self.__g_sNvrAdManagerLoginId = dict_nvr_ad_acct['manager_login_id']
        self.__g_sEncodedApiKey = dict_nvr_ad_acct['api_key']
        self.__g_sEncodedSecretKey = dict_nvr_ad_acct['secret_key']
        del dict_nvr_ad_acct['manager_login_id'], dict_nvr_ad_acct['api_key'], dict_nvr_ad_acct['secret_key']

        # replace any # % & + to safe word
        dictErrornousSpecialChars = {'#':'_SVHASH_', '%':'_SVPERCENT_', '&':'_SVAMPERSAND_', '+':'_SVPLUS_'}
        for sErrornousChar in dictErrornousSpecialChars:
            self.__g_sEncodedApiKey = self.__g_sEncodedApiKey.replace(sErrornousChar, dictErrornousSpecialChars[sErrornousChar])
            self.__g_sEncodedSecretKey = self.__g_sEncodedSecretKey.replace(sErrornousChar, dictErrornousSpecialChars[sErrornousChar])

        s_cid = list(dict_nvr_ad_acct.keys())[0]

        self.__printDebug( '-> '+ s_cid +' delete master reports' )
        sTargetUrl = self.__g_sUrl + '?mode=clear&cid=' + s_cid + '&a=' + self.__g_sEncodedApiKey + '&s=' + self.__g_sEncodedSecretKey + '&f=' + self.__g_sConfigLoc
        oResp = self.__getHttpResponse( sTargetUrl )
        
        self.__g_sRetrieveInfoPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + s_sv_acct_id + '/' + dict_acct_info[s_sv_acct_id]['account_title'] + '/naver_ad/' + s_cid  
        if( os.path.isdir(self.__g_sRetrieveInfoPath) is False ):
            os.makedirs(self.__g_sRetrieveInfoPath)
        
        self.__g_sDownloadPath = basic_config.ABSOLUTE_PATH_XE + '/files/svnvcrawl/' + s_sv_acct_id + '/' + dict_acct_info[s_sv_acct_id]['account_title'] + '/naver_ad/' + s_cid
        self.__reirieveNvMasterReport( s_sv_acct_id, s_cid, dict_nvr_ad_acct[s_cid]['nvr_master_report'] )
        self.__retrieveNvStatReport( s_sv_acct_id, s_cid, dict_nvr_ad_acct[s_cid]['nvr_stat_report'] ) #statdate arg should be defined

    def __reirieveNvMasterReport(self, sSvAcctId, sNvrAdCustomerID, lstReport ):
        """ retrieve NV Ad mster report """
        dictMasterRereportQueue = dict()
        dictMasterRereportExceptionCnt = dict()  # memory to restrict exceptional occurrence count
        for report in lstReport:
            dictMasterRereportQueue.update({report:0})
            dictMasterRereportExceptionCnt.update({report:0})
        
        nQueueLen = len( dictMasterRereportQueue )
        if( nQueueLen ):
            while True:
                try:
                    sTobeHandledTaskName = list(dictMasterRereportQueue.keys())[list(dictMasterRereportQueue.values()).index(0)] # find unhandled report task
                    if( sTobeHandledTaskName ): # if there exists unhandled report task
                        dtFromRetrieval = None
                        try:
                            sLatestFilepath = self.__g_sRetrieveInfoPath+'/'+sTobeHandledTaskName+'.latest'
                            f = open(sLatestFilepath, 'r')
                            sMaxReportDate = f.readline()
                            dtFromRetrieval = datetime.strptime(sMaxReportDate, '%Y%m%d%H%M%S')
                            f.close()
                        except FileNotFoundError:
                            nLatestRetrieveDate = 0
                            for root, dirs, filenames in os.walk(self.__g_sDownloadPath):
                                for f in filenames:
                                    if f.find(sTobeHandledTaskName+'_') != -1:
                                        aNames = f.split('_')
                                        if nLatestRetrieveDate < int(aNames[0] ):
                                            nLatestRetrieveDate = int(aNames[0] )

                            if nLatestRetrieveDate != 0:
                                dtFromRetrieval = datetime.strptime(str(nLatestRetrieveDate), '%Y%m%d%H%M%S') + timedelta(days=1) 
                            
                        self.__printDebug( '--> singleview account id: ' + sSvAcctId + ' nvr ad id: ' + sNvrAdCustomerID +' retrieve master reports - ' + sTobeHandledTaskName )
                        #sTargetUrl = self.__g_sUrl + '?mode=retrieval&cid=' + sNvrAdCustomerID + '&report=' + sTobeHandledTaskName
                        sTargetUrl = self.__g_sUrl + '?mode=retrieval&cid=' + sNvrAdCustomerID + '&a=' + self.__g_sEncodedApiKey + '&s=' + self.__g_sEncodedSecretKey \
                                    + '&f=' + self.__g_sConfigLoc  + '&report=' + sTobeHandledTaskName

                        if( dtFromRetrieval != None ):
                            if( dtFromRetrieval.date() >= datetime.today().date() ):
                                self.__printDebug('fromdate is later than yesterday - ' + dtFromRetrieval.strftime('%Y%m%d%H%M%S') )
                                dictMasterRereportQueue[sTobeHandledTaskName] = 1
                                continue

                            sTargetUrl += '&fromdate=' + dtFromRetrieval.strftime('%Y%m%d%H%M%S')
                        
                        oResp = self.__getHttpResponse( sTargetUrl )
                        try:
                            if( self.__g_sNvrAdManagerLoginId != oResp['variables']['managerLoginId'] ):
                                self.__printDebug('NVR AD manager login ID is different: ' + self.__g_sNvrAdManagerLoginId + ' on bog, NVR API returned ' + oResp['variables']['managerLoginId'] )
                                raise Exception('stop')
                        except KeyError:
                            pass;

                        try:
                            self.__printDebug( 'http result: ' + str( oResp['variables']['status'] ) )
                        except KeyError:
                            self.__printDebug( oResp )
                        
                        if( oResp['error'] == -1 ):
                            if( dictMasterRereportExceptionCnt[sTobeHandledTaskName] < 3 ): # allow exception occurrence count to 3 by each report
                                dictMasterRereportExceptionCnt[sTobeHandledTaskName] += 1
                                nWaitSec = 60
                                if( oResp['variables'] ): # oResp['variables'] list has items
                                    try:
                                       oResp['variables']['todo']
                                    except KeyError: # if ['variables']['todo'] is not defined
                                        self.__printDebug( 'error occured but todo is not defined -> wait ' + str(nWaitSec) + ' sec and go')
                                        time.sleep(nWaitSec)
                                        continue
                                    else: # if ['variables']['todo'] is defined
                                        sTodo = oResp['variables']['todo']
                                        if( sTodo ):
                                            if( sTodo == 'wait' ):
                                                self.__printDebug( 'wait ' + str(nWaitSec) + ' sec and go')
                                                time.sleep(nWaitSec)
                                                continue
                                            if( sTodo == 'pass' ):
                                                self.__printDebug( 'pass and go')
                                                dictMasterRereportQueue[sTobeHandledTaskName] = 1
                                                continue
                            else:
                                self.__printDebug('too many exceptions raise exception!!')
                                dictMasterRereportQueue[sTobeHandledTaskName] = 1
                                continue
                        else:
                            dictMasterRereportQueue[sTobeHandledTaskName] = 1
                            sDateRetrieval = time.strftime('%Y%m%d%H%M%S')
                            f = open(sLatestFilepath, 'w')
                            f.write(sDateRetrieval)
                            f.close()
                            continue
                        
                        return
                except ValueError:
                    if sTobeHandledTaskName == 'Qi':
                        self.__printDebug( '-> '+ sNvrAdCustomerID + ' completed: retrieve master reports!')
                        break
                    else:
                        self.__printDebug( '-> '+ sNvrAdCustomerID + ' failed: retrieve ' + sTobeHandledTaskName + ' master reports!')
                        raise Exception('stop')

    def __retrieveNvStatReport(self, sSvAcctId, sNvrAdCustomerID, lstReport ):
        """ retrieve NV Ad stat report """
        dictMasterReportQueue = dict()
        for report in lstReport:
            dictMasterReportQueue.update({report:0})
        
        nQueueLen = len( dictMasterReportQueue )
        if( nQueueLen ):
            while True: # loop for each report type
                try:
                    sTobeHandledTaskName = list(dictMasterReportQueue.keys())[list(dictMasterReportQueue.values()).index(0)] # find unhandled report task
                    if( sTobeHandledTaskName ): # if there exists unhandled report task
                        try:
                            sLatestFilepath = self.__g_sRetrieveInfoPath+'/'+sTobeHandledTaskName+'.latest'
                            f = open(sLatestFilepath, 'r')
                            sMaxReportDate = f.readline()
                            dtStartRetrieval = datetime.strptime(sMaxReportDate, '%Y%m%d') + timedelta(days=1)
                            f.close()
                        except FileNotFoundError:
                            nLatestRetrieveDate = 0
                            for root, dirs, filenames in os.walk(self.__g_sDownloadPath):
                                for f in filenames:
                                    if f.find(sTobeHandledTaskName+'.') != -1:
                                        aNames = f.split('_')
                                        if nLatestRetrieveDate < int(aNames[0] ):
                                            nLatestRetrieveDate = int(aNames[0] )

                            if nLatestRetrieveDate != 0:
                                dtStartRetrieval = datetime.strptime(str(nLatestRetrieveDate), '%Y%m%d') + timedelta(days=1) 
                            else:
                                dtStartRetrieval = datetime.now() - timedelta(days=1)

                        # requested stat date should not be later than today
                        dtDateEndRetrieval = datetime.now() - timedelta(days=1) # yesterday
                        dtDateDiff = dtDateEndRetrieval - dtStartRetrieval
                        nNumDays = int(dtDateDiff.days ) + 1
                        dictDateQueue = dict()
                        dictDateExceptionCnt = dict()  # memory to restrict exceptional occurrence count
                        for x in range (0, nNumDays):
                            dtElement = dtStartRetrieval + timedelta(days = x)
                            sDate = dtElement.strftime('%Y%m%d')
                            dictDateQueue.update({sDate:0})
                            dictDateExceptionCnt.update({sDate:0})
                        
                        if( len(dictDateQueue ) == 0 ):
                            dictMasterReportQueue[sTobeHandledTaskName] = 1
                            continue;
                        
                        while True: # loop for each report date
                            try:
                                sDateRetrieval = list(dictDateQueue.keys())[list(dictDateQueue.values()).index(0)] # find unhandled report task
                                self.__printDebug( '--> singleview account id: ' + sSvAcctId + ' nvr ad id: ' + sNvrAdCustomerID +' will retrieve stat report - ' + sTobeHandledTaskName +' on ' + sDateRetrieval)
                                #sTargetUrl = self.__g_sUrl + '?mode=retrieval&cid=' + sNvrAdCustomerID + '&report=' + sTobeHandledTaskName + '&statdate=' + sDateRetrieval
                                sTargetUrl = self.__g_sUrl + '?mode=retrieval&cid=' + sNvrAdCustomerID + '&a=' + self.__g_sEncodedApiKey + '&s=' + self.__g_sEncodedSecretKey \
                                    + '&f=' + self.__g_sConfigLoc  + '&report=' + sTobeHandledTaskName + '&statdate=' + sDateRetrieval

                                oResp = self.__getHttpResponse( sTargetUrl )
                                try:
                                    if( self.__g_sNvrAdManagerLoginId != oResp['variables']['loginId'] ):
                                        self.__printDebug('NVR AD manager login ID is different: ' + self.__g_sNvrAdManagerLoginId + ' on bog, NVR API returned ' + oResp['variables']['loginId'] )
                                        raise Exception('stop')
                                except KeyError:
                                    pass;
                                
                                try:
                                    self.__printDebug( 'http result: ' + str( oResp['variables']['status'] ) )
                                except KeyError:
                                    self.__printDebug( oResp )

                                if( oResp['error'] == -1 ):
                                    if( dictDateExceptionCnt[sDateRetrieval] < 3 ): # allow exception occurrence count to 3 by each report
                                        dictDateExceptionCnt[sDateRetrieval] += 1
                                        nWaitSec = 60
                                        if( oResp['variables'] ): # oResp['variables'] list has items
                                            try:
                                               oResp['variables']['todo']
                                            except KeyError: # if ['variables']['todo'] is not defined
                                                self.__printDebug( 'error occured but todo is not defined -> wait ' + str(nWaitSec) + ' sec and go')
                                                time.sleep(nWaitSec)
                                                continue
                                            else: # if ['variables']['todo'] is defined
                                                sTodo = oResp['variables']['todo']
                                                if( sTodo ):
                                                    if( sTodo == 'wait' ):
                                                        self.__printDebug( 'wait ' + str(nWaitSec) + ' sec and go')
                                                        time.sleep(nWaitSec)
                                                        continue
                                                    if( sTodo == 'pass' ):
                                                        self.__printDebug( 'pass and go')
                                                        dictDateQueue[sDateRetrieval] = 1
                                                        continue
                                    else:
                                        self.__printDebug('too many exceptions raise exception!!')
                                        dictDateQueue[sDateRetrieval] = 1
                                        continue
                                else:
                                    dictDateQueue[sDateRetrieval] = 1
                                    f = open(sLatestFilepath, 'w')
                                    f.write(sDateRetrieval)
                                    f.close()
                                    continue
                            except ValueError:
                                dictMasterReportQueue[sTobeHandledTaskName] = 1
                                break
                except ValueError:
                    break

if __name__ == '__main__': # for console debugging and execution
    dictPluginParams = {'config_loc':None, 'host_url':None} # {'config_loc':'1/test_acct', 'host_url': 'http://localhost/devel/svtest'}
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
        print( 'warning! [config_loc] [host_url] params are required for console execution.' )