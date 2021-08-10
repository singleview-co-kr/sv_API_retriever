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
    __g_sVersion = '1.0.6'
    __g_sLastModifiedDate = '4th, Jul 2021'
    __g_oLogger = None
    __g_oSvHttp = None
    __g_sUrl = None
    __g_sEncodedApiKey = None
    __g_sEncodedSecretKey = None
    __g_sNvrAdManagerLoginId = None
    __g_sConfigLoc = None
    __g_sDataLastDate = None
    __g_sDataFirstDate = None
    __g_sRetrieveInfoPath = None
    __g_sDownloadPath = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_sConfigLoc = dictParams['config_loc']
        self.__g_sUrl = dictParams['host_url']
        self.__g_sDataLastDate = dictParams['data_last_date'].replace('-','')
        self.__g_sDataFirstDate = dictParams['data_first_date'].replace('-','')

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
                        self.__printDebug('HTTP response raised exception!! with message: ' + oResp['message'])
                        raise Exception(sTodo)
        return oResp

    def procTask(self):
        # oRegEx = re.compile(r"https?:\/\/[\w\-.]+\/\w+\/\w+\w/?$") # host_url pattern ex) /aaa/bbb or /aaa/bbb/
        # m = oRegEx.search(self.__g_sConfigLoc) # match() vs search()
        # if( m ): # if arg matches desinated host_url
        #	sTargetUrl = self.__g_sConfigLoc + '?mode=check_status'
        #	oResp = self.__getHttpResponse( sTargetUrl )
        # else:
        #	oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        #	oResp = oSvApiConfigParser.getConfig()
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
                    self.__retrieveNvReport( sSvAcctId, cid, aNvrAdAcct[cid]['nvr_stat_report'] )"""
        
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
        self.__retrieveNvStatReport( s_sv_acct_id, s_cid, dict_nvr_ad_acct[s_cid]['nvr_stat_report'] ) #statdate arg should be defined

    def __retrieveNvReport(self, sSvAcctId, sNvrAdCustomerID, lstReport ):
        dictMasterReportQueue = dict()
        dictMasterReportExceptionCnt = dict()  # memory to restrict exceptional occurrence count
        isDoneSomething = False
        
        for report in lstReport:
            dictMasterReportQueue.update({report:'readytogo'})
            dictMasterReportExceptionCnt.update({report:0})
        
        nQueueLen = len( dictMasterReportQueue )
        if( nQueueLen ):
            while True: # loop for each report type
                try:
                    sTobeHandledTaskName = list(dictMasterReportQueue.keys())[list(dictMasterReportQueue.values()).index('readytogo')] # find unhandled report task
                    if( sTobeHandledTaskName ): # if there exists unhandled report task
                        try:
                            sEarliestFilepath = self.__g_sRetrieveInfoPath+'/'+sTobeHandledTaskName+'.earliest'
                            f = open(sEarliestFilepath, 'r')
                            sMinReportDate = f.readline()
                            dtDateStatRetrieval = datetime.strptime(sMinReportDate, '%Y%m%d') - timedelta(days=1)
                            f.close()
                        except FileNotFoundError:
                            dtDateStatRetrieval = datetime.strptime(self.__g_sDataLastDate, '%Y%m%d')
                        
                        dtDelta = datetime.today() - dtDateStatRetrieval
                        if( dtDelta.days > 365 ):
                            self.__printDebug( '--> can not retrieve older than a year ago' )
                            raise Exception('remove')
                            return

                        self.__printDebug( '--> singleview account id: ' + sSvAcctId + ' nvr ad id: ' + sNvrAdCustomerID +' will retrieve stat report - ' + sTobeHandledTaskName +' on ' + str(dtDateStatRetrieval))
                        
                        # if requested stat date is earlier than stat first date
                        if( dtDateStatRetrieval - datetime.strptime(self.__g_sDataFirstDate, '%Y%m%d') < timedelta(days=0) ): 
                            self.__printDebug('meet first stat date')
                            dictMasterReportQueue[sTobeHandledTaskName] = 'finish'
                            continue;
                        
                        sTargetUrl = self.__g_sUrl + '?mode=retrieval&cid=' + sNvrAdCustomerID + '&a=' + self.__g_sEncodedApiKey + '&s=' + self.__g_sEncodedSecretKey + '&f=' + self.__g_sConfigLoc \
                                + '&report=' + sTobeHandledTaskName + '&statdate=' + dtDateStatRetrieval.strftime('%Y%m%d')
                        oResp = self.__getHttpResponse( sTargetUrl )

                        if( oResp['error'] == -1 ):
                            if( dictMasterReportExceptionCnt[sTobeHandledTaskName] < 3 ): # allow exception occurrence count to 3 by each report
                                dictMasterReportExceptionCnt[sTobeHandledTaskName] += 1
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
                                                dictMasterReportQueue[sTobeHandledTaskName] = 'pass'
                                                continue
                                            else:
                                                self.__printDebug('HTTP response raised exception!!')
                                                raise Exception(sTodo)
                            else:
                                self.__printDebug('too many exceptions raise exception!!')
                                dictMasterReportQueue[sTobeHandledTaskName] = 'finish'
                                continue
                        else:
                            dictMasterReportQueue[sTobeHandledTaskName] = 'finish'
                            isDoneSomething = True
                            f = open(sEarliestFilepath, 'w')
                            f.write(dtDateStatRetrieval.strftime('%Y%m%d'))
                            f.close()
                            continue
                        
                except ValueError:
                    break
            
            #self.__printDebug( dictMasterReportQueue )
            nPassedReportCnt = 0
            for sReport,sRst in dictMasterReportQueue.items():
                if( sRst == 'pass' ):
                    nPassedReportCnt += 1
            if( nPassedReportCnt == len( dictMasterReportQueue ) ): # if naver ad server answeres 'code': 11001 for all reports
                self.__printDebug( 'all reports have been passed -> remove the job and toggle the job table' )
                raise Exception('completed' )
            
            if(isDoneSomething is False):
                self.__printDebug('did nothing -> check whether job should be removed')
                # https://godoftyping.wordpress.com/2015/04/19/python-%EB%82%A0%EC%A7%9C-%EC%8B%9C%EA%B0%84%EA%B4%80%EB%A0%A8-%EB%AA%A8%EB%93%88/
                try:
                    dtStart = datetime.strptime(self.__g_sDataLastDate, '%Y%m%d')
                except ValueError:
                    self.__printDebug('Invalid start date!')

                try:
                    dtReverseEnd = datetime.strptime(self.__g_sDataFirstDate, '%Y%m%d')
                except ValueError:
                    self.__printDebug('Invalid end date!')

                try:
                    isSomeReportMissed = False
                    dtDateDiff = dtStart - dtReverseEnd
                    nNumDays = int(dtDateDiff.days ) + 1
                    dateList = []
                    for x in range (0, nNumDays):
                        dtElement = dtStart - timedelta(days = x)
                        dateList.append(dtElement.strftime('%Y%m%d'))
                    
                    for sReport in lstReport:
                        for sSingleDay in dateList:
                            if( aEarliestStatDate[0]['COUNT(*)'] == 0 ):  # if last stat date exists
                                isSomeReportMissed = True
                    
                    if( isSomeReportMissed is False):
                        self.__printDebug( 'no more report remained -> remove the job and toggle the job table' )
                        raise Exception('completed' )
                    
                except NameError:
                    self.__printDebug('deny to calculate day difference')
				
if __name__ == '__main__': # for console debugging
    dictPluginParams = {'config_loc':'2/test_acct', 'host_url': 'http://localhost/devel/svtest', 'data_last_date':'20181210','data_first_date':'20170913'}
    with svJobPlugin(dictPluginParams) as oJob: # to enforce to call plugin destructor
        oJob.procTask()