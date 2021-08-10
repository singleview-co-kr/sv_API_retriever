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
import datetime
import time
import configparser # https://docs.python.org/3/library/configparser.html
import calendar

import re # https://docs.python.org/3/library/re.html

# singleview library
if __name__ == '__main__': # for console debugging
    import sys
    sys.path.append('../../classes')
    import sv_http
    import sv_mysql
    import sv_api_config_parser
    sys.path.append('../../conf') # singleview config
    import basic_config
else: # for platform running
    from classes import sv_http
    from classes import sv_mysql
    from classes import sv_api_config_parser
    # singleview config
    from conf import basic_config # singleview config

class svJobPlugin():
    __g_sVersion = '0.0.2'
    __g_sLastModifiedDate = '4th, Jul 2021'
    #__g_nRecordsToSend = 11000 # 29786
    __g_sConfigLoc = None
    __g_sTargetUrl = None
    __g_sMode = None
    __g_sStartYmd = None
    __g_dictMsg = { 
        'OK':1, # OK
        'ED':2, # error detected
        }
    __g_oConfig = None
    __g_oLogger = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_oConfig = configparser.ConfigParser()
        self.__g_sConfigLoc = dictParams['config_loc']
        self.__g_sTargetUrl = dictParams['target_host_url']
        
        self.__g_sMode = dictParams['mode']

        try: 
            len(dictParams['start_ymd'])
        except:
            dictParams['start_ymd'] = ''

        if len(dictParams['start_ymd']) > 0:
            # parse respond about retrieval date range
            try:
                sStartDate = datetime.datetime.strptime(dictParams['start_ymd'], '%Y%m%d').strftime('%Y%m%d')
                dictParams['start_ymd'] = sStartDate #dictParams['start_ymd']
            except ValueError: # if sStartDate == 'na'
                raise ValueError("Incorrect data format, should be YYYYMMDD")
        
        self.__g_sStartYmd = dictParams['start_ymd']
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

    def __getSecuredHttpResponse(self, sTargetUrl):
        dictParams = {}
        dictParams['secret'] = self.__g_oConfig['basic']['sv_secret_key']
        dictParams['iv'] = self.__g_oConfig['basic']['sv_iv']
        oSvHttp = sv_http.svHttpCom(sTargetUrl)
        oResp = oSvHttp.getSecuredUrl(dictParams)
        oSvHttp.close()
        return oResp

    def __getHttpResponse(self, sTargetUrl ):
        oSvHttp = sv_http.svHttpCom(sTargetUrl)
        oResp = oSvHttp.getUrl()
        oSvHttp.close()
        return oResp

    def __postHttpResponse(self, sTargetUrl, dictParams ):
        dictParams['secret'] = self.__g_oConfig['basic']['sv_secret_key']
        dictParams['iv'] = self.__g_oConfig['basic']['sv_iv']
        oSvHttp = sv_http.svHttpCom(sTargetUrl)
        oResp = oSvHttp.postUrl( dictParams )
        oSvHttp.close()
        
        if( oResp['error'] == -1 ):
            sTodo = oResp['variables']['todo']
            if( sTodo ):
                self.__printDebug('HTTP response raised exception!!')
                raise Exception(sTodo)
        else:
            return oResp

    def __translateMsgCode(self, nMsgKey):
        for sMsg, nKey in self.__g_dictMsg.items():
            if nKey == nMsgKey:
                return sMsg
        
    def __getKeyConfig(self, sSvAcctId, sAcctTitle):
        sKeyConfigPath = basic_config.ABSOLUTE_PATH_BOT + '/files/' + sSvAcctId +'/' + sAcctTitle + '/key.config.ini'
        try:
            with open(sKeyConfigPath) as f:
                self.__g_oConfig.read_file(f)
        except FileNotFoundError:
            self.__printDebug( 'key.config.ini not exist')
            raise Exception('stop')

        self.__g_oConfig.read(sKeyConfigPath)

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
        
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self.__printDebug('invalid config_loc')
            raise Exception('stop')
            
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        self.__getKeyConfig( s_sv_acct_id, s_acct_title )

        self.__printDebug( '-> crontab trigger URL begin' )
        if( self.__g_sMode == 'collect_npay_order' ):
            self.__collectNpayOrder()
        elif( self.__g_sMode == 'collect_npay_review' ):
            self.__collectNpayReview()
        elif( self.__g_sMode == 'collect_npay_inquiry' ):
            pass
        elif( self.__g_sMode == 'arrange_sv_order_status' ):
            pass
        else:
            self.__printDebug( 'invalid mode' )
        self.__printDebug( '-> crontab finish' )

        """
        aAcctInfo = oResp['variables']['acct_info']
        if aAcctInfo is not None:
            for sSvAcctId in aAcctInfo:
                sAcctTitle = aAcctInfo[sSvAcctId]['account_title']
                self.__getKeyConfig( sSvAcctId, sAcctTitle )

            self.__printDebug( '-> crontab trigger URL begin' )
            if( self.__g_sMode == 'collect_npay_order' ):
                self.__collectNpayOrder()
            elif( self.__g_sMode == 'collect_npay_review' ):
                self.__collectNpayReview()
            elif( self.__g_sMode == 'collect_npay_inquiry' ):
                pass
            elif( self.__g_sMode == 'arrange_sv_order_status' ):
                pass
            else:
                self.__printDebug( 'invalid mode' )
            self.__printDebug( '-> crontab finish' )
        """
        return 

    def __collectNpayOrder(self):
        # python3.6 task.py config_loc=2/test_acct target_host_url=http://server.com/crontab mode=collect_npay_order start_ymd=20191011
        self.__g_sTargetUrl = self.__g_sTargetUrl + '?task=getNpayOrder'
        if len(self.__g_sStartYmd) > 0:
            self.__g_sTargetUrl = self.__g_sTargetUrl + '&start_ymd=' + self.__g_sStartYmd

        #print( self.__g_sTargetUrl )
        oResp = self.__getSecuredHttpResponse( self.__g_sTargetUrl )
        #print('###########')
        #print(oResp)
        #print('###########')
        nMsgKey = oResp['variables']['a'][0]
        sRespCode = self.__translateMsgCode(nMsgKey)
        if( sRespCode == 'OK' ): # crontab client: all good
            #print('fine')
            pass
        elif( sRespCode == 'ED' ): # crontab client: all good
            self.__printDebug( 'something wrong' )
        return

    def __collectNpayReview(self):
        # python3.6 task.py config_loc=2/test_acct target_host_url=http://server.com/crontab mode=collect_npay_review start_ymd=20191011
        self.__g_sTargetUrl = self.__g_sTargetUrl + '?task=getNpayReview'
        if len(self.__g_sStartYmd) > 0:
            self.__g_sTargetUrl = self.__g_sTargetUrl + '&start_ymd=' + self.__g_sStartYmd

        oResp = self.__getSecuredHttpResponse( self.__g_sTargetUrl )
        #print('###########')
        #print(oResp)
        #print('###########')
        nMsgKey = oResp['variables']['a'][0]
        sRespCode = self.__translateMsgCode(nMsgKey)
        if( sRespCode == 'OK' ): # crontab client: all good
            #print('fine')
            pass
        elif( sRespCode == 'ED' ): # crontab client: all good
            self.__printDebug( 'something wrong' )
        return

    def __translateMsgCode(self, nMsgKey):
        for sMsg, nKey in self.__g_dictMsg.items():
            if nKey == nMsgKey:
                return sMsg

if __name__ == '__main__': # for console debugging
    # CLI example ->  {'config_loc':'2/test_acct', 'target_host_url': 'http://server.com/crontab mode=collect_npay_order', 'start_ymd': '20191011'}
    # CLI example ->  {'config_loc':'2/test_acct', 'target_host_url': 'http://server.com/crontab mode=collect_npay_review', 'start_ymd': '20191011'}
    dictPluginParams = {'config_loc':None, 'target_host_url':None, 'mode':None, 'start_ymd':None}
    nCliParams = len(sys.argv)
    if( nCliParams >= 3 ):
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
        print( 'warning! [config_loc] [target_host_url] params are required for console execution.' )