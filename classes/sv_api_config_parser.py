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

import re # https://docs.python.org/3/library/re.html

# singleview config
if __name__ == '__main__': # for class console debugging
    import sys
    sys.path.append('../conf')
    import basic_config
elif __name__ == 'sv_api_config_parser': # for plugin console debugging
    import sys
    sys.path.append('../../conf')
    import basic_config
elif __name__ == 'classes.sv_api_config_parser' : # for platform running
    from conf import basic_config

# standard library
import logging
import configparser # https://docs.python.org/3/library/configparser.html

class SvApiConfigParser:
    __g_oLogger = None
    __g_sConfigLoc = None
    __g_sConfigSource = None  # file or http
    __g_sApiConfigFile = None
    __g_lstAcctInfo = []

    def __init__(self, sConfigLocation):
        self.__g_oConfig = configparser.RawConfigParser() # make python3 config parser parse key case sensitive
        self.__g_oConfig.optionxform = lambda option: option # make python3 config parser parse key case sensitive

        self.__g_oLogger = logging.getLogger(__file__)
        oRegEx = re.compile(r"https?:\/\/[\w\-.]+\/\w+\/\w+\w/?$") # host_url pattern ex) /aaa/bbb or /aaa/bbb/
        m = oRegEx.search(sConfigLocation) # match() vs search()
        if( m ): # if arg matches desinated host_url
            self.__g_sConfigSource = 'http'
            self.__g_sConfigLoc = sConfigLocation + '?mode=check_status'
        else:
            self.__g_sConfigSource = 'file'
            self.__g_sConfigLoc = sConfigLocation

            if( self.__g_sConfigLoc[0] == '/' ): # absolute config path case
                #sApiConfigFile = 'api_info.ini'
                self.__printDebug( 'absolute api_info.ini not process is not defined')
                raise IOError('failed to read api_info.ini')
            else: # relative config path case
                self.__g_sApiConfigFile = basic_config.ABSOLUTE_PATH_BOT +'/files/' + self.__g_sConfigLoc + '/api_info.ini'
                self.__g_lstAcctInfo = self.__g_sConfigLoc.split('/')

    def getConfig(self):
        if self.__g_sConfigSource == 'http':
            dictResp = self.__getHttpResponse(self.__g_sConfigLoc)
        elif self.__g_sConfigSource == 'file':
            try:
                with open(self.__g_sApiConfigFile) as f:
                    self.__g_oConfig.read_file(f)
            except IOError:
                self.__printDebug( 'api_info.ini not exist')
                raise IOError('failed to read api_info.ini')

            self.__g_oConfig.read(self.__g_sApiConfigFile)

            dictNvrAdAcct = {}
            lstGoogleadsAcct = []
            dictOtherAdsApiInfo = {}
            lstNvrMasterReport = [] 
            lstNvrStatReport = []

            for sSectionTitle in self.__g_oConfig:
                if( sSectionTitle == 'naver_searchad' ):
                    for sValueTitle in self.__g_oConfig[sSectionTitle]:
                        if( sValueTitle == 'api_key' or sValueTitle == 'secret_key' or sValueTitle == 'manager_login_id' ): #######
                            dictNvrAdAcct.update( { sValueTitle: self.__g_oConfig[sSectionTitle][sValueTitle] } )
                        elif( sValueTitle == 'customer_id' ):
                            dictNvrAdAcct.update( { self.__g_oConfig[sSectionTitle][sValueTitle]:None } )
                elif( sSectionTitle == 'nvr_master_report' ):
                    for sValueTitle in self.__g_oConfig[sSectionTitle]:
                        if( self.__g_oConfig[sSectionTitle][sValueTitle] == '1' ):
                            lstNvrMasterReport.append( sValueTitle )
                elif( sSectionTitle == 'nvr_stat_report' ):
                    for sValueTitle in self.__g_oConfig[sSectionTitle]:
                        if( self.__g_oConfig[sSectionTitle][sValueTitle] == '1' ):
                            lstNvrStatReport.append( sValueTitle )
                elif( sSectionTitle == 'google_ads' ):
                    for sValueTitle in self.__g_oConfig[sSectionTitle]:
                        if self.__g_oConfig[sSectionTitle][sValueTitle].lower() == 'on':
                            lstGoogleadsAcct.append(sValueTitle)
                        dictOtherAdsApiInfo.update( { 'adw_cid': lstGoogleadsAcct } )
                elif( sSectionTitle == 'others' ):
                    for sValueTitle in self.__g_oConfig[sSectionTitle]:
                        dictOtherAdsApiInfo.update( { sValueTitle: self.__g_oConfig[sSectionTitle][sValueTitle] } )
            
            dictNvrReportGroup = { 'nvr_master_report': lstNvrMasterReport, 'nvr_stat_report': lstNvrStatReport }
            for sNvrAdAcctKey in dictNvrAdAcct:
                if( sNvrAdAcctKey != 'api_key' and sNvrAdAcctKey != 'secret_key' and sNvrAdAcctKey != 'manager_login_id'): ########
                    dictNvrAdAcct[sNvrAdAcctKey] = dictNvrReportGroup		

            dict2ndLayer = { 'account_title': self.__g_lstAcctInfo[1], 'nvr_ad_acct': dictNvrAdAcct }
            for sValueTitle in dictOtherAdsApiInfo:
                    dict2ndLayer.update( { sValueTitle: dictOtherAdsApiInfo[sValueTitle] } )

            dict1stLayer = { self.__g_lstAcctInfo[0] : dict2ndLayer }
            dictVars = {'acct_info': dict1stLayer }
            dictResp = {'error': 0, 'message': 'success', 'variables': dictVars, 'httpStatusCode': None}
        return dictResp
    
    def __getHttpResponse(self, sTargetUrl ):
        #################################################################
        #### warning! need to complete the code; eg import statement ####
        #### or deprecate this method
        #################################################################
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

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __printDebug( self, sMsg ):
        if __name__ == '__main__' or __name__ == 'sv_api_config_parser':
            print( sMsg )
        else:
            if( self.__g_oLogger is not None ):
                self.__g_oLogger.debug( sMsg )

    def __del__(self):
        pass

if __name__ == '__main__': # for console debugging
	pass