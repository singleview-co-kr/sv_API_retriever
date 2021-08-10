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
import re
import os
import csv
import calendar
import codecs
import math

# singleview library
if __name__ == '__main__': # for console debugging
    import sys
    sys.path.append('../../classes')
    import sv_mysql
    import csv
    import sv_api_config_parser
    sys.path.append('../../conf') # singleview config
    import basic_config
else: # for platform running
    from classes import sv_mysql
    import sv_api_config_parser
    # singleview config
    from conf import basic_config # singleview config	

class svJobPlugin():
    __g_bFbProcess = False
    __g_nPnsTouchingDate = 20190126 # to seperate the old & non-systematic & complicated situation for PNS cost process
    __g_sVersion = '0.0.2'
    __g_sLastModifiedDate = '15th, Dec 2020'
    __g_sConfigLoc = None
    __g_sDataPath = None
    __g_sRetrieveMonth = None

    __g_sMode = None
    __g_oLogger = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_sConfigLoc = dictParams['config_loc']
        self.__g_sRetrieveMonth = dictParams['yyyymm']
        self.__g_sMode = dictParams['mode']

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
        oRegEx = re.compile(r"https?:\/\/[\w\-.]+\/\w+\/\w+\w/?$") # host_url pattern ex) /aaa/bbb or /aaa/bbb/
        m = oRegEx.search(self.__g_sConfigLoc) # match() vs search()
        if( m ): # if arg matches desinated host_url
            sTargetUrl = self.__g_sConfigLoc + '?mode=check_status'
            oResp = self.__getHttpResponse( sTargetUrl )
        else:
            oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
            oResp = oSvApiConfigParser.getConfig()
        
        aAcctInfo = oResp['variables']['acct_info']
        if aAcctInfo is not None:
            for sSvAcctId in aAcctInfo:
                sAcctTitle = aAcctInfo[sSvAcctId]['account_title']
                self.__g_sDataPath = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', sSvAcctId, sAcctTitle, 'extract')

                if not os.path.isdir(self.__g_sDataPath):
                    os.mkdir(self.__g_sDataPath)
                
                with sv_mysql.SvMySql('job_plugins.extract_db') as oSvMysql:
                    oSvMysql.setTablePrefix(sAcctTitle)
                    oSvMysql.initialize()
                
                aNvrAdAcct = aAcctInfo[sSvAcctId]['nvr_ad_acct']
                for cid in aNvrAdAcct:
                    if( cid == 'api_key' or cid == 'secret_key' or cid == 'manager_login_id'):
                        continue
                                        
                    dictDateRange = self.__extractData( sAcctTitle )

    def __extractData(self, sAcctTitle ):
        with sv_mysql.SvMySql('job_plugins.extract_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            lstRst = oSvMysql.executeQuery('getCompiledDailyLogCount')

        nSentinel = int(lstRst[0]['COUNT(*)'])
        lstRst.clear()
        with sv_mysql.SvMySql('job_plugins.extract_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(sAcctTitle)
            lstRst = oSvMysql.executeQuery('getCompiledDailyLogAll')
        
        s_full_path_extracted_csv = os.path.join(self.__g_sDataPath, sAcctTitle+'_ga_compiled_log.csv')
        try:
            with codecs.open(s_full_path_extracted_csv, "w", "utf-8") as f:
                writer = csv.writer(f)
                # write csv header
                writer.writerow(['log_srl', 'media_ua', 'media_term', 'media_source', 'media_rst_type', 'media_media', 
                                    'media_brd', 'media_camp1st', 'media_camp2nd', 'media_camp3rd', 'media_raw_cost', 
                                    'media_agency_cost', 'media_cost_vat', 'media_imp', 'media_click', 'media_conv_cnt', 
                                    'media_conv_amnt', 'in_site_tot_session', 'in_site_tot_new', 'in_site_tot_bounce', 
                                    'in_site_tot_duration_sec', 'in_site_tot_pvs', 'in_site_trs', 'in_site_revenue', 
                                    'in_site_registrations',
                                    'log_yr', 'log_mo', 'log_day', 'log_qr', 'log_yr_qr', 'log_wk_no', 'log_yr_wk_no',
                                    'log_day_name', 'log_day_mo', 'log_day_yr','logdate'])
                # write csv body
                nIdx = 0
                for dictSingleRec in lstRst:
                    s_quater = self.__get_quater(dictSingleRec['logdate'])
                    s_yr = dictSingleRec['logdate'].strftime("%Y")
                    s_wk = dictSingleRec['logdate'].strftime("%V")
                    writer.writerow([dictSingleRec['log_srl'],
                                     dictSingleRec['media_ua'],
                                     "'"+dictSingleRec['media_term']+"'",
                                     dictSingleRec['media_source'],
                                     dictSingleRec['media_rst_type'],
                                     dictSingleRec['media_media'],
                                     dictSingleRec['media_brd'],
                                     dictSingleRec['media_camp1st'],
                                     dictSingleRec['media_camp2nd'],
                                     dictSingleRec['media_camp3rd'],
                                     dictSingleRec['media_raw_cost'],
                                     dictSingleRec['media_agency_cost'],
                                     dictSingleRec['media_cost_vat'],
                                     dictSingleRec['media_imp'],
                                     dictSingleRec['media_click'],
                                     dictSingleRec['media_conv_cnt'],
                                     dictSingleRec['media_conv_amnt'],
                                     dictSingleRec['in_site_tot_session'],
                                     dictSingleRec['in_site_tot_new'],
                                     dictSingleRec['in_site_tot_bounce'],
                                     dictSingleRec['in_site_tot_duration_sec'],
                                     dictSingleRec['in_site_tot_pvs'],
                                     dictSingleRec['in_site_trs'],
                                     dictSingleRec['in_site_revenue'],
                                     dictSingleRec['in_site_registrations'],
                                     s_yr,
                                     dictSingleRec['logdate'].strftime("%m"),
                                     dictSingleRec['logdate'].strftime("%d"),
                                     s_quater,
                                     s_yr + '-' + s_quater,
                                     s_wk,
                                     s_yr + '-' + s_wk,
                                     dictSingleRec['logdate'].strftime("%a"),
                                     dictSingleRec['logdate'].strftime("%d"),  # will be deprecated
                                     dictSingleRec['logdate'].strftime("%j"),
                                     dictSingleRec['logdate'].strftime("%Y%m%d")])
                    self.__printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data:', suffix = 'Complete', length = 50)
                    nIdx += 1
        except Exception as e:
            print(e)

        lstRst.clear()
        return

    def __get_quater(self, dt):
        return 'Q'+str(int(math.ceil(dt.month / 3.)))

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
    dictPluginParams = {'config_loc':None, 'yyyymm':None, 'mode':None} # {'config_loc':'1/test_acct', 'yyyymm':'201811'}
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