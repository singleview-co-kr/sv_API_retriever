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
import sys
import gc
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
    __g_sVersion = '1.0.1'
    __g_sLastModifiedDate = '16th, Jul 2021'
    #__g_nRecordsToSend = 11000 # 29786
    __g_nMaxBytesToSend = 19000000
    # if __g_nRecordsToSend is too big for a web server, 
    # a web server occurs error and return "Allowed memory size of 134,217,728(gabia) 67,108,864(teamjang) bytes exhausted (tried to allocate XX bytes)"
    # finally, sv_http report error "http generic error raised arg0: Data must be padded to 16 byte boundary in CBC mode"
    __g_sConfigLoc = None
    __g_sTargetUrl = None
    __g_sMode = None
    __g_sReplaceYearMonth = None
    __g_dictMsg = {}
    """__g_dictMsg = { 
        'OK':1, # OK
        'FIN':2, # finish 
        'MIHY':3, # may i help you?
        'LMKL':4, # let me know new data
        'IWSY':5, # I will send you
        'ALD':6, # add latest data
        'MTG':7, # more to go
        'IHND':8, # i have new data
        'IWWFY':9, # i will wait for you
        'IHNI':10, # i have no idea
        'RRC':11, # remaining record count
        'PUP':12, # Plz Update Period
        'LMKP':13, # Let me know Period
        'WLYK':14, # will Let you know
        }
    """
    __g_oConfig = None
    __g_oLogger = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_oConfig = configparser.ConfigParser()
        self.__g_sConfigLoc = dictParams['config_loc']
        self.__g_sTargetUrl = dictParams['target_host_url']
        if( dictParams['mode'] != None ):
            self.__g_sMode = dictParams['mode']
        if( dictParams['mode'] == 'update' ):
            self.__g_sReplaceYearMonth = dictParams['yyyymm']

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
        del oSvHttp
        return oResp

    def __postHttpResponse(self, sTargetUrl, dictParams ):
        dictParams['secret'] = self.__g_oConfig['basic']['sv_secret_key']
        dictParams['iv'] = self.__g_oConfig['basic']['sv_iv']
        oSvHttp = sv_http.svHttpCom(sTargetUrl)
        oResp = oSvHttp.postUrl( dictParams )
        oSvHttp.close()
        del oSvHttp
        
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
        # if( m ): # if [config_loc] arg matches designated host_url not a directory
        #    sTargetUrl = self.__g_sConfigLoc + '?mode=check_status'
        #    oResp = self.__getHttpResponse( sTargetUrl )
        # else: # if [config_loc] arg matches designated directory like 2/yuhanrox
        #    oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        #    oResp = oSvApiConfigParser.getConfig()
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        oResp = oSvApiConfigParser.getConfig()
            
        # begin - get Protocol message dictionary
        oSvHttp = sv_http.svHttpCom('')
        self.__g_dictMsg = oSvHttp.getMsgDict()
        oSvHttp.close()
        del oSvHttp
        # end - get Protocol message dictionary

        self.__printDebug( '-> send new data' )
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self.__printDebug('invalid config_loc')
            raise Exception('stop')
            
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        self.__getKeyConfig( s_sv_acct_id, s_acct_title )

        self.__printDebug( '-> communication begin' )
        if( self.__g_sMode == 'update' ):
            self.__updatePeriod(s_acct_title)
        elif( self.__g_sMode == 'listen' ):
            pass
        else:
            self.__addNew(s_acct_title)
        self.__printDebug( '-> communication finish' )

        """
        aAcctInfo = oResp['variables']['acct_info']
        if aAcctInfo is not None:
            for sSvAcctId in aAcctInfo:
                sAcctTitle = aAcctInfo[sSvAcctId]['account_title']
                self.__getKeyConfig( sSvAcctId, sAcctTitle )

                self.__printDebug( '-> communication begin' )
                if( self.__g_sMode == 'update' ):
                    self.__updatePeriod(sAcctTitle)
                elif( self.__g_sMode == 'listen' ):
                    pass
                else:
                    self.__addNew(sAcctTitle)
                self.__printDebug( '-> communication finish' )
        """
        return 
        
    def __addNew(self, sAcctTitle):
        # server give data to dashboard client case
        # bot server: may i help you?
        dictParams = {'c': [self.__g_dictMsg['MIHY']]} 
        oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
        
        #self.__printDebug( 'rsp of MIHY' )
        #self.__printDebug( oResp )
        nMsgKey = oResp['variables']['a'][0]
        if( self.__translateMsgCode(nMsgKey) == 'LMKL' ): # dashboard client: let me know new data with required info
            lstDateRange = None
            #self.__printDebug( 'will send you what you request' ) 
            dictRetrievalDateRange = oResp['variables']['d']
            
            dictParams = {'c': [self.__g_dictMsg['IWSY']], 'd': oResp['variables']['d']} # I will send you what you request
            oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
        elif( self.__translateMsgCode(nMsgKey) == 'FIN' ): # dashboard client: stop communication by unknown reason
            self.__printDebug( 'stop communication 1' )
            raise Exception('stop')

        #self.__printDebug( 'rsp of IWSY' )
        #self.__printDebug( oResp )
        nMsgKey = oResp['variables']['a'][0]
        if( self.__translateMsgCode(nMsgKey) != 'IWWFY' ): # dashboard client: i will wait for you
            self.__printDebug( 'stop communication 2' )
            raise Exception('stop')

        # send requested data set
        lstColumnHeaderInfo = []
        lstRows = []
        nRowCount = 0
        nGrossSizeBytesToSync = 0 #################
        
        with sv_mysql.SvMySql('job_plugins.client_serve') as oSvMysql:
            oSvMysql.setTablePrefix(sAcctTitle)
            oSvMysql.initialize()

            # parse respond about retrieval date range
            try:
                sStartDate = dictRetrievalDateRange['start_date']
                sStartDate = datetime.datetime.strptime(sStartDate, '%Y%m%d').strftime('%Y-%m-%d')
                self.__printDebug( 'get from ' + sStartDate )
                lstRetrievedCompiledLog = oSvMysql.executeQuery('getCompiledLogFrom', sStartDate )
            except ValueError: # if sStartDate == 'na'
                self.__printDebug( 'get whole' )
                sEndDate = dictRetrievalDateRange['end_date']
                lstRetrievedCompiledLog = oSvMysql.executeQuery('getCompiledLogGross' )

            nRecCount = len(lstRetrievedCompiledLog )
            if( nRecCount == 0 ):
                self.__printDebug( 'stop communication - no more data to update' )
                raise Exception('stop')
            elif( nRecCount > 0 ):
                # get column info
                for sColTitle in lstRetrievedCompiledLog[0]:
                    lstColumnHeaderInfo.append( sColTitle )
                
                lstRows.append( lstColumnHeaderInfo  )  # append column header

                # get simple row
                for dictSingleRow in lstRetrievedCompiledLog:
                    #self.__printDebug( dictSingleRow  )
                    lstSingleRow = []
                    for sColTitle in dictSingleRow:
                        if( sColTitle == 'logdate' ):
                            lstSingleRow.append( dictSingleRow[sColTitle].strftime('%Y%m%d') )
                        else:
                            lstSingleRow.append( dictSingleRow[sColTitle] )

                    lstRows.append( lstSingleRow  )
                    #nRowCount = nRowCount + 1
                    nThisChunkBytes = self.__getObjSize(lstSingleRow)
                    nGrossSizeBytesToSync = nGrossSizeBytesToSync + nThisChunkBytes

                    #if nRowCount > self.__g_nRecordsToSend:
                    if nGrossSizeBytesToSync + nThisChunkBytes > self.__g_nMaxBytesToSend: # "+ nThisChunkBytes" means to estimate to add following chunk
                        dictParams = {'c': [self.__g_dictMsg['ALD']], 'd':  lstRows} # I will send you what you request
                        oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
                        
                        lstRows[:] = []
                        #nRowCount = 0
                        nGrossSizeBytesToSync = 0
                        self.__printDebug( 'transmit and initialize' )
                        
                dictParams = {'c': [self.__g_dictMsg['ALD']], 'd':  lstRows} # I will send you what you request
                oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
                self.__printDebug( 'transmit residual' )
                self.__printDebug( '-> resp of sending new data' )
                self.__printDebug( oResp )
        return

    def __updatePeriod(self, sAcctTitle):
        # server replace data in dashboard client case
        if( self.__g_sReplaceYearMonth == None ):
            self.__printDebug( 'invalid yyyymm' )
            raise Exception('remove' )
            return
        
        nYr = int(self.__g_sReplaceYearMonth[:4])
        nMo = int(self.__g_sReplaceYearMonth[4:None])
        try:
            lstMonthRange = calendar.monthrange(nYr, nMo)
        except calendar.IllegalMonthError:
            self.__printDebug( 'invalid yyyymm' )
            raise Exception('remove' )
            return

        # bot server: Plz Update Period
        dictParams = {'c': [self.__g_dictMsg['PUP']]} 
        oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
        
        #self.__printDebug( 'rsp of PUP' )
        #self.__printDebug( oResp )
        nMsgKey = oResp['variables']['a'][0]
        if( self.__translateMsgCode(nMsgKey) == 'LMKP' ): # dashboard client: Let me know Period
            self.__printDebug( 'Let me know Period' )
            dictParams = {'c': [self.__g_dictMsg['WLYK']], 'd':  self.__g_sReplaceYearMonth} # I will send you what you request
            oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
        elif( self.__translateMsgCode(nMsgKey) == 'FIN' ): # dashboard client: stop communication by unknown reason
            self.__printDebug( 'stop communication 1' )
            raise Exception('stop')

        #self.__printDebug( 'rsp of WLYK' )
        #self.__printDebug( oResp )
        
        nMsgKey = oResp['variables']['a'][0]
        if( self.__translateMsgCode(nMsgKey) != 'IWWFY' ): # dashboard client: i will wait for you
            self.__printDebug( 'stop communication 2' )
            raise Exception('stop')
        
        self.__printDebug( 'add period data' )
        # send requested data set
        lstColumnHeaderInfo = []
        lstRows = []
        nRowCount = 0
        nGrossSizeBytesToSync = 0

        sStartDateRetrieval = self.__g_sReplaceYearMonth[:4] + '-' + self.__g_sReplaceYearMonth[4:None] + '-01'
        sEndDateRetrieval = self.__g_sReplaceYearMonth[:4] + '-' + self.__g_sReplaceYearMonth[4:None] + '-' + str(lstMonthRange[1])
        with sv_mysql.SvMySql('job_plugins.client_serve') as oSvMysql:
            oSvMysql.setTablePrefix(sAcctTitle)
            oSvMysql.initialize()
            lstRetrievedCompiledLog = oSvMysql.executeQuery('getCompiledLogPeriod', sStartDateRetrieval, sEndDateRetrieval)
            nRecCount = len(lstRetrievedCompiledLog )
            if( nRecCount == 0 ):
                self.__printDebug( 'stop communication - no more data to update' )
                raise Exception('stop')
            elif( nRecCount > 0 ):
                # get column info
                for sColTitle in lstRetrievedCompiledLog[0]:
                    lstColumnHeaderInfo.append( sColTitle )
                
                lstRows.append( lstColumnHeaderInfo  )  # append column header

                # get simple row
                for dictSingleRow in lstRetrievedCompiledLog:
                    lstSingleRow = []
                    for sColTitle in dictSingleRow:
                        if( sColTitle == 'logdate' ):
                            lstSingleRow.append( dictSingleRow[sColTitle].strftime('%Y%m%d') )
                        else:
                            lstSingleRow.append( dictSingleRow[sColTitle] )

                    lstRows.append( lstSingleRow )
                    nThisChunkBytes = self.__getObjSize(lstSingleRow)

                    #nRowCount = nRowCount + 1
                    nGrossSizeBytesToSync = nGrossSizeBytesToSync + nThisChunkBytes

                    #if( nRowCount > 25516 ):
                    if nGrossSizeBytesToSync + nThisChunkBytes > self.__g_nMaxBytesToSend: # "+ nThisChunkBytes" means to estimate to add following chunk
                        dictParams = {'c': [self.__g_dictMsg['ALD']], 'd':  lstRows} # I will send you what you request
                        
                        oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
                        
                        lstRows[:] = []
                        #nRowCount = 0
                        nGrossSizeBytesToSync = 0
                        self.__printDebug( 'transmit and initialize' )
                                        
                dictParams = {'c': [self.__g_dictMsg['ALD']], 'd':  lstRows} # I will send you what you request
                oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
                
                self.__printDebug( 'transmit residual' )
                self.__printDebug( '-> resp of sending new data' )
                self.__printDebug( oResp )
        return

    def __getObjSize(self, obj):
        ''' https://stackoverflow.com/questions/13530762/how-to-know-bytes-size-of-python-object-like-arrays-and-dictionaries-the-simp '''
        marked = {id(obj)}
        obj_q = [obj]
        sz = 0

        while obj_q:
            sz += sum(map(sys.getsizeof, obj_q))

            # Lookup all the object referred to by the object in obj_q.
            # See: https://docs.python.org/3.7/library/gc.html#gc.get_referents
            all_refr = ((id(o), o) for o in gc.get_referents(*obj_q))

            # Filter object that are already marked.
            # Using dict notation will prevent repeated objects.
            new_refr = {o_id: o for o_id, o in all_refr if o_id not in marked and not isinstance(o, type)}

            # The new obj_q will be the ones that were not marked,
            # and we will update marked with their ids so we will
            # not traverse them again.
            obj_q = new_refr.values()
            marked.update(new_refr.keys())

        return sz

    def __listenToDashboardClient(self, sAcctTitle):
        # dashboard client give data to server case
        return
        nWaitSec = 0
        self.__printDebug( 'bot server: may i help you?' )
        time.sleep(nWaitSec)
        self.__printDebug( 'dashboard client: i have new data with new info' )
        time.sleep(nWaitSec)

        self.__printDebug( 'bot server: ok' )
        time.sleep(nWaitSec)
        self.__printDebug( 'dashboard client: i will wait for you' )
        time.sleep(nWaitSec)
        
        nRecCnt = 3
        for i in range(0,nRecCnt):
            self.__printDebug( 'bot server: let me know new data ' )
            time.sleep(nWaitSec)
            if( nRecCnt - i > 1 ):
                self.__printDebug( 'dashboard client: remaining rec cnt ' + str(nRecCnt-i))
                time.sleep(nWaitSec)
            else:
                self.__printDebug( 'dashboard client: finish' )
                time.sleep(nWaitSec)
        return

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
    # CLI example ->  {'config_loc':'1/test_acct', 'target_host_url': 'http://localhost/devel/modules/svestudio/b2c.php'}
    # CLI example ->  python3.6 task.py config_loc=1/test_acct target_host_url=http://localhost/devel/modules/svestudio/b2c.php
    # CLI example ->  {'config_loc':'1/test_acct', 'target_host_url': 'https://testserver.com/devel/modules/svestudio/b2c.php'}
    # CLI example ->  python3.6 task.py config_loc=1/test_acct target_host_url=https://testserver.com/devel/modules/svestudio/b2c.php
    dictPluginParams = {'config_loc':None, 'target_host_url':None, 'mode':None, 'yyyymm':None}
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