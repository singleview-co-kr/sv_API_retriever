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
from datetime import datetime
import sys
import os
import configparser # https://docs.python.org/3/library/configparser.html
import calendar
import sys
import gc

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_http
    import sv_mysql
    import sv_object
    import sv_plugin
    import settings
    import ga_media_log
    import ga_int_search_log
    import ga_itemperf_log
    import kw_qi_log
    import word_cloud
    import edi_log
    import sv_adr
else: # for platform running
    from svcommon import sv_http
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from django.conf import settings
    from svplugins.client_serve import ga_media_log
    from svplugins.client_serve import ga_int_search_log
    from svplugins.client_serve import ga_itemperf_log
    from svplugins.client_serve import kw_qi_log
    from svplugins.client_serve import word_cloud
    from svplugins.client_serve import edi_log
    from svplugins.client_serve import sv_adr


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    #__g_nRecordsToSend = 11000
    __g_nMaxBytesToSend = 19000000
    __g_oConfig = configparser.ConfigParser()
    # if __g_nRecordsToSend is too big for a web server, 
    # a web server occurs error and return "Allowed memory size of 134,217,728(gabia) 67,108,864(teamjang) bytes exhausted (tried to allocate XX bytes)"
    # finally, sv_http report error "http generic error raised arg0: Data must be padded to 16 byte boundary in CBC mode"
    
    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230605)')
        
        self._g_dictParam.update({'target_host_url': None, 'mode': None, 'yyyymm': None, 'top_n_cnt': None})
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict.
        # To create instance variables, you declare them with self in __init__.
        self.__g_sTargetUrl = None
        self.__g_sMode = None
        self.__g_sReplaceYearMonth = None
        self.__g_sTblPrefix = None
        self.__g_dictMsg = {}

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sTargetUrl = None
        self.__g_sMode = None
        self.__g_sReplaceYearMonth = None
        self.__g_sTblPrefix = None
        self.__g_dictMsg = {}

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_sTargetUrl = self._g_dictParam['target_host_url']
        if self._g_dictParam['mode'] is not None:
            self.__g_sMode = self._g_dictParam['mode']
        if self._g_dictParam['mode'] in ['update_ga_media_sql', 'update_ga_media_encrypted']:
            self.__g_sReplaceYearMonth = self._g_dictParam['yyyymm']
        elif self._g_dictParam['mode'] in ['add_wc_sql']:
            s_top_n_cnt = self._g_dictParam['top_n_cnt']

        # begin - get Protocol message dictionary
        o_sv_http = sv_http.SvHttpCom('')
        self.__g_dictMsg = o_sv_http.getMsgDict()
        o_sv_http.close()
        del o_sv_http
        # end - get Protocol message dictionary

        self._print_debug('-> send new data')
        
        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info:
            self._print_debug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']
        self.__get_key_config(s_sv_acct_id, s_brand_id)

        if self.__g_sTargetUrl is None:
            if 'server' in self.__g_oConfig:
                self.__g_sTargetUrl = self.__g_oConfig['server']['etl_host_url']
            else:
                self._print_debug('stop -> invalid etl_host_url')
                self._task_post_proc(self._g_oCallback)
                if self._g_bDaemonEnv:  # for running on dbs.py only
                    raise Exception('remove')
                else:
                    return

        self._print_debug('-> communication begin')
        if self.__g_sMode in ['add_ga_media_sql', 'update_ga_media_sql']:
            self._print_debug('-> transfer ga media log to BI DB via SQL')
            o_ga_media_log = ga_media_log.SvGaMediaLog()
            o_ga_media_log.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix,
                                    self._printDebug, self._print_progress_bar, self._continue_iteration,
                                    self.__g_sReplaceYearMonth)
            o_ga_media_log.proc(self.__g_sMode)
            del o_ga_media_log
        elif self.__g_sMode in ['add_ga_intsearch_sql']:
            self._print_debug('-> transfer ga internal search log to BI DB via SQL')
            o_ga_media_log = ga_int_search_log.SvGaIntSearchLog()
            o_ga_media_log.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix,
                                    self._printDebug, self._print_progress_bar, self._continue_iteration)
            o_ga_media_log.proc(self.__g_sMode)
            del o_ga_media_log
        elif self.__g_sMode in ['add_ga_itemperf_sql', 'clear_ga_itemperf_sql']:
            self._print_debug('-> transfer ga item performance log to BI DB via SQL')
            o_ga_media_log = ga_itemperf_log.SvGaItemPerfLog()
            o_ga_media_log.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix,
                                    self._printDebug, self._print_progress_bar, self._continue_iteration)
            o_ga_media_log.proc(self.__g_sMode)
            del o_ga_media_log
        elif self.__g_sMode in ['add_nvr_qi_sql']:
            self._print_debug('-> transfer de-normed Naver CPC keyword quality index to BI DB via SQL')
            o_kw_qi_log = kw_qi_log.SvKeywordQi()
            o_kw_qi_log.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix,
                                  self._printDebug, self._print_progress_bar, self._continue_iteration)
            o_kw_qi_log.proc(self.__g_sMode)
            del o_kw_qi_log
        elif self.__g_sMode in ['add_wc_sql']:
            self._print_debug('-> transfer de-normed word cloud to BI DB via SQL')
            o_ga_media_log = word_cloud.SvWordCloud()
            o_ga_media_log.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix,
                                    self._printDebug, self._print_progress_bar, self._continue_iteration)
            o_ga_media_log.proc(self.__g_sMode, s_top_n_cnt)
            del o_ga_media_log
        elif self.__g_sMode in ['add_sv_adr_sql']:
            self._print_debug('-> transfer de-normed sv addr log to BI DB via SQL')
            o_sv_addr_log = sv_adr.SvAddress()
            o_sv_addr_log.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix,
                                    self._printDebug, self._print_progress_bar, self._continue_iteration)
            o_sv_addr_log.proc(self.__g_sMode)
            del o_sv_addr_log
        elif self.__g_sMode in ['add_edi_sql']:
            self._print_debug('-> transfer de-normed edi daily log to BI DB via SQL')
            o_ga_media_log = edi_log.SvEdiLog()
            o_ga_media_log.init_var(self._g_dictSvAcctInfo, self.__g_sTblPrefix,
                                    self._printDebug, self._print_progress_bar, self._continue_iteration)
            o_ga_media_log.proc(self.__g_sMode)
            del o_ga_media_log
        elif self.__g_sMode == 'add_ga_media_encrypted':  # will separate to sub class
            self.__add_new_ga_media_encrypted()
        elif self.__g_sMode == 'update_ga_media_encrypted':  # will separate to sub class
            self.__update_period_ga_media_encrypted()
        else:
            self._print_debug('weird mode designated')
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                self._task_post_proc(self._g_oCallback)
                return
        self._print_debug('-> communication finish')
        self._task_post_proc(self._g_oCallback)
        
    def __add_new_ga_media_encrypted(self):
        # server give data to dashboard client case
        # bot server: may i help you?
        dictParams = {'c': [self.__g_dictMsg['MIHY']]} 
        oResp = self.__post_http(self.__g_sTargetUrl, dictParams)        
        nMsgKey = oResp['variables']['a'][0]
        if self.__translate_msg_code(nMsgKey) == 'LMKL': # dashboard client: let me know new data with required info
            dictRetrievalDateRange = oResp['variables']['d']
            dictParams = {'c': [self.__g_dictMsg['IWSY']], 'd': oResp['variables']['d']} # I will send you what you request
            oResp = self.__post_http(self.__g_sTargetUrl, dictParams)
        elif self.__translate_msg_code(nMsgKey) == 'FIN': # dashboard client: stop communication by unknown reason
            self._print_debug('stop communication 1')
            return

        if not self._continue_iteration():
            return

        nMsgKey = oResp['variables']['a'][0]
        if self.__translate_msg_code(nMsgKey) != 'IWWFY': # dashboard client: i will wait for you
            self._print_debug('stop communication 2')
            return

        # send requested data set
        lstColumnHeaderInfo = []
        lstRows = []
        nGrossSizeBytesToSync = 0
        with sv_mysql.SvMySql() as oSvMysql:
            oSvMysql.set_tbl_prefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.client_serve')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            # parse respond about retrieval date range
            try:
                sStartDate = dictRetrievalDateRange['start_date']
                sStartDate = datetime.strptime(sStartDate, '%Y%m%d').strftime('%Y-%m-%d')
                self._print_debug('get from ' + sStartDate)
                lstRetrievedCompiledLog = oSvMysql.executeQuery('getCompiledGaMediaLogFrom', sStartDate)
            except ValueError: # if sStartDate == 'na'
                self._print_debug('get whole')
                sEndDate = dictRetrievalDateRange['end_date']
                lstRetrievedCompiledLog = oSvMysql.executeQuery('getCompiledGaMediaLogGross')

            nRecCount = len(lstRetrievedCompiledLog)
            if nRecCount == 0:
                self._print_debug('stop communication - no more data to update')
                return
            elif nRecCount > 0:
                # get column info
                for sColTitle in lstRetrievedCompiledLog[0]:
                    lstColumnHeaderInfo.append(sColTitle)
                
                lstRows.append(lstColumnHeaderInfo)  # append column header
                # get simple row
                for dictSingleRow in lstRetrievedCompiledLog:
                    if not self._continue_iteration():
                        return

                    lstSingleRow = []
                    for sColTitle in dictSingleRow:
                        if sColTitle == 'logdate':
                            lstSingleRow.append(dictSingleRow[sColTitle].strftime('%Y%m%d'))
                        else:
                            lstSingleRow.append(dictSingleRow[sColTitle])

                    lstRows.append(lstSingleRow)
                    nThisChunkBytes = self.__get_obj_size(lstSingleRow)
                    nGrossSizeBytesToSync = nGrossSizeBytesToSync + nThisChunkBytes
                    if nGrossSizeBytesToSync + nThisChunkBytes > self.__g_nMaxBytesToSend: # "+ nThisChunkBytes" means to estimate to add following chunk
                        dictParams = {'c': [self.__g_dictMsg['ALD']], 'd':  lstRows} # I will send you what you request
                        oResp = self.__post_http(self.__g_sTargetUrl, dictParams)
                        lstRows[:] = []
                        nGrossSizeBytesToSync = 0
                        self._print_debug('transmit and initialize')
                        
                dictParams = {'c': [self.__g_dictMsg['ALD']], 'd':  lstRows} # I will send you what you request
                oResp = self.__post_http(self.__g_sTargetUrl, dictParams)
                self._print_debug('transmit residual')
                self._print_debug('-> resp of sending new data')
                self._print_debug(oResp)
        return

    def __update_period_ga_media_encrypted(self):
        # server replace data in dashboard client case
        if self.__g_sReplaceYearMonth == None:
            self._print_debug('stop -> invalid yyyymm')
            return
        
        nYr = int(self.__g_sReplaceYearMonth[:4])
        nMo = int(self.__g_sReplaceYearMonth[4:None])
        try:
            lstMonthRange = calendar.monthrange(nYr, nMo)
        except calendar.IllegalMonthError:
            self._print_debug('stop -> invalid yyyymm')
            return

        # bot server: Plz Update Period
        dictParams = {'c': [self.__g_dictMsg['PUP']]} 
        oResp = self.__post_http(self.__g_sTargetUrl, dictParams)
        nMsgKey = oResp['variables']['a'][0]
        if self.__translate_msg_code(nMsgKey) == 'LMKP': # dashboard client: Let me know Period
            self._print_debug('Let me know Period')
            dictParams = {'c': [self.__g_dictMsg['WLYK']], 'd':  self.__g_sReplaceYearMonth} # I will send you what you request
            oResp = self.__post_http(self.__g_sTargetUrl, dictParams)
        elif self.__translate_msg_code(nMsgKey) == 'FIN': # dashboard client: stop communication by unknown reason
            self._print_debug('stop -> stop communication 1')
            return

        nMsgKey = oResp['variables']['a'][0]
        if self.__translate_msg_code(nMsgKey) != 'IWWFY': # dashboard client: i will wait for you
            self._print_debug('stop communication 2')
            return
        
        self._print_debug('add period data')
        # send requested data set
        lstColumnHeaderInfo = []
        lstRows = []
        nGrossSizeBytesToSync = 0
        sStartDateRetrieval = self.__g_sReplaceYearMonth[:4] + '-' + self.__g_sReplaceYearMonth[4:None] + '-01'
        sEndDateRetrieval = self.__g_sReplaceYearMonth[:4] + '-' + self.__g_sReplaceYearMonth[4:None] + '-' + str(lstMonthRange[1])
        with sv_mysql.SvMySql('svplugins.client_serve', self._g_dictSvAcctInfo) as oSvMysql:
            oSvMysql.set_tbl_prefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.client_serve')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            lstRetrievedCompiledLog = oSvMysql.executeQuery('getCompiledGaMediaLogPeriod', sStartDateRetrieval, sEndDateRetrieval)
            nRecCount = len(lstRetrievedCompiledLog )
            if nRecCount == 0:
                self._print_debug('stop communication - no more data to update')
                raise Exception('stop')
            elif nRecCount > 0:
                # get column info
                for sColTitle in lstRetrievedCompiledLog[0]:
                    lstColumnHeaderInfo.append(sColTitle)
                
                lstRows.append(lstColumnHeaderInfo)  # append column header
                for dictSingleRow in lstRetrievedCompiledLog:
                    if not self._continue_iteration():
                        return
                        
                    lstSingleRow = []
                    for sColTitle in dictSingleRow:
                        if sColTitle == 'logdate':
                            lstSingleRow.append(dictSingleRow[sColTitle].strftime('%Y%m%d'))
                        else:
                            lstSingleRow.append(dictSingleRow[sColTitle])

                    lstRows.append(lstSingleRow)
                    nThisChunkBytes = self.__get_obj_size(lstSingleRow)
                    nGrossSizeBytesToSync = nGrossSizeBytesToSync + nThisChunkBytes
                    if nGrossSizeBytesToSync + nThisChunkBytes > self.__g_nMaxBytesToSend: # "+ nThisChunkBytes" means to estimate to add following chunk
                        dictParams = {'c': [self.__g_dictMsg['ALD']], 'd':  lstRows} # I will send you what you request
                        
                        oResp = self.__post_http(self.__g_sTargetUrl, dictParams)
                        lstRows[:] = []
                        nGrossSizeBytesToSync = 0
                        self._print_debug('transmit and initialize')
                                        
                dictParams = {'c': [self.__g_dictMsg['ALD']], 'd':  lstRows} # I will send you what you request
                oResp = self.__post_http(self.__g_sTargetUrl, dictParams)
                self._print_debug('transmit residual')
                self._print_debug('-> resp of sending new data')
                self._print_debug(oResp)
        return

    def __post_http(self, sTargetUrl, dictParams):
        dictParams['secret'] = self.__g_oConfig['basic']['sv_secret_key']
        dictParams['iv'] = self.__g_oConfig['basic']['sv_iv']
        oSvHttp = sv_http.SvHttpCom(sTargetUrl)
        oResp = oSvHttp.post_url(dictParams)
        oSvHttp.close()
        del oSvHttp
        if oResp['error'] == -1:
            sTodo = oResp['variables']['todo']
            if sTodo:
                self._print_debug('HTTP response raised exception!!')
                raise Exception(sTodo)
        else:
            return oResp

    def __translate_msg_code(self, nMsgKey):
        for sMsg, nKey in self.__g_dictMsg.items():
            if nKey == nMsgKey:
                return sMsg
        
    def __get_key_config(self, sSvAcctId, sAcctTitle):
        sKeyConfigPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'key.config.ini')
        try:
            with open(sKeyConfigPath) as f:
                self.__g_oConfig.read_file(f)
        except FileNotFoundError:
            self._print_debug( 'key.config.ini not exist')
            return # raise Exception('stop')
        self.__g_oConfig.read(sKeyConfigPath)

    def __get_obj_size(self, obj):
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


if __name__ == '__main__': # for console debugging
    # python task.py config_loc=1/1 target_host_url=http://localhost/devel/modules/svestudio/b2c.php
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('client_serve')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [mode] params are required for console execution.')
