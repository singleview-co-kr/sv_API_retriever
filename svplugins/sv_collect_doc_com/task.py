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
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import os
import configparser # https://docs.python.org/3/library/configparser.html
import sys

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_http
    import sv_mysql
    import sv_object
    import sv_plugin
    import settings
else: # for platform running
    from svcommon import sv_http
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sFirstDateOfTheUniv = '20000101'
    __g_dictSource = {
        'singleview_estudio': 1,
        'twitter': 2,
        'naver': 3,
        'google': 4
        }

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_oLogger = logging.getLogger(__name__ + ' modified at 27th, Mar 2022')
        self.__g_oConfig = configparser.ConfigParser()
        self._g_dictParam.update({'target_host_url':None, 'mode':None})
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sTblPrefix = None
        self.__g_sTargetUrl = None
        self.__g_sMode = None
        self.__g_dictMsg = {}
    
    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sTblPrefix = None
        self.__g_sTargetUrl = None
        self.__g_sMode = None
        self.__g_dictMsg = {}
        self.__g_oConfig = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_sTargetUrl = self._g_dictParam['target_host_url']
        self.__g_sMode = self._g_dictParam['mode']

        # begin - get Protocol message dictionary
        oSvHttp = sv_http.SvHttpCom('')
        self.__g_dictMsg = oSvHttp.getMsgDict()
        oSvHttp.close()
        del oSvHttp
        # end - get Protocol message dictionary

        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
          'nvr_ad_acct' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            return

        if self.__g_sMode is None:
            self._printDebug('you should designate mode')
            self._task_post_proc(self._g_oCallback)
            return
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']
        self.__getKeyConfig(s_sv_acct_id, s_brand_id)
        if self.__g_sTargetUrl is None:
            if 'server' in self.__g_oConfig:
                self.__g_sTargetUrl = self.__g_oConfig['server']['sv_doc_host_url']
            else:
                self._printDebug('stop -> invalid sv_doc_host_url')
                self._task_post_proc(self._g_oCallback)
                return
        
        self._printDebug('-> communication begin')
        if self.__g_sMode == 'retrieve':
            self._printDebug('-> ask new docs')
            self.__ask_sv_xe_web_service()
        elif self.__g_sMode == 'sql_transfer':
            self._printDebug('-> transfer new docs to BI DB via SQL')
            self.__transfer_new_2_bi_db()
        elif self.__g_sMode == 'extract':
            self._printDebug('-> send new docs')
            self.__send_new_2_dashboard_server()

        self._printDebug('-> communication finish')
        self._task_post_proc(self._g_oCallback)
    
    def __transfer_new_2_bi_db(self):
        """
        transfer de-normalized table to BI db
        """
        # begin - ext bi denorm word count date range
        dt_yesterday = datetime.now() - timedelta(1)
        s_yesterday = datetime.strftime(dt_yesterday, '%Y%m%d')
        dict_date_range = {'s_wc_start_date': 'na', 's_wc_end_date': s_yesterday}
        del dt_yesterday
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.sv_collect_doc_com')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
            o_sv_mysql.create_table_on_demand('_wc_word_cnt_denorm')  # for google data studio
            lst_wc_date_range = o_sv_mysql.executeQuery('getWordCountDenormDateRange')
        if lst_wc_date_range[0]['maxdate']:
            dt_maxdate = lst_wc_date_range[0]['maxdate']
            dt_startdate = dt_maxdate + timedelta(1)
            s_startdate = dt_startdate.strftime('%Y%m%d')
            if int(s_startdate) <= int(s_yesterday):
                dict_date_range['s_wc_start_date'] = s_startdate
        del lst_wc_date_range
        # end - ext bi denorm word count date range

        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.sv_collect_doc_com')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            if not self._continue_iteration():
                return
            # retrieve word count
            s_wc_end_date = datetime.strptime(dict_date_range['s_wc_end_date'], '%Y%m%d').strftime('%Y-%m-%d')
            if dict_date_range['s_wc_start_date'] == 'na':  # get whole wc
                self._printDebug('get whole wc')
                lst_word_cnt = o_sv_mysql.executeQuery('getAllWordCountTo', s_wc_end_date)
            else:
                s_wc_start_date = datetime.strptime(dict_date_range['s_wc_start_date'], '%Y%m%d').strftime('%Y-%m-%d')
                self._printDebug('wc get from ' + s_wc_start_date + ' to ' + s_wc_end_date)
                lst_word_cnt = o_sv_mysql.executeQuery('getWordCountFromTo', dict_date_range['s_wc_start_date'], s_wc_end_date)

            if len(lst_word_cnt):
                # retrieve dictionary if word count log exists
                self._printDebug('get whole dictionary')
                dict_dictionary = {}
                lst_dictionary = o_sv_mysql.executeQuery('getAllDictionaryCompact')
                for dict_single_word in lst_dictionary:
                    if not self._continue_iteration():
                        return
                    dict_dictionary[dict_single_word['word_srl']] = {'word': dict_single_word['word'],
                                                                    'b_ignore': dict_single_word['b_ignore']}
                del lst_dictionary
        nIdx = 0
        nSentinel = len(lst_word_cnt)
        if nSentinel:
            self._printDebug('transfer word count via SQL')
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.sv_collect_doc_com')
                o_sv_mysql.initialize(self._g_dictSvAcctInfo, s_ext_target_host='BI_SERVER')
                # lst_wc_date_range = o_sv_mysql.executeQuery('getWordCountDenormDateRange')
                for dict_single_wc in lst_word_cnt:
                    if not self._continue_iteration():
                        return
                    if dict_dictionary[dict_single_wc['word_srl']]['b_ignore'] == '0':
                        o_sv_mysql.executeQuery('insertWordCountDenorm', dict_single_wc['log_srl'],
                                               dict_dictionary[dict_single_wc['word_srl']]['word'],
                                               dict_single_wc['cnt'], dict_single_wc['logdate'])
                    self._printProgressBar(nIdx, nSentinel, prefix = 'transfer wc data:', suffix = 'Complete', length = 50)
                    nIdx += 1
        del lst_word_cnt
        del dict_dictionary

    def __send_new_2_dashboard_server(self):
        """
        extract manipulated dictionary and word cnt of new docs to Dashboard Server
        # case 2: Bot Server sends manipulated dictionary and word count to a dashboard server
        """
        # server give data to dashboard client case
        # bot server: may i help you?
        dictParams = {'c': [self.__g_dictMsg['MIHY']]} 
        oResp = self.__postHttpResponse(self.__g_sTargetUrl, dictParams)
        
        self._printDebug('rsp of MIHY')
        if not self._continue_iteration():
            return

        n_msg_key = oResp['variables']['a'][0]
        if self.__translateMsgCode(n_msg_key ) == 'LMKL':
            dict_date_range = oResp['variables']['d']
            self._printDebug(dict_date_range['wc_start_date'])
            if dict_date_range['wc_start_date'] == 'na':
                self._printDebug('extract whole wc')
            if dict_date_range['dictionary_start_date'] == 'na':
                self._printDebug('extract whole dictionary')
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.sv_collect_doc_com')
                o_sv_mysql.initialize(self._g_dictSvAcctInfo)
                s_end_start_date = dict_date_range['wc_end_date']
                s_wc_end_date = (datetime.strptime(s_end_start_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y-%m-%d')

                try:
                    if not self._continue_iteration():
                        return
                    s_wc_start_date = dict_date_range['wc_start_date']
                    s_wc_start_date = datetime.strptime(s_wc_start_date, '%Y%m%d').strftime('%Y-%m-%d')
                    self._printDebug('wc get from ' + s_wc_start_date + ' to ' + s_wc_end_date)
                    lst_word_cnt = o_sv_mysql.executeQuery('getWordCountFromTo', s_wc_start_date, s_wc_end_date)
                except ValueError: # if sStartDate == 'na'
                    self._printDebug('get whole wc')
                    lst_word_cnt = o_sv_mysql.executeQuery('getAllWordCountTo', s_wc_end_date)

                # retrieve dictionary
                s_dictionary_end_date = dict_date_range['dictionary_end_date']
                s_dictionary_end_date = datetime.strptime(s_dictionary_end_date, '%Y%m%d').strftime('%Y-%m-%d')
                try:
                    if not self._continue_iteration():
                        return
                    s_dictionary_start_date = dict_date_range['dictionary_start_date']
                    s_dictionary_start_date = datetime.strptime(s_dictionary_start_date, '%Y%m%d').strftime('%Y-%m-%d')
                    self._printDebug('dictionary get from ' + s_wc_start_date)
                    lst_dictionary = o_sv_mysql.executeQuery('getDictionaryFrom', s_dictionary_start_date)
                except ValueError: # if sStartDate == 'na'
                    self._printDebug('get whole dictionary')
                    lst_dictionary = o_sv_mysql.executeQuery('getAllDictionary')
        
        if not self._continue_iteration():
            return

        for dict_row in lst_word_cnt:
            dict_row['logdate'] = dict_row['logdate'].strftime("%Y%m%d%H%M%S")
        for dict_row in lst_dictionary:
            dict_row['regdate'] = dict_row['regdate'].strftime("%Y%m%d%H%M%S")

        dict_rst = {'lst_word_cnt': lst_word_cnt, 'lst_dictionary': lst_dictionary}
        del lst_word_cnt, lst_dictionary
        self._printDebug(len(dict_rst['lst_word_cnt']))
        self._printDebug(len(dict_rst['lst_dictionary']))
        dictParams = {'c': [self.__g_dictMsg['HYA']], 'd':  dict_rst}  # here you are
        oResp = self.__postHttpResponse(self.__g_sTargetUrl, dictParams)

    def __ask_sv_xe_web_service(self):
        """
        collect plain text of new docs from SvWebService
        # case 1: bot server ask new documents and comments list since last sync date to SV XE Web Service
        """
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.sv_collect_doc_com')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            lst_latest_doc_date = o_sv_mysql.executeQuery('getLatestDocumentDate')
        if not self._continue_iteration():
            return

        dt_last_sync_date = lst_latest_doc_date[0]['maxdate']
        del lst_latest_doc_date
        if dt_last_sync_date is None:
            s_begin_date_to_sync = self.__g_sFirstDateOfTheUniv
        else:
            dt_date_to_sync = dt_last_sync_date + timedelta(1)
            s_begin_date_to_sync = dt_date_to_sync.strftime("%Y%m%d")
            del dt_date_to_sync
        # s_date_to_sync = '20210707'
        dt_yesterday = datetime.today() - relativedelta(days=1)
        s_end_date_to_sync = dt_yesterday.strftime("%Y%m%d")
        if int(s_begin_date_to_sync) > int(s_end_date_to_sync):
            self._printDebug('begin_date is later than end_date')
            return

        dict_date_param = {'s_begin_date': s_begin_date_to_sync, 's_end_date': s_end_date_to_sync}
        dictParams = {'c': [self.__g_dictMsg['LMKL']], 'd':  dict_date_param}
        oResp = self.__postHttpResponse(self.__g_sTargetUrl, dictParams)
        if not self._continue_iteration():
            return

        self._printDebug('rsp of LMKL')
        nMsgKey = oResp['variables']['a'][0]
        if self.__translateMsgCode(nMsgKey) == 'ALD':
            #self._printDebug( 'in resp of add latest data' ) 
            dictRetrieveStuff = oResp['variables']['d']
            if dictRetrieveStuff['aDocSrls'] != 'na':
                # check already registered doc_srl
                lst_doc_srl = [str(doc_srl) for doc_srl in dictRetrieveStuff['aDocSrls']]
                with sv_mysql.SvMySql() as o_sv_mysql:
                    o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                    o_sv_mysql.set_app_name('svplugins.sv_collect_doc_com')
                    o_sv_mysql.initialize(self._g_dictSvAcctInfo)
                    dict_param = {'s_updated_doc_srls': ','.join(lst_doc_srl)}
                    lst_duplicated_doc_srls = o_sv_mysql.executeDynamicQuery('getOldDocumentLogByDocSrl', dict_param)
                del dict_param
                for dict_rec in lst_duplicated_doc_srls:
                    dictRetrieveStuff['aDocSrls'].remove(dict_rec['document_srl'])

                if type(dictRetrieveStuff['aDocSrls']) == list and len(dictRetrieveStuff['aDocSrls']):
                    self._printDebug('begin - doc sync')
                    dictParams = {'c': [self.__g_dictMsg['GMDL']], 'd': dictRetrieveStuff['aDocSrls']} #  give me document list  -> data: doc_srls
                    oResp = self.__postHttpResponse(self.__g_sTargetUrl, dictParams)
            #if type(dictRetrieveStuff['aComSrls']) == list and len(dictRetrieveStuff['aComSrls']):
            #    self._printDebug('begin - comment sync')
            #    dictParams = {'c': [self.__g_dictMsg['GMCL']], 'd': dictRetrieveStuff['aComSrls']} #  give me comment list  -> data: com_srls
            #    oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
            #    # self._printDebug( type(dictRetrieveStuff['aComSrls']) )
        elif self.__translateMsgCode(nMsgKey) == 'FIN': # nothing to sync
            self._printDebug('stop communication 1')
            return

        if not self._continue_iteration():
            return

        self._printDebug('rsp of ALD')
        nMsgKey = oResp['variables']['a'][0]
        if self.__translateMsgCode(nMsgKey) != 'HYA': # here you are
            self._printDebug('stop communication 2')
            return

        n_singleview_referral_code = self.__g_dictSource['singleview_estudio']
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.sv_collect_doc_com')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            for dict_single_doc in oResp['variables']['d']:
                n_sv_doc_srl = dict_single_doc['document_srl']
                s_title = dict_single_doc['title'].replace(u'\xa0', u'')
                s_content = dict_single_doc['content'].replace(u'\xa0', u'')
                dt_regdate = datetime.strptime(dict_single_doc['regdate'], '%Y%m%d%H%M%S')
                o_sv_mysql.executeQuery('insertDocumentLog', n_singleview_referral_code, n_sv_doc_srl, s_title, s_content, dt_regdate)
        return
    
    def __postHttpResponse(self, sTargetUrl, dictParams):
        dictParams['secret'] = self.__g_oConfig['basic']['sv_secret_key']
        dictParams['iv'] = self.__g_oConfig['basic']['sv_iv']
        oSvHttp = sv_http.SvHttpCom(sTargetUrl)
        oResp = oSvHttp.postUrl(dictParams)
        oSvHttp.close()
        if oResp['error'] == -1:
            sTodo = oResp['variables']['todo']
            if sTodo:
                self._printDebug('HTTP response raised exception!!')
                raise Exception(sTodo)
        else:
            return oResp

    def __translateMsgCode(self, nMsgKey):
        for sMsg, nKey in self.__g_dictMsg.items():
            if nKey == nMsgKey:
                return sMsg
        
    def __getKeyConfig(self, sSvAcctId, sAcctTitle):
        sKeyConfigPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'key.config.ini')
        try:
            with open(sKeyConfigPath) as f:
                self.__g_oConfig.read_file(f)
        except FileNotFoundError:
            self._printDebug('key.config.ini not exist')
            return  # raise Exception('stop')

        self.__g_oConfig.read(sKeyConfigPath)


if __name__ == '__main__': # for console debugging
    # CLI example -> python3.7 task.py config_loc=1/1 mode=retrieve target_host_url=https://testserver.co.kr/modules/svestudio/wcl.php
    # CLI example -> python3.7 task.py config_loc=1/1 mode=extract target_host_url=http://testserver.co.kr/extract/port/345/345371/
    # sv_collect_doc_com mode=retrieve target_host_url=https://testserver.co.kr/modules/svestudio/wcl.php
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('sv_collect_doc_com')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [mode] [target_host_url] params are required for console execution.')
