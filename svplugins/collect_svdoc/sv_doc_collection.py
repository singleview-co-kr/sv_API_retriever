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
import sys
import logging
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta


# singleview library
if __name__ == 'sv_doc_collection': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_http
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_http


class SvDocCollection():
    __g_sFirstDateOfTheUniv = '20000101'
    
    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        # print('item:__init__')
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        self.__g_sTblPrefix = None
        self.__g_dictSource = None

        self.__g_dictMsg = {}
        self.__g_oConfig = None
        self.__g_sTargetUrl = None

        self._g_oLogger = logging.getLogger(__name__)

    def __del__(self):
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        self.__g_sTblPrefix = None

        self.__g_dictMsg = None
        self.__g_oConfig = None
        self.__g_sTargetUrl = None
        
    def init_var(self, dict_sv_acct_info, s_tbl_prefix, 
                    f_print_debug, f_print_progress_bar, f_continue_iteration, 
                    o_config, dict_source, s_target_url):
        self.__g_dictSvAcctInfo = dict_sv_acct_info
        self.__continue_iteration = f_continue_iteration
        self.__print_debug = f_print_debug
        self.__print_progress_bar = f_print_progress_bar
        self.__g_sTblPrefix = s_tbl_prefix
        self.__g_sTargetUrl = s_target_url
        self.__g_dictSource = dict_source
        self.__g_oConfig = o_config

    def collect_sv_doc(self):
        # begin - get Protocol message dictionary
        oSvHttp = sv_http.SvHttpCom('')
        self.__g_dictMsg = oSvHttp.getMsgDict()
        oSvHttp.close()
        del oSvHttp
        # end - get Protocol message dictionary

        self.__print_debug('-> communication begin')
        self.__print_debug('-> ask new docs')
        self.__ask_sv_xe_web_service()
        self.__print_debug('-> communication finish')
    
    def __ask_sv_xe_web_service(self):
        """
        collect plain text of new docs from SvWebService
        # case 1: bot server ask new documents and comments list since last sync date to SV XE Web Service
        """
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_svdoc')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            lst_latest_doc_date = o_sv_mysql.executeQuery('getLatestDocumentDate')
        if not self.__continue_iteration():
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
            self.__print_debug('begin_date is later than end_date')
            return

        dict_date_param = {'s_begin_date': s_begin_date_to_sync, 's_end_date': s_end_date_to_sync}
        dictParams = {'c': [self.__g_dictMsg['LMKL']], 'd':  dict_date_param}
        oResp = self.__post_http(self.__g_sTargetUrl, dictParams)
        if not self.__continue_iteration():
            return

        self.__print_debug('rsp of LMKL')
        nMsgKey = oResp['variables']['a'][0]
        if self.__translate_msg_code(nMsgKey) == 'ALD':
            #self.__print_debug( 'in resp of add latest data' ) 
            dictRetrieveStuff = oResp['variables']['d']
            if dictRetrieveStuff['aDocSrls'] != 'na':
                # check already registered doc_srl
                lst_doc_srl = [str(doc_srl) for doc_srl in dictRetrieveStuff['aDocSrls']]
                with sv_mysql.SvMySql() as o_sv_mysql:
                    o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                    o_sv_mysql.set_app_name('svplugins.collect_svdoc')
                    o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
                    dict_param = {'s_updated_doc_srls': ','.join(lst_doc_srl)}
                    lst_duplicated_doc_srls = o_sv_mysql.executeDynamicQuery('getOldDocumentLogByDocSrl', dict_param)
                del dict_param
                for dict_rec in lst_duplicated_doc_srls:
                    dictRetrieveStuff['aDocSrls'].remove(dict_rec['document_srl'])

                if type(dictRetrieveStuff['aDocSrls']) == list and len(dictRetrieveStuff['aDocSrls']):
                    self.__print_debug('begin - doc sync')
                    dictParams = {'c': [self.__g_dictMsg['GMDL']], 'd': dictRetrieveStuff['aDocSrls']} #  give me document list  -> data: doc_srls
                    oResp = self.__post_http(self.__g_sTargetUrl, dictParams)
            #if type(dictRetrieveStuff['aComSrls']) == list and len(dictRetrieveStuff['aComSrls']):
            #    self.__print_debug('begin - comment sync')
            #    dictParams = {'c': [self.__g_dictMsg['GMCL']], 'd': dictRetrieveStuff['aComSrls']} #  give me comment list  -> data: com_srls
            #    oResp = self.__post_http( self.__g_sTargetUrl, dictParams )
            #    # self.__print_debug( type(dictRetrieveStuff['aComSrls']) )
        elif self.__translate_msg_code(nMsgKey) == 'FIN': # nothing to sync
            self.__print_debug('stop communication 1')
            return

        if not self.__continue_iteration():
            return

        self.__print_debug('rsp of ALD')
        nMsgKey = oResp['variables']['a'][0]
        if self.__translate_msg_code(nMsgKey) != 'HYA': # here you are
            self.__print_debug('stop communication 2')
            return

        n_singleview_referral_code = self.__g_dictSource['singleview_estudio']

        n_idx = 0
        n_sentinel = len(oResp['variables']['d'])
        self.__print_debug(str(n_sentinel) + ' documents will be registered into DB.')
        if n_sentinel:
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.collect_svdoc')
                o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
                for dict_single_doc in oResp['variables']['d']:
                    if not self.__continue_iteration():
                        return

                    n_sv_doc_srl = dict_single_doc['document_srl']
                    n_sv_module_srl = dict_single_doc['module_srl']
                    s_title = dict_single_doc['title'].replace(u'\xa0', u'')
                    s_content = dict_single_doc['content'].replace(u'\xa0', u'')
                    dt_regdate = datetime.strptime(dict_single_doc['regdate'], '%Y%m%d%H%M%S')
                    o_sv_mysql.executeQuery('insertDocumentLog', n_singleview_referral_code, 
                        n_sv_doc_srl, n_sv_module_srl, s_title, s_content, dt_regdate)
                    self.__print_progress_bar(n_idx+1, n_sentinel, prefix = 'register sv documents:', suffix = 'Complete', length = 50)
                    n_idx += 1

        return
    
    def __post_http(self, sTargetUrl, dictParams):
        dictParams['secret'] = self.__g_oConfig['basic']['sv_secret_key']
        dictParams['iv'] = self.__g_oConfig['basic']['sv_iv']
        oSvHttp = sv_http.SvHttpCom(sTargetUrl)
        oResp = oSvHttp.postUrl(dictParams)
        oSvHttp.close()
        if oResp['error'] == -1:
            sTodo = oResp['variables']['todo']
            if sTodo:
                self.__print_debug('HTTP response raised exception!!')
                raise Exception(sTodo)
        else:
            return oResp

    def __translate_msg_code(self, nMsgKey):
        for sMsg, nKey in self.__g_dictMsg.items():
            if nKey == nMsgKey:
                return sMsg
