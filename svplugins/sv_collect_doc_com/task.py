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
import configparser # https://docs.python.org/3/library/configparser.html
import sys

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_http
    import sv_mysql
    import sv_object, sv_api_config_parser, sv_plugin
    sys.path.append('../../conf') # singleview config
    import basic_config
else: # for platform running
    from svcommon import sv_http
    from svcommon import sv_mysql
    from svcommon import sv_object, sv_api_config_parser, sv_plugin
    # singleview config
    from conf import basic_config # singleview config


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sTblPrefix = None
    __g_sFirstDateOfTheUniv = '20000101'
    __g_sTargetUrl = None
    __g_sMode = None
    __g_dictSource = {
        'singleview_estudio': 1,
		'twitter': 2,
		'naver': 3,
		'google': 4
        }
    __g_dictMsg = {}
    __g_oConfig = None

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sVersion = '0.0.1'
        self._g_sLastModifiedDate = '30th, Jun 2021'
        self._g_oLogger = logging.getLogger(__name__ + ' v'+self._g_sVersion)
        self.__g_oConfig = configparser.ConfigParser()
        self._g_dictParam.update({'target_host_url':None, 'mode':None, 'yyyymm':None})
        
    def __postHttpResponse(self, sTargetUrl, dictParams):
        dictParams['secret'] = self.__g_oConfig['basic']['sv_secret_key']
        dictParams['iv'] = self.__g_oConfig['basic']['sv_iv']
        oSvHttp = sv_http.svHttpCom(sTargetUrl)
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
        sKeyConfigPath = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', sSvAcctId, sAcctTitle, 'key.config.ini')
        try:
            with open(sKeyConfigPath) as f:
                self.__g_oConfig.read_file(f)
        except FileNotFoundError:
            self._printDebug('key.config.ini not exist')
            raise Exception('stop')

        self.__g_oConfig.read(sKeyConfigPath)

    def do_task(self):
        self.__g_sTargetUrl = self._g_dictParam['target_host_url']
        self.__g_sMode = self._g_dictParam['mode']

        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self._g_dictParam['analytical_namespace'], self._g_dictParam['config_loc'])
        oResp = oSvApiConfigParser.getConfig()

        # begin - get Protocol message dictionary
        oSvHttp = sv_http.svHttpCom('')
        self.__g_dictMsg = oSvHttp.getMsgDict()
        oSvHttp.close()
        del oSvHttp
        # end - get Protocol message dictionary

        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self._printDebug('invalid config_loc')
            raise Exception('stop')
            
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        self.__g_sTblPrefix = dict_acct_info[s_sv_acct_id]['tbl_prefix']

        self.__getKeyConfig(s_sv_acct_id, s_acct_title)
        
        self._printDebug('-> communication begin')
        if self.__g_sMode == 'retrieve':
            self._printDebug('-> ask new docs')
            self.__askNewToSvXeWebService()
        elif self.__g_sMode == 'extract':
            self._printDebug('-> send new docs')
            self.__sendNewToDashboardServer()

        self._printDebug('-> communication finish')
        return 
    
    def __sendNewToDashboardServer(self):
        """
        extract manipulated dictionary and word cnt of new docs to Dashboard Server
        # case 2: Bot Server sends manipulated dictionary and word count to a dashboard server
        """
         # server give data to dashboard client case
        # bot server: may i help you?
        dictParams = {'c': [self.__g_dictMsg['MIHY']]} 
        oResp = self.__postHttpResponse(self.__g_sTargetUrl, dictParams)
        
        self._printDebug('rsp of MIHY')
        # self._printDebug( oResp )
        n_msg_key = oResp['variables']['a'][0]
        if self.__translateMsgCode(n_msg_key ) == 'LMKL':
            # lstDateRange = None
            dict_date_range = oResp['variables']['d']
            print(dict_date_range['wc_start_date'])

            if dict_date_range['wc_start_date'] == 'na':
                print('extract whole wc')
            if dict_date_range['dictionary_start_date'] == 'na':
                print('extract whole dictionary')
            with sv_mysql.SvMySql('svplugins.sv_collect_doc_com') as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.initialize()
                lst_word_cnt = o_sv_mysql.executeQuery('getAllWordCount')
                lst_dictionary = o_sv_mysql.executeQuery('getAllDictionary')
        
        for dict_row in lst_word_cnt:
            dict_row['logdate'] = dict_row['logdate'].strftime("%Y%m%d%H%M%S")
        
        for dict_row in lst_dictionary:
            dict_row['regdate'] = dict_row['regdate'].strftime("%Y%m%d%H%M%S")

        dict_rst = {'lst_word_cnt': lst_word_cnt, 'lst_dictionary': lst_dictionary}
        del lst_word_cnt, lst_dictionary
        print(len(dict_rst['lst_word_cnt']))
        print(len(dict_rst['lst_dictionary']))
        dictParams = {'c': [self.__g_dictMsg['HYA']], 'd':  dict_rst}  # here you are
        oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )

    def __askNewToSvXeWebService(self):
        """
        collect plain text of new docs from SvWebService
        # case 1: bot server ask new documents and comments list since last sync date to SV XE Web Service
        """
        with sv_mysql.SvMySql('svplugins.sv_collect_doc_com') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.initialize()
            lst_latest_doc_date = o_sv_mysql.executeQuery('getLatestDocumentDate')
        
        dt_last_sync_date = lst_latest_doc_date[0]['maxdate']
        del lst_latest_doc_date
        if dt_last_sync_date is None:
            s_date_to_sync = self.__g_sFirstDateOfTheUniv
        else:
            dt_date_to_sync = dt_last_sync_date + timedelta(1)
            s_date_to_sync = dt_date_to_sync.strftime("%Y%m%d")
            del dt_date_to_sync
        # s_date_to_sync = '20210707'
        dictParams = {'c': [self.__g_dictMsg['LMKL']], 'd':  s_date_to_sync}  # the day after last sync date
        oResp = self.__postHttpResponse(self.__g_sTargetUrl, dictParams)
        
        self._printDebug('rsp of LMKL')
        nMsgKey = oResp['variables']['a'][0]
        if self.__translateMsgCode(nMsgKey) == 'ALD':
            #self._printDebug( 'in resp of add latest data' ) 
            dictRetrieveStuff = oResp['variables']['d']
            
            # check already registered doc_srl
            lst_doc_srl = [str(doc_srl) for doc_srl in dictRetrieveStuff['aDocSrls']]
            with sv_mysql.SvMySql('svplugins.sv_collect_doc_com') as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.initialize()
                dict_param = {'s_updated_doc_srls': ','.join(lst_doc_srl)}
                lst_duplicated_doc_srls = o_sv_mysql.executeDynamicQuery('getOldDocumentLogByDocSrl', dict_param)
            del dict_param
            for dict_rec in lst_duplicated_doc_srls:
                dictRetrieveStuff['aDocSrls'].remove(dict_rec['document_srl'])

            if type(dictRetrieveStuff['aDocSrls']) == list and len(dictRetrieveStuff['aDocSrls']):
                self._printDebug('begin - doc sync')
                dictParams = {'c': [self.__g_dictMsg['GMDL']], 'd': dictRetrieveStuff['aDocSrls']} #  give me document list  -> data: doc_srls
                oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
            
            #if type(dictRetrieveStuff['aComSrls']) == list and len(dictRetrieveStuff['aComSrls']):
            #    self._printDebug('begin - comment sync')
            #    dictParams = {'c': [self.__g_dictMsg['GMCL']], 'd': dictRetrieveStuff['aComSrls']} #  give me comment list  -> data: com_srls
            #    oResp = self.__postHttpResponse( self.__g_sTargetUrl, dictParams )
            #    # self._printDebug( type(dictRetrieveStuff['aComSrls']) )
        elif self.__translateMsgCode(nMsgKey) == 'FIN': # nothing to sync
            self._printDebug('stop communication 1')
            #raise Exception('stop')
            return

        self._printDebug('rsp of ALD')
        # self._printDebug( oResp['variables']['d'][0] )
        nMsgKey = oResp['variables']['a'][0]
        # self._printDebug(self.__translateMsgCode(nMsgKey))
        if self.__translateMsgCode(nMsgKey) != 'HYA': # here you are
            self._printDebug('stop communication 2')
            # raise Exception('stop')
            return

        n_singleview_referral_code = self.__g_dictSource['singleview_estudio']
        with sv_mysql.SvMySql('svplugins.sv_collect_doc_com') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.initialize()
            for dict_single_doc in oResp['variables']['d']:
                n_sv_doc_srl = dict_single_doc['document_srl']
                s_title = dict_single_doc['title'].replace(u'\xa0', u'')
                s_content = dict_single_doc['content'].replace(u'\xa0', u'')
                dt_regdate = datetime.strptime(dict_single_doc['regdate'], '%Y%m%d%H%M%S')
                # dt_last_update = datetime.strptime(dict_single_doc['last_update'], '%Y%m%d%H%M%S')
                o_sv_mysql.executeQuery('insertDocumentLog', n_singleview_referral_code, n_sv_doc_srl, s_title, s_content, dt_regdate )
        
        return


if __name__ == '__main__': # for console debugging
    # CLI example -> python3.7 task.py analytical_namespace=test config_loc=1/test_acct mode=retrieve target_host_url=https://testserver.co.kr/modules/svestudio/wcl.php
    # CLI example -> python3.7 task.py analytical_namespace=test config_loc=1/test_acct mode=extract target_host_url=http://testserver.co.kr/extract/port/345/345371/
    # sv_collect_doc_com mode=retrieve target_host_url=https://yuhanrox.co.kr/modules/svestudio/wcl.php
    nCliParams = len(sys.argv)
    if nCliParams > 2:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.parse_command(sys.argv)
            oJob.do_task()
            pass
    else:
        print('warning! [analytical_namesapce] [config_loc] [mode] [target_host_url] params are required for console execution.')
