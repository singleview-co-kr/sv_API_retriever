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
    import sv_addr_parser
    import settings
else: # for platform running
    from svcommon import sv_http
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svcommon import sv_addr_parser
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sFirstDateOfTheUniv = '20000101'
    __g_lstAllowedCollectionBase = ['date', 'document_srl']

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230605)')
        
        self.__g_oConfig = configparser.ConfigParser()
        self._g_dictParam.update({'target_host_url': None, 'mode': None})
        # Declaring a dict outside __init__ is declaring a class-level variable.
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

        self.__g_lstModuleSrl = None
        self.__g_lstAddrFieldTitle = None
        self.__g_lstCollectionBase = None

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

        if self.__g_sMode == 'awacs':  # retrieve cherry picker 
            self.__retrieve_chery_picker()
        else:
            dict_rst = self.__validate_configuration()
            if dict_rst['b_err']:
                self._print_debug(dict_rst['s_msg'])
                self._task_post_proc(self._g_oCallback)
                if self._g_bDaemonEnv:  # for running on dbs.py only
                    raise Exception('remove')
                else:
                    return
            self._print_debug('-> communication begin')
            self._print_debug('-> ask new adr')
            n_idx = len(self.__g_lstModuleSrl)
            for i in range(0, n_idx):
                self.__ask_sv_xe_seb_service(self.__g_lstCollectionBase[i], 
                                                self.__g_lstModuleSrl[i], self.__g_lstAddrFieldTitle[i])
            self._print_debug('-> communication finish')

        self._task_post_proc(self._g_oCallback)

    def __retrieve_chery_picker(self):
        n_module_srl = 162
        self._print_debug('-> __retrieve_chery_picker begin')
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_svadr')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            lst_latest_doc_srl = o_sv_mysql.execute_query('getAllDocuments', n_module_srl)
        if not self._continue_iteration():
            return
        # import time
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_svadr')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
           
            for dict_row in lst_latest_doc_srl:
                lst_close_doc_list = o_sv_mysql.execute_query('getCloseDocumentsByPostcode', n_module_srl,
                                                              dict_row['postcode'], dict_row['document_srl'])
                
                if len(lst_close_doc_list):
                    print('document_srl: ' + str(dict_row['document_srl']) + ' @ ' + str(dict_row['logdate']))
                    print(dict_row['addr_raw'] + ' close addr list')
                    print('================================================')
                    for dict_single_addr in lst_close_doc_list:
                        print(dict_single_addr['addr_raw'] + ' * ' + str(dict_single_addr['document_srl'])  + ' * ' + str(dict_single_addr['logdate']))
                    print('================================================')
                    print()
                    print()
                # time.sleep(3)

        del lst_latest_doc_srl

    def __ask_sv_xe_seb_service(self, s_collection_base, n_module_srl, s_addr_field_title):
        """
        collect plain text of new docs from SvWebService based on logdate 
        # case 1: bot server ask new documents and comments list since last sync date to SV XE Web Service
        """
        dict_date_param = {'s_collection_base': None, 
                            'n_module_srl': n_module_srl, 's_addr_field_title': s_addr_field_title,
                            's_begin_date': None, 's_end_date': None,
                            'n_last_doc_srl': None}

        if 'date' == s_collection_base:
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.collect_svadr')
                o_sv_mysql.initialize(self._g_dictSvAcctInfo)
                lst_latest_doc_date = o_sv_mysql.execute_query('getLatestAdrDate', n_module_srl)
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
            dt_yesterday = datetime.today() - relativedelta(days=1)
            s_end_date_to_sync = dt_yesterday.strftime("%Y%m%d")
            if int(s_begin_date_to_sync) > int(s_end_date_to_sync):
                self._print_debug('begin_date is later than end_date')
                return
            dict_date_param['s_collection_base'] = s_collection_base
            dict_date_param['s_begin_date'] = s_begin_date_to_sync
            dict_date_param['s_end_date'] = s_end_date_to_sync
        elif 'document_srl' == s_collection_base:
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.collect_svadr')
                o_sv_mysql.initialize(self._g_dictSvAcctInfo)
                lst_latest_doc_srl = o_sv_mysql.execute_query('getLatestDocumentSrl', n_module_srl)
            if not self._continue_iteration():
                return

            n_last_sync_doc_srl = lst_latest_doc_srl[0]['max_doc_srl']
            del lst_latest_doc_srl
            if n_last_sync_doc_srl is None:
                n_last_sync_doc_srl = 0
            # print(n_last_sync_doc_srl)
            dict_date_param['s_collection_base'] = s_collection_base
            dict_date_param['n_last_doc_srl'] = n_last_sync_doc_srl

        if dict_date_param['s_collection_base'] is None:
            self._print_debug('stop communication - collection_base is weird')
            return
        
        # print(dict_date_param)
        # return
        dict_params = {'c': [self.__g_dictMsg['LMKL']], 'd':  dict_date_param}
        dict_rsp = self.__post_http(self.__g_sTargetUrl, dict_params)
        if not self._continue_iteration():
            return
        
        self._print_debug('rsp of LMKL')
        nMsgKey = dict_rsp['variables']['a'][0]
        if self.__translate_msg_code(nMsgKey) == 'ERR':
            self._print_debug('stop communication - ' + dict_rsp['variables']['d'])
            return

        if self.__translate_msg_code(nMsgKey) == 'ALD':
            dictRetrieveStuff = dict_rsp['variables']['d']
            if dictRetrieveStuff['aDocSrls'] != 'na':
                # check already registered doc_srl
                lst_doc_srl = [str(doc_srl) for doc_srl in dictRetrieveStuff['aDocSrls']]
                with sv_mysql.SvMySql() as o_sv_mysql:
                    o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
                    o_sv_mysql.set_app_name('svplugins.collect_svadr')
                    o_sv_mysql.initialize(self._g_dictSvAcctInfo)
                    dict_param = {'s_updated_doc_srls': ','.join(lst_doc_srl)}
                    lst_duplicated_doc_srls = o_sv_mysql.execute_dynamic_query('getOldDocumentLogByDocSrl', dict_param)
                del dict_param
                for dict_rec in lst_duplicated_doc_srls:
                    dictRetrieveStuff['aDocSrls'].remove(dict_rec['document_srl'])

                if type(dictRetrieveStuff['aDocSrls']) == list and len(dictRetrieveStuff['aDocSrls']):
                    self._print_debug('begin - doc sync')
                    #  give me document list  -> data: doc_srls
                    dict_params_tmp = {'s_collection_base': 'date', 
                                        'n_module_srl': n_module_srl, 's_addr_field_title': s_addr_field_title,
                                        'a_doc_srl': dictRetrieveStuff['aDocSrls']}
                    dict_params = {'c': [self.__g_dictMsg['GMDL']], 'd': dict_params_tmp}
                    dict_rsp = self.__post_http(self.__g_sTargetUrl, dict_params)
                    del dict_params
                    del dict_params_tmp
            
        elif self.__translate_msg_code(nMsgKey) == 'FIN': # nothing to sync
            self._print_debug('stop communication 1')
            return

        if not self._continue_iteration():
            return

        self._print_debug('rsp of ALD')
        nMsgKey = dict_rsp['variables']['a'][0]
        if self.__translate_msg_code(nMsgKey) == 'ERR':
            self._print_debug('stop communication - ' + dict_rsp['variables']['d'])
            return

        if self.__translate_msg_code(nMsgKey) != 'HYA': # here you are
            self._print_debug('stop communication 2')
            return

        o_sv_addr_parser = sv_addr_parser.SvAddrParser()
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_svadr')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            for dict_single_doc in dict_rsp['variables']['d']:
                n_sv_doc_srl = dict_single_doc['document_srl']
                n_sv_module_srl = dict_single_doc['module_srl']
                s_addr = dict_single_doc['adr']
                lst_addr = s_addr.split('|@|')
                s_postcode = 'None'
                if lst_addr[0].isdigit():  # catch postcode
                    s_postcode = lst_addr[0]
                    del lst_addr[0]
                o_sv_addr_parser.parse(' '.join(lst_addr))
                dict_addr_parsed = o_sv_addr_parser.get_header()
                # print(dict_addr_parsed)
                dt_regdate = datetime.strptime(dict_single_doc['regdate'], '%Y%m%d%H%M%S')
                if dict_addr_parsed['do'] or dict_addr_parsed['si'] or \
                   dict_addr_parsed['gu_gun'] or dict_addr_parsed['dong_myun_eup']:
                    o_sv_mysql.execute_query('insertAdrLog', n_sv_doc_srl, n_sv_module_srl, s_postcode,
                                             str(dict_addr_parsed['do']), str(dict_addr_parsed['si']),
                                             str(dict_addr_parsed['gu_gun']), str(dict_addr_parsed['dong_myun_eup']),
                                             s_addr, dt_regdate)
                del dict_addr_parsed
        del o_sv_addr_parser
        return
    
    def __post_http(self, s_target_url, dict_params):
        dict_params['secret'] = self.__g_oConfig['basic']['sv_secret_key']
        dict_params['iv'] = self.__g_oConfig['basic']['sv_iv']
        o_sv_http = sv_http.SvHttpCom(s_target_url)
        dict_rsp = o_sv_http.post_url(dict_params)
        o_sv_http.close()
        del o_sv_http
        if dict_rsp['error'] == -1:
            s_todo = dict_rsp['variables']['todo']
            if s_todo:
                self._print_debug('HTTP response raised exception!!')
                raise Exception(s_todo)
        else:
            return dict_rsp

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
            self._print_debug('key.config.ini not exist')
            return  # raise Exception('stop')

        self.__g_oConfig.read(sKeyConfigPath)
    
    def __validate_configuration(self):
        dict_rst = {'b_err': False, 's_msg': None}
        if self.__g_sTargetUrl is None:
            if 'server' in self.__g_oConfig:
                self.__g_sTargetUrl = self.__g_oConfig['sv_adr_server']['sv_adr_host_url']
            else:
                dict_rst['b_err'] = True
                dict_rst['s_msg'] = 'stop -> invalid sv_adr_host_url'
                return dict_rst
        
        self.__g_lstModuleSrl = self.__g_oConfig['sv_adr_server']['module_srl'].split(',')
        self.__g_lstAddrFieldTitle = self.__g_oConfig['sv_adr_server']['addr_field_title'].split(',')
        self.__g_lstCollectionBase = self.__g_oConfig['sv_adr_server']['collection_base'].split(',')
        if len(self.__g_lstModuleSrl) != len(self.__g_lstAddrFieldTitle) and \
                len(self.__g_lstAddrFieldTitle) != len(self.__g_lstCollectionBase):
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'stop -> configuration pair not matched'
            return dict_rst
        
        for s_collection_base in self.__g_lstCollectionBase:
            if s_collection_base not in self.__g_lstAllowedCollectionBase:
                dict_rst['b_err'] = True
                dict_rst['s_msg'] = 'stop -> invalid collection base'
                return dict_rst
        return dict_rst



if __name__ == '__main__': # for console debugging
    # CLI example -> python3.7 task.py config_loc=1/1 target_host_url=https://testserver.co.kr/modules/svestudio/adr.php
    # collect_svadr target_host_url=https://testserver.co.kr/modules/svestudio/adr.php
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('collect_svadr')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [target_host_url] params are required for console execution.')
