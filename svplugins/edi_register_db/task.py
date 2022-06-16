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

# refer to https://github.com/google/google-api-python-client/tree/master/samples/analytics
# you firstly need to install by cmd "pip3.6 install --upgrade google-api-python-client"
# refer to https://developers.google.com/analytics/devguides/config/mgmt/v3/quickstart/installed-py
# to create desinated credential refer to https://console.developers.google.com/apis/credentials
# to get console credential you firstly need to run with the option --noauth_local_webserver 
# to monitor API traffic refer to https://console.developers.google.com/apis/api/analytics.googleapis.com/quotas?project=svgastudio

# standard library
import sys
import logging
from datetime import datetime

# 3rd party library

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    sys.path.append('../../svstorage')
    import sv_mysql
    import sv_object
    import sv_plugin
    import sv_storage
    import edi_model
    import edi_extract
    import edi_transform
else:
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svstorage import sv_storage
    from svplugins.edi_register_db import edi_model
    from svplugins.edi_register_db import edi_extract
    from svplugins.edi_register_db import edi_transform


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_oLogger = logging.getLogger(__name__ + ' modified at 16th, May 2022')
        self._g_dictParam.update({'mode':None, 'sv_file_id':None, 'new_sku_id':None,
                                  'start_yyyymmdd': None, 'end_yyyymmdd': None})
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_oSvStorage = sv_storage.SvStorage()
        self.__oEdiExtractor = None
        self.__oEdiTransformer = None
        self.__g_sMode = None
        self.__g_nSvFileId = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_oSvStorage = None
        self.__oSvMysql = None
        self.__g_sMode = None
        self.__g_nSvFileId = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_sMode = self._g_dictParam['mode']
        
        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']

        self.__g_nSvFileId = self._g_dictParam['sv_file_id']
        self.__oSvMysql = sv_mysql.SvMySql()
        self.__oSvMysql.setTablePrefix(dict_acct_info['tbl_prefix'])
        self.__oSvMysql.set_app_name('svplugins.edi_register_db')
        self.__oSvMysql.initialize(self._g_dictSvAcctInfo)

        self.__g_oSvStorage.init(s_sv_acct_id, s_brand_id)
        dict_rst_storage = self.__g_oSvStorage.validate(sv_storage.SV_STORAGE_UPLOAD)
        if dict_rst_storage['b_err']:
            self.__oSvMysql = None
            del dict_rst_storage
            self._printDebug(dict_rst_storage['s_msg'])
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        del dict_rst_storage

        dict_rst = self.__g_oSvStorage.get_uploaded_file(self.__g_nSvFileId)
        if dict_rst['b_err']:
            self.__oSvMysql = None
            self._printDebug(dict_rst['s_msg'])
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        
        # print(sv_hypermart_model.EdiDataType.QTY_AMNT.label)
        if self.__g_sMode is None:
            self.__oSvMysql = None
            self._printDebug('you should designate mode')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        s_uploaded_filename = dict_rst['dict_val']['s_original_filename'] + '.' + dict_rst['dict_val']['s_original_file_ext']
        self._printDebug(s_uploaded_filename + ' will be extracted')

        self.__oEdiExtractor = edi_extract.ExtractEdiExcel()
        self.__oEdiTransformer = edi_transform.TransformEdiDb()
        
        if self.__g_sMode == 'lookup':
            self._printDebug('-> lookup EDI file')
            self.__prepare_EDI_file(dict_rst)
        elif self.__g_sMode == 'register_sku':
            self._printDebug('-> register new skus')
            self.__register_new_sku(dict_rst)
        elif self.__g_sMode == 'register_db':
            self._printDebug('-> register into DB')
            self.__register_db(dict_rst)
        elif self.__g_sMode == 'transform_db':
            self._printDebug('-> transform DB')
            self.__transform_db()
        else:
            self._printDebug('error -> invalid mode')
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        
        self.__oEdiTransformer.clear()
        self.__oEdiExtractor.clear()
        self.__oSvMysql = None
        self._task_post_proc(self._g_oCallback)
        return

    def __transform_db(self):
        if 'start_yyyymmdd' not in self._g_dictParam:
            self._printDebug('error! invalid start_yyyymmdd')
            return
        s_start_yyyymmdd = self._g_dictParam['start_yyyymmdd']
        if s_start_yyyymmdd is None:
            self._printDebug('error! invalid start_yyyymmdd')
            return
        try:
            datetime.strptime(s_start_yyyymmdd, '%Y%m%d')
        except ValueError:
            self._printDebug('error! invalid start_yyyymmdd')
            return
        if 'end_yyyymmdd' not in self._g_dictParam:
            self._printDebug('error! invalid end_yyyymmdd')
            return
        s_end_yyyymmdd = self._g_dictParam['end_yyyymmdd']
        if s_end_yyyymmdd is None:
            self._printDebug('error! invalid end_yyyymmdd')
            return
        try:
            datetime.strptime(s_end_yyyymmdd, '%Y%m%d')
        except ValueError:
            self._printDebug('error! invalid start_yyyymmdd')
            return        
        
        dict_param = {'b_google_data_studio_edi': True,
                      's_period_start': s_start_yyyymmdd,
                      's_period_end': s_end_yyyymmdd}
        self.__oEdiTransformer.init_var(self._printDebug, self._printProgressBar, self._continue_iteration)
        self.__oEdiTransformer.initialize(self.__oSvMysql, dict_param)

    def __register_db(self, dict_rst):
        if 's_path_abs_unzip' not in dict_rst['dict_val']:
            self._printDebug('error! csv data not ready')
            return
        s_path_abs_unzip = dict_rst['dict_val']['s_path_abs_unzip']
        self.__oEdiExtractor.init_var(self._printDebug, self._printProgressBar, self._continue_iteration)
        self.__oEdiExtractor.initialize(self.__oSvMysql, s_path_abs_unzip)
        self.__oEdiExtractor.transform_csv_to_db()
        # unset unzip file
        self.__g_oSvStorage.unset_temp_file(self.__g_nSvFileId)
        # self.__g_oSvStorage.register_uploaded_file_config(n_sv_file_id, lst_edi_file_info)

    def __register_new_sku(self, dict_rst):
        if 's_path_abs_unzip' not in dict_rst['dict_val']:
            self._printDebug('error! csv data not ready')
            return
        s_path_abs_unzip = dict_rst['dict_val']['s_path_abs_unzip']
        self.__oEdiExtractor.init_var(self._printDebug, self._printProgressBar, self._continue_iteration)
        self.__oEdiExtractor.initialize(self.__oSvMysql, s_path_abs_unzip)
        if self._g_dictParam['new_sku_id'] is None:
            self._printDebug('error! new_sku_id is empty')
            return
        self.__oEdiExtractor.add_new_sku_info(self._g_dictParam['new_sku_id'])

    def __prepare_EDI_file(self, dict_rst):
        """ load EDI excel file """
        s_path_abs_unzip = None
        if dict_rst['dict_val']['s_original_file_ext'] == 'zip':  # regarding Emart
            lst_zipped_file_list = dict_rst['dict_val']['lst_zipped_files']
            dict_unzip_rst = self.__g_oSvStorage.unzip_uploaded_file(self.__g_nSvFileId)
            s_path_abs_unzip = dict_unzip_rst['dict_val']['s_path_abs_unzip']
            del dict_unzip_rst
        else:  # regarding Lmart
            s_original_filename = dict_rst['dict_val']['s_original_filename'] + '.' + dict_rst['dict_val']['s_original_file_ext']
            lst_zipped_file_list = [{'filename': s_original_filename}]
            dict_lmart_rst = self.__g_oSvStorage.reveal_act_file(self.__g_nSvFileId)
            s_path_abs_unzip = dict_lmart_rst['dict_val']['s_path_abs_unzip']
            del dict_lmart_rst
        
        lst_edi_file_info = []
        o_edi_model = edi_model.SvEdiExcel()
        for dict_single_file in lst_zipped_file_list:
            self._printDebug('-> analyzing EDI file: ' + dict_single_file['filename'])
            dict_mart_rst = o_edi_model.classify_mart(s_path_abs_unzip, dict_single_file['filename'])
            lst_edi_file_info.append({'s_filename': dict_single_file['filename'],
                                        'n_edi_data_year': dict_mart_rst['dict_val']['n_edi_data_year'],
                                        'n_hyper_mart': dict_mart_rst['dict_val']['n_hyper_mart'],
                                        'n_edi_data_type': dict_mart_rst['dict_val']['n_edi_data_type'],
                                        'status': dict_mart_rst['dict_val']['status']})
            del dict_mart_rst
        del o_edi_model

        self.__oEdiExtractor.init_var(self._printDebug, self._printProgressBar, self._continue_iteration)
        self.__oEdiExtractor.initialize(self.__oSvMysql, s_path_abs_unzip, lst_edi_file_info)
        self.__oEdiExtractor.transfer_excel_to_csv()
        dict_rst = self.__oEdiExtractor.check_new_entity()
        if len(dict_rst['dict_new_branch']):
            self._printDebug('unknown branch has been detected\nplease contact system admin')
            for s_mart_id_branch_code, s_branch_name in dict_rst['dict_new_branch'].items():
                self._printDebug(s_mart_id_branch_code+'||'+s_branch_name)
        if len(dict_rst['dict_new_sku']):
            self._printDebug('unknown SKU has been detected')
            for s_mart_id_sku_code_sku_name, s_first_detect_date in dict_rst['dict_new_sku'].items():
                self._printDebug(s_mart_id_sku_code_sku_name)


if __name__ == '__main__': # for console debugging
    # python task.py config_loc=1/1 sv_file_id=3 mode=lookup
    # python task.py config_loc=1/1 sv_file_id=3 mode=register_sku new_sku_id=8806006500375,8806006500399
    # edi_register_db sv_file_id=# mode=register_sku new_sku_id=  means register all detected SKU with deny flag
    # python task.py config_loc=1/1 sv_file_id=3 mode=register_db
    nCliParams = len(sys.argv)
    if nCliParams > 2:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('edi_register_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [sv_file_id] params are required for console execution.')
