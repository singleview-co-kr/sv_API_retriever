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
import os
import sys
import string
import random
import zipfile
import json
import shutil
# import logging

# 3rd party library
from decouple import config  # https://pypi.org/project/python-decouple/

# singleview library
if __name__ == 'sv_storage': # for console debugging
    sys.path.append('../../svdjango')
    sys.path.append('../../svcommon')
    import settings
    import sv_mysql
else:
    from django.conf import settings
    from svcommon import sv_mysql

SV_STORAGE_API = 'api'
SV_STORAGE_UPLOAD = 'upload'


class SvStorage():
    __g_sAbsRootPath = None
    # __g_oLogger = None
    __g_nSecuredLength = 10
    __g_sFileConfigPostfix = '_config_json'
    __g_sUnzipFilePostfix = '_unzip'
    __g_lstStorageType = [SV_STORAGE_API, SV_STORAGE_UPLOAD]
    __g_lstAllowedFileExt = ['xls', 'xlsx', 'csv', 'zip',
                             'png'  # for test
                            ]

    def __init__(self):
        # self.__g_oLogger = logging.getLogger(__name__ + ' modified at 1st, Feb 2022')
        self.__g_sAbsRootPath = config('ABSOLUTE_PATH_BOT')
        self.__g_oSvMysql = sv_mysql.SvMySql()
        self.__g_dictSvAcctInfo = None
    
    def init(self, s_sv_acct_id, s_brand_id):
        self.__g_dictSvAcctInfo = {'s_sv_acct_id': s_sv_acct_id, 's_brand_id': s_brand_id}  # can't recognize attr if move to __init__
        self.__g_oSvMysql.set_tbl_prefix(s_sv_acct_id+'_'+s_brand_id)
        self.__g_oSvMysql.set_app_name('svstorage.sv_storage')
        self.__g_oSvMysql.initialize({'n_acct_id':int(s_sv_acct_id), 'n_brand_id': int(s_brand_id)})

    def validate(self, s_type):
        """ set the brand designated directory """
        dict_rst = {'b_err': False, 's_msg': None}
        if s_type not in self.__g_lstStorageType:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'invalid storage type'
            return dict_rst

        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        s_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_type, s_acct_id, s_brand_id)
        if os.path.isdir(s_path_abs) == False:
            os.makedirs(s_path_abs)
        return dict_rst

    def register_uploaded_file(self, n_request_user_id, s_file_name, o_uploading_file):
        """ add new file into a storage """
        dict_rst = {'b_err': False, 's_msg': None, 'dict_val': None}
        #############################
        # need for quota feature
        #############################
        lst_file_info = s_file_name.split('.')
        if lst_file_info[-1] not in self.__g_lstAllowedFileExt:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'invalid file type'
            return dict_rst

        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        s_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, s_acct_id, s_brand_id)
        if os.path.isdir(s_path_abs) == False:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'plz validate storage first before file registration'
            return dict_rst
        s_original_file_name = '.'.join(lst_file_info[:-1])
        s_file_ext = lst_file_info[-1]
        s_secured_file_name = self.__get_unique_filename()
        # begin - file write
        s_file_path_abs = os.path.join(s_path_abs, s_secured_file_name)
        o_dest = open(s_file_path_abs, 'wb+')
        for chunk in o_uploading_file.chunks():
            o_dest.write(chunk)
        o_dest.close()
        del o_dest
        # end - file write
        del lst_file_info
        # begin - file registration into db
        self.__g_oSvMysql.execute_query('insertUploadedFile', n_request_user_id,
                                        s_original_file_name, s_file_ext, s_secured_file_name, '')
        # end - file registration into db
        dict_rst['b_err'] = False
        dict_rst['s_msg'] = None
        dict_rst['dict_val'] = {'s_original_file_name': s_original_file_name, 's_file_ext': s_file_ext,
                                's_secured_file_name': s_secured_file_name}
        return dict_rst

    def register_uploaded_file_config(self, n_file_id, o_config):
        dict_rst = {'b_err': False, 's_msg': None, 'dict_val': None}
        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        s_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, s_acct_id, s_brand_id)
        dict_req_file = self.__get_file_info_by_id(n_file_id)
        if os.path.isdir(s_path_abs) == False:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'plz validate storage first before get file'
            del dict_req_file
            return dict_rst
        s_path_abs = os.path.join(s_path_abs, dict_req_file['secured_filename'])
        if not os.path.exists(s_path_abs) and not os.path.isfile(s_path_abs):
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'invalidate req file'
            del dict_req_file
            return dict_rst        
        with open(s_path_abs + self.__g_sFileConfigPostfix, 'w', encoding='utf-8') as o_config_file:
            json.dump(o_config, o_config_file, ensure_ascii=False, indent='\t')
        # print(json.dumps(o_config, ensure_ascii=False, indent='\t'))

    def get_uploaded_file_all(self):
        return self.__g_oSvMysql.execute_query('getUploadedFileAll')

    def get_uploaded_file(self, n_file_id):
        # access right control - n_user_id, b_admin, 
        dict_rst = {'b_err': False, 's_msg': None, 'dict_val': None}
        if not n_file_id.isdigit():
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'invalid sv file id'
            return dict_rst
        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        dict_req_file = self.__get_file_info_by_id(n_file_id)
        s_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, s_acct_id, s_brand_id)
        if os.path.isdir(s_path_abs) == False:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'plz validate storage first before get file'
            del dict_req_file
            return dict_rst
        if dict_req_file['secured_filename'] is None:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'invalid file request'
            return dict_rst
        s_path_abs = os.path.join(s_path_abs, dict_req_file['secured_filename'])
        if os.path.exists(s_path_abs) and os.path.isfile(s_path_abs):
            dict_rst['b_err'] = False
            dict_rst['s_msg'] = None
            dict_rst['dict_val'] = {'s_storage_path_abs': s_path_abs, 
                                    's_original_filename': dict_req_file['source_filename'],
                                    's_original_file_ext': dict_req_file['file_ext']}
        else:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'invalid file request'
            dict_rst['dict_val'] = None
        
        # https://code.tutsplus.com/ko/tutorials/compressing-and-extracting-files-in-python--cms-26816
        if dict_req_file['file_ext'] == 'zip':
            lst_zipped_file_list = []
            o_zip_file = zipfile.ZipFile(s_path_abs)
            for o_single_file in o_zip_file.namelist():
                lst_zipped_file_list.append({
                    'filename': o_zip_file.getinfo(o_single_file).filename,
                    'file_size': o_zip_file.getinfo(o_single_file).file_size})
                    # o_zip_file.extract('file_name.xls', 'C:\\Stories\\Short\\Funny')
            o_zip_file.close()
            del o_zip_file
            dict_rst['dict_val']['lst_zipped_files'] = lst_zipped_file_list
        
        s_path_abs_unzip = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, 
                                s_acct_id, s_brand_id, dict_req_file['secured_filename'] + self.__g_sUnzipFilePostfix)
        if os.path.isdir(s_path_abs_unzip):
            dict_rst['dict_val']['s_path_abs_unzip'] = s_path_abs_unzip

        s_config_json = s_path_abs + self.__g_sFileConfigPostfix
        if os.path.exists(s_config_json) and os.path.isfile(s_config_json):
            o_json_file = open(s_config_json, 'r')
            s_json_file = o_json_file.read()
            dict_rst['dict_val']['o_config'] = json.loads(s_json_file)
            o_json_file.close()
        else:
            dict_rst['dict_val']['o_config'] = None
        del dict_req_file
        return dict_rst

    def unzip_uploaded_file(self, n_file_id):  
        dict_rst = {'b_err': False, 's_msg': None, 'dict_val': None}
        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        dict_req_file = self.__get_file_info_by_id(n_file_id)
        s_secured_file = dict_req_file['secured_filename']
        s_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, 
                                    s_acct_id, s_brand_id, s_secured_file)
        s_path_abs_unzip = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, 
                                    s_acct_id, s_brand_id, s_secured_file + self.__g_sUnzipFilePostfix)
        if os.path.isdir(s_path_abs_unzip) == False:
            os.makedirs(s_path_abs_unzip)

        o_zip = zipfile.ZipFile(s_path_abs)
        o_zip.extractall(s_path_abs_unzip)
        o_zip.close()
        del o_zip
        dict_rst['dict_val'] = {'s_path_abs_unzip': s_path_abs_unzip}
        return dict_rst
    
    def reveal_act_file(self, n_file_id):
        dict_rst = {'b_err': False, 's_msg': None, 'dict_val': None}
        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        dict_req_file = self.__get_file_info_by_id(n_file_id)
        s_secured_file = dict_req_file['secured_filename']
        s_path_secured_file_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, 
                                                s_acct_id, s_brand_id, s_secured_file)
        s_path_abs_unzip = s_path_secured_file_abs + self.__g_sUnzipFilePostfix
        if os.path.isdir(s_path_abs_unzip) == False:
            os.makedirs(s_path_abs_unzip)
        s_filename = dict_req_file['source_filename'] + '.' + dict_req_file['file_ext']
        s_path_abs_act_file = os.path.join(s_path_abs_unzip, s_filename)
        if os.path.isfile(s_path_abs_unzip) == False:
            shutil.copyfile(s_path_secured_file_abs, s_path_abs_act_file)
        dict_rst['dict_val'] = {'s_path_abs_unzip': s_path_abs_unzip}
        return dict_rst

    def withdraw_uploaded_file(self, n_user_id, b_admin, n_file_id):
        """ unset uploaded file if authenticated """
        dict_rst = {'b_err': False, 's_msg': None}
        dict_req_file = self.__get_file_info_by_id(n_file_id)
        if dict_req_file['owner_id'] != n_user_id:
            if not b_admin:
                dict_rst['b_err'] = True
                dict_rst['s_msg'] = 'you don\'t own the file you request.'
                del dict_req_file
                return dict_rst
                
        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        s_removing_file_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, 
                                                s_acct_id, s_brand_id, dict_req_file['secured_filename'])
        if os.path.isfile(s_removing_file_path_abs):
            os.remove(s_removing_file_path_abs)
            self.__g_oSvMysql.execute_query('updateUploadedFileDeletedById', n_file_id)
            dict_rst['b_err'] = False
            dict_rst['s_msg'] = None
        else:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'a requested file does not exist'
        return dict_rst

    def unset_temp_file(self, n_file_id):
        """ unset temp file if existed
            temp files are unzipped, converted ones """
        dict_rst = {'b_err': False, 's_msg': None}
        dict_req_file = self.__get_file_info_by_id(n_file_id)
        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        s_removing_file_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, 
                                                s_acct_id, s_brand_id, dict_req_file['secured_filename'])
        s_secured_file = dict_req_file['secured_filename']
        s_path_abs_unzip = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, 
                                    s_acct_id, s_brand_id, s_secured_file + self.__g_sUnzipFilePostfix)
        if os.path.isdir(s_path_abs_unzip):
            try:
                shutil.rmtree(s_path_abs_unzip)
            except OSError as e:
                dict_rst['b_err'] = True
                dict_rst['s_msg'] = "Error: %s : %s" % (s_path_abs_unzip, e.strerror)
        else:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'a requested temp file does not exist'
        return dict_rst

    def __get_file_info_by_id(self, n_file_id):
        lst_req_file = self.__g_oSvMysql.execute_query('getUploadedFileById', n_file_id)
        if len(lst_req_file):
            return lst_req_file[0]
        else:
            return {'id': None, 'owner_id': None, 'source_filename': None, 'file_ext': None, 'secured_filename': None}
        

    def __get_unique_filename(self):
        # securing file name
        # https://stackoverflow.com/questions/28007770/how-to-to-make-a-file-private-by-securing-the-url-that-only-authenticated-users
        # https://stackoverflow.com/questions/28166784/restricting-access-to-private-file-downloads-in-django
        # https://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits/23728630#23728630
        return ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(self.__g_nSecuredLength))
