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
# import logging
import string
import random

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
    __g_lstStorageType = [SV_STORAGE_API, SV_STORAGE_UPLOAD]
    __g_lstAllowedFileExt = ['xls', 'xlsx', 'csv', 'zip',
                             'png'  # for test
                            ]

    def __init__(self):
        # self.__g_oLogger = logging.getLogger(__name__ + ' modified at 1st, Feb 2022')
        self.__g_sAbsRootPath = config('ABSOLUTE_PATH_BOT')
        self.__g_oSvMysql = sv_mysql.SvMySql()
        self.__g_dictSvAcctInfo = None
        self.__g_dictRst = {'b_err': False, 's_msg': None, 'dict_val': None}
        
    
    def init(self, s_sv_acct_id, s_brand_id):
        self.__g_dictSvAcctInfo = {'s_sv_acct_id': s_sv_acct_id, 's_brand_id': s_brand_id}  # can't recognize attr if move to __init__
        self.__g_oSvMysql.setTablePrefix(s_sv_acct_id+'_'+s_brand_id)
        self.__g_oSvMysql.set_app_name(__name__)
        self.__g_oSvMysql.initialize({'n_acct_id':int(s_sv_acct_id), 'n_brand_id': int(s_brand_id)})

    def validate(self, s_type):
        """ set the brand designated directory """
        if s_type not in self.__g_lstStorageType:
            self.__g_dictRst['b_err'] = True
            self.__g_dictRst['s_msg'] = 'invalid storage type'
            return self.__g_dictRst

        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        s_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_type, s_acct_id, s_brand_id)
        if os.path.isdir(s_path_abs) == False:
            os.makedirs(s_path_abs)
        self.__g_dictRst['b_err'] = False
        self.__g_dictRst['s_msg'] = None
        self.__g_dictRst['dict_val'] = None
        return self.__g_dictRst
    
    def register_uploaded_file(self, n_request_user_id, s_file_name, o_file):
        """ add new file into a storage """
        #############################
        # need for quota feature
        #############################
        lst_file_info = s_file_name.split('.')
        if lst_file_info[-1] not in self.__g_lstAllowedFileExt:
            self.__g_dictRst['b_err'] = True
            self.__g_dictRst['s_msg'] = 'invalid file type'
            return self.__g_dictRst

        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        s_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, s_acct_id, s_brand_id)
        if os.path.isdir(s_path_abs) == False:
            self.__g_dictRst['b_err'] = True
            self.__g_dictRst['s_msg'] = 'plz validate storage first before file registration'
            return self.__g_dictRst
        s_original_file_name = '.'.join(lst_file_info[:-1])
        s_file_ext = lst_file_info[-1]
        s_secured_file_name = self.__get_unique_file_name()
        # begin - file write
        s_file_path_abs = os.path.join(s_path_abs, s_secured_file_name)
        o_dest = open(s_file_path_abs, 'wb+')
        for chunk in o_file.chunks():
            o_dest.write(chunk)
        o_dest.close()
        del o_dest
        # end - file write
        del lst_file_info

        # begin - file registration into db
        self.__g_oSvMysql.executeQuery('insertUploadedFile', n_request_user_id, 
                                        s_original_file_name, s_file_ext, s_secured_file_name, '')
        # end - file registration into db

        self.__g_dictRst['b_err'] = False
        self.__g_dictRst['s_msg'] = None
        self.__g_dictRst['dict_val'] = {'s_original_file_name': s_original_file_name,
                                        's_file_ext': s_file_ext,
                                        's_secured_file_name': s_secured_file_name}
        return self.__g_dictRst

    def get_uploaded_file_all(self):
        return self.__g_oSvMysql.executeQuery('getUploadedFileAll')

    def get_uploaded_file(self, n_file_id):  
        # access right control - n_user_id, b_admin, 
        dict_rst = {'b_err': False, 's_msg': None, 'dict_val': None}

        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        lst_req_file = self.__g_oSvMysql.executeQuery('getUploadedFileById', n_file_id)  #kwargs['n_file_id'])
        # if lst_req_file[0]['owner_id'] != n_user_id:
        #     if not b_admin:
        #         dict_rst['b_err'] = True
        #         dict_rst['s_msg'] = 'you don\'t own the file you request.'
        #         dict_rst['dict_val'] = None
        #         del lst_req_file
        #         return dict_rst
        s_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, s_acct_id, s_brand_id)
        if os.path.isdir(s_path_abs) == False:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'plz validate storage first before get file'
            del lst_req_file
            return self.__g_dictRst
        s_path_abs = os.path.join(s_path_abs, lst_req_file[0]['secured_filename'])
        if os.path.exists(s_path_abs) and os.path.isfile(s_path_abs):
            dict_rst['b_err'] = False
            dict_rst['s_msg'] = None
            dict_rst['dict_val'] = {'s_storage_path_abs': s_path_abs, 
                                    's_original_filename': lst_req_file[0]['source_filename'],
                                    's_original_file_ext': lst_req_file[0]['file_ext']}
        else:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'invalid file request'
            dict_rst['dict_val'] = None
        del lst_req_file
        return dict_rst

    def withdraw_uploaded_file(self, n_user_id, b_admin, n_file_id):
        dict_rst = {'b_err': False, 's_msg': None}

        lst_req_file = self.__g_oSvMysql.executeQuery('getUploadedFileById', n_file_id)
        if lst_req_file[0]['owner_id'] != n_user_id:
            if not b_admin:
                dict_rst['b_err'] = True
                dict_rst['s_msg'] = 'you don\'t own the file you request.'
                del lst_req_file
                return dict_rst
                
        s_acct_id = self.__g_dictSvAcctInfo['s_sv_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['s_brand_id']
        s_removing_file_path_abs = os.path.join(self.__g_sAbsRootPath, settings.SV_STORAGE_ROOT, SV_STORAGE_UPLOAD, 
                                                s_acct_id, s_brand_id, lst_req_file[0]['secured_filename'])
        if os.path.isfile(s_removing_file_path_abs):
            os.remove(s_removing_file_path_abs)
            self.__g_oSvMysql.executeQuery('updateUploadedFileDeletedById', n_file_id)
            dict_rst['b_err'] = False
            dict_rst['s_msg'] = None
        else:
            dict_rst['b_err'] = True
            dict_rst['s_msg'] = 'a requested file does not exist'
        return dict_rst

    def __get_unique_file_name(self):
        # securing file view
        # https://stackoverflow.com/questions/28007770/how-to-to-make-a-file-private-by-securing-the-url-that-only-authenticated-users
        # https://stackoverflow.com/questions/28166784/restricting-access-to-private-file-downloads-in-django
        # https://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits/23728630#23728630
        return ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(self.__g_nSecuredLength))
