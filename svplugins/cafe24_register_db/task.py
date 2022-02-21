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
import logging
from datetime import datetime
import sys
import os
import shutil

# 3rd party library
import pandas as pd

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    sys.path.append('../../svstorage')
    import sv_mysql
    import sv_object
    import sv_plugin
    import sv_addr_parser
    # import settings
    import sv_storage
else:
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svcommon import sv_addr_parser
    # from django.conf import settings
    from svstorage import sv_storage


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_oLogger = logging.getLogger(__name__ + ' modified at 22nd, Feb 2022')
        
        self.__g_oSvStorage = sv_storage.SvStorage()
        self._g_dictParam.update({'sv_file_id':None})
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sTblPrefix = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sTblPrefix = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        
        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            return
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']

        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.cafe24_register_db')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)

        n_sv_file_id = self._g_dictParam['sv_file_id']
        self._printDebug(n_sv_file_id)

        self.__g_oSvStorage.init(s_sv_acct_id, s_brand_id)
        dict_rst_storage = self.__g_oSvStorage.validate(sv_storage.SV_STORAGE_UPLOAD)
        if dict_rst_storage['b_err']:
            self._printDebug(dict_rst_storage['s_msg'])
            self._task_post_proc(self._g_oCallback)
            del dict_rst_storage
            return
        del dict_rst_storage

        dict_rst = self.__g_oSvStorage.get_uploaded_file(n_sv_file_id)
        if dict_rst['b_err']:
            self._printDebug(dict_rst['s_msg'])
            self._task_post_proc(self._g_oCallback)
            return

        # load cafe24 order excel file
        s_uploaded_filename = dict_rst['dict_val']['s_original_filename'] + '.' + dict_rst['dict_val']['s_original_file_ext']
        self._printDebug(s_uploaded_filename + ' will be transformed')
        s_secured_filename = dict_rst['dict_val']['s_storage_path_abs']
        df = pd.read_excel(s_secured_filename, engine='openpyxl')
        # remove unnecessary column
        df.drop(['주문번호', '배송시작일', '결제일시(입금확인일)', '수령인', '수령인 상세 주소', '수령인 휴대전화',
                 '총 결제금액', '판매가', '상품별 추가할인 상세', '배송비 정보', '사용한 적립금액', '주문경로(PC/모바일)',
                 '주문자 가입일', '배송메시지', '네이버 포인트', '상품구매금액', '결제수단', '주문자명', '주문자 휴대전화'], 
                 axis=1, inplace=True)
        df = df.reset_index()  # make sure indexes pair with number of rows

        o_sv_addr_parser = sv_addr_parser.SvAddrParser()
        nIdx = 0
        nSentinel = len(df)
        with sv_mysql.SvMySql() as o_sv_mysql: # to enforce follow strict mysql connection mgmt
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.cafe24_register_db')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)

            for idx, o_row in df.iterrows():
                # for j, column in o_row.iteritems():
                #     print(column)
                o_sv_addr_parser.parse(o_row['수령인 주소(전체)'])
                dict_addr_parsed = o_sv_addr_parser.get_header()
                dt_order_date = datetime.strptime(o_row['품목별 주문번호'].split('-')[0], '%Y%m%d')
                s_item_option = None if pd.isnull(o_row['상품옵션(기본)']) else o_row['상품옵션(기본)']
                s_item_code = None if pd.isnull(o_row['자체품목코드']) else o_row['자체품목코드']
                s_cancel_type = None if pd.isnull(o_row['취소유형']) else o_row['취소유형']
                s_coupon_title = None if pd.isnull(o_row['사용한 쿠폰명']) else o_row['사용한 쿠폰명']
                s_purchaser_id = None if pd.isnull(o_row['주문자ID']) else o_row['주문자ID']
                s_purchaser_dob = None if pd.isnull(o_row['주문자 생년월일']) else o_row['주문자 생년월일']
                s_ext_order_id = None if pd.isnull(o_row['주문경로 주문번호(마켓/네이버페이 등)']) else str(int(o_row['주문경로 주문번호(마켓/네이버페이 등)']))
                o_sv_mysql.executeQuery('insertCafe24OrderLog', o_row['품목별 주문번호'], o_row['상품명(DOMESTIC)'], 
                    s_item_option, s_item_code, 
                    str(dict_addr_parsed['do']), str(dict_addr_parsed['si']),
                    str(dict_addr_parsed['gu_gun']), str(dict_addr_parsed['dong_myun_eup']),
                    s_cancel_type, o_row['실제 환불금액'], str(o_row['총 실결제금액(최초정보) (KRW)']), str(o_row['수량']), 
                    str(o_row['상품별 추가할인금액']), s_coupon_title, str(o_row['쿠폰 할인금액']), s_purchaser_id, s_purchaser_dob, 
                    o_row['주문경로'], s_ext_order_id, dt_order_date)

                self._printProgressBar(nIdx, nSentinel, prefix = 'Register DB:', suffix = 'Complete', length = 50)
                nIdx += 1
        del o_sv_addr_parser

        self._task_post_proc(self._g_oCallback)
        return


if __name__ == '__main__': # for console debugging
    # python task.py config_loc=1/1 sv_file_id=1
    nCliParams = len(sys.argv)
    if nCliParams > 2:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('cafe24_register_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [sv_file_id] params are required for console execution.')
