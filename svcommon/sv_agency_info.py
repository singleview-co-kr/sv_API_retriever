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
import re
import sys
import csv
import logging
from datetime import datetime
from datetime import timedelta

# 3rd party library

# singleview config
if __name__ == 'svcommon.sv_agency_info': # for websocket running
    from svcommon import sv_object
    from django.conf import settings
elif __name__ == 'sv_agency_info': # for plugin console debugging
    sys.path.append('../../svdjango')
    import sv_object
    import settings
elif __name__ == '__main__': # for class console debugging
    pass


class SvAgencyInfo(sv_object.ISvObject):
    __g_sBirthOfUniverse = '20010101' # define default ancient begin date
    __g_sAgencyFeeTypeBackmargin = 'back'
    __g_sAgencyFeeTypeMarkup = 'markup'
    __g_sAgencyFeeTypeDirect = 'direct'
    __g_sSvNull = '#%'
    __g_oRegEx = re.compile(r"\d+%$") # pattern ex) 2% 23%

    def __init__(self):
        self._g_oLogger = logging.getLogger(__file__)
        self.__sTodayYyyymmdd = datetime.today().strftime('%Y%m%d')
        self.__sAgencyInfoPath = None
        self.__lstAgencyInfo = []
        self.__dictAgencyInfo = {}

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __del__(self):
        pass
    
    def load_agency_info_by_source_id(self, s_acct_pk, s_brand_pk, s_data_source, s_data_source_id):
        s_cur_agency_info_id = str(s_data_source_id) + '-' + s_data_source
        if self.__dictAgencyInfo.get(s_cur_agency_info_id, self.__g_sSvNull) != self.__g_sSvNull:  # if s_agency_info_id exists
            self.__lstAgencyInfo = self.__dictAgencyInfo[s_cur_agency_info_id]
        else:  # if new s_agency_info_id requested
            self.__sAgencyInfoPath = os.path.join(settings.SV_STORAGE_ROOT, s_acct_pk, s_brand_pk, s_data_source, s_data_source_id, 'conf', 'agency_info.tsv')
            lst_temp = []
            try:
                with open(self.__sAgencyInfoPath, 'r') as o_tsv_file:
                    o_tsv_reader = csv.reader(o_tsv_file, delimiter='\t')
                    for lst_row in o_tsv_reader:
                        lst_temp.append(lst_row)
            except FileNotFoundError:
                self._printDebug(self.__sAgencyInfoPath + ' does not exist')
                return False
            self.__lstAgencyInfo = lst_temp
            self.__dictAgencyInfo[s_cur_agency_info_id] = lst_temp
        return True

    def get_agency_fee_type(self):
        return [self.__g_sAgencyFeeTypeBackmargin, self.__g_sAgencyFeeTypeMarkup, self.__g_sAgencyFeeTypeDirect]

    def get_latest_agency_info_dict(self):
        dict_agency_info = {
                's_begin_date': '',
                's_end_date': '',
                's_agency_name': '',
                'n_fee_rate': 0,
                's_fee_type': ''
            }
        if len(self.__lstAgencyInfo):
            lst_latest = self.__lstAgencyInfo.pop()
            lst_period = lst_latest[0].split('-')
            dict_agency_info['s_begin_date'] = lst_period[0]
            if(len(lst_period)==2):
                dict_agency_info['s_end_date'] = lst_period[1]
            dict_agency_info['s_agency_name'] = lst_latest[1]
            dict_agency_info['n_fee_rate'] = int(lst_latest[2].rstrip('%'))
            dict_agency_info['s_fee_type'] = lst_latest[3]
        return dict_agency_info

    def set_agency_info(self, s_begin_date, s_agency_name, n_fee_percent, s_fee_type):
        """ """
        dt_today = datetime.today()
        n_row_cnt = len(self.__lstAgencyInfo)
        if n_row_cnt:
            dt_yesterday = dt_today - timedelta(1)
            s_yesterday = str(dt_yesterday.strftime('%Y%m%d'))
            del dt_yesterday
            self.__lstAgencyInfo [n_row_cnt-1][0] = self.__lstAgencyInfo [n_row_cnt-1][0] + s_yesterday
        # lst_new_info = [s_begin_date, s_agency_name, n_fee_percent, s_fee_type]
        self.__lstAgencyInfo.append([str(dt_today.strftime('%Y%m%d')) + '-', s_agency_name, str(n_fee_percent) + '%', s_fee_type])
        with open(self.__sAgencyInfoPath, 'w', newline='') as o_csvfile:
            write = csv.writer(o_csvfile, delimiter='\t') 
            write.writerows(self.__lstAgencyInfo ) 
        return True

    def redefine_agency_cost(self, s_data_source, s_data_source_id, s_touching_date, n_cost, b_debug=False):
        dict_rst = {'cost':0, 'agency_fee':0, 'vat':0}
        if n_cost <= 0:
            return dict_rst

        s_cur_agency_info_id = str(s_data_source_id) + '-' + s_data_source
        if self.__dictAgencyInfo.get(s_cur_agency_info_id, self.__g_sSvNull) != self.__g_sSvNull:  # if s_agency_info_id exists
            self.__lstAgencyInfo = self.__dictAgencyInfo[s_cur_agency_info_id]

        s_touching_date = datetime.strptime(s_touching_date, '%Y-%m-%d').strftime('%Y%m%d')
        s_begin_date = self.__g_sBirthOfUniverse  # define default ancient begin date
        # s_agency_name = None
        f_fee_rate = 0.0
        s_fee_type = None
        if len(self.__lstAgencyInfo):
            for lst_single_period in self.__lstAgencyInfo:
                s_end_date = self.__sTodayYyyymmdd
                lst_period = lst_single_period[0].split('-')
                if len(lst_period[0]) > 0:
                    try: # validate requsted date
                        s_begin_date = datetime.strptime(lst_period[0], '%Y%m%d').strftime('%Y%m%d')
                    except ValueError:
                        self._printDebug('start date:' + lst_period[0] + ' is invalid date string')

                if len(lst_period[1]) > 0:
                    try: # validate requsted date
                        s_end_date = datetime.strptime(lst_period[1], '%Y%m%d').strftime('%Y%m%d')
                    except ValueError:
                        self._printDebug('end date:' + lst_period[0] + ' is invalid date string')
                
                if int(s_begin_date) > int(s_touching_date):
                    continue
                if int(s_end_date) < int(s_touching_date):
                    continue
                # s_agency_name = lst_single_period[1]
                m = self.__g_oRegEx.search(lst_single_period[2]) # match() vs search()
                if m: # if valid percent string
                    n_percent = lst_single_period[2].replace('%','')
                    f_fee_rate = int(n_percent)/100
                else: # if invalid percent string
                    self._printDebug('invalid percent string ' + lst_single_period[2])
                    raise Exception('stop')
                s_fee_type = lst_single_period[3]
                break
        # warn and ignore an agency fee if no belonged period
        if f_fee_rate == 0.0 and s_fee_type is None:
            self._printDebug(s_cur_agency_info_id + ' has no agency fee info on ' + s_touching_date)
            s_fee_type = self.__g_sAgencyFeeTypeMarkup

        n_final_cost = 0
        n_agency_cost = 0
        if s_fee_type == self.__g_sAgencyFeeTypeBackmargin:  #'back':
            n_final_cost =int((1 - f_fee_rate) * n_cost)
            n_agency_cost = int(f_fee_rate * n_cost)
            # validate naver ad cost division
            n_tmp_cost = n_final_cost + n_agency_cost
            if n_cost > n_tmp_cost:
                n_residual = n_cost - n_tmp_cost
                n_final_cost = n_final_cost + n_residual
            elif n_cost < n_tmp_cost:
                n_residual = n_tmp_cost - n_cost
                n_final_cost = n_final_cost + n_residual
        elif s_fee_type == self.__g_sAgencyFeeTypeMarkup:  #'markup':
            n_final_cost = n_cost
            n_agency_cost = f_fee_rate * n_cost
        elif s_fee_type == self.__g_sAgencyFeeTypeDirect:  #'direct':
            n_final_cost = n_cost

        if s_data_source == 'naver_ad':
            if n_cost != n_final_cost + n_agency_cost:
                self._printDebug('calculation mismatches on naver ad')
                self._printDebug(n_cost)
                self._printDebug(n_final_cost)
                self._printDebug(n_agency_cost)

        if s_data_source == 'kakao': # csv download based data
            n_vat_from_final_cost = int(n_final_cost * 0.1)
            n_vat_fromn_agency_cost = int(n_agency_cost * 0.1)
            dict_rst['cost'] = n_final_cost - n_vat_from_final_cost
            dict_rst['agency_fee'] = n_agency_cost - n_vat_fromn_agency_cost
            dict_rst['vat'] = n_vat_from_final_cost + n_vat_fromn_agency_cost
        else:
            dict_rst['cost'] = n_final_cost
            dict_rst['agency_fee'] = n_agency_cost
            dict_rst['vat'] = (n_final_cost + n_agency_cost) * 0.1
        
        if b_debug:
            self._printDebug(s_touching_date + ' ' + str(f_fee_rate) + ' ' + s_fee_type)
        return dict_rst
