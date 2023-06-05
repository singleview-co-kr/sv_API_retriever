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
import os.path
import re
import sys
import csv
import logging
from datetime import datetime

# 3rd party library

# singleview config
if __name__ == 'svcommon.sv_agency_info':  # for websocket running
    from svcommon import sv_object
    from django.conf import settings
    from svacct.models import MediaAgency
    # from svload.pandas_plugins import contract
elif __name__ == 'sv_agency_info':  # for plugin console debugging
    sys.path.append('../../svdjango')
    import sv_object
    import settings
    # import contract
elif __name__ == '__main__':  # for class console debugging
    pass


class SvAgencyInfo(sv_object.ISvObject):
    __g_sBirthOfUniverse = '20010101'  # define default ancient begin date
    __g_sAgencyFeeTypeBackmargin = 'back'
    __g_sAgencyFeeTypeMarkup = 'markup'
    __g_sAgencyFeeTypeDirect = 'direct'
    __g_sSvNull = '#%'
    __g_oRegEx = re.compile(r"\d+%$")  # pattern ex) 2% 23%
    __g_dictNvPnsUaCostPortion = {'M': 0.7, 'P': 0.3}  # sum must be 1

    def __init__(self):
        self._g_oLogger = logging.getLogger(__file__)
        self.__sTodayYyyymmdd = datetime.today().strftime('%Y%m%d')
        self.__sAgencyInfoPath = None
        self.__lstAgencyInfo = []
        self.__dictAgencyInfo = {}
        # o_pns_info = contract.PnsInfo()
        # self.__g_dictPnsSource = o_pns_info.get_inverted_source_type_dict()
        # self.__g_dictPnsContract = o_pns_info.get_contract_type_dict()
        # del o_pns_info

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __del__(self):
        pass

    def load_agency_dict(self):
        """
        called by svload.pandas_plugins.budget.py::get_list_by_period()
        this method is executed on Django-context only
        :return:
        """
        dict_agency_info = {}
        qs_agency = MediaAgency.objects.all()
        for o_single_agency in qs_agency:
            dict_agency_info[o_single_agency.pk] = o_single_agency.s_agency_name
        return dict_agency_info

    def load_by_source_id(self, s_acct_pk, s_brand_pk, s_data_source, s_data_source_id):
        """
        called by svacct.signals.py
        called by svplugins.integrate_db.task.py
        """
        s_acct_pk = str(s_acct_pk)  # enforce to stringfy
        s_brand_pk = str(s_brand_pk)  # enforce to stringfy
        s_data_source = str(s_data_source)  # enforce to stringfy
        s_data_source_id = str(s_data_source_id)  # enforce to stringfy
        s_cur_agency_info_id = s_data_source_id + '-' + s_data_source
        # if s_agency_info_id exists
        if self.__dictAgencyInfo.get(s_cur_agency_info_id, self.__g_sSvNull) != self.__g_sSvNull:
            self.__lstAgencyInfo = self.__dictAgencyInfo[s_cur_agency_info_id]
        else:  # if new s_agency_info_id requested
            self.__sAgencyInfoPath = os.path.join(settings.SV_STORAGE_ROOT, s_acct_pk, s_brand_pk, s_data_source,
                                                  s_data_source_id, 'conf', 'agency_info.tsv')
            lst_temp = []
            if os.path.exists(self.__sAgencyInfoPath):
                with open(self.__sAgencyInfoPath, 'r', encoding='utf8') as o_tsv_file:
                    o_tsv_reader = csv.reader(o_tsv_file, delimiter='\t')
                    for lst_row in o_tsv_reader:
                        lst_temp.append(lst_row)
            else:
                self._print_debug(self.__sAgencyInfoPath + ' does not exist')
                return False
            self.__lstAgencyInfo = lst_temp
            self.__dictAgencyInfo[s_cur_agency_info_id] = lst_temp
        return True

    def get_agency_fee_type(self):
        return [self.__g_sAgencyFeeTypeBackmargin, self.__g_sAgencyFeeTypeMarkup, self.__g_sAgencyFeeTypeDirect]

    def set_agency_info(self, lst_agency_info):
        """ convert django admin model into csv for svplugin console execution """
        lst_agency_info_new = []
        for dict_single_contract in lst_agency_info:
            s_date_begin = '' if dict_single_contract['date_begin'] is None else dict_single_contract['date_begin'].strftime('%Y%m%d')
            s_date_end = '' if dict_single_contract['date_end'] is None else dict_single_contract['date_end'].strftime('%Y%m%d')
            lst_agency_info_new.append(
                [s_date_begin+'-'+s_date_end, str(dict_single_contract['media_agency_id']), str(dict_single_contract['media_agency_name']),
                 str(dict_single_contract['n_agent_fee_percent']) + '%', dict_single_contract['s_fee_type']]
            )
        with open(self.__sAgencyInfoPath, 'w', encoding='utf8', newline='') as o_csvfile:
            write = csv.writer(o_csvfile, delimiter='\t')
            write.writerows(lst_agency_info_new)
        self.__lstAgencyInfo = lst_agency_info_new
        del lst_agency_info_new
        return True

    def get_cost_info(self, s_data_source, s_data_source_id, s_touching_date, n_cost, b_debug=False):
        dict_rst = {'agency_id': 0, 'agency_name': '', 'cost': 0, 'agency_fee': 0, 'vat': 0}
        if n_cost <= 0:
            return dict_rst

        s_cur_agency_info_id = str(s_data_source_id) + '-' + s_data_source
        # if s_agency_info_id exists
        if self.__dictAgencyInfo.get(s_cur_agency_info_id, self.__g_sSvNull) != self.__g_sSvNull:
            self.__lstAgencyInfo = self.__dictAgencyInfo[s_cur_agency_info_id]

        s_touching_date = datetime.strptime(s_touching_date, '%Y-%m-%d').strftime('%Y%m%d')
        s_begin_date = self.__g_sBirthOfUniverse  # define default ancient begin date
        n_agency_id = 0
        s_agency_name = ''
        f_fee_rate = 0.0
        s_fee_type = None
        if len(self.__lstAgencyInfo):
            for lst_single_period in self.__lstAgencyInfo:
                s_end_date = self.__sTodayYyyymmdd
                lst_period = lst_single_period[0].split('-')
                if len(lst_period[0]) > 0:
                    try:  # validate requested date
                        s_begin_date = datetime.strptime(lst_period[0], '%Y%m%d').strftime('%Y%m%d')
                    except ValueError:
                        self._print_debug('start date:' + lst_period[0] + ' is invalid date string')

                if len(lst_period[1]) > 0:
                    try:  # validate requested date
                        s_end_date = datetime.strptime(lst_period[1], '%Y%m%d').strftime('%Y%m%d')
                    except ValueError:
                        self._print_debug('end date:' + lst_period[1] + ' is invalid date string')
                
                if int(s_begin_date) > int(s_touching_date):
                    continue
                if int(s_end_date) < int(s_touching_date):
                    continue
                n_agency_id = lst_single_period[1]
                s_agency_name = lst_single_period[2]
                m = self.__g_oRegEx.search(lst_single_period[3])  # match() vs search()
                if m:  # if valid percent string
                    n_percent = lst_single_period[3].replace('%', '')
                    f_fee_rate = int(n_percent)/100
                else:  # if invalid percent string
                    self._print_debug('invalid percent string ' + lst_single_period[3])
                    raise Exception('stop')
                s_fee_type = lst_single_period[4]
                break
        # warn and ignore an agency fee if no belonged period
        if f_fee_rate == 0.0 and s_fee_type is None:
            self._print_debug(s_cur_agency_info_id + ' has no agency fee info on ' + s_touching_date)
            s_fee_type = self.__g_sAgencyFeeTypeMarkup

        n_final_cost = 0
        n_agency_cost = 0
        if s_fee_type == self.__g_sAgencyFeeTypeBackmargin:  # 'back':
            n_final_cost = int((1 - f_fee_rate) * n_cost)
            n_agency_cost = int(f_fee_rate * n_cost)
            # validate naver ad cost division
            n_tmp_cost = n_final_cost + n_agency_cost
            if n_cost > n_tmp_cost:
                n_residual = n_cost - n_tmp_cost
                n_final_cost = n_final_cost + n_residual
            elif n_cost < n_tmp_cost:
                n_residual = n_tmp_cost - n_cost
                n_final_cost = n_final_cost + n_residual
        elif s_fee_type == self.__g_sAgencyFeeTypeMarkup:  # 'markup':
            n_final_cost = n_cost
            n_agency_cost = f_fee_rate * n_cost
        elif s_fee_type == self.__g_sAgencyFeeTypeDirect:  # 'direct':
            n_final_cost = n_cost

        if s_data_source == 'naver_ad':
            if n_cost != n_final_cost + n_agency_cost:
                self._print_debug('calculation mismatches on naver ad')
                self._print_debug(n_cost)
                self._print_debug(n_final_cost)
                self._print_debug(n_agency_cost)

        if s_data_source == 'kakao':  # csv download based data
            n_vat_from_final_cost = int(n_final_cost * 0.1)
            n_vat_from_agency_cost = int(n_agency_cost * 0.1)
            dict_rst['cost'] = n_final_cost - n_vat_from_final_cost
            dict_rst['agency_fee'] = n_agency_cost - n_vat_from_agency_cost
            dict_rst['vat'] = n_vat_from_final_cost + n_vat_from_agency_cost
        else:
            dict_rst['cost'] = n_final_cost
            dict_rst['agency_fee'] = n_agency_cost
            dict_rst['vat'] = (n_final_cost + n_agency_cost) * 0.1
        dict_rst['agency_id'] = n_agency_id
        dict_rst['agency_name'] = s_agency_name
        if b_debug:
            self._print_debug(s_touching_date + ' ' + str(f_fee_rate) + ' ' + s_fee_type)
        return dict_rst

    # def get_allocated_pns_cost(self, o_sv_db, n_pns_touching_date, s_touching_date, s_source):
    #     """
    #     get allocated Paid NS cost
    #     this method requires sv_db object created from each svplugin
    #     """
    #     dict_pns_info = {}
    #     if s_source not in self.__g_dictPnsSource:
    #         self._print_debug('invalid pns info request :' + s_source)
    #         return dict_pns_info
    #
    #     dt_touching_date = datetime.strptime(s_touching_date, '%Y-%m-%d').date()
    #     # sql file is in svplugins.integrate_db.queries
    #     lst_contract_info = o_sv_db.execute_query('getPnsContract', self.__g_dictPnsSource[s_source],
    #                                              dt_touching_date, dt_touching_date)
    #     if len(lst_contract_info) > 0:
    #         n_pns_info_idx = 0
    #         n_touching_date = int(s_touching_date.replace('-', ''))
    #         # o_reg_ex = re.compile(r"\d+%$")  # pattern ex) 2% 23%
    #         for dict_single_contract in lst_contract_info:
    #             # define raw cost & agency cost -> calculate vat from sum of define raw cost & agency cost
    #             n_period_cost_incl_vat = dict_single_contract['cost_incl_vat']
    #             n_period_cost_exc_vat = math.ceil(n_period_cost_incl_vat / 1.1)
    #             m = self.__g_oRegEx.search(dict_single_contract['agency_rate_percent'])  # match() vs search()
    #             if m:  # if valid percent string
    #                 f_rate = int(dict_single_contract['agency_rate_percent'].replace('%', '')) / 100
    #             else:  # if invalid percent string
    #                 self._print_debug('invalid percent string ' + dict_single_contract['agency_rate_percent'])
    #                 raise Exception('stop')
    #             del m
    #
    #             f_contract_raw_cost = int((1 - f_rate) * n_period_cost_exc_vat)
    #             f_agency_cost = int(f_rate * n_period_cost_exc_vat)
    #             dt_delta_days = dict_single_contract['execute_date_end'] - dict_single_contract['execute_date_begin']
    #             s_contract_type = self.__g_dictPnsContract[dict_single_contract['contract_type']]
    #             s_term = dict_single_contract['media_term']
    #             s_nickname = dict_single_contract['contractor_id']
    #             s_regdate = dict_single_contract['regdate'].strftime('%Y%m%d')
    #             for s_ua in self.__g_dictNvPnsUaCostPortion:
    #                 f_portion = self.__g_dictNvPnsUaCostPortion[s_ua]
    #                 f_daily_media_raw_cost = f_contract_raw_cost / (dt_delta_days.days + 1) * f_portion
    #                 f_daily_agency_cost = f_agency_cost / (dt_delta_days.days + 1) * f_portion
    #                 f_vat = (f_daily_media_raw_cost + f_daily_agency_cost) * 0.1
    #                 if n_touching_date <= n_pns_touching_date:  # for the old & non-systematic & complicated situation
    #                     dict_pns_info[n_pns_info_idx] = {
    #                         'service_type': s_contract_type, 'term': s_term, 'nick': s_nickname, 'ua': s_ua,
    #                         'media_raw_cost': f_daily_media_raw_cost, 'media_agency_cost': f_daily_agency_cost,
    #                         'vat': f_vat, 'regdate': s_regdate
    #                     }
    #                     n_pns_info_idx += 1
    #                 else:  # for the latest & systematic situation
    #                     s_row_id = s_term + '_' + s_contract_type + '_' + s_nickname + '_' + s_regdate + '_' + s_ua
    #                     dict_pns_info[s_row_id] = {
    #                         'media_raw_cost': f_daily_media_raw_cost, 'media_agency_cost': f_daily_agency_cost,
    #                         'vat': f_vat
    #                     }
    #     return dict_pns_info

    # def set_agency_info(self, s_begin_date, s_agency_name, n_fee_percent, s_fee_type):
    #     """ """
    #     dt_today = datetime.today()
    #     n_row_cnt = len(self.__lstAgencyInfo)
    #     if n_row_cnt:
    #         dt_yesterday = dt_today - timedelta(1)
    #         s_yesterday = str(dt_yesterday.strftime('%Y%m%d'))
    #         del dt_yesterday
    #         self.__lstAgencyInfo [n_row_cnt-1][0] = self.__lstAgencyInfo [n_row_cnt-1][0] + s_yesterday
    #     # lst_new_info = [s_begin_date, s_agency_name, n_fee_percent, s_fee_type]
    #     self.__lstAgencyInfo.append([str(dt_today.strftime('%Y%m%d')) + '-', s_agency_name, str(n_fee_percent) + '%', s_fee_type])
    #     with open(self.__sAgencyInfoPath, 'w', newline='') as o_csvfile:
    #         write = csv.writer(o_csvfile, delimiter='\t')
    #         write.writerows(self.__lstAgencyInfo )
    #     return True

    # def get_latest_agency_info_dict(self):
    #     dict_agency_info = {
    #             's_begin_date': '',
    #             's_end_date': '',
    #             's_agency_name': '',
    #             'n_fee_rate': 0,
    #             's_fee_type': ''
    #         }
    #     if len(self.__lstAgencyInfo):
    #         lst_latest = self.__lstAgencyInfo.pop()
    #         lst_period = lst_latest[0].split('-')
    #         dict_agency_info['s_begin_date'] = lst_period[0]
    #         if len(lst_period) == 2:
    #             dict_agency_info['s_end_date'] = lst_period[1]
    #         dict_agency_info['s_agency_name'] = lst_latest[1]
    #         dict_agency_info['n_fee_rate'] = int(lst_latest[2].rstrip('%'))
    #         dict_agency_info['s_fee_type'] = lst_latest[3]
    #     return dict_agency_info
