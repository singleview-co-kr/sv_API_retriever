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
import logging
import configparser # https://docs.python.org/3/library/configparser.html

# 3rd party library
from decouple import config

# singleview config
if __name__ == 'svcommon.sv_api_config_parser' : # for websocket running
    from svcommon import sv_object
    from django.conf import settings
elif __name__ == 'sv_api_config_parser': # for plugin console debugging
    sys.path.append('../../svdjango')
    import sv_object
    import settings


class SvApiConfigParser(sv_object.ISvObject):
    __g_oConfig = None
    __g_sAbsolutePath = None
    __g_sApiConfigFile = None
    __g_lstAcctInfo = []

    def __init__(self, s_config_location):
        self._g_oLogger = logging.getLogger(__file__)
        self.__g_oConfig = configparser.RawConfigParser()  # make python3 config parser parse key case sensitive
        self.__g_oConfig.optionxform = lambda option: option  # make python3 config parser parse key case sensitive
        self.__g_sAbsolutePath = config('ABSOLUTE_PATH_BOT')
        self.__g_lstAcctInfo = s_config_location.split('/')  # 1/1 means relative config path case
        self.__g_sApiConfigFile = os.path.join(self.__g_sAbsolutePath, settings.SV_STORAGE_ROOT,
                                               self.__g_lstAcctInfo[0], self.__g_lstAcctInfo[1], 'api_info.ini')

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __del__(self):
        pass

    def get_config(self):
        s_err_msg = 'no_api_info_ini'
        dict_2nd_layer = {'brand_name': s_err_msg, 'sv_account_id': s_err_msg, 'brand_id': s_err_msg,
                          'tbl_prefix': s_err_msg, 'nvr_ad_acct': {}}
        try:
            with open(self.__g_sApiConfigFile) as f:
                self.__g_oConfig.read_file(f)
        except IOError:  # run plugin for the first time
            self._print_debug('api_info.ini not exist')
            return dict_2nd_layer

        s_brand_name = None
        dict_nvr_ad_acct = {}
        lst_googleads_acct = []
        lst_fb_biz_aid = []
        lst_nvr_master_rpt = []
        lst_nvr_stat_rpt = []
        lst_nvr_search_api_media = []
        dict_other_ads_api_info = {}
        for s_section_title in self.__g_oConfig:
            if s_section_title == 'general':
                for s_value_title in self.__g_oConfig[s_section_title]:
                    s_brand_name = self.__g_oConfig[s_section_title][s_value_title]
            elif s_section_title == 'naver_searchad':
                for s_value_title in self.__g_oConfig[s_section_title]:
                    if s_value_title == 'api_key' or s_value_title == 'secret_key' or s_value_title == 'manager_login_id' or \
                     s_value_title == 'customer_id':
                        dict_nvr_ad_acct[s_value_title] = self.__g_oConfig[s_section_title][s_value_title]
            elif s_section_title == 'nvr_master_report':
                for s_value_title in self.__g_oConfig[s_section_title]:
                    if self.__g_oConfig[s_section_title][s_value_title] == '1':
                        lst_nvr_master_rpt.append(s_value_title)
            elif s_section_title == 'nvr_stat_report':
                for s_value_title in self.__g_oConfig[s_section_title]:
                    if self.__g_oConfig[s_section_title][s_value_title] == '1':
                        lst_nvr_stat_rpt.append(s_value_title)
            elif s_section_title == 'nvr_search':
                for s_value_title in self.__g_oConfig[s_section_title]:
                    if self.__g_oConfig[s_section_title][s_value_title] == '1':
                        lst_nvr_search_api_media.append(s_value_title)
            elif s_section_title == 'google_ads':
                for s_value_title in self.__g_oConfig[s_section_title]:
                    if self.__g_oConfig[s_section_title][s_value_title].lower() == 'on':
                        lst_googleads_acct.append(s_value_title)
                    dict_other_ads_api_info['adw_cid'] = lst_googleads_acct
            elif s_section_title == 'facebook_business':
                for s_value_title in self.__g_oConfig[s_section_title]:
                    if self.__g_oConfig[s_section_title][s_value_title].lower() == 'on':
                        lst_fb_biz_aid.append(s_value_title)
                    dict_other_ads_api_info['fb_biz_aid'] = lst_fb_biz_aid
            elif s_section_title == 'google_analytics':
                dict_temp = {'s_version': None, 's_property_or_view_id':None, 'lst_access_level': []}
                for s_value_title in self.__g_oConfig[s_section_title]:
                    if s_value_title == 'version':
                        s_version = self.__g_oConfig[s_section_title][s_value_title]
                        dict_temp['s_version'] = self.__g_oConfig[s_section_title][s_value_title]
                    elif s_value_title == 'property_or_view_id':
                        dict_temp['s_property_or_view_id'] = self.__g_oConfig[s_section_title][s_value_title]
                    elif self.__g_oConfig[s_section_title][s_value_title].lower() == 'on':
                        dict_temp['lst_access_level'].append(s_value_title)
                    dict_other_ads_api_info['google_analytics'] = dict_temp
            elif s_section_title == 'others':
                for s_value_title in self.__g_oConfig[s_section_title]:
                    dict_other_ads_api_info[s_value_title] = self.__g_oConfig[s_section_title][s_value_title]
        dict_nvr_ad_acct['nvr_master_report'] = lst_nvr_master_rpt
        dict_nvr_ad_acct['nvr_stat_report'] = lst_nvr_stat_rpt

        dict_2nd_layer['brand_name'] = s_brand_name
        dict_2nd_layer['sv_account_id'] = self.__g_lstAcctInfo[0]
        dict_2nd_layer['brand_id'] = self.__g_lstAcctInfo[1]
        dict_2nd_layer['tbl_prefix'] = self.__g_lstAcctInfo[0] + '_' + self.__g_lstAcctInfo[1]
        dict_2nd_layer['nvr_ad_acct'] = dict_nvr_ad_acct
        dict_2nd_layer['nvr_search'] = lst_nvr_search_api_media
        for s_title in dict_other_ads_api_info:
            dict_2nd_layer[s_title] = dict_other_ads_api_info[s_title]
        return dict_2nd_layer


# if __name__ == '__main__': # for console debugging
# 	pass
