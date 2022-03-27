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
    __g_sConfigLoc = None
    __g_sApiConfigFile = None
    __g_lstAcctInfo = []

    def __init__(self, sConfigLocation):
        self._g_oLogger = logging.getLogger(__file__)
        self.__g_oConfig = configparser.RawConfigParser() # make python3 config parser parse key case sensitive
        self.__g_oConfig.optionxform = lambda option: option # make python3 config parser parse key case sensitive
        self.__g_sAbsolutePath = config('ABSOLUTE_PATH_BOT')
        self.__g_sConfigLoc = sConfigLocation
        if self.__g_sConfigLoc[0] == '/': # absolute config path case
            self._printDebug( 'absolute api_info.ini not process is not defined')
            raise IOError('failed to read api_info.ini')
        else: # relative config path case
            self.__g_sApiConfigFile = os.path.join(self.__g_sAbsolutePath, settings.SV_STORAGE_ROOT, self.__g_sConfigLoc, 'api_info.ini')
            self.__g_lstAcctInfo = self.__g_sConfigLoc.split('/')

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __del__(self):
        pass

    def getConfig(self):
        try:
            with open(self.__g_sApiConfigFile) as f:
                self.__g_oConfig.read_file(f)
        except IOError: # run plugin for the first time
            self._printDebug('api_info.ini not exist')
            if __name__ == 'sv_api_config_parser': # for plugin console running
                # sys.path.append('../../classes')
                import sv_install
                o_install = sv_install.SvInstall()
                dict_config_info = {'s_abs_path_bot': self.__g_sAbsolutePath, 's_acct_info': self.__g_sConfigLoc}
                o_install.start_up_job_plugin(dict_config_info)
                del o_install
            # raise IOError('failed to read api_info.ini')

        dictNvrAdAcct = {}
        lstGoogleadsAcct = []
        lstNvrMasterReport = [] 
        lstNvrStatReport = []
        dictOtherAdsApiInfo = {}
        self.__g_oConfig.read(self.__g_sApiConfigFile)
        for sSectionTitle in self.__g_oConfig:
            if sSectionTitle == 'naver_searchad':
                for sValueTitle in self.__g_oConfig[sSectionTitle]:
                    if sValueTitle == 'api_key' or sValueTitle == 'secret_key' or sValueTitle == 'manager_login_id' or \
                     sValueTitle == 'customer_id':
                        dictNvrAdAcct[sValueTitle] = self.__g_oConfig[sSectionTitle][sValueTitle]
            elif sSectionTitle == 'nvr_master_report':
                for sValueTitle in self.__g_oConfig[sSectionTitle]:
                    if self.__g_oConfig[sSectionTitle][sValueTitle] == '1':
                        lstNvrMasterReport.append(sValueTitle)
            elif sSectionTitle == 'nvr_stat_report':
                for sValueTitle in self.__g_oConfig[sSectionTitle]:
                    if self.__g_oConfig[sSectionTitle][sValueTitle] == '1':
                        lstNvrStatReport.append(sValueTitle)
            elif sSectionTitle == 'google_ads':
                for sValueTitle in self.__g_oConfig[sSectionTitle]:
                    if self.__g_oConfig[sSectionTitle][sValueTitle].lower() == 'on':
                        lstGoogleadsAcct.append(sValueTitle)
                    dictOtherAdsApiInfo['adw_cid'] = lstGoogleadsAcct
            elif sSectionTitle == 'google_analytics':
                dict_temp = {'s_version': None, 's_property_or_view_id':None, 'lst_access_level': []}
                for sValueTitle in self.__g_oConfig[sSectionTitle]:
                    if sValueTitle == 'version':
                        s_version = self.__g_oConfig[sSectionTitle][sValueTitle]
                        dict_temp['s_version'] = self.__g_oConfig[sSectionTitle][sValueTitle]
                    elif sValueTitle == 'property_or_view_id':
                        dict_temp['s_property_or_view_id'] = self.__g_oConfig[sSectionTitle][sValueTitle]
                    elif self.__g_oConfig[sSectionTitle][sValueTitle].lower() == 'on':
                        dict_temp['lst_access_level'].append(sValueTitle)
                    dictOtherAdsApiInfo['google_analytics'] = dict_temp
            elif sSectionTitle == 'others':
                for sValueTitle in self.__g_oConfig[sSectionTitle]:
                    dictOtherAdsApiInfo[sValueTitle] = self.__g_oConfig[sSectionTitle][sValueTitle]
        dictNvrAdAcct['nvr_master_report'] = lstNvrMasterReport
        dictNvrAdAcct['nvr_stat_report'] = lstNvrStatReport
        
        s_tbl_prefix = self.__g_lstAcctInfo[0] + '_' + self.__g_lstAcctInfo[1]
        dict2ndLayer = {'sv_account_id': self.__g_lstAcctInfo[0], 'brand_id': self.__g_lstAcctInfo[1], 
                        'tbl_prefix': s_tbl_prefix, 'nvr_ad_acct': dictNvrAdAcct}
        for sValueTitle in dictOtherAdsApiInfo:
                dict2ndLayer[sValueTitle] = dictOtherAdsApiInfo[sValueTitle]
        return dict2ndLayer


# if __name__ == '__main__': # for console debugging
# 	pass
