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
from datetime import timedelta
import os
import sys
import importlib

# 3rd party library

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql
    import sv_object
    import sv_plugin
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svload.pandas_plugins import budget


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230605)')

        # self._g_dictParam.update({'yyyymm': None, 'mode': None})
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        # self.__g_sMode = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        # self.__g_sMode = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        
        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
          'nvr_ad_acct' not in dict_acct_info:
            self._print_debug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        # begin - retrieve PS budget list for yesterday
        yesterday = datetime.now() - timedelta(1)
        s_yesterday = datetime.strftime(yesterday, '%Y-%m-%d')
        del yesterday
        with sv_mysql.SvMySql() as oSvMysql:
            oSvMysql.set_tbl_prefix(dict_acct_info['tbl_prefix'])
            oSvMysql.set_app_name('svplugins.daily_cron')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            lst_rst = oSvMysql.executeQuery('getBudgetByDay', s_yesterday, s_yesterday)
        # end - retrieve PS budget list for yesterday
        # begin - decide jobs to do now
        o_budget = budget.Budget(None)
        dict_budget_type = o_budget.get_budget_type_dict()
        del o_budget
        dict_source_toggle = {'naver': False, 'google': False, 'youtube': False, 'facebook': False}
        for dict_row in lst_rst:
            n_acct_id = dict_row['acct_id']
            if n_acct_id in dict_budget_type:
                if dict_budget_type[n_acct_id]['media_media'] == 'cpc' or \
                    dict_budget_type[n_acct_id]['media_media'] == 'display':
                    dict_source_toggle[dict_budget_type[n_acct_id]['media_source']] = True
        del dict_budget_type
        del lst_rst

        lst_jobs_to_cron = ['ga_get_day', 'ga_register_db']  # GA is default and base
        if dict_source_toggle['naver']:
            lst_jobs_to_cron.append('nvad_get_day')
            lst_jobs_to_cron.append('nvad_register_db')
        if dict_source_toggle['google'] or dict_source_toggle['youtube'] :
            lst_jobs_to_cron.append('aw_get_day')
            lst_jobs_to_cron.append('aw_register_db')
        if dict_source_toggle['facebook']:
            lst_jobs_to_cron.append('fb_get_day')
            lst_jobs_to_cron.append('fb_register_db')
        del dict_source_toggle
        # end - decide jobs to do now
        # begin - execute jobs to do
        for s_job_to_do in lst_jobs_to_cron:
            o_job_plugin = importlib.import_module('svplugins.' + s_job_to_do + '.task')
            self._print_debug('sub task: ' + s_job_to_do + ' has been launched')
            with o_job_plugin.svJobPlugin() as o_job:
                o_job.set_websocket_output(self._printDebug)
                o_job.set_my_name(s_job_to_do)
                o_job.parse_command([])
                o_job.do_task(self._g_oCallback)
            del o_job_plugin
            self._print_debug('sub task: ' + s_job_to_do + ' has been finished')
        # end - execute jobs to do
        self._task_post_proc(self._g_oCallback)
        return


if __name__ == '__main__': # for console debugging
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('daily_cron')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params are required for console execution.')
