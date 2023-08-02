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
import logging
import sys
import configparser  # https://docs.python.org/3/library/configparser.html
from slack_cleaner2.predicates import match

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_object
    import sv_slack
    import sv_plugin
    import settings
else: # for platform running
    from svcommon import sv_object
    from svcommon import sv_slack
    from svcommon import sv_plugin
    from django.conf import settings


class SvJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230802)')

        self._g_dictParam.update({'slack_ch_ttl': None})
        self.__g_oConfig = configparser.ConfigParser()

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        del self.__g_oConfig

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

        s_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        self.__get_key_config(s_acct_id, s_brand_id)

        s_slack_ch_title = None
        if 'default' in self.__g_oConfig and 'slack_ch_title' in self.__g_oConfig['default']:
            s_slack_ch_title = self.__g_oConfig['default']['slack_ch_title']
        if self._g_dictParam['slack_ch_ttl']:
            s_slack_ch_title = self._g_dictParam['slack_ch_ttl']
        elif s_slack_ch_title is None:
            self._print_debug('execution denied! -> slack_ch_ttl is required via slack.config.ini or slack_ch_ttl param')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        o_sv_slack = sv_slack.SvSlack(s_calling_bot='dbs')
        self._print_debug('Prepare to clear slack channel - ' + s_slack_ch_title)
        o_slack_cleaner = o_sv_slack.get_slack_cleaner(s_slack_ch_title)
        # delete all messages in general channels
        n_cnt = 1
        for o_msg in o_slack_cleaner.msgs(filter(match(s_slack_ch_title), o_slack_cleaner.conversations)):
            if not self._continue_iteration():
                break
            # delete messages, its files, and all its replies (thread)
            o_msg.delete(replies=True, files=True)
            if o_callback:  # regarding an execution on a web console 
                self._print_debug('single slack msg has been deleted - ' + str(n_cnt))
                n_cnt = n_cnt + 1
        self._print_debug(str(n_cnt) + ' slack msgs have been deleted')
        self._task_post_proc(self._g_oCallback)

    def __get_key_config(self, s_acct_id, s_acct_title):
        s_config_path = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_acct_id, s_acct_title, 'slack.config.ini')
        try:
            with open(s_config_path) as f:
                self.__g_oConfig.read_file(f)
        except FileNotFoundError:
            self._print_debug('slack.config.ini not exist')
            return  # raise Exception('stop')

        self.__g_oConfig.read(s_config_path)


if __name__ == '__main__':  # for console debugging and execution
    # python task.py config_loc=1/1 slack_ch_ttl=dbs_bot
    # python task.py config_loc=1/1 # if defined in slack.config.ini
    # slack_clear slack_ch_ttl=yuhen_web
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with SvJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('slack_clear')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [slack_ch_ttl] params are required for console execution.')
