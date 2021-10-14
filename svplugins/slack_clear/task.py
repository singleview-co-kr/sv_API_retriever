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
import logging
import sys
from slack_cleaner2.predicates import match

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_object, sv_slack, sv_plugin
else: # for platform running
    from svcommon import sv_object, sv_slack, sv_plugin


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sVersion = '0.0.2'
        self._g_sLastModifiedDate = '13th, Oct 2021'
        self._g_oLogger = logging.getLogger(__name__ + ' v'+self._g_sVersion)
        self._g_dictParam.update({'slack_ch_ttl':None})

    def do_task(self, o_callback):
        o_rsp = self._task_pre_proc(o_callback)
        del o_rsp
        if self._g_dictParam['slack_ch_ttl'] is None:
            self._printDebug('execution denied! -> you need to define slack_ch_ttl')
            return

        s_slack_ch_title = self._g_dictParam['slack_ch_ttl']
        o_sv_slack = sv_slack.svSlack(sCallingBot='dbs')
        
        self._printDebug('Prepare to clear slack channel - ' + s_slack_ch_title)
        o_slack_cleaner = o_sv_slack.get_slack_cleaner(s_slack_ch_title)
        # delete all messages in general channels
        n_cnt = 1
        for o_msg in o_slack_cleaner.msgs(filter(match(s_slack_ch_title), o_slack_cleaner.conversations)):
            if not self._continue_iteration():
                break
            # delete messages, its files, and all its replies (thread)
            o_msg.delete(replies=True, files=True)
            if o_callback:  # regarding an execution on a web console 
                self._printDebug('single slack msg has been deleted - ' + str(n_cnt))
                n_cnt = n_cnt + 1
        self._printDebug(str(n_cnt) + ' slack msgs have been deleted')

        self._task_post_proc(o_callback)
        return

if __name__ == '__main__': # for console debugging and execution
    # python task.py analytical_namespace=test config_loc=1/ynox slack_ch_ttl=dbs_bot
    # slack_clear slack_ch_ttl=yuhen_web
    nCliParams = len(sys.argv)
    if nCliParams > 2:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('aw_get_day')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [analytical_namespace] [config_loc] [slack_ch_ttl] params are required for console execution.')
