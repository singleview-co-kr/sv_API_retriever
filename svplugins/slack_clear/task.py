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

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_object, sv_slack, sv_plugin
else: # for platform running
    from svcommon import sv_object, sv_slack, sv_plugin


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sSlackChTitle = None # slack channel to bulk delete

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sVersion = '0.0.1'
        self._g_sLastModifiedDate = '4th, Jul 2021'
        self._g_oLogger = logging.getLogger(__name__ + ' v'+self._g_sVersion)
        self._g_dictParam.update({'slack_ch_ttl':None})

    def do_task(self):
        if self._g_dictParam['slack_ch_ttl'] is None:
            self._printDebug('execution denied! -> you need to define slack_ch_ttl')
            return

        self.__g_sSlackChTitle = self._g_dictParam['slack_ch_ttl']
        oSvSlack = sv_slack.svSlack(sCallingBot='dbs')
        # oSvSlack.sendMsg('ddd')
        oSvSlack.delete_all(self.__g_sSlackChTitle)
        return

if __name__ == '__main__': # for console debugging and execution
    # python task.py analysis_namespace=test config_loc=1/ynox slack_ch_ttl=dbs_bot
    # slack_clear slack_ch_ttl=dbs_bot
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.parse_command(sys.argv)
            oJob.do_task()
            pass
    else:
        print('warning! [slack_ch_ttl] params are required for console execution.')
