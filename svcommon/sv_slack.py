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
import requests
import logging
import configparser # https://docs.python.org/3/library/configparser.html

# 3rd party library
from slack_cleaner2 import *

# singleview config
if __name__ == 'svcommon.sv_slack': # for platform running
    from conf import basic_config
    from svcommon import sv_object
elif __name__ == 'sv_slack': # for plugin console debugging
    sys.path.append('../../conf')
    import basic_config
    import sv_object
elif __name__ == '__main__': # for class console debugging
    sys.path.append('../conf')
    import basic_config

    
class svSlack(sv_object.ISvObject):
    """ bot notice through slack messenger class for singleview only """
    # https://somjang.tistory.com/entry/Python-Slack-WebHooks-%EC%9D%84-%ED%86%B5%ED%95%B4-%EC%9E%91%EC%97%85-%EC%A7%84%ED%96%89%EC%83%81%ED%99%A9-%EC%95%8C%EB%A6%BC-%EB%B0%9B%EC%95%84%EB%B3%B4%EA%B8%B0-feat-Incoming-WebHooks
    # https://lsjsj92.tistory.com/594
    __g_oConfig = None
    __g_sCallingBot = None
    __g_bAvailable = False

    def __init__(self, sCallingBot):
        self.__g_oConfig = configparser.ConfigParser()
        self._g_oLogger = logging.getLogger(__file__)
        sSlackConfigFile = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'conf', 'slack_config.ini')
        
        if sCallingBot == 'dbs':
            self.__g_sCallingBot = 'DBS'
        elif sCallingBot == 'dbo':
            self.__g_sCallingBot = 'DBO'
        
        try:
            with open(sSlackConfigFile) as f:
                self.__g_oConfig.read_file(f)
                self.__g_bAvailable = True
        except IOError:
            self._printDebug('slack_config.ini does not exist')
            # raise IOError('failed to initialize SvSlack')

        if self.__g_bAvailable:
            self.__g_oConfig.read(sSlackConfigFile)

    def sendMsg(self, s_msg):
        if not self.__g_bAvailable:
            self._printDebug('execution denied')
            return

        if len(s_msg):
            dict_msg_body = { "channel": self.__g_oConfig[self.__g_sCallingBot]['channel'], 
                                "username": self.__g_oConfig['COMMON']['bot_name'], 
                                "text": self.__g_sCallingBot + ' > ' + s_msg + '\n' }
            webhook_url = self.__g_oConfig['COMMON']['web_hook_url']
            response = requests.post(
                webhook_url, json=dict_msg_body, headers={'Content-Type': 'application/json'}
            )
            if response.status_code != 200:
                raise ValueError(
                    'Request to slack returned an error %s, the response is:\n%s' % (response.status_code, response.text)
                )
            # slack_client = slack.WebClient(self.__g_oConfig['COMMON']['api_token'], timeout=30)
            # sChannel = '#' + self.__g_oConfig[self.__g_sCallingBot]['channel']
            # sMsgToSend = self.__g_oConfig['COMMON']['bot_name'] + ' > ' + self.__g_sCallingBot + ' > ' + sMsg + '\n'
            # slack_client.chat_postMessage(channel=sChannel, text=sMsgToSend)
        else:
            self._printDebug(__file__ + ' has requested to send blank message!')

    def delete_all(self, s_channel_name):
        if not self.__g_bAvailable:
            self._printDebug('execution denied')
            return
            
        s = SlackCleaner(self.__g_oConfig['COMMON']['slack_user_oauth_token'])
        # list of users
        self._printDebug(s.users)
        # list of all kind of channels
        # print(s.conversations)

        # s.conversations returns <class 'slack_cleaner2.model.SlackChannel'>
        lst_channels = [str(o_slack_ch) for o_slack_ch in s.conversations]
        if s_channel_name not in lst_channels:
            self._printDebug('invalid channel title')
            return

        # delete all messages in general channels
        for msg in s.msgs(filter(match(s_channel_name), s.conversations)):
        	# delete messages, its files, and all its replies (thread)
        	msg.delete(replies=True, files=True)

        # delete all general messages and also iterate over all replies
        for msg in s.c.general.msgs(with_replies=True):
        	msg.delete()


#if __name__ == '__main__': # for console debugging
#    oSvSlack = svSlack('dbs')
#    # oSvSlack.sendMsg('ddd')
#    oSvSlack.delete_all('dbs_bot')
