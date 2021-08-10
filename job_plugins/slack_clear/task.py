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
import time

# singleview library
if __name__ == '__main__': # for console debugging
    import sys
    sys.path.append('../../classes')
    import sv_slack
else: # for platform running
    from classes import sv_slack
    # singleview config
    pass

class svJobPlugin():
    __g_sVersion = '0.0.1'
    __g_sLastModifiedDate = '21st, Jun 2021'
    __g_oLogger = None
    __g_sSlackChTitle = None # slack channel to bulk delete

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        # print(dictParams)
        self.__g_sSlackChTitle = dictParams['slack_ch_ttl']

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __printDebug( self, sMsg ):
        if __name__ == '__main__': # for console debugging
            print( sMsg )
        else: # for platform running
            if( self.__g_oLogger is not None ):
                self.__g_oLogger.debug( sMsg )

    def procTask(self):
        self.__printDebug( 'start' )
        oSvSlack = sv_slack.svSlack('dbs')
        # oSvSlack.sendMsg('ddd')
        oSvSlack.delete_all(self.__g_sSlackChTitle)
        return

if __name__ == '__main__': # for console debugging and execution
    dictPluginParams = {'slack_ch_ttl':None} # {'slack_ch_ttl':'test_acct_web'}
    nCliParams = len(sys.argv)
    if( nCliParams > 1 ):
        for i in range(nCliParams):
            if i is 0:
                continue

            sArg = sys.argv[i]
            for sParamName in dictPluginParams:
                nIdx = sArg.find( sParamName + '=' )
                if( nIdx > -1 ):
                    aModeParam = sArg.split('=')
                    dictPluginParams[sParamName] = aModeParam[1]
                
        #print( dictPluginParams )
        with svJobPlugin(dictPluginParams) as oJob: # to enforce to call plugin destructor
            oJob.procTask()
    else:
        print( 'warning! [slack_ch_ttl] params are required for console execution.' )
