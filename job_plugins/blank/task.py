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
else: # for platform running
    #from classes import sv_mysql
    # singleview config
    pass

class svJobPlugin():
    __g_sVersion = '0.0.1'
    __g_sLastModifiedDate = '15th, Feb 2018'
    __g_oLogger = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)

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
        self.__printDebug( 'no matched brs_info.tsv' )
        raise Exception('stop')
        return
        #for i in range(1):
        #	self.__printDebug( '-> blank job ' + ' cnt ' + str(i) + ' has raised handover exception!')
        #	#raise Exception('completed' )
        #	#time.sleep(3)

if __name__ == '__main__': # for console debugging
    dictPluginParams = {'thread': 12345678}
    with svJobPlugin(dictPluginParams) as oJob: # to enforce to call plugin destructor
        try:
            if( sys.argv[1] ):
                oJob.getConsoleAuth( sys.argv )
        except IndexError as error:
            oJob.procTask()