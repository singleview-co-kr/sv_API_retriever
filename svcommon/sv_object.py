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

import logging
# from classes import sv_events


class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class SvErrorHandler(Error):
    """ Raised when the http ['variables']['todo'] is set """
    __g_oLogger = None

    def __init__(self, s_todo):
        self.__g_oLogger = logging.getLogger(__file__)
        # self.__print_debug(sTodo)
        if s_todo == 'stop':
            self.__print_debug('should stop job and wait next schedule')
            # sys.exit(sv_events.EVENT_JOB_SHOULD_BE_STOPPED)
            # sys.exit() signal in sv_http module does not reach to scheduler, as this module is not called directly
        else:
            self.__print_debug('general error occured')


class ISvObject(Error):
    _g_oLogger = None
    _g_oWebsocket = None

    def __init__(self):
        # not work at all
        pass

    def set_websocket_output(self, o_websocket_display_pipe):
        self._g_oWebsocket = o_websocket_display_pipe

    def _print_progress_bar(self, iteration, total, prefix='', suffix='', decimals=1, length=100, fill='='):
        """
        Print iterations progress
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
        """
        if __name__ == 'svcommon.sv_object' and self._g_oWebsocket is not None:
            if iteration == total:
                self._print_debug(prefix + ' 100% done')
            elif iteration == int(total / 4):
                self._print_debug(prefix + ' 25% done')
            elif iteration == int(total / 2):
                self._print_debug(prefix + ' 50% done')
            elif iteration == int(total * 0.75):
                self._print_debug(prefix + ' 75% done')
        elif __name__ == 'sv_object':  # for console debugging
            percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
            n_filled_length = int(length * iteration // total)
            bar = fill * n_filled_length + '-' * (length - n_filled_length)
            print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
            # Print New Line on Complete
            if iteration == total: 
                print()

    def _print_debug(self, s_msg):
        if __name__ == 'svcommon.sv_object' and self._g_oWebsocket is not None:
            if type(s_msg) != str:
                s_msg = str(s_msg)
            self._g_oWebsocket(s_msg)
        elif __name__ == 'sv_object':  # for console debugging
            print(s_msg)

        if self._g_oLogger is not None:
            self._g_oLogger.debug(s_msg)
