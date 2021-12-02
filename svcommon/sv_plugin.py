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
import sys
from abc import ABC
from abc import abstractmethod
import threading
import time

# 3rd party library
from decouple import config  # https://pypi.org/project/python-decouple/

# singleview library
if __name__ == 'sv_plugin': # for console debugging
    sys.path.append('../../svcommon')
    import sv_api_config_parser
else:
    from svcommon import sv_api_config_parser

class ISvPlugin(ABC):
    _g_sPluginName = None
    _g_sVersion = None
    _g_sAbsRootPath = None
    _g_sLastModifiedDate = None
    _g_oLogger = None
    _g_dictParam = {'analytical_namespace':None, 'config_loc':None}
    _g_oThread = None

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """ grammtical method to use with "with" statement """
        """ unconditionally calling desctructor """
        pass
    
    def set_my_name(self, s_plugin_name):
        self._g_sPluginName = s_plugin_name

    def _task_pre_proc(self, o_callback):
        if o_callback:  # regarding an execution on a web console 
            if self._g_sPluginName is None:
                self._printDebug('You must run .set_my_name() before launch a plugin task')
                o_callback(self._g_sPluginName)
                return
            self._g_oThread = threading.currentThread()

        self._g_sAbsRootPath = config('ABSOLUTE_PATH_BOT')
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self._g_dictParam['analytical_namespace'], self._g_dictParam['config_loc'])
        return oSvApiConfigParser.getConfig()

    def _task_post_proc(self, o_callback):
        if o_callback:  # regarding an execution on a web console 
            o_callback(self._g_sPluginName)

    @abstractmethod
    def do_task(self, o_callback):
        if self._g_sPluginName is None:
            o_callback(self._g_sPluginName)
            return

        self._g_oThread = threading.currentThread()  # in the begining
        # begin something
        i = 0
        while self._continue_iteration():  # getattr(self._g_oThread, "b_run", True):
            self._printDebug(self._g_sPluginName + " "+str(i))
            i = i + 1
            time.sleep(2)
            if i > 105:
                break
        # finish something
        o_callback(self._g_sPluginName)  # in the end
    
    def _continue_iteration(self):
        return getattr(self._g_oThread, 'b_run', True)  # regading a console execution, return True if attr not existed

    def parse_command(self, lst_command):
        n_params = len(lst_command)
        if n_params >= 2:
            for i in range(1, n_params):
                sArg = lst_command[i]
                for s_param_name in self._g_dictParam:
                    n_pos = sArg.find(s_param_name + '=')
                    if n_pos > -1:
                        lst_param_pair = sArg.split('=')
                        self._g_dictParam[s_param_name] = lst_param_pair[1]


# if __name__ == '__main__': # for console debugging
# 	pass
