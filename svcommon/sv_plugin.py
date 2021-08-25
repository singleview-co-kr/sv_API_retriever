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
from abc import ABC, abstractmethod

# 3rd party library

# singleview library

class ISvPlugin(ABC):
    _g_sVersion = None
    _g_sLastModifiedDate = None
    _g_oLogger = None
    _g_dictParam = {'analytical_namespace':None, 'config_loc':None}

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """ grammtical method to use with "with" statement """
        """ unconditionally calling desctructor """
        pass

    @abstractmethod
    def do_task(self):
        pass
    
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
        # print(self._g_dictParam)

if __name__ == '__main__': # for console debugging
	pass
