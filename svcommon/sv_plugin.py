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

import os
from os import listdir
import logging
import importlib

# 3rd party library
from decouple import config  # https://pypi.org/project/python-decouple/

# singleview library
if __name__ == 'sv_plugin': # for console debugging
    sys.path.append('../../svcommon')
    import sv_api_config_parser
    import sv_events
else:
    from svcommon import sv_api_config_parser
    from svcommon import sv_events

class ISvPlugin(ABC):
    _g_sAbsRootPath = None
    _g_oLogger = None
    _g_oThread = None  # AttributeError: 'svJobPlugin' object has no attribute '_g_oThread' if move to __init__
    _g_dictParam = {'config_loc':None}  # can't recognize attr if move to __init__
    _g_dictSvAcctInfo = {'n_acct_id':None, 'n_brand_id': None}  # can't recognize attr if move to __init__
    """ to raise an exeception on the daemonocle running env """
    _g_bDaemonEnv = False

    def __init__(self):
        self._g_sPluginName = None
        self._g_oCallback = None  # callback for self desturction

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """ grammtical method to use with "with" statement """
        """ unconditionally calling desctructor """
        pass
    
    def set_my_name(self, s_plugin_name):
        self._g_sPluginName = s_plugin_name

    def toggle_daemon_env(self):
        """ for svPluginDaemonJob only """
        self._g_bDaemonEnv = True

    def _task_pre_proc(self, o_callback):
        if o_callback:  # regarding an execution on a web console 
            if self._g_sPluginName is None:
                self._printDebug('You must run .set_my_name() before launch a plugin task')
                o_callback(self._g_sPluginName)
                return
            self._g_oThread = threading.currentThread()

        self._g_sAbsRootPath = config('ABSOLUTE_PATH_BOT')
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self._g_dictParam['config_loc'])
        return oSvApiConfigParser.getConfig()

    def _task_post_proc(self, o_callback):
        if o_callback:  # regarding an execution on a web console 
            o_callback(self._g_sPluginName)
        self._g_oThread = None

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
                s_arg = lst_command[i]
                for s_param_name in self._g_dictParam:
                    n_pos = s_arg.find(s_param_name + '=')
                    if n_pos > -1:
                        lst_param_pair = s_arg.split('=')
                        self._g_dictParam[s_param_name] = lst_param_pair[1]
        
        if 'config_loc' in self._g_dictParam.keys():
            lst_acct_info = self._g_dictParam['config_loc'].split('/')
            self._g_dictSvAcctInfo['n_acct_id'] = lst_acct_info[0]
            self._g_dictSvAcctInfo['n_brand_id'] = lst_acct_info[1]


class svPluginDaemonJob():
    """ for dbs.py only """
    __g_oLogger = None

    def __init__(self, *lst_plugin_params):
        # https://docs.python.org/3.6/library/importlib.html
        # logging.info('svPluginDaemonJob has been started')
        s_plugin_title = lst_plugin_params[0]
        s_config_loc_param = lst_plugin_params[1]
        s_extra_param = lst_plugin_params[2]
        # raise SvErrorHandler('remove') # raise event code to remove job if the connected method is invalid
        lst_command = []  # make same cmd line like a console
        lst_command.append(s_plugin_title)
        lst_command.append(s_config_loc_param)
        s_extra_param_without_eol = s_extra_param.replace("\r\n", " ")
        lst_command += s_extra_param_without_eol.split(' ')
        lst_command = [x for x in lst_command if x]  # remove empty entity after replace "\r\n" to " "
        try:
            o_job_plugin = importlib.import_module('svplugins.' + s_plugin_title + '.task')
            with o_job_plugin.svJobPlugin() as o_job: # to enforce each plugin follow strict guideline or remove from scheduler
                self.__print_debug(o_job.__class__.__name__ + ' has been initiated')
                o_job.set_my_name(s_plugin_title)
                o_job.toggle_daemon_env()
                o_job.parse_command(lst_command)
                o_job.do_task(None)
        except AttributeError: # if task module does not have svJobPlugin
            self.__print_debug('plugin does not have correct method -> remove job')
            raise SvErrorHandler('remove')
        except ModuleNotFoundError:
            self.__print_debug('plugin is not existed -> remove job')
            raise SvErrorHandler('remove')
        except Exception as err:
            nIdx = 0
            for e in err.args:
                if e == 'stop' or e == 'wait': # handle HTTP err response from XE
                    self.__print_debug('handle stop: ' + e)
                    raise SvErrorHandler(e)
                elif e == 'completed': # handle completed exception signal from each job
                    self.__print_debug('raised completed job!')
                    raise SvErrorHandler(e)
                try:
                    self.__print_debug('plugin occured general exception arg' + str(nIdx) + ': ' + e)
                except TypeError:
                    self.__print_debug('plugin occured general exception arg' + str(nIdx) + ': ')
                    self.__print_debug(e)
                nIdx += 1
            raise SvErrorHandler('remove')
        finally:
            del lst_command

    def __print_debug(self, s_msg):
        # if __name__ == '__main__' or __name__ == 'sv_plugin':
        if __name__ in ['__main__', 'sv_plugin']:
            print(s_msg)
        else:
            if self.__g_oLogger is not None:
                self.__g_oLogger.debug(s_msg)


class SvPluginValidation():
        
    def validate(self, s_plugin_name):
        """ find the module directory in /svplugins folder """
        if s_plugin_name in listdir(os.path.join(config('ABSOLUTE_PATH_BOT'), 'svplugins')):
            return True
        return False


class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class SvErrorHandler(Error):
    """Raised when the http ['variables']['todo'] is set """
    __g_oLogger = None

    def __init__(self, s_todo):
        self.__g_oLogger = logging.getLogger(__file__)
        # create logger
        logging.basicConfig(
            filename= config('ABSOLUTE_PATH_BOT') + '/log/SvErrorHandler.log',
            level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s',
        )
        logging.info('svPluginDaemonJob has been started')
        
        if s_todo == 'stop':
            self.__print_debug('should stop job and wait next schedule')
            sys.exit(sv_events.EVENT_JOB_SHOULD_BE_STOPPED) # sys.exit() signal in sv_http module does not reach to scheduler, as this module is not called directly
        elif s_todo == 'wait': # might be removed????
            self.__print_debug('wait 3 seconds till problem solved')
            time.sleep(3)
        elif s_todo == 'remove':
            self.__print_debug('remove job immediately')
            sys.exit(sv_events.EVENT_JOB_SHOULD_BE_REMOVED) # raise event code to remove job if the connected method is invalid
        elif s_todo == 'completed':
            self.__print_debug('remove and toggle job table')
            sys.exit(sv_events.EVENT_JOB_COMPLETED) # raise event code to remove job if the connected method is invalid
        elif s_todo == 'time_sync':
            self.__print_debug('stop scheduler')
        else:
            self.__print_debug('general error occured')

    def __print_debug(self, s_msg):
        if __name__ == '__main__':
            print(s_msg)
        else:
            if self.__g_oLogger is not None:
                self.__g_oLogger.debug(s_msg)


# if __name__ == '__main__': # for console debugging
# 	pass
