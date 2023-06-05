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
import os
import configparser # https://docs.python.org/3/library/configparser.html
import sys

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_object
    import sv_plugin
    import settings
    import sv_doc_collection
    import morpheme_retriever
else: # for platform running
    from svcommon import sv_object
    from svcommon import sv_plugin
    from django.conf import settings
    from svplugins.collect_svdoc import sv_doc_collection
    from svplugins.collect_svdoc import morpheme_retriever


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_dictSource = {
        'singleview_estudio': 1,
        'twitter': 2,
        'naver': 3,
        'google': 4
        }

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230605)')
        
        self.__g_oConfig = configparser.ConfigParser()
        self._g_dictParam.update({'mode': None,
                                  'target_host_url': None,  # for sv doc retrieval
                                  'words': None, 'start_yyyymmdd': None, 'end_yyyymmdd': None,  # for morpheme analysis
                                  'module_srl': None  # for module-level retrieval
                                })
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
    
    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_oConfig = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        s_target_host_url = self._g_dictParam['target_host_url']
        s_mode = self._g_dictParam['mode']

        s_comma_sep_words = self._g_dictParam['words']
        s_start_yyyymmdd = self._g_dictParam['start_yyyymmdd']
        s_end_yyyymmdd = self._g_dictParam['end_yyyymmdd']
        s_module_srl = self._g_dictParam['module_srl']

        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
                'nvr_ad_acct' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        if s_mode is None:
            self._printDebug('you should designate mode')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        s_tbl_prefix = dict_acct_info['tbl_prefix']
        self.__get_key_config(s_sv_acct_id, s_brand_id)
        
        if s_mode == 'retrieve':
            if s_target_host_url is None:
                if 'server' in self.__g_oConfig:
                    s_target_host_url = self.__g_oConfig['server']['sv_doc_host_url']
                else:
                    self._printDebug('stop -> invalid sv_doc_host_url')
                    self._task_post_proc(self._g_oCallback)
                    if self._g_bDaemonEnv:  # for running on dbs.py only
                        raise Exception('remove')
                    else:
                        return
            o_sv_doc_collector = sv_doc_collection.SvDocCollection()
            o_sv_doc_collector.init_var(self._g_dictSvAcctInfo, s_tbl_prefix,
                                        self._printDebug, self._printProgressBar, self._continue_iteration,
                                        self.__g_oConfig, self.__g_dictSource, s_target_host_url)
            o_sv_doc_collector.collect_sv_doc()
            del o_sv_doc_collector
        elif s_mode in ['analyze_new', 'tag_ignore_word', 'add_custom_noun', 'get_period']:
            o_sv_morpheme_retriever = morpheme_retriever.SvMorphRetriever()
            o_sv_morpheme_retriever.init_var(self._g_dictSvAcctInfo, s_tbl_prefix,
                                        self._printDebug, self._printProgressBar, self._continue_iteration,
                                        self._g_sPluginName, self._g_sAbsRootPath, settings.SV_STORAGE_ROOT,
                                        s_mode, s_comma_sep_words, s_start_yyyymmdd, s_end_yyyymmdd, s_module_srl)
            o_sv_morpheme_retriever.do_task()
            del o_sv_morpheme_retriever
        else:
            self._printDebug('weird')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        self._task_post_proc(self._g_oCallback)
        
    def __get_key_config(self, sSvAcctId, sAcctTitle):
        sKeyConfigPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, sSvAcctId, sAcctTitle, 'key.config.ini')
        try:
            with open(sKeyConfigPath) as f:
                self.__g_oConfig.read_file(f)
        except FileNotFoundError:
            self._printDebug('key.config.ini not exist')
            return  # raise Exception('stop')

        self.__g_oConfig.read(sKeyConfigPath)


if __name__ == '__main__': # for console debugging
    # CLI example -> python3.7 task.py config_loc=1/1
    # collect_svdoc mode=retrieve
    # collect_svdoc mode=analyze_new
    # collect_svdoc mode=tag_ignore_word words=a,b,c,d
    # collect_svdoc mode=add_custom_noun words=a,b,c,d
    # collect_svdoc mode=get_period start_yyyymmdd=20220101 end_yyyymmdd=20220102 module_srl=1234
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('collect_svdoc')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [mode] params are required for console execution.')
