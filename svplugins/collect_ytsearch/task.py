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
import os
import re
import json
import time
import shutil
from datetime import datetime
# import html
# from urllib import parse

# # 3rd-party library

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_object
    import sv_plugin
    import sv_search_api
    import settings
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svcommon import sv_search_api
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oHtmlRemover = re.compile(r"<[^<]+?>")

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230605)')
        
        self._g_dictParam.update({'mode':None, 'morpheme':None})
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sDownloadPath = None
        self.__g_sConfPath = None
        self.__g_sTblPrefix = None
        # self.__g_lstMedia = []

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sTblPrefix = None
        self.__g_sMode = {}

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_sMode = self._g_dictParam['mode']

        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
          'nvr_ad_acct' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']

        # begin - set important folder
        self.__g_sDownloadPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, dict_acct_info['sv_account_id'], 
                                                dict_acct_info['brand_id'], 'youtube_search', 'data')
        if not os.path.isdir(self.__g_sDownloadPath):
            os.makedirs(self.__g_sDownloadPath)
        self.__g_sConfPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, dict_acct_info['sv_account_id'], 
                                            dict_acct_info['brand_id'], 'youtube_search', 'conf')
        if not os.path.isdir(self.__g_sConfPath):
            os.makedirs(self.__g_sConfPath)
        # end - set important folder

        if self.__g_sMode == 'collect_api':
            self._printDebug('-> communication begin')
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.collect_ytsearch')
                o_sv_mysql.initialize(self._g_dictSvAcctInfo)
                lst_morpheme = o_sv_mysql.executeQuery('getMorphemeActivated')
            
            n_idx = 0
            n_sentinel = len(lst_morpheme)
            for dict_single_morpheme in lst_morpheme:
                n_morpheme_srl=dict_single_morpheme['morpheme_srl']
                s_morpheme = dict_single_morpheme['morpheme']
                n_total_effective_cnt = self.__get_keyword_from_ytsearch(n_morpheme_srl, s_morpheme)
                self._printDebug(str(n_total_effective_cnt) + ' times retrieved')
                # return  #### limit to first morpheme ##################
                self._printProgressBar(n_idx + 1, n_sentinel, prefix = 'Collect JSON data:', suffix = 'Complete', length = 50)
                n_idx += 1
            self._printDebug('-> communication finish')
        elif self.__g_sMode == 'register_db':
            self.__register_raw_json_file()
        else:
            self._printDebug('mode is not specified.')

        self._task_post_proc(self._g_oCallback)
    
    def __get_keyword_from_ytsearch(self, n_param_morpheme_srl, s_param_morpheme):
        """ retrieve text from naver search API """
        o_sv_mysql = sv_mysql.SvMySql()
        o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
        o_sv_mysql.set_app_name('svplugins.collect_ytsearch')
        o_sv_mysql.initialize(self._g_dictSvAcctInfo)

        s_base_tsv_filename = datetime.now().strftime('%Y%m%d') + '_' + str(n_param_morpheme_srl)
        n_total_effective_cnt = 0
        n_term_retrieval_cnt = 0
        n_display_cnt = 50

        self._printDebug(s_param_morpheme + ' will be retrieved')
        o_sv_ytsearch = sv_search_api.SvYtSearch()
        o_sv_ytsearch.set_display_cnt(n_display_cnt)
        dict_1st_rst = o_sv_ytsearch.search_query(s_morpheme=s_param_morpheme)  # (s_morpheme='유한락스')  #
        if dict_1st_rst['b_error']:
            self._printDebug(dict_1st_rst['s_msg'])
            return

        n_total_effective_cnt += 1
        s_tsv_filename = s_base_tsv_filename + '_' + str(n_term_retrieval_cnt)
        self.__write_into_file(s_tsv_filename, dict_1st_rst['dict_body'])

        n_total_results = dict_1st_rst['dict_body']['pageInfo']['totalResults']
        if n_total_results > n_display_cnt:
            while True:
                time.sleep(1)
                dict_iter_rst = o_sv_ytsearch.search_query(s_morpheme=s_param_morpheme)
                n_term_retrieval_cnt += 1
                dict_iter_retrieval = dict_iter_rst['dict_body']
                if 'pageInfo' in dict_iter_retrieval and \
                        dict_iter_retrieval['pageInfo']['totalResults'] < n_term_retrieval_cnt * n_display_cnt:
                    self._printDebug('called requests exceeds API result count')
                    break

                n_total_effective_cnt += 1
                if dict_iter_rst['b_error']:
                    self._printDebug(dict_iter_rst['s_msg'])
                    break
                
                s_tsv_filename = s_base_tsv_filename + '_' + str(n_term_retrieval_cnt)
                self.__write_into_file(s_tsv_filename, dict_iter_retrieval)
                if 'items' in dict_iter_retrieval and dict_iter_retrieval['items']:
                    f_new_rate = self.__get_new_rate(o_sv_mysql, n_param_morpheme_srl, dict_iter_retrieval['items'])
                if f_new_rate < 0.1:
                    self._printDebug('too many duplicated item')
                    break
                del dict_iter_retrieval
                del dict_iter_rst
        else:
            time.sleep(1)

        self._printDebug(s_param_morpheme + ' has been retrieved')
        del dict_1st_rst
        del o_sv_mysql
        del o_sv_ytsearch
        return n_total_effective_cnt

    def __get_new_rate(self, o_sv_mysql, n_morpheme_srl, lst_item):
        """ get new API result rate to decide to stop stupid API call """
        f_new_rate = 0.0
        if lst_item:
            lst_new_old = [0, 0]  # new cnt, old cnt
            for dict_single_item in lst_item:
                if dict_single_item['kind'] == 'youtube#searchResult' and \
                        dict_single_item['id']['kind'] == 'youtube#video':
                    if self.__is_duplicated(o_sv_mysql, n_morpheme_srl, dict_single_item['id']['videoId']):
                        lst_new_old[1] += 1
                    else:
                        lst_new_old[0] += 1
                else:
                    lst_new_old[1] += 1
            f_new_rate = lst_new_old[0] / (lst_new_old[0] + lst_new_old[1])
        return f_new_rate  # return new item rate
    
    def __is_duplicated(self, o_sv_mysql, n_morpheme_srl, s_video_id):
        lst_rst = o_sv_mysql.executeQuery('getSearchLogByMorphemeVideoId', n_morpheme_srl, s_video_id) 
        if len(lst_rst) and 'log_srl' in lst_rst[0]:
            return True
        else:
            return False

    def __write_into_file(self, s_tsv_filename, s_content):
        # https://hayjo.tistory.com/75
        with open(os.path.join(self.__g_sDownloadPath, s_tsv_filename + '.json'), 'w', encoding='utf-8') as out:
            json.dump(s_content, out)  #, ensure_ascii=False)

    def __register_raw_json_file(self):
        """ referring to raw_data_file, arrange raw data file to register """
        o_sv_mysql = sv_mysql.SvMySql()
        o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
        o_sv_mysql.set_app_name('svplugins.collect_ytsearch')
        o_sv_mysql.initialize(self._g_dictSvAcctInfo)

        o_sv_ytsearch = sv_search_api.SvYtSearch()
        # traverse directory and collect json data files
        lst_json_files = os.listdir(self.__g_sDownloadPath)
        n_idx = 0
        n_sentinel = len(lst_json_files)
        for _, s_json_filename in enumerate(lst_json_files):
            lst_file_ext = os.path.splitext(s_json_filename)
            if lst_file_ext[1] != '.json':
                continue
            lst_file_info = s_json_filename.split('_')
            s_log_date = lst_file_info[0]
            s_morpheme_srl = lst_file_info[1]
            s_json_file_fullname = os.path.join(self.__g_sDownloadPath, s_json_filename)
            with open(s_json_file_fullname, 'r') as file:
                dict_body = json.load(file)
            self.__append_into_article_db(o_sv_mysql, s_morpheme_srl, dict_body, s_log_date)
            del dict_body
            self.__archive_data_file(s_json_filename)
            self._printProgressBar(n_idx + 1, n_sentinel, prefix = 'Register JSON file:', suffix = 'Complete', length = 50)
            n_idx += 1
        del o_sv_ytsearch
        del o_sv_mysql
        return

    def __append_into_article_db(self, o_sv_mysql, s_morpheme_srl, dict_body, s_log_date):
        for dict_single_item in dict_body['items']:
            # check duplication before registration
            if not self.__is_duplicated(o_sv_mysql, s_morpheme_srl, dict_single_item['id']['videoId']):
                dict_snippet = dict_single_item['snippet']
                o_sv_mysql.executeQuery('insertSearchLog', s_morpheme_srl, dict_snippet['channelId'], 
                                        dict_snippet['channelTitle'], dict_single_item['id']['videoId'], dict_snippet['title'], 
                                        dict_snippet['description'], dict_snippet['publishedAt'], s_log_date) 
                del dict_snippet

    def __archive_data_file(self, s_current_filename):
        if not os.path.exists(self.__g_sDownloadPath):
            self._printDebug('error: youtube search source directory does not exist!')
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        s_archive_path = os.path.join(self.__g_sDownloadPath, 'archive')
        if not os.path.exists(s_archive_path):
            os.makedirs(s_archive_path)

        s_source_filepath = os.path.join(self.__g_sDownloadPath, s_current_filename)
        s_archive_filepath = os.path.join(s_archive_path, s_current_filename)		
        shutil.move(s_source_filepath, s_archive_filepath)


if __name__ == '__main__': # for console debugging
    # CLI example -> python3.7 task.py config_loc=1/1 mode=collect_api
    # CLI example -> python3.7 task.py config_loc=1/1 mode=register_db
    # collect_ytsearch
    nCliParams = len(sys.argv)
    if nCliParams > 2:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('collect_ytsearch')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [mode] params are required for console execution.')
