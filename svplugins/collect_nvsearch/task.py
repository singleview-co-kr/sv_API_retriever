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
import html
import time
import shutil
from datetime import datetime
from urllib import parse
from random import choice
import requests
from requests.exceptions import ProxyError
from requests.exceptions import SSLError
from requests.exceptions import ConnectTimeout
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ReadTimeout
from requests.exceptions import ConnectionError

# 3rd-party library
from bs4 import BeautifulSoup
from lxml import etree

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_object
    import sv_plugin
    import sv_search_api
    import sv_slack
    import settings
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svcommon import sv_search_api
    from svcommon import sv_slack
    from django.conf import settings


class SvJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oHtmlRemover = re.compile(r"<[^<]+?>")
    __g_nDelaySec = 2

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20240102)')
        
        self._g_dictParam.update({'mode': None, 'morpheme': None})
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sDownloadPath = None
        self.__g_sConfPath = None
        self.__g_sTblPrefix = None
        self.__g_lstMedia = []
        self.__g_sStoragePath = None
        self.__g_sMode = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sDownloadPath = None
        self.__g_sConfPath = None
        self.__g_sTblPrefix = None
        self.__g_lstMedia = []
        self.__g_sStoragePath = None
        self.__g_sMode = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_sMode = self._g_dictParam['mode']

        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
                'nvr_ad_acct' not in dict_acct_info:
            self._print_debug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']
        self.__g_lstMedia = dict_acct_info['nvr_search']

        # begin - set important folder
        self.__g_sDownloadPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT,
                                              dict_acct_info['sv_account_id'], dict_acct_info['brand_id'],
                                              'naver_search', 'data')
        if not os.path.isdir(self.__g_sDownloadPath):
            os.makedirs(self.__g_sDownloadPath)
        self.__g_sConfPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT,
                                          dict_acct_info['sv_account_id'], dict_acct_info['brand_id'],
                                          'naver_search', 'conf')
        if not os.path.isdir(self.__g_sConfPath):
            os.makedirs(self.__g_sConfPath)
        # end - set important folder
        
        if self.__g_sMode == 'collect_api':
            self.__collect_api()
        elif self.__g_sMode == 'register_db':
            self.__register_raw_xml_file()
        elif self.__g_sMode == 'update_kin_date':
            self.__update_kin_date(dict_acct_info)
        else:
            self._print_debug('start collect api')
            self.__collect_api()
            self._print_debug('finish collect api')
            self._print_debug('start register db')
            self.__register_raw_xml_file()
            self._print_debug('finish register db')
            self._print_debug('start update kin date')
            self.__update_kin_date(dict_acct_info)
            self._print_debug('finish update kin date')

        self._task_post_proc(self._g_oCallback)

    def __collect_api(self):
        self._print_debug('-> communication begin')
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_nvsearch')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            lst_morpheme = o_sv_mysql.execute_query('getMorphemeActivated')
        
        for dict_single_morpheme in lst_morpheme:
            n_morpheme_srl=dict_single_morpheme['morpheme_srl']
            s_morpheme = dict_single_morpheme['morpheme']
            n_total_effective_cnt = self.__get_keyword_from_nvsearch(n_morpheme_srl, s_morpheme)
            del dict_single_morpheme
            self._print_debug(str(n_total_effective_cnt) + ' times retrieved')
            # return  #### limit to first morpheme ##################
        self._print_debug('-> communication finish')

    def __update_kin_date(self, dict_acct_info):
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        self.__g_sStoragePath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id,
                                             s_brand_id, 'naver_search', 'kin_html')
        if not os.path.isdir(self.__g_sStoragePath):
            os.makedirs(self.__g_sStoragePath)

        o_sv_mysql = sv_mysql.SvMySql()
        o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
        o_sv_mysql.set_app_name('svplugins.collect_nvsearch')
        o_sv_mysql.initialize(self._g_dictSvAcctInfo)
        lst_nvsearch_log = o_sv_mysql.execute_query('getNvrSearchApiKinByLogdate')
        n_sentinel = len(lst_nvsearch_log)
        if n_sentinel == 0:
            self._print_debug('no more crawling task to proceed')
            return

        # self._print_debug('crawling task will take ' + str(int(n_sentinel * self.__g_nDelaySec / 60)) + ' mins at most')
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36",
            # "content-type": "application/json",
        }
        lst_proxy = []
        s_proxy_server = None
        dict_log = None
        n_idx = 0
        while True:
            if s_proxy_server is None:
                if len(lst_proxy) == 0:
                    lst_proxy = self.__get_proxy_list()
                if len(lst_proxy):
                    s_proxy_server = choice(lst_proxy)
                else:
                    self._print_debug("wait 2 seconds from now on...")
                    time.sleep(2)
                    continue
                proxies = {"http": s_proxy_server, 'https': s_proxy_server}
            if dict_log is None:
                dict_log = lst_nvsearch_log.pop(0)
                n_trial_cnt_for_single_kin = 0
            o_resp = None
            try:
                o_resp = requests.get(dict_log['link'], headers=headers, proxies=proxies, timeout=5)
            except (ProxyError, SSLError, ConnectTimeout, ChunkedEncodingError, ReadTimeout, ConnectionError) as e:
                lst_proxy.remove(s_proxy_server)
                s_proxy_server = None
            if o_resp:
                o_soup = BeautifulSoup(o_resp.text, 'html.parser')
                o_dom = etree.HTML(str(o_soup))  # 15567 끌올
                lst_pub_date = o_dom.xpath('//*[@id="content"]/div[1]/div/div[3]/div[1]/span[1]/text()')
                n_elem = len(lst_pub_date)

                if n_elem == 1:  # available to access and get post date + hidden naver nickname
                    s_pub_date = lst_pub_date[0]
                elif n_elem == 2:  # available to access and get post date + shown naver nickname
                    s_pub_date = lst_pub_date[1]
                else:
                    s_pub_date = None

                if s_pub_date:
                    if s_pub_date.find('시간') == -1:  # 2023.03.19
                        pass
                    else:  # 15시간 전
                        s_pub_date = datetime.today().strftime('%Y.%m.%d')
                    o_sv_mysql.execute_query('updateNvrSearchApiByLogSrl', s_pub_date, dict_log['log_srl'])
                    self.__save_html(dict_log['log_srl'], o_resp.text)
                else:  # adult only kin posting has been restricted
                    o_sv_mysql.execute_query('updateNvrSearchApiCrawledByLogSrl', dict_log['log_srl'])
                del o_resp
                del o_dom
                dict_log = None
                self._print_progress_bar(n_idx + 1, n_sentinel, prefix='retrieve NVR kin post date:', suffix='Complete', length=50)
                n_idx += 1
            else:
                n_trial_cnt_for_single_kin += 1
                if n_trial_cnt_for_single_kin == 4:
                    lst_proxy.remove(s_proxy_server)
                    s_proxy_server = None
                    n_trial_cnt_for_single_kin = 0
                    self._print_debug('change proxy server and try again the kin post ' + str(dict_log['log_srl']))
            if len(lst_nvsearch_log) == 0:
                break
            time.sleep(self.__g_nDelaySec)

        del o_sv_mysql
        s_brand_name = dict_acct_info['brand_name'] if dict_acct_info['brand_name'] else 'unknown brand'
        o_slack = sv_slack.SvSlack('dbs')
        o_slack.send_msg(s_brand_name + ' scraping has been finished successfully')
        del o_slack

    def __save_html(self, n_log_srl, s_html):
        s_html_file_path = os.path.join(self.__g_sStoragePath, str(n_log_srl) + '.html')
        with open(s_html_file_path, 'w', encoding='utf-8') as out:
            out.write(s_html)

    def __get_proxy_list(self):
        o_resp = requests.get("https://free-proxy-list.net/")
        o_soup = BeautifulSoup(o_resp.text, 'html.parser')
        del o_resp
        o_proxy_tbl = o_soup.select_one('#list > div > div.table-responsive > div > table > tbody')
        del o_soup
        # lst_allowed_country = ['KR', 'JP', 'US', 'TW', 'SG', 'HK']
        lst_proxy = []
        lst_tr = o_proxy_tbl.find_all("tr")
        for o_tr in lst_tr:
            s_ip = o_tr.select_one('td:nth-of-type(1)').get_text()
            s_port = o_tr.select_one('td:nth-of-type(2)').get_text()
            s_code = o_tr.select_one('td:nth-of-type(3)').get_text()
            s_https = o_tr.select_one('td:nth-of-type(7)').get_text()
            # if s_code in lst_allowed_country and s_https == "yes":
            # print(s_https )
            if s_https == "yes":
                s_server = f"{s_ip}:{s_port}"
                lst_proxy.append(s_server)
        #if len(lst_proxy) == 0:  # select http proxy serve if no https server
        #    for o_tr in lst_tr:
        #        s_ip = o_tr.select_one('td:nth-of-type(1)').get_text()
        #        s_port = o_tr.select_one('td:nth-of-type(2)').get_text()
        #        s_code = o_tr.select_one('td:nth-of-type(3)').get_text()
        #        s_https = o_tr.select_one('td:nth-of-type(7)').get_text()
        #        s_server = f"{s_ip}:{s_port}"
        #        lst_proxy.append(s_server)
        self._print_debug('retrieve ' + str(len(lst_proxy)) + ' new proxies')
        del o_proxy_tbl
        # del lst_allowed_country
        del lst_tr
        del o_tr
        return lst_proxy

    def __get_keyword_from_nvsearch(self, n_param_morpheme_srl, s_param_morpheme):
        """ retrieve text from naver search API """        
        o_sv_nvsearch = sv_search_api.SvNvSearch()
        dict_media_lbl_id = o_sv_nvsearch.get_media_lbl_id_dict()

        o_sv_mysql = sv_mysql.SvMySql()
        o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
        o_sv_mysql.set_app_name('svplugins.collect_nvsearch')
        o_sv_mysql.initialize(self._g_dictSvAcctInfo)

        s_base_tsv_filename = datetime.now().strftime('%Y%m%d') + '_' + str(n_param_morpheme_srl)
        n_total_effective_cnt = 0

        self._print_debug(s_param_morpheme + ' will be retrieved')
        n_idx = 0
        n_sentinel = len(self.__g_lstMedia)
        for s_media in self.__g_lstMedia:
            if s_media in dict_media_lbl_id:
                n_media_retrieval_cnt = 0
                n_display_cnt = 100
                n_media_id = dict_media_lbl_id[s_media]
                o_sv_nvsearch.set_media(s_media)
                o_sv_nvsearch.set_display_cnt(n_display_cnt)
                dict_1st_rst = o_sv_nvsearch.search_query(s_morpheme=s_param_morpheme)
                if dict_1st_rst['b_error']:
                    self._print_debug(dict_1st_rst['s_msg'])
                    continue
                n_total_effective_cnt += 1
                s_tsv_filename = s_base_tsv_filename + '_' + s_media + '_' + str(n_media_retrieval_cnt)
                self.__write_into_file(s_tsv_filename, dict_1st_rst['s_plain_resp'])
                dict_1st_retrieval = dict_1st_rst['dict_xml_body']
                if dict_1st_retrieval['total'] > n_display_cnt:
                    while True:
                        time.sleep(1)
                        dict_iter_rst = o_sv_nvsearch.search_query(s_morpheme=s_param_morpheme)
                        n_media_retrieval_cnt += 1
                        dict_iter_retrieval = dict_iter_rst['dict_xml_body']
                        if 'total' in dict_iter_retrieval and \
                                dict_iter_retrieval['total'] < n_media_retrieval_cnt * n_display_cnt:  # 'total' attr changes randomly if media is webkr
                            # self._print_debug('called requests exceeds API result count')
                            break

                        n_total_effective_cnt += 1
                        if dict_iter_rst['b_error']:
                            # self._print_debug(dict_iter_rst['s_msg'])
                            break
                        
                        s_tsv_filename = s_base_tsv_filename + '_' + s_media + '_' + str(n_media_retrieval_cnt)
                        self.__write_into_file(s_tsv_filename, dict_iter_rst['s_plain_resp'])
                        if 'item' in dict_iter_retrieval and \
                                dict_iter_retrieval['item']:
                            f_new_rate = self.__get_new_rate(o_sv_mysql, n_param_morpheme_srl, s_media, n_media_id, dict_iter_retrieval['item'])
                        if f_new_rate < 0.1:
                            # self._print_debug('too many duplicated item')
                            break
                        del dict_iter_retrieval
                        del dict_iter_rst
                else:
                    time.sleep(1)
                        
                del dict_1st_retrieval
                del dict_1st_rst
            self._print_progress_bar(n_idx + 1, n_sentinel, prefix='Collect XML data:', suffix='Complete', length=50)
            n_idx += 1
        self._print_debug(s_param_morpheme + ' has been retrieved')
        del o_sv_mysql
        del o_sv_nvsearch
        del dict_media_lbl_id
        return n_total_effective_cnt

    def __get_new_rate(self, o_sv_mysql, n_morpheme_srl, s_media, n_media_id, lst_item):
        """ get new API result rate to decide to stop stupid API call """
        f_new_rate = 0.0
        if lst_item:
            lst_new_old = [0, 0]  # new cnt, old cnt
            for dict_single_item in lst_item:
                if s_media == 'kin':  # naver search API arbirarily changes KIN doc URL
                    dict_single_item['link'] = self.__cleanup_kin_url(dict_single_item['link'])
                if self.__is_duplicated(o_sv_mysql, n_morpheme_srl, n_media_id, dict_single_item['link']):
                    lst_new_old[1] += 1
                else:
                    lst_new_old[0] += 1
            f_new_rate = lst_new_old[0] / (lst_new_old[0] + lst_new_old[1])
        return f_new_rate  # return new item rate
    
    def __is_duplicated(self, o_sv_mysql, n_morpheme_srl, n_media_id, s_link):
        lst_rst = o_sv_mysql.execute_query('getSearchLogByMorphemeMediaLink', n_morpheme_srl, n_media_id, s_link)
        if len(lst_rst) and 'log_srl' in lst_rst[0]:
            return True
        else:
            return False

    def __write_into_file(self, s_tsv_filename, s_content):
        with open(os.path.join(self.__g_sDownloadPath, s_tsv_filename + '.xml'), 'w', encoding='utf-8') as out:
            out.write(s_content)

    def __register_raw_xml_file(self):
        """ referring to raw_data_file, arrange raw data file to register """
        o_sv_mysql = sv_mysql.SvMySql()
        o_sv_mysql.set_tbl_prefix(self.__g_sTblPrefix)
        o_sv_mysql.set_app_name('svplugins.collect_nvsearch')
        o_sv_mysql.initialize(self._g_dictSvAcctInfo)

        o_sv_nvsearch = sv_search_api.SvNvSearch()
        dict_media_lbl_id = o_sv_nvsearch.get_media_lbl_id_dict()

        # traverse directory and collect xml data files
        lst_xml_files = os.listdir(self.__g_sDownloadPath)
        n_idx = 0
        n_sentinel = len(lst_xml_files)
        for _, s_xml_filename in enumerate(lst_xml_files):
            lst_file_ext = os.path.splitext(s_xml_filename)
            if lst_file_ext[1] != '.xml':
                continue
            lst_file_info = s_xml_filename.split('_')
            s_log_date = lst_file_info[0]
            s_morpheme_srl = lst_file_info[1]
            s_media = lst_file_info[2]
            n_media_id = dict_media_lbl_id[s_media]
            # s_iter_cnt = lst_file_info[3]
            s_xml_file_fullname = os.path.join(self.__g_sDownloadPath, s_xml_filename)
            with open(s_xml_file_fullname, 'r') as file:
                s_data = file.read()
            dict_xml_body = o_sv_nvsearch.load_xml(s_data)
            lst_standardized_log = self.__standardize_log(s_morpheme_srl, s_media, n_media_id, dict_xml_body)
            del dict_xml_body
            self.__append_into_article_db(o_sv_mysql, lst_standardized_log, s_log_date)
            self.__archive_xml_file(s_xml_filename)
            self._print_progress_bar(n_idx + 1, n_sentinel, prefix='Register XML file:', suffix='Complete', length=50)
            n_idx += 1
        del dict_media_lbl_id
        del o_sv_nvsearch
        del o_sv_mysql
        return

    def __append_into_article_db(self, o_sv_mysql, lst_standardized_log, s_log_date):
        for dict_single_item in lst_standardized_log:
            # check duplication before registration
            if not self.__is_duplicated(o_sv_mysql, dict_single_item['n_morpheme_srl'], dict_single_item['n_media_id'], dict_single_item['link']):
                o_sv_mysql.execute_query('insertSearchLog', dict_single_item['n_morpheme_srl'], dict_single_item['n_media_id'],
                                         dict_single_item['title'], dict_single_item['link'], dict_single_item['description'],
                                         dict_single_item['s_jsonfy_extra'], dict_single_item['s_local_time'], s_log_date)

    def __archive_xml_file(self, s_current_filename):
        if not os.path.exists(self.__g_sDownloadPath):
            self._print_debug('error: naver search source directory does not exist!')
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

    def __archive_kin_html_file(self, s_data_path, s_cur_filename):
        if not os.path.exists(s_data_path):
            self._print_debug('error: naver API raw directory does not exist!' )
            return
        s_data_archive_path = os.path.join(s_data_path, 'archive')
        if not os.path.exists(s_data_archive_path):
            os.makedirs(s_data_archive_path)
        s_source_filepath = os.path.join(s_data_path, s_cur_filename)
        s_archive_data_file_path = os.path.join(s_data_archive_path, s_cur_filename)
        shutil.move(s_source_filepath, s_archive_data_file_path)

    def __standardize_log(self, n_morpheme_srl, s_media, n_media_id, dict_xml_body):
        lst_log = []
        if 'item' not in dict_xml_body or not dict_xml_body['item']:
            return lst_log

        for dict_single_item in dict_xml_body['item']:
            dict_single_item['title'] = self.__g_oHtmlRemover.sub('', html.unescape(dict_single_item['title']))
            if 'description' in dict_single_item and dict_single_item['description'] is not None:
                dict_single_item['description'] = self.__g_oHtmlRemover.sub('', html.unescape(dict_single_item['description']))
            else:  # regarding image and shop search
                dict_single_item['description'] = None
            
            dict_single_item['n_morpheme_srl'] = n_morpheme_srl
            dict_single_item['n_media_id'] = n_media_id
            dict_single_item['s_jsonfy_extra'] = None
            dict_single_item['s_local_time'] = None
            if s_media == 'blog':
                dict_extra = {'bloggername': dict_single_item['bloggername'], 'bloggerlink': dict_single_item['bloggerlink']}
                dict_single_item['s_jsonfy_extra'] = json.dumps(dict_extra, ensure_ascii=False).encode('utf8')
                del dict_single_item['bloggername']
                del dict_single_item['bloggerlink']
                dict_single_item['s_local_time'] = dict_single_item['postdate']
                del dict_single_item['postdate']
            elif s_media == 'news':
                s_local_time = dict_single_item['pubDate'].replace(' +0900', '')
                dict_single_item['s_local_time'] = datetime.strptime(s_local_time, "%a, %d %b %Y %H:%M:%S")
                del dict_single_item['pubDate']
            elif s_media == 'encyc':
                dict_extra = {'thumbnail': dict_single_item['thumbnail']}
                dict_single_item['s_jsonfy_extra'] = json.dumps(dict_extra, ensure_ascii=False).encode('utf8')
            elif s_media == 'cafearticle':
                dict_extra = {'cafename': dict_single_item['cafename'], 'cafeurl': dict_single_item['cafeurl']}
                dict_single_item['s_jsonfy_extra'] = json.dumps(dict_extra, ensure_ascii=False).encode('utf8')
            elif s_media == 'image':
                dict_extra = {'sizeheight': dict_single_item['sizeheight'], 'sizewidth': dict_single_item['sizewidth']}
                dict_single_item['s_jsonfy_extra'] = json.dumps(dict_extra, ensure_ascii=False).encode('utf8')
            elif s_media == 'kin':
                dict_single_item['link'] = self.__cleanup_kin_url(dict_single_item['link'])
            elif s_media == 'webkr' or s_media == 'doc':
                pass
            elif s_media == 'shop':
                dict_extra = {'image': dict_single_item['image'], 'lprice': dict_single_item['lprice'],
                                'hprice': dict_single_item['hprice'], 'mallName': dict_single_item['mallName'],
                                'productId': dict_single_item['productId'], 'productType': dict_single_item['productType'],
                                'brand': dict_single_item['brand'], 'maker': dict_single_item['maker'],
                                'category1': dict_single_item['category1'], 'category2': dict_single_item['category2'],
                                'category3': dict_single_item['category3'], 'category4': dict_single_item['category4']
                            }
                dict_single_item['s_jsonfy_extra'] = json.dumps(dict_extra, ensure_ascii=False).encode('utf8')
            lst_log.append(dict_single_item)
        return lst_log
    
    def __cleanup_kin_url(self, s_link):
        # naver search API arbirarily changes KIN doc URL
        o_link_parsed = parse.urlparse(s_link)
        dict_query = parse.parse_qs(o_link_parsed.query)
        s_new_query = 'd1id=' + dict_query['d1id'][0] + '&' + 'dirId=' + dict_query['dirId'][0] + '&' + 'docId=' + dict_query['docId'][0]
        o_link_parsed = o_link_parsed._replace(query=s_new_query)
        s_link = parse.urlunparse(o_link_parsed)
        del o_link_parsed, s_new_query, dict_query
        return s_link


if __name__ == '__main__':  # for console debugging
    # CLI example -> python3.7 task.py config_loc=1/1 mode=collect_api
    # CLI example -> python3.7 task.py config_loc=1/1 mode=register_db
    # collect_nvsearch
    nCliParams = len(sys.argv)
    if nCliParams > 2:
        with SvJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('collect_nvsearch')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [mode] params are required for console execution.')
