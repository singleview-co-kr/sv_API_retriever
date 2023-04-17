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
import re
import os
import sys
from datetime import timedelta
from datetime import date
import time
from timeit import default_timer as timer

# https://www.geeksforgeeks.org/python-program-to-recursively-scrape-all-the-urls-of-the-website/
from bs4 import BeautifulSoup
from bs4 import element as bs4_elem
import requests
from urllib.parse import urlparse
from urllib.parse import parse_qs
# from urllib.parse import parse_qsl
import hashlib

import configparser  # https://docs.python.org/3/library/configparser.html

# singleview library
if __name__ == '__main__':  # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_object
    import sv_plugin
    import sv_site_scraper
    import settings
else:  # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svcommon import sv_site_scraper
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    # __g_nDelaySec = 1

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name + '(20230416)')

        # self._g_dictParam.update({'target_url': None})
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        # self.__g_sTblPrefix = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        # self.__g_sTblPrefix = None
        pass

    def do_task(self, o_callback):
        self._g_oCallback = o_callback

        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
                'nvr_ad_acct' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        s_tbl_prefix = dict_acct_info['tbl_prefix']
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        s_storage_path = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_brand_id)

        o_sv_mysql = sv_mysql.SvMySql()
        o_sv_mysql.setTablePrefix(s_tbl_prefix)
        o_sv_mysql.set_app_name('svplugins.scraper_site')
        o_sv_mysql.initialize(self._g_dictSvAcctInfo)

        o_config = configparser.ConfigParser()
        s_scrape_conf_path = os.path.join(s_storage_path, 'site_scraper.ini')
        if os.path.isfile(s_scrape_conf_path):
            o_config.read(s_scrape_conf_path)
        if 'server' in o_config:
            s_host_url = o_config['server']['host_url']
            s_cms_type = o_config['server']['cms_type']
        else:
            self._printDebug('stop -> site_scraper.ini')
            return
        del o_config

        t_start = timer()
        # website to be scraped
        if s_cms_type == 'singleview':
            o_extractor = SvCmsIntLinkExtractor()

        if o_extractor:
            o_extractor.set_host_url(s_host_url)
            o_extractor.set_storage_path(s_storage_path)
            o_extractor.set_sv_mysql(o_sv_mysql)
            o_extractor.set_print_func(self._printDebug)
            o_extractor.extract_internal_link()
            self._printDebug(str(o_extractor.get_request_cnt()) + ' times requested')
            del o_extractor
        del o_sv_mysql

        t_end = timer()
        self._printDebug('-> proc elapsed for ' + str(int((t_end - t_start) / 60)) + ' mins')
        del t_start
        del t_end
        self._task_post_proc(self._g_oCallback)


class SvCmsIntLinkExtractor(sv_site_scraper.ISvSiteScraper):

    def _set_site_config(self):
        self._g_dictIgnoreQryName = {'act':  # ignore by qry value
                                         ['dispMemberLoginForm', 'dispMemberLogout', 'dispMemberFindAccount',
                                          'dispMemberSignUpForm', 'dispBoardWrite', 'dispBoardDelete',
                                          'dispBoardDeleteComment', 'dispBoardModifyComment',
                                          'dispBoardReplyComment', 'procFileDownload'],
                                     # unconditionally ignore qry
                                     'listStyle': None, 'search_target': None, 'search_keyword': None,
                                     'sort_index': None, 'order_type': None, 'tags': None, 'catalog': None,
                                     'category': None, 'cpage': None, 'comment_srl': None, 'utm_source': None,
                                     'utm_medium': None, 'utm_campaign': None, 'utm_term': None, 'utm_content': None
                                     }
        self._g_dictSelector['body'] = 'div.rd.rd_nav_style2.clear > div.rd_body.clear > article > div'
        self._g_dictSelector['doc_date'] = \
            'div.rd.rd_nav_style2.clear > div.rd_hd.clear > div.board.clear > div.top_area.ngeb > div > span'
        if 'singleview' in self._g_sSiteUrl:
            self._g_dictSelector['list'] = 'div.bd_lst_wrp > ol'
        elif 'yuhan' in self._g_sSiteUrl:
            self._g_dictSelector['list'] = 'div.bd_lst_wrp > table > tbody'


# class SiteInternalLinkExtractor:
#     __g_nDelaySec = .5
#     __g_dictScraperHeaders = {'User-Agent': 'sv-bot-v1.0',
#                               # 'From': 'root@domain.example'  # This is another valid field
#                               }
#
#     def __init__(self, o_sv_mysql):
#         self.__g_oSvMysql = o_sv_mysql
#         self.__g_dictUrlSeenIneffective = {}
#         self.__g_dictUrlSeenEffective = {}
#         self.__g_sSiteUrl = None
#         self.__g_nNewEffectiveUrlCnt = 0
#         self.__g_nNewIneffectiveUrlCnt = 0
#         self.__g_nRequestCnt = 0
#         self.__g_sStoragePath = None
#         self.__g_dictScrapeConf = {}
#         self.__g_lstIgnoreAct = []
#         self.__g_lstIgnoreQryName = []
#         self.__g_dictSelector = {}
#         self.__printDebug = None
#
#     def __del__(self):
#         """ __del__() is not executed if try except occurred """
#         del self.__g_oSvMysql
#         del self.__g_dictUrlSeenIneffective
#         del self.__g_dictUrlSeenEffective
#         del self.__g_sSiteUrl
#         del self.__g_nNewEffectiveUrlCnt
#         del self.__g_nNewIneffectiveUrlCnt
#         del self.__g_nRequestCnt
#         del self.__g_sStoragePath
#         del self.__g_dictScrapeConf
#         del self.__g_lstIgnoreAct
#         del self.__g_lstIgnoreQryName
#         del self.__g_dictSelector
#         del self.__printDebug
#
#     # def set_site_url(self, s_site_arg):
#     #     self.__g_sSiteUrl = s_site_arg
#
#     def set_display_func(self, o_func):
#         self.__printDebug = o_func
#
#     def set_storage_path(self, s_root_path):
#         s_storage_path = os.path.join(s_root_path, 'site_scraper')
#         self.__g_sStoragePath = s_storage_path
#         if not os.path.isdir(s_storage_path):
#             os.makedirs(s_storage_path)
#
#         o_config = configparser.ConfigParser()
#         s_scrape_conf_path = os.path.join(s_root_path, 'site_scraper.ini')
#         if os.path.isfile(s_scrape_conf_path):
#             o_config.read(s_scrape_conf_path)
#         if 'server' in o_config:
#             self.__g_sSiteUrl = o_config['server']['host_url']
#             s_cms_type = o_config['server']['cms_type']
#         del o_config
#
#         if s_cms_type == 'singleview':
#             self.__g_lstIgnoreAct = ['dispMemberLoginForm', 'dispMemberLogout', 'dispMemberFindAccount',
#                                      'dispMemberSignUpForm', 'dispBoardWrite', 'dispBoardDelete',
#                                      'dispBoardDeleteComment', 'dispBoardModifyComment', 'dispBoardReplyComment',
#                                      'procFileDownload']
#             self.__g_lstIgnoreQryName = ['listStyle', 'search_target', 'search_keyword', 'sort_index', 'order_type',
#                                          'tags', 'catalog', 'category', 'cpage', 'comment_srl', 'utm_source',
#                                          'utm_medium', 'utm_campaign', 'utm_term', 'utm_content']
#
#         if 'singleview' in self.__g_sSiteUrl:
#             self.__g_dictSelector['body'] = 'div.rd.rd_nav_style2.clear > div.rd_body.clear > article > div'
#             self.__g_dictSelector['list'] = 'div.bd_lst_wrp > ol'
#         elif 'yuhan' in self.__g_sSiteUrl:
#             self.__g_dictSelector['body'] = 'div.rd.rd_nav_style2.clear > div.rd_body.clear > article > div'
#             self.__g_dictSelector['list'] = 'div.bd_lst_wrp > table > tbody'
#
#     def get_request_cnt(self):
#         return self.__g_nRequestCnt
#
#     def extract_internal_link(self):
#         if self.__g_sSiteUrl is None:
#             self.__printDebug('site url not designated')
#             return
#         dt_today = date.today()
#         dt_old = dt_today - timedelta(days=5)
#         # for initialize only
#         lst_sub_page_all = self.__g_oSvMysql.executeQuery('getScrapeLogAll', dt_old)
#         n_rec_cnt = len(lst_sub_page_all)
#         del lst_sub_page_all
#         if n_rec_cnt == 0:
#             dict_rst = self.__analyze_url()
#             for i in dict_rst['lst_anchor']:
#                 if 'href' in i.attrs:
#                     s_href = i.attrs['href']
#                 else:
#                     continue
#                 s_href = self.__check_sub_url(s_href)
#                 if s_href:
#                     dict_sub_rst = self.__analyze_url(s_href)
#                     lst_subpage = self.__g_oSvMysql.executeQuery('getScrapeLogByFingerprint', dict_sub_rst['s_fingerprint'])
#                     n_rec_cnt = len(lst_subpage)
#                     del lst_subpage
#                     if n_rec_cnt == 0:
#                         self.__register_url(s_href, dict_sub_rst['s_fingerprint'], dict_sub_rst['s_content'],
#                                             dict_sub_rst['s_html'], dict_sub_rst['n_status_code'])
#         # for traverse
#         while True:
#             # traverse newly appended sub-urls only
#             lst_sub_page_all = self.__g_oSvMysql.executeQuery('getScrapeLogAll', dt_old)
#             if len(lst_sub_page_all) == 0:
#                 self.__printDebug('no urls to traverse')
#                 return
#
#             for dict_row in lst_sub_page_all:
#                 if self.__is_analyzed_url_effective(dict_row['url']):  # do not look at more than twice
#                     continue
#                 dict_rst = self.__analyze_url(dict_row['url'])
#                 self.__g_nNewEffectiveUrlCnt = 0
#                 self.__g_nNewIneffectiveUrlCnt = 0
#                 for i in dict_rst['lst_anchor']:
#                     try:
#                         s_new_href_validation = i.attrs['href']
#                     except KeyError:
#                         continue
#                     s_href = self.__check_sub_url(s_new_href_validation)
#                     if s_href:
#                         dict_sub_rst = self.__analyze_url(s_href)
#                         lst_sub_page = self.__g_oSvMysql.executeQuery('getScrapeLogByFingerprint',
#                                                                       dict_sub_rst['s_fingerprint'])
#                         n_rec_cnt = len(lst_sub_page)
#                         del lst_sub_page
#                         if n_rec_cnt == 0:  # a unregistered effective url
#                             self.__register_url(s_href, dict_sub_rst['s_fingerprint'], dict_sub_rst['s_content'],
#                                                 dict_sub_rst['s_html'], dict_rst['n_status_code'])
#                     else:
#                         self.__append_url_ineffective(s_href)
#                 # tag scraped date sub url
#                 self.__g_oSvMysql.executeQuery('updateScrapeDateByLogSrl', dt_today, dict_row['log_srl'])
#             self.__printDebug(str(self.__g_nRequestCnt) + ' requested')
#             del lst_sub_page_all
#
#     def __is_analyzed_url_effective(self, s_href):
#         if s_href in self.__g_dictUrlSeenEffective:
#             if self.__g_dictUrlSeenEffective[s_href] > 1:
#                 return True
#         return False
#
#     def __append_url_effective(self, s_href):
#         if s_href in self.__g_dictUrlSeenEffective:
#             self.__g_dictUrlSeenEffective[s_href] += 1
#         else:
#             self.__g_dictUrlSeenEffective[s_href] = 1
#             self.__g_nNewEffectiveUrlCnt += 1
#
#     def __append_url_ineffective(self, s_href):
#         if s_href in self.__g_dictUrlSeenIneffective:
#             self.__g_dictUrlSeenIneffective[s_href] += 1
#         else:
#             self.__g_dictUrlSeenIneffective[s_href] = 1
#             self.__g_nNewIneffectiveUrlCnt += 1
#
#     def __register_url(self, s_url, s_fingerprint, s_content, s_html, n_status_code):
#         lst_rst = self.__g_oSvMysql.executeQuery('insertScrapeLog', s_url, s_fingerprint, s_content, n_status_code)
#         if 'id' in lst_rst[0]:
#             self.__save_html(lst_rst[0]['id'], s_html)
#
#     def __save_html(self, n_log_srl, s_html):
#         with open(os.path.join(self.__g_sStoragePath, str(n_log_srl) + '.html'), 'w', encoding='utf-8') as out:
#             out.write(s_html)
#
#     def __read_html(self, n_log_srl):
#         f = open(os.path.join(self.__g_sStoragePath, str(n_log_srl) + '.html'), 'r', encoding='utf-8')
#         s_data = f.read()
#         f.close()
#         return s_data
#
#     def __analyze_url(self, s_sub_url=None):
#         # getting the request from url
#         dict_rst = {'s_html': None, 'n_status_code': 0, 's_fingerprint': None, 's_content': '', 'lst_anchor': None}
#         s_scrape_url = self.__g_sSiteUrl if s_sub_url is None else self.__g_sSiteUrl + s_sub_url
#         o_resp = requests.get(s_scrape_url, headers=self.__g_dictScraperHeaders)
#         self.__g_nRequestCnt += 1
#         if o_resp.status_code == 200:
#             self.__printDebug('request ' + s_scrape_url + ' succeed')
#             s_html = o_resp.text
#             o_soup = BeautifulSoup(s_html, 'html.parser')
#             # try extract contents body
#             for s_type, s_select in self.__g_dictSelector.items():
#                 s_finger_print = o_soup.select_one(s_select)
#                 if s_finger_print is not None:
#                     break
#             if s_finger_print is None:  # means neither list nor body page; extract html
#                 s_finger_print = o_soup.get_text()
#                 s_type = 'general'
#             # s_finger_print = o_soup.select_one('div.rd.rd_nav_style2.clear > div.rd_body.clear > article > div')
#             # if s_finger_print is None:  # try contents list
#             #     s_finger_print = o_soup.select_one('div.bd_lst_wrp > table > tbody')
#             # if s_finger_print is None:  # try whole page
#             #     s_finger_print = o_soup.get_text()
#             if type(s_finger_print) == bs4_elem.Tag:
#                 s_finger_print = s_finger_print.get_text()
#             dict_rst['s_html'] = s_html
#             if s_type == 'body':
#                 dict_rst['s_content'] = re.sub('\s{2,}', ' ', s_finger_print)
#             dict_rst['s_fingerprint'] = hashlib.md5(s_finger_print.encode('utf-8')).hexdigest()
#             dict_rst['lst_anchor'] = o_soup.find_all("a")
#         else:
#             self.__printDebug('request ' + s_scrape_url + 'failed with status code ' + str(o_resp.status_code))
#
#         dict_rst['n_status_code'] = o_resp.status_code
#         o_resp.close()
#         del o_resp
#         if s_sub_url:
#             self.__append_url_effective(s_sub_url)
#         time.sleep(self.__g_nDelaySec)  # Sleep for 3 seconds
#         return dict_rst
#         # if s_sub_url == '/ask_dr_laundary':
#         #     for i in s.find_all("a"):
#         #         print(i)
#
#     def __check_sub_url(self, s_href):
#         s_href = s_href.replace(self.__g_sSiteUrl, '')
#         lst_href = s_href.split('#')
#         s_href = lst_href[0]
#         if s_href != '/' and s_href.startswith("/") and not s_href.startswith("//"):
#             o_url_parts = urlparse(s_href)
#             dict_url = parse_qs(o_url_parts.query)
#             if 'act' in dict_url:
#                 if dict_url['act'][0] in self.__g_lstIgnoreAct:
#                     # self.__printDebug('ignore act')
#                     return False
#                 # else:
#                 #     self.__printDebug(dict_url['act'][0] + ' detected')
#             else:
#                 for s_qry_name in dict_url.keys():
#                     if s_qry_name in self.__g_lstIgnoreQryName:
#                         # self.__printDebug(s_qry_name + ' has been ignored')
#                         return False
#             if s_href in self.__g_dictUrlSeenEffective:
#                 return False
#             return s_href
#         return False


if __name__ == '__main__':  # for console debugging
    # python task.py config_loc=1/1
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('scraper_site')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [mode] params are required for console execution.')
