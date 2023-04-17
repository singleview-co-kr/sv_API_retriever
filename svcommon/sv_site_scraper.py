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
# import sys
import os
import configparser  # https://docs.python.org/3/library/configparser.html
from abc import ABC
from abc import abstractmethod
from datetime import timedelta
from datetime import date
import requests
from bs4 import BeautifulSoup
from bs4 import element as bs4_elem
import re
import hashlib
import time
from urllib.parse import urlparse
from urllib.parse import parse_qs
# import threading
# import time
# from datetime import datetime

# import os
# import logging
# import importlib

# 3rd party library
# from decouple import config  # https://pypi.org/project/python-decouple/

# singleview library
if __name__ == 'sv_plugin': # for console debugging
    # sys.path.append('../../svcommon')
    # import sv_api_config_parser
    # import sv_events
    pass
else:
    # from svcommon import sv_api_config_parser
    # from svcommon import sv_events
    pass


class ISvSiteScraper(ABC):
    _g_dictScraperHeaders = {'User-Agent': 'sv-bot-v1.0',
                             # 'From': 'root@domain.example'  # This is another valid field
                             }

    def __init__(self):
        self._g_nDelaySec = .5
        self._g_dictUrlSeenEffective = {}
        self._g_dictIgnoreQryName = {}
        self._g_sSiteUrl = None
        self._g_dictSelector = {}
        self._printDebug = None
        self.__g_oSvMysql = None
        self.__g_dictUrlSeenIneffective = {}
        self.__g_nNewEffectiveUrlCnt = 0
        self.__g_nNewIneffectiveUrlCnt = 0
        self.__g_nRequestCnt = 0
        self.__g_sStoragePath = None

    def __del__(self):
        """ __del__() is not executed if try except occurred """
        del self._g_nDelaySec
        del self._g_dictUrlSeenEffective
        del self._g_dictIgnoreQryName
        del self._g_sSiteUrl
        del self._g_dictSelector
        del self._printDebug
        del self.__g_oSvMysql
        del self.__g_dictUrlSeenIneffective
        del self.__g_nNewEffectiveUrlCnt
        del self.__g_nNewIneffectiveUrlCnt
        del self.__g_nRequestCnt
        del self.__g_sStoragePath

    def set_sv_mysql(self, o_sv_mysql):
        self.__g_oSvMysql = o_sv_mysql

    def set_print_func(self, o_func):
        self._printDebug = o_func

    def set_delay_sec(self, f_sec):
        if f_sec > 0:
            self._g_nDelaySec = f_sec

    def set_host_url(self, s_host_url):
        self._g_sSiteUrl = s_host_url

    def set_storage_path(self, s_root_path):
        s_storage_path = os.path.join(s_root_path, 'site_scraper')
        self.__g_sStoragePath = s_storage_path
        if not os.path.isdir(s_storage_path):
            os.makedirs(s_storage_path)
        self._set_site_config()

    def get_request_cnt(self):
        return self.__g_nRequestCnt

    def extract_internal_link(self):
        if self._g_sSiteUrl is None:
            self._printDebug('site url not designated')
            return
        dt_today = date.today()
        dt_old = dt_today - timedelta(days=5)
        # for initialize only
        lst_sub_page_all = self.__g_oSvMysql.executeQuery('getScrapeLogAll', dt_old)
        n_rec_cnt = len(lst_sub_page_all)
        del lst_sub_page_all
        if n_rec_cnt == 0:
            dict_rst = self.__analyze_url()
            for i in dict_rst['lst_anchor']:
                if 'href' in i.attrs:
                    s_href = i.attrs['href']
                else:
                    continue
                s_href = self.__is_effective_sub_url(s_href)
                if s_href:
                    dict_sub_rst = self.__analyze_url(s_href)
                    lst_subpage = self.__g_oSvMysql.executeQuery('getScrapeLogByFingerprint', dict_sub_rst['s_fingerprint'])
                    n_rec_cnt = len(lst_subpage)
                    del lst_subpage
                    if n_rec_cnt == 0:
                        self.__register_url(s_href, dict_sub_rst['s_fingerprint'], dict_sub_rst['s_content'],
                                            dict_sub_rst['s_html'], dict_sub_rst['n_status_code'],
                                            dict_sub_rst['s_document_date'])
        # for traverse
        while True:
            # traverse newly appended sub-urls only
            lst_sub_page_all = self.__g_oSvMysql.executeQuery('getScrapeLogAll', dt_old)
            if len(lst_sub_page_all) == 0:
                self._printDebug('no urls to traverse')
                return

            for dict_row in lst_sub_page_all:
                if self.__is_analyzed_url_effective(dict_row['url']):  # do not look at more than twice
                    continue
                dict_rst = self.__analyze_url(dict_row['url'])
                self.__g_nNewEffectiveUrlCnt = 0
                self.__g_nNewIneffectiveUrlCnt = 0
                for i in dict_rst['lst_anchor']:
                    try:
                        s_new_href_validation = i.attrs['href']
                    except KeyError:
                        continue
                    s_href = self.__is_effective_sub_url(s_new_href_validation)
                    if s_href:
                        dict_sub_rst = self.__analyze_url(s_href)
                        lst_sub_page = self.__g_oSvMysql.executeQuery('getScrapeLogByFingerprint',
                                                                      dict_sub_rst['s_fingerprint'])
                        n_rec_cnt = len(lst_sub_page)
                        del lst_sub_page
                        if n_rec_cnt == 0:  # a unregistered effective url
                            self.__register_url(s_href, dict_sub_rst['s_fingerprint'], dict_sub_rst['s_content'],
                                                dict_sub_rst['s_html'], dict_sub_rst['n_status_code'],
                                                dict_sub_rst['s_document_date'])
                    else:
                        self.__append_url_ineffective(s_href)
                # tag scraped date sub url
                self.__g_oSvMysql.executeQuery('updateScrapeDateByLogSrl', dt_today, dict_row['log_srl'])
            self._printDebug(str(self.__g_nRequestCnt) + ' requested')
            del lst_sub_page_all

    @abstractmethod
    def _set_site_config(self, s_cms_type):
        pass

    def __is_effective_sub_url(self, s_href):
        s_href = s_href.replace(self._g_sSiteUrl, '')
        lst_href = s_href.split('#')
        s_href = lst_href[0]
        if s_href != '/' and s_href.startswith("/") and not s_href.startswith("//"):
            o_url_parts = urlparse(s_href)
            dict_url = parse_qs(o_url_parts.query)
            for s_qry_name, lst_qry_val in dict_url.items():
                if s_qry_name in self._g_dictIgnoreQryName:
                    if self._g_dictIgnoreQryName[s_qry_name] is None:
                        return False
                    elif lst_qry_val[0] in self._g_dictIgnoreQryName[s_qry_name]:
                        return False
            if s_href in self._g_dictUrlSeenEffective:
                return False
            return s_href
        return False

    def __is_analyzed_url_effective(self, s_href):
        if s_href in self._g_dictUrlSeenEffective:
            if self._g_dictUrlSeenEffective[s_href] > 1:
                return True
        return False

    def __append_url_effective(self, s_href):
        if s_href in self._g_dictUrlSeenEffective:
            self._g_dictUrlSeenEffective[s_href] += 1
        else:
            self._g_dictUrlSeenEffective[s_href] = 1
            self.__g_nNewEffectiveUrlCnt += 1

    def __append_url_ineffective(self, s_href):
        if s_href in self.__g_dictUrlSeenIneffective:
            self.__g_dictUrlSeenIneffective[s_href] += 1
        else:
            self.__g_dictUrlSeenIneffective[s_href] = 1
            self.__g_nNewIneffectiveUrlCnt += 1

    def __register_url(self, s_url, s_fingerprint, s_content, s_html, n_status_code, s_document_date):
        if s_document_date:
            lst_rst = self.__g_oSvMysql.executeQuery('insertScrapeLogDocdate', s_url, s_fingerprint, s_content,
                                                     n_status_code, s_document_date)
        else:
            lst_rst = self.__g_oSvMysql.executeQuery('insertScrapeLog', s_url, s_fingerprint, s_content, n_status_code)
        if 'id' in lst_rst[0]:
            # self._save_html(lst_rst[0]['id'], s_html)
            with open(os.path.join(self.__g_sStoragePath, str(lst_rst[0]['id']) + '.html'), 'w', encoding='utf-8') as out:
                out.write(s_html)

    # def _save_html(self, n_log_srl, s_html):
    #     with open(os.path.join(self.__g_sStoragePath, str(n_log_srl) + '.html'), 'w', encoding='utf-8') as out:
    #         out.write(s_html)

    def __read_html(self, n_log_srl):
        f = open(os.path.join(self.__g_sStoragePath, str(n_log_srl) + '.html'), 'r', encoding='utf-8')
        s_data = f.read()
        f.close()
        return s_data

    def __analyze_url(self, s_sub_url=None):
        # getting the request from url
        dict_rst = {'s_html': None, 'n_status_code': 0, 's_fingerprint': None, 's_content': '',
                    's_document_date': None, 'lst_anchor': None}
        s_scrape_url = self._g_sSiteUrl if s_sub_url is None else self._g_sSiteUrl + s_sub_url
        o_resp = requests.get(s_scrape_url, headers=self._g_dictScraperHeaders)
        self.__g_nRequestCnt += 1
        if o_resp.status_code == 200:
            self._printDebug('request ' + s_scrape_url + ' succeed')
            s_html = o_resp.text
            o_soup = BeautifulSoup(s_html, 'html.parser')
            # try extract contents body
            for s_type, s_select in self._g_dictSelector.items():
                s_finger_print = o_soup.select_one(s_select)
                if s_finger_print is not None:
                    break
            if s_finger_print is None:  # means neither list nor body page; extract html
                s_finger_print = o_soup.get_text()
                s_type = 'general'
            if type(s_finger_print) == bs4_elem.Tag:
                s_finger_print = s_finger_print.get_text()
            dict_rst['s_html'] = s_html
            if s_type == 'body':  # if contents body, remove duplicated white space and extract document date
                dict_rst['s_content'] = re.sub('\s{2,}', ' ', s_finger_print)
                dict_rst['s_document_date'] = o_soup.select_one(self._g_dictSelector['doc_date']).get_text()
                print('body detect')
                print(o_soup.select_one(self._g_dictSelector['doc_date']).get_text())
            dict_rst['s_fingerprint'] = hashlib.md5(s_finger_print.encode('utf-8')).hexdigest()
            dict_rst['lst_anchor'] = o_soup.find_all("a")
            del o_soup
        else:
            self._printDebug('request ' + s_scrape_url + 'failed with status code ' + str(o_resp.status_code))

        dict_rst['n_status_code'] = o_resp.status_code
        o_resp.close()
        del o_resp
        if s_sub_url:
            self.__append_url_effective(s_sub_url)
        time.sleep(self._g_nDelaySec)  # Sleep for 3 seconds
        return dict_rst
        # if s_sub_url == '/ask_dr_laundary':
        #     for i in s.find_all("a"):
        #         print(i)


# if __name__ == '__main__': # for console debugging
# 	pass