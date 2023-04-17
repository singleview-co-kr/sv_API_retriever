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
import csv
import os
import re
import json
import html
import time
import shutil
from datetime import datetime
from urllib import parse

# 3rd-party library
import scrapy
from scrapy.spiders import CrawlSpider
from scrapy.crawler import CrawlerProcess

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


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oHtmlRemover = re.compile(r"<[^<]+?>")
    __g_nDelaySec = 10

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230417)')
        
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
            self._printDebug('stop -> invalid config_loc')
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
            self._printDebug('-> communication begin')
            with sv_mysql.SvMySql() as o_sv_mysql:
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.collect_nvsearch')
                o_sv_mysql.initialize(self._g_dictSvAcctInfo)
                lst_morpheme = o_sv_mysql.executeQuery('getMorphemeActivated')
            
            for dict_single_morpheme in lst_morpheme:
                n_morpheme_srl=dict_single_morpheme['morpheme_srl']
                s_morpheme = dict_single_morpheme['morpheme']
                n_total_effective_cnt = self.__get_keyword_from_nvsearch(n_morpheme_srl, s_morpheme)
                self._printDebug(str(n_total_effective_cnt) + ' times retrieved')
                # return  #### limit to first morpheme ##################
            self._printDebug('-> communication finish')
        elif self.__g_sMode == 'register_db':
            self.__register_raw_xml_file()
        elif self.__g_sMode == 'update_kin_date':
            s_sv_acct_id = dict_acct_info['sv_account_id']
            s_brand_id = dict_acct_info['brand_id']
            self.__g_sStoragePath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id,
                                                 s_brand_id, 'naver_search')
            s_kin_html_path = os.path.join(self.__g_sStoragePath, 'kin_html')
            if not os.path.isdir(s_kin_html_path):
                os.makedirs(s_kin_html_path)
            s_conf_path = os.path.join(self.__g_sStoragePath, 'conf')

            o_sv_mysql = sv_mysql.SvMySql()
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_nvsearch')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            lst_nvsearch_log = o_sv_mysql.executeQuery('getNvrSearchApiKinByLogdate')

            n_url_cnt = len(lst_nvsearch_log)
            self._printDebug('crawling task will take ' + str(int(n_url_cnt * self.__g_nDelaySec / 60)) + ' mins at most')
            if n_url_cnt:  # limit 300 urls per a trial
                self._printDebug(str(n_url_cnt) + ' urls will be scrapped')
                s_conf_file_path = 'naver_kin_' + str(lst_nvsearch_log[0]['log_srl']) + '_' + str(
                    lst_nvsearch_log[-1]['log_srl']) + '.csv'
                s_csv_path = os.path.join(s_conf_path, s_conf_file_path)

                o_scraper = CrawlerProcess({
                    'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
                    'FEED_FORMAT': 'csv',
                    'FEED_URI': s_csv_path,
                    # 'DEPTH_LIMIT': 2,
                    'CLOSESPIDER_PAGECOUNT': 300,  # limit 300 urls per a trial
                    'ROBOTSTXT_OBEY': False,
                    # Minimum seconds to wait between 2 consecutive requests to the same domain.
                    'DOWNLOAD_DELAY': self.__g_nDelaySec,
                    'LOG_LEVEL': 'INFO',
                })
                o_scraper.crawl(NvrKinSpider, s_kin_html_path=s_kin_html_path, lst_urls=lst_nvsearch_log)
                o_scraper.start()
                del o_scraper

                f = open(s_csv_path, 'r')
                o_rdr = csv.reader(f)
                next(o_rdr, None)  # skip the headers
                for lst_line in o_rdr:
                    if len(lst_line[1]):  # available to access and get post date
                        if lst_line[1].find('시간') == -1:  # 2023.03.19
                            s_logdate = lst_line[1]
                        else:  # 15시간 전
                            s_logdate = datetime.today().strftime('%Y.%m.%d')
                        o_sv_mysql.executeQuery('updateNvrSearchApiByLogSrl', s_logdate, lst_line[0])
                    else:  # not available to access and get post date
                        o_sv_mysql.executeQuery('updateNvrSearchApiCrawledByLogSrl', lst_line[0])
                del o_rdr
                f.close()
                self.__archive_kin_html_file(s_conf_path, s_conf_file_path)
            del o_sv_mysql
            s_brand_name = dict_acct_info['brand_name'] if dict_acct_info['brand_name'] else 'unknown brand'
            o_slack = sv_slack.SvSlack('dbs')
            # o_slack.sendMsg(s_brand_name + ' scraping has been finished successfully')
            del o_slack
        else:
            self._printDebug('mode is not specified.')

        self._task_post_proc(self._g_oCallback)
    
    def __get_keyword_from_nvsearch(self, n_param_morpheme_srl, s_param_morpheme):
        """ retrieve text from naver search API """        
        o_sv_nvsearch = sv_search_api.SvNvSearch()
        dict_media_lbl_id = o_sv_nvsearch.get_media_lbl_id_dict()

        o_sv_mysql = sv_mysql.SvMySql()
        o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
        o_sv_mysql.set_app_name('svplugins.collect_nvsearch')
        o_sv_mysql.initialize(self._g_dictSvAcctInfo)

        s_base_tsv_filename = datetime.now().strftime('%Y%m%d') + '_' + str(n_param_morpheme_srl)
        n_total_effective_cnt = 0

        self._printDebug(s_param_morpheme + ' will be retrieved')
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
                    self._printDebug(dict_1st_rst['s_msg'])
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
                            # self._printDebug('called requests exceeds API result count')
                            break

                        n_total_effective_cnt += 1
                        if dict_iter_rst['b_error']:
                            # self._printDebug(dict_iter_rst['s_msg'])
                            break
                        
                        s_tsv_filename = s_base_tsv_filename + '_' + s_media + '_' + str(n_media_retrieval_cnt)
                        self.__write_into_file(s_tsv_filename, dict_iter_rst['s_plain_resp'])
                        if 'item' in dict_iter_retrieval and \
                                dict_iter_retrieval['item']:
                            f_new_rate = self.__get_new_rate(o_sv_mysql, n_param_morpheme_srl, s_media, n_media_id, dict_iter_retrieval['item'])
                        if f_new_rate < 0.1:
                            # self._printDebug('too many duplicated item')
                            break
                        del dict_iter_retrieval
                        del dict_iter_rst
                else:
                    time.sleep(1)
                        
                del dict_1st_retrieval
                del dict_1st_rst
            self._printProgressBar(n_idx + 1, n_sentinel, prefix='Collect XML data:', suffix='Complete', length=50)
            n_idx += 1
        self._printDebug(s_param_morpheme + ' has been retrieved')
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
        lst_rst = o_sv_mysql.executeQuery('getSearchLogByMorphemeMediaLink', n_morpheme_srl, n_media_id, s_link) 
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
        o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
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
            self._printProgressBar(n_idx + 1, n_sentinel, prefix='Register XML file:', suffix='Complete', length=50)
            n_idx += 1
        del dict_media_lbl_id
        del o_sv_nvsearch
        del o_sv_mysql
        return

    def __append_into_article_db(self, o_sv_mysql, lst_standardized_log, s_log_date):
        for dict_single_item in lst_standardized_log:
            # check duplication before registration
            if not self.__is_duplicated(o_sv_mysql, dict_single_item['n_morpheme_srl'], dict_single_item['n_media_id'], dict_single_item['link']):
                o_sv_mysql.executeQuery('insertSearchLog', dict_single_item['n_morpheme_srl'], dict_single_item['n_media_id'], 
                                        dict_single_item['title'], dict_single_item['link'], dict_single_item['description'], 
                                        dict_single_item['s_jsonfy_extra'], dict_single_item['s_local_time'], s_log_date) 

    def __archive_xml_file(self, s_current_filename):
        if not os.path.exists(self.__g_sDownloadPath):
            self._printDebug('error: naver search source directory does not exist!')
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
            self._printDebug('error: naver API raw directory does not exist!' )
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


class NvrKinSpider(CrawlSpider):
    name = "nvr_kin_publish_date_bot"
    allowed_domains = ["kin.naver.com"]

    # start_urls = ['http://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=001']

    def __init__(self, s_kin_html_path, lst_urls, *a, **kw):
        super(NvrKinSpider, self).__init__(*a, **kw)
        self.__g_lstUrl = lst_urls
        self.__g_sKinHtmlPath = s_kin_html_path

    def __del__(self):
        """ __del__() is not executed if try except occurred """
        self.__g_lstUrl = None
        self.__g_sKinHtmlPath = None

    def start_requests(self):
        # https://stackoverflow.com/questions/32252201/passing-a-argument-to-a-callback-function
        for dict_record in self.__g_lstUrl:
            yield scrapy.Request(url=dict_record['link'], callback=self.parse,
                                 cb_kwargs=dict(n_log_srl=dict_record['log_srl']))

    def parse(self, response, n_log_srl):
        # backup scrapped HTML source
        with open(os.path.join(self.__g_sKinHtmlPath, str(n_log_srl) + '.html'), 'w', encoding='utf-8') as out:
            out.write(response.text)
        s_pub_date = response.xpath('//*[@id="content"]/div[1]/div/div[3]/div[1]/span[1]/text()').extract_first()
        try:
            s_pub_date_stripped = s_pub_date.strip()  # 2023.02.02
        except AttributeError as err:  # adult only kin posting has been restricted
            # print(str(n_log_srl) + ' can\'t extract pub_date from adult only kin posting')
            s_pub_date_stripped = ''
        scraped_info = {
            'n_log_srl': n_log_srl,
            's_pub_date': s_pub_date_stripped,  # 2023.02.02
        }
        yield scraped_info


if __name__ == '__main__': # for console debugging
    # CLI example -> python3.7 task.py config_loc=1/1 mode=collect_api
    # CLI example -> python3.7 task.py config_loc=1/1 mode=register_db
    # collect_nvsearch
    nCliParams = len(sys.argv)
    if nCliParams > 2:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('collect_nvsearch')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [mode] params are required for console execution.')
