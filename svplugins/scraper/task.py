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
import sys
import csv
import shutil
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
    import settings
    import sv_slack
else:  # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svcommon import sv_slack
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_nDelaySec = 10


    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230310)')
        
        # self._g_dictParam.update({'target_host_url':None, 'mode':None, 'yyyymm':None, 'top_n_cnt':None})
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sTblPrefix = None
        self.__g_sStoragePath = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sTblPrefix = None
        self.__g_sStoragePath = None

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
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        self.__g_sStoragePath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_brand_id, 'naver_search')
        s_kin_html_path = os.path.join(self.__g_sStoragePath, 'kin_html')
        if not os.path.isdir(s_kin_html_path):
            os.makedirs(s_kin_html_path)
        s_conf_path = os.path.join(self.__g_sStoragePath, 'conf')

        o_sv_mysql = sv_mysql.SvMySql()
        o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
        o_sv_mysql.set_app_name('svplugins.scraper')
        o_sv_mysql.initialize(self._g_dictSvAcctInfo)
        lst_nvsearch_log = o_sv_mysql.executeQuery('getNvrSearchApiKinByLogdate')
        n_url_cnt = len(lst_nvsearch_log)
        self._printDebug('crawling task will take ' + str(int(n_url_cnt*self.__g_nDelaySec/60)) + ' mins at most')
        if n_url_cnt:  # limit 300 urls per a trial
            self._printDebug(str(n_url_cnt) + ' urls will be scrapped')
            s_conf_file_path = 'naver_kin_'+ str(lst_nvsearch_log[0]['log_srl'])+'_'+str(lst_nvsearch_log[-1]['log_srl'])+'.csv'
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

            f = open(s_csv_path,'r')
            o_rdr = csv.reader(f)
            next(o_rdr, None)  # skip the headers
            for lst_line in o_rdr:
                if len(lst_line[1]):
                    o_sv_mysql.executeQuery('updateNvrSearchApiByLogSrl', lst_line[1], lst_line[0])
            del o_rdr
            f.close()

            self.__archive_file(s_conf_path, s_conf_file_path)
        del o_sv_mysql

        s_brand_name = dict_acct_info['brand_name'] if dict_acct_info['brand_name'] else 'unknown brand'
        o_slack = sv_slack.SvSlack('dbs')
        o_slack.sendMsg(s_brand_name + ' scraping has been finished successfully')
        del o_slack

        self._printDebug('-> communication finish')
        self._task_post_proc(self._g_oCallback)

    def __archive_file(self, s_data_path, s_cur_filename):
        if not os.path.exists(s_data_path):
            self._printDebug('error: naver API raw directory does not exist!' )
            return
        s_data_archive_path = os.path.join(s_data_path, 'archive')
        if not os.path.exists(s_data_archive_path):
            os.makedirs(s_data_archive_path)
        s_source_filepath = os.path.join(s_data_path, s_cur_filename)
        s_archive_data_file_path = os.path.join(s_data_archive_path, s_cur_filename)
        shutil.move(s_source_filepath, s_archive_data_file_path)


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
    # python task.py config_loc=1/1
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('scraper')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [mode] params are required for console execution.')
