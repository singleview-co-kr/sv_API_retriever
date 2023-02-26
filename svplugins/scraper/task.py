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
import configparser  # https://docs.python.org/3/library/configparser.html
import sys
import scrapy
from scrapy.spiders import CrawlSpider
from scrapy.crawler import CrawlerProcess

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    # sys.path.append('../../svdjango')
    import sv_mysql
    import sv_object
    import sv_plugin
else:  # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oConfig = configparser.ConfigParser()

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230119)')
        
        # self._g_dictParam.update({'target_host_url':None, 'mode':None, 'yyyymm':None, 'top_n_cnt':None})
        # Declaring a dict outside __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sTblPrefix = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
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
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']

        with sv_mysql.SvMySql() as oSvMysql:  # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.scraper')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            lst_stat_date = oSvMysql.executeQuery('getStatDateList')
            print(lst_stat_date)
            # for dictDate in lstStatDate:
            #     sCompileDate = datetime.strptime(str(dictDate['date']), '%Y-%m-%d').strftime('%Y%m%d')
            #     self.__g_lstDatadateToCompile.append(int(sCompileDate))

        # self.__g_sTargetUrl = self._g_dictParam['target_host_url']
        # if self._g_dictParam['mode'] != None:
        #     self.__g_sMode = self._g_dictParam['mode']

        c = CrawlerProcess({
            'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
            'FEED_FORMAT': 'csv',
            'FEED_URI': 'naver_news.csv',
            'DEPTH_LIMIT': 2,
            'CLOSESPIDER_PAGECOUNT': 3,
            'ROBOTSTXT_OBEY': False,
        })
        # c.crawl(QuotesSpider, urls_file='input.txt')
        # c.start()

        self._printDebug('-> communication finish')
        self._task_post_proc(self._g_oCallback)


class QuotesSpider(CrawlSpider):
    name = "naver_newsbot"
    allowed_domains = ["news.naver.com"]
    # start_urls = ['http://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=001']

    def __init__(self, urls_file, *a, **kw):
        super(QuotesSpider, self).__init__(*a, **kw)
        # print(urls_file)

    def start_requests(self):
        urls = [
            'https://news.naver.com/main/list.naver?mode=LSD&mid=sec&sid1=001'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # page = response.url.split("/")[-2]
        # filename = 'quotes-%s.html' % page
        # with open(filename, 'wb') as f:
        #     f.write(response.body)
        # self.log('Saved file %s' % filename)
        titles = response.xpath('//*[@id="main_content"]/div[2]/ul/li/dl/dt[2]/a/text()').extract()
        authors = response.css('.writing::text').extract()
        previews = response.css('.lede::text').extract()

        for item in zip(titles, authors, previews):
            # print(item)
            scraped_info = {
                'title': item[0].strip(),
                'author': item[1].strip(),
                'preview': item[2].strip(),
            }
            yield scraped_info


if __name__ == '__main__': # for console debugging
    # python task.py config_loc=1/1 target_host_url=http://localhost/devel/modules/svestudio/b2c.php
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob:  # to enforce to call plugin destructor
            oJob.set_my_name('client_serve')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [mode] params are required for console execution.')
