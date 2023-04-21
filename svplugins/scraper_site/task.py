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
import time
from timeit import default_timer as timer
import configparser  # https://docs.python.org/3/library/configparser.html

# 3rd party library
from selenium.webdriver.common.by import By

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
        self._g_oLogger = logging.getLogger(s_plugin_name + '(20230421)')

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

        if 'login' in o_config:
            dict_login_info = {'login_url': o_config['login']['login_url'],
                               'user_name': o_config['login']['user_name'],
                               'user_name_input_text_name': o_config['login']['user_name_input_text_name'],
                               'password': o_config['login']['password'],
                               'user_pw_input_text_name': o_config['login']['user_pw_input_text_name'],
                               'login_btn_xpath': o_config['login']['login_btn_xpath']}
        else:
            dict_login_info = None
        del o_config

        t_start = timer()
        # website to be scraped
        if s_cms_type == 'singleview':
            o_extractor = SvCmsIntLinkExtractor()

        if o_extractor:
            o_extractor.set_host_url(s_host_url)
            o_extractor.set_login_info(dict_login_info)
            #s_driver_path = 'dd' #os.path.join(self._g_sAbsRootPath, 'static', 'chromedriver', 'chro1medriver')
            o_extractor.set_chrome_driver() #s_driver_path)
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
                                          'dispBoardDeleteComment', 'dispBoardModifyComment', 'dispBoardAdminBoardInfo',
                                          'dispPageAdminContentModify', 'dispPageAdminInfo',
                                          'dispPageAdminMobileContent', 'dispBoardReplyComment', 'procFileDownload',
                                          'dispBoardAdminCategoryInfo', 'dispBoardAdminExtraVars',
                                          'dispBoardAdminGrantInfo', 'dispBoardAdminBoardAdditionSetup',
                                          'dispBoardAdminSkinInfo', 'dispBoardAdminMobileSkinInfo',
                                          'dispBoardAdminContent', 'dispAdminConfigGeneral', 'dispMemberInfo',
                                          'dispDocumentManageDocument'
                                          ],
                                     'module': ['admin'],
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

    def _set_logged_in(self):
        # https://hyunsooworld.tistory.com/entry/%EC%85%80%EB%A0%88%EB%8B%88%EC%9B%80-%EC%98%A4%EB%A5%98-AttributeError-WebDriver-object-has-no-attribute-findelementbycssselector-%EC%98%A4%EB%A5%98%ED%95%B4%EA%B2%B0
        if self._g_dictLoginConfig:
            self._g_WdChrome.get(self._g_sSiteUrl + self._g_dictLoginConfig['login_url'])
            self._g_WdChrome.find_element(By.NAME, self._g_dictLoginConfig['user_name_input_text_name']).\
                send_keys(self._g_dictLoginConfig['user_name'])  # key in
            self._g_WdChrome.find_element(By.NAME, self._g_dictLoginConfig['user_pw_input_text_name']).\
                send_keys(self._g_dictLoginConfig['password'])  # key in
            time.sleep(1)
            self._g_WdChrome.find_element(By.XPATH, self._g_dictLoginConfig['login_btn_xpath']).click()  # click login btn
            # time.sleep(5)


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
