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

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql
    import sv_object
    import sv_plugin
    import sv_twitter
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svcommon import sv_twitter


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        s_plugin_name = os.path.abspath(__file__).split(os.path.sep)[-2]
        self._g_oLogger = logging.getLogger(s_plugin_name+'(20230218)')
        
        self._g_dictParam.update({'mode':None, 'morpheme':None})
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sTblPrefix = None
        self.__g_sMode = {}
        self.__g_sMorpheme = None

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sTblPrefix = None
        self.__g_sMode = {}
        self.__g_sMorpheme = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_sMode = self._g_dictParam['mode']
        self.__g_sMorpheme = self._g_dictParam['morpheme']

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
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_twitter')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)

        if self.__g_sMode in ['analyze_new', 'tag_ignore_word', 'add_custom_noun', 'get_period']:
            self._printDebug('-> retrieve status to extract morpheme')

            self.__get_keyword_from_db()
            # o_sv_morpheme_retriever = morpheme_retriever.SvMorphRetriever()
            # o_sv_morpheme_retriever.init_var(self._g_dictSvAcctInfo, s_tbl_prefix,
            #                             self._printDebug, self._printProgressBar, self._continue_iteration,
            #                             self._g_sPluginName, self._g_sAbsRootPath, settings.SV_STORAGE_ROOT,
            #                             s_mode, s_comma_sep_words, s_start_yyyymmdd, s_end_yyyymmdd)
            # o_sv_morpheme_retriever.do_task()
            # del o_sv_morpheme_retriever
        else:
            self._printDebug('-> communication begin')
            with sv_mysql.SvMySql() as o_sv_mysql: # to enforce follow strict mysql connection mgmt
                o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
                o_sv_mysql.set_app_name('svplugins.collect_twitter')
                o_sv_mysql.initialize(self._g_dictSvAcctInfo)
                lst_morpheme = o_sv_mysql.executeQuery('getMorphemeActivated')

            for dict_single_morpheme in lst_morpheme:
                n_morpheme_srl=dict_single_morpheme['morpheme_srl']
                s_morpheme = dict_single_morpheme['morpheme']
                lst_status_registered = self.__get_keyword_from_twitter(n_morpheme_srl, s_morpheme)
                self._printDebug(str(len(lst_status_registered)) + ' tweets have been retrieved for ' + s_morpheme)
                del lst_status_registered
            self._printDebug('-> communication finish')

        self._task_post_proc(self._g_oCallback)

    def __get_keyword_from_twitter(self, n_morpheme_srl, s_morpheme):
        """ retrieve text from twitter API """
        with sv_mysql.SvMySql() as o_sv_mysql: # to enforce follow strict mysql connection mgmt
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_twitter')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            lst_rst = o_sv_mysql.executeQuery('getLatestStatusId', n_morpheme_srl)
            n_since_id = lst_rst[0]['status_id']

        o_sv_twitter = sv_twitter.SvTwitter()
        lst_status = o_sv_twitter.searchQuery(s_morpheme, n_twt_limit=400, n_since_id=n_since_id)
        del o_sv_twitter
        lst_status_registered = []

        for dict_single_status in lst_status:
            # print('https://twitter.com/i/web/status/' + dict_single_status['s_status_id'])
            lst_status_registered.append(dict_single_status['s_status_id'])

            n_full_text_srl = None
            if dict_single_status['dict_rtwit']:
                # print(dict_single_status['s_user_id'] + ' has retwitted status by user ' + dict_single_status['dict_rtwit']['s_user_id'])
                # print('https://twitter.com/i/web/status/' + dict_single_status['dict_rtwit']['s_status_id'])
                # print(dict_single_status['dict_rtwit']['s_full_text'] + ' at ' + str(dict_single_status['dict_rtwit']['dt_created_at']))
                self.__register_user(dict_single_status['dict_rtwit']['s_user_id'], dict_single_status['dict_rtwit']['n_user_followers_cnt'],
                                    dict_single_status['dict_rtwit']['n_user_friends_cnt'], dict_single_status['dict_rtwit']['n_user_favourites_cnt'])
                n_full_text_srl = self.__register_status(n_morpheme_srl, dict_single_status['dict_rtwit'])
                
                if dict_single_status['dict_qtwit']:
                    # print('this twit quoted status by user ' + dict_single_status['dict_qtwit']['s_user_id'])
                    # print('https://twitter.com/i/web/status/' + dict_single_status['dict_qtwit']['s_status_id'])
                    # print(dict_single_status['dict_qtwit']['s_full_text'] + ' at ' + str(dict_single_status['dict_qtwit']['dt_created_at']))
                    self.__register_user(dict_single_status['dict_qtwit']['s_user_id'], dict_single_status['dict_qtwit']['n_user_followers_cnt'],
                                        dict_single_status['dict_qtwit']['n_user_friends_cnt'], dict_single_status['dict_qtwit']['n_user_favourites_cnt'])
                    self.__register_status(n_morpheme_srl, dict_single_status['dict_qtwit'])
            # else:
            #     print(dict_single_status['s_user_id'] + ' twitted')
            #     print(dict_single_status['s_full_text'] + ' at ' + str(dict_single_status['dt_created_at']))

            self.__register_user(dict_single_status['s_user_id'], dict_single_status['n_user_followers_cnt'],
                                dict_single_status['n_user_friends_cnt'], dict_single_status['n_user_favourites_cnt'])
            self.__register_status(n_morpheme_srl, dict_single_status, n_full_text_srl)
            # print('')
        return lst_status_registered
    
    def __register_status(self, n_morpheme_srl, dict_single_status, n_full_text_srl=None):
        """ sub method for __getKeywordFromTwitter() """
        # lst_keys = list(dict_single_status.keys())
        s_rtweet_status_id = 0
        s_qtweet_status_id = 0
        if 'dict_rtwit' in dict_single_status and dict_single_status['dict_rtwit']:
            s_rtweet_status_id = dict_single_status['dict_rtwit']['s_status_id']
        if 'dict_qtwit' in dict_single_status and dict_single_status['dict_qtwit']:
            s_qtweet_status_id = dict_single_status['dict_qtwit']['s_status_id']
        # del lst_keys

        s_status_id = dict_single_status['s_status_id']
        s_user_id = dict_single_status['s_user_id']
        dt_regdate = dict_single_status['dt_created_at']
        n_retweet_cnt = dict_single_status['n_retweet_cnt']
        n_favorite_cnt = dict_single_status['n_favorite_cnt']
        with sv_mysql.SvMySql() as o_sv_mysql: # to enforce follow strict mysql connection mgmt
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_twitter')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            lst_twt_status = o_sv_mysql.executeQuery('getTwtStatusByMorphemeSrl', n_morpheme_srl, s_status_id)
            if len(lst_twt_status) == 0:  # register new twt status
                o_sv_mysql.executeQuery('insertTwtStatus', n_morpheme_srl, s_status_id, s_user_id, 
                                        s_rtweet_status_id, s_qtweet_status_id, n_retweet_cnt, n_favorite_cnt,
                                        dt_regdate)
                if s_rtweet_status_id == 0:
                    lst_rst = o_sv_mysql.executeQuery('insertTwtFullText', dict_single_status['s_full_text'])
                    o_sv_mysql.executeQuery('updateTwtStatusFulltextSrl', lst_rst[0]['id'], s_status_id)
                    return lst_rst[0]['id']
                else:
                    o_sv_mysql.executeQuery('updateTwtStatusFulltextSrl', n_full_text_srl, s_status_id)

        return None

    def __register_user(self, s_user_id, n_followers_cnt, n_friends_cn, n_favourites_cnt):
        """ sub method for __getKeywordFromTwitter() """
        with sv_mysql.SvMySql() as o_sv_mysql: # to enforce follow strict mysql connection mgmt
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_twitter')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)
            lst_twt_user = o_sv_mysql.executeQuery('getTwtUserInfoById', s_user_id)
            if len(lst_twt_user) == 0:  # register new twt user
                o_sv_mysql.executeQuery('insertTwtUserInfo', s_user_id, n_followers_cnt, n_friends_cn, n_favourites_cnt)
            else:  # update twt user
                o_sv_mysql.executeQuery('updateTwtUserInfo', n_followers_cnt, n_friends_cn, n_favourites_cnt, s_user_id)

    def __get_keyword_from_db(self, lst_status_registered=None):
        """ debug method; retrieve text from SV DB """
        with sv_mysql.SvMySql() as o_sv_mysql: # to enforce follow strict mysql connection mgmt
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_twitter')
            o_sv_mysql.initialize(self._g_dictSvAcctInfo)

            if lst_status_registered:
                dict_param = {'s_new_status_ids': ','.join(lst_status_registered)}
                lst_status_id = o_sv_mysql.executeDynamicQuery('getStatusById', dict_param)
                del dict_param
            else:
                lst_status_id = o_sv_mysql.executeQuery('getAllStatus')
            
            for dict_single_status in lst_status_id:
                print('https://twitter.com/i/web/status/' + str(dict_single_status['status_id']))

                if dict_single_status['rtweet_status_id']:
                    lst_rtwt_status = o_sv_mysql.executeQuery('getTwtStatusInfoById', dict_single_status['rtweet_status_id'])
                    print(str(dict_single_status['user_id']) + ' has retwitted status by user ' + str(lst_rtwt_status[0]['user_id']))
                    print('https://twitter.com/i/web/status/' + str(dict_single_status['rtweet_status_id']))
                    lst_rtwt_fulltext = o_sv_mysql.executeQuery('getTwtFulltextBySrl', lst_rtwt_status[0]['full_text_srl'])
                    print(lst_rtwt_fulltext[0]['full_text'] + ' at ' + str(lst_rtwt_status[0]['logdate']))
                    del lst_rtwt_fulltext, lst_rtwt_status

                    if dict_single_status['qtweet_status_id']:
                        lst_qtwt_status = o_sv_mysql.executeQuery('getTwtStatusInfoById', dict_single_status['qtweet_status_id'])
                        print('this twit quoted status by user ' + str(lst_qtwt_status[0]['user_id']))
                        print('https://twitter.com/i/web/status/' + str(dict_single_status['qtweet_status_id']))
                        lst_qtwt_fulltext = o_sv_mysql.executeQuery('getTwtFulltextBySrl', lst_qtwt_status[0]['full_text_srl'])
                        print(lst_qtwt_fulltext[0]['full_text'] + ' at ' + str(lst_qtwt_status[0]['logdate']))
                        del lst_qtwt_fulltext, lst_qtwt_status
                else:
                    print(str(dict_single_status['user_id']) + ' twitted')
                    lst_twt_fulltext = o_sv_mysql.executeQuery('getTwtFulltextBySrl', dict_single_status['full_text_srl'])
                    print(lst_twt_fulltext[0]['full_text'] + ' at ' + str(dict_single_status['logdate']))
                    del lst_twt_fulltext

                print('')


if __name__ == '__main__': # for console debugging
    # CLI example -> python3.7 task.py config_loc=1/1
    # collect_twitter
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('collect_twitter')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params are required for console execution.')
