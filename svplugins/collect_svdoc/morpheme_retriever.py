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
import os
import sys
import re
import logging
from datetime import datetime
from collections import Counter

# 3rd-party library
from ckonlpy.tag import Twitter
from wordcloud import WordCloud
# import nltk  # this disturbs plugin deallocation on an web console 


# singleview library
if __name__ == 'morpheme_retriever': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_mysql
else: # for platform running
    from svcommon import sv_mysql


class SvMorphRetriever():
    __o_reg_korean = re.compile('[\u3131-\u3163\uac00-\ud7a3]+')  # get Korean unicode
    __o_reg_alpha_numeric = re.compile('[a-zA-Z0-9]+')  # get alphanumeric
    __o_lambda_is_noun = None  # lambda pos: pos[:2] == 'NN'

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        # print('item:__init__')
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        self.__g_sTblPrefix = None
        self.__g_sMode = None 

        self._g_oLogger = logging.getLogger(__name__)

        self.__g_sAbsRootPath = None
        self.__g_sSvStorageRoot = None
        self.__g_sPluginName = None
        self.__g_sCommaSeparatedWords = None
        self.__g_sStartYyyymmdd = None
        self.__g_sEndYyyymmdd = None
        self.__g_dictCountingNouns = {}
        self.__g_lstCountingNounsSrl = []
        self.__g_dictRegisteredNouns = {}
        self.__g_oTwitter = None
        self.__o_lambda_is_noun = lambda pos: pos[:2] == 'NN'

    def __del__(self):
        self.__continue_iteration = None
        self.__print_debug = None
        self.__print_progress_bar = None
        self.__g_sTblPrefix = None
        self.__g_sMode = None

        self.__g_sAbsRootPath = None
        self.__g_sSvStorageRoot = None
        self.__g_sPluginName = None
        self.__g_sCommaSeparatedWords = None
        self.__g_sStartYyyymmdd = None
        self.__g_sEndYyyymmdd = None
        self.__g_dictCountingNouns = {}
        self.__g_lstCountingNounsSrl = []
        self.__g_dictRegisteredNouns = {}
        self.__g_oTwitter = None

    def init_var(self, dict_sv_acct_info, s_tbl_prefix, 
                    f_print_debug, f_print_progress_bar, f_continue_iteration,
                    s_plugin_name, s_abs_root_path, s_sv_storage_root,
                    s_mode, s_comma_sep_words, s_start_yyyymmdd, s_end_yyyymmdd):
        self.__g_dictSvAcctInfo = dict_sv_acct_info
        self.__continue_iteration = f_continue_iteration
        self.__print_debug = f_print_debug
        self.__print_progress_bar = f_print_progress_bar
        self.__g_sTblPrefix = s_tbl_prefix
        self.__g_sPluginName = s_plugin_name
        self.__g_sAbsRootPath = s_abs_root_path
        self.__g_sSvStorageRoot = s_sv_storage_root
        self.__g_sMode = s_mode 
        self.__g_sCommaSeparatedWords = s_comma_sep_words
        self.__g_sStartYyyymmdd = s_start_yyyymmdd
        self.__g_sEndYyyymmdd = s_end_yyyymmdd

    def do_task(self):
        import nltk  # this import disturbs the plugin turn into __del__() automatically
        self.__g_oTwitter = Twitter()
        # print(self.__g_dictSvAcctInfo)

        ################################
        # https://www.inflearn.com/questions/158593
        nltk.download('punkt')
        nltk.download('averaged_perceptron_tagger')
        ################################
        s_sv_acct_id = self.__g_dictSvAcctInfo['n_acct_id']
        s_brand_id = self.__g_dictSvAcctInfo['n_brand_id']
        lst_noun = []
        if self.__g_sMode == 'tag_ignore_word':
            self.__print_debug('tag_ignore_word')
            self.__tag_ignore_word()
            self.__load_custom_noun()
            # self.__register_new_word_cnt(s_acct_title)
            lst_noun = self.__retrieve_word_cnt()
            self.__print_debug(lst_noun)
        elif self.__g_sMode == 'add_custom_noun':
            self.__print_debug('add_custom_noun')
            self.__add_custom_noun()
            self.__load_custom_noun()
            self.__register_new_word_cnt()
            lst_noun = self.__retrieve_word_cnt()
            self.__print_debug(lst_noun)
        elif self.__g_sMode == 'analyze_new':
            self.__print_debug('analyze_new')
            self.__load_custom_noun()
            self.__register_new_word_cnt()
            lst_noun = self.__retrieve_word_cnt()
            self.__print_debug(lst_noun)
        elif self.__g_sMode == 'get_period':
            self.__print_debug('get_period')
            dict_period = {'dt_start': None, 'dt_end': None}
            if self.__g_sStartYyyymmdd:
                dict_period['dt_start'] = datetime.strptime(self.__g_sStartYyyymmdd, '%Y%m%d')
            if self.__g_sEndYyyymmdd:
                dict_period['dt_end'] = datetime.strptime(self.__g_sEndYyyymmdd, '%Y%m%d')
            
            lst_noun = self.__retrieve_word_cnt(dict_period)
            self.__print_debug(lst_noun)
        else:
            self.__print_debug('weird')
            return
        
        try:
            n_noun_cnt = len(lst_noun)
            if n_noun_cnt:
                s_font_file_path_abs = os.path.join(self.__g_sAbsRootPath, 'svplugins', 'collect_svdoc', 'fonts', 'godoMaum.ttf')
                wc = WordCloud(font_path=s_font_file_path_abs, \
                    background_color="white", \
                    width=1000, \
                    height=1000, \
                    max_words=100, \
                    max_font_size=300)
                    
                # wc.generate(news)
                wc.generate_from_frequencies(dict(lst_noun))
                s_wc_file_path_abs = os.path.join(self.__g_sAbsRootPath, self.__g_sSvStorageRoot , s_sv_acct_id, s_brand_id, 'keyword.png')
                wc.to_file(s_wc_file_path_abs)
        except TypeError:  # len(lst_noun) occurs exception if interrupted on a web console
            self.__print_debug('Processing has been interrupted abnormally')
            return
            
    def __tag_ignore_word(self):
        lst_ignore_word = self.__g_sCommaSeparatedWords.split(',')

        if len(lst_ignore_word) == 0:
            return
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_svdoc')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            for s_ignore_word in lst_ignore_word:
                o_sv_mysql.executeQuery('updateIgnoreWord', s_ignore_word)
    
    def __add_custom_noun(self):
        lst_custom_noun = self.__g_sCommaSeparatedWords.split(',')
        if len(lst_custom_noun) == 0:
            return
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_svdoc')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            for s_custom_noun in lst_custom_noun:
                o_sv_mysql.executeQuery('insertCustomNoun', s_custom_noun)
        
            o_sv_mysql.truncateTable('wc_word_cnt')  # reset word_cnt
            o_sv_mysql.executeQuery('updateAllDocNonProced')  # reset all document processed status

    def __load_custom_noun(self):
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_svdoc')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            lst_rst = o_sv_mysql.executeQuery('getCustomDictionary')
            
        lst_customized_nouns = []
        for dict_row in lst_rst:
            lst_customized_nouns.append(dict_row['word'])
        del lst_rst

        if len(lst_customized_nouns):
            self.__g_oTwitter.add_dictionary(words=lst_customized_nouns, tag='Noun')
        del lst_customized_nouns
    
    def __register_new_word_cnt(self):
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_svdoc')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
            lst_rst = o_sv_mysql.executeQuery('getRegisteredWords')
            for dict_row in lst_rst:
                self.__g_dictRegisteredNouns[dict_row['word']] = {'word_srl': dict_row['word_srl'], 'b_ignore': dict_row['b_ignore']}
            del lst_rst

            n_iter_cnt = 0
            lst_new_doc_detail = o_sv_mysql.executeQuery('getNonProcedDocs')
            for dict_row in lst_new_doc_detail:
                lst_counting_word = self.__get_noun(dict_row['content'])
                counter_noun_count = Counter(lst_counting_word)
                for s_word, n_cnt in counter_noun_count.items():
                    if not self.__continue_iteration():
                        return
                    else:
                        if n_iter_cnt % 5000 == 0:
                            self.__print_debug(self.__g_sPluginName + ' is registering ' + str(n_iter_cnt) + 'th word')
                    
                    n_iter_cnt = n_iter_cnt + 1
                    # try:
                    if self.__g_dictRegisteredNouns.get(s_word, 0):  # returns 0 if sRowId does not exist
                        n_word_srl = self.__g_dictRegisteredNouns[s_word]['word_srl']
                    # except KeyError:
                    else:
                        lst_rst = o_sv_mysql.executeQuery('insertDictionary', s_word )
                        n_word_srl = lst_rst[0]['id']
                        self.__g_dictRegisteredNouns[s_word] = {'word_srl': n_word_srl, 'b_ignore': 0}

                    o_sv_mysql.executeQuery('insertWordCnt', dict_row['referral'], dict_row['document_srl'], dict_row['module_srl'], n_word_srl, n_cnt, dict_row['logdate'])
                del counter_noun_count
                # set document processed to avoid duplicated analyze
                o_sv_mysql.executeQuery('updateDocProcedByLogSrl', dict_row['log_srl'])
            del lst_new_doc_detail
    
    def __get_noun(self, s_phrase):
        lst_eng_kor_word = self.__get_noun_eng_kor_combined(s_phrase)
        lst_counting_word = self.__get_noun_pure_korean(s_phrase)
        return lst_counting_word + lst_eng_kor_word

    def __get_noun_eng_kor_combined(self, s_phrase):
        # begin - eng + kor combined morpheme
        lst_eng_kor_word = []
        s_phrase = s_phrase.upper()  # upper if contains English
        import nltk
        lst_eng_tokenized = nltk.word_tokenize(s_phrase)
        lst_eng_temp = [word for (word, pos) in nltk.pos_tag(lst_eng_tokenized) if self.__o_lambda_is_noun(pos)]
        del lst_eng_tokenized
        
        for s_eng_kor_word in lst_eng_temp:
            if self.__o_reg_alpha_numeric.match(s_eng_kor_word):
                # print(s_eng_kor_word + " contains an alphabet")
                s_pure_english = re.sub(self.__o_reg_korean, '', s_eng_kor_word)
                #print('remove korean : ' + s_pure_english)
                s_pure_korean = re.sub(self.__o_reg_alpha_numeric, '', s_eng_kor_word)
                #print('remove english : ' + s_pure_korean)
                lst_kor_morpheme = self.__get_noun_pure_korean(s_pure_korean)
                if len(lst_kor_morpheme) > 0:
                    #print(s_pure_english + lst_kor_morpheme[0])
                    lst_eng_kor_word.append(s_pure_english + lst_kor_morpheme[0])
                elif len(s_pure_english) > 1:
                        lst_eng_kor_word.append(s_pure_english)
        # end - eng + kor combined morpheme
        ################################
        return lst_eng_kor_word

    def __get_noun_pure_korean(self, s_phrase):
        # begin - pure Korean morpheme
        lst_counting_word = []
        lst_retrieved_nouns = self.__g_oTwitter.nouns(s_phrase)
        for n_idx, s_noun in enumerate(lst_retrieved_nouns):
            s_noun_stripped = s_noun.strip()
            if len(s_noun_stripped) < 2:
                continue

            lst_counting_word.append(s_noun_stripped)
        del lst_retrieved_nouns
        # end - pure Korean morpheme
        return lst_counting_word

    def __retrieve_word_cnt(self, dict_period=None):
        if dict_period is None:
            b_mode = 'full_period'
        elif dict_period['dt_start'] is None or dict_period['dt_end'] is None:
            b_mode = 'full_period'
        else:
            b_mode = 'partial_period'

        n_iter_cnt = 1
        with sv_mysql.SvMySql() as o_sv_mysql:
            o_sv_mysql.setTablePrefix(self.__g_sTblPrefix)
            o_sv_mysql.set_app_name('svplugins.collect_svdoc')
            o_sv_mysql.initialize(self.__g_dictSvAcctInfo)
                        
            lst_counting_words_rst = o_sv_mysql.executeQuery('getCollectedDictionary')
            for dict_row in lst_counting_words_rst:
                self.__g_dictCountingNouns[dict_row['word_srl']] = dict_row['word']
            del lst_counting_words_rst
            
            # lst_counting_words_srls = list(self.__g_dictCountingNouns.keys())
            if  b_mode == 'partial_period':
                lst_rst = o_sv_mysql.executeQuery('getWordCntByPeriod', dict_period['dt_start'], dict_period['dt_end'])
            elif b_mode == 'full_period':
                lst_rst = o_sv_mysql.executeQuery('getWordCnt')
            
            self.__print_debug(str(len(lst_rst)) + ' documents')
            for dict_row in lst_rst:
                n_word_srl = dict_row['word_srl']
                # if n_word_srl in lst_counting_words_srls:
                if n_word_srl in self.__g_dictCountingNouns:
                    s_translate_word = self.__g_dictCountingNouns[n_word_srl]
                    for n_dummy in range(0, dict_row['cnt']):
                        if not self.__continue_iteration():
                            return
                        else:
                            if n_iter_cnt % 5000 == 0:
                                self.__print_debug(self.__g_sPluginName + ' is retrieving ' + str(n_iter_cnt) + 'th word')
                        n_iter_cnt = n_iter_cnt + 1
                        
                        self.__g_lstCountingNounsSrl.append(s_translate_word)
               
        counter_noun_count = Counter(self.__g_lstCountingNounsSrl)  # sum freq by noun and sort
        lst_noun = counter_noun_count.most_common(100)
        return lst_noun