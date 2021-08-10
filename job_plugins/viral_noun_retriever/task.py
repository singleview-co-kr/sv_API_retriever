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

"""
how to word cloud
https://konlpy.org/ko/v0.5.2/install/
https://liveyourit.tistory.com/58

Install KoNLPy on CentOS 7, 8
# yum install gcc-c++
# yum install java-1.8.0-openjdk-devel
# python3.8 -m pip install --upgrade pip
# python3.8 -m pip install konlpy
# yum install curl git
# bash <(curl -s https://raw.githubusercontent.com/konlpy/konlpy/master/scripts/mecab.sh)

install wordcloud 
# pip3.8 install wordcloud

# https://inspiringpeople.github.io/data%20analysis/ckonlpy/
# https://pypi.org/project/customized-KoNLPy/

# how to install customized_konlpy
# git clone https://github.com/lovit/customized_konlpy.git
# pip3.x install customized_konlpy
"""

# standard library
import logging
import time
import re # https://docs.python.org/3/library/re.html
from datetime import datetime

# 3rd-party library
# from konlpy.tag import Okt
from ckonlpy.tag import Twitter

from collections import Counter
from wordcloud import WordCloud

# singleview library
if __name__ == '__main__': # for console debugging
    import sys
    sys.path.append('../../classes')
    # import sv_slack
    import sv_mysql
    import sv_api_config_parser
    sys.path.append('../../conf') # singleview config
else: # for platform running
    # from classes import sv_slack
    # singleview config
    from classes import sv_mysql
    from classes import sv_api_config_parser
    # singleview config
    from conf import basic_config # singleview config

class svJobPlugin():
    __g_sVersion = '0.0.1'
    __g_sLastModifiedDate = '4th, Jul 2021'
    
    __g_sConfigLoc = None
    __g_sMode = None
    __g_sCommaSeparatedWords = None
    __g_sStartYyyymmdd = None
    __g_sEndYyyymmdd = None

    __g_oLogger = None
    __g_dictCountingNouns = {}
    __g_lstCountingNounsSrl = []
    __g_dictRegisteredNouns = {}

    __g_oTwitter = None

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_sConfigLoc = dictParams['config_loc']
        self.__g_sMode = dictParams['mode']
        
        if dictParams['words']:
            self.__g_sCommaSeparatedWords = dictParams['words']
        if dictParams['start_yyyymmdd']:
            self.__g_sStartYyyymmdd = dictParams['start_yyyymmdd']
        if dictParams['end_yyyymmdd']:
            self.__g_sEndYyyymmdd = dictParams['end_yyyymmdd']

        self.__g_oTwitter = Twitter()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __printDebug( self, sMsg ):
        if __name__ == '__main__': # for console debugging
            print( sMsg )
        else: # for platform running
            if( self.__g_oLogger is not None ):
                self.__g_oLogger.debug( sMsg )

    def procTask(self):
        self.__printDebug( 'start' )
        # config file can be http URL or file path
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self.__g_sConfigLoc)
        oResp = oSvApiConfigParser.getConfig()
        
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self.__printDebug('invalid config_loc')
            raise Exception('stop')

        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        
        lst_noun = []
        if self.__g_sMode == 'tag_ignore_word':
            print('tag_ignore_word')
            self.__tag_ignore_word(s_acct_title)
            self.__load_custom_noun(s_acct_title)
            # self.__register_new_word_cnt(s_acct_title)
            lst_noun = self.__retrieve_word_cnt(s_acct_title)
            print(lst_noun)
        elif self.__g_sMode == 'add_custom_noun':
            print('add_custom_noun')
            self.__add_custom_noun(s_acct_title)
            self.__load_custom_noun(s_acct_title)
            self.__register_new_word_cnt(s_acct_title)
            lst_noun = self.__retrieve_word_cnt(s_acct_title)
            print(lst_noun)
        elif self.__g_sMode == 'analyze_new':
            print('analyze_new')
            self.__load_custom_noun(s_acct_title)
            self.__register_new_word_cnt(s_acct_title)
            lst_noun = self.__retrieve_word_cnt(s_acct_title)
            print(lst_noun)
        elif self.__g_sMode == 'get_period':
            print('get_period')
            dict_period = {'dt_start': None, 'dt_end': None}
            if self.__g_sStartYyyymmdd:
                dict_period['dt_start'] = datetime.strptime(self.__g_sStartYyyymmdd, '%Y%m%d')
            if self.__g_sEndYyyymmdd:
                dict_period['dt_end'] = datetime.strptime(self.__g_sEndYyyymmdd, '%Y%m%d')
            
            lst_noun = self.__retrieve_word_cnt(s_acct_title, dict_period)
            print(lst_noun)
        else:
            print('weird')
        
        if len(lst_noun):
            # wc = WordCloud(font_path='C:\\Windows\\Fonts\\08SeoulNamsanB_0.ttf', \
            # wc = WordCloud(
            wc = WordCloud(font_path='/sata_data/python/godoMaum.ttf', \
                background_color="white", \
                width=1000, \
                height=1000, \
                max_words=100, \
                max_font_size=300)
                
            # wc.generate(news)
            wc.generate_from_frequencies(dict(lst_noun))
            wc.to_file('keyword.png')
        return
    
    def __tag_ignore_word(self, s_acct_title):
        lst_ignore_word = self.__g_sCommaSeparatedWords.split(',')

        if len(lst_ignore_word) == 0:
            return
        with sv_mysql.SvMySql('job_plugins.viral_noun_retriever') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(s_acct_title)
            o_sv_mysql.initialize()
            for s_ignore_word in lst_ignore_word:
                lst_rst = o_sv_mysql.executeQuery('updateIgnoreWord', s_ignore_word)
    
    def __add_custom_noun(self, s_acct_title):
        lst_custom_noun = self.__g_sCommaSeparatedWords.split(',')

        if len(lst_custom_noun) == 0:
            return
        with sv_mysql.SvMySql('job_plugins.viral_noun_retriever') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(s_acct_title)
            o_sv_mysql.initialize()
            for s_custom_noun in lst_custom_noun:
                o_sv_mysql.executeQuery('insertCustomNoun', s_custom_noun)
        
            # o_sv_mysql.truncateTable('wc_collected_dictionary')
            o_sv_mysql.truncateTable('wc_word_cnt')  # reset word_cnt
            o_sv_mysql.executeQuery('updateAllDocNonProced')  # reset all document processed status

    def __load_custom_noun(self, s_acct_title):
        with sv_mysql.SvMySql('job_plugins.viral_noun_retriever') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(s_acct_title)
            o_sv_mysql.initialize()
            lst_rst = o_sv_mysql.executeQuery('getCustomDictionary')
            
        lst_customized_nouns = []
        for dict_row in lst_rst:
            lst_customized_nouns.append(dict_row['word'])
        del lst_rst

        if len(lst_customized_nouns):
            self.__g_oTwitter.add_dictionary(words=lst_customized_nouns, tag='Noun')
        del lst_customized_nouns
    
    def __register_new_word_cnt(self, s_acct_title):
        with sv_mysql.SvMySql('job_plugins.viral_noun_retriever') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(s_acct_title)
            o_sv_mysql.initialize()
            lst_rst = o_sv_mysql.executeQuery('getRegisteredWords')
            
            for dict_row in lst_rst:
                self.__g_dictRegisteredNouns[dict_row['word']] = {'word_srl': dict_row['word_srl'], 'b_ignore': dict_row['b_ignore']}
            del lst_rst

            lst_new_doc_detail = o_sv_mysql.executeQuery('getNonProcedDocs')
            # print(lst_new_doc_detail)

            for dict_row in lst_new_doc_detail:
                lst_counting_word = self.__get_noun(dict_row['content'])
                # self.__printDebug(dict_row['content'][:40])
                # print(str(dict_row['document_srl']) + ':' + str(dict_row['logdate']))
                counter_noun_count = Counter(lst_counting_word)
                # print(counter_noun_count )
                
                for s_word, n_cnt in counter_noun_count.items():
                    try:
                        n_word_srl = self.__g_dictRegisteredNouns[s_word]['word_srl']
                    except KeyError:
                        lst_rst = o_sv_mysql.executeQuery('insertDictionary', s_word )
                        n_word_srl = lst_rst[0]['id']
                        self.__g_dictRegisteredNouns[s_word] = {'word_srl': n_word_srl, 'b_ignore': 0}

                    # print(s_word + ' -> ' + str(n_word_srl) + ' -> ' + str(n_cnt))
                    o_sv_mysql.executeQuery('insertWordCnt', dict_row['referral'], dict_row['document_srl'], n_word_srl, n_cnt, dict_row['logdate'])
                del counter_noun_count
                # set document processed to avoid duplicated analyze
                o_sv_mysql.executeQuery('updateDocProcedByLogSrl', dict_row['log_srl'])
            del lst_new_doc_detail
    
    def __get_noun(self, s_phrase):
        lst_counting_word = []
        lst_retrieved_nouns = self.__g_oTwitter.nouns(s_phrase)
        for n_idx, s_noun in enumerate(lst_retrieved_nouns):
            s_noun_stripped = s_noun.strip()
            if len(s_noun_stripped) < 2:
                continue

            lst_counting_word.append(s_noun_stripped)
        del lst_retrieved_nouns
        return lst_counting_word

    def __retrieve_word_cnt(self, s_acct_title, dict_period=None):
        
        if dict_period is None:
            b_mode = 'full_period'
        elif dict_period['dt_start'] is None or dict_period['dt_end'] is None:
            b_mode = 'full_period'
        else:
            b_mode = 'partial_period'

        with sv_mysql.SvMySql('job_plugins.viral_noun_retriever') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(s_acct_title)
            o_sv_mysql.initialize()
            
            lst_counting_words_rst = o_sv_mysql.executeQuery('getCollectedDictionary')
            for dict_row in lst_counting_words_rst:
                self.__g_dictCountingNouns[dict_row['word_srl']] = dict_row['word']
            del lst_counting_words_rst
            
            lst_counting_words_srls = list(self.__g_dictCountingNouns.keys())
            
            if  b_mode == 'partial_period':
                lst_rst = o_sv_mysql.executeQuery('getWordCntByPeriod', dict_period['dt_start'], dict_period['dt_end'])
            elif b_mode == 'full_period':
                lst_rst = o_sv_mysql.executeQuery('getWordCnt')
            
            print( str(len(lst_rst)) + ' documents')
            for dict_row in lst_rst:
                n_word_srl = dict_row['word_srl']
                if n_word_srl in lst_counting_words_srls:
                    s_translate_word = self.__g_dictCountingNouns[n_word_srl]
                    for n_dummy in range(0, dict_row['cnt']):
                        self.__g_lstCountingNounsSrl.append(s_translate_word)
               
        counter_noun_count = Counter(self.__g_lstCountingNounsSrl)  # sum freq by noun and sort
        lst_noun = counter_noun_count.most_common(100)
        return lst_noun

if __name__ == '__main__': # for console debugging and execution  
    # CLI example -> python3.7 task.py config_loc=2/test_acct mode=tag_ignore_word words=
    # CLI example -> python3.7 task.py config_loc=2/test_acct mode=add_custom_noun words=
    # CLI example -> python3.7 task.py config_loc=2/test_acct mode=analyze_new
    # CLI example -> python3.7 task.py config_loc=2/test_acct mode=get_period start_yyyymmdd=20210708 end_yyyymmdd=20210711
    dictPluginParams = {'config_loc': None, 'mode': None, 'words': None, 'start_yyyymmdd': None, 'end_yyyymmdd': None }
    nCliParams = len(sys.argv)
    if( nCliParams > 1 ):
        for i in range(nCliParams):
            if i is 0:
                continue

            sArg = sys.argv[i]
            for sParamName in dictPluginParams:
                nIdx = sArg.find( sParamName + '=' )
                if( nIdx > -1 ):
                    aModeParam = sArg.split('=')
                    dictPluginParams[sParamName] = aModeParam[1]
                
        #print( dictPluginParams )
        with svJobPlugin(dictPluginParams) as oJob: # to enforce to call plugin destructor
            oJob.procTask()
    else:
        print( 'warning! [config_loc] params are required for console execution.' )

"""
import sys
from konlpy.tag import Okt
from collections import Counter
from wordcloud import WordCloud

def get_noun(news):
	okt = Okt()
	noun = okt.nouns(news)
	for i,v in enumerate(noun):
		if len(v) < 2:
			noun.pop(i)

	count = Counter(noun)
	noun_list = count.most_common(100)

	return noun_list


def visualize(noun_list):
	wc = WordCloud(font_path='C:\\Windows\\Fonts\\08SeoulNamsanB_0.ttf', \
		background_color="white", \
		width=1000, \
		height=1000, \
		max_words=100, \
		max_font_size=300)

	wc.generate_from_frequencies(dict(noun_list))
	wc.to_file('keyword.png')


if __name__=="__main__":
	filename = sys.argv[1]
	f = open(filename,'r',encoding='utf-8')	
	news = f.read()
	noun_list = get_noun(news)
	visualize(noun_list)
"""