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
"""

# standard library
import logging
import time
import re # https://docs.python.org/3/library/re.html

# 3rd-party library
from konlpy.tag import Okt
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
    __g_oLogger = None
    __g_lstRetrievedNouns = []
    __g_lstRemoveWord = ['당신', '경우', '때문', '사실', '저희', '가지', '널리', '여러', '다른', '모든', '위해', '가장', '직접', '계속', '이제', '통해' ]
    
    __g_dictRegisteredNouns = {}

    def __init__( self, dictParams ):
        """ validate dictParams and allocate params to private global attribute """
        self.__g_oLogger = logging.getLogger(__name__ + ' v'+self.__g_sVersion)
        self.__g_sConfigLoc = dictParams['config_loc']
        self.__g_oOkt = Okt()
        # self.__g_sTxtFilePath = dictParams['txt_file']

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
        lst_noun = self.__retrieve_viral(s_acct_title)

        # wc = WordCloud(font_path='C:\\Windows\\Fonts\\08SeoulNamsanB_0.ttf', \
        # wc = WordCloud(
        wc = WordCloud(font_path='/sata_data/python/godoMaum.ttf', \
            background_color="white", \
            width=1000, \
            height=1000, \
            max_words=100, \
            max_font_size=300)
            
        # wc.generate(news)
        # wc.generate_from_frequencies(dict(lst_noun))
        # wc.to_file('keyword.png')
        return
    
    def __retrieve_viral(self, s_acct_title):
        with sv_mysql.SvMySql('job_plugins.viral_noun_retriever') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(s_acct_title)
            o_sv_mysql.initialize()
            lst_rst = o_sv_mysql.executeQuery('getRegisteredWords')
            
            for dict_row in lst_rst:
                self.__g_dictRegisteredNouns[dict_row['word']] = {'word_srl': dict_row['word_srl'], 'b_ignore': dict_row['b_ignore']}
        del lst_rst
        # print(self.__g_dictRegisteredNouns)

        with sv_mysql.SvMySql('job_plugins.viral_noun_retriever') as o_sv_mysql:
            o_sv_mysql.setTablePrefix(s_acct_title)
            o_sv_mysql.initialize()
            lst_rst = o_sv_mysql.executeQuery('getSvDocuments')
        
            for dict_row in lst_rst:
                lst_counting_word = self.__get_noun(dict_row['content'])
                # self.__printDebug(dict_row['content'][:40])
                print(dict_row['document_srl'])
                print(dict_row['logdate'])
                counter_noun_count = Counter(lst_counting_word)
                # print(counter_noun_count )
                
                for s_word, n_cnt in counter_noun_count.items():
                    try:
                        n_word_srl = self.__g_dictRegisteredNouns[s_word]['word_srl']
                    except KeyError:
                        lst_rst = o_sv_mysql.executeQuery('insertDictionary', s_word )
                        n_word_srl = lst_rst[0]['id']
                    print(s_word + ' -> ' + str(n_word_srl) + ' -> ' + str(n_cnt))
                    # `document_srl`, `word_srl`, `cnt`, `logdate`
                    o_sv_mysql.executeQuery('insertRetrievedWord', dict_row['document_srl'], n_word_srl, n_cnt, dict_row['logdate'] )
                del counter_noun_count 
            del lst_rst
        
        # print(self.__g_dictRegisteredNouns)
        counter_noun_count = Counter(self.__g_lstRetrievedNouns)  # sum freq by noun and sort
        lst_noun = counter_noun_count  # .most_common(100)
        return lst_noun 

    def __get_noun(self, s_phrase):
        lst_counting_word = []
        lst_retrieved_nouns = self.__g_oOkt.nouns(s_phrase)
        for n_idx, s_noun in enumerate(lst_retrieved_nouns):
            s_noun_stripped = s_noun.strip()
            if len(s_noun_stripped) < 2:
                # print( s_noun_stripped + ' -> ' + str(len(s_noun_stripped)))
                # lst_retrieved_nouns.pop(n_idx)
                continue

            if s_noun_stripped in self.__g_lstRemoveWord:
                # lst_retrieved_nouns.pop(n_idx)
                continue
            lst_counting_word.append(s_noun_stripped)
        
        # print(lst_retrieved_nouns)
        self.__g_lstRetrievedNouns = self.__g_lstRetrievedNouns + lst_counting_word
        del lst_retrieved_nouns  #, lst_counting_word
        
        # counter_noun_count = Counter(lst_retrieved_nouns)  # sum freq by noun and sort
        # lst_noun = counter_noun_count .most_common(100)
        return lst_counting_word

if __name__ == '__main__': # for console debugging and execution  
    # CLI example -> python3.7 task.py config_loc=2/test_acct
    dictPluginParams = {'config_loc':None }
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