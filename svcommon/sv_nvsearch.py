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
import urllib.request
import logging
import configparser # https://docs.python.org/3/library/configparser.html

# 3rd party library
from decouple import config
import xmltodict

# singleview config
if __name__ == 'svcommon.sv_nvsearch': # for platform running
    from svcommon import sv_object
elif __name__ == 'sv_nvsearch': # for plugin console debugging
    import sv_object
elif __name__ == '__main__': # for class console debugging
    import sv_object

    
class SvNvsearch(sv_object.ISvObject):
    """  """
    __g_sApiReqUrl = "https://openapi.naver.com/v1/search/"
    __g_lstMediaInfo = [
        (1, 'blog', 'naver blog'),
        (2, 'news', 'naver news'),
        (3, 'encyc', 'naver encyclopedia'),  # useless, date sort unavailable
        (4, 'cafearticle', 'naver cafe article'),
        (5, 'kin', 'naver knowledge people'),
        (6, 'webkr', 'naver web document'),  # date sort unavailable
        (7, 'image', 'naver image'),
        (8, 'shop', 'naver shopping'),  # asc: 가격순으로 오름차순 정렬
        (9, 'doc', 'naver professional document'),  # useless, date sort unavailable
    ]
    
    __g_lstMedia = None
    __g_dictMediaLblId = None
    __g_nDisplayCnt = 100  # maximum number of results to retrieve from API
    __g_nIterationCnt = 0
    __g_sClientId = None
    __g_sClientSecret = False

    def __init__(self):
        self.__g_sCurMedia = None
        o_config = configparser.ConfigParser()
        self._g_oLogger = logging.getLogger(__file__)
        s_viral_config_file = os.path.join(config('ABSOLUTE_PATH_BOT'), 'conf', 'viral_config.ini')
        
        try:
            with open(s_viral_config_file) as f:
                o_config.read_file(f)
                self.__g_bAvailable = True
        except IOError:
            self._printDebug('viral_config.ini does not exist')

        if self.__g_bAvailable:
            o_config.read(s_viral_config_file)
                
        try:  # attempt to read API key
            self.__g_sClientId = o_config['NV_SEARCH']['client_id']
            self.__g_sClientSecret = o_config['NV_SEARCH']['client_secret']
        except:
            self._printDebug('Error: Invalid Naver Search API info')
        finally:
            del o_config
        
        self.__g_lstMedia = [tup_single[1] for tup_single in self.__g_lstMediaInfo]
        self.__g_dictMediaLblId = {tup_single[1]: tup_single[0] for tup_single in self.__g_lstMediaInfo}

    def __del__(self):
        self.__g_sCurMedia = None
        self.__g_lstMedia = None
        self.__g_dictMediaLblId = None

    def get_media_lbl_id_dict(self):
        return self.__g_dictMediaLblId

    def set_media(self, s_media):
        self.__g_nIterationCnt = 0
        if s_media in self.__g_lstMedia:
            self.__g_sCurMedia = s_media
        else:
            self.__g_sCurMedia = None
            self._printDebug('invalid naver search API media')

    def set_display_cnt(self, n_cnt):
        if n_cnt > 10:  # no reason to request less than 10; default list count is 10, consumes one API daily hit regardless requested list count
            self.__g_nDisplayCnt = n_cnt

    def load_xml(self, s_xml):
        # https://m.blog.naver.com/PostView.naver?isHttpsRedirect=true&blogId=pk3152&logNo=221367256441
        dict_xml = xmltodict.parse(s_xml)
        dict_body = dict_xml['rss']['channel']
        del dict_xml
        dict_body['total'] = int(dict_body['total'])
        return dict_body

    def search_query(self, s_morpheme):
        """ https://developers.naver.com/docs/serviceapi/search/blog/blog.md#python """
        # query	String	필수	검색어. UTF-8로 인코딩되어야 합니다.
        # display	Integer	선택	한 번에 표시할 검색 결과 개수(기본값: 10, 최댓값: 100)
        # start	Integer	선택	검색 시작 위치(기본값: 1, 최댓값: 1000)
        # sort	String	선택	검색 결과 정렬 방법
        # - sim: 정확도순으로 내림차순 정렬(기본값)
        # - date: 날짜순으로 내림차순 정렬
        # url = "https://openapi.naver.com/v1/search/blog?query=" + encText # JSON 결과
        # url = "https://openapi.naver.com/v1/search/blog.xml?query=" + encText # XML 결과
        dict_rst = {'b_error': False, 's_msg': None, 's_plain_resp': None, 'dict_xml_body': {}}

        if self.__g_sCurMedia is None:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'invaid media'
            return dict_rst
        if len(s_morpheme) == 0:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'invaid morpheme'
            return dict_rst
        if self.__g_nIterationCnt >= 10:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'too many iteration'
            return dict_rst
        
        n_start = self.__g_nIterationCnt * self.__g_nDisplayCnt + 1
        self.__g_nIterationCnt += 1
        # JSON 사용 불가능 value 내부의 " 기호 escape를 \"로 처리해서 json.load() 작동 불가
        url = self.__g_sApiReqUrl + self.__g_sCurMedia + '.xml?query=' + urllib.parse.quote(s_morpheme) + \
                '&start=' + str(n_start) + '&display=' + str(self.__g_nDisplayCnt)
        if self.__g_sCurMedia not in ['encyc', 'webkr', 'doc']:
            url += '&sort=date'
        if self.__g_sCurMedia in ['shop']:
            url += '&sort=asc'  # 가격순으로 오름차순 정렬, 최저가 우선

        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id", self.__g_sClientId)
        request.add_header("X-Naver-Client-Secret", self.__g_sClientSecret)
        response = urllib.request.urlopen(request)
        rescode = response.getcode()
        if rescode==200:
            response_body = response.read()
            dict_rst['s_plain_resp'] = response_body.decode('utf-8')
        else:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = "Error Code:" + rescode
        # print('##########################')
        # print('##########################')
        # print('##########################')
        # print(dict_rst['s_plain_resp'])
        # print('##########################')
        # print('##########################')
        # print('##########################')

        # if self.__g_sCurMedia == 'blog':
        #     ddd = """"""
        # else:
        #     ddd = """<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Naver Open API - blog ::&apos;유한락스&apos;</title><link>https://search.naver.com</link><description>Naver Search Result</description><lastBuildDate>Mon, 23 Jan 2023 17:50:43 +0900</lastBuildDate><total>10804</total><start>1</start><display>100</display><item></item></channel></rss>"""
        # dict_rst['s_plain_resp'] = ddd

        dict_rst['dict_xml_body'] = self.load_xml(dict_rst['s_plain_resp'])
        return dict_rst


# if __name__ == '__main__': # for console debugging
#     o_sv_nvsearch = SvNvsearch()
#     o_sv_nvsearch.set_media('blog')
#     o_sv_nvsearch.search_query(s_morpheme='유한락스')
