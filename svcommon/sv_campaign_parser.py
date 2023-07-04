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
import csv
import sys
import re
from datetime import datetime

# 3rd party library
from decouple import config  # https://pypi.org/project/python-decouple/

# singleview config
if __name__ == 'svcommon.sv_campaign_parser':  # for platform running
    from svcommon import sv_object
elif __name__ == 'sv_campaign_parser':  # for plugin console debugging
    import sv_object
elif __name__ == '__main__':  # for class console debugging
    sys.path.append('../svcommon')
    import sv_object


class SvCampaignParser(sv_object.ISvObject):
    """ campaign parser class for singleview only 
        naver=NV, google=GG, youtube=YT, facebook=FB, instagram=IN, daumkakao=DAUM
        TG=targetinggates
        how to categorize owned NS & owned SNS
    """
    __g_sAbsRootPath = config('ABSOLUTE_PATH_BOT')

    __g_oRegEx = re.compile(r'^[A-Za-z0-9-]+$')  # pattern ex) GA3AGE-H

    # __g_oLogger = None
    # caution! sv campaign code does not allow NS but allow PNS only, as pure NS could not be designated
    __g_lstSourceInfo = [  # (id, title, tag_name)
        (0, 'unknown', 'UNKNOWN'),
        (1, 'naver', 'NVR'),  # NVR will be deprecated in near future
        (1, 'naver', 'NV'),  # NV has priority than NVR
        (2, 'google', 'GG'),
        (3, 'youtube', 'YT'),
        (11, 'facebook', 'FBIG'),  # facebook PNS is mainly for instagram but API depends on facebook
        (5, 'instagram', 'IG'),
        (4, 'facebook', 'FB'),  # FB has priority than IG and FBIG
        (6, 'kakao', 'KKO'),
        (7, 'daum', 'DAUM'),
        (8, 'targeting', 'TG'),
        (9, 'mobon', 'MBO'),
        (10, 'smr', 'SMR'),
        (10, 'signal_play', 'SGP'),
        (15, 'twitter', 'TWT')
    ]
    __g_dictSourceIdTitle = None  # {1:'naver'}
    __g_dictSourceTagTitle = None  # {'NV': 'naver'}
    __g_dictSourceIdTag = None  # {1: 'NV'}

    __g_lstSearchRstInfo = [  # (id, title, tag_name)
        (0, 'unknown', 'UNKNOWN'),
        (1, 'Paid Search', 'PS'),
        (2, 'Paid Natural Search', 'PNS'),
        (3, 'Natural Search', 'NS'),
        (4, 'SNS', 'SNS')
    ]
    __g_dictSearchRstIdTitle = None  # {1:'Paid Search', 2:'Paid Natural Search', 3:'Natural Search', 4:'SNS',} 
    __g_dictSearchResultTypeTagTitle = None  # {'PS':'Paid Search'}
    __g_dictSearchRstIdTag = None  # {1: 'PS'}
    # __g_dictSearchResultTypeTagTitle = {'PS':'PS', 'paid_search':'PS', 'PNS':'PNS', 'paid_natural_search':'PNS','NS':'NS', 'natural_search':'NS', 'SNS':'SNS', 'sns':'SNS'}

    __g_lstMediumInfo = [  # (id, title, tag_name)
        (0, 'unknown', 'UNKNOWN'),
        (1, 'cpc', 'CPC'),
        (2, 'display', 'DISP'),
        (3, 'cpi', 'CPI'),
        (4, 'organic', 'PAGE'),
        (4, 'organic', 'REF')
    ]
    __g_dictMediumIdTitle = None  # {1:'Cost Per Click', 2:'Display', 3:'Cost Per Impression', 4:'Referral'} 
    __g_dictMediumTagTitle = None  # {'CPC':'cpc', 'DISP':'display', 'CPI':'cpi', 'REF':'organic', 'PAGE':'organic'}
    __g_dictMediumIdTag = None  # {1: 'CPC'}
    __g_dictGaMedium = {
        '(none)': '(none)',
        '(not set)': '(none)',
        'referral': 'referral',
        'organic': 'organic',
        'owned': 'organic',  # could be "blog / owned", but weird, hence enforce to categorize in blog / organic
        'cpc': 'cpc',
        'display': 'display',
        'social': 'social',
        'sns': 'social',
        'group': 'social',  # could be "facebook / group", hence enforce to categorize in facebook / social
        'email': 'email',
        'zalo': 'zalo'  # vietnamese messenger app
    }

    __g_dictSourceMediumType = {
        1: {'title': 'GADS_CPC', 'media_rst_type': 'PS', 'media_source': 'google',
            'media_media': 'cpc', 'desc': 'GDN, 구글 키워드 광고', 'camp_prefix': 'GG_PS_CPC_'},
        11: {'title': 'GADS_DISP', 'media_rst_type': 'PS', 'media_source': 'google',
             'media_media': 'display', 'desc': 'GDN, 구글 키워드 광고', 'camp_prefix': 'GG_PS_DISP_'},
        2: {'title': 'YT_DISP', 'media_rst_type': 'PS', 'media_source': 'youtube',
            'media_media': 'display', 'desc': '유튜브 동영상 광고', 'camp_prefix': 'YT_PS_DISP_'},
        21: {'title': 'YT_CPC', 'media_rst_type': 'PS', 'media_source': 'youtube',
             'media_media': 'cpc', 'desc': '유튜브 동영상 광고', 'camp_prefix': 'YT_PS_CPC_'},
        3: {'title': 'FB_CPC', 'media_rst_type': 'PS', 'media_source': 'facebook',
            'media_media': 'cpc', 'desc': '페이스북 광고', 'camp_prefix': 'FB_PS_CPC_'},
        31: {'title': 'FB_DISP', 'media_rst_type': 'PS', 'media_source': 'facebook',
             'media_media': 'display', 'desc': '페이스북 광고', 'camp_prefix': 'FB_PS_DISP_'},
        32: {'title': 'FBIG_CPC', 'media_rst_type': 'PS', 'media_source': 'facebook',
             'media_media': 'cpc', 'desc': '페이스북 광고', 'camp_prefix': 'FBIG_PS_CPC_'},
        33: {'title': 'FBIG_DISP', 'media_rst_type': 'PS', 'media_source': 'facebook',
             'media_media': 'display', 'desc': '페이스북 광고', 'camp_prefix': 'FBIG_PS_DISP_'},
        34: {'title': 'IG_CPC', 'media_rst_type': 'PS', 'media_source': 'instagram',
             'media_media': 'cpc', 'desc': '페이스북 광고', 'camp_prefix': 'IG_PS_CPC_'},
        35: {'title': 'IG_DISP', 'media_rst_type': 'PS', 'media_source': 'facebook',
             'media_media': 'display', 'desc': '페이스북 광고', 'camp_prefix': 'IG_PS_DISP_'},
        36: {'title': 'FB_PNS', 'media_rst_type': 'PS', 'media_source': 'facebook',
             'media_media': 'organic', 'desc': '페이스북 유료 자연검색 광고', 'camp_prefix': 'FB_PNS_REF_'},
        4: {'title': 'NVR_CPC', 'media_rst_type': 'PS', 'media_source': 'naver',
            'media_media': 'cpc', 'desc': '네이버 키워드 광고', 'camp_prefix': 'NV_PS_CPC_'},
        5: {'title': 'NVR_SEO', 'media_rst_type': 'PNS', 'media_source': 'naver',
            'media_media': 'organic', 'desc': '네이버 블로그 바이럴', 'camp_prefix': 'NV_PNS_REF_'},
        6: {'title': 'NVR_BRS', 'media_rst_type': 'PS', 'media_source': 'naver',
            'media_media': 'display', 'desc': '네이버 브랜드 검색 페이지', 'camp_prefix': 'NV_PS_DISP_'},
        7: {'title': 'KKO_CPC', 'media_rst_type': 'PS', 'media_source': 'kakao',
            'media_media': 'cpc', 'desc': '카카오 모먼트', 'camp_prefix': 'KKO_PS_CPC_'},
        71: {'title': 'KKO_DISP', 'media_rst_type': 'PS', 'media_source': 'kakao',
             'media_media': 'display', 'desc': '카카오 모먼트', 'camp_prefix': 'KKO_PS_DISP_'},
        72: {'title': 'DAUM_CPC', 'media_rst_type': 'PS', 'media_source': 'daum',
             'media_media': 'cpc', 'desc': '카카오 모먼트', 'camp_prefix': 'DAUM_PS_CPC_'},
        73: {'title': 'TG_CPC', 'media_rst_type': 'PS', 'media_source': 'targetinggates',
             'media_media': 'cpc', 'desc': '타게팅게이츠', 'camp_prefix': 'TG_PS_CPC_'},
        74: {'title': 'MOBON_CPC', 'media_rst_type': 'PS', 'media_source': 'mobon',
             'media_media': 'cpc', 'desc': '모비온', 'camp_prefix': 'MBO_PS_CPC_'},
        75: {'title': 'SMR_DISP', 'media_rst_type': 'PS', 'media_source': 'smr',
             'media_media': 'display', 'desc': '포탈에 개시되는 동영상 광고, 항상 DISP', 'camp_prefix': 'SMR_PS_DISP_'},
        76: {'title': 'SGP_DISP', 'media_rst_type': 'PS', 'media_source': 'signal_play',
             'media_media': 'display', 'desc': '시그널 플레이 항상 DISP', 'camp_prefix': 'SGP_PS_DISP_'}
        # 100: {'title': 'ETC', 'media_rst_type': None, 'media_source': None,
        #       'media_media': None, 'desc': '기타 비용', 'camp_prefix': None}
    }

    __g_lstPnsContractInfo = [  # (id, title, tag_name)
        (1, '파블', 'REF'),
        (2, '체험단', 'REF'),
        (3, '상위노출', 'REF'),
        (4, '인플루언서', 'REF'),
        (5, '카페활동', 'REF'),
        (6, '연관검색어', 'RELATED'),
        (7, '지식인활동', 'REF')
    ]
    __g_dictPnsContractType = None  # {1:'파블', 2:'체험단', 3:'상위노출', 4:'인플루언서', 5:'카페활동', 6:'연관검색어'}
    __g_dictPnsContractTypeNamed = None  # {'파블':'REF', '체험단':'REF', '상위노출':'REF', '인플루언서':'REF', '카페활동':'REF', '연관검색어':'RELATED', '지식인활동':'REF'}

    __g_lstLatestSvCampaignPrefix = [  # 'GG_PS_CPC_', 'GG_PS_DISP_'
        # (1, 'GOOGLE_ADS', 'GG_PS_CPC_'),  # 구글 애즈 GDN  1
        # (11, 'GG_DISP', 'GG_PS_DISP_'),  # 구글 애즈 GDN 1-1
        # (2, 'YT_DISP', 'YT_PS_DISP_'),  # 유튜브 광고  2
        # (21, 'YT_CPC', 'YT_PS_CPC_'),  # 유튜브 광고  2-1
        # (3, 'FB_CPC', 'FB_PS_CPC_'),  # 페이스북 광고  3
        # (31, 'FB_DISP', 'FB_PS_DISP_'),  # 페이스북 광고  3-1
        # (32, 'FBIG_CPC', 'FBIG_PS_CPC_'),  # 페이스북 광고  3-2
        # (33, 'FBIG_DISP', 'FBIG_PS_DISP_'),  # 페이스북 광고  3-3
        # (34, 'IG_CPC', 'IG_PS_CPC_'),  # instagram designated  3-4
        # (35, 'IG_DISP', 'IG_PS_DISP_'),  # instagram designated  3-5
        # (36, 'FB_PNS', 'FB_PNS_REF_'),
        # (4, 'NV_CPC', 'NV_PS_CPC_'),  # 네이버 CPC 광고  4
        # (5, 'NVR_SEO', 'NV_PNS_REF_'),  # 네이버 바이럴  5
        # (6, 'NVR_BRS', 'NV_PS_DISP_BRS_'),  # 네이버 브랜드 검색  6
        # (7, 'KKO_CPC', 'KKO_PS_CPC_'),  # 7
        # (71, 'KKO_DISP', 'KKO_PS_DISP_'),  # 7-1
        # (72, 'DAUM_CPC', 'DAUM_PS_CPC_'),  # 7-2
        # (73, '타게팅케이츠_CPC', 'TG_PS_CPC_'),  # 타게팅게이츠
        # (74, '모비온_CPC', 'MBO_PS_CPC_'),  # 모비온
        # (75, 'SMR_DISP', 'SMR_PS_DISP_')  # SMR; 포탈에 개시되는 SMR 광고; 항상 DISP
    ]
    # __g_lstObsoleteSvCampaignPrefix = ['NVR_BRAND_SEARCH_MOB','NV_PS_BRSEARCH_MOB','NVR_BRAND_SEARCH_PC','NV_PS_BRSEARCH_PC','NV_PS_','NV_NS_','NVR_NS_','FB_NS_','DAUM_PS_']
    __g_lstBrdedTag = ['_BRS_', '_BR_']
    __g_lstRmkTag = ['_RMK_', '_GDNRMK_']
    __g_lstGaUselessTerm = ['(not set)', '(not provided)', '(automatic matching)', 'undetermined',
                            '(remarketing/content targeting)', '(user vertical targeting)']
    __g_lstBrandedTrunc = None
    # adwords placement reserved title begin
    __g_lstAdwordsPlacement = ['World Localities', 'Travel', 'Sports', 'Science', 'Reference', 'Real Estate',
                               'Pets & Animals',
                               'People & Society', 'News', 'Law & Government', 'Home & Garden', 'Hobbies & Leisure',
                               'Health', 'Finance', 'Content',
                               'Computers & Electronics', 'Business & Industrial', 'boomuserlist', 'Books & Literature',
                               'Beauty & Fitness',
                               'Autos & Vehicles', 'AutomaticContent', 'Arts & Entertainment',
                               '18-24', '25-34', '35-44', '45-54', '55-64', '65 or more',
                               'Top 10%', '11-20%', '21-30%', '31-40%', '41-50%', 'Lower 50%', 'Undetermined']
    # adwords placement reserved title end

    __g_dictUaTag = {
        'mobile_app': 'M', 'mobile_web': 'M', 'desktop': 'P',  # for facebook registration
        'MOB': 'M', 'PC': 'P',  # for ga registration
        'UNSPECIFIED': 'M', 'UNKNOWN': 'M', 'MOBILE': 'M', 'TABLET': 'M', 'DESKTOP': 'P', 'CONNECTED_TV': 'P',
        'OTHER': 'M',  # for google ads api registration
        'Mobile devices with full browsers': 'M', 'Tablets with full browsers': 'M', 'Other': 'M',
        # for new adwords api registration
        'Computers': 'P', 'Devices streaming video content to TV screens': 'P',  # for old adwords api registration
        'PC': 'P', '기타': 'P', 'Android': 'M', 'iOS': 'M'  # for kakao ads registration
    }

    def __init__(self):
        self._g_oLogger = logging.getLogger('sv_campaign_parser')
        self._g_oLogger.setLevel(logging.ERROR)

        # log format
        formatter = logging.Formatter('%(name)s [%(levelname)s] %(message)s @ %(asctime)s')
        # log output format
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self._g_oLogger.addHandler(stream_handler)
        # log output to file stream
        file_handler = logging.FileHandler(self.__g_sAbsRootPath + '/log/sv_campaign_parser.' + str(datetime.today().strftime('%Y%m%d')) + '.log')
        file_handler.setFormatter(formatter)
        self._g_oLogger.addHandler(file_handler)
        
        self.__g_dictSourceIdTitle = {tup_single[0]: tup_single[1] for tup_single in self.__g_lstSourceInfo}
        self.__g_dictSourceTagTitle = {tup_single[2]: tup_single[1] for tup_single in self.__g_lstSourceInfo}
        self.__g_dictSourceIdTag = {tup_single[0]: tup_single[2] for tup_single in self.__g_lstSourceInfo}

        self.__g_dictSearchRstIdTitle = {tup_single[0]: tup_single[1] for tup_single in self.__g_lstSearchRstInfo}
        self.__g_dictSearchResultTypeTagTitle = {tup_single[2]: tup_single[1] for tup_single in
                                                 self.__g_lstSearchRstInfo}
        self.__g_dictSearchRstIdTag = {tup_single[0]: tup_single[2] for tup_single in self.__g_lstSearchRstInfo}

        self.__g_dictMediumIdTitle = {tup_single[0]: tup_single[1] for tup_single in self.__g_lstMediumInfo}
        self.__g_dictMediumTagTitle = {tup_single[2]: tup_single[1] for tup_single in self.__g_lstMediumInfo}
        self.__g_dictMediumIdTag = {tup_single[0]: tup_single[2] for tup_single in self.__g_lstMediumInfo}

        self.__g_dictPnsContractType = {tup_single[0]: tup_single[1] for tup_single in self.__g_lstPnsContractInfo}
        self.__g_dictPnsContractTypeNamed = {tup_single[1]: tup_single[2] for tup_single in self.__g_lstPnsContractInfo}

        self.__g_lstLatestSvCampaignPrefix = [dict_source_medium['camp_prefix'] for _, dict_source_medium in
                                              self.__g_dictSourceMediumType.items()]

    def close(self):
        pass

    def get_ua(self, s_ua):
        try:
            return self.__g_dictUaTag[s_ua]
        except KeyError:
            return 'err_ua'

    def get_source_tag(self, s_source_tag):
        if s_source_tag in self.__g_dictSourceTagTitle:
            return self.__g_dictSourceTagTitle[s_source_tag]
        else:
            return False

    def get_source_id_title_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictSourceIdTitle.items()}
        return self.__g_dictSourceIdTitle

    def get_source_tag_title_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictSourceTagTitle.items()}
        return self.__g_dictSourceTagTitle

    def get_source_id_tag_dict(self, b_inverted=False):  # unuse?
        if b_inverted:
            return {v: k for k, v in self.__g_dictSourceIdTag.items()}
        return self.__g_dictSourceIdTag

    def validate_search_rst_tag(self, s_search_rst_type):
        if s_search_rst_type in self.__g_dictSearchResultTypeTagTitle:
            return s_search_rst_type
        else:
            return False

    def get_search_rst_type_id_title_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictSearchRstIdTitle.items()}
        return self.__g_dictSearchRstIdTitle

    def get_search_rst_type_id_tag_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictSearchRstIdTag.items()}
        return self.__g_dictSearchRstIdTag

    def get_sv_medium_tag(self, s_sv_medium_code):
        if s_sv_medium_code in self.__g_dictMediumTagTitle:
            return self.__g_dictMediumTagTitle[s_sv_medium_code]
        else:
            return False

    def get_medium_type_id_title_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictMediumIdTitle.items()}
        return self.__g_dictMediumIdTitle

    def get_medium_type_id_tag_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictMediumIdTag.items()}
        return self.__g_dictMediumIdTag

    def get_medium_type_tag_title_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictMediumTagTitle.items()}
        return self.__g_dictMediumTagTitle

    def get_pns_contract_type_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictPnsContractType.items()}
        return self.__g_dictPnsContractType

    def get_source_medium_type_dict(self):
        return self.__g_dictSourceMediumType

    def validate_sv_campaign_level_tag(self, s_sv_campaign_level_tag):
        """ 
        validate sv campaign level 1 2 3 4
        :param 
        """
        dict_rst = {'b_error': False, 's_msg': None, 'dict_ret': None}
        s_sv_campaign_level_tag = s_sv_campaign_level_tag.strip()
        if len(s_sv_campaign_level_tag) == 0:
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'empty string'
            return dict_rst
        
        # if not s_sv_campaign_level_tag.isalnum():
        m = self.__g_oRegEx.search(s_sv_campaign_level_tag)  # match() vs search()
        if not m:  # if invalid campaign_level_tag string
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'alphanumeric allowed only'
            return dict_rst
        dict_rst['dict_ret'] = s_sv_campaign_level_tag.upper()
        return dict_rst

    def validate_ga_medium_tag(self, s_ga_medium_tag):
        s_ga_medium_tag = s_ga_medium_tag.lower()
        dictRst = {'medium': 'weird', 'found_pos': -1}
        if s_ga_medium_tag in self.__g_dictGaMedium:  # remedy erronous UTM parameter
            dictRst['medium'] = self.__g_dictGaMedium[s_ga_medium_tag]
        else:
            for sGaOfficialMediaCode in self.__g_dictGaMedium:
                nPos = s_ga_medium_tag.find(sGaOfficialMediaCode)
                if nPos > -1:
                    dictRst['medium'] = self.__g_dictGaMedium[sGaOfficialMediaCode]
                    dictRst['found_pos'] = nPos
        return dictRst

    def get_gad_placement_tag_by_term(self, sTerm):
        # aw means deprecated name of google ads
        for sAdwPlacement in self.__g_lstAdwordsPlacement:
            if sTerm.find(sAdwPlacement) > -1:
                return True
        return False

    def decide_brded_by_term(self, s_brded_terms_path, s_term):
        dict_rst = {'b_brded': False, 'b_error': False, 's_err_msg': None}
        if s_term is None:
            return dict_rst
        if self.__g_lstBrandedTrunc is None:
            self.__g_lstBrandedTrunc = self.get_branded_trunc(s_brded_terms_path)
            if len(self.__g_lstBrandedTrunc) == 0:
                dict_rst['b_error'] = True
                dict_rst['s_err_msg'] = '브랜디드 키워드 목록을 등록하면 온라인 브랜딩을 측정할 수 있습니다!'
                return dict_rst
        if s_term not in self.__g_lstGaUselessTerm:
            for sBrandedTrunc in self.__g_lstBrandedTrunc:
                if s_term.find(sBrandedTrunc) > -1:
                    dict_rst['b_brded'] = True
                    break
        return dict_rst

    def get_sv_pns_contract_type_named_tag(self, s_sv_service_type):
        if s_sv_service_type in self.__g_dictPnsContractTypeNamed:
            return self.__g_dictPnsContractTypeNamed[s_sv_service_type]
        else:
            raise Exception('stop')

    def parse_campaign_code_fb(self, dict_camp_info):
        dict_rst = {'source': 'unknown', 'source_code': '', 'rst_type': '',
                    'medium': '', 'medium_code': '', 'brd': 0,
                    'campaign1st': '00', 'campaign2nd': '00', 'campaign3rd': '00',
                    'detected': False}
        if dict_camp_info['url_tags'] == 'n/a':  # facebook inlink ad or outlink ad without UTM params
            sAdName = dict_camp_info['ad_name']
            dict_rst['rst_type'] = self.validate_search_rst_tag('SNS')
            if sAdName.find('게시물: ') > -1:
                sNonSvCampaignCode = sAdName.replace('게시물: ', '').replace('"', '').strip()
                dict_rst['source'] = self.get_source_tag('FB')
                dict_rst['source_code'] = 'FB'
                dict_rst['brd'] = 1
                dict_rst['medium'] = self.get_sv_medium_tag('CPI')
                dict_rst['medium_code'] = 'CPI'
                dict_rst['campaign1st'] = sNonSvCampaignCode
                dict_rst['detected'] = True
            elif sAdName.find('INSTAGRAM POST: ') > -1:
                dict_rst['source'] = self.get_source_tag('IG')
                dict_rst['source_code'] = 'IG'
                sNonSvCampaignCode = sAdName.replace('Instagram Post: ', '').replace('"', '').strip()
                dict_rst['brd'] = 1
                dict_rst['medium'] = self.get_sv_medium_tag('CPI')
                dict_rst['medium_code'] = 'CPI'
                dict_rst['campaign1st'] = sNonSvCampaignCode
                dict_rst['detected'] = True
            else:  # this case sometimes means facebook 3rd-party outlink ad
                pass
                # try: # this case sometimes means facebook 3rd-party outlink ad
                #     sCampaignName = sAdName
                #     dictCampaignNameAlias[sCampaignName]
                #     dict_rst['source'] =  self.get_source_tag('FB')  #self.__g_dictSourceTagTitle['FB']
                #     dict_rst['brd'] = 0
                #     dict_rst['rst_type'] = dictCampaignNameAlias[sCampaignName]['rst_type']
                #     dict_rst['medium'] = dictCampaignNameAlias[sCampaignName ]['medium'].lower()
                #     dict_rst['campaign1st'] = dictCampaignNameAlias[sCampaignName]['camp1st']
                #     dict_rst['campaign2nd'] = dictCampaignNameAlias[sCampaignName]['camp2nd']
                #     dict_rst['campaign3rd'] = dictCampaignNameAlias[sCampaignName]['camp3rd']
                #     dict_rst['detected'] = True
                # except KeyError: # if facebook inlink ad with unknown campaign name
                #     dict_rst['source'] = self.get_source_tag('FB')  #self.__g_dictSourceTagTitle['FB']
                #     dict_rst['brd'] = 1
                #     dict_rst['medium'] = self.get_sv_medium_tag('CPI')  # self.__g_dictMediumTagTitle[ 'CPI' ]
                #     dict_rst['campaign1st'] = sAdName
        else:  # facebook outlink ad
            dictTempRst = self.__analyze_sv_campaign_code(dict_camp_info['campaign_code'])
            lstCampaignCode = dictTempRst['sv_code']
            if lstCampaignCode[0] in ['FB', 'IG', 'FBIG'] and \
                    lstCampaignCode[2] in ['CPC', 'DISP', 'CPI']:
                dict_rst['source'] = self.get_source_tag(lstCampaignCode[0])
                dict_rst['source_code'] = lstCampaignCode[0]
                dict_rst['rst_type'] = self.validate_search_rst_tag(lstCampaignCode[1])
                dict_rst['brd'] = dictTempRst['brd']
                dict_rst['medium'] = self.get_sv_medium_tag(lstCampaignCode[2])
                dict_rst['medium_code'] = lstCampaignCode[2]
                dict_rst['campaign1st'] = lstCampaignCode[3]
                dict_rst['detected'] = True
                try:
                    dict_rst['campaign2nd'] = lstCampaignCode[4]
                    dict_rst['campaign3rd'] = lstCampaignCode[5]
                except IndexError:
                    pass
            elif lstCampaignCode[0] == '{{AD.NAME}}' or lstCampaignCode[0] == '{{ADSET.NAME}}' or lstCampaignCode[
                0] == '{{CAMPAIGN.NAME}}':
                dict_rst['rst_type'] = self.validate_search_rst_tag('PS')
                dict_rst['medium'] = self.get_sv_medium_tag('CPC')
                dict_rst['campaign1st'] = dict_camp_info['ad_name']
                dict_rst['detected'] = True
        return dict_rst

    def parse_campaign_code(self, s_sv_campaign_code=''):
        """ this method might be for ga_register???
            should be synchronized with sv_API_retriever/svcommon/sv_campaign_parser
        """
        dict_rst = {'source': 'unknown', 'source_code': '', 'rst_type': '',
                    'medium': '', 'medium_code': '', 'brd': 0,
                    'campaign1st': '00', 'campaign2nd': '00', 'campaign3rd': '00',
                    'detected': False}
        if s_sv_campaign_code == '(not set)':
            return dict_rst
        
        s_sv_campaign_code = s_sv_campaign_code.upper()
        b_latest_sv_campaign_found = False
        try:
            for s_campaign_prefix in self.__g_lstLatestSvCampaignPrefix:
                self._g_oLogger.info(s_campaign_prefix)
                if s_campaign_prefix is None:  # somtimes None added onto __g_lstLatestSvCampaignPrefix
                    contiue
                if s_sv_campaign_code.find(s_campaign_prefix) > -1:
                    b_latest_sv_campaign_found = True
                    break
        except Exception as err:
            self._g_oLogger.error('2-parse_campaign_code err?')
            self._g_oLogger.error(err)
            self._g_oLogger.error(self.__g_lstLatestSvCampaignPrefix)
            self._g_oLogger.error(s_sv_campaign_code)
            self._g_oLogger.error(s_campaign_prefix)

        if b_latest_sv_campaign_found:  # latest version of SV campaign code
            dict_tmp_rst = self.__analyze_sv_campaign_code(s_sv_campaign_code)
            dict_rst['brd'] = dict_tmp_rst['brd']
            list_campaign_code = dict_tmp_rst['sv_code']
            del dict_tmp_rst

            if list_campaign_code[0] == 'OLD':  # first of all, process OLD prefix
                del list_campaign_code[0]
                n_elem_cnt = len(list_campaign_code)
                list_campaign_code[n_elem_cnt - 1] = list_campaign_code[n_elem_cnt - 1] + '_OLD'
            # set source tag
            dict_rst['source'] = self.get_source_tag(list_campaign_code[0])
            dict_rst['source_code'] = list_campaign_code[0]
            # set search result type tag
            dict_rst['rst_type'] = self.validate_search_rst_tag(list_campaign_code[1])
            # set media tag
            dict_rst['medium'] = self.get_sv_medium_tag(list_campaign_code[2])
            dict_rst['medium_code'] = list_campaign_code[2]
            if dict_rst['source'] != 'unknown':  # handle no sv campaign code data
                dict_rst['campaign1st'] = list_campaign_code[3]
                try:
                    dict_rst['campaign2nd'] = list_campaign_code[4]
                    dict_rst['campaign3rd'] = list_campaign_code[5]
                except IndexError:
                    pass
                n_campaign_elem = len(list_campaign_code)
                if n_campaign_elem > 6:  # to handle more than 6 chunks properly
                    for n_idx in range(6, n_campaign_elem):
                        dict_rst['campaign3rd'] += '-' + list_campaign_code[n_idx]
                dict_rst['detected'] = True
        
        # else:  # process old style SV campaign code; this section will be deprecated after balanceseat complete analysis
        #     b_obsolete_sv_campaign_found = False
        #     for sCampaignPrefix in self.__g_lstObsoleteSvCampaignPrefix:
        #         if s_sv_campaign_code.find(sCampaignPrefix) > -1:
        #             b_obsolete_sv_campaign_found = True
        #             break

        #     if b_obsolete_sv_campaign_found:
        #         if s_sv_campaign_code == 'NVR_BRAND_SEARCH_MOB' or s_sv_campaign_code == 'NV_PS_BRSEARCH_MOB':
        #             dict_rst['source'] = 'naver'
        #             dict_rst['rst_type'] = self.validate_search_rst_tag('PS')  # self.__g_dictSearchResultTypeTagTitle['PS']
        #             dict_rst['medium'] = 'display'
        #             dict_rst['campaign1st'] = 'BRS'
        #             dict_rst['campaign2nd'] = 'MOB'
        #         elif s_sv_campaign_code == 'NVR_BRAND_SEARCH_PC' or s_sv_campaign_code == 'NV_PS_BRSEARCH_PC':
        #             dict_rst['source'] = 'naver'
        #             dict_rst['rst_type'] = self.validate_search_rst_tag('PS')  # self.__g_dictSearchResultTypeTagTitle['PS']
        #             dict_rst['medium'] = 'display'
        #             dict_rst['campaign1st'] = 'BRS'
        #             dict_rst['campaign2nd'] = 'PC'
        #         else:
        #             lst_campaign_code = s_sv_campaign_code.split('_')
        #             # set source tag
        #             dict_rst['source'] = self.get_source_tag(list_campaign_code[0])
        #             # set media tag
        #             if lst_campaign_code[1] == 'PS':
        #                 dict_rst['rst_type'] = self.validate_search_rst_tag('PS')  # self.__g_dictSearchResultTypeTagTitle['PS']
        #                 dict_rst['medium'] = 'cpc'
        #                 dict_rst['medium_code'] = 'CPC'
        #             elif lst_campaign_code[1] == 'NS':
        #                 dict_rst['rst_type'] = self.validate_search_rst_tag('PS')  # self.__g_dictSearchResultTypeTagTitle['PNS']
        #                 dict_rst['medium'] = 'organic'
        #                 dict_rst['medium_code'] = 'REF'
        #                 if lst_campaign_code[2] == 'BLOG':  # or lst_campaign_code[2] == 'BL':
        #                     lst_campaign_code[2] = 'BL'
        #                 elif lst_campaign_code[2] == 'EXAM':  # this condition deals with balanceseat yr 2015, 2016 only
        #                     lst_campaign_code[2] = 'BL'
        #                     lst_campaign_code[3] = 'EXAM'
        #                 elif lst_campaign_code[2] == 'POWER':  # this condition deals with balanceseat yr 2015, 2016 only
        #                     lst_campaign_code[2] = 'BL'
        #                     lst_campaign_code[3] = 'POWERBLOGGER'
        #                 elif lst_campaign_code[2] == 'CAFE':
        #                     lst_campaign_code[2] = 'CF'
        #                 elif lst_campaign_code[2] == 'KIN' or lst_campaign_code[2] == 'PAGE':
        #                     pass

        #             n_campaign_code_elem = len(lst_campaign_code)
        #             if n_campaign_code_elem == 4:  # ex) NVR_NS_KIN_20150724 > very seldom
        #                 dict_rst['campaign1st'] = lst_campaign_code[2]
        #                 dict_rst['campaign2nd'] = lst_campaign_code[3]
        #             elif n_campaign_code_elem == 5:  # ex) NV_PS_WELCOMGUEST_00_00
        #                 dict_rst['campaign1st'] = lst_campaign_code[2]
        #                 dict_rst['campaign2nd'] = lst_campaign_code[3]
        #                 dict_rst['campaign3rd'] = lst_campaign_code[4]
        #             elif n_campaign_code_elem == 6:  # ex) NV_PS_WELCOM_GUEST_MAIN_01 > very seldom
        #                 dict_rst['campaign1st'] = lst_campaign_code[2]
        #                 dict_rst['campaign2nd'] = lst_campaign_code[3]
        #                 dict_rst['campaign3rd'] = lst_campaign_code[4] + '_' + lst_campaign_code[5]
        #         dict_rst['detected'] = True
        #     else:
        #         dict_rst['campaign1st'] = s_sv_campaign_code.strip()

        #     if dict_rst['campaign1st'].find('BR') > -1:
        #         dict_rst['brd'] = 1
        return dict_rst

    def get_branded_trunc(self, s_brded_terms_path):
        """ 
        called by self.decideBrandedByTerm()
        call from svload.pandas_plugins.brded_term:get_list() 
        """
        if self.__g_lstBrandedTrunc != None:  # sentinel to prevent duplicated process
            return self.__g_lstBrandedTrunc
        lst_branded_trunc = []
        if s_brded_terms_path.find('branded_term.conf') > -1:
            try:
                with open(s_brded_terms_path, 'r', encoding='utf8') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    for lst_term in reader:
                        if len(lst_term):
                            lst_branded_trunc.append(lst_term[0])
            except FileNotFoundError:
                pass
        return self.__get_unique_sorted_trimmed_list(lst_branded_trunc)

    def set_branded_trunc(self, s_brded_terms_path, lst_line):
        """ call from svload.pandas_plugins.brded_term:update_list() """
        dict_rst = {'updated': False}
        if s_brded_terms_path.find('branded_term.conf') == -1:
            return dict_rst

        lst_old_branded_term = self.get_branded_trunc(s_brded_terms_path)
        lst_line = self.__get_unique_sorted_trimmed_list(lst_line)
        if lst_old_branded_term != lst_line:
            try:
                with open(s_brded_terms_path, 'w', encoding='utf8') as fp:
                    for s_term in lst_line:
                        if len(s_term):
                            fp.write("%s\n" % s_term.strip())
                dict_rst['updated'] = True
            except FileNotFoundError:
                pass
        return dict_rst

    def __get_unique_sorted_trimmed_list(self, lst_source):
        lst_source = list(set(lst_source))  # get unique
        lst_source.sort()  # get sorted
        return [s_term.strip() for s_term in lst_source]  # get trimmed

    def __analyze_sv_campaign_code(self, s_sv_campaign_code):
        dict_rst = {'sv_code': '0', 'brd': '0'}
        if s_sv_campaign_code == '':
            return dict_rst

        s_sv_campaign_code = s_sv_campaign_code.upper()
        dict_rst['sv_code'] = s_sv_campaign_code.split('_')
        for s_brded_tag in self.__g_lstBrdedTag:
            if s_sv_campaign_code.find(s_brded_tag) > -1:
                dict_rst['brd'] = '1'
                break

        if dict_rst['brd'] == '0':
            for s_rmk_tag in self.__g_lstRmkTag:
                if s_sv_campaign_code.find(s_rmk_tag) > -1:
                    dict_rst['brd'] = '1'
                    break
        return dict_rst

# if __name__ == '__main__': # for console debugging
#	oSvCampaignParser = SvCampaignParser()
#	oSvCampaignParser.send_msg('ddd')
