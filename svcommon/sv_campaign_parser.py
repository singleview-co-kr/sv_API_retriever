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

# 3rd party library

# singleview config
if __name__ == 'svcommon.sv_campaign_parser': # for platform running
    from svcommon import sv_object
elif __name__ == 'sv_campaign_parser': # for plugin console debugging
    import sv_object
elif __name__ == '__main__': # for class console debugging
    sys.path.append('../svcommon')
    import sv_object

class SvCampaignParser(sv_object.ISvObject):
    """ campaign parser class for singleview only 
        naver=NV, google=GG, youtube=YT, facebook=FB, instagram=IN, daumkakao=DAUM
        TG=targetinggates
        how to categorize owned NS & owned SNS
    """
    # __g_oLogger = None
    # caution! sv campaign code does not allow NS but allow PNS only, as pure NS could not be designated
    __g_lstSourceInfo = [ # (id, title, tag_name)
        (0, 'unknown', 'UNKNOWN'),
        (1, 'naver', 'NVR'),
        (1, 'naver', 'NV'),
        (2, 'google', 'GG'),
        (3, 'youtube', 'YT'),
        (4, 'facebook', 'FB'),  # facebook PNS is mainly for instagram but API depends on facebook
        (5, 'instagram', 'IG'),
        (6, 'kakao', 'KKO'),
        (7, 'daum', 'DAUM'),
        (8, 'targeting', 'TG'),
        (9, 'mobon', 'MBO'),
        (10, 'smr', 'SMR')
    ]
    __g_dictSourceIdTitle = None  # {1:'naver'}
    __g_dictSourceTagTitle = None  # {'NV': 'naver'}
    __g_dictSourceIdTag = None  # {1: 'NV'}
    
    __g_lstSearchRstInfo = [ # (id, title, tag_name)
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

    __g_lstMediumInfo = [ # (id, title, tag_name)
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
        '(none)':'(none)',
        '(not set)':'(none)',
        'referral':'referral',
        'organic':'organic',
        'owned':'organic', # could be "blog / owned", but weird, hence enforce to categorize in blog / organic 
        'cpc':'cpc',
        'display':'display',
        'social':'social',
        'sns':'social',
        'group':'social', # could be "facebook / group", hence enforce to categorize in facebook / social 
        'email':'email',
        'zalo':'zalo' # vietnamese messenger app
        }

    __g_lstPnsContractInfo = [ # (id, title, tag_name)
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
    # __g_dictPnsServiceType = {'파블':'BL', '체험단':'BL', '상위노출':'BL', '인플루언서':'IGIF', '카페활동':'CF', '연관검색어':'RELATED', '기자단':'BL','지식인활동':'KIN'}
    
    __g_lstLatestSvCampaignPrefix = [
        'NV_PS_CPC_',
        'NV_PS_DISP_BRS_',
        'NV_PNS_REF_',
        'GG_PS_DISP_',
        'GG_PS_CPC_',
        'YT_PS_DISP_',
        'YT_PS_CPC_',
        'DAUM_PS_CPC_',
        'KKO_PS_CPC_',
        'FB_PS_CPC_',
        'FBIG_PS_CPC_',
        'IG_PS_CPC_',  # instagram designated
        'FB_PNS_REF',
        'TG_PS_CPC_',  # 타게팅게이츠
        'MBO_PS_CPC_',  # 모비온
        'SMR_PS_DISP_'  # SMR; 포탈에 개시되는 SMR 광고; 항상 DISP
        ]
    __g_lstObsoleteSvCampaignPrefix = ['NVR_BRAND_SEARCH_MOB','NV_PS_BRSEARCH_MOB','NVR_BRAND_SEARCH_PC','NV_PS_BRSEARCH_PC','NV_PS_','NV_NS_','NVR_NS_','FB_NS_','DAUM_PS_']
    __g_lstBrdedTag = ['_BRS_','_BR_']
    __g_lstRmkTag = ['_RMK_','_GDNRMK_']
    __g_lstGaUselessTerm = ['(not set)','(not provided)','(automatic matching)', 'undetermined', '(remarketing/content targeting)', '(user vertical targeting)']
    __g_lstBrandedTrunc = None
    # adwords placement reserved title begin
    __g_lstAdwordsPlacement = [ 'World Localities', 'Travel', 'Sports', 'Science', 'Reference', 'Real Estate', 'Pets & Animals',
            'People & Society', 'News', 'Law & Government', 'Home & Garden', 'Hobbies & Leisure', 'Health', 'Finance', 'Content', 
            'Computers & Electronics', 'Business & Industrial', 'boomuserlist', 'Books & Literature', 'Beauty & Fitness',
            'Autos & Vehicles', 'AutomaticContent', 'Arts & Entertainment', 
            '18-24', '25-34', '35-44', '45-54', '55-64', '65 or more',
            'Top 10%', '11-20%', '21-30%', '31-40%', '41-50%', 'Lower 50%', 'Undetermined' ]
    # adwords placement reserved title end
    
    __g_dictUaTag = {
        'mobile_app':'M', 'mobile_web':'M', 'desktop':'P', # for facebook registration
        'MOB': 'M', 'PC':'P', # for ga registration
        'UNSPECIFIED':'M', 'UNKNOWN':'M', 'MOBILE':'M', 'TABLET':'M', 'DESKTOP':'P', 'CONNECTED_TV':'P', 'OTHER': 'M', # for google ads api registration
        'Mobile devices with full browsers':'M', 'Tablets with full browsers': 'M', 'Other':'M', # for new adwords api registration
        'Computers':'P', 'Devices streaming video content to TV screens':'P', # for old adwords api registration
        'PC':'P', '기타':'P','Android':'M', 'iOS':'M' # for kakao ads registration
        }

    def __init__(self):
        self._g_oLogger = logging.getLogger(__file__)

        self.__g_dictSourceIdTitle = {tup_single[0]: tup_single[1] for tup_single in self.__g_lstSourceInfo}
        self.__g_dictSourceTagTitle = {tup_single[2]: tup_single[1] for tup_single in self.__g_lstSourceInfo}
        self.__g_dictSourceIdTag = {tup_single[0]: tup_single[2] for tup_single in self.__g_lstSourceInfo}

        self.__g_dictSearchRstIdTitle = {tup_single[0]: tup_single[1] for tup_single in self.__g_lstSearchRstInfo}
        self.__g_dictSearchResultTypeTagTitle = {tup_single[2]: tup_single[1] for tup_single in self.__g_lstSearchRstInfo}
        self.__g_dictSearchRstIdTag = {tup_single[0]: tup_single[2] for tup_single in self.__g_lstSearchRstInfo}
        
        self.__g_dictMediumIdTitle = {tup_single[0]: tup_single[1] for tup_single in self.__g_lstMediumInfo}
        self.__g_dictMediumTagTitle = {tup_single[2]: tup_single[1] for tup_single in self.__g_lstMediumInfo}
        self.__g_dictMediumIdTag = {tup_single[0]: tup_single[2] for tup_single in self.__g_lstMediumInfo}
        
        self.__g_dictPnsContractType = {tup_single[0]: tup_single[1] for tup_single in self.__g_lstPnsContractInfo}
        self.__g_dictPnsContractTypeNamed = {tup_single[1]: tup_single[2] for tup_single in self.__g_lstPnsContractInfo}

    def close(self):
        pass

    def get_ua(self, sUa):
        try:
            return self.__g_dictUaTag[sUa]
        except KeyError:
            return 'err_ua'

    def get_source_id_title_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictSourceIdTitle.items()}
        return self.__g_dictSourceIdTitle

    def get_source_tag_title_dict(self, b_inverted=False):  # unuse?
        if b_inverted:
            return {v: k for k, v in self.__g_dictSourceTagTitle.items()}
        return self.__g_dictSourceTagTitle

    def get_source_id_tag_dict(self, b_inverted=False):  # unuse?
        if b_inverted:
            return {v: k for k, v in self.__g_dictSourceIdTag.items()}
        return self.__g_dictSourceIdTag

    def get_search_rst_type_id_title_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictSearchRstIdTitle.items()}
        return self.__g_dictSearchRstIdTitle

    def get_search_rst_type_id_tag_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictSearchRstIdTag.items()}
        return self.__g_dictSearchRstIdTag
    
    def get_medium_type_id_title_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictMediumIdTitle.items()}
        return self.__g_dictMediumIdTitle
    
    def get_medium_type_id_tag_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictMediumIdTag.items()}
        return self.__g_dictMediumIdTag
    
    def get_pns_contract_type_dict(self, b_inverted=False):
        if b_inverted:
            return {v: k for k, v in self.__g_dictPnsContractType.items()}
        return self.__g_dictPnsContractType

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
        if not s_sv_campaign_level_tag.isalnum():
            print(s_sv_campaign_level_tag)
            dict_rst['b_error'] = True
            dict_rst['s_msg'] = 'alphanumeric allowed only'
            return dict_rst
        dict_rst['dict_ret'] = s_sv_campaign_level_tag.upper()
        return dict_rst

    def validate_ga_medium_tag(self, s_ga_medium_tag):
        s_ga_medium_tag = s_ga_medium_tag.lower()
        dictRst = {'medium':'weird', 'found_pos':-1}
        if s_ga_medium_tag in self.__g_dictGaMedium: # remedy erronous UTM parameter 
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
        # lst_type = list(self.__g_dictPnsContractTypeNamed.keys())
        if s_sv_service_type in self.__g_dictPnsContractTypeNamed:
            return self.__g_dictPnsContractTypeNamed[s_sv_service_type]
        else:
            raise Exception('stop')
            # return 'err_service_type'

    def get_source_tag(self, s_source_tag):
        if s_source_tag in self.__g_dictSourceTagTitle:
            return self.__g_dictSourceTagTitle[s_source_tag]
        else:
            # raise Exception('stop')
            return False  # 'err_source_tag'

    def validate_search_rst_tag(self, s_search_rst_type):
        if s_search_rst_type in self.__g_dictSearchResultTypeTagTitle:
            return s_search_rst_type
        else:
            # raise Exception('stop')
            return False  # 'err_search_rst_tag'

    def get_sv_medium_tag(self, s_sv_medium_code):
        # lst_tag = list(self.__g_dictMediumTagTitle.keys())
        if s_sv_medium_code in self.__g_dictMediumTagTitle:
            return self.__g_dictMediumTagTitle[s_sv_medium_code]
        else:
            # raise Exception('stop')
            return False  # 'err_medium_tag'
    
    def parse_campaign_code_fb(self, dictCampaignInfo, dictCampaignNameAlias):
        dictRst = {'source':'unknown','rst_type':'','medium':'','brd':'0','campaign1st':'0','campaign2nd':'0','campaign3rd':'0','detected':False}
        if dictCampaignInfo['url_tags'] == 'n/a': # facebook inlink ad or outlink ad without UTM params
            sAdName = dictCampaignInfo['ad_name']
            #self._printDebug('weird Fb business log!')
            dictRst['rst_type'] = self.validate_search_rst_tag('SNS')  # self.__g_dictSearchResultTypeTagTitle['SNS']
            if sAdName.find('게시물: ') > -1:
                sNonSvCampaignCode = sAdName.replace('게시물: ', '').replace('"','').strip()
                dictRst['source'] = self.get_source_tag('FB')  #self.__g_dictSourceTagTitle['FB']
                dictRst['brd'] = '1'
                dictRst['medium'] = self.get_sv_medium_tag('CPI')  # self.__g_dictMediumTagTitle['CPI']
                dictRst['campaign1st'] = sNonSvCampaignCode
                dictRst['detected'] = True
            elif sAdName.find('INSTAGRAM POST: ') > -1:
                dictRst['source'] = self.get_source_tag('IG')  #self.__g_dictSourceTagTitle['IG']
                sNonSvCampaignCode = sAdName.replace('Instagram Post: ', '').replace('"','').strip()
                dictRst['brd'] = '1'
                dictRst['medium'] = self.get_sv_medium_tag('CPI')  # self.__g_dictMediumTagTitle['CPI']
                dictRst['campaign1st'] = sNonSvCampaignCode
                dictRst['detected'] = True
            else:
                try: # this case sometimes means facebook 3rd-party outlink ad
                    sCampaignName = sAdName
                    dictCampaignNameAlias[sCampaignName]
                    dictRst['source'] =  self.get_source_tag('FB')  #self.__g_dictSourceTagTitle['FB']
                    dictRst['brd'] = '0'
                    dictRst['rst_type'] = dictCampaignNameAlias[sCampaignName]['rst_type']
                    dictRst['medium'] = dictCampaignNameAlias[sCampaignName ]['medium'].lower()
                    dictRst['campaign1st'] = dictCampaignNameAlias[sCampaignName]['camp1st']
                    dictRst['campaign2nd'] = dictCampaignNameAlias[sCampaignName]['camp2nd']
                    dictRst['campaign3rd'] = dictCampaignNameAlias[sCampaignName]['camp3rd']
                    dictRst['detected'] = True
                except KeyError: # if facebook inlink ad with unknown campaign name
                    dictRst['source'] = self.get_source_tag('FB')  #self.__g_dictSourceTagTitle['FB']
                    dictRst['brd'] = '1'
                    dictRst['medium'] = self.get_sv_medium_tag('CPI')  # self.__g_dictMediumTagTitle[ 'CPI' ]
                    dictRst['campaign1st'] = sAdName
        else: # facebook outlink ad
            dictTempRst = self.__analyze_sv_campaign_code(dictCampaignInfo['campaign_code'])
            sSourceAbbreviation = dictTempRst['sv_code'][0]
            dictRst['source'] = self.get_source_tag(sSourceAbbreviation)  #self.__g_dictSourceTagTitle[sSourceAbbreviation]
            lstCampaignCode = dictTempRst['sv_code']
            if lstCampaignCode[0] in ['FB', 'IG', 'FBIG']:  # == 'FB' or lstCampaignCode[0] == 'IG' or lstCampaignCode[0] == 'FBIG':
                dictRst['rst_type'] = self.validate_search_rst_tag(lstCampaignCode[1])  # self.__g_dictSearchResultTypeTagTitle[lstCampaignCode[1]]
                dictRst['brd'] = dictTempRst['brd']
                dictRst['medium'] = self.get_sv_medium_tag(lstCampaignCode[2])  # self.__g_dictMediumTagTitle[lstCampaignCode[2]]
                dictRst['campaign1st'] = lstCampaignCode[3]
                dictRst['detected'] = True
                try:
                    dictRst['campaign2nd'] = lstCampaignCode[4]
                    dictRst['campaign3rd'] = lstCampaignCode[5]
                except IndexError:
                    pass
            else: #if lstCampaignCode[0] == '{{AD.NAME}}' or lstCampaignCode[0] == '{{ADSET.NAME}}' or lstCampaignCode[0] == '{{CAMPAIGN.NAME}}':
                dictRst['rst_type'] = self.validate_search_rst_tag('PS')  # self.__g_dictSearchResultTypeTagTitle['PS']
                dictRst['medium'] = self.get_sv_medium_tag('CPC')  # self.__g_dictMediumTagTitle['CPC']
                dictRst['campaign1st'] = dictCampaignInfo['ad_name']
                dictRst['detected'] = True
        return dictRst

    def parse_campaign_code(self, s_sv_campaign_code=''):
        """ this method might be for ga_register???
            should be synchronized with sv_API_retriever/svcommon/sv_campaign_parser
        """
        dict_rst = {'source': 'unknown', 'source_code': '', 'rst_type': '',
                    'medium': '', 'medium_code': '', 'brd': 0,
                    'campaign1st': '00', 'campaign2nd': '00', 'campaign3rd': '00'}
        if s_sv_campaign_code == '(not set)':
            return dict_rst

        s_sv_campaign_code = s_sv_campaign_code.upper()
        b_latest_sv_campaign_found = False
        for sCampaignPrefix in self.__g_lstLatestSvCampaignPrefix:
            if s_sv_campaign_code.find(sCampaignPrefix) > -1:
                b_latest_sv_campaign_found = True
                break

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
            # if list_campaign_code[0] in self.__g_dictSourceTagTitle:
            #     dict_rst['source'] = self.__g_dictSourceTagTitle[list_campaign_code[0]]
            #     dict_rst['source_code'] = list_campaign_code[0]
            # else:
            #     dict_rst['source'] = 'unknown'
            #     raise Exception('stop')
            
            # set search result type tag
            dict_rst['rst_type'] = self.validate_search_rst_tag(list_campaign_code[1])
            # if list_campaign_code[1] in self.__g_dictSearchResultTypeTagTitle:
            #     dict_rst['rst_type'] = list_campaign_code[1]  # self.__g_dictSearchResultTypeTagTitle[list_campaign_code[1]]
            # else:
            #     dict_rst['rst_type'] = 'unknown'
            #     raise Exception('stop')
            # set media tag
            dict_rst['medium'] = self.get_sv_medium_tag(list_campaign_code[2])
            dict_rst['medium_code'] = list_campaign_code[2]
            # if list_campaign_code[2] in self.__g_dictMediumTagTitle:
            #     dict_rst['medium'] = self.__g_dictMediumTagTitle[list_campaign_code[2]]
            #     dict_rst['medium_code'] = list_campaign_code[2]
            # else:
            #     dict_rst['medium'] = 'unknown'
            #     raise Exception('stop')

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
        else:  # process old style SV campaign code; this section will be deprecated after balanceseat complete analysis
            b_obsolete_sv_campaign_found = False
            for sCampaignPrefix in self.__g_lstObsoleteSvCampaignPrefix:
                if s_sv_campaign_code.find(sCampaignPrefix) > -1:
                    b_obsolete_sv_campaign_found = True
                    break

            if b_obsolete_sv_campaign_found:
                if s_sv_campaign_code == 'NVR_BRAND_SEARCH_MOB' or s_sv_campaign_code == 'NV_PS_BRSEARCH_MOB':
                    dict_rst['source'] = 'naver'
                    dict_rst['rst_type'] = self.validate_search_rst_tag('PS')  # self.__g_dictSearchResultTypeTagTitle['PS']
                    dict_rst['medium'] = 'display'
                    dict_rst['campaign1st'] = 'BRS'
                    dict_rst['campaign2nd'] = 'MOB'
                elif s_sv_campaign_code == 'NVR_BRAND_SEARCH_PC' or s_sv_campaign_code == 'NV_PS_BRSEARCH_PC':
                    dict_rst['source'] = 'naver'
                    dict_rst['rst_type'] = self.validate_search_rst_tag('PS')  # self.__g_dictSearchResultTypeTagTitle['PS']
                    dict_rst['medium'] = 'display'
                    dict_rst['campaign1st'] = 'BRS'
                    dict_rst['campaign2nd'] = 'PC'
                else:
                    lst_campaign_code = s_sv_campaign_code.split('_')
                    # set source tag
                    dict_rst['source'] = self.get_source_tag(list_campaign_code[0])

                    # if list_campaign_code[0] in self.__g_dictSourceTagTitle:
                    #     dict_rst['source'] = self.__g_dictSourceTagTitle[list_campaign_code[0]]
                    # else:
                    #     dict_rst['source'] = 'unknown'
                    #     raise Exception('stop')
                    # try:
                    #     dict_rst['source'] = self.__g_dictSourceTagTitle[lst_campaign_code[0]]
                    # except KeyError:
                    #     dict_rst['source'] = 'unknown'
                    #     raise Exception('stop')
                    # set media tag
                    if lst_campaign_code[1] == 'PS':
                        dict_rst['rst_type'] = self.validate_search_rst_tag('PS')  # self.__g_dictSearchResultTypeTagTitle['PS']
                        dict_rst['medium'] = 'cpc'
                        dict_rst['medium_code'] = 'CPC'
                    elif lst_campaign_code[1] == 'NS':
                        dict_rst['rst_type'] = self.validate_search_rst_tag('PS')  # self.__g_dictSearchResultTypeTagTitle['PNS']
                        dict_rst['medium'] = 'organic'
                        dict_rst['medium_code'] = 'REF'
                        if lst_campaign_code[2] == 'BLOG':  # or lst_campaign_code[2] == 'BL':
                            lst_campaign_code[2] = 'BL'
                        elif lst_campaign_code[2] == 'EXAM':  # this condition deals with balanceseat yr 2015, 2016 only
                            lst_campaign_code[2] = 'BL'
                            lst_campaign_code[3] = 'EXAM'
                        elif lst_campaign_code[2] == 'POWER':  # this condition deals with balanceseat yr 2015, 2016 only
                            lst_campaign_code[2] = 'BL'
                            lst_campaign_code[3] = 'POWERBLOGGER'
                        elif lst_campaign_code[2] == 'CAFE':
                            lst_campaign_code[2] = 'CF'
                        elif lst_campaign_code[2] == 'KIN' or lst_campaign_code[2] == 'PAGE':
                            pass

                    n_campaign_code_elem = len(lst_campaign_code)
                    if n_campaign_code_elem == 4:  # ex) NVR_NS_KIN_20150724 > very seldom
                        dict_rst['campaign1st'] = lst_campaign_code[2]
                        dict_rst['campaign2nd'] = lst_campaign_code[3]
                    elif n_campaign_code_elem == 5:  # ex) NV_PS_WELCOMGUEST_00_00
                        dict_rst['campaign1st'] = lst_campaign_code[2]
                        dict_rst['campaign2nd'] = lst_campaign_code[3]
                        dict_rst['campaign3rd'] = lst_campaign_code[4]
                    elif n_campaign_code_elem == 6:  # ex) NV_PS_WELCOM_GUEST_MAIN_01 > very seldom
                        dict_rst['campaign1st'] = lst_campaign_code[2]
                        dict_rst['campaign2nd'] = lst_campaign_code[3]
                        dict_rst['campaign3rd'] = lst_campaign_code[4] + '_' + lst_campaign_code[5]
            else:
                dict_rst['campaign1st'] = s_sv_campaign_code.strip()

            if dict_rst['campaign1st'].find('BR') > -1:
                dict_rst['brd'] = 1
        return dict_rst
    
    def get_branded_trunc(self, s_brded_terms_path):
        """ 
        called by self.decideBrandedByTerm()
        call from svload.pandas_plugins.brded_term:get_list() 
        """
        if self.__g_lstBrandedTrunc != None: # sentinel to prevent duplicated process
            return self.__g_lstBrandedTrunc
        lst_branded_trunc = []
        if s_brded_terms_path.find('/branded_term.conf') > -1:
            try:
                with open(s_brded_terms_path, 'r') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    for lst_term in reader:
                        if len(lst_term):
                            lst_branded_trunc.append(lst_term[0])
            except FileNotFoundError:
                pass
        return self.__get_unique_sored_trimmed_list(lst_branded_trunc)
    
    def set_branded_trunc(self, s_brded_terms_path, lst_line):
        """ call from svload.pandas_plugins.brded_term:update_list() """
        dict_rst = {'updated': False}
        if s_brded_terms_path.find('/branded_term.conf') == -1:
            return dict_rst

        lst_old_branded_term = self.get_branded_trunc(s_brded_terms_path)
        lst_line = self.__get_unique_sored_trimmed_list(lst_line)
        if lst_old_branded_term != lst_line:
            try:
                with open(s_brded_terms_path, 'w') as fp:
                    for s_term in lst_line:
                        if len(s_term):
                            fp.write("%s\n" % s_term.strip())
                dict_rst['updated'] = True
            except FileNotFoundError:
                pass
        return dict_rst

    def __get_unique_sored_trimmed_list(self, lst_source):
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


#if __name__ == '__main__': # for console debugging
#	oSvCampaignParser = SvCampaignParser()
#	oSvCampaignParser.sendMsg('ddd')
