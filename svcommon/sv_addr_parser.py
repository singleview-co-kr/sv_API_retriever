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
from operator import le
import sys
import logging
import re

# 3rd party library

# singleview config
if __name__ == 'svcommon.sv_addr_parser': # for platform running
    from svcommon import sv_object
elif __name__ == 'sv_addr_parser': # for plugin console debugging
    import sv_object
elif __name__ == '__main__': # for class console debugging
    sys.path.append('../svcommon')
    import sv_object

############ 해석이 어려운 주소 ######################
# 충청남도 예산군 예산읍 산성공원1길 20 (충청남도 예산군 예산읍 산성리 123) 예림오피스텔B동 111호 (산성리)
# 경기도 광주시 오포읍 창뜰윗길 123 (경기도 광주시 오포읍 능평리 123-45) 뉴파크뷰빌라(Z동) 123호 (능평리, 뉴파크뷰빌라)
# 경기도 광주시 오포읍 마루들길 123 (경기도 광주시 오포읍 양벌리 123) 123동 123호 (양벌리, 대주파크빌제9차Z블럭제123동) 

############# 도 생략 주소
# 경기 구리시 수91동 주공아파트 123동 1234호
# 경북 구미시 황상동 화진금봉타운9차아파트 123동 1234호  
# 강원	원주시	무실동	무실이편한세상아파트	123-345
# 경남	진주시	망경동	망경한보아파트	123동	345호
# 경북	안동시	정하동	현진에버빌	123동	345호
# 전북	군산시	소룡동	123번지	(유)소차공업사	보험간판보이는사무실	

############# 광역시 생략 주소
# 울산 남구 여천동 123-1 ABC케미칼(주) 부설연구소
# 광주	남구	봉선동	포스코아파트123동1234호
# 대구	북구	읍내동	한양코스모스아파트	123/1234
# 대전	서구	둔산0동	가람아파트	12동345호
# 부산	사하구	신평동	123-456번지	12통	3반	4층
# 인천

############## 특별시 생략 주소
# 서울	서초구	서초0동	1234-5	대우엘카티	123호

class SvAddrParser(sv_object.ISvObject):
    __g_regexSpecialChar = re.compile(r"[ #\&\+%@=\/\\\:;,\.'\"\^`~\_|\!\?\*$#<>()\[\]\{\}]")
    __g_regexUnicodeChar = re.compile(r"[^\x20-\x7e]")
    __g_regexDigitChar = re.compile(r"[0-9]+")
    __g_dictProvinceSynonym = {'서울': '서울특별시', '서울시': '서울특별시', 
                                '세종': '세종특별자치시', '세종시': '세종특별자치시',
								'인천': '인천광역시', '대전': '대전광역시', '대구': '대구광역시',
                                '광주': '광주광역시', '울산': '울산광역시', '부산': '부산광역시',
								'경기': '경기도', '충북': '충청북도', '충남': '충청남도',
                                '경북': '경상북도', '경남': '경상남도', '전북': '전라북도',
                                '전남': '전라남도', '강원': '강원도', 
                                '제주': '제주특별자치도','제주도':'제주특별자치도'}
    __g_lstProvinceFullname = ['서울특별시', '서울시', '세종특별자치시', '인천광역시', 
                                '광주광역시', '울산광역시', '부산광역시', '대전광역시', '대구광역시', 
								'경기도', '충청북도', '충청남도', '경상북도',
                                '경상남도', '전라북도', '전라남도', '강원도', '제주특별자치도']
    __g_dictStandardizeMetropolis = {'서울특별시': '서울', '세종특별자치시': '세종',
									'인천광역시': '인천', '대전광역시': '대전', '대구광역시': '대구',
                                    '광주광역시': '광주', '울산광역시': '울산', '부산광역시':'부산'}

    def __init__(self):
        self._g_oLogger = logging.getLogger(__file__)
        # self.__g_sAddrRaw = None  # for debug
        self.__g_lstAddrRaw = []
        self.__g_dictAddrParsed = {'do': None, 'si': None,'gu_gun': None,'dong_myun_eup': None,'bunji_ri': None}
        self.__g_dictAddrHeader = {'s_do': None, 's_si': None, 's_gu': None, 's_gun': None,
                                   's_dong': None, 's_myun': None, 's_eup': None, 's_ri': None, 
                                   's_bunji': None, # 동/리의 하위 번지
                                   # 's_ri_misc': None,
                                   's_ro': None, 's_ro_no': None   # 도로명 주소 지도 좌표 DB가 없어서 후순위
                                  }

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __del__(self):
        pass

    def parse(self, s_addr):
        s_addr = s_addr.strip()
        if len(s_addr) == 0:
            return

        self.__set_skeleton_header()
        # self.__g_sAddrRaw = s_addr
        self.__g_lstAddrRaw = s_addr.split(' ')
        if self.__g_lstAddrRaw[0].isnumeric():  # 첫 요소가 우편번호라면 제거
            self.__g_lstAddrRaw.pop(0)
        if not self.__validate_province_name():
            return

        lstTmp = range(len(self.__g_lstAddrRaw))
        for i in lstTmp:
            self.__g_lstAddrRaw[i] = re.sub(self.__g_regexSpecialChar, '', self.__g_lstAddrRaw[i])

        for _ in lstTmp:
            self.__extract_addr_elem()
        return

    def get_header(self):
        """ 계층화된 주소 정보 반환 """
        self.__standardize_header()
        return self.__g_dictAddrParsed

    def get_metropolis_dict(self):
        """ svplugins/client_serve.sv_adr.ph에서 호출 """
        return self.__g_dictStandardizeMetropolis
        
    def __standardize_header(self):
        """ 한국 주소는 도,시,(구),동,번지 / 도,시,(군),면/읍,리  로 분류되는 체제로 나뉨
        도, 시, 구/군, 동/면/읍, 번지/리 """
        # 최상위 주소 요소를 추출하여 서울특별시, 세종특별시 등의 예외를 [도] 체제와 일치시킴
        for _, s_val in self.__g_dictAddrHeader.items():
            if s_val:  
                if s_val in self.__g_dictStandardizeMetropolis.keys():
                    self.__g_dictAddrParsed['do'] = self.__g_dictStandardizeMetropolis[s_val]
                break
        
        if self.__g_dictAddrParsed['do'] is None:
            self.__g_dictAddrParsed['do'] = self.__g_dictAddrHeader['s_do']

        self.__g_dictAddrParsed['si'] = self.__g_dictAddrHeader['s_si']

        self.__g_dictAddrParsed['gu_gun'] = self.__g_dictAddrHeader['s_gu']
        if self.__g_dictAddrParsed['gu_gun'] is None:
            self.__g_dictAddrParsed['gu_gun'] = self.__g_dictAddrHeader['s_gun']

        self.__g_dictAddrParsed['dong_myun_eup'] = self.__g_dictAddrHeader['s_dong']
        if self.__g_dictAddrParsed['dong_myun_eup'] is None:
            self.__g_dictAddrParsed['dong_myun_eup'] = self.__g_dictAddrHeader['s_myun']
        if self.__g_dictAddrParsed['dong_myun_eup'] is None:
            self.__g_dictAddrParsed['dong_myun_eup'] = self.__g_dictAddrHeader['s_eup']
        
        self.__g_dictAddrParsed['bunji_ri'] = self.__g_dictAddrHeader['s_ri']
        if self.__g_dictAddrParsed['bunji_ri'] is None:
            self.__g_dictAddrParsed['bunji_ri'] = self.__g_dictAddrHeader['s_bunji']

    def __extract_addr_elem(self):
        """주소 요소 추출하여 할당"""
        if len(self.__g_lstAddrRaw):
            s_cur_elem = self.__g_lstAddrRaw[0]
            if not len(s_cur_elem):  # 무의미한 빈칸 버림
                self.__g_lstAddrRaw.pop(0)
                return
            s_addr_lvl = s_cur_elem[-1]
            b_duplicated = False
            if s_addr_lvl == '도':
                if self.__g_dictAddrHeader['s_do'] is None:
                    s_do_name = self.__g_lstAddrRaw.pop(0)
                    if s_do_name in self.__g_lstProvinceFullname:  # 도의 공식 명칭인지 확인 ex)세부주소의 여의도, 송도, 시세이도 등 회피
                        self.__g_dictAddrHeader['s_do'] = s_do_name
                else:
                    b_duplicated = True
            elif s_addr_lvl == '시':
                if self.__g_dictAddrHeader['s_si'] is None:
                    self.__g_dictAddrHeader['s_si'] = self.__g_lstAddrRaw.pop(0)
                else:
                    b_duplicated = True
            elif s_addr_lvl == '구':
                if self.__g_dictAddrHeader['s_gu'] is None:
                    self.__g_dictAddrHeader['s_gu'] = self.__g_lstAddrRaw.pop(0)
                else:
                    b_duplicated = True
            elif s_addr_lvl == '군':
                if self.__g_dictAddrHeader['s_gun'] is None:
                    self.__g_dictAddrHeader['s_gun'] = self.__g_lstAddrRaw.pop(0)
                else:
                    b_duplicated = True
            elif s_addr_lvl == '로' or s_addr_lvl == '길':
                if self.__g_dictAddrHeader['s_ro'] is None:
                    self.__g_dictAddrHeader['s_ro'] = self.__g_lstAddrRaw.pop(0)
                    if len(self.__g_lstAddrRaw):
                        self.__g_dictAddrHeader['s_ro_no'] = self.__g_lstAddrRaw.pop(0)
                else:
                    b_duplicated = True
            elif s_addr_lvl == '동':
                # 이미 읍 면 정보 추출됬으면 이후의 동 정보는 무시
                if self.__g_dictAddrHeader['s_eup'] is None and self.__g_dictAddrHeader['s_myun'] is None:
                    if self.__g_dictAddrHeader['s_dong'] is None:
                        s_kr_removed_elem = re.sub(self.__g_regexUnicodeChar, '', s_cur_elem)  # ASCII 범주 코드 영문+특수문자만 남김
                        if len(s_kr_removed_elem):  # 한글만 제거했는데 숫자영문특수기호가 남으면 아파트동호수이므로 버림
                            self.__g_lstAddrRaw.pop(0)
                        else:
                            self.__g_dictAddrHeader['s_dong'] = self.__g_lstAddrRaw.pop(0)
                            if len(self.__g_lstAddrRaw):
                                m = self.__g_regexDigitChar.search(s_cur_elem)
                                if m: # 숫자 포함된 문자열이면 번지 정보로 할당
                                    self.__g_dictAddrHeader['s_bunji'] = self.__g_lstAddrRaw.pop(0)
                                else:
                                    self.__g_lstAddrRaw.pop(0)
                    else:
                        b_duplicated = True
                else:
                    b_duplicated = True
            elif s_addr_lvl == '가':  # 예) 당산동2가, 한강로2가
                if self.__g_dictAddrHeader['s_eup'] is None and self.__g_dictAddrHeader['s_myun'] is None:
                    if self.__g_dictAddrHeader['s_dong'] is None:
                        s_number_cleared_elem = re.sub(self.__g_regexDigitChar, '', s_cur_elem)
                        s_last_elem = s_number_cleared_elem[-2:]
                        if s_last_elem == '동가' or s_last_elem == '로가':
                            self.__g_dictAddrHeader['s_dong'] = self.__g_lstAddrRaw.pop(0)
                            if len(self.__g_lstAddrRaw):
                                m = self.__g_regexDigitChar.search(s_cur_elem)
                                if m: # 숫자 포함된 문자열이면 번지 정보로 할당
                                    self.__g_dictAddrHeader['s_bunji'] = self.__g_lstAddrRaw.pop(0)
                                else:
                                    self.__g_lstAddrRaw.pop(0)
                        else:
                            self.__g_lstAddrRaw.pop(0)
                    else:
                        b_duplicated = True
                else:
                    b_duplicated = True
            elif s_addr_lvl == '읍':
                # 이미 동 정보 추출됬으면 이후의 읍 면 정보는 무시
                if self.__g_dictAddrHeader['s_dong'] is None:
                    if self.__g_dictAddrHeader['s_eup'] is None:
                        self.__g_dictAddrHeader['s_eup'] = self.__g_lstAddrRaw.pop(0)
                    else:
                        b_duplicated = True
                else:
                    b_duplicated = True
            elif s_addr_lvl == '면':
                # 이미 동 정보 추출됬으면 이후의 읍 면 정보는 무시
                if self.__g_dictAddrHeader['s_dong'] is None:
                    if self.__g_dictAddrHeader['s_myun'] is None:
                        self.__g_dictAddrHeader['s_myun'] = self.__g_lstAddrRaw.pop(0)
                    else:
                        b_duplicated = True
                else:
                    b_duplicated = True
            elif s_addr_lvl == '리':
                if self.__g_dictAddrHeader['s_dong'] is None:
                    if self.__g_dictAddrHeader['s_ri'] is None:
                        self.__g_dictAddrHeader['s_ri'] = self.__g_lstAddrRaw.pop(0)
                        if len(self.__g_lstAddrRaw):
                            m = self.__g_regexDigitChar.search(s_cur_elem)
                            if m: # 숫자 포함된 문자열이면 번지 정보로 할당
                                self.__g_dictAddrHeader['s_bunji'] = self.__g_lstAddrRaw.pop(0)
                            else:
                                self.__g_lstAddrRaw.pop(0)
                    else:
                        b_duplicated = True
                else:
                    b_duplicated = True
            elif s_addr_lvl == '호':
                self.__g_lstAddrRaw.pop(0)  # 일반적으로 아파트 호수 버림
            else:
                self.__g_lstAddrRaw.pop(0)  # print('처리 규칙이 없는 요소 버림: (' + self.__g_lstAddrRaw.pop(0) + ')');

            if b_duplicated:
                self.__g_lstAddrRaw.pop(0)  # print('중복 가능 요소 버림: (' + self.__g_lstAddrRaw.pop(0) + ')');

    def __validate_province_name(self):
        """ 시,도 약어를 정식명칭으로 변환 """
        if self.__g_lstAddrRaw[0] in list(self.__g_dictProvinceSynonym.keys()):
            self.__g_lstAddrRaw[0] = self.__g_dictProvinceSynonym[self.__g_lstAddrRaw[0]]

        if self.__g_lstAddrRaw[0] in self.__g_lstProvinceFullname:
            return True
        else:
            return False  # 첫 주소 요소가 시도 명칭이 아니면

    def __set_skeleton_header(self):
		# 입력값 초기화
        self.__g_dictAddrHeader = {k: None for k in self.__g_dictAddrHeader}
        # 해석 결과 저장공간 초기화
        self.__g_dictAddrParsed = {k: None for k in self.__g_dictAddrParsed}
        

# if __name__ == '__main__': # for console debugging
# 	pass
