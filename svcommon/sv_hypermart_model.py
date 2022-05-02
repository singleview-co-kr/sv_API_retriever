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

# singleview library
# if __name__ == 'sv_hypermart_model': # for console debugging
#     pass
# else: # for platform running
#     pass

from django.db import models


# to write korean string from excel into csv, or exception 'ascii' codec can't encode characters in position - begin
# # http://blog.dscpl.com.au/2014/09/setting-lang-and-lcall-when-using.html -> launch WSGI Daemon with utf-8 locale is first
# import locale
# import platform
# s_os_title = platform.system()
# if s_os_title == 'Linux':
#    # https://stackoverflow.com/questions/9942594/unicodeencodeerror-ascii-codec-cant-encode-character-u-xa0-in-position-20
#    os.environ["PYTHONIOENCODING"] = "utf-8"
#    myLocale=locale.setlocale(category=locale.LC_ALL, locale="ko_KR.UTF-8")
# elif s_os_title == 'Window':
#    pass
# # to write korean string from excel into csv, or exception 'ascii' codec can't encode characters in position - end
class SvHyperMartType(models.IntegerChoices):
    # https://docs.djangoproject.com/en/3.1/ref/models/fields/#enumeration-types
    ESTIMATION = 1, '분석 중'
    NOT_SURE = 2, '판단 불가'
    EMART = 3, 'Emart'
    LOTTEMART = 4, 'Lottemart'
    HOMEPLUS = 5, 'Homeplus'

    @classmethod
    def get_dict_by_idx(cls):
        return {key.value: key.name.title() for key in cls}

    @classmethod
    def get_dict_by_title(cls):
        return {key.name.title(): key.value for key in cls}


class EdiDataType(models.IntegerChoices):
    """사용자가 직접 업로드한 파일에서 추출한 실제 로우 데이터 파일 기록: 개별 엑셀 파일"""
    ESTIMATION = 1, '분석 중'
    NOT_SURE = 2, '판단 불가'  # for emart only
    IGNORE = 3, '무시'
    QTY = 4, '수량 EDI'  # for emart only
    AMNT = 5, '금액 EDI'  # for emart only
    QTY_AMNT = 6, '수량금액 EDI'  # for lottemart, homeplus


class BranchType(models.IntegerChoices):
    OFFLINE = 1, 'Offline'
    ONLINE = 2, 'Online'

    @classmethod
    def choices(cls):
        return [(key.value, key.name) for key in cls]
    
    @classmethod
    def get_dict_by_title(cls):
        return {key.name.title(): key.value for key in cls}



class ProgressStatus(models.IntegerChoices):
    DENIED = 1, '거부'  # data file has been uploaded
    UPLOADED = 2, '업로드 완료'  # data file has been uploaded
    ON_TRANSFORMING = 3, '변환 중'  # data file is on transforming to csv
    TRANSFORMED = 4, '변환 완료'  # data file has been transformed to csv


class SvHypermartGeoInfo():
    lst_hypermart_geo_info = [
        {'id': 1, 'hypermart_name': 'Emart', 'branch_code': '1000', 'name': '창동점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '도봉구', 'dong_myun_ri': '창동', 'latitude': '37.651793', 'longitude': '127.046965'},
        {'id': 2, 'hypermart_name': 'Emart', 'branch_code': '1001', 'name': '일산점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '일산동구', 'dong_myun_ri': '백석동', 'latitude': '37.648015', 'longitude': '126.782407'},
        {'id': 3, 'hypermart_name': 'Emart', 'branch_code': '1003', 'name': '부평점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '부평구', 'dong_myun_ri': '갈산동', 'latitude': '37.516875', 'longitude': '126.725972'},
        {'id': 4, 'hypermart_name': 'Emart', 'branch_code': '1006', 'name': '제주점', 'branch_type': 'Offline', 'do_name': '제주특별자치도', 'si_name': '제주시', 'gu_gun': None, 'dong_myun_ri': '삼도이동', 'latitude': '33.518195', 'longitude': '126.521227'},
        {'id': 5, 'hypermart_name': 'Emart', 'branch_code': '1007', 'name': '분당점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '성남시', 'gu_gun': '분당구', 'dong_myun_ri':'정자3동', 'latitude': '37.358911', 'longitude': '127.119727'},
        {'id': 6, 'hypermart_name': 'Emart', 'branch_code': '1008', 'name': '덕이점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '일산서구', 'dong_myun_ri': '송산동', 'latitude': '37.69004', 'longitude': '126.760365'},
        {'id': 7, 'hypermart_name': 'Emart', 'branch_code': '1009', 'name': '남원점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '남원시', 'gu_gun': '남문로', 'dong_myun_ri': None, 'latitude': '35.408049', 'longitude': '127.375068'},
        {'id': 8, 'hypermart_name': 'Emart', 'branch_code': '1010', 'name': '안양점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '안양시', 'gu_gun': '동안구', 'dong_myun_ri':'비산동', 'latitude': '37.398572', 'longitude': '126.935213'},
        {'id': 9, 'hypermart_name': 'Emart', 'branch_code': '1011', 'name': '서부산점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '사상구', 'dong_myun_ri': '감전동', 'latitude': '35.142727', 'longitude': '128.971256'},
        {'id': 10, 'hypermart_name': 'Emart', 'branch_code': '1013', 'name': '인천점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '남동구', 'dong_myun_ri': '구월1동', 'latitude': '37.446212', 'longitude': '126.700816'},
        {'id': 11, 'hypermart_name': 'Emart', 'branch_code': '1014', 'name': '김천점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '김천시', 'gu_gun': None, 'dong_myun_ri': '대신동', 'latitude': '36.135057', 'longitude': '128.116792'},
        {'id': 12, 'hypermart_name': 'Emart', 'branch_code': '1015', 'name': '동광주점', 'branch_type': 'Offline', 'do_name': '광주', 'si_name': '광주광역시', 'gu_gun': '동구', 'dong_myun_ri': '계림2동', 'latitude': '35.163918', 'longitude': '126.920357'},
        {'id': 13, 'hypermart_name': 'Emart', 'branch_code': '1016', 'name': '청주점', 'branch_type': 'Offline', 'do_name': '충청북도', 'si_name': '청주시', 'gu_gun': '흥덕구', 'dong_myun_ri': '미평동', 'latitude': '36.601342', 'longitude': '127.47466'},
        {'id': 14, 'hypermart_name': 'Emart', 'branch_code': '1017', 'name': '전주점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '전주시', 'gu_gun': '완산구', 'dong_myun_ri': '서신동', 'latitude': '35.832718', 'longitude': '127.120479'},
        {'id': 15, 'hypermart_name': 'Emart', 'branch_code': '1018', 'name': '부천점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '부천시', 'gu_gun': None, 'dong_myun_ri': '심곡본동', 'latitude': '37.484268', 'longitude': '126.782665'},
        {'id': 16, 'hypermart_name': 'Emart', 'branch_code': '1019', 'name': '원주점', 'branch_type': 'Offline', 'do_name': '강원도', 'si_name': '원주시', 'gu_gun': '무실동', 'dong_myun_ri': None, 'latitude': '37.320794', 'longitude': '127.919011'},
        {'id': 17, 'hypermart_name': 'Emart', 'branch_code': '1020', 'name': '역삼점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '강남구', 'dong_myun_ri': '역삼동', 'latitude': '37.49934', 'longitude': '127.04853'},
        {'id': 18, 'hypermart_name': 'Emart', 'branch_code': '1021', 'name': '구로점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '구로구', 'dong_myun_ri': '구로동', 'latitude': '37.484538', 'longitude': '126.897945'},
        {'id': 19, 'hypermart_name': 'Emart', 'branch_code': '1022', 'name': '신월점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '양천구', 'dong_myun_ri': '신월동', 'latitude': '37.539906', 'longitude': '126.828506'},
        {'id': 20, 'hypermart_name': 'Emart', 'branch_code': '1023', 'name': '성서점', 'branch_type': 'Offline', 'do_name': '대구', 'si_name': '대구광역시', 'gu_gun': '달서구', 'dong_myun_ri': '이곡동', 'latitude': '35.853564', 'longitude': '128.510197'},
        {'id': 21, 'hypermart_name': 'Emart', 'branch_code': '1024', 'name': '산본점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '군포시', 'gu_gun': None, 'dong_myun_ri': '광정동', 'latitude': '37.361212', 'longitude': '126.931578'},
        {'id': 22, 'hypermart_name': 'Emart', 'branch_code': '1025', 'name': '천호점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '강동구', 'dong_myun_ri': '천호동', 'latitude': '37.538773', 'longitude': '127.125655'},
        {'id': 23, 'hypermart_name': 'Emart', 'branch_code': '1026', 'name': '가양점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '강서구', 'dong_myun_ri': '가양동', 'latitude': '37.55832', 'longitude': '126.861766'},
        {'id': 24, 'hypermart_name': 'Emart', 'branch_code': '1027', 'name': '해운대점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '해운대구', 'dong_myun_ri': '중동', 'latitude': '35.166204', 'longitude': '129.167366'},
        {'id': 25, 'hypermart_name': 'Emart', 'branch_code': '1028', 'name': '시화점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '시흥시', 'gu_gun': None, 'dong_myun_ri': '정왕본동', 'latitude': '37.34613', 'longitude': '126.736876'},
        {'id': 26, 'hypermart_name': 'Emart', 'branch_code': '1029', 'name': '상봉점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '중랑구', 'dong_myun_ri': '망우동', 'latitude': '37.596648', 'longitude': '127.093682'},
        {'id': 27, 'hypermart_name': 'Emart', 'branch_code': '1030', 'name': '이천점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '이천시', 'gu_gun': None, 'dong_myun_ri': '증포동', 'latitude': '37.294102', 'longitude': '127.458966'},
        {'id': 28, 'hypermart_name': 'Emart', 'branch_code': '1032', 'name': '진주점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '진주시', 'gu_gun': None, 'dong_myun_ri': '인사동', 'latitude': '35.191787', 'longitude': '128.077497'},
        {'id': 29, 'hypermart_name': 'Emart', 'branch_code': '1033', 'name': '시지점', 'branch_type': 'Offline', 'do_name': '대구', 'si_name': '대구광역시', 'gu_gun': '수성구', 'dong_myun_ri': '신매동', 'latitude': '35.840532', 'longitude': '128.704148'},
        {'id': 30, 'hypermart_name': 'Emart', 'branch_code': '1034', 'name': '목포점', 'branch_type': 'Offline', 'do_name': '전라남도', 'si_name': '목포시', 'gu_gun': None, 'dong_myun_ri': '옥암동', 'latitude': '34.811833', 'longitude': '126.425329'},
        {'id': 31, 'hypermart_name': 'Emart', 'branch_code': '1035', 'name': '동인천점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '중구', 'dong_myun_ri': '신생동', 'latitude': '37.466849', 'longitude': '126.628993'},
        {'id': 32, 'hypermart_name': 'Emart', 'branch_code': '1036', 'name': '만촌점', 'branch_type': 'Offline', 'do_name': '대구', 'si_name': '대구광역시', 'gu_gun': '수성구', 'dong_myun_ri': '만촌1동', 'latitude': '35.87124', 'longitude': '128.636752'},
        {'id': 33, 'hypermart_name': 'Emart', 'branch_code': '1037', 'name': '군산점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '군산시', 'gu_gun': None, 'dong_myun_ri': '경암동', 'latitude': '35.982924', 'longitude': '126.734833'},
        {'id': 34, 'hypermart_name': 'Emart', 'branch_code': '1038', 'name': '성수점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '성동구', 'dong_myun_ri': '성수2가1동', 'latitude': '37.53984', 'longitude': '127.053122'},
        {'id': 35, 'hypermart_name': 'Emart', 'branch_code': '1039', 'name': '월배점', 'branch_type': 'Offline', 'do_name': '대구', 'si_name': '대구광역시', 'gu_gun': '달서구', 'dong_myun_ri': '대천동', 'latitude': '35.816902', 'longitude': '128.523077'},
        {'id': 36, 'hypermart_name': 'Emart', 'branch_code': '1040', 'name': '천안점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '천안시', 'gu_gun': '서북구', 'dong_myun_ri': '충무로', 'latitude': '36.796066', 'longitude': '127.127098'},
        {'id': 37, 'hypermart_name': 'Emart', 'branch_code': '1041', 'name': '수서점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '강남구', 'dong_myun_ri': '수서동', 'latitude': '37.487436', 'longitude': '127.103098'},
        {'id': 38, 'hypermart_name': 'Emart', 'branch_code': '1042', 'name': '화정점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '덕양구', 'dong_myun_ri': '화정2동', 'latitude': '37.632816', 'longitude': '126.833452'},
        {'id': 39, 'hypermart_name': 'Emart', 'branch_code': '1044', 'name': '상무점', 'branch_type': 'Offline', 'do_name': '광주', 'si_name': '광주광역시', 'gu_gun': '광산구', 'dong_myun_ri': '첨단2동', 'latitude': '35.210739', 'longitude': '126.846885'},
        {'id': 40, 'hypermart_name': 'Emart', 'branch_code': '1046', 'name': '수원점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '수원시', 'gu_gun': '권선구', 'dong_myun_ri': '권선2동', 'latitude': '37.250093', 'longitude': '127.021549'},
        {'id': 41, 'hypermart_name': 'Emart', 'branch_code': '1047', 'name': '충주점', 'branch_type': 'Offline', 'do_name': '충청북도', 'si_name': '충주시', 'gu_gun': None, 'dong_myun_ri': '문화동', 'latitude': '36.972634', 'longitude': '127.923362'},
        {'id': 42, 'hypermart_name': 'Emart', 'branch_code': '1048', 'name': '평택점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '평택시', 'gu_gun': None, 'dong_myun_ri': '세교동', 'latitude': '37.021256', 'longitude': '127.075569'},
        {'id': 43, 'hypermart_name': 'Emart', 'branch_code': '1049', 'name': '은평점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '은평구', 'dong_myun_ri': '응암동', 'latitude': '37.600956', 'longitude': '126.920499'},
        {'id': 44, 'hypermart_name': 'Emart', 'branch_code': '1050', 'name': '포항점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '포항시', 'gu_gun': '남구', 'dong_myun_ri': '제철동', 'latitude': '35.99162', 'longitude': '129.398998'},
        {'id': 45, 'hypermart_name': 'Emart', 'branch_code': '1051', 'name': '여수점', 'branch_type': 'Offline', 'do_name': '전라남도', 'si_name': '여수시', 'gu_gun': None, 'dong_myun_ri': '오림동', 'latitude': '34.758175', 'longitude': '127.71507'},
        {'id': 46, 'hypermart_name': 'Emart', 'branch_code': '1052', 'name': '연제점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '연제구', 'dong_myun_ri': '연산동', 'latitude': '35.175814', 'longitude': '129.080736'},
        {'id': 47, 'hypermart_name': 'Emart', 'branch_code': '1054', 'name': '중동점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '부천시', 'gu_gun': '원미구', 'dong_myun_ri': '중1동', 'latitude': '37.504189', 'longitude': '126.763924'},
        {'id': 48, 'hypermart_name': 'Emart', 'branch_code': '1055', 'name': '칠성점', 'branch_type': 'Offline', 'do_name': '대구', 'si_name': '대구광역시', 'gu_gun': '북구', 'dong_myun_ri': '칠성동', 'latitude': '35.885346', 'longitude': '128.589679'},
        {'id': 49, 'hypermart_name': 'Emart', 'branch_code': '1056', 'name': '둔산점', 'branch_type': 'Offline', 'do_name': '대전', 'si_name': '대전광역시', 'gu_gun': '서구', 'dong_myun_ri': '둔산동', 'latitude': '36.356464', 'longitude': '127.378148'},
        {'id': 50, 'hypermart_name': 'Emart', 'branch_code': '1057', 'name': '계양점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '계양구', 'dong_myun_ri': '작전동', 'latitude': '37.531235', 'longitude': '126.736883'},
        {'id': 51, 'hypermart_name': 'Emart', 'branch_code': '1058', 'name': '구미점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '구미시', 'gu_gun': None, 'dong_myun_ri': '광평동', 'latitude': '36.109694', 'longitude': '128.364796'},
        {'id': 52, 'hypermart_name': 'Emart', 'branch_code': '1059', 'name': '창원점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '창원시', 'gu_gun': '성산구', 'dong_myun_ri': '중앙동', 'latitude': '35.226989', 'longitude': '128.680396'},
        {'id': 53, 'hypermart_name': 'Emart', 'branch_code': '1060', 'name': '평촌점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '안양시', 'gu_gun': '동안구', 'dong_myun_ri': '관양동', 'latitude': '37.394917', 'longitude': '126.963739'},
        {'id': 54, 'hypermart_name': 'Emart', 'branch_code': '1061', 'name': '감삼점', 'branch_type': 'Offline', 'do_name': '대구', 'si_name': '대구광역시', 'gu_gun': '달서구', 'dong_myun_ri': '감삼동', 'latitude': '35.845635', 'longitude': '128.536504'},
        {'id': 55, 'hypermart_name': 'Emart', 'branch_code': '1062', 'name': '명일점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '강동구', 'dong_myun_ri': '명일동', 'latitude': '37.554856', 'longitude': '127.155773'},
        {'id': 56, 'hypermart_name': 'Emart', 'branch_code': '1063', 'name': '마산점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '창원시', 'gu_gun': '마산합포구', 'dong_myun_ri': '신포동1가', 'latitude': '35.198101', 'longitude': '128.570161'},
        {'id': 57, 'hypermart_name': 'Emart', 'branch_code': '1064', 'name': '연수점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '연수구', 'dong_myun_ri': '동춘동', 'latitude': '37.404474', 'longitude': '126.681153'},
        {'id': 58, 'hypermart_name': 'Emart', 'branch_code': '1065', 'name': '강릉점', 'branch_type': 'Offline', 'do_name': '강원도', 'si_name': '강릉시', 'gu_gun': '송정동', 'dong_myun_ri': '경강로', 'latitude': '37.770748', 'longitude': '128.922616'},
        {'id': 59, 'hypermart_name': 'Emart', 'branch_code': '1067', 'name': '고잔점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '안산시', 'gu_gun': '단원구', 'dong_myun_ri': '초지동', 'latitude': '37.303322', 'longitude': '126.813042'},
        {'id': 60, 'hypermart_name': 'Emart', 'branch_code': '1068', 'name': '문현점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '남구', 'dong_myun_ri': '문현동', 'latitude': '35.144544', 'longitude': '129.064205'},
        {'id': 61, 'hypermart_name': 'Emart', 'branch_code': '1069', 'name': '금정점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '금정구', 'dong_myun_ri': '구서동', 'latitude': '35.25009', 'longitude': '129.090617'},
        {'id': 62, 'hypermart_name': 'Emart', 'branch_code': '1070', 'name': '수지점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '용인시', 'gu_gun': '수지구', 'dong_myun_ri': '신봉동', 'latitude': '37.319893', 'longitude': '127.083408'},
        {'id': 63, 'hypermart_name': 'Emart', 'branch_code': '1071', 'name': '신제주점', 'branch_type': 'Offline', 'do_name': '제주특별자치도', 'si_name': '제주시', 'gu_gun': None, 'dong_myun_ri': '노형동', 'latitude': '33.485119', 'longitude': '126.480528'},
        {'id': 64, 'hypermart_name': 'Emart', 'branch_code': '1072', 'name': '속초점', 'branch_type': 'Offline', 'do_name': '강원도', 'si_name': '속초시', 'gu_gun': None, 'dong_myun_ri': '청호동', 'latitude': '38.191222', 'longitude': '128.595789'},
        {'id': 65, 'hypermart_name': 'Emart', 'branch_code': '1073', 'name': '사상점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '사상구', 'dong_myun_ri': '괘법동', 'latitude': '35.164396', 'longitude': '128.978873'},
        {'id': 66, 'hypermart_name': 'Emart', 'branch_code': '1074', 'name': '울산점', 'branch_type': 'Offline', 'do_name': '울산', 'si_name': '울산광역시', 'gu_gun': '남구', 'dong_myun_ri': '삼산동', 'latitude': '35.538166', 'longitude': '129.348332'},
        {'id': 67, 'hypermart_name': 'Emart', 'branch_code': '1075', 'name': '동해점', 'branch_type': 'Offline', 'do_name': '강원도', 'si_name': '동해시', 'gu_gun': None, 'dong_myun_ri': '천곡동', 'latitude': '37.52406', 'longitude': '129.117122'},
        {'id': 68, 'hypermart_name': 'Emart', 'branch_code': '1076', 'name': '영천점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '영천시', 'gu_gun': None, 'dong_myun_ri': '오수동', 'latitude': '35.958708', 'longitude': '128.910082'},
        {'id': 69, 'hypermart_name': 'Emart', 'branch_code': '1077', 'name': '광산점', 'branch_type': 'Offline', 'do_name': '광주', 'si_name': '광주광역시', 'gu_gun': '광산구', 'dong_myun_ri': '우산동', 'latitude': '35.160234', 'longitude': '126.808634'},
        {'id': 70, 'hypermart_name': 'Emart', 'branch_code': '1078', 'name': '양주점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '양주시', 'gu_gun': None, 'dong_myun_ri': '회정동', 'latitude': '37.841908', 'longitude': '127.05381'},
        {'id': 71, 'hypermart_name': 'Emart', 'branch_code': '1079', 'name': '이마트몰', 'branch_type': 'Online', 'do_name': '온라인', 'si_name': '온라인', 'gu_gun': '온라인', 'dong_myun_ri': '온라인', 'latitude': None, 'longitude': None},
        {'id': 72, 'hypermart_name': 'Emart', 'branch_code': '1080', 'name': '양산점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '양산시', 'gu_gun': None, 'dong_myun_ri': '중부동', 'latitude': '35.337163', 'longitude': '129.026443'},
        {'id': 73, 'hypermart_name': 'Emart', 'branch_code': '1081', 'name': '파주점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '파주시', 'gu_gun': None, 'dong_myun_ri': '교하동', 'latitude': '37.747379', 'longitude': '126.750833'},
        {'id': 74, 'hypermart_name': 'Emart', 'branch_code': '1082', 'name': '반야월점', 'branch_type': 'Offline', 'do_name': '대구', 'si_name': '대구광역시', 'gu_gun': '동구', 'dong_myun_ri': '신서동', 'latitude': '35.870927', 'longitude': '128.727465'},
        {'id': 75, 'hypermart_name': 'Emart', 'branch_code': '1083', 'name': '포항이동점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '포항시', 'gu_gun': '북구', 'dong_myun_ri': '득량동', 'latitude': '36.031972', 'longitude': '129.339044'},
        {'id': 76, 'hypermart_name': 'Emart', 'branch_code': '1084', 'name': '월계점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '노원구', 'dong_myun_ri': '월계3동', 'latitude': '37.62675', 'longitude': '127.061886'},
        {'id': 77, 'hypermart_name': 'Emart', 'branch_code': '1085', 'name': '용산점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '용산구', 'dong_myun_ri': '한강로3가', 'latitude': '37.5297', 'longitude': '126.965513'},
        {'id': 78, 'hypermart_name': 'Emart', 'branch_code': '1086', 'name': '안동점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '안동시', 'gu_gun': None, 'dong_myun_ri': '옥동', 'latitude': '36.558525', 'longitude': '128.699381'},
        {'id': 79, 'hypermart_name': 'Emart', 'branch_code': '1087', 'name': '양재점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '서초구', 'dong_myun_ri': '양재2동', 'latitude': '37.4636', 'longitude': '127.036938'},
        {'id': 80, 'hypermart_name': 'Emart', 'branch_code': '1088', 'name': '인천공항점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '중구', 'dong_myun_ri': '운서동', 'latitude': '37.439861', 'longitude': '126.458398'},
        {'id': 81, 'hypermart_name': 'Emart', 'branch_code': '1089', 'name': '통영점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '통영시', 'gu_gun': None, 'dong_myun_ri': '광도면', 'latitude': '34.887293', 'longitude': '128.418048'},
        {'id': 82, 'hypermart_name': 'Emart', 'branch_code': '1090', 'name': '순천점', 'branch_type': 'Offline', 'do_name': '전라남도', 'si_name': '순천시', 'gu_gun': None, 'dong_myun_ri': '덕암동', 'latitude': '34.942606', 'longitude': '127.508359'},
        {'id': 83, 'hypermart_name': 'Emart', 'branch_code': '1091', 'name': '서수원점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '수원시', 'gu_gun': '권선구', 'dong_myun_ri': '구운동', 'latitude': '37.282831', 'longitude': '126.969992'},
        {'id': 84, 'hypermart_name': 'Emart', 'branch_code': '1092', 'name': '죽전점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '용인시', 'gu_gun': '수지구', 'dong_myun_ri': '죽전동', 'latitude': '37.32558', 'longitude': '127.109976'},
        {'id': 85, 'hypermart_name': 'Emart', 'branch_code': '1093', 'name': '춘천점', 'branch_type': 'Offline', 'do_name': '강원도', 'si_name': '춘천시', 'gu_gun': None, 'dong_myun_ri': '온의동', 'latitude': '37.863871', 'longitude': '127.718627'},
        {'id': 86, 'hypermart_name': 'Emart', 'branch_code': '1094', 'name': '남양주점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '남양주시', 'gu_gun': None, 'dong_myun_ri': '호평동', 'latitude': '37.655105', 'longitude': '127.243219'},
        {'id': 87, 'hypermart_name': 'Emart', 'branch_code': '1095', 'name': '오산점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '오산시', 'gu_gun': None, 'dong_myun_ri': '대원동', 'latitude': '37.140824', 'longitude': '127.072705'},
        {'id': 88, 'hypermart_name': 'Emart', 'branch_code': '1096', 'name': '용인점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '용인시', 'gu_gun': '처인구', 'dong_myun_ri': '역북동', 'latitude': '37.231574', 'longitude': '127.189132'},
        {'id': 89, 'hypermart_name': 'Emart', 'branch_code': '1097', 'name': '서귀포점', 'branch_type': 'Offline', 'do_name': '제주특별자치도', 'si_name': '서귀포시', 'gu_gun': None, 'dong_myun_ri': '대륜동', 'latitude': '33.248629', 'longitude': '126.509231'},
        {'id': 90, 'hypermart_name': 'Emart', 'branch_code': '1098', 'name': '경산점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '경산시', 'gu_gun': None, 'dong_myun_ri': '중산동', 'latitude': '35.835204', 'longitude': '128.720177'},
        {'id': 91, 'hypermart_name': 'Emart', 'branch_code': '1099', 'name': '(구)광주점', 'branch_type': 'Offline', 'do_name': '광주', 'si_name': '광주광역시', 'gu_gun': '서구', 'dong_myun_ri': '화정1동', 'latitude': '35.159003', 'longitude': '126.882083'},
        {'id': 92, 'hypermart_name': 'Emart', 'branch_code': '1100', 'name': '동백점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '용인시', 'gu_gun': '기흥구', 'dong_myun_ri': '동백동', 'latitude': '37.278146', 'longitude': '127.151605'},
        {'id': 93, 'hypermart_name': 'Emart', 'branch_code': '1101', 'name': '검단점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '서구', 'dong_myun_ri': '당하동', 'latitude': '37.585747', 'longitude': '126.677205'},
        {'id': 94, 'hypermart_name': 'Emart', 'branch_code': '1102', 'name': '익산점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '익산시', 'gu_gun': None, 'dong_myun_ri': '동산동', 'latitude': '35.930755', 'longitude': '126.958023'},
        {'id': 95, 'hypermart_name': 'Emart', 'branch_code': '1103', 'name': '아산점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '아산시', 'gu_gun': None, 'dong_myun_ri': '온양6동', 'latitude': '36.776957', 'longitude': '127.021778'},
        {'id': 96, 'hypermart_name': 'Emart', 'branch_code': '1104', 'name': '태백점', 'branch_type': 'Offline', 'do_name': '강원도', 'si_name': '태백시', 'gu_gun': None, 'dong_myun_ri': '화전동', 'latitude': '37.200099', 'longitude': '128.961872'},
        {'id': 97, 'hypermart_name': 'Emart', 'branch_code': '1105', 'name': '광명점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '광명시', 'gu_gun': None, 'dong_myun_ri': '광명동', 'latitude': '37.479641', 'longitude': '126.855528'},
        {'id': 98, 'hypermart_name': 'Emart', 'branch_code': '1106', 'name': '자양점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '광진구', 'dong_myun_ri': '자양3동', 'latitude': '37.538699', 'longitude': '127.073358'},
        {'id': 99, 'hypermart_name': 'Emart', 'branch_code': '1107', 'name': '상주점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '상주시', 'gu_gun': None, 'dong_myun_ri': '만산동', 'latitude': '36.427899', 'longitude': '128.150998'},
        {'id': 100, 'hypermart_name': 'Emart', 'branch_code': '1108', 'name': '봉선점', 'branch_type': 'Offline', 'do_name': '광주', 'si_name': '광주광역시', 'gu_gun': '남구', 'dong_myun_ri': '봉선2동', 'latitude': '35.122485', 'longitude': '126.916429'},
        {'id': 101, 'hypermart_name': 'Emart', 'branch_code': '1109', 'name': '신도림점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '구로구', 'dong_myun_ri': '구로동', 'latitude': '37.507256', 'longitude': '126.890129'},
        {'id': 102, 'hypermart_name': 'Emart', 'branch_code': '1110', 'name': '여주점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '여주군', 'gu_gun': '여주읍', 'dong_myun_ri': '홍문리', 'latitude': '37.284572', 'longitude': '127.635613'},
        {'id': 103, 'hypermart_name': 'Emart', 'branch_code': '1111', 'name': '동탄점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '화성시', 'gu_gun': None, 'dong_myun_ri': '동탄1동', 'latitude': '37.214476', 'longitude': '127.079569'},
        {'id': 104, 'hypermart_name': 'Emart', 'branch_code': '1113', 'name': '여의도점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '영등포구', 'dong_myun_ri': '여의도동', 'latitude': '37.518728', 'longitude': '126.926516'},
        {'id': 105, 'hypermart_name': 'Emart', 'branch_code': '1114', 'name': '다산점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '남양주시', 'gu_gun': None, 'dong_myun_ri': '도농동', 'latitude': '37.60877', 'longitude': '127.15856'},
        {'id': 106, 'hypermart_name': 'Emart', 'branch_code': '1115', 'name': '하남점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '하남시', 'gu_gun': None, 'dong_myun_ri': '덕풍3동', 'latitude': '37.553044', 'longitude': '127.205326'},
        {'id': 107, 'hypermart_name': 'Emart', 'branch_code': '1116', 'name': '청계천점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '중구', 'dong_myun_ri': '황학동', 'latitude': '37.571202', 'longitude': '127.022977'},
        {'id': 108, 'hypermart_name': 'Emart', 'branch_code': '1117', 'name': '왕십리점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '성동구', 'dong_myun_ri': '행당1동', 'latitude': '37.561941', 'longitude': '127.038488'},
        {'id': 109, 'hypermart_name': 'Emart', 'branch_code': '1118', 'name': '보령점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '보령시', 'gu_gun': None, 'dong_myun_ri': '궁촌동', 'latitude': '36.3442', 'longitude': '126.588632'},
        {'id': 110, 'hypermart_name': 'Emart', 'branch_code': '1119', 'name': '안성점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '안성시', 'gu_gun': None, 'dong_myun_ri': '안성2동', 'latitude': '37.013213', 'longitude': '127.255835'},
        {'id': 111, 'hypermart_name': 'Emart', 'branch_code': '1120', 'name': '미아점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '성북구', 'dong_myun_ri': '길음동', 'latitude': '37.611052', 'longitude': '127.029845'},
        {'id': 112, 'hypermart_name': 'Emart', 'branch_code': '1121', 'name': '보라점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '용인시', 'gu_gun': '기흥구', 'dong_myun_ri': '보라동', 'latitude': '37.253801', 'longitude': '127.104543'},
        {'id': 113, 'hypermart_name': 'Emart', 'branch_code': '1123', 'name': '이문점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '동대문구', 'dong_myun_ri': '이문1동', 'latitude': '37.598472', 'longitude': '127.06164'},
        {'id': 114, 'hypermart_name': 'Emart', 'branch_code': '1124', 'name': '목동점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '양천구', 'dong_myun_ri': '목1동', 'latitude': '37.525683', 'longitude': '126.870905'},
        {'id': 115, 'hypermart_name': 'Emart', 'branch_code': '1125', 'name': '흥덕점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '용인시', 'gu_gun': '기흥구', 'dong_myun_ri': '영덕동', 'latitude': '37.275882', 'longitude': '127.073543'},
        {'id': 116, 'hypermart_name': 'Emart', 'branch_code': '1126', 'name': '영등포점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '영등포구', 'dong_myun_ri': '영등포동4가', 'latitude': '37.517055', 'longitude': '126.903146'},
        {'id': 117, 'hypermart_name': 'Emart', 'branch_code': '1127', 'name': '경기광주점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '광주시', 'gu_gun': None, 'dong_myun_ri': '경안동', 'latitude': '37.410214', 'longitude': '127.261226'},
        {'id': 118, 'hypermart_name': 'Emart', 'branch_code': '1128', 'name': '수색점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '은평구', 'dong_myun_ri': '수색동', 'latitude': '37.580119', 'longitude': '126.898372'},
        {'id': 119, 'hypermart_name': 'Emart', 'branch_code': '1129', 'name': '제천점', 'branch_type': 'Offline', 'do_name': '충청북도', 'si_name': '제천시', 'gu_gun': None, 'dong_myun_ri': '화산동', 'latitude': '37.123413', 'longitude': '128.222809'},
        {'id': 120, 'hypermart_name': 'Emart', 'branch_code': '1132', 'name': '포천점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '포천시', 'gu_gun': None, 'dong_myun_ri': '설운동', 'latitude': '37.847876', 'longitude': '127.160401'},
        {'id': 121, 'hypermart_name': 'Emart', 'branch_code': '1133', 'name': '성남점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '성남시', 'gu_gun': '수정구', 'dong_myun_ri': '태평동', 'latitude': '37.44428', 'longitude': '127.141551'},
        {'id': 122, 'hypermart_name': 'Emart', 'branch_code': '1134', 'name': '광명소하점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '광명시', 'gu_gun': None, 'dong_myun_ri': '소하동', 'latitude': '37.446863', 'longitude': '126.884608'},
        {'id': 123, 'hypermart_name': 'Emart', 'branch_code': '1135', 'name': '천안터미널점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '천안시', 'gu_gun': '동남구', 'dong_myun_ri': '신부동', 'latitude': '36.819795', 'longitude': '127.156646'},
        {'id': 124, 'hypermart_name': 'Emart', 'branch_code': '1136', 'name': '진접점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '남양주시', 'gu_gun': None, 'dong_myun_ri': '진접읍', 'latitude': '37.709037', 'longitude': '127.181963'},
        {'id': 125, 'hypermart_name': 'Emart', 'branch_code': '1137', 'name': '사천점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '사천시', 'gu_gun': None, 'dong_myun_ri': '좌룡동', 'latitude': '34.950828', 'longitude': '128.077376'},
        {'id': 126, 'hypermart_name': 'Emart', 'branch_code': '1138', 'name': '이수점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '동작구', 'dong_myun_ri': '사당동', 'latitude': '37.484694', 'longitude': '126.980395'},
        {'id': 127, 'hypermart_name': 'Emart', 'branch_code': '1139', 'name': '묵동점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '중랑구', 'dong_myun_ri': '묵동', 'latitude': '37.61385', 'longitude': '127.077568'},
        {'id': 128, 'hypermart_name': 'Emart', 'branch_code': '1140', 'name': '가든5점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '송파구', 'dong_myun_ri': '문정동', 'latitude': '37.478419', 'longitude': '127.11936'},
        {'id': 129, 'hypermart_name': 'Emart', 'branch_code': '1141', 'name': '파주운정점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '파주시', 'gu_gun': None, 'dong_myun_ri': '운정3동', 'latitude': '37.710863', 'longitude': '126.745495'},
        {'id': 130, 'hypermart_name': 'Emart', 'branch_code': '1142', 'name': '동구미점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '구미시', 'gu_gun': None, 'dong_myun_ri': '임수동', 'latitude': '36.106493', 'longitude': '128.404166'},
        {'id': 131, 'hypermart_name': 'Emart', 'branch_code': '1143', 'name': '대전터미널점', 'branch_type': 'Offline', 'do_name': '대전', 'si_name': '대전광역시', 'gu_gun': '동구', 'dong_myun_ri': '용전동', 'latitude': '36.350225', 'longitude': '127.437265'},
        {'id': 132, 'hypermart_name': 'Emart', 'branch_code': '1144', 'name': '마포점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '마포구', 'dong_myun_ri': '신공덕동', 'latitude': '37.542575', 'longitude': '126.953308'},
        {'id': 133, 'hypermart_name': 'Emart', 'branch_code': '1145', 'name': '서산점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '서산시', 'gu_gun': None, 'dong_myun_ri': '잠홍동', 'latitude': '36.788589', 'longitude': '126.478583'},
        {'id': 134, 'hypermart_name': 'Emart', 'branch_code': '1146', 'name': '펜타포트점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '천안시', 'gu_gun': '서북구', 'dong_myun_ri': '불당동', 'latitude': '36.798791', 'longitude': '127.101396'},
        {'id': 135, 'hypermart_name': 'Emart', 'branch_code': '1148', 'name': '하월곡점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '성북구', 'dong_myun_ri': '하월곡동', 'latitude': '37.605077', 'longitude': '127.031196'},
        {'id': 136, 'hypermart_name': 'Emart', 'branch_code': '1150', 'name': '화성봉담점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '화성시', 'gu_gun': '봉담읍', 'dong_myun_ri': None, 'latitude': '37.222606', 'longitude': '126.973215'},
        {'id': 137, 'hypermart_name': 'Emart', 'branch_code': '1152', 'name': '천안서북점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '천안시', 'gu_gun': '서북구', 'dong_myun_ri': '성성동', 'latitude': '36.838824', 'longitude': '127.122366'},
        {'id': 138, 'hypermart_name': 'Emart', 'branch_code': '1153', 'name': 'GT센터', 'branch_type': 'Online', 'do_name': '온라인', 'si_name': '온라인', 'gu_gun': '온라인', 'dong_myun_ri': '온라인', 'latitude': None, 'longitude': None},
        {'id': 139, 'hypermart_name': 'Emart', 'branch_code': '1154', 'name': '의정부점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '의정부시', 'gu_gun': '송산2동', 'dong_myun_ri': None, 'latitude': '37.743196', 'longitude': '127.102335'},
        {'id': 140, 'hypermart_name': 'Emart', 'branch_code': '1155', 'name': '별내점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '남양주시', 'gu_gun': '별내동', 'dong_myun_ri': None, 'latitude': '37.643106', 'longitude': '127.126042'},
        {'id': 141, 'hypermart_name': 'Emart', 'branch_code': '1156', 'name': '풍산점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '일산동구', 'dong_myun_ri': '풍동', 'latitude': '37.673796', 'longitude': '126.786752'},
        {'id': 142, 'hypermart_name': 'Emart', 'branch_code': '1157', 'name': '김포한강점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '김포시', 'gu_gun': '구래동', 'dong_myun_ri': None, 'latitude': '37.644631', 'longitude': '126.628289'},
        {'id': 143, 'hypermart_name': 'Emart', 'branch_code': '1158', 'name': '세종점', 'branch_type': 'Offline', 'do_name': '세종', 'si_name': '세종특별자치시', 'gu_gun': '남면', 'dong_myun_ri': '금송로', 'latitude': '36.471163', 'longitude': '127.250996'},
        {'id': 144, 'hypermart_name': 'Emart', 'branch_code': '1159', 'name': '킨텍스점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '일산서구', 'dong_myun_ri': '송포동', 'latitude': '37.66182', 'longitude': '126.744188'},
        {'id': 145, 'hypermart_name': 'Emart', 'branch_code': '1160', 'name': '광교점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '수원시', 'gu_gun': '영통구', 'dong_myun_ri': '원천동', 'latitude': '37.296596', 'longitude': '127.047711'},
        {'id': 146, 'hypermart_name': 'Emart', 'branch_code': '1161', 'name': '과천점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '과천시', 'gu_gun': None, 'dong_myun_ri': '별양동', 'latitude': '37.426431', 'longitude': '126.991909'},
        {'id': 147, 'hypermart_name': 'Emart', 'branch_code': '1162', 'name': '김해점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '김해시', 'gu_gun': None, 'dong_myun_ri':'외동', 'latitude': '35.228826', 'longitude': '128.872165'},
        {'id': 148, 'hypermart_name': 'Emart', 'branch_code': '1163', 'name': '의왕점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '의왕시', 'gu_gun': None, 'dong_myun_ri': '오전동', 'latitude': '37.350264', 'longitude': '126.973299'},
        {'id': 149, 'hypermart_name': 'Emart', 'branch_code': '1164', 'name': '광주점', 'branch_type': 'Offline', 'do_name': '광주', 'si_name': '광주광역시', 'gu_gun': '서구', 'dong_myun_ri': '화정1동', 'latitude': '35.159003', 'longitude': '126.882083'},
        {'id': 150, 'hypermart_name': 'Emart', 'branch_code': '1166', 'name': '신촌점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '마포구', 'dong_myun_ri': '노고산동', 'latitude': '37.555166', 'longitude': '126.936001'},
	    {'id': 151, 'hypermart_name': 'Emart', 'branch_code': '6100', 'name': 'LIFE컨테이너STF고양', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '덕양구','dong_myun_ri': '창릉동', 'latitude': '37.646976', 'longitude': '126.894833'},
        {'id': 152, 'hypermart_name': 'Emart', 'branch_code': '6101', 'name': 'LIFE컨테이너STF위례', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '하남시', 'gu_gun': None, 'dong_myun_ri': '거여2동', 'latitude': '37.480093', 'longitude': '127.14837'},
	    {'id': 153, 'hypermart_name': 'Emart', 'branch_code': '6400', 'name': 'PS STF코엑스점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '강남구', 'dong_myun_ri': '삼성1동', 'latitude': '37.511906', 'longitude': '127.059254'},
        {'id': 154, 'hypermart_name': 'Emart', 'branch_code': '6401', 'name': 'PS 두타몰점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '중구', 'dong_myun_ri': '광희동', 'latitude': '37.569128', 'longitude': '127.008732'},
        {'id': 155, 'hypermart_name': 'Emart', 'branch_code': '9737', 'name': '이마트몰복합몰', 'branch_type': 'Online', 'do_name': '온라인', 'si_name': '온라인', 'gu_gun': '온라인', 'dong_myun_ri': '온라인', 'latitude': None, 'longitude': None},
        {'id': 156, 'hypermart_name': 'Emart', 'branch_code': '9800', 'name': 'EM김포몰가상점', 'branch_type': 'Online', 'do_name': '온라인', 'si_name': '온라인', 'gu_gun': '온라인', 'dong_myun_ri': '온라인', 'latitude': None, 'longitude': None},
        {'id': 157, 'hypermart_name': 'Emart', 'branch_code': '9801', 'name': 'EM보정몰가상점', 'branch_type': 'Online', 'do_name': '온라인', 'si_name': '온라인', 'gu_gun': '온라인', 'dong_myun_ri': '온라인', 'latitude': None, 'longitude': None},
        {'id': 158, 'hypermart_name': 'Emart', 'branch_code': '9807', 'name': 'EM김포2몰가상점', 'branch_type': 'Online', 'do_name': '온라인', 'si_name': '온라인', 'gu_gun': '온라인', 'dong_myun_ri': '온라인', 'latitude': None, 'longitude': None},
        {'id': 159, 'hypermart_name': 'Lottemart', 'branch_code': '101', 'name': 'VIC금천점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '금천구', 'dong_myun_ri': None, 'latitude': '37.4653423', 'longitude': '126.895754'},
        {'id': 160, 'hypermart_name': 'Lottemart', 'branch_code': '103', 'name': 'VIC영등포점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '영등포구', 'dong_myun_ri': '영등포동', 'latitude': '37.5279427', 'longitude': '126.905011'},
        {'id': 161, 'hypermart_name': 'Lottemart', 'branch_code': '102', 'name': 'VIC신영통점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '화성시', 'gu_gun': None, 'dong_myun_ri': '반월동 ', 'latitude': '37.2360122', 'longitude': '127.066464'},
        {'id': 162, 'hypermart_name': 'Lottemart', 'branch_code': '104', 'name': 'VIC도봉점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '도봉구', 'dong_myun_ri': '방학동 ', 'latitude': '37.6677532', 'longitude': '127.045472'},
        {'id': 163, 'hypermart_name': 'Lottemart', 'branch_code': '106', 'name': 'VIC킨텍스점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '일산서구', 'dong_myun_ri': '대화동', 'latitude': '37.6770688', 'longitude': '126.752371'},
        {'id': 164, 'hypermart_name': 'Lottemart', 'branch_code': '111', 'name': 'marketD수원점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '수원시', 'gu_gun': '권선구', 'dong_myun_ri': '서둔동', 'latitude': '37.2666915', 'longitude': '126.995053'},
        {'id': 165, 'hypermart_name': 'Lottemart', 'branch_code': '200', 'name': '서울역점(위탁경영점)', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '중구', 'dong_myun_ri': '회현동', 'latitude': '37.5559951', 'longitude': '126.970351'},
        {'id': 166, 'hypermart_name': 'Lottemart', 'branch_code': '301', 'name': '강변점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '광진구', 'dong_myun_ri': '구의동', 'latitude': '37.5357773', 'longitude': '127.095724'},
        {'id': 167, 'hypermart_name': 'Lottemart', 'branch_code': '302', 'name': '잠실점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '송파구', 'dong_myun_ri': '잠실동', 'latitude': '37.5117881', 'longitude': '127.096233'},
        {'id': 168, 'hypermart_name': 'Lottemart', 'branch_code': '307', 'name': '중계점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '노원구', 'dong_myun_ri': '중계1동', 'latitude': '37.6467843', 'longitude': '127.070960'},
        {'id': 169, 'hypermart_name': 'Lottemart', 'branch_code': '312', 'name': '청량리점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '동대문구', 'dong_myun_ri': '전농1동', 'latitude': '37.5808435', 'longitude': '127.049097'},
        {'id': 170, 'hypermart_name': 'Lottemart', 'branch_code': '316', 'name': '삼양점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '강북구', 'dong_myun_ri': '삼양동', 'latitude': '37.6255945', 'longitude': '127.017824'},
        {'id': 171, 'hypermart_name': 'Lottemart', 'branch_code': '322', 'name': '송파점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '송파구', 'dong_myun_ri': '문정동', 'latitude': '37.4918471', 'longitude': '127.117708'},
        {'id': 172, 'hypermart_name': 'Lottemart', 'branch_code': '323', 'name': '행당역점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '성동구', 'dong_myun_ri': '행당동', 'latitude': '37.5568154', 'longitude': '127.028804'},
        {'id': 173, 'hypermart_name': 'Lottemart', 'branch_code': '328', 'name': '양평점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '영등포구', 'dong_myun_ri': '양평동', 'latitude': '37.5266250', 'longitude': '126.891325'},
        {'id': 174, 'hypermart_name': 'Lottemart', 'branch_code': '334', 'name': '월드타워점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '송파구', 'dong_myun_ri': '잠실6동', 'latitude': '37.5143514', 'longitude': '127.104812'},
        {'id': 175, 'hypermart_name': 'Lottemart', 'branch_code': '335', 'name': '금천점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '금천구', 'dong_myun_ri': None, 'latitude': '37.4610076', 'longitude': '126.897138'},
        {'id': 176, 'hypermart_name': 'Lottemart', 'branch_code': '340', 'name': '서초점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '서초구', 'dong_myun_ri': '서초동', 'latitude': '37.4907987', 'longitude': '127.005176'},
        {'id': 177, 'hypermart_name': 'Lottemart', 'branch_code': '342', 'name': '은평점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '은평구', 'dong_myun_ri': '진관동', 'latitude': '37.6388252', 'longitude': '126.918145'},
        {'id': 178, 'hypermart_name': 'Lottemart', 'branch_code': '360', 'name': 'Mealguru삼성점(롯데슈퍼)', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '강남구', 'dong_myun_ri': '대치동', 'latitude': '37.5113947', 'longitude': '127.065652'},
        {'id': 179, 'hypermart_name': 'Lottemart', 'branch_code': '402', 'name': '구리점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '구리시', 'gu_gun': None, 'dong_myun_ri': '인창동', 'latitude': '37.6126853', 'longitude': '127.141329'},
        {'id': 180, 'hypermart_name': 'Lottemart', 'branch_code': '403', 'name': '주엽점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '일산서구', 'dong_myun_ri': '주엽동', 'latitude': '37.6735425', 'longitude': '126.755332'},
        {'id': 181, 'hypermart_name': 'Lottemart', 'branch_code': '404', 'name': '부평역점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '부평구', 'dong_myun_ri': '부평동', 'latitude': '37.4898533', 'longitude': '126.723308'},
        {'id': 182, 'hypermart_name': 'Lottemart', 'branch_code': '406', 'name': '연수점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '연수구', 'dong_myun_ri': '청학동', 'latitude': '37.4192138', 'longitude': '126.675135'},	
        {'id': 183, 'hypermart_name': 'Lottemart', 'branch_code': '408', 'name': '화정점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '덕양구', 'dong_myun_ri': '화정2동', 'latitude': '37.6331622', 'longitude': '126.831520'},
        {'id': 184, 'hypermart_name': 'Lottemart', 'branch_code': '409', 'name': '의왕점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '의왕시', 'gu_gun': None, 'dong_myun_ri': '내손동', 'latitude': '37.3806616', 'longitude': '126.972814'},
        {'id': 185, 'hypermart_name': 'Lottemart', 'branch_code': '410', 'name': '오산점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '오산시', 'gu_gun': None, 'dong_myun_ri': '중앙동', 'latitude': '37.1495896', 'longitude': '127.073108'},
        {'id': 186, 'hypermart_name': 'Lottemart', 'branch_code': '411', 'name': '천천점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '수원시', 'gu_gun': '장안구', 'dong_myun_ri': '천천동', 'latitude': '37.2963329', 'longitude': '126.982493'},
        {'id': 187, 'hypermart_name': 'Lottemart', 'branch_code': '415', 'name': '안산점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '안산시', 'gu_gun': '상록구', 'dong_myun_ri': '성포동', 'latitude': '37.3181709', 'longitude': '126.846146'},
        {'id': 188, 'hypermart_name': 'Lottemart', 'branch_code': '417', 'name': '안성점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '안성시', 'gu_gun': None, 'dong_myun_ri': '서동대로', 'latitude': '37.0078327', 'longitude': '127.199351'},
        {'id': 189, 'hypermart_name': 'Lottemart', 'branch_code': '418', 'name': '삼산점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '부평구', 'dong_myun_ri': '삼산동', 'latitude': '37.5080281', 'longitude': '126.732055'},
        {'id': 190, 'hypermart_name': 'Lottemart', 'branch_code': '422', 'name': '이천점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '이천시', 'gu_gun': None, 'dong_myun_ri': None, 'latitude': '37.2746855', 'longitude': '127.454046'},
        {'id': 191, 'hypermart_name': 'Lottemart', 'branch_code': '424', 'name': '영종도점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '중구', 'dong_myun_ri': '운서동', 'latitude': '37.4926621', 'longitude': '126.491291'},
        {'id': 192, 'hypermart_name': 'Lottemart', 'branch_code': '426', 'name': '부평점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '부평구', 'dong_myun_ri': '산곡동', 'latitude': '37.5055515', 'longitude': '126.703686'},
        {'id': 193, 'hypermart_name': 'Lottemart', 'branch_code': '430', 'name': '장암점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '의정부시', 'gu_gun': None, 'dong_myun_ri': '장암동', 'latitude': '37.7233715', 'longitude': '127.053122'},
        {'id': 194, 'hypermart_name': 'Lottemart', 'branch_code': '433', 'name': '검단점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '서구', 'dong_myun_ri': '검단1동', 'latitude': '37.5944727', 'longitude': '126.664429'},
        {'id': 195, 'hypermart_name': 'Lottemart', 'branch_code': '435', 'name': '동두천점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '동두천시', 'gu_gun': None, 'dong_myun_ri': '송내동', 'latitude': '37.8806433', 'longitude': '127.053272'},
        {'id': 196, 'hypermart_name': 'Lottemart', 'branch_code': '436', 'name': '평택점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '평택시', 'gu_gun': None, 'dong_myun_ri': '합정동', 'latitude': '36.9885163', 'longitude': '127.111806'},
        {'id': 197, 'hypermart_name': 'Lottemart', 'branch_code': '441', 'name': '김포공항점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '강서구', 'dong_myun_ri': '방화2동', 'latitude': '37.5637251', 'longitude': '126.803815'},
        {'id': 198, 'hypermart_name': 'Lottemart', 'branch_code': '446', 'name': '롯데몰수지점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '용인시', 'gu_gun': '수지구', 'dong_myun_ri': '성복동', 'latitude': '37.3129494', 'longitude': '127.081257'},
        {'id': 199, 'hypermart_name': 'Lottemart', 'branch_code': '453', 'name': '마석점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '남양주시', 'gu_gun': None, 'dong_myun_ri': None, 'latitude': '37.6504391', 'longitude': '127.309487'},
        {'id': 200, 'hypermart_name': 'Lottemart', 'branch_code': '455', 'name': '고양점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '덕양구', 'dong_myun_ri': '행신3동', 'latitude': '37.6252582', 'longitude': '126.836162'},
        {'id': 201, 'hypermart_name': 'Lottemart', 'branch_code': '456', 'name': '시화점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '시흥시', 'gu_gun': None, 'dong_myun_ri': '정왕동', 'latitude': '37.3370901', 'longitude': '126.729745'},
        {'id': 202, 'hypermart_name': 'Lottemart', 'branch_code': '457', 'name': '덕소점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '남양주시', 'gu_gun': None, 'dong_myun_ri': '덕소리', 'latitude': '37.5848745', 'longitude': '127.214704'},
        {'id': 203, 'hypermart_name': 'Lottemart', 'branch_code': '458', 'name': '권선점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '수원시', 'gu_gun': '권선구', 'dong_myun_ri': '권선2동', 'latitude': '37.2502431', 'longitude': '127.034654'},
        {'id': 204, 'hypermart_name': 'Lottemart', 'branch_code': '459', 'name': '시흥점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '시흥시', 'gu_gun': None, 'dong_myun_ri': '대야동', 'latitude': '37.4431488', 'longitude': '126.791910'},
        {'id': 205, 'hypermart_name': 'Lottemart', 'branch_code': '461', 'name': '청라점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '서구', 'dong_myun_ri': '경서동', 'latitude': '37.5314163', 'longitude': '126.648695'},
        {'id': 206, 'hypermart_name': 'Lottemart', 'branch_code': '462', 'name': '수원점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '수원시', 'gu_gun': '권선구', 'dong_myun_ri': '서둔동', 'latitude': '37.2637713', 'longitude': '126.995836'},
        {'id': 207, 'hypermart_name': 'Lottemart', 'branch_code': '463', 'name': '광교점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '수원시', 'gu_gun': '영통구', 'dong_myun_ri': '원천동', 'latitude': '37.2904580', 'longitude': '127.050964'},
        {'id': 208, 'hypermart_name': 'Lottemart', 'branch_code': '464', 'name': '상록점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '안산시', 'gu_gun': '상록구', 'dong_myun_ri': '본오동', 'latitude': '37.2967826', 'longitude': '126.861992'},
        {'id': 209, 'hypermart_name': 'Lottemart', 'branch_code': '465', 'name': '송도점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '연수구', 'dong_myun_ri': '송도1동', 'latitude': '37.3885440', 'longitude': '126.642844'},
        {'id': 210, 'hypermart_name': 'Lottemart', 'branch_code': '467', 'name': '평촌점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '안양시', 'gu_gun': '동안구', 'dong_myun_ri': '호계동', 'latitude': '37.3897961', 'longitude': '126.949829'},
        {'id': 211, 'hypermart_name': 'Lottemart', 'branch_code': '468', 'name': '신갈점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '용인시', 'gu_gun': '기흥구', 'dong_myun_ri': '신갈동', 'latitude': '37.2725357', 'longitude': '127.108245'},
        {'id': 212, 'hypermart_name': 'Lottemart', 'branch_code': '469', 'name': '계양점', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '계양구', 'dong_myun_ri': '계산동', 'latitude': '37.5408769', 'longitude': '126.736084'},
        {'id': 213, 'hypermart_name': 'Lottemart', 'branch_code': '470', 'name': '영통점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '수원시', 'gu_gun': '영통구', 'dong_myun_ri': '영통동', 'latitude': '37.2528829', 'longitude': '127.071599'},
        {'id': 214, 'hypermart_name': 'Lottemart', 'branch_code': '471', 'name': '판교점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '성남시', 'gu_gun': '분당구', 'dong_myun_ri': '삼평동', 'latitude': '37.3960187', 'longitude': '127.113278'},
        {'id': 215, 'hypermart_name': 'Lottemart', 'branch_code': '473', 'name': '경기양평점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '양평군', 'gu_gun': '양평읍', 'dong_myun_ri': '남북로', 'latitude': '37.4896398', 'longitude': '127.502693'},
        {'id': 216, 'hypermart_name': 'Lottemart', 'branch_code': '475', 'name': '선부점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '안산시', 'gu_gun': '단원구', 'dong_myun_ri': '선부동', 'latitude': '37.3415739', 'longitude': '126.815374'},
        {'id': 217, 'hypermart_name': 'Lottemart', 'branch_code': '476', 'name': '시흥배곧점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '시흥시', 'gu_gun': None, 'dong_myun_ri': None, 'latitude': '37.3691539', 'longitude': '126.731384'},
        {'id': 218, 'hypermart_name': 'Lottemart', 'branch_code': '479', 'name': '김포한강점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '김포시', 'gu_gun': None, 'dong_myun_ri': '장기동', 'latitude': '37.6406408', 'longitude': '126.677416'},
        {'id': 219, 'hypermart_name': 'Lottemart', 'branch_code': '495', 'name': '인천터미널점(위탁경영점)', 'branch_type': 'Offline', 'do_name': '인천', 'si_name': '인천광역시', 'gu_gun': '남구', 'dong_myun_ri': None, 'latitude': '37.4410521', 'longitude': '126.701390'},
        {'id': 220, 'hypermart_name': 'Lottemart', 'branch_code': '501', 'name': '청주점', 'branch_type': 'Offline', 'do_name': '충청북도', 'si_name': '청주시', 'gu_gun': '흥덕구', 'dong_myun_ri': '가경동', 'latitude': '36.6275755', 'longitude': '127.431608'},
        {'id': 221, 'hypermart_name': 'Lottemart', 'branch_code': '504', 'name': '서대전점', 'branch_type': 'Offline', 'do_name': '대전', 'si_name': '대전광역시', 'gu_gun': '유성구', 'dong_myun_ri': '원내동', 'latitude': '36.3006266', 'longitude': '127.324950'},
        {'id': 222, 'hypermart_name': 'Lottemart', 'branch_code': '505', 'name': '충주점', 'branch_type': 'Offline', 'do_name': '충청북도', 'si_name': '충주시', 'gu_gun': None, 'dong_myun_ri': '칠금동', 'latitude': '36.9817004', 'longitude': '127.914280'},
        {'id': 223, 'hypermart_name': 'Lottemart', 'branch_code': '506', 'name': '서산점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '서산시', 'gu_gun': None, 'dong_myun_ri': '예천동', 'latitude': '36.7736707', 'longitude': '126.439501'},
        {'id': 224, 'hypermart_name': 'Lottemart', 'branch_code': '507', 'name': '성정점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '천안시', 'gu_gun': '서북구', 'dong_myun_ri': '성정2동', 'latitude': '36.8263637', 'longitude': '127.139931'},
        {'id': 225, 'hypermart_name': 'Lottemart', 'branch_code': '508', 'name': '대덕점', 'branch_type': 'Offline', 'do_name': '대전', 'si_name': '대전광역시', 'gu_gun': '유성구', 'dong_myun_ri': '관평동', 'latitude': '36.4271222', 'longitude': '127.386853'},
        {'id': 226, 'hypermart_name': 'Lottemart', 'branch_code': '509', 'name': '제천점', 'branch_type': 'Offline', 'do_name': '충청북도', 'si_name': '제천시', 'gu_gun': None, 'dong_myun_ri': None, 'latitude': '37.1402157', 'longitude': '128.200747'},
        {'id': 227, 'hypermart_name': 'Lottemart', 'branch_code': '512', 'name': '아산터미널점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '아산시', 'gu_gun': None, 'dong_myun_ri': '모종동', 'latitude': '36.7851964', 'longitude': '127.015898'},
        {'id': 228, 'hypermart_name': 'Lottemart', 'branch_code': '513', 'name': '서청주점', 'branch_type': 'Offline', 'do_name': '충청북도', 'si_name': '청주시', 'gu_gun': '흥덕구', 'dong_myun_ri': '비하동', 'latitude': '36.6454219', 'longitude': '127.421904'},
        {'id': 229, 'hypermart_name': 'Lottemart', 'branch_code': '515', 'name': '당진점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '당진시', 'gu_gun': None, 'dong_myun_ri': '원당동', 'latitude': '36.9060083', 'longitude': '126.646344'},
        {'id': 230, 'hypermart_name': 'Lottemart', 'branch_code': '516', 'name': '노은점', 'branch_type': 'Offline', 'do_name': '대전', 'si_name': '대전광역시', 'gu_gun': '유성구', 'dong_myun_ri': None, 'latitude': '36.3843949', 'longitude': '127.321557'},
        {'id': 231, 'hypermart_name': 'Lottemart', 'branch_code': '518', 'name': '홍성점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '', 'gu_gun': '홍성군', 'dong_myun_ri': '고암리', 'latitude': '36.6011274', 'longitude': '126.676300'},
        {'id': 232, 'hypermart_name': 'Lottemart', 'branch_code': '519', 'name': '상당점', 'branch_type': 'Offline', 'do_name': '충청북도', 'si_name': '청주시', 'gu_gun': '상당구', 'dong_myun_ri': '용암동', 'latitude': '36.6204780', 'longitude': '127.516071'},
        {'id': 233, 'hypermart_name': 'Lottemart', 'branch_code': '601', 'name': '울산점', 'branch_type': 'Offline', 'do_name': '울산', 'si_name': '울산광역시', 'gu_gun': '남구', 'dong_myun_ri': '달동', 'latitude': '35.5341171', 'longitude': '129.315715'},
        {'id': 234, 'hypermart_name': 'Lottemart', 'branch_code': '603', 'name': '사하점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '사하구', 'dong_myun_ri': '장림1동', 'latitude': '35.0852540', 'longitude': '128.972118'},
        {'id': 235, 'hypermart_name': 'Lottemart', 'branch_code': '605', 'name': '화명점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '북구', 'dong_myun_ri': '화명동', 'latitude': '35.2353171', 'longitude': '129.013122'},
        {'id': 236, 'hypermart_name': 'Lottemart', 'branch_code': '607', 'name': '마산점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '창원시', 'gu_gun': '마산합포구', 'dong_myun_ri': '창포동', 'latitude': '35.1815419', 'longitude': '128.560691'},
        {'id': 237, 'hypermart_name': 'Lottemart', 'branch_code': '608', 'name': '통영점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '통영시', 'gu_gun': None, 'dong_myun_ri': '무전동', 'latitude': '34.8608234', 'longitude': '128.428274'},
        {'id': 238, 'hypermart_name': 'Lottemart', 'branch_code': '609', 'name': '장유점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '김해시', 'gu_gun': None, 'dong_myun_ri': '장유면', 'latitude': '35.1932164', 'longitude': '128.801310'},
        {'id': 239, 'hypermart_name': 'Lottemart', 'branch_code': '610', 'name': '웅상점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '양산시', 'gu_gun': None, 'dong_myun_ri': '삼호동', 'latitude': '35.4126931', 'longitude': '129.166885'},
        {'id': 240, 'hypermart_name': 'Lottemart', 'branch_code': '611', 'name': '진해점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '창원시', 'gu_gun': '진해구', 'dong_myun_ri': '석동', 'latitude': '35.1583241', 'longitude': '128.698728'},
        {'id': 241, 'hypermart_name': 'Lottemart', 'branch_code': '612', 'name': '사상점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '사상구', 'dong_myun_ri': '엄궁동', 'latitude': '35.1277834', 'longitude': '128.968355'},
        {'id': 242, 'hypermart_name': 'Lottemart', 'branch_code': '613', 'name': '구미점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '구미시', 'gu_gun': None, 'dong_myun_ri': '신평동', 'latitude': '36.1168341', 'longitude': '128.366130'},
        {'id': 243, 'hypermart_name': 'Lottemart', 'branch_code': '614', 'name': '진장점', 'branch_type': 'Offline', 'do_name': '울산', 'si_name': '울산광역시', 'gu_gun': '북구', 'dong_myun_ri': '효문동', 'latitude': '35.5762413', 'longitude': '129.358992'},
        {'id': 244, 'hypermart_name': 'Lottemart', 'branch_code': '616', 'name': '창원중앙점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '창원시', 'gu_gun': '성산구', 'dong_myun_ri': '중앙동', 'latitude': '35.2255842', 'longitude': '128.680215'},
        {'id': 245, 'hypermart_name': 'Lottemart', 'branch_code': '618', 'name': '동래점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '동래구', 'dong_myun_ri': '온천2동', 'latitude': '35.2125007', 'longitude': '129.077515'},
        {'id': 246, 'hypermart_name': 'Lottemart', 'branch_code': '620', 'name': '시티세븐점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '창원시', 'gu_gun': '의창구', 'dong_myun_ri': '두대동', 'latitude': '35.2404457', 'longitude': '128.653110'},
        {'id': 247, 'hypermart_name': 'Lottemart', 'branch_code': '623', 'name': '포항점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '포항시', 'gu_gun': '남구', 'dong_myun_ri': '지곡동', 'latitude': '36.0278074', 'longitude': '129.324656'},
        {'id': 248, 'hypermart_name': 'Lottemart', 'branch_code': '626', 'name': '부산점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '부산진구', 'dong_myun_ri': '부암1동', 'latitude': '35.1635037', 'longitude': '129.049344'},
        {'id': 249, 'hypermart_name': 'Lottemart', 'branch_code': '629', 'name': '대구율하점', 'branch_type': 'Offline', 'do_name': '대구', 'si_name': '대구광역시', 'gu_gun': '동구', 'dong_myun_ri': '율하동', 'latitude': '35.8690301', 'longitude': '128.692745'},
        {'id': 250, 'hypermart_name': 'Lottemart', 'branch_code': '639', 'name': '삼계점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '창원시', 'gu_gun': '마산회원구', 'dong_myun_ri': None, 'latitude': '35.2331354', 'longitude': '128.504176'},
        {'id': 251, 'hypermart_name': 'Lottemart', 'branch_code': '642', 'name': '김해점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '김해시', 'gu_gun': None, 'dong_myun_ri': '부원동', 'latitude': '35.2257578', 'longitude': '128.881934'},
        {'id': 252, 'hypermart_name': 'Lottemart', 'branch_code': '643', 'name': '양덕점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '창원시', 'gu_gun': '마산회원구', 'dong_myun_ri': '양덕2동', 'latitude': '35.2234816', 'longitude': '128.585201'},
        {'id': 253, 'hypermart_name': 'Lottemart', 'branch_code': '645', 'name': '거제점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '거제시', 'gu_gun': None, 'dong_myun_ri': '옥포동', 'latitude': '34.8896548', 'longitude': '128.69031'},
        {'id': 254, 'hypermart_name': 'Lottemart', 'branch_code': '647', 'name': '김천점', 'branch_type': 'Offline', 'do_name': '경상북도', 'si_name': '김천시', 'gu_gun': None, 'dong_myun_ri': '신음동', 'latitude': '36.1365424', 'longitude': '128.117490'},
        {'id': 255, 'hypermart_name': 'Lottemart', 'branch_code': '648', 'name': '진주점', 'branch_type': 'Offline', 'do_name': '경상남도', 'si_name': '진주시', 'gu_gun': None, 'dong_myun_ri': None, 'latitude': '35.1803717', 'longitude': '128.139942'},
        {'id': 256, 'hypermart_name': 'Lottemart', 'branch_code': '655', 'name': '광복점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '중구', 'dong_myun_ri': '중앙동', 'latitude': '35.0975382', 'longitude': '129.036382'},
        {'id': 257, 'hypermart_name': 'Lottemart', 'branch_code': '658', 'name': '동부산점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '기장군', 'dong_myun_ri': '기장읍', 'latitude': '35.1927154', 'longitude': '129.211294'},
        {'id': 258, 'hypermart_name': 'Lottemart', 'branch_code': '701', 'name': '상무점', 'branch_type': 'Offline', 'do_name': '광주', 'si_name': '광주광역시', 'gu_gun': '서구', 'dong_myun_ri': '치평동', 'latitude': '35.1525567', 'longitude': '126.852410'},
        {'id': 259, 'hypermart_name': 'Lottemart', 'branch_code': '702', 'name': '익산점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '익산시', 'gu_gun': None, 'dong_myun_ri': '영등동', 'latitude': '35.9593077', 'longitude': '126.974199'},
        {'id': 260, 'hypermart_name': 'Lottemart', 'branch_code': '703', 'name': '목포점', 'branch_type': 'Offline', 'do_name': '전라남도', 'si_name': '목포시', 'gu_gun': None, 'dong_myun_ri': '상동', 'latitude': '34.7974016', 'longitude': '126.429467'},
        {'id': 261, 'hypermart_name': 'Lottemart', 'branch_code': '704', 'name': '첨단점', 'branch_type': 'Offline', 'do_name': '광주', 'si_name': '광주광역시', 'gu_gun': '광산구', 'dong_myun_ri': '쌍암동', 'latitude': '35.2165314', 'longitude': '126.852029'},
        {'id': 262, 'hypermart_name': 'Lottemart', 'branch_code': '705', 'name': '여수점', 'branch_type': 'Offline', 'do_name': '전라남도', 'si_name': '여수시', 'gu_gun': None, 'dong_myun_ri': '국동', 'latitude': '34.7285713', 'longitude': '127.721912'},
        {'id': 263, 'hypermart_name': 'Lottemart', 'branch_code': '706', 'name': '월드컵점', 'branch_type': 'Offline', 'do_name': '광주', 'si_name': '광주광역시', 'gu_gun': '서구', 'dong_myun_ri': '풍암동', 'latitude': '35.1331673', 'longitude': '126.876768'},
        {'id': 264, 'hypermart_name': 'Lottemart', 'branch_code': '707', 'name': '군산점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '군산시', 'gu_gun': None, 'dong_myun_ri': '수송동', 'latitude': '35.9651130', 'longitude': '126.716709'},
        {'id': 265, 'hypermart_name': 'Lottemart', 'branch_code': '708', 'name': '전주점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '전주시', 'gu_gun': '완산구', 'dong_myun_ri': '효자동', 'latitude': '35.8165671', 'longitude': '127.102464'},
        {'id': 266, 'hypermart_name': 'Lottemart', 'branch_code': '709', 'name': '정읍점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '정읍시', 'gu_gun': None, 'dong_myun_ri': '농소동', 'latitude': '35.5799668', 'longitude': '126.839708'},
        {'id': 267, 'hypermart_name': 'Lottemart', 'branch_code': '710', 'name': '여천점', 'branch_type': 'Offline', 'do_name': '전라남도', 'si_name': '여수시', 'gu_gun': None, 'dong_myun_ri': '선원동', 'latitude': '34.7770128', 'longitude': '127.651989'},
        {'id': 268, 'hypermart_name': 'Lottemart', 'branch_code': '712', 'name': '송천점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '전주시', 'gu_gun': '덕진구', 'dong_myun_ri': None, 'latitude': '35.8544035', 'longitude': '127.120092'},
        {'id': 269, 'hypermart_name': 'Lottemart', 'branch_code': '713', 'name': '남원점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '남원시', 'gu_gun': None, 'dong_myun_ri': '향교동', 'latitude': '35.4235441', 'longitude': '127.392469'},
        {'id': 270, 'hypermart_name': 'Lottemart', 'branch_code': '715', 'name': '수완점', 'branch_type': 'Offline', 'do_name': '광주', 'si_name': '광주광역시', 'gu_gun': '광산구', 'dong_myun_ri': '수완동', 'latitude': '35.1903895', 'longitude': '126.820635'},
        {'id': 271, 'hypermart_name': 'Lottemart', 'branch_code': '719', 'name': '나주점', 'branch_type': 'Offline', 'do_name': '전라남도', 'si_name': '나주시', 'gu_gun': None, 'dong_myun_ri': '송월동', 'latitude': '35.0134757', 'longitude': '126.712709'},
        {'id': 272, 'hypermart_name': 'Lottemart', 'branch_code': '724', 'name': '남악점', 'branch_type': 'Offline', 'do_name': '전라남도', 'si_name': '', 'gu_gun': '무안군', 'dong_myun_ri': '남악리', 'latitude': '34.8035879', 'longitude': '126.465033'},
        {'id': 273, 'hypermart_name': 'Lottemart', 'branch_code': '801', 'name': '원주점', 'branch_type': 'Offline', 'do_name': '강원도', 'si_name': '원주시', 'gu_gun': None, 'dong_myun_ri': '단계동', 'latitude': '37.3463791', 'longitude': '127.927537'},
        {'id': 274, 'hypermart_name': 'Lottemart', 'branch_code': '802', 'name': '춘천점', 'branch_type': 'Offline', 'do_name': '강원도', 'si_name': '춘천시', 'gu_gun': None, 'dong_myun_ri': '온의동', 'latitude': '37.8691967', 'longitude': '127.717953'},
        {'id': 275, 'hypermart_name': 'Lottemart', 'branch_code': '804', 'name': '석사점', 'branch_type': 'Offline', 'do_name': '강원도', 'si_name': '춘천시', 'gu_gun': None, 'dong_myun_ri': '석사동', 'latitude': '37.8684299', 'longitude': '127.759077'},
        {'id': 276, 'hypermart_name': 'Lottemart', 'branch_code': '852', 'name': '제주점', 'branch_type': 'Offline', 'do_name': '제주특별자치도', 'si_name': '제주시', 'gu_gun': None, 'dong_myun_ri': '노형동', 'latitude': '33.4826011', 'longitude': '126.481910'},
        {'id': 277, 'hypermart_name': 'Lottemart', 'branch_code': '309', 'name': '구로점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '구로구', 'dong_myun_ri': '구로동', 'latitude': '37.4986059', 'longitude': '126.872694'},
        {'id': 278, 'hypermart_name': 'Lottemart', 'branch_code': '343', 'name': 'TRU은평점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '은평구', 'dong_myun_ri': '진관동', 'latitude': '37.6389559', 'longitude': '126.917770'},
        {'id': 279, 'hypermart_name': 'Lottemart', 'branch_code': '401', 'name': '서현점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '성남시', 'gu_gun': '분당구', 'dong_myun_ri': '서현동', 'latitude': '37.3861972', 'longitude': '127.120551'},
        {'id': 280, 'hypermart_name': 'Lottemart', 'branch_code': '407', 'name': '의정부점', 'branch_type': 'Offline', 'do_name': '경기도 ', 'si_name': '의정부시', 'gu_gun': None, 'dong_myun_ri': '송산1동', 'latitude': '37.7429964', 'longitude': '127.084233'},
        {'id': 281, 'hypermart_name': 'Lottemart', 'branch_code': '472', 'name': '마장휴게소점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '이천시', 'gu_gun': None, 'dong_myun_ri': '마장면', 'latitude': '37.2643868', 'longitude': '127.406630'},
        {'id': 282, 'hypermart_name': 'Lottemart', 'branch_code': '503', 'name': '천안점', 'branch_type': 'Offline', 'do_name': '충청남도', 'si_name': '천안시', 'gu_gun': '서북구', 'dong_myun_ri': '쌍용동', 'latitude': '36.7998977', 'longitude': '127.117626'},
        {'id': 283, 'hypermart_name': 'Lottemart', 'branch_code': '637', 'name': '금정점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '금정구', 'dong_myun_ri': '부곡3동', 'latitude': '35.2392590', 'longitude': '129.091693'},
        {'id': 284, 'hypermart_name': 'Lottemart', 'branch_code': '660', 'name': '칠성점', 'branch_type': 'Offline', 'do_name': '대구', 'si_name': '대구광역시', 'gu_gun': '북구', 'dong_myun_ri': '칠성동', 'latitude': '35.8842245', 'longitude': '128.590935'},
        {'id': 285, 'hypermart_name': 'Lottemart', 'branch_code': '977', 'name': '의왕온라인센터', 'branch_type': 'Online', 'do_name': '온라인', 'si_name': '온라인', 'gu_gun': '온라인', 'dong_myun_ri': '온라인', 'latitude': None, 'longitude': None},
        {'id': 286, 'hypermart_name': 'Lottemart', 'branch_code': '978', 'name': '부산온라인센터', 'branch_type': 'Online', 'do_name': '온라인', 'si_name': '온라인', 'gu_gun': '온라인', 'dong_myun_ri': '온라인', 'latitude': None, 'longitude': None},
        {'id': 287, 'hypermart_name': 'Lottemart', 'branch_code': '985', 'name': '김포온라인센터', 'branch_type': 'Online', 'do_name': '온라인', 'si_name': '온라인', 'gu_gun': '온라인', 'dong_myun_ri': '온라인', 'latitude': None, 'longitude': None},
        {'id': 288, 'hypermart_name': 'Lottemart', 'branch_code': '449', 'name': '비바건강마켓 남양주진접점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '남양주시', 'gu_gun': None, 'dong_myun_ri': ' 진접읍 부평리', 'latitude': '37.7562265', 'longitude': '127.200309'},
        {'id': 289, 'hypermart_name': 'Emart', 'branch_code': '1167', 'name': '에코시티점', 'branch_type': 'Offline', 'do_name': '전라북도', 'si_name': '전주시', 'gu_gun': '덕진구', 'dong_myun_ri': None, 'latitude': '35.8734116', 'longitude': '127.123715'},
        {'id': 290, 'hypermart_name': 'Emart', 'branch_code': '8201', 'name': '몰리센트럴시티', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '서초구', 'dong_myun_ri': '반포동', 'latitude': '37.5043439', 'longitude': '127.003599'},
        {'id': 291, 'hypermart_name': 'Emart', 'branch_code': '8210', 'name': '몰리스STF하남점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '하남시', 'gu_gun': None, 'dong_myun_ri': '신장동', 'latitude': '37.5454092', 'longitude': '127.223737'},
        {'id': 292, 'hypermart_name': 'Emart', 'branch_code': '8214', 'name': '몰리스STF고양점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '고양시', 'gu_gun': '덕양구', 'dong_myun_ri': '동산동', 'latitude': '37.6476807', 'longitude': '126.896455'},
        {'id': 293, 'hypermart_name': 'Emart', 'branch_code': '8216', 'name': '몰리스STF부천옥길점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '부천시', 'gu_gun': None, 'dong_myun_ri': '옥길동', 'latitude': '37.4615688', 'longitude': '126.813955'},
        {'id': 294, 'hypermart_name': 'Emart', 'branch_code': '8217', 'name': '몰리스STF안성점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '안성시', 'gu_gun': None, 'dong_myun_ri': '진사리', 'latitude': '36.9943473', 'longitude': '127.147307'},
        {'id': 295, 'hypermart_name': 'Emart', 'branch_code': '8204', 'name': '몰리스T월평점', 'branch_type': 'Offline', 'do_name': '대전', 'si_name': '대전광역시', 'gu_gun': '서구', 'dong_myun_ri': '월평동', 'latitude': '36.3576768', 'longitude': '127.362976'},
        {'id': 296, 'hypermart_name': 'Emart', 'branch_code': '8209', 'name': '몰리스센텀점', 'branch_type': 'Offline', 'do_name': '부산', 'si_name': '부산광역시', 'gu_gun': '해운대구', 'dong_myun_ri': '우동', 'latitude': '35.1698887', 'longitude': '129.128295'},
        {'id': 297, 'hypermart_name': 'Emart', 'branch_code': '8215', 'name': '몰리스STF위례점', 'branch_type': 'Offline', 'do_name': '경기도', 'si_name': '하남시', 'gu_gun': None, 'dong_myun_ri': '학암동', 'latitude': '37.4801236', 'longitude': '127.148382'},
        {'id': 298, 'hypermart_name': 'Emart', 'branch_code': '1177', 'name': '쓱고우논현점', 'branch_type': 'Offline', 'do_name': '서울', 'si_name': '서울특별시', 'gu_gun': '강남구', 'dong_myun_ri': '논현동', 'latitude': '37.5078828', 'longitude': '127.023324'},
    ]
    
    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        # print('search:__init__')
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        pass

    def __del__(self):
        self.lst_hypermart_geo_info
        pass
