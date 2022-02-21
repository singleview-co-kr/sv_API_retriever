# -*- coding: utf-8 -*-
import os
import re
# pip install xlrd
import xlrd  # openpyxl does not support the old .xls file format
import csv
from datetime import datetime
# from edi_models import BranchType, HyperMartType, EdiDataType, ProgressStatus

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
class HyperMartType(models.IntegerChoices):
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


class ProgressStatus(models.IntegerChoices):
    # DENIED = 1, '거부'  # data file has been uploaded
    UPLOADED = 2, '업로드 완료'  # data file has been uploaded
    ON_TRANSFORMING = 3, '변환 중'  # data file is on transforming to csv
    TRANSFORMED = 4, '변환 완료'  # data file has been transformed to csv


class SvEdiExcel:
    N_MAX_LOOKUP_ROWS = 10
    __g_oActiveSheet = None
    __g_nEdiDataYear = None

    def set_edi_data_year(self, n_edi_data_year):
        self.__g_nEdiDataYear = n_edi_data_year

    def classify_mart(self, s_unzip_path, s_unzipped_file):
        dict_rst = {'b_err': True, 's_msg': None, 'dict_val': None}
        if s_unzip_path is None or s_unzipped_file is None:
            dict_rst['s_msg'] = 'invalid excel_filename'
            return dict_rst

        s_excel_filename = os.path.join(s_unzip_path, s_unzipped_file)
        # load edi excel file
        o_book = xlrd.open_workbook(s_excel_filename)
        self.__g_oActiveSheet = o_book.sheet_by_index(0)

        n_hyper_mart = HyperMartType.NOT_SURE
        n_edi_data_type = None
        n_edi_data_year = None  # for emart only
        if self.__is_emart():  # try emart
            # caution! create duplicated ActiveSheet
            o_excel = EmartEdiExcel()
            o_excel.set_edi_data_year(self.__g_nEdiDataYear)  # 개별 EDI 파일에 연도 추정 표시가 없으면 사용
            o_excel.load_file(s_unzip_path, s_unzipped_file, b_data_year_est=True)
            n_hyper_mart = HyperMartType.EMART
            n_edi_data_year = o_excel.get_edi_data_year()
            n_edi_data_type = o_excel.lookup_edi_data_type()
            del o_excel
        elif self.__is_lotte_mart():  # try lotte mart
            print('lotte mart detected')
            o_excel = LotteMartEdiExcel()
            o_excel.load_file(s_unzip_path, s_unzipped_file)
            n_hyper_mart = HyperMartType.LOTTEMART
            n_edi_data_year = o_excel.get_edi_data_year()
            n_edi_data_type = EdiDataType.QTY_AMNT
            del o_excel
        else:
            dict_rst['s_msg'] = 'invalid hypermart EDI file detected'
            # print(HyperMartType.HOMEPLUS)
            # print(HyperMartType.NOT_SURE)

        dict_rst['b_err'] = False
        dict_rst['dict_val'] = {'n_edi_data_year': n_edi_data_year,
                                'n_hyper_mart': n_hyper_mart,
                                'n_edi_data_type': n_edi_data_type,  # 이마트만 수량과 금액 파일을 분리함
                                'status': ProgressStatus.UPLOADED}
        return dict_rst

    def __is_emart(self):
        n_tried_row_cnt = 0
        for n_row_idx in range(self.__g_oActiveSheet.nrows):
            lst_col_info = self.__g_oActiveSheet.row_values(n_row_idx)
            if lst_col_info.pop(0) == '상품코드' and lst_col_info.pop(0) == '상품명' and \
                    lst_col_info.pop(0) == '점포코드' and lst_col_info.pop(0) == '점포명':
                return True  # 헤더를 확인했으므로 중단
            if n_tried_row_cnt > self.N_MAX_LOOKUP_ROWS:
                return False
            else:
                n_tried_row_cnt = n_tried_row_cnt + 1
        return False

    def __is_lotte_mart(self):
        n_tried_row_cnt = 0
        for n_row_idx in range(self.__g_oActiveSheet.nrows):
            lst_col_info = self.__g_oActiveSheet.row_values(n_row_idx)
            if lst_col_info.pop(0) == '매출정보 상품상세별 현황표':
                return True  # 헤더를 확인했으므로 중단
            if n_tried_row_cnt > self.N_MAX_LOOKUP_ROWS:
                return False
            else:
                n_tried_row_cnt = n_tried_row_cnt + 1
        return False


class LotteMartEdiExcel:
    __g_lstDefaultDataColumn = ['점포명', '점포코드', '상품코드', '상품명', '판매코드', '매출수량', '매출금액']
    __g_fAssumedAvgHyperMarginRate = 0.38  # 할인점 평균 마진율이 38%라고 가정

    def __new__(cls):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
        return super().__new__(cls)

    def __init__(self):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        self.__g_oActiveSheet = None
        self.__g_nDataYear = None
        self.__g_lstLogDate = None  # ['from yyyy-mm-dd', 'to yyyy-mm-dd']
        self.__g_nDataRowIdx = None
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        return self

    def load_file(self, s_unzip_path, s_unzipped_file):
        if s_unzip_path is None or s_unzipped_file is None:
            print('invalid excel_filename')
            return

        s_excel_filename = os.path.join(s_unzip_path, s_unzipped_file)
        o_book = xlrd.open_workbook(s_excel_filename)
        self.__g_oActiveSheet = o_book.sheet_by_index(0)

    def get_edi_data_year(self):
        s_pattern = r"\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])"
        for n_header_row_idx in range(7):  # range(self.__g_oActiveSheet.nrows):
            lst_col_info = self.__g_oActiveSheet.row_values(n_header_row_idx)
            lst_tmp_col_info = lst_col_info.copy()
            s_1st_col = lst_tmp_col_info.pop(0)
            if s_1st_col.find('조회기간') != -1:
                o_found_edi_year = re.finditer(s_pattern, s_1st_col)  # find all match in iterator
                lst_log_date = [x.group() for x in o_found_edi_year]
                break
        self.__g_nDataRowIdx = n_header_row_idx + 1  # 매출정보 상품상세별 현황표 줄과 [조회기간 ㅌㅌㅌㅌ] 줄 이후를 표시함

        try:
            dt_since_logdate = datetime.strptime(lst_log_date[0], '%Y-%m-%d')
        except ValueError:
            raise Exception('lotte mart since logdate is not correct')  # need user define Exception

        try:
            dt_to_logdate = datetime.strptime(lst_log_date[0], '%Y-%m-%d')
        except ValueError:
            raise Exception('lotte mart since logdate is not correct')  # need user define Exception

        # chek from year is same with to year
        if dt_since_logdate.year != dt_to_logdate.year or dt_since_logdate.month != dt_to_logdate.month:
            raise Exception('lotte mart; from from year month is different with to year month')

        self.__g_lstLogDate = lst_log_date
        self.__g_nDataYear = dt_to_logdate.year  # lst_to_date[0]
        return self.__g_nDataYear

    def to_csv(self, s_full_path_csv_file):
        """
        홈플 롯데 금액은 부가세 포함된 판매액, 할인점 평균 마진율이 38%라고 가정하고 이마트 EDI 부가세 제외 공급가로 표준화
        :param s_full_path_csv_file:
        :return:
        """
        # nHighestRow = oActiveSheet.nrows  # 마지막 행
        # nHighestColumn = oActiveSheet.ncols  # 마지막 컬럼
        if self.__g_nDataYear is None:
            print('LotteMartDataYear is none')
            return

        # https://datascienceschool.net/view-notebook/5524dd924b9e4a3399a33b5e70269458/
        lst_edi_data_body = []
        # 데이터 컬럼 헤더 찾기
        for n_data_header_row_idx in range(self.__g_nDataRowIdx, self.__g_oActiveSheet.nrows):
            lst_col_info = self.__g_oActiveSheet.row_values(n_data_header_row_idx)
            if '점포명' in lst_col_info and '상품명' in lst_col_info:
                if lst_col_info != self.__g_lstDefaultDataColumn:
                    print('weird lottemart data header')
                break  # 데이터 컬럼 헤더를 확인했으므로 중단

            if n_data_header_row_idx > 20:
                print('weird lottemart edi file')
                return

        s_since_logdate = self.__g_lstLogDate[0].replace('-', '')
        s_to_logdate = self.__g_lstLogDate[1].replace('-', '')

        # 데이터 본체를 CSV로 전환
        # 점포명,점포코드,상품코드,상품명,판매코드,매출수량,매출금액
        # ['VIC금천점', 101.0, '1000746119', '\xa0 유한덴탈케어 탄력초극세모', '8806006527563', 185.0, 1829150.0]  # , '']
        n_data_body_row_idx = n_data_header_row_idx + 1
        f_deduction_rate = 1 / 1.1 / (1+self.__g_fAssumedAvgHyperMarginRate)  # deduct VAT and mart margin
        for n_data_body_row_idx in range(n_data_body_row_idx, self.__g_oActiveSheet.nrows):
            lst_data_row = self.__g_oActiveSheet.row_values(n_data_body_row_idx)
            if lst_data_row[0] == '소계' or lst_data_row[0] == '합 계':
                continue
            try:
                lst_data_row[1] = int(lst_data_row[1])
                lst_data_row[3] = lst_data_row[3].replace(u'\xa0', '').lstrip()  # remove consequent white space
                lst_data_row[5] = int(lst_data_row[5])  # qty
                lst_data_row[6] = int(lst_data_row[6])  # original EDI amnt
                lst_data_row.append(s_since_logdate)  # lst_data_row[7] = s_since_logdate
                lst_data_row.append(s_to_logdate)
                lst_data_row.append(int(lst_data_row[6] * f_deduction_rate))  # standardized EDI amnt
            except Exception as e:
                print(e)
                print(lst_data_row)
                break

            lst_edi_data_body.append(lst_data_row)
            # if n_data_body_row_idx > 35:
            #    break
        # csv 파일 형식
        # 점포명,점포코드,상품코드,상품명,판매코드,매출수량,매출금액,로그시작일,로그종료일,이마트표준화금액
        try:
            with open(s_full_path_csv_file, "a+", newline="") as f:
                writer = csv.writer(f)
                for lst_row in lst_edi_data_body:
                    writer.writerow(lst_row)
        except Exception as e:
            print(e)
        del lst_edi_data_body
        return


class EmartEdiExcel:
    __g_dictDataType = {EdiDataType.QTY: 'qty', EdiDataType.AMNT: 'amnt'}
    __g_dictColTitleTranslation = {'상품코드': 'item_code', '상품명': 'item_name', '점포코드': 'shop_code', '점포명': 'shop_name'}

    def __new__(cls):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
        return super().__new__(cls)

    def __init__(self):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        self.__g_sDeterminedDataType = None
        self.__g_nDataYear = None
        self.__g_bCsvHeaderPrinted = False
        self.__g_oActiveSheet = None
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        return self

    # from openpyxl import load_workbook
    # data_only=Ture로 해줘야 수식이 아닌 값으로 받아온다.
    # load_wb = load_workbook("D:\이마트 200101_16.xls", data_only=True)
    # 시트 이름으로 불러오기
    # load_ws = load_wb['일자상품별상세리스트(0033200000228)']

    # 셀 주소로 값 출력
    # print(load_ws['A1'].value)

    # 셀 좌표로 값 출력
    # print(load_ws.cell(1, 2).value)
    def set_edi_data_year(self, n_edi_data_year):
        self.__g_nDataYear = n_edi_data_year

    def get_edi_data_year(self):
        return self.__g_nDataYear

    def set_emart_data_type(self, n_emart_data_type):
        self.__g_sDeterminedDataType = self.__g_dictDataType[n_emart_data_type]

    def load_file(self, s_unzip_path, s_unzipped_file, b_data_year_est=False):
        if s_unzip_path is None or s_unzipped_file is None:
            print('invalid excel_filename')
            return

        # lookup edi year info from each filename - begin
        if b_data_year_est:  # do not estimate data year by filename when transfer_excel_to_csv()
            s_pattern = r"(16|17|18|19|20|21|22|23|24|25|26)\d{2}"
            o_found_emart_edi_year = re.search(s_pattern, s_unzipped_file)
            if o_found_emart_edi_year:
                n_emart_edi_year_from_filename = int(o_found_emart_edi_year.group())  # 매칭된 문자열 # 2019
                # print(o_found_emart_edi_year)
                # print(o_found_emart_edi_year.start())  # 매칭된 문자열 시작 위치 # 0
                # print(o_found_emart_edi_year.end())  # 매칭된 문자열 종료 위치 # 2
                # print(o_found_emart_edi_year.span())  # 매칭된 문자열 시작,종료 위치 # (0, 2)
                # validate detected edi_data_year
                # eg., 1901 means 2019-01-XX, 2012 means 2020-12-XX, 2101 means 2021-01-XX
                if n_emart_edi_year_from_filename <= 2012 or n_emart_edi_year_from_filename > 2100:
                    n_real_year_2_digit = int(n_emart_edi_year_from_filename / 100)
                    n_emart_edi_year_from_filename = int('20' + str(n_real_year_2_digit))
                # prioritize yr info from filename, only if yr info could be detected on filename
                self.set_edi_data_year(n_emart_edi_year_from_filename)

        # lookup edi year info from each filename - end
        # load edi excel file
        s_excel_filename = os.path.join(s_unzip_path, s_unzipped_file)
        o_book = xlrd.open_workbook(s_excel_filename)
        self.__g_oActiveSheet = o_book.sheet_by_index(0)
        # print(o_book.sheet_names())
        # print(oActiveSheet.ncols)
        # print(oActiveSheet.nrows)
        # print(oActiveSheet.name)
        # print(oActiveSheet.row(5))

    def to_csv(self, s_full_path_csv_file):
        """
        이마트 금액은 공급가 표시(VAT제외)
        :param s_full_path_csv_file:
        :return:
        """
        # nHighestRow = oActiveSheet.nrows  # 마지막 행
        # nHighestColumn = oActiveSheet.ncols  # 마지막 컬럼
        if self.__g_nDataYear is None:
            print('EmartDataYear is none')
            return

        # https://datascienceschool.net/view-notebook/5524dd924b9e4a3399a33b5e70269458/
        # 헤더 찾기
        dict_default_col_info = {}
        dict_data_col_info = {}  # 연월 데이터 컬럼의 위치 저장
        for n_header_row_idx in range(self.__g_oActiveSheet.nrows):
            lst_col_info = self.__g_oActiveSheet.row_values(n_header_row_idx)
            lst_tmp_col_info = lst_col_info.copy()
            if lst_tmp_col_info.pop(0) == '상품코드' and lst_tmp_col_info.pop(0) == '상품명' and \
                    lst_tmp_col_info.pop(0) == '점포코드' and lst_tmp_col_info.pop(0) == '점포명':
                del lst_tmp_col_info  # 헤더를 확인했으므로 임시 복사한 행 정보를 제거함

                # 추출 컬럼 분석
                n_col_seq = 0
                for sColTitle in lst_col_info:
                    if sColTitle.find('월') != -1 and sColTitle.find('일') != -1:
                        lst_tmp_column = sColTitle.split('월 ')
                        s_formatted_month = lst_tmp_column.pop(0).rjust(2, '0')
                        s_formatted_day = lst_tmp_column.pop(0).replace('일', '').rjust(2, '0')
                        dict_data_col_info[n_col_seq] = s_formatted_month + s_formatted_day
                    elif sColTitle.find('합계') != -1 or sColTitle.find('평균') != -1:
                        pass
                    else:  # 기본 추출 컬럼 식별
                        dict_default_col_info[n_col_seq] = sColTitle
                    n_col_seq = n_col_seq + 1
                break

        f = open(s_full_path_csv_file, 'a+', newline=''""'')
        writer = csv.writer(f)
        lst_header_col_info = []
        for nIdx, sValue in dict_default_col_info.items():
            lst_header_col_info.append(self.__g_dictColTitleTranslation[sValue])
        lst_header_col_info.append(self.__g_sDeterminedDataType)
        lst_header_col_info.append('log_date')
        if not self.__g_bCsvHeaderPrinted:
            self.__g_bCsvHeaderPrinted = True

        # lst_default_col_key = list(dict_default_col_info.keys())
        # lst_data_col_key =  list(dict_data_col_info.keys())
        # 데이터 한줄씩 읽기;
        for n_data_row_idx in range(n_header_row_idx + 1, self.__g_oActiveSheet.nrows):
            lst_col_info = self.__g_oActiveSheet.row_values(n_data_row_idx)
            n_col_seq = 0
            lst_single_row = []
            # set default column
            for s_col in lst_col_info:
                if n_col_seq in dict_default_col_info:  # lst_default_col_key:
                    lst_single_row.append(s_col)
                else:
                    break
                n_col_seq += 1
            # set data column
            n_col_seq = 0
            for sCol in lst_col_info:
                if n_col_seq in dict_data_col_info:  # lst_data_col_key:
                    # 필수 기본 컬럼에 날짜와 수량 컬럼 추가
                    s_log_date = str(self.__g_nDataYear) + dict_data_col_info[n_col_seq]
                    n_perf_val = int(sCol)
                    if n_perf_val != 0:  # minus qty amnt exists
                        s_qty = str(n_perf_val)
                        lst_single_row.append(s_qty)
                        lst_single_row.append(s_log_date)
                        # 작성된 single rec을 CSV로 변형하여 출력 list에 추가
                        # lstCsvData.append(','.join(lstSingleRow))
                        # 필수 기본 컬럼에 날짜와 수량 컬럼 제거
                        writer.writerow(lst_single_row)
                        del lst_single_row[4:6]
                else:
                    pass
                n_col_seq += 1
            # if nRowIdx == 8: # break for shorten test
            #    break
        f.close
        return

    def lookup_edi_data_type(self):
        dict_edifile_data_type_col_info = {}  # emart edi 파일이 수량인지 금액인 판별하기 위한 [합계] [평균] 컬럼 위치 저장
        # lst_edifile_date_col_info = []  # 업로드된 emart edi 파일 중 중복된 일자가 있는지 검토

        for n_row_idx in range(self.__g_oActiveSheet.nrows):
            lst_col_info = self.__g_oActiveSheet.row_values(n_row_idx)
            lst_tmp_col_info = lst_col_info.copy()
            if lst_tmp_col_info.pop(0) == '상품코드' and lst_tmp_col_info.pop(0) == '상품명' and \
                    lst_tmp_col_info.pop(0) == '점포코드' and lst_tmp_col_info.pop(0) == '점포명':
                del lst_tmp_col_info  # 헤더를 확인했으므로 임시 복사한 행 정보를 제거함

                # 추출 컬럼 분석
                n_col_seq = 0
                for s_col_title in lst_col_info:
                    if s_col_title.find('합계') != -1 or s_col_title.find('평균') != -1:
                        dict_edifile_data_type_col_info[s_col_title] = n_col_seq
                    """else:  # edi date 추출 컬럼 식별
                        if s_col_title.find('상품코드') == -1 and s_col_title.find('상품명') == -1 and \
                                s_col_title.find('점포코드') == -1 and s_col_title.find('점포명') == -1:
                            lst_date = s_col_title.split('월')
                            n_col_date = int(str(self.__g_nDataYear) + str(lst_date[0]).zfill(2) + \
                                         str(lst_date[1]).replace('일', '').strip().zfill(2))
                            # print(s_col_date)  # day
                            lst_edifile_date_col_info.append(n_col_date)
                        pass"""
                    n_col_seq = n_col_seq + 1
                break
        # 5개의 [합계] [평균] 값을 통해 edi 파일이 수량인지 금액인지 판단
        n_detect_cnt = 0
        dict_vote = {'qty': 0, 'amnt': 0}
        for n_data_row_idx in range(n_row_idx + 1, self.__g_oActiveSheet.nrows):
            lst_col_info = self.__g_oActiveSheet.row_values(n_data_row_idx)
            if n_detect_cnt < 5:
                for s_col_title in dict_edifile_data_type_col_info:
                    if s_col_title == '합계':
                        n_col_seq = dict_edifile_data_type_col_info[s_col_title]
                        n_sum = lst_col_info[n_col_seq]
                        if n_sum <= 0:  # pass if zero; no information to decide
                            continue
                        else:
                            # print('함계:' + str(lst_col_info[n_col_seq]))
                            if n_sum < 1500:
                                dict_vote['qty'] = dict_vote['qty'] + 1
                            else:
                                dict_vote['amnt'] = dict_vote['amnt'] + 1
                            n_detect_cnt = n_detect_cnt + 1
                    elif s_col_title == '평균':
                        # n_col_seq = dict_edifile_data_type_col_info[s_col_title]
                        # print('평균:' + str(lst_col_info[n_col_seq]))
                        pass
            else:
                break
        # dict_edifile_data_type_col_info.clear()
        del dict_edifile_data_type_col_info
        # dict_edifile_lookup_rst = {'n_emart_edi_data_type': None, 'lst_date_range': lst_edifile_date_col_info}
        # print(lst_edifile_date_col_info)

        if dict_vote['qty'] > dict_vote['amnt']:
            return EdiDataType.QTY
        elif dict_vote['qty'] < dict_vote['amnt']:
            return EdiDataType.AMNT
        else:
            return EdiDataType.NOT_SURE

    def get_edi_data_date_lst(self):
        dict_edifile_data_type_col_info = {}  # emart edi 파일이 수량인지 금액인 판별하기 위한 [합계] [평균] 컬럼 위치 저장
        lst_edifile_date_col_info = []  # 업로드된 emart edi 파일 중 중복된 일자가 있는지 검토

        for n_row_idx in range(self.__g_oActiveSheet.nrows):
            lst_col_info = self.__g_oActiveSheet.row_values(n_row_idx)
            lst_tmp_col_info = lst_col_info.copy()
            if lst_tmp_col_info.pop(0) == '상품코드' and lst_tmp_col_info.pop(0) == '상품명' and \
                    lst_tmp_col_info.pop(0) == '점포코드' and lst_tmp_col_info.pop(0) == '점포명':
                del lst_tmp_col_info  # 헤더를 확인했으므로 임시 복사한 행 정보를 제거함

                # 추출 컬럼 분석
                n_col_seq = 0
                for s_col_title in lst_col_info:
                    if s_col_title.find('합계') != -1 or s_col_title.find('평균') != -1:
                        dict_edifile_data_type_col_info[s_col_title] = n_col_seq
                    else:  # edi date 추출 컬럼 식별
                        if s_col_title.find('상품코드') == -1 and s_col_title.find('상품명') == -1 and \
                                s_col_title.find('점포코드') == -1 and s_col_title.find('점포명') == -1:
                            lst_date = s_col_title.split('월')
                            n_col_date = int(str(self.__g_nDataYear) + str(lst_date[0]).zfill(2) + \
                                             str(lst_date[1]).replace('일', '').strip().zfill(2))
                            # print(s_col_date)  # day
                            lst_edifile_date_col_info.append(n_col_date)
                        pass
                    n_col_seq = n_col_seq + 1
                break
        return lst_edifile_date_col_info
