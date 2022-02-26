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
import csv
from pathlib import Path
import pymysql

# singleview library
if __name__ == 'edi_extract': # for console debugging
    import edi_model
    import sv_hypermart_model
else: # for platform running
    pass


class TransformEdiExcel:
    __g_dictEdiCsvFileMap = {}  # internal memory to handle emart edi file type

    def __new__(cls):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)  # turn to decorator
        return super().__new__(cls)

    def __init__(self):
        # print(__file__ + ':' + sys._getframe().f_code.co_name)
        self.__g_oSvDb = None
        self.__g_sActEdiFileFullPath = None
        self.__g_lstCsvTransferFile = []
        self.__g_dictSkuInfo = {}  # handling sku info
        self.__g_dictBranchInfo = {}  # handling hyper mart branch info
        super().__init__()

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __del__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        return self

    def initialize(self, o_sv_mysql, s_path_abs_unzip, lst_edi_file_info=None):
        self.__g_oSvDb = o_sv_mysql
        if not self.__g_oSvDb:
            raise Exception('invalid db handler')
        self.__g_sActEdiFileFullPath = s_path_abs_unzip
        # begin - set csv transferring filename
        self.__g_dictEdiCsvFileMap = {
            str(sv_hypermart_model.SvHyperMartType.EMART) + '_' + str(sv_hypermart_model.EdiDataType.QTY):
                {'s_csv_filename': os.path.join(self.__g_sActEdiFileFullPath, 'compiled_emart_qty.csv'), 'b_db_proc': False},
            str(sv_hypermart_model.SvHyperMartType.EMART) + '_' + str(sv_hypermart_model.EdiDataType.AMNT):
                {'s_csv_filename': os.path.join(self.__g_sActEdiFileFullPath, 'compiled_emart_amnt.csv'), 'b_db_proc': False},
            str(sv_hypermart_model.SvHyperMartType.LOTTEMART) + '_' + str(sv_hypermart_model.EdiDataType.QTY_AMNT):
                {'s_csv_filename': os.path.join(self.__g_sActEdiFileFullPath, 'compiled_lottemart_qty_amnt.csv'), 'b_db_proc': False},
            str(sv_hypermart_model.SvHyperMartType.HOMEPLUS) + '_' + str(sv_hypermart_model.EdiDataType.QTY_AMNT):
                {'s_csv_filename': os.path.join(self.__g_sActEdiFileFullPath, 'compiled_homeplus_qty_amnt.csv'), 'b_db_proc': False},
        }
        # end - set csv transferring filename
        # prepare for qty and amnt edi file
        if lst_edi_file_info:
            self.__g_lstCsvTransferFile.clear()  # clear lst info to prevent short-term duplicated work
            for dict_single_edi in lst_edi_file_info:
                if dict_single_edi['status'] != sv_hypermart_model.ProgressStatus.UPLOADED:
                    print(dict_single_edi['s_filename'] + ' is already on transforming')
                    continue
                if dict_single_edi['n_hyper_mart'] == int(sv_hypermart_model.SvHyperMartType.EMART):
                    if dict_single_edi['n_edi_data_type'] == int(sv_hypermart_model.EdiDataType.ESTIMATION) or \
                        dict_single_edi['n_edi_data_type'] == int(sv_hypermart_model.EdiDataType.NOT_SURE):
                        raise Exception('plz define edi data type')
                    self.__g_lstCsvTransferFile.append(dict_single_edi)
                elif dict_single_edi['n_hyper_mart'] == sv_hypermart_model.SvHyperMartType.LOTTEMART or \
                        dict_single_edi['n_hyper_mart'] == sv_hypermart_model.SvHyperMartType.HOMEPLUS:
                    self.__g_lstCsvTransferFile.append(dict_single_edi)
        return

    def transfer_excel_to_csv(self):
        # begin - unset old csv file
        for s_csv_file_id, dict_csv_file_info in self.__g_dictEdiCsvFileMap.items():
            o_csv_file = Path(dict_csv_file_info['s_csv_filename'])
            if o_csv_file.is_file():
                os.remove(dict_csv_file_info['s_csv_filename'])
        # end - unset old csv file

        o_emart_excel = edi_model.EmartEdiExcel()
        o_lmart_excel = edi_model.LotteMartEdiExcel()
        for dict_single_edi in self.__g_lstCsvTransferFile:
            s_csv_file_id = str(dict_single_edi['n_hyper_mart']) + '_' + str(dict_single_edi['n_edi_data_type'])
            s_csv_file_path = self.__g_dictEdiCsvFileMap[s_csv_file_id]['s_csv_filename']
            if dict_single_edi['n_hyper_mart'] == int(sv_hypermart_model.SvHyperMartType.EMART):
                print('emart xls -> csv detected')
                o_emart_excel.set_edi_data_year(dict_single_edi['n_edi_data_year'])
                o_emart_excel.load_file(self.__g_sActEdiFileFullPath, dict_single_edi['s_filename'], b_data_year_est=False)
                o_emart_excel.to_csv(s_csv_file_path)
                self.__g_dictEdiCsvFileMap[s_csv_file_id]['b_db_proc'] = True
            elif dict_single_edi['n_hyper_mart'] == int(sv_hypermart_model.SvHyperMartType.LOTTEMART):
                print('lmart xls -> csv detected')
                o_lmart_excel.load_file(self.__g_sActEdiFileFullPath, dict_single_edi['s_filename'])
                o_lmart_excel.get_edi_data_year()
                o_lmart_excel.to_csv(s_csv_file_path)
                self.__g_dictEdiCsvFileMap[s_csv_file_id]['b_db_proc'] = True
            elif dict_single_edi['n_hyper_mart'] == int(sv_hypermart_model.SvHyperMartType.HOMEPLUS):
                print('homeplus xls -> csv detected')
        del o_emart_excel
        del o_lmart_excel
        return

    def check_new_entity(self):
        """ check new branch, SKU to notify user """
        self.__get_hypermart_branch_info()
        self.__get_hypermart_old_sku_info()

        if not self.__g_dictBranchInfo:
            print('invalid hypermart branch info')
            raise Exception('invalid hypermart branch info')

        for s_csv_file_id, dict_csv_file_info in self.__g_dictEdiCsvFileMap.items():
            if not dict_csv_file_info['b_db_proc']:
                continue
            s_csv_file_path = dict_csv_file_info['s_csv_filename']
            o_csv_file = Path(s_csv_file_path)
            if o_csv_file.is_file():
                lst_file_info = s_csv_file_id.split('_')  # [0]=SvHyperMartType, [1]=.EdiDataType
                lst_file_info = [int(x) for x in lst_file_info]  # convert string to int
                if lst_file_info[0] == sv_hypermart_model.SvHyperMartType.EMART:
                    print('emart csv detected -> check new entity ')
                    return self.__emart_check_new(s_csv_file_path)
                elif lst_file_info[0] == sv_hypermart_model.SvHyperMartType.LOTTEMART:
                    print('lmart csv detected -> check new entity')
                    return self.__lmart_check_new(s_csv_file_path)
                elif lst_file_info[0] == sv_hypermart_model.SvHyperMartType.HOMEPLUS:
                    print('HP csv detected -> check new entity')
                    print(s_csv_file_path + ' goes HP registering')
    
    def add_new_sku_info(self, s_serialized_appending_skus):
        """ check new branch, SKU to notify user """
        self.__get_hypermart_branch_info()
        self.__get_hypermart_old_sku_info()
        if not self.__g_dictBranchInfo:
            print('invalid hypermart branch info')
            raise Exception('invalid hypermart branch info')
        for s_csv_file_id, dict_csv_file_info in self.__g_dictEdiCsvFileMap.items():
            s_csv_file_path = dict_csv_file_info['s_csv_filename']
            o_csv_file = Path(s_csv_file_path)
            if o_csv_file.is_file():
                lst_file_info = s_csv_file_id.split('_')  # [0]=SvHyperMartType, [1]=.EdiDataType
                lst_file_info = [int(x) for x in lst_file_info]  # convert string to int
                if lst_file_info[0] == sv_hypermart_model.SvHyperMartType.EMART:
                    print('emart csv -> db detected')
                    dict_rst = self.__emart_check_new(s_csv_file_path)
                elif lst_file_info[0] == sv_hypermart_model.SvHyperMartType.LOTTEMART:
                    print('lmart csv -> db detected')
                    dict_rst = self.__lmart_check_new(s_csv_file_path)
                elif lst_file_info[0] == sv_hypermart_model.SvHyperMartType.HOMEPLUS:
                    print('HP csv -> db detected')
                    print(s_csv_file_path + ' goes HP registering')
        
        lst_brand_sku = s_serialized_appending_skus.split(',')
        for s_unique_sku_id, s_first_detect_date in dict_rst['dict_new_sku'].items():
            lst_sku_info = s_unique_sku_id.split('||')
            if lst_sku_info[1] in lst_brand_sku:
                print('accept ' + lst_sku_info[1] + ' ' + lst_sku_info[2])
                self.__add_hypermart_sku_info(1, lst_sku_info[0], lst_sku_info[1], lst_sku_info[2], s_first_detect_date)
            else:
                print('deny ' + lst_sku_info[1] + ' ' + lst_sku_info[2])
                self.__add_hypermart_sku_info(0, lst_sku_info[0], lst_sku_info[1], lst_sku_info[2], s_first_detect_date)

    def transform_csv_to_db(self):
        """ transform existing csv file to db """
        self.__get_hypermart_branch_info()
        self.__get_hypermart_old_sku_info(b_accepted=True)
        if not self.__g_dictBranchInfo:
            print('invalid hypermart branch info')
            raise Exception('invalid hypermart branch info')

        for s_csv_file_id, dict_csv_file_info in self.__g_dictEdiCsvFileMap.items():
            # if not dict_csv_file_info['b_db_proc']:
            #     continue
            s_csv_file_path = dict_csv_file_info['s_csv_filename']
            o_csv_file = Path(s_csv_file_path)
            if o_csv_file.is_file():
                lst_file_info = s_csv_file_id.split('_')  # [0]=SvHyperMartType, [1]=.EdiDataType
                lst_file_info = [int(x) for x in lst_file_info]  # convert string to int
                if lst_file_info[0] == sv_hypermart_model.SvHyperMartType.EMART:
                    print('emart csv -> db detected')
                    self.__emart_csv_to_db(s_csv_file_path, lst_file_info[1])
                elif lst_file_info[0] == sv_hypermart_model.SvHyperMartType.LOTTEMART:
                    print('lmart csv -> db detected')
                    self.__lmart_csv_to_db(s_csv_file_path)
                elif lst_file_info[0] == sv_hypermart_model.SvHyperMartType.HOMEPLUS:
                    print('HP csv -> db detected')
                    print(s_csv_file_path + ' goes HP registering')

        # edi file status update and file remove
        # lst_path = os.path.split(os.path.abspath(self.__g_oUploadedFile.uploaded_file.path))
        # s_uploaded_directory_abs_path = lst_path[0]
        # s_uploaded_file_abs_path = lst_path[1]
        # del lst_path
        # for o_edi_file in self.__g_lstCsvTransferFile:
        #     o_edi_file.status = edi_model.ProgressStatus.TRANSFORMED
        #     o_edi_file.registered_to_db = True
        #     o_edi_file.save()

        #     s_edi_filename = str(o_edi_file)
        #     if s_uploaded_file_abs_path != s_edi_filename:
        #         s_file_abs_path = os.path.join(s_uploaded_directory_abs_path, str(o_edi_file))
        #         if os.path.exists(s_file_abs_path):
        #             os.remove(s_file_abs_path)

        # # uploaded file status update
        # self.__g_oUploadedFile.status = edi_model.ProgressStatus.TRANSFORMED
        # self.__g_oUploadedFile.save()

        # summarize registered edi date info
        # o_sv_slack = SvSlack()
        # o_sv_slack.send_msg('transforming completed')
        # del o_sv_slack
        return

    def clear(self):
        self.__g_dictSkuInfo.clear()
        self.__g_dictBranchInfo.clear()
        print('TransformEdiExcel::clear() called')
        return

    def __add_hypermart_sku_info(self, b_accept, s_mart_type, s_mart_item_code, s_mart_sku_name, s_first_detect_date):
        """ register brand specific SKU info table """
        try:
            self.__g_oSvDb.executeQuery('insertEdiSkuInfo', b_accept, s_mart_type, s_mart_item_code, s_mart_sku_name, s_first_detect_date)
        except pymysql.err.IntegrityError:  # change to latest item name if item code is duplicated 
            lst_rst = self.__g_oSvDb.executeQuery('getEdiSkuInfoByMartIdSkuName', s_mart_type, s_mart_item_code)
            n_sku_id = lst_rst[0]['id']
            self.__g_oSvDb.executeQuery('updateEdiSkuName', s_mart_sku_name, n_sku_id)
        return

    def __emart_csv_to_db(self, s_csv_file, n_emart_data_type):
        """
        # 이마트 csv 파일 형식
        # 상품코드,상품명,점포코드,점포명,값,로그일(값이 금액일 경우, 공급가, VAT 별도): N줄
        :param s_csv_file:
        :param n_emart_data_type:
        :return:
        """
        s_emart_id = str(sv_hypermart_model.SvHyperMartType.EMART)
        f = open(s_csv_file, 'r')
        reader = csv.reader(f, delimiter=",")
        for i, line in enumerate(reader):
            if int(line[4]) == 0:  # qty or amnt is not 0; refund is minus
                continue

            # get sku info
            s_unique_sku_id = s_emart_id + '_' + line[0]
            if s_unique_sku_id in self.__g_dictSkuInfo:
                n_sku_id = self.__g_dictSkuInfo[s_unique_sku_id]
            else:
                continue  # ignore other SKU

            # get branch info
            s_unique_branch_id = s_emart_id + '_' + line[2]
            if s_unique_branch_id in self.__g_dictBranchInfo:
                n_branch_id = self.__g_dictBranchInfo[s_unique_branch_id]
            else:
                s_branch_name = line[3].replace(' ', '')
                print('new branch detected ' + line[2] + ' ' + s_branch_name)
                raise Exception('new branch detected ' + line[2] + ' ' + s_branch_name)  # need user define Exception
            
            # check an existing log
            lst_rst = self.__g_oSvDb.executeQuery('getEmartLogByItemBranchDate', n_sku_id, n_branch_id, line[5])
            s_query_stmt = None
            if len(lst_rst) == 0:  # add new log; emart separate QTY and AMNT report
                if n_emart_data_type == sv_hypermart_model.EdiDataType.QTY:
                    s_query_stmt = 'insertEmartDailyQtyLog'
                elif n_emart_data_type == sv_hypermart_model.EdiDataType.AMNT:
                    s_query_stmt = 'insertEmartDailyAmntLog'
                self.__g_oSvDb.executeQuery(s_query_stmt, n_sku_id, n_branch_id, line[4], line[5])
            elif len(lst_rst) == 1:  # update existing log; emart separate QTY and AMNT report
                n_log_id = lst_rst[0]['id']
                if lst_rst[0]['qty'] != 0:
                    if n_emart_data_type == sv_hypermart_model.EdiDataType.QTY:
                        print('denied: duplicated emart qty for log id -> ' + str(n_log_id) + ' on date ' +
                              str(line[5]) + ' qty -> ' + str(line[4]))
                        continue
                    elif n_emart_data_type == sv_hypermart_model.EdiDataType.AMNT:
                        s_query_stmt = 'updateEmartDailyAmntLog'
                elif lst_rst[0]['amnt'] != 0:
                    if n_emart_data_type == sv_hypermart_model.EdiDataType.AMNT:
                        print('denied: duplicated emart amnt for log id -> ' + str(n_log_id) + ' on date ' +
                              str(line[5]) + ' amnt -> ' + str(line[4]))
                        continue
                    elif n_emart_data_type == sv_hypermart_model.EdiDataType.QTY:
                        s_query_stmt = 'updateEmartDailyQtyLog'
                if s_query_stmt is None:
                    raise Exception('data corrupted')  # need user define Exception    
                else:
                    self.__g_oSvDb.executeQuery(s_query_stmt, line[4], n_log_id)
            else:
                raise Exception('data corrupted')  # need user define Exception
            if i % 50000 == 0:
                print('beacon:' + str(i))
            # if i == 50:  # for shorten test
            #    break
        f.close()

    def __lmart_csv_to_db(self, s_csv_file):
        """
        # 롯데마트 csv 파일 형식
        # 점포명,점포코드,상품코드,상품명,판매코드,매출수량,매출금액(소비자가,VAT포함),로그시작일,로그종료일,이마트표준화금액(부가세제외 공급가) N줄
        :param s_csv_file:
        :return:
        """
        s_ltmart_id = str(sv_hypermart_model.SvHyperMartType.LOTTEMART)
        f = open(s_csv_file, 'r')
        reader = csv.reader(f, delimiter=",")
        for i, line in enumerate(reader):
            if int(line[5]) == 0 and int(line[6]) == 0:  # qty and amnt is not 0; refund is minus
                continue
            
            # get sku info
            s_unique_sku_id = s_ltmart_id + '_' + line[4]
            if s_unique_sku_id in self.__g_dictSkuInfo:
                n_sku_id = self.__g_dictSkuInfo[s_unique_sku_id]
            else:
                continue  # ignore other SKU

            # check new branch info
            s_unique_branch_id = s_ltmart_id + '_' + line[1]
            if s_unique_branch_id in self.__g_dictBranchInfo:
                n_branch_id = self.__g_dictBranchInfo[s_unique_branch_id]
            else:
                s_branch_name = line[0].replace(' ', '')
                print('new branch detected ' + line[1] + ' ' + s_branch_name)
                raise Exception('new branch detected ' + line[1] + ' ' + s_branch_name)  # need user define Exception

            s_since_logdate = line[7]
            s_to_logdate = line[8]

            # check an existing log
            lst_since_rst = self.__g_oSvDb.executeQuery('getLtmartLogByItemBranchDateIn', n_sku_id, n_branch_id,
                                                        s_since_logdate, s_since_logdate)
            if len(lst_since_rst) >= 1:  # deny adding log
                print('denied: duplicated qty for log id -> ' + str(lst_since_rst[0]['id']) + ' on since date ' + str(
                    line[5]) + ' qty -> ' + str(line[4]))
                continue
            lst_to_rst = self.__g_oSvDb.executeQuery('getLtmartLogByItemBranchDateIn', n_sku_id, n_branch_id,
                                                     s_to_logdate, s_to_logdate)
            if len(lst_to_rst) >= 1:  # deny adding log
                print('denied: duplicated qty for log id -> ' + str(lst_to_rst[0]['id']) + ' on to date ' + str(
                    line[5]) + ' qty -> ' + str(line[4]))
                continue
            lst_range_rst = self.__g_oSvDb.executeQuery('getLtmartLogByItemBranchDateOut', n_sku_id, n_branch_id,
                                                        s_since_logdate, s_to_logdate)
            if len(lst_range_rst) >= 1:  # deny adding log
                print('denied: duplicated qty for log id -> ' + str(lst_range_rst[0]['id']) + ' on range date ' + str(
                    line[5]) + ' qty -> ' + str(line[4]))
                continue
            # add new -> owner_id`, `item_id`, `branch_id`, `qty`, `original_amnt(판매액)`, `amnt(공급액)`, `since_logdate`, `logdate`
            self.__g_oSvDb.executeQuery('insertLtmartDailyLog', n_sku_id, n_branch_id, line[5], line[6],
                                        line[9], s_since_logdate, s_to_logdate)
            if i % 50000 == 0:
                print('beacon:' + str(i))
            # if i == 50:  # for shorten test
            #    break
        f.close()

    def __get_hypermart_branch_info(self):
        dict_branch_by_title = sv_hypermart_model.SvHyperMartType.get_dict_by_title()
        o_mart_geo_info = sv_hypermart_model.SvHypermartGeoInfo()
        for dict_single_branch in o_mart_geo_info.lst_hypermart_geo_info:
            n_hypermart_id = dict_branch_by_title[dict_single_branch['hypermart_name']]
            self.__g_dictBranchInfo[str(n_hypermart_id)+'_'+dict_single_branch['branch_code']] = dict_single_branch['id']
        return

    def __get_hypermart_old_sku_info(self, b_accepted=None):
        # retrieve account specific SKU info dictionary from account dependent table
        if b_accepted is None:
            lst_rst = self.__g_oSvDb.executeQuery('getEdiSkuCodeAll')  # to check new sku only
        else:
            lst_rst = self.__g_oSvDb.executeQuery('getEdiSkuAccepted', 1)
        for dictSingleRow in lst_rst:
            self.__g_dictSkuInfo[str(dictSingleRow['mart_id']) + '_' + dictSingleRow['item_code']] = dictSingleRow['id']
        return
    
    def __lmart_check_new(self, s_csv_file):
        """
        a subsidiary of check_new_entity
        # 롯데마트 csv 파일 형식
        # 점포명,점포코드,상품코드,상품명,판매코드,매출수량,매출금액(소비자가,VAT포함),로그시작일,로그종료일,이마트표준화금액(부가세제외 공급가) N줄
        :param s_csv_file:
        :return:
        """
        s_ltmart_id = str(sv_hypermart_model.SvHyperMartType.LOTTEMART)
        dict_new_branch = {}
        dict_new_sku = {}
        f = open(s_csv_file, 'r')
        reader = csv.reader(f, delimiter=",")
        for i, line in enumerate(reader):
            if int(line[5]) == 0 and int(line[6]) == 0:  # qty and amnt is not 0; refund is minus
                continue
            # check new branch info
            if s_ltmart_id + '_' + line[1] not in self.__g_dictBranchInfo:
                s_unique_branch_id = s_ltmart_id + '||' + line[1]
                if s_unique_branch_id not in dict_new_branch:
                    s_branch_name = line[1].replace(' ', '')
                    dict_new_branch[s_unique_branch_id] = s_branch_name
            # check new sku info
            if s_ltmart_id + '_' + line[4] not in self.__g_dictSkuInfo:
                s_unique_sku_id = s_ltmart_id + '||' + line[4] + '||' + line[3]
                if s_unique_sku_id in dict_new_sku:
                    if dict_new_sku[s_unique_sku_id] > line[7]:
                        dict_new_sku[s_unique_sku_id] = line[7]
                else:
                    dict_new_sku[s_unique_sku_id] = line[7]
            if i % 50000 == 0:
                print('beacon:' + str(i))
            # if i == 50:  # for shorten test
            #    break
        f.close()
        return {'dict_new_branch': dict_new_branch, 'dict_new_sku': dict_new_sku}
    
    def __emart_check_new(self, s_csv_file):
        """
        a subsidiary of check_new_entity
        # 이마트 csv 파일 형식
        # 상품코드,상품명,점포코드,점포명,값,로그일(값이 금액일 경우, 공급가, VAT 별도): N줄
        :param s_csv_file:
        :return:
        """
        s_emart_id = str(sv_hypermart_model.SvHyperMartType.EMART)
        dict_new_branch = {}
        dict_new_sku = {}
        f = open(s_csv_file, 'r')
        reader = csv.reader(f, delimiter=",")
        for i, line in enumerate(reader):
            if int(line[4]) == 0:  # qty or amnt is not 0; refund is minus
                continue
            # check new branch info
            if s_emart_id + '_' + line[2] not in self.__g_dictBranchInfo:
                s_unique_branch_id = s_emart_id + '||' + line[2]
                if s_unique_branch_id not in dict_new_branch:
                    s_branch_name = line[3].replace(' ', '')
                    dict_new_branch[s_unique_branch_id] = s_branch_name
            # check new sku info
            if s_emart_id + '_' + line[0] not in self.__g_dictSkuInfo:
                s_unique_sku_id = s_emart_id + '||' + line[0] + '||' + line[1]
                if s_unique_sku_id in dict_new_sku:
                    if dict_new_sku[s_unique_sku_id] > line[5]:
                        dict_new_sku[s_unique_sku_id] = line[5]
                else:
                    dict_new_sku[s_unique_sku_id] = line[5]

            if i % 50000 == 0:
                print('beacon:' + str(i))
            # if i == 50:  # for shorten test
            #    break
        f.close()
        return {'dict_new_branch': dict_new_branch, 'dict_new_sku': dict_new_sku}
