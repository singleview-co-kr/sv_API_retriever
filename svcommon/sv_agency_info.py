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
import re
import sys
import csv
import logging
# import datetime
from datetime import datetime
from datetime import timedelta

# 3rd party library

# singleview config
if __name__ == 'svcommon.sv_agency_info': # for websocket running
    from svcommon import sv_object
elif __name__ == 'sv_agency_info': # for plugin console debugging
    sys.path.append('../../svdjango')
    import sv_object
elif __name__ == '__main__': # for class console debugging
    pass


class SvAgencyInfo(sv_object.ISvObject):
    __g_sAgencyFeeTypeBackmargin = 'back'
    __g_sAgencyFeeTypeMarkup = 'markup'
    __g_sAgencyFeeTypeDirect = 'direct'

    def __init__(self):
        self._g_oLogger = logging.getLogger(__file__)
        self.__sAgencyInfoPath = None
        self.__lstAgencyInfo = []

    def __enter__(self):
        """ grammtical method to use with "with" statement """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ unconditionally calling desctructor """
        pass

    def __del__(self):
        pass
    
    def load_agency_info_file(self, s_agency_info_path):
        self.__sAgencyInfoPath = s_agency_info_path
        try:
            with open(self.__sAgencyInfoPath, 'r') as o_tsv_file:
                o_tsv_reader = csv.reader(o_tsv_file, delimiter='\t')
                for lst_row in o_tsv_reader:
                    self.__lstAgencyInfo.append(lst_row)
        except FileNotFoundError:
            self._printDebug('agency_info.tsv does not exist -> ' + s_agency_info_path)

    def get_agency_fee_type(self):
        return [self.__g_sAgencyFeeTypeBackmargin, self.__g_sAgencyFeeTypeMarkup, self.__g_sAgencyFeeTypeDirect]

    def get_latest_agency_info_dict(self):
        dict_agency_info = {
                's_begin_date': '',
                's_end_date': '',
                's_agency_name': '',
                'n_fee_rate': 0,
                's_fee_type': ''
            }
        if len(self.__lstAgencyInfo):
            lst_latest = self.__lstAgencyInfo.pop()
            lst_period = lst_latest[0].split('-')
            dict_agency_info['s_begin_date'] = lst_period[0]
            if(len(lst_period)==2):
                dict_agency_info['s_end_date'] = lst_period[1]
            dict_agency_info['s_agency_name'] = lst_latest[1]
            dict_agency_info['n_fee_rate'] = int(lst_latest[2].rstrip('%'))
            dict_agency_info['s_fee_type'] = lst_latest[3]
        return dict_agency_info

    def set_agency_info(self, lst_new_info):
        """
        lst_new_info = [s_begin_date, s_agency_name, n_fee_percent, s_fee_type]
        """
        n_row_cnt = len(self.__lstAgencyInfo)
        if n_row_cnt:
            dt_yesterday = datetime.today() - timedelta(1)
            s_yesterday = str(dt_yesterday.strftime('%Y%m%d'))
            del dt_yesterday
            self.__lstAgencyInfo [n_row_cnt-1][0] = self.__lstAgencyInfo [n_row_cnt-1][0] + s_yesterday
        lst_new_info[0] = lst_new_info[0] + '-'
        lst_new_info[2] = str(lst_new_info[2]) + '%'
        self.__lstAgencyInfo .append(lst_new_info)
        with open(self.__sAgencyInfoPath, 'w', newline='') as o_csvfile:
            write = csv.writer(o_csvfile, delimiter='\t') 
            write.writerows(self.__lstAgencyInfo ) 
        return True

    def redefine_agency_cost(self, s_agency_info_path, nCost):
        dictRst = {'cost':0, 'agency_fee':0, 'vat':0}
        if nCost > 0:
            sBeginDate = '20010101' # define default ancient begin date
            sEndDate = datetime.today().strftime('%Y%m%d')
            fRate = 0.0
            # sAgencyInfoFilePath = os.path.join(self.__g_sDataPath, sMedia, str(sCustomerId), 'conf', 'agency_info.tsv')
            try:
                with open(s_agency_info_path, 'r') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    for row in reader:
                        pass # read last line only
            except FileNotFoundError:
                self._printDebug('agency_info.tsv does not exist -> ' + s_agency_info_path)
                return dictRst  # raise Exception('stop')
                
            aPeriod = row[0].split('-')
            if len(aPeriod[0]) > 0:
                try: # validate requsted date
                    sBeginDate = datetime.strptime(aPeriod[0], '%Y%m%d').strftime('%Y%m%d')
                except ValueError:
                    self._printDebug('start date:' + aPeriod[0] + ' is invalid date string')

            if len(aPeriod[1]) > 0:
                try: # validate requsted date
                    sEndDate = datetime.strptime(aPeriod[1], '%Y%m%d').strftime('%Y%m%d')
                except ValueError:
                    self._printDebug('end date:' + aPeriod[0] + ' is invalid date string')

            dtBegin = datetime.strptime(sBeginDate, '%Y%m%d').date()
            dtEnd = datetime.strptime(sEndDate, '%Y%m%d').date()
            dtNow = datetime.today()
            if dtNow < dtBegin:
                self._printDebug('invalid agency begin date - ' + s_agency_info_path)
                raise Exception('stop')
            
            if dtNow > dtEnd:
                self._printDebug('invalid agency begin date' + s_agency_info_path)
                raise Exception('stop')

            oRegEx = re.compile(r"\d+%$") # pattern ex) 2% 23%
            m = oRegEx.search(row[2]) # match() vs search()
            if m: # if valid percent string
                nPercent = row[2].replace('%','')
                fRate = int(nPercent)/100
            else: # if invalid percent string
                self._printDebug('invalid percent string ' + row[2])
                raise Exception('stop')
 
            nFinalCost = 0
            nAgencyCost = 0
            if row[3] == self.__g_sAgencyFeeTypeBackmargin:  #'back':
                nFinalCost =int((1 - fRate) * nCost)
                nAgencyCost = int(fRate * nCost)

                # validate naver ad cost division
                nTempCost = nFinalCost + nAgencyCost
                if nCost > nTempCost:
                    nResidual = nCost - nTempCost
                    nFinalCost = nFinalCost + nResidual
                elif nCost < nTempCost:
                    nResidual = nTempCost - nCost
                    nFinalCost = nFinalCost + nResidual
            elif row[3] == self.__g_sAgencyFeeTypeMarkup:  #'markup':
                nFinalCost = nCost
                nAgencyCost = fRate * nCost
            elif row[3] == self.__g_sAgencyFeeTypeDirect:  #'direct':
                nFinalCost = nCost
            else:
                self._printDebug('invalid margin type ' + row[3])
                raise Exception('stop')

            # if sMedia == 'naver_ad':
            #     if nCost != nFinalCost + nAgencyCost:
            #         self._printDebug(nCost)
            #         self._printDebug(nFinalCost)
            #         self._printDebug(nAgencyCost)

            # # if dictSourceToRetrieve[sMedia] == 'kakao': # csv download based data
            # if sMedia == 'kakao': # csv download based data
            #     nVatFromFinalCost = int(nFinalCost * 0.1)
            #     nVatFromnAgencyCost = int(nAgencyCost * 0.1)
            #     dictRst['cost'] = nFinalCost - nVatFromFinalCost
            #     dictRst['agency_fee'] = nAgencyCost - nVatFromnAgencyCost
            #     dictRst['vat'] = nVatFromFinalCost + nVatFromnAgencyCost
            # else:
            #     dictRst['cost'] = nFinalCost
            #     dictRst['agency_fee'] = nAgencyCost
            #     dictRst['vat'] = (nFinalCost + nAgencyCost ) * 0.1
        return dictRst