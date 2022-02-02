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
from datetime import datetime
from datetime import timedelta
import time
import os
import sys
import csv
import random

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svdjango')
    import sv_object
    import sv_plugin
    import settings
    from powernad.API.MasterReport import *
    from powernad.Object.MasterReport.RequestObject.CreateMasterReportObject import CreateMasterReportObject
    from powernad.API.StatReport import *
    from powernad.Object.StatReport.RequestObject.CreateStatReportObject import CreateStatReportObject
else:
    from svcommon import sv_object
    from svcommon import sv_plugin
    from django.conf import settings
    sys.path.append(os.path.join(os.getcwd(),'svcommon'))
    from svcommon.powernad.API.MasterReport import *
    from svcommon.powernad.Object.MasterReport.RequestObject.CreateMasterReportObject import CreateMasterReportObject
    from svcommon.powernad.API.StatReport import *
    from svcommon.powernad.Object.StatReport.RequestObject.CreateStatReportObject import CreateStatReportObject


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sNaveradApiBaseUrl = 'https://api.naver.com'
    __g_nRptWaitingSec = 60

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_oLogger = logging.getLogger(__name__ + ' modified at 15th, Jan 2022')
        self._g_dictParam.update({'data_first_date':None, 'data_last_date':None})
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sEncodedApiKey = None
        self.__g_sEncodedSecretKey = None
        self.__g_sNvrAdManagerLoginId = None
        self.__g_bNvrAdManagerLoginIdWarned = False
        self.__g_sRetrieveInfoPath = None
        self.__g_sDataLastDate = None
        self.__g_sDataFirstDate = None
        self.__g_nRetryBackoffCnt = 0

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sEncodedApiKey = None
        self.__g_sEncodedSecretKey = None
        self.__g_sNvrAdManagerLoginId = None
        self.__g_bNvrAdManagerLoginIdWarned = False
        self.__g_sRetrieveInfoPath = None
        self.__g_sDataLastDate = None
        self.__g_sDataFirstDate = None
        self.__g_nRetryBackoffCnt = 0

    def do_task(self, o_callback):
        self._g_oCallback = o_callback

        if self._g_dictParam['data_first_date'] is None or \
            self._g_dictParam['data_last_date'] is None:
            self._printDebug('you should designate data_first_date and data_last_date')
            self._task_post_proc(self._g_oCallback)
            return
        self.__g_sDataFirstDate = self._g_dictParam['data_first_date'].replace('-','')
        self.__g_sDataLastDate = self._g_dictParam['data_last_date'].replace('-','')

        dict_acct_info = self._task_pre_proc(o_callback)
        lst_conf_keys = list(dict_acct_info.keys())
        if 'sv_account_id' not in lst_conf_keys and 'brand_id' not in lst_conf_keys and \
          'nvr_ad_acct' not in lst_conf_keys:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            return
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        dict_nvr_ad_acct = dict_acct_info['nvr_ad_acct']
        self.__g_sNvrAdManagerLoginId = dict_nvr_ad_acct['manager_login_id']
        self.__g_sEncodedApiKey = dict_nvr_ad_acct['api_key']
        self.__g_sEncodedSecretKey = dict_nvr_ad_acct['secret_key']
        s_customer_id = dict_nvr_ad_acct['customer_id']
        
        # delete master report
        o_master_report = MasterReport(self.__g_sNaveradApiBaseUrl, self.__g_sEncodedApiKey, self.__g_sEncodedSecretKey, s_customer_id)
        dict_rst = o_master_report.delete_master_report_all()
        if 'transaction_id' not in list(dict_rst.keys()):
            self._printDebug('communication failed - stop')
            self._task_post_proc(self._g_oCallback)
            return
        else:
            self._printDebug('-> '+ s_customer_id +' delete master reports with transaction id - ' + dict_rst['transaction_id'])
        
        self.__g_sRetrieveInfoPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_brand_id, 'naver_ad', s_customer_id, 'conf')
        if os.path.isdir(self.__g_sRetrieveInfoPath) == False:
            os.makedirs(self.__g_sRetrieveInfoPath)
        self.__g_sDownloadPathNew = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_brand_id, 'naver_ad', s_customer_id, 'data')
        if os.path.isdir(self.__g_sDownloadPathNew) == False:
            os.makedirs(self.__g_sDownloadPathNew)

        o_stat_report = StatReport(self.__g_sNaveradApiBaseUrl, self.__g_sEncodedApiKey, self.__g_sEncodedSecretKey, s_customer_id)
        self.__retrieveNvStatReport(o_stat_report, s_sv_acct_id, s_customer_id, dict_nvr_ad_acct['nvr_stat_report'])  # statdate arg should be defined

        self._task_post_proc(self._g_oCallback)

    def __retrieveNvStatReport(self, o_stat_report, sSvAcctId, sNvrAdCustomerID, lstReport):
        dictMasterReportQueue = dict()
        dictMasterReportExceptionCnt = dict()  # memory to restrict exceptional occurrence count
        isDoneSomething = False

        for report in lstReport:
            dictMasterReportQueue.update({report:'readytogo'})
            dictMasterReportExceptionCnt.update({report:0})
        
        nQueueLen = len(dictMasterReportQueue)
        if nQueueLen:
            while True: # loop for each report type
                self.__g_nRetryBackoffCnt = 0
                try:
                    sTobeHandledTaskName = list(dictMasterReportQueue.keys())[list(dictMasterReportQueue.values()).index('readytogo')] # find unhandled report task
                    if sTobeHandledTaskName: # if there exists unhandled report task
                        try:
                            sEarliestFilepath = os.path.join(self.__g_sRetrieveInfoPath, sTobeHandledTaskName+'.earliest')
                            f = open(sEarliestFilepath, 'r')
                            sMinReportDate = f.readline()
                            dtDateStatRetrieval = datetime.strptime(sMinReportDate, '%Y%m%d') - timedelta(days=1)
                            f.close()
                        except FileNotFoundError:
                            dtDateStatRetrieval = datetime.strptime(self.__g_sDataLastDate, '%Y%m%d')
                        
                        dtDelta = datetime.today() - dtDateStatRetrieval
                        if dtDelta.days > 365:
                            self._printDebug('--> can not retrieve older than a year ago')
                            return  # raise Exception('remove')

                        self._printDebug('--> nvr ad id: ' + sNvrAdCustomerID +' will retrieve stat report - ' + sTobeHandledTaskName +' on ' + str(dtDateStatRetrieval))
                        # if requested stat date is earlier than stat first date
                        if dtDateStatRetrieval - datetime.strptime(self.__g_sDataFirstDate, '%Y%m%d') < timedelta(days=0): 
                            self._printDebug('finish: meet first stat date')
                            dictMasterReportQueue[sTobeHandledTaskName] = 'finish'
                            continue

                        sDateRetrieval = dtDateStatRetrieval.strftime('%Y%m%d')
                        dict_rst = self.__retrieveNvStatReportAct(o_stat_report, sTobeHandledTaskName, sDateRetrieval)
                        if dict_rst['b_error'] is True:
                            if dictMasterReportExceptionCnt[sTobeHandledTaskName] < 3: # allow exception occurrence count to 3 by each report
                                dictMasterReportExceptionCnt[sTobeHandledTaskName] += 1
                                if dict_rst['s_todo']:  # if todo is defined
                                    s_todo = dict_rst['s_todo']
                                    if s_todo == 'wait':
                                        self._printDebug('wait ' + str(self.__g_nRptWaitingSec) + ' sec and go')
                                        time.sleep(self.__g_nRptWaitingSec)
                                        continue
                                    elif s_todo == 'pass':
                                        dictMasterReportQueue[sTobeHandledTaskName] = 'pass'
                                        continue
                                    elif s_todo == 'stop':
                                        self._printDebug('stop')
                                        dictMasterReportQueue[sTobeHandledTaskName] = 'stop'
                                        return
                                    elif s_todo == 'close':
                                        self._printDebug('close')
                                        dictMasterReportQueue[sTobeHandledTaskName] = 'close'
                                        f = open(sEarliestFilepath, 'w')
                                        f.write(self.__g_sDataFirstDate)
                                        f.close()
                                else:  # default is waiting if todo is not defined
                                    self._printDebug('error occured but todo is not defined -> wait ' + str(self.__g_nRptWaitingSec) + ' sec and go')
                                    time.sleep(self.__g_nRptWaitingSec)
                                    continue
                            else:
                                self._printDebug('too many exceptions raise exception!!')
                                dictMasterReportQueue[sTobeHandledTaskName] = 'finish'
                                continue    
                        else:
                            dictMasterReportQueue[sTobeHandledTaskName] = 'finish'
                            isDoneSomething = True
                            f = open(sEarliestFilepath, 'w')
                            f.write(dtDateStatRetrieval.strftime('%Y%m%d'))
                            f.close()
                except ValueError:
                    break
            
            nPassedReportCnt = 0
            for sReport,sRst in dictMasterReportQueue.items():
                if sRst == 'pass':
                    nPassedReportCnt += 1
            if nPassedReportCnt == len(dictMasterReportQueue): # if naver ad server answeres 'code': 11001 for all reports
                self._printDebug('all reports have been passed -> remove the job and toggle the job table')
                raise Exception('completed')
            
            if isDoneSomething == False:
                self._printDebug('did nothing -> check whether job should be removed')
                # https://godoftyping.wordpress.com/2015/04/19/python-%EB%82%A0%EC%A7%9C-%EC%8B%9C%EA%B0%84%EA%B4%80%EB%A0%A8-%EB%AA%A8%EB%93%88/
                try:
                    dtStart = datetime.strptime(self.__g_sDataLastDate, '%Y%m%d')
                except ValueError:
                    self._printDebug('Invalid start date!')

                try:
                    dtReverseEnd = datetime.strptime(self.__g_sDataFirstDate, '%Y%m%d')
                except ValueError:
                    self._printDebug('Invalid end date!')

                try:
                    isSomeReportMissed = False
                    dtDateDiff = dtStart - dtReverseEnd
                    nNumDays = int(dtDateDiff.days ) + 1
                    dateList = []
                    for x in range (0, nNumDays):
                        dtElement = dtStart - timedelta(days = x)
                        dateList.append(dtElement.strftime('%Y%m%d'))
                    
                    for sReport in lstReport:
                        for sSingleDay in dateList:
                            if aEarliestStatDate[0]['COUNT(*)'] == 0:  # if last stat date exists
                                isSomeReportMissed = True
                    
                    if isSomeReportMissed == False:
                        self._printDebug('no more report remained -> remove the job and toggle the job table')
                        # raise Exception('completed')
                        return
                except NameError:
                    self._printDebug('deny to calculate day difference')

    def __retrieveNvStatReportAct(self, o_stat_report, s_req_rpt_name, s_date_retrieval):
        """ an actual method retrieving a stat report """
        dict_rst = {'b_error': False, 's_todo': None} #, 's_msg': None}
        o_report_conf = CreateStatReportObject(s_req_rpt_name, s_date_retrieval)
        o_rst = o_stat_report.create_stat_report(o_report_conf)
        s_rpt_report_job_id = str(o_rst.reportJobId)
        s_rpt_status = o_rst.status
        s_download_url = o_rst.downloadUrl
        # print(o_rst.reportTp)  # eg 'AD'
        # o_rst.statDt, o_rst.regTm, o_rst.updateTm
        if self.__g_sNvrAdManagerLoginId != o_rst.loginId and not self.__g_bNvrAdManagerLoginIdWarned:
            self._printDebug('NVR AD manager login ID (' + self.__g_sNvrAdManagerLoginId + ') is different with NVR API returned ID (' + str(o_rst.loginId) + ')\n' + self.__g_sNvrAdManagerLoginId + ' might be a Naver ID')
            self.__g_bNvrAdManagerLoginIdWarned = True
            # dict_rst['b_error'] = True
            # dict_rst['s_todo'] = 'stop'
            # del o_rst
            # return dict_rst

        if s_rpt_status is None:
            n_error_code = o_rst.code
            if n_error_code == 11001: # http status 400 with {"code":11001,"message":"잘못된 파라미터 형식입니다."} 1년 이전의 데이터를 요청하면 발생
                dict_rst['b_error'] = True
                dict_rst['s_todo'] = 'close'
                self._printDebug(o_rst.message + ' - 너무 오래된 데이터 요청')
            if n_error_code == 20007: # "해당 일자 지표 준비중입니다."
                dict_rst['b_error'] = True
                dict_rst['s_todo'] = 'stop'
                self._printDebug(o_rst.message + ' - stop')
            del o_rst    
            return dict_rst

        while self._continue_iteration():
            if s_rpt_status == 'REGIST' or s_rpt_status == 'RUNNING' or s_rpt_status == 'WAITING':
                if self.__g_nRetryBackoffCnt < 5:
                    if self.__g_nRetryBackoffCnt > 0:
                        self._printDebug('Retry with exponential back-off')
                        self._printDebug('Wait for a report ' + s_req_rpt_name + ' with registed rpt job id=' + s_rpt_report_job_id + ', status=' + s_rpt_status)
                    
                    time.sleep((2 ** self.__g_nRetryBackoffCnt ) + random.random())
                    self.__g_nRetryBackoffCnt = self.__g_nRetryBackoffCnt + 1
                    o_rst = o_stat_report.get_stat_report(s_rpt_report_job_id)
                    s_rpt_status = o_rst.status
                    s_download_url = o_rst.downloadUrl
                    self._printDebug('NVR ad server responded with , status=' + s_rpt_status + ' requested id=' + s_rpt_report_job_id)
                else:
                    self._printDebug('exceed trial limit and giveup a report with requested id=' + s_rpt_report_job_id + ', status=' + s_rpt_status)
                    dict_rst['b_error'] = True
                    dict_rst['s_todo'] = 'pass'
                    return dict_rst
            else:
                break
        del o_rst

        if s_rpt_status == 'BUILT':
            s_rpt_filename = s_date_retrieval + '_' + s_req_rpt_name + '.tsv'
            s_download_path_abs = os.path.join(self.__g_sDownloadPathNew, s_rpt_filename)
            if os.path.isfile(s_download_path_abs):
                self._printDebug('download file duplicated - ' + s_req_rpt_name + ' report registed rpt job id=' + s_rpt_report_job_id + ' from ' + s_download_url + ' to ' + s_download_path_abs)
                s_rpt_filename = s_date_retrieval + '_' + s_req_rpt_name + '_' + self.__generate_random_str() + '.tsv'
                s_download_path_abs = os.path.join(self.__g_sDownloadPathNew, s_rpt_filename)
            
            o_stat_report.download_stat_report_by_url(s_download_url, s_download_path_abs)
            if self.__validate_downloaded_file(s_download_path_abs) is False:
                self._printDebug('NVR Ad API msg - NVR server returned BUILT but send erroronous tsv file')
                dict_rst['b_error'] = True
                dict_rst['s_todo'] = 'wait'
        elif s_rpt_status == 'AGGREGATING':
            self._printDebug('NVR Ad API msg - NVR API server is aggregating... stop')
            dict_rst['b_error'] = True
            dict_rst['s_todo'] = 'stop'
        elif s_rpt_status == 'NONE':
            pass
        else:  # $sStatus == 'ERROR' || $sStatus == 'NONE' || $sStatus == '4XX' || $sStatus == '5XX'
            # if( (int)($oOutput->get('status') / 200) != 1 ) // if status is not 2XX - wait
			# 	$this->_debug('finish retrieve '.$sReportType.' report from NVR ad server on '.$sStatDate.', transaction id '.$sTransactionId.' report job id '.$sReportJobId.' with HTTP status '.$oOutput->get('status') );
			# 	if( (int)($oOutput->get('status') / 500) == 1 )  // if status is 5XX - wait
			# 		$oOutput->add( 'todo', 'wait' );
            self._printDebug('pass and go: received ' + s_rpt_status + ' status... done')
            dict_rst['b_error'] = True
            dict_rst['s_todo'] = 'pass'

        return dict_rst

    def __validate_downloaded_file(self, s_download_path_abs):
        # PHP based BUILT but error situation 
        # array(1) { [0]=> string(140) "{"timestamp":1517205663149,"status":500,"error":"Internal Server Error","exception":"java.net.SocketException","message":"Connection reset"}" } 
        # PHP based successful situation
        # array(13) { [0]=> string(8) "20180207" [1]=> string(6) "741305" [2]=> string(27) "cmp-a001-01-000000000792959" [3]=> string(27) "grp-a001-01-000000004673859" [4]=> string(27) "nkw-a001-01-000000865469824" [5]=> string(27) "nad-a001-01-000000020447461" [6]=> string(27) "bsn-m001-00-000000001262571" [7]=> string(5) "33421" [8]=> string(1) "M" [9]=> string(1) "1" [10]=> string(1) "0" [11]=> string(1) "0" [12]=> string(1) "1" } 
        if os.path.isfile(s_download_path_abs) == False:
            return False

        with open(s_download_path_abs, "r") as f:
            reader = csv.reader(f, delimiter="\t")
            lst_csv_content = list(reader)
        
        if len(lst_csv_content[0]) == 1:  # means first line of downloaded file contains single column only; not a tsv formatted string
            lst_error_content = ['"status":500,"error":"Internal Server Error","exception":"java.net.SocketException","message":"Connection reset"', '"name":"InternalServerError","status":500,"title":"Internal Server Error","detail":"A failure of processing the request message due to the internal execution error of the NAVER Search Ads system"']
            lst_header = lst_csv_content[0][0]  # get first line of downloaded file
            for s_single_error in lst_error_content:
                if lst_header.find(s_single_error) == -1:
                    os.remove(s_download_path_abs)
                    return False

    def __generate_random_str(self, n_length = 10):
        """
        n_length:  # number of characters in the string.  
        """
        import string
        import random # define the random module  
        # call random.choices() string module to find the string in Uppercase + numeric data.  
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k = n_length))    


if __name__ == '__main__': # for console debugging and execution
    # dict_plugin_params = {'config_loc':'1/1','data_first_date':'20210930', 'data_last_date':'20211124'}
    nCliParams = len(sys.argv)
    if nCliParams > 2:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('nvad_get_period')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] [data_first_date] [data_last_date] params are required for console execution.')
