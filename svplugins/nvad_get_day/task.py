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
from datetime import datetime, timedelta
import time
import os 
import sys
import csv
import random

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_object, sv_api_config_parser, sv_plugin
    sys.path.append('../../conf') # singleview config
    import basic_config
    from powernad.API.MasterReport import *
    from powernad.Object.MasterReport.RequestObject.CreateMasterReportObject import CreateMasterReportObject
    from powernad.API.StatReport import *
    from powernad.Object.StatReport.RequestObject.CreateStatReportObject import CreateStatReportObject
else:
    from svcommon import sv_object, sv_api_config_parser, sv_plugin
    # singleview config
    from conf import basic_config # singleview config
    sys.path.append(os.path.join(os.getcwd(),'svplugins', 'nvad_get_day'))
    from powernad.API.MasterReport import *
    from powernad.Object.MasterReport.RequestObject.CreateMasterReportObject import CreateMasterReportObject
    from powernad.API.StatReport import *
    from powernad.Object.StatReport.RequestObject.CreateStatReportObject import CreateStatReportObject


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sNaveradApiBaseUrl = 'https://api.naver.com'
    __g_dtCurDatetime = None
    __g_sEncodedApiKey = None
    __g_sEncodedSecretKey = None
    __g_sNvrAdManagerLoginId = None
    __g_sRetrieveInfoPath = None
    __g_sDownloadPathNew = None
    __g_nRptWaitingSec = 60

    def __init__(self):  # , dictParams):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sVersion = '1.0.11'
        self._g_sLastModifiedDate = '11th, Apr 2019'
        self._g_oLogger = logging.getLogger(__name__ + ' v'+self._g_sVersion)
        self.__g_dtCurDatetime = datetime.now().strftime("%Y%m%d%H%M%S")

    def do_task(self):
        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self._g_dictParam['analytical_namespace'], self._g_dictParam['config_loc'])
        oResp = oSvApiConfigParser.getConfig()
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self._printDebug('stop -> invalid config_loc')
            # raise Exception('stop')
            return

        s_sv_acct_id = list(dict_acct_info.keys())[0]
        dict_nvr_ad_acct = dict_acct_info[s_sv_acct_id]['nvr_ad_acct']
        self.__g_sNvrAdManagerLoginId = dict_nvr_ad_acct['manager_login_id']
        self.__g_sEncodedApiKey = dict_nvr_ad_acct['api_key']
        self.__g_sEncodedSecretKey = dict_nvr_ad_acct['secret_key']
        self.__g_sCustomerId = dict_nvr_ad_acct['customer_id']
        
        del dict_nvr_ad_acct['manager_login_id'], dict_nvr_ad_acct['api_key'], dict_nvr_ad_acct['secret_key'], dict_nvr_ad_acct['customer_id']

        self._printDebug('-> '+ self.__g_sCustomerId +' delete master reports')
        o_master_report = MasterReport(self.__g_sNaveradApiBaseUrl, self.__g_sEncodedApiKey, self.__g_sEncodedSecretKey, self.__g_sCustomerId)
        dict_rst = o_master_report.delete_master_report_all()
        if 'transaction_id' not in list(dict_rst.keys()):
            self._printDebug('communication failed - stop')
            return
        else:
            self._printDebug('transaction id - ' + dict_rst['transaction_id'])
        # lst_master_rpt = o_master_report.get_master_report_list()
        # for o_single_rpt in lst_master_rpt:
        #     print(o_single_rpt.status)
        #     print(o_single_rpt.downloadUrl)
            
        self.__g_sRetrieveInfoPath = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', s_sv_acct_id, dict_acct_info[s_sv_acct_id]['account_title'], 'naver_ad', self.__g_sCustomerId, 'conf')
        if os.path.isdir(self.__g_sRetrieveInfoPath) == False:
            os.makedirs(self.__g_sRetrieveInfoPath)
        self.__g_sDownloadPathNew = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', s_sv_acct_id, dict_acct_info[s_sv_acct_id]['account_title'], 'naver_ad', self.__g_sCustomerId, 'data')
        if os.path.isdir(self.__g_sDownloadPathNew) == False:
            os.makedirs(self.__g_sDownloadPathNew)

        s_test_filepath = os.path.join(self.__g_sDownloadPathNew, 'write_test.tsv')
        try:
            f = open(s_test_filepath, 'w')
            os.remove(s_test_filepath)
        except PermissionError:
            self._printDebug('write on ' + self.__g_sDownloadPathNew + ' is not permitted')
            return
            
        self.__reirieveNvMasterReport(o_master_report, s_sv_acct_id, self.__g_sCustomerId, dict_nvr_ad_acct[self.__g_sCustomerId]['nvr_master_report'] )
        del o_master_report
        o_stat_report = StatReport(self.__g_sNaveradApiBaseUrl, self.__g_sEncodedApiKey, self.__g_sEncodedSecretKey, self.__g_sCustomerId)
        self.__retrieveNvStatReport(o_stat_report, s_sv_acct_id, self.__g_sCustomerId, dict_nvr_ad_acct[self.__g_sCustomerId]['nvr_stat_report']) #statdate arg should be defined
        del o_stat_report
        return        
    
    def __retrieveNvStatReport(self, o_stat_report, sSvAcctId, sNvrAdCustomerID, lstReport):
        """ retrieve NV Ad stat report """
        dictMasterReportQueue = {}
        for report in lstReport:
            # dictMasterReportQueue.update({report:0})
            dictMasterReportQueue[report] = 0
        
        nQueueLen = len(dictMasterReportQueue)
        if nQueueLen == 0:
            return

        # 네이버 stat report 보관일수
        dict_stat_rpt_expired_days = {'AD_DETAIL': 31, 'AD_CONVERSION_DETAIL': 45, 'AD': 366, 'AD_CONVERSION': 366, 'ADEXTENSION': 366, 'ADEXTENSION_CONVERSION': 366, 'EXPKEYWORD': 366, 'NAVERPAY_CONVERSION': 366}
        while True: # loop for each report type
            try:
                sTobeHandledTaskName = list(dictMasterReportQueue.keys())[list(dictMasterReportQueue.values()).index(0)] # find unhandled report task
            except ValueError:
                break

            try:
                sLatestFilepath = os.path.join(self.__g_sRetrieveInfoPath, sTobeHandledTaskName+'.latest')
                f = open(sLatestFilepath, 'r')
                sMaxReportDate = f.readline()
                dtStartRetrieval = datetime.strptime(sMaxReportDate, '%Y%m%d') + timedelta(days=1)
                f.close()
            except FileNotFoundError:
                nLatestRetrieveDate = 0
                for root, dirs, filenames in os.walk(self.__g_sDownloadPathNew):
                    for f in filenames:
                        if f.find(sTobeHandledTaskName+'.') != -1:
                            aNames = f.split('_')
                            if nLatestRetrieveDate < int(aNames[0] ):
                                nLatestRetrieveDate = int(aNames[0] )

                if nLatestRetrieveDate != 0:
                    dtStartRetrieval = datetime.strptime(str(nLatestRetrieveDate), '%Y%m%d') + timedelta(days=1) 
                else:
                    dtStartRetrieval = datetime.now() - timedelta(days=1)
            
            # requested stat date should not be later than today
            dtDateEndRetrieval = datetime.now() - timedelta(days=1) # yesterday
            dtDateDiff = dtDateEndRetrieval - dtStartRetrieval
            nNumDays = int(dtDateDiff.days) + 1
            dictDateQueue = {}
            dictDateExceptionCnt = {}  # memory to restrict exceptional occurrence count
            for x in range(0, nNumDays):
                dtElement = dtStartRetrieval + timedelta(days = x)
                dt_date_elem = dtElement.date()
                dictDateQueue[dt_date_elem] = 0
                dictDateExceptionCnt[dt_date_elem] = 0
            
            if len(dictDateQueue) == 0:
                dictMasterReportQueue[sTobeHandledTaskName] = 1
                continue
            
            while True: # loop for each report date
                try:
                    dtDateRetrieval = list(dictDateQueue.keys())[list(dictDateQueue.values()).index(0)] # find unhandled report task
                    self._printDebug('--> singleview account id: ' + sSvAcctId + ' nvr ad id: ' + sNvrAdCustomerID +' will retrieve stat report - ' + sTobeHandledTaskName +' on ' + dtDateRetrieval.strftime('%Y%m%d'))
                    dt_today = datetime.today().date()
                    dt_delta = dt_today - dtDateRetrieval
                    if dt_delta.days <= 0:
                        self._printDebug('NVR Ad API msg - stat report ' + sTobeHandledTaskName + ' has been disallowed. reason is request for today or future!')
                        dict_rst['b_error'] = True
                        dict_rst['s_todo'] = 'pass'
                        return dict_rst

                    n_expired_days = dict_stat_rpt_expired_days[sTobeHandledTaskName]
                    if dt_delta.days <= -n_expired_days: 
                        self._printDebug('NVR Ad API msg - stat report ' + sTobeHandledTaskName + ' has been disallowed. reason is ' + n_expired_days + ' days ago from today!')
                        continue
                        
                    ###################################
                    sDateRetrieval = dtDateRetrieval.strftime('%Y%m%d')
                    dict_rst = self.__retrieveNvStatReportAct(o_stat_report, sTobeHandledTaskName, sDateRetrieval) # '20190202') #
                    ###################################
                    if dict_rst['b_error'] is True:
                        if dictDateExceptionCnt[dtDateRetrieval] < 3: # allow exception occurrence count to 3 by each report
                            dictDateExceptionCnt[dtDateRetrieval] += 1
                            # nWaitSec = 60
                            if dict_rst['s_todo']:  # if todo is defined
                                s_todo = dict_rst['s_todo']
                                if s_todo == 'wait':
                                    self._printDebug('wait ' + str(self.__g_nRptWaitingSec) + ' sec and go')
                                    time.sleep(self.__g_nRptWaitingSec)
                                    continue
                                elif s_todo == 'pass':
                                    self._printDebug('pass and go')
                                    dictDateQueue[dtDateRetrieval] = 1
                                    continue
                                elif s_todo == 'stop':
                                    self._printDebug('stop')
                                    dictDateQueue[dtDateRetrieval] = 1
                                    return
                            else:  # default is waiting if todo is not defined
                                self._printDebug('error occured but todo is not defined -> wait ' + str(self.__g_nRptWaitingSec) + ' sec and go')
                                time.sleep(self.__g_nRptWaitingSec)
                                continue
                        else:
                            self._printDebug('too many exceptions raise exception!!')
                            dictDateQueue[dtDateRetrieval] = 1
                            continue    
                    else:
                        dictDateQueue[dtDateRetrieval] = 1
                        f = open(sLatestFilepath, 'w')
                        f.write(sDateRetrieval)
                        f.close()
                        continue
                except ValueError:
                    break

    def __retrieveNvStatReportAct(self, o_stat_report, s_req_rpt_name, s_date_retrieval):
        dict_rst = {'b_error': False, 's_todo': None} #, 's_msg': None}

        o_report_conf = CreateStatReportObject(s_req_rpt_name, s_date_retrieval)
        o_rst = o_stat_report.create_stat_report(o_report_conf)
        s_rpt_report_job_id = str(o_rst.reportJobId)
        s_rpt_status = o_rst.status
        s_download_url = o_rst.downloadUrl
        # print(o_rst.reportTp)  # eg 'AD'
        # o_rst.statDt, o_rst.regTm, o_rst.updateTm

        if self.__g_sNvrAdManagerLoginId != o_rst.loginId:
            self._printDebug('NVR AD manager login ID is different: ' + self.__g_sNvrAdManagerLoginId + ' on bog, NVR API returned ' + o_rst.loginId)
            dict_rst['b_error'] = True
            dict_rst['s_todo'] = 'stop'
            del o_rst
            return dict_rst

        if s_rpt_status is None:
			#$oOutput->add('status', 'unhandled');
			#$sStatus = 'unhandled';
            n_error_code = o_rst.code
            if n_error_code == 11001: # http status 400 with {"code":11001,"message":"잘못된 파라미터 형식입니다."} 1년 이전의 데이터를 요청하면 발생
                dict_rst['b_error'] = True
                dict_rst['s_todo'] = 'pass'
                self._printDebug(o_rst.message + ' - 1년 이전의 데이터 요청')
            if n_error_code == 20007: # "해당 일자 지표 준비중입니다."
                dict_rst['b_error'] = True
                dict_rst['s_todo'] = 'stop'
                self._printDebug(o_rst.message + ' - stop')
            del o_rst    
            return dict_rst

        nRetryBackoffCnt = 0
        while True:
            if s_rpt_status == 'REGIST' or s_rpt_status == 'RUNNING' or s_rpt_status == 'WAITING':
                if nRetryBackoffCnt < 5:
                    self._printDebug('start retrying with exponential back-off')
                    self._printDebug('waiting for a report ' + s_req_rpt_name + ' with registed rpt job id=' + s_rpt_report_job_id + ', status=' + s_rpt_status)
                    time.sleep((2 ** nRetryBackoffCnt ) + random.random())
                    nRetryBackoffCnt = nRetryBackoffCnt + 1
                    o_rst = o_stat_report.get_stat_report(s_rpt_report_job_id)
                    s_rpt_status = o_rst.status
                    s_download_url = o_rst.downloadUrl
                    self._printDebug('received respond from NVR ad server for ' + s_req_rpt_name + ' with registed rpt job id=' + s_rpt_report_job_id + ', status=' + s_rpt_status)
                else:
                    self._printDebug('exceed trial limit and giveup a report ' + s_req_rpt_name + ' with registed rpt job id=' + s_rpt_report_job_id + ', status=' + s_rpt_status)
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
        else:  # $sStatus == 'ERROR' || $sStatus == 'NONE' || $sStatus == '4XX' || $sStatus == '5XX'
            # if( (int)($oOutput->get('status') / 200) != 1 ) // if status is not 2XX - wait
			# 	$this->_debug('finish retrieve '.$sReportType.' report from NVR ad server on '.$sStatDate.', transaction id '.$sTransactionId.' report job id '.$sReportJobId.' with HTTP status '.$oOutput->get('status') );
			# 	if( (int)($oOutput->get('status') / 500) == 1 )  // if status is 5XX - wait
			# 		$oOutput->add( 'todo', 'wait' );
            self._printDebug('received ' + s_rpt_status + ' status... done')
            dict_rst['b_error'] = True
            dict_rst['s_todo'] = 'pass'

        return dict_rst

    def __reirieveNvMasterReport(self, o_master_report, sSvAcctId, sNvrAdCustomerID, lstReport):
        """ retrieve NV Ad mster report """
        dictMasterRereportQueue = {}
        dictMasterRereportExceptionCnt = {}  # memory to restrict exceptional occurrence count
        for report in lstReport:
            dictMasterRereportQueue[report] = 0
            dictMasterRereportExceptionCnt[report] = 0
        
        nQueueLen = len(dictMasterRereportQueue)
        if nQueueLen == 0:
            return

        lst_master_rpt_daily = ['BusinessChannel', 'Campaign', 'CampaignBudget', 'Adgroup', 'AdgroupBudget', 'Keyword', 'Ad','AdExtension']
        while True:
            try:
                sTobeHandledTaskName = list(dictMasterRereportQueue.keys())[list(dictMasterRereportQueue.values()).index(0)] # find unhandled report task
            except ValueError:
                if sTobeHandledTaskName == 'Qi':
                    self._printDebug('-> '+ sNvrAdCustomerID + ' completed: retrieve master reports!')
                    break
                else:
                    self._printDebug('-> '+ sNvrAdCustomerID + ' failed: retrieve ' + sTobeHandledTaskName + ' master reports!')
                    return # raise Exception('stop')

            # if sTobeHandledTaskName: # if there exists unhandled report task
            dtFromRetrieval = None
            try:
                sLatestFilepath = os.path.join(self.__g_sRetrieveInfoPath, sTobeHandledTaskName+'.latest')
                f = open(sLatestFilepath, 'r')
                sMaxReportDate = f.readline()
                dtFromRetrieval = datetime.strptime(sMaxReportDate, '%Y%m%d%H%M%S')
                f.close()
            except FileNotFoundError:
                nLatestRetrieveDate = 0
                for root, dirs, filenames in os.walk(self.__g_sDownloadPathNew):
                    for f in filenames:
                        if f.find(sTobeHandledTaskName+'_') != -1:
                            aNames = f.split('_')
                            if nLatestRetrieveDate < int(aNames[0]):
                                nLatestRetrieveDate = int(aNames[0])
                if nLatestRetrieveDate != 0:
                    dtFromRetrieval = datetime.strptime(str(nLatestRetrieveDate), '%Y%m%d%H%M%S') + timedelta(days=1) 

            # 2021-08-17T10:33:24+09:00
            # print(datetime.now(timezone('Asia/Seoul')).isoformat(timespec='seconds'))
            # print(dtFromRetrieval.replace(tzinfo=timezone('Asia/Seoul')).isoformat(timespec='seconds'))
            self._printDebug( '--> singleview account id: ' + sSvAcctId + ' nvr ad id: ' + sNvrAdCustomerID +' retrieve master reports - ' + sTobeHandledTaskName )
            if dtFromRetrieval is not None:
                dt_today = datetime.today().date()
                dt_from_retrieval = dtFromRetrieval.date()
                if sTobeHandledTaskName in lst_master_rpt_daily:
                    if dt_from_retrieval >= dt_today:
                        self._printDebug('NVR Ad API msg - ' + sTobeHandledTaskName + ' report retrieval is allowed once a day! last retrieval datetime was ' + dtFromRetrieval.strftime('%Y%m%d%H%M%S'))
                        dictMasterRereportQueue[sTobeHandledTaskName] = 1
                        continue
                elif sTobeHandledTaskName == 'Qi':  # 품질지수 보고서는 1주일에 한번만
                    dt_delta = dt_today - dt_from_retrieval
                    if dt_delta.days <= 7:
                        self._printDebug('NVR Ad API msg - ' + sTobeHandledTaskName + ' report retrieval is allowed once a week')
                        dictMasterRereportQueue[sTobeHandledTaskName] = 1
                        continue
                    pass
            #############################
            dict_rst = self.__reirieveNvMasterReportAct(o_master_report, sTobeHandledTaskName, dtFromRetrieval)
            ###########################################
            if dict_rst['b_error'] == True:
                if dictMasterRereportExceptionCnt[sTobeHandledTaskName] < 3: # allow exception occurrence count to 3 by each report
                    dictMasterRereportExceptionCnt[sTobeHandledTaskName] += 1
                    nW1aitSec = 60
                    if dict_rst['s_todo']:  # if todo is defined
                        s_todo = dict_rst['s_todo']
                        if s_todo == 'wait':
                            self._printDebug('wait ' + str(self.__g_nRptWaitingSec) + ' sec and go')
                            time.sleep(self.__g_nRptWaitingSec)
                            continue
                        elif s_todo == 'pass':
                            self._printDebug('pass and go')
                            dictMasterRereportQueue[sTobeHandledTaskName] = 1
                            continue
                        elif s_todo == 'stop':
                            self._printDebug('stop')
                            dictMasterRereportQueue[sTobeHandledTaskName] = 1
                            return
                    else:  # default is waiting if todo is not defined
                        self._printDebug('error occured but todo is not defined -> wait ' + str(self.__g_nRptWaitingSec) + ' sec and go')
                        time.sleep(self.__g_nRptWaitingSec)
                        continue
                else:
                    self._printDebug('too many exceptions raise exception!!')
                    dictMasterRereportQueue[sTobeHandledTaskName] = 1
                    continue    
            else:
                dictMasterRereportQueue[sTobeHandledTaskName] = 1
                sDateRetrieval = time.strftime('%Y%m%d%H%M%S')
                f = open(sLatestFilepath, 'w')
                f.write(sDateRetrieval)
                f.close()
                continue
            return
            
    def __reirieveNvMasterReportAct(self, o_master_report, s_req_rpt_name, dt_from_retrieval):
        dict_rst = {'b_error': False, 's_todo': None}
        o_report_conf = CreateMasterReportObject(s_req_rpt_name, dt_from_retrieval)
        o_rst = o_master_report.create_master_report(o_report_conf)
        s_rpt_id = o_rst.id
        s_rpt_status = o_rst.status
        if self.__g_sNvrAdManagerLoginId != o_rst.managerLoginId:
            self._printDebug('NVR AD manager login ID is different: ' + self.__g_sNvrAdManagerLoginId + ' on bog, NVR API returned ' + o_rst.managerLoginId)
            dict_rst['b_error'] = True
            dict_rst['s_todo'] = 'pass'
            del o_rst
            return dict_rst
        del o_rst
        
        nRetryBackoffCnt = 0
        while True:
            if s_rpt_status == 'REGIST' or s_rpt_status == 'RUNNING':
                if nRetryBackoffCnt < 5:
                    self._printDebug('start retrying with exponential back-off')
                    self._printDebug('waiting for a report ' + s_req_rpt_name + ' with registed id=' + s_rpt_id + ', status=' + s_rpt_status)
                    time.sleep((2 ** nRetryBackoffCnt ) + random.random())
                    nRetryBackoffCnt = nRetryBackoffCnt + 1
                    o_rst = o_master_report.get_master_report_by_id(s_rpt_id)
                    s_rpt_status = o_rst.status
                    s_download_url = o_rst.downloadUrl
                    self._printDebug('received respond from NVR ad server for ' + s_req_rpt_name + ' with registed id=' + s_rpt_id + ', status=' + s_rpt_status)
                else:
                    self._printDebug('exceed trial limit and giveup a report ' + s_req_rpt_name + ' with registed id=' + s_rpt_id + ', status=' + s_rpt_status)
                    dict_rst['b_error'] = True
                    dict_rst['s_todo'] = 'pass'
                    del o_rst
                    return dict_rst
            else:
                break
        del o_rst

        if s_rpt_status == 'BUILT':
            s_rpt_type = 'delta' if dt_from_retrieval is not None else 'full'
            s_rpt_filename = str(self.__g_dtCurDatetime) + '_' + s_req_rpt_name + '_' + s_rpt_type + '.tsv'
            s_download_path_abs = os.path.join(self.__g_sDownloadPathNew, s_rpt_filename)
            if os.path.isfile(s_download_path_abs):
                self._printDebug('download file duplicated - ' + s_req_rpt_name + ' report registed id=' + s_rpt_id + ' from ' + s_download_url + ' to ' + s_download_path_abs)
                s_rpt_filename = str(self.__g_dtCurDatetime) + '_' + s_req_rpt_name + '_' + s_rpt_type + '_' + self.__generate_random_str() + '.tsv'
                s_download_path_abs = os.path.join(self.__g_sDownloadPathNew, s_rpt_filename)
            
            o_master_report.download_master_report_by_url(s_download_url, s_download_path_abs)
            if self.__validate_downloaded_file(s_download_path_abs) is False:
                self._printDebug('NVR Ad API msg - NVR server returned BUILT but send erroronous tsv file')
                dict_rst['b_error'] = True
                dict_rst['s_todo'] = 'wait'
        else:
            self._printDebug('received ' + s_rpt_status + ' status... done')
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
    dictPluginParams = {'config_loc':None} # {'config_loc':'1/test_acct'}
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        for i in range(nCliParams):
            if i is 0:
                continue

            sArg = sys.argv[i]
            for sParamName in dictPluginParams:
                nIdx = sArg.find( sParamName + '=' )
                if nIdx > -1:
                    aModeParam = sArg.split('=')
                    dictPluginParams[sParamName] = aModeParam[1]
                
        #print( dictPluginParams )
        with svJobPlugin(dictPluginParams) as oJob: # to enforce to call plugin destructor
            oJob.do_task()
    else:
        print( 'warning! [config_loc] params are required for console execution.' )
