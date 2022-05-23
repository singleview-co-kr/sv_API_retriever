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

# refer to https://github.com/google/google-api-python-client/tree/master/samples/analytics
# you firstly need to install by cmd "pip3.6 install --upgrade google-api-python-client"
# refer to https://developers.google.com/analytics/devguides/config/mgmt/v3/quickstart/installed-py
# to create desinated credential refer to https://console.developers.google.com/apis/credentials
# to get console credential you firstly need to run with the option --noauth_local_webserver 
# to monitor API traffic refer to https://console.developers.google.com/apis/api/analytics.googleapis.com/quotas?project=svgastudio

# standard library
import logging
import datetime
import sys
import re
import os
import csv
import calendar
import codecs
import math

# 3rd party library

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    sys.path.append('../../svload/pandas_plugins')
    sys.path.append('../../svdjango')
    import sv_mysql
    import sv_campaign_parser
    import sv_object
    import sv_plugin
    import contract
    import settings
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_object
    from svcommon import sv_plugin
    from svload.pandas_plugins import contract
    from django.conf import settings


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_oSvCampaignParser = sv_campaign_parser.SvCampaignParser()  #None
    __g_nPnsTouchingDate = 20190126 # to seperate the old & non-systematic & complicated situation for PNS cost process
    __g_dictNvPnsUaCostPortion = {'M':0.7, 'P':0.3} # sum must be 1
    __g_sSvNull = '#%'

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_oLogger = logging.getLogger(__name__ + ' modified at 23rd, May 2022')
        self._g_dictParam.update({'yyyymm':None, 'mode':None})
        # Declaring a dict outside of __init__ is declaring a class-level variable.
        # It is only created once at first, 
        # whenever you create new objects it will reuse this same dict. 
        # To create instance variables, you declare them with self in __init__.
        self.__g_sMode = None
        self.__g_bFbProcess = False
        self.__g_bGoogleAdsProcess = False
        self.__g_sDataPath = None
        self.__g_sRetrieveMonth = None
        self.__g_sNvrPnsInfoFilePath = None
        self.__g_sFbPnsInfoFilePath = None
        self.__g_dictNvadMergedDailyLog = None
        self.__g_dictAdwMergedDailyLog = None
        self.__g_dictKkoMergedDailyLog = None
        self.__g_dictYtMergedDailyLog = None
        self.__g_dictFbMergedDailyLog = None
        self.__g_dictOtherMergedDailyLog = None
        self.__g_dictPnsSource = None
        self.__g_dictPnsContract = None        

    def __del__(self):
        """ never place self._task_post_proc() here 
            __del__() is not executed if try except occurred """
        self.__g_sMode = None
        self.__g_bFbProcess = False
        self.__g_bGoogleAdsProcess = False
        self.__g_sDataPath = None
        self.__g_sRetrieveMonth = None
        self.__g_sNvrPnsInfoFilePath = None
        self.__g_sFbPnsInfoFilePath = None
        self.__g_dictNvadMergedDailyLog = None
        self.__g_dictAdwMergedDailyLog = None
        self.__g_dictKkoMergedDailyLog = None
        self.__g_dictYtMergedDailyLog = None
        self.__g_dictFbMergedDailyLog = None
        self.__g_dictOtherMergedDailyLog = None
        self.__g_dictPnsSource = None
        self.__g_dictPnsContract = None

    def do_task(self, o_callback):
        self._g_oCallback = o_callback
        self.__g_sRetrieveMonth = self._g_dictParam['yyyymm']
        self.__g_sMode = self._g_dictParam['mode']
        
        dict_acct_info = self._task_pre_proc(o_callback)
        if 'sv_account_id' not in dict_acct_info and 'brand_id' not in dict_acct_info and \
          'nvr_ad_acct' not in dict_acct_info:
            self._printDebug('stop -> invalid config_loc')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return
        s_sv_acct_id = dict_acct_info['sv_account_id']
        s_brand_id = dict_acct_info['brand_id']
        dict_nvr_ad_acct = dict_acct_info['nvr_ad_acct']
        self.__g_sTblPrefix = dict_acct_info['tbl_prefix']
        s_cid = dict_nvr_ad_acct['customer_id']
        self.__g_sDataPath = os.path.join(self._g_sAbsRootPath, settings.SV_STORAGE_ROOT, s_sv_acct_id, s_brand_id)
        self.__g_sNvrPnsInfoFilePath = os.path.join(self.__g_sDataPath, 'naver_ad', s_cid, 'conf', 'contract_pns_info.tsv')
        self.__g_sFbPnsInfoFilePath = os.path.join(self.__g_sDataPath, 'fb_biz', dict_acct_info['fb_biz_aid'], 'conf', 'contract_pns_info.tsv')

        with sv_mysql.SvMySql() as oSvMysql:
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.integrate_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
        if self.__g_sRetrieveMonth != None:
            dictDateRange = self.__deleteCertainMonth()
        else:
            dictDateRange = self.__getTouchDateRange()
        
        if dictDateRange['start_date'] is None and dictDateRange['end_date'] is None:
            self._printDebug('stop - weird raw data last date')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        start = dictDateRange['start_date']
        end = dictDateRange['end_date']
        if start > end:
            self._printDebug('error - weird raw data last date')
            self._task_post_proc(self._g_oCallback)
            if self._g_bDaemonEnv:  # for running on dbs.py only
                raise Exception('remove')
            else:
                return

        o_pns_info = contract.PnsInfo()
        self.__g_dictPnsSource = o_pns_info.get_inverted_source_type_dict()
        self.__g_dictPnsContract = o_pns_info.get_contract_type_dict()
        del o_pns_info

        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]
        nIdx = 0
        nSentinel = len(date_generated)
        for date in date_generated:
            if not self._continue_iteration():
                break
            
            sDate = date.strftime('%Y-%m-%d')
            self.__compileDailyRecord(sDate)
            self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data:', suffix = 'Complete', length = 50)
            nIdx += 1
        
        self._task_post_proc(self._g_oCallback)

    def __deleteCertainMonth(self):
        dictRst = {'start_date': None, 'end_date': None}
        nYr = int(self.__g_sRetrieveMonth[:4])
        nMo = int(self.__g_sRetrieveMonth[4:None])
        try:
            lstMonthRange = calendar.monthrange(nYr, nMo)
        except calendar.IllegalMonthError:
            self._printDebug( 'invalid yyyymm' )
            return dictRst
        
        try:
            lstDirectory = os.listdir(os.path.join(self.__g_sDataPath, 'adwords'))
            self.__g_bGoogleAdsProcess = True
        except FileNotFoundError:
            pass

        try:
            lstDirectory = os.listdir(os.path.join(self.__g_sDataPath, 'fb_biz'))
            for sCid in lstDirectory:
                if sCid == 'alias_info_campaign.tsv':
                    continue
                try:
                    open(os.path.join(self.__g_sDataPath, 'fb_biz', sCid, 'conf', 'general.latest'))
                    self.__g_bFbProcess = True
                except FileNotFoundError:
                    pass
        except FileNotFoundError:
            pass
        
        sStartDateRetrieval = self.__g_sRetrieveMonth[:4] + '-' + self.__g_sRetrieveMonth[4:None] + '-01'
        sEndDateRetrieval = self.__g_sRetrieveMonth[:4] + '-' + self.__g_sRetrieveMonth[4:None] + '-' + str(lstMonthRange[1])
        with sv_mysql.SvMySql() as oSvMysql:
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.integrate_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            oSvMysql.executeQuery('deleteCompiledGaMediaLogByPeriod', sStartDateRetrieval, sEndDateRetrieval)

        dictRst['start_date'] = datetime.datetime.strptime(sStartDateRetrieval, '%Y-%m-%d')
        dictRst['end_date'] = datetime.datetime.strptime(sEndDateRetrieval, '%Y-%m-%d')
        return dictRst  # {'start_date': dtStartDateRetrieval, 'end_date':dtEndDateRetrieval}

    def __getTouchDateRange(self):
        """ get latest retrieval info of naver_ad """
        dictRst = {'start_date': None, 'end_date': None}
        lstDirectory = os.listdir(os.path.join(self.__g_sDataPath, 'naver_ad'))
        lstNaveradLastDate = []
        try:
            lstDirectory.remove('alias_info_adgrp.tsv')
        except ValueError:
            pass
        for sCid in lstDirectory:
            # check naver_ad agency_info.tsv
            dictCostRst = self.__redefineCost('naver_ad', sCid, 100)  # 100 is dummy to check agency_info.tsv file
            if dictCostRst['cost'] == 0 and dictCostRst['agency_fee'] == 0 and dictCostRst['vat'] == 0:
                return dictRst
            file = open(os.path.join(self.__g_sDataPath, 'naver_ad', sCid, 'conf', 'AD.latest'), 'r') 
            sLatestDate = file.read()
            dtLatest = datetime.date(int(sLatestDate[:4]), int(sLatestDate[4:6]), int(sLatestDate[6:8]))
            lstNaveradLastDate.append(dtLatest)
            
        # get latest retrieval info of adwords
        try:
            lstDirectory = os.listdir(os.path.join(self.__g_sDataPath, 'adwords'))
            self.__g_bGoogleAdsProcess = True
        except FileNotFoundError:
            pass
        
        lstAdwordsLastDate = []
        if self.__g_bGoogleAdsProcess:
            try:
                lstDirectory.remove('alias_info_campaign.tsv')
            except ValueError:
                pass
            for sCid in lstDirectory:
                # check adwords agency_info.tsv
                dictCostRst = self.__redefineCost('adwords', sCid, 100)  # 100 is dummy to check agency_info.tsv file
                if dictCostRst['cost'] == 0 and dictCostRst['agency_fee'] == 0 and dictCostRst['vat'] == 0:
                    return dictRst
                file = open(os.path.join(self.__g_sDataPath, 'adwords', sCid, 'conf', 'general.latest'), 'r') 
                sLatestDate = file.read()
                dtLatest = datetime.date(int(sLatestDate[:4]), int(sLatestDate[4:6]), int(sLatestDate[6:8]))
                lstAdwordsLastDate.append(dtLatest)
        
        # get latest retrieval info of fb_biz
        b_fb_biz_proc = True
        try:
            lstDirectory = os.listdir(os.path.join(self.__g_sDataPath, 'fb_biz'))
        except FileNotFoundError:
            b_fb_biz_proc = False
            self.__g_bFbProcess = False
        lstFbbizLastDate = []
        if b_fb_biz_proc:
            for sCid in lstDirectory:
                if sCid == 'alias_info_campaign.tsv':
                    continue
                # check agency_info.tsv
                dictCostRst = self.__redefineCost('fb_biz', sCid, 100)  # 100 is dummy to check agency_info.tsv file
                if dictCostRst['cost'] == 0 and dictCostRst['agency_fee'] == 0 and dictCostRst['vat'] == 0:
                    return dictRst
                try:
                    file = open(os.path.join(self.__g_sDataPath, 'fb_biz', sCid, 'conf', 'general.latest'), 'r')
                    sLatestDate = file.read()
                    dtLatest = datetime.date(int(sLatestDate[:4]), int(sLatestDate[4:6]), int(sLatestDate[6:8]))
                    lstFbbizLastDate.append(dtLatest)
                    self.__g_bFbProcess = True
                except FileNotFoundError:
                    pass
        if self.__g_sMode == 'ignore_fb':
            self.__g_bFbProcess = False

        with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.integrate_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            # init rst dictionary
            dictRst = {}
            # define last date of process
            lstLastDate = []
            lstGaLogDateRange = oSvMysql.executeQuery('getGaLogDateMaxMin')
            lstLastDate.append(lstGaLogDateRange[0]['maxdate'])

            if self.__g_bGoogleAdsProcess:
                lstLastDate.append(max(lstAdwordsLastDate))

            lstLastDate.append(max(lstNaveradLastDate))
            dictRst['end_date'] = min(lstLastDate)
            # define first date of process
            lstFirstDate = []
            lstGrossCompiledLogDateRange = oSvMysql.executeQuery('getCompiledGaMediaLogDateMaxMin')
            if lstGrossCompiledLogDateRange[0]['maxdate'] is None:
                self._printDebug( 'zero base')
                # decide that GA data period is a population on 20190216
                lstFirstDate.append(lstGaLogDateRange[0]['mindate'])
                dictRst['start_date'] = max(lstFirstDate)
            else:
                dictRst['start_date'] = lstGrossCompiledLogDateRange[0]['maxdate'] + datetime.timedelta(days=1)

        return dictRst

    def __compileDailyRecord(self, sTouchingDate):
        try: # validate requsted date
            datetime.datetime.strptime(sTouchingDate, '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            self._printDebug(sTouchingDate + ' is invalid date string')
            return

        # clear log
        self.__g_dictNvadMergedDailyLog = {}  
        self.__g_dictAdwMergedDailyLog = {}
        self.__g_dictKkoMergedDailyLog = {}
        self.__g_dictYtMergedDailyLog = {}
        self.__g_dictFbMergedDailyLog = {}
        self.__g_dictOtherMergedDailyLog = {}
        # check non integrated date
        with sv_mysql.SvMySql() as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.set_app_name('svplugins.integrate_db')
            oSvMysql.initialize(self._g_dictSvAcctInfo)
            lstGaLogDaily = oSvMysql.executeQuery('getGaLogDaily', sTouchingDate)
            #if sTouchingDate == '2019-01-27':
            for lstGaLog in lstGaLogDaily:
                # get naver ad media data
                if lstGaLog['source'] == 'naver' and lstGaLog['rst_type'] == 'PS':
                    self.__cleanupNvAdGaRaw(lstGaLog, oSvMysql, sTouchingDate)
                elif self.__g_bGoogleAdsProcess and lstGaLog['source'] == 'google' and lstGaLog['rst_type'] == 'PS':
                    self.__cleanupAdwGaRaw(lstGaLog)
                elif self.__g_bGoogleAdsProcess and lstGaLog['source'] == 'youtube' and lstGaLog['rst_type'] == 'PS':
                    self.__cleanupYtGaRaw(lstGaLog)
                elif lstGaLog['source'] == 'kakao' and lstGaLog['rst_type'] == 'PS':
                    self.__cleanupKkoGaRaw(lstGaLog)
                elif self.__g_bFbProcess and lstGaLog['source'] == 'facebook' or lstGaLog['source'] == 'instagram':
                    if lstGaLog['rst_type'] == 'PS':
                        self.__cleanupFbGaRaw(lstGaLog)
                else: # others
                    self.__cleanupOtherGaRaw(lstGaLog, sTouchingDate)
            
            if len(self.__g_dictNvadMergedDailyLog) > 0: # only if nvad log exists
                self.__mergeNvAdGaRaw(oSvMysql, sTouchingDate)
            if len(self.__g_dictAdwMergedDailyLog) > 0: # only if google ads log exists
                self.__mergeAdwGaRaw(oSvMysql, sTouchingDate)
            if len(self.__g_dictYtMergedDailyLog) > 0: # only if YT ads exists
                self.__mergeYtGaRaw(oSvMysql, sTouchingDate)
            if len(self.__g_dictFbMergedDailyLog) > 0: # only if facebook business log exists
                self.__mergeFbGaRaw(oSvMysql, sTouchingDate)
            if len(self.__g_dictKkoMergedDailyLog) > 0: # only if kakao moment log exists
                self.__mergeKkoGaRaw(oSvMysql, sTouchingDate)
            
            self.__mergeOtherGaRaw(oSvMysql, sTouchingDate)

    def __cleanupFbGaRaw(self, lstGaLogSingle):  #, oSvMysql, sTouchingDate):
        sTerm = lstGaLogSingle['term'].replace(' ', '').upper() # facebook never provides term via their insight API
        # but sometimes set utm_term on their campaign, then GA report term-level specified FB campaign
        # that's why GA term needs to be capitalized before integrate with FB log
        sRowId = lstGaLogSingle['ua']+'|@|'+lstGaLogSingle['source']+'|@|'+lstGaLogSingle['rst_type']+'|@|'+lstGaLogSingle['media']+'|@|'+str(lstGaLogSingle['brd'])+'|@|'+ \
            lstGaLogSingle['campaign_1st']+'|@|'+lstGaLogSingle['campaign_2nd']+'|@|'+ lstGaLogSingle['campaign_3rd'] +'|@|'+sTerm
        
        nSession = int(lstGaLogSingle['session'])
        fNewPer = float(lstGaLogSingle['new_per'])/100
        fBouncePer = float(lstGaLogSingle['bounce_per'])/100
        fDurSec = float(lstGaLogSingle['duration_sec'])
        fPvs = float(lstGaLogSingle['pvs'])
        nTrs = int(lstGaLogSingle['transactions'])
        nRev = int(lstGaLogSingle['revenue'])
        nReg = int(lstGaLogSingle['registrations'])
        fTotNew = nSession * fNewPer
        fTotBounce = nSession * fBouncePer
        fTotDurSec = nSession * fDurSec
        fTotPvs = nSession * fPvs
        if self.__g_dictFbMergedDailyLog.get(sRowId, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictFbMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictFbMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictFbMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictFbMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictFbMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictFbMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictFbMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictFbMergedDailyLog[sRowId]['tot_registrations'] += nReg
        else:  # if new log requested
            self.__g_dictFbMergedDailyLog[sRowId] = {
                'session':nSession,'tot_new_session':fTotNew,'tot_bounce':fTotBounce,'tot_duration_sec':fTotDurSec,
                'tot_pvs':fTotPvs,'tot_transactions':nTrs,'tot_revenue':nRev,'tot_registrations':nReg
            }

    def __mergeFbGaRaw(self, oSvMysql, sTouchingDate):
        dictFbLogDailyLogSrl = {}
        lstFbLogDaily = oSvMysql.executeQuery('getFbIgLogDaily', sTouchingDate)
        for dictSingleLog in lstFbLogDaily:
            dictFbLogDailyLogSrl[dictSingleLog['log_srl']] = {
                'biz_id':dictSingleLog['biz_id'], 'ua':dictSingleLog['ua'], 'source':dictSingleLog['source'], 'rst_type':dictSingleLog['rst_type'],
                'media':dictSingleLog['media'],'brd':dictSingleLog['brd'],'campaign_1st':dictSingleLog['campaign_1st'],
                'campaign_2nd':dictSingleLog['campaign_2nd'],'campaign_3rd':dictSingleLog['campaign_3rd'],
                'cost':dictSingleLog['cost'],'imp':dictSingleLog['imp'],'click':dictSingleLog['click'],
                'conv_cnt':dictSingleLog['conv_cnt'],'conv_amnt':dictSingleLog['conv_amnt']
            }
        
        # sort FB log dictionary by session number; FB sometimes reports utm_term and GA separate FB campaign code by utm_term
        # dictFbMergedDailyLogSortBySession = sorted(self.__g_dictFbMergedDailyLog.items(), key=(lambda x:x[1]['session']), reverse=True)
        for s_unique_tag, dict_row in self.__g_dictFbMergedDailyLog.items():
            aRowId = s_unique_tag.split('|@|')
            sUa = aRowId[0]
            sSource = aRowId[1]
            sRstType = aRowId[2]
            sMedia = aRowId[3]
            sBrd = aRowId[4]
            sCamp1st = aRowId[5]
            sCamp2nd = aRowId[6]
            sCamp3rd = aRowId[7]
            sTerm = aRowId[8]
            lstFbLogDaily = oSvMysql.executeQuery('getFbIgLogSpecific', sTouchingDate, sMedia, sSource, sCamp1st, sCamp2nd, sCamp3rd, sUa)
            nRecCnt = len(lstFbLogDaily)
            if nRecCnt == 0:
                lstFbLogDaily = oSvMysql.executeQuery('getFbIgLogSpecific', sTouchingDate, 'cpc', sSource, sCamp1st, sCamp2nd, sCamp3rd, sUa)
                nRecCnt = len(lstFbLogDaily)
                if nRecCnt == 1:
                    dictFbLogDailyLogSrl.pop(lstFbLogDaily[0]['log_srl'])
                    dictCostRst = self.__redefineCost('fb_biz', lstFbLogDaily[0]['biz_id'], lstFbLogDaily[0]['cost'])
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), 
                        lstFbLogDaily[0]['imp'],lstFbLogDaily[0]['click'], lstFbLogDaily[0]['conv_cnt'], lstFbLogDaily[0]['conv_amnt'], 
                        dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                        dict_row['tot_pvs'], dict_row['tot_transactions'], dict_row['tot_revenue'], dict_row['tot_registrations'],
                        sTouchingDate)
                
                if nRecCnt == 0:
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        0,0,0,0,0,0,0,
                        dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                        dict_row['tot_pvs'], dict_row['tot_transactions'], dict_row['tot_revenue'], dict_row['tot_registrations'],
                        sTouchingDate)
            elif nRecCnt == 1:
                try: # if designated log exists
                    dictFbLogDailyLogSrl.pop(lstFbLogDaily[0]['log_srl'])
                    dictCostRst = self.__redefineCost('fb_biz', lstFbLogDaily[0]['biz_id'], lstFbLogDaily[0]['cost'])
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        str(dictCostRst['cost']), str(dictCostRst['agency_fee']), 
                        str(dictCostRst['vat']), lstFbLogDaily[0]['imp'], lstFbLogDaily[0]['click'],
                        lstFbLogDaily[0]['conv_cnt'], lstFbLogDaily[0]['conv_amnt'], 
                        dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                        dict_row['tot_pvs'], dict_row['tot_transactions'], dict_row['tot_revenue'], dict_row['tot_registrations'],
                        sTouchingDate)
                except KeyError: 
                    # if designated log has been already popped; FB sometimes reports utm_term and GA separate FB campaign code by utm_term;
                    # self.__g_dictFbMergedDailyLog already has been sorted by session number; 
                    # as a result specified cost already has been allocated to the most-sessioned utm_term
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        0,0,0,0,0,0,0,
                        dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                        dict_row['tot_pvs'], dict_row['tot_transactions'], dict_row['tot_revenue'], dict_row['tot_registrations'],
                        sTouchingDate)
            else:
                self._printDebug('fb record with multiple media data on ' + sTouchingDate)
            
        # proc residual - fb api sends log but GA does not detect
        if len(dictFbLogDailyLogSrl):
            for s_remaing_log, dict_remaing_row in dictFbLogDailyLogSrl.items():
                dictCostRst = self.__redefineCost('fb_biz', dict_remaing_row['biz_id'], dict_remaing_row['cost'])
                sUa = dict_remaing_row['ua']
                sSource = dict_remaing_row['source']
                sRstType = dict_remaing_row['rst_type']
                sMedia = dict_remaing_row['media']
                sBrd = dict_remaing_row['brd']
                sCamp1st = dict_remaing_row['campaign_1st']
                sCamp2nd = dict_remaing_row['campaign_2nd']
                sCamp3rd = dict_remaing_row['campaign_3rd']
                sTerm = ''
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                    str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), 
                    dict_remaing_row['imp'], dict_remaing_row['click'], dict_remaing_row['conv_cnt'], 
                    dict_remaing_row['conv_amnt'],  0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate)

    def __cleanupAdwGaRaw(self, lstGaLogSingle):  # , oSvMysql, sTouchingDate):
        sTerm = lstGaLogSingle['term'].replace(' ', '').upper() # adwords sometimes provides log like "campaign code = (not set) term = (not set)"
        # upper() needs to be done as ADW capitalizes term always but GA term is case sensitive.
        # that's why GA term needs to be capitalized before integrate with ADW log
        if lstGaLogSingle['media'] == 'cpc' and lstGaLogSingle['campaign_1st'].find('GDN' ) > -1: # merge and create new performance row for GDN
            sTerm = lstGaLogSingle['campaign_1st'].lower()
            lstGaLogSingle['brd'] = 0  # GA starts to allocate term on session via GDN; some of terms are branded; this corrupts GDN aggregation logic
        
        sRowId = lstGaLogSingle['ua']+'|@|'+lstGaLogSingle['source']+'|@|'+lstGaLogSingle['rst_type']+'|@|'+lstGaLogSingle['media']+'|@|'+str(lstGaLogSingle['brd'])+'|@|'+ \
            lstGaLogSingle['campaign_1st']+'|@|'+lstGaLogSingle['campaign_2nd']+'|@|'+ lstGaLogSingle['campaign_3rd']+'|@|'+sTerm

        nSession = int(lstGaLogSingle['session'])
        fNewPer = float(lstGaLogSingle['new_per'])/100
        fBouncePer = float(lstGaLogSingle['bounce_per'])/100
        fDurSec = float(lstGaLogSingle['duration_sec'])
        fPvs = float(lstGaLogSingle['pvs'])
        nTrs = int(lstGaLogSingle['transactions'])
        nRev = int(lstGaLogSingle['revenue'])
        nReg = int(lstGaLogSingle['registrations'])
        fTotNew = nSession * fNewPer
        fTotBounce = nSession * fBouncePer
        fTotDurSec = nSession * fDurSec
        fTotPvs = nSession * fPvs
        if self.__g_dictAdwMergedDailyLog.get(sRowId, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictAdwMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_registrations'] += nReg
        else:  # if new log requested
            self.__g_dictAdwMergedDailyLog[sRowId] = {
                'session':nSession, 'tot_new_session':fTotNew, 'tot_bounce':fTotBounce, 'tot_duration_sec':fTotDurSec,
                'tot_pvs':fTotPvs, 'tot_transactions':nTrs, 'tot_revenue':nRev, 'tot_registrations':nReg
            }

    def __mergeAdwGaRaw(self, oSvMysql, sTouchingDate):
        # assume that adwords log is identified by term and UA basically
        # GDN campaign sometimes is identified by Campaign Code and UA
        # this is very different with YOUTUBE campaign from google ads
        dictAdwLogDailyLogSrl = {}
        lstAdwLogDaily = oSvMysql.executeQuery('getAdwLogDaily', sTouchingDate, 'GG')
        for dictSingleLog in lstAdwLogDaily:
            dictAdwLogDailyLogSrl[dictSingleLog['log_srl']] = {
                'customer_id':dictSingleLog['customer_id'], 'ua':dictSingleLog['ua'], 'term':dictSingleLog['term'], 
                'rst_type':dictSingleLog['rst_type'], 'media':dictSingleLog['media'], 'brd':dictSingleLog['brd'],
                'campaign_1st':dictSingleLog['campaign_1st'], 'campaign_2nd':dictSingleLog['campaign_2nd'],
                'campaign_3rd':dictSingleLog['campaign_3rd'], 'cost':dictSingleLog['cost'], 'imp':dictSingleLog['imp'],
                'click':dictSingleLog['click'], 'conv_cnt':dictSingleLog['conv_cnt'],'conv_amnt':dictSingleLog['conv_amnt']
            }
        for s_unique_tag, dict_row in self.__g_dictAdwMergedDailyLog.items():
            aRowId = s_unique_tag.split('|@|')
            sUa = aRowId[0]
            # sSource = aRowId[1]
            sRstType = aRowId[2]
            sMedia = aRowId[3]
            sBrd = aRowId[4]
            sCamp1st = aRowId[5]
            sCamp2nd = aRowId[6]
            sCamp3rd = aRowId[7]
            sTerm = aRowId[8]
            if sCamp1st.find('GDN') > -1:  # if the campaign is related with GDN
                lstAdwLogDailyTemp = oSvMysql.executeQuery('getAdwLogSpecificRmk', sTouchingDate, 
                    'GG', sMedia, sCamp1st, sCamp2nd, sCamp3rd, sUa )
                nRecCnt = len(lstAdwLogDailyTemp)
                if nRecCnt == 0:
                    lstAdwLogDaily = []
                elif nRecCnt == 1:
                    lstAdwLogDaily = lstAdwLogDailyTemp
                elif nRecCnt > 1:
                    lstAdwLogDaily = []
                    nGrossCost = 0
                    nGrossImp = 0
                    nGrossClick = 0
                    nGrossConvCnt = 0
                    nGrossConvAmnt = 0
                    for lstAdwLogSingle in lstAdwLogDailyTemp:
                        nGrossCost += lstAdwLogSingle['cost']
                        nGrossImp += lstAdwLogSingle['imp']
                        nGrossClick += lstAdwLogSingle['click']
                        nGrossConvCnt += lstAdwLogSingle['conv_cnt']
                        nGrossConvAmnt += lstAdwLogSingle['conv_amnt']

                    dictTmp = {'customer_id':lstAdwLogSingle['customer_id'], 'cost': nGrossCost, 'imp': nGrossImp, 
                        'click': nGrossClick, 'conv_cnt': nGrossConvCnt, 'conv_amnt': nGrossConvAmnt}
                    lstAdwLogDaily.append(dictTmp)
            else: # if the campaign is related with normal ADW
                lstAdwLogDaily = oSvMysql.executeQuery('getAdwLogSpecific', sTouchingDate, 'GG', sMedia, sTerm, 
                    sCamp1st, sCamp2nd, sCamp3rd, sUa)

            nRecCnt = len(lstAdwLogDaily)
            if nRecCnt == 0: # add new non-media bounded log, if unable to find related ADW raw log
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog',	sUa,sTerm, 'google', sRstType, 
                    sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                    0,0,0,0,0,0,0,
                    dict_row['session'], dict_row['tot_new_session'],
                    dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                    dict_row['tot_pvs'], dict_row['tot_transactions'],
                    dict_row['tot_revenue'], dict_row['tot_registrations'], sTouchingDate)
            elif nRecCnt == 1: # add new media bounded log, if able to find related ADW raw log
                try:
                    dictAdwLogDailyLogSrl.pop(lstAdwLogDaily[0]['log_srl'])
                except KeyError:
                    for lstAdwLogSingle in lstAdwLogDailyTemp:
                        dictAdwLogDailyLogSrl.pop(lstAdwLogSingle['log_srl'])
                finally:
                    dictCostRst = self.__redefineCost('adwords', lstAdwLogDaily[0]['customer_id'], lstAdwLogDaily[0]['cost'])
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog',	sUa,sTerm, 'google', sRstType, sMedia,
                        sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), 
                        lstAdwLogDaily[0]['imp'],lstAdwLogDaily[0]['click'],
                        lstAdwLogDaily[0]['conv_cnt'],lstAdwLogDaily[0]['conv_amnt'], 
                        dict_row['session'], dict_row['tot_new_session'],
                        dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                        dict_row['tot_pvs'], dict_row['tot_transactions'],
                        dict_row['tot_revenue'], dict_row['tot_registrations'], sTouchingDate)
            else:
                self._printDebug('adw record with multiple media data on ' + sTouchingDate)
                self._printDebug(s_unique_tag)
                self._printDebug(dict_row)
                self._printDebug(lstAdwLogDaily)
        # proc residual
        dictImpression = {'M_1':0, 'M_0':0, 'P_1':0, 'P_0':0} # means mob_brded':0, 'mob_nonbrded':0, 'pc_brded':0, 'pc_nonbrded
        for nLogSrl in dictAdwLogDailyLogSrl:
            if dictAdwLogDailyLogSrl[nLogSrl]['cost'] > 0:
                dictCostRst = self.__redefineCost('adwords', dictAdwLogDailyLogSrl[nLogSrl]['customer_id'], 
                    dictAdwLogDailyLogSrl[nLogSrl]['cost'])
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', dictAdwLogDailyLogSrl[nLogSrl]['ua'],
                    dictAdwLogDailyLogSrl[nLogSrl]['term'], 'google', dictAdwLogDailyLogSrl[nLogSrl]['rst_type'],
                    dictAdwLogDailyLogSrl[nLogSrl]['media'],dictAdwLogDailyLogSrl[nLogSrl]['brd'],
                    dictAdwLogDailyLogSrl[nLogSrl]['campaign_1st'], dictAdwLogDailyLogSrl[nLogSrl]['campaign_2nd'], 
                    dictAdwLogDailyLogSrl[nLogSrl]['campaign_3rd'],
                    str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), 
                    dictAdwLogDailyLogSrl[nLogSrl]['imp'],dictAdwLogDailyLogSrl[nLogSrl]['click'],
                    dictAdwLogDailyLogSrl[nLogSrl]['conv_cnt'],dictAdwLogDailyLogSrl[nLogSrl]['conv_amnt'], 
                    0, 0, 0, 0,	0, 0, 0, 0,	sTouchingDate)
            elif dictAdwLogDailyLogSrl[nLogSrl]['cost'] == 0:
                sIndication = dictAdwLogDailyLogSrl[nLogSrl]['ua'] + '_' + str(dictAdwLogDailyLogSrl[nLogSrl]['brd'])
                dictImpression[sIndication] = dictImpression[sIndication] + int(dictAdwLogDailyLogSrl[nLogSrl]['imp'])
        
        for sIdx in dictImpression:
            if dictImpression[sIdx] > 0:
                aIdx = sIdx.split('_')
                sUa = aIdx[0]
                bBrded = aIdx[1]
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog',	sUa, '|@|sv', 
                    'google', 'PS','cpc',bBrded, '|@|sv','|@|sv', '|@|sv',
                    0,0, 0, dictImpression[sIdx],0,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate)

    def __cleanupYtGaRaw(self, lstGaLogSingle):  # , oSvMysql, sTouchingDate):
        # assume that term from youtube is always '(not set)'
        sRowId = lstGaLogSingle['ua']+'|@|'+lstGaLogSingle['source']+'|@|'+lstGaLogSingle['rst_type']+'|@|'+lstGaLogSingle['media']+'|@|'+str(lstGaLogSingle['brd'])+'|@|'+ \
            lstGaLogSingle['campaign_1st']+'|@|'+lstGaLogSingle['campaign_2nd']+'|@|'+ lstGaLogSingle['campaign_3rd']
        
        nSession = int(lstGaLogSingle['session'])
        fNewPer = float(lstGaLogSingle['new_per'])/100
        fBouncePer = float(lstGaLogSingle['bounce_per'])/100
        fDurSec = float(lstGaLogSingle['duration_sec'])
        fPvs = float(lstGaLogSingle['pvs'])
        nTrs = int(lstGaLogSingle['transactions'])
        nRev = int(lstGaLogSingle['revenue'])
        nReg = int(lstGaLogSingle['registrations'])
        fTotNew = nSession * fNewPer
        fTotBounce = nSession * fBouncePer
        fTotDurSec = nSession * fDurSec
        fTotPvs = nSession * fPvs
        if self.__g_dictYtMergedDailyLog.get(sRowId, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictYtMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictYtMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictYtMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictYtMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictYtMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictYtMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictYtMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictYtMergedDailyLog[sRowId]['tot_registrations'] += nReg
        else:  # if new log requested
            self.__g_dictYtMergedDailyLog[sRowId] = {
                'session':nSession, 'tot_new_session':fTotNew, 'tot_bounce':fTotBounce, 'tot_duration_sec':fTotDurSec,
                'tot_pvs':fTotPvs, 'tot_transactions':nTrs, 'tot_revenue':nRev, 'tot_registrations':nReg
            }

    def __mergeYtGaRaw(self, oSvMysql, sTouchingDate):
        # assume that YOUTUBE log is identified by Campaign Code and UA always
        # this is very different with adwords campaign from google ads
        dictYtLogDailyLogSrl = {}
        lstYtLogDaily = oSvMysql.executeQuery('getAdwLogDaily', sTouchingDate, 'YT') # assume that term from youtube is always '(not set)'
        for dictSingleLog in lstYtLogDaily:
            dictYtLogDailyLogSrl[dictSingleLog['log_srl']] = {
                'customer_id':dictSingleLog['customer_id'],'ua':dictSingleLog['ua'],'rst_type':dictSingleLog['rst_type'], # 'term':dictSingleLog['term'],
                'media':dictSingleLog['media'],'brd':dictSingleLog['brd'],'campaign_1st':dictSingleLog['campaign_1st'],
                'campaign_2nd':dictSingleLog['campaign_2nd'],'campaign_3rd':dictSingleLog['campaign_3rd'],
                'cost':dictSingleLog['cost'],'imp':dictSingleLog['imp'],'click':dictSingleLog['click'],
                'conv_cnt':dictSingleLog['conv_cnt'],'conv_amnt':dictSingleLog['conv_amnt']
            }
        for s_unique_tag, dict_row in self.__g_dictYtMergedDailyLog.items():
            aRowId = s_unique_tag.split('|@|')
            sUa = aRowId[0]
            # sSource = aRowId[1]
            sRstType = aRowId[2]
            sMedia = aRowId[3]
            sBrd = aRowId[4]
            sCamp1st = aRowId[5]
            sCamp2nd = aRowId[6]
            sCamp3rd = aRowId[7]
            sTerm = '(not set)' # assume that term from youtube is always '(not set)'
            lstYtLogDailyTemp = oSvMysql.executeQuery('getAdwLogSpecificRmk', sTouchingDate, 'YT', sMedia, 
                sCamp1st, sCamp2nd, sCamp3rd, sUa)
            nRecCnt = len(lstYtLogDailyTemp)
            if nRecCnt == 0:
                lstYtLogDaily = []
            elif nRecCnt == 1:
                lstYtLogDaily = lstYtLogDailyTemp
            elif nRecCnt > 1:
                lstYtLogDaily = []
                nGrossCost = 0
                nGrossImp = 0
                nGrossClick = 0
                nGrossConvCnt = 0
                nGrossConvAmnt = 0
                for lstYtLogSingle in lstYtLogDailyTemp:
                    nGrossCost += lstYtLogSingle['cost']
                    nGrossImp += lstYtLogSingle['imp']
                    nGrossClick += lstYtLogSingle['click']
                    nGrossConvCnt += lstYtLogSingle['conv_cnt']
                    nGrossConvAmnt += lstYtLogSingle['conv_amnt']

                dictTmp = {'customer_id':lstYtLogSingle['customer_id'], 'cost': nGrossCost, 'imp': nGrossImp, 'click': nGrossClick,
                    'conv_cnt': nGrossConvCnt, 'conv_amnt': nGrossConvAmnt}
                lstYtLogDaily.append(dictTmp)
            nRecCnt = len(lstYtLogDaily)
            if nRecCnt == 0:
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa, sTerm, 'youtube', sRstType, sMedia,sBrd,
                    sCamp1st,sCamp2nd,sCamp3rd,
                    0,0,0,0,0,0,0,
                    dict_row['session'], dict_row['tot_new_session'],
                    dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                    dict_row['tot_pvs'], dict_row['tot_transactions'],
                    dict_row['tot_revenue'], dict_row['tot_registrations'],
                    sTouchingDate)
            elif nRecCnt == 1:
                try:
                    dictYtLogDailyLogSrl.pop(lstYtLogDaily[0]['log_srl'])
                except KeyError:
                    try:
                        for lstYtLogSingle in lstYtLogDailyTemp:
                            dictYtLogDailyLogSrl.pop(lstYtLogSingle['log_srl'])
                    except KeyError:
                        lstYtLogDaily[0]['cost'] = 0
                        # 비슷한 캠페인 명칭에 이미 비용이 할당되었기 때문에 0비용으로 처리함 ex) YT_PS_DISP_JOX[빈칸]_INDOOR_DISINFECT  vs YT_PS_DISP_JOX_INDOOR_DISINFECT
                finally:
                    dictCostRst = self.__redefineCost('adwords', lstYtLogDaily[0]['customer_id'], lstYtLogDaily[0]['cost'])
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa, sTerm, 'youtube', sRstType, sMedia,
                        sBrd, sCamp1st, sCamp2nd, sCamp3rd,
                        str(dictCostRst['cost']), str(dictCostRst['agency_fee']), 
                        str(dictCostRst['vat']), lstYtLogDaily[0]['imp'],lstYtLogDaily[0]['click'],
                        lstYtLogDaily[0]['conv_cnt'],lstYtLogDaily[0]['conv_amnt'], 
                        dict_row['session'], dict_row['tot_new_session'],
                        dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                        dict_row['tot_pvs'], dict_row['tot_transactions'],
                        dict_row['tot_revenue'], dict_row['tot_registrations'],
                        sTouchingDate)
            else:
                self._printDebug('youtube record with multiple media data on ' + sTouchingDate)
                self._printDebug(s_unique_tag)
                self._printDebug(dict_row)
                self._printDebug(lstYtLogDaily)
        # proc residual
        dictYtResidualArrangedLog = {}
        for nLogSrl, dict_single_yt_log in dictYtLogDailyLogSrl.items():
            sRowId = dict_single_yt_log['customer_id'] + '|@|'+ dict_single_yt_log['ua']  +'|@|'+ \
                dict_single_yt_log['media']+'|@|' + dict_single_yt_log['rst_type'] + '|@|' + str(dict_single_yt_log['brd'])+'|@|' + \
                dict_single_yt_log['campaign_1st']+'|@|'+ dict_single_yt_log['campaign_2nd']+'|@|'+ dict_single_yt_log['campaign_3rd']
            nCost = int(dict_single_yt_log['cost'])
            nImp = int(dict_single_yt_log['imp'])
            nClick = int(dict_single_yt_log['click'])
            nConvCnt = int(dict_single_yt_log['conv_cnt'])
            nConvAmnt = int(dict_single_yt_log['conv_amnt'])
            if dictYtResidualArrangedLog.get(sRowId, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
                dictYtResidualArrangedLog[sRowId]
                dictYtResidualArrangedLog[sRowId]['cost'] += nCost
                dictYtResidualArrangedLog[sRowId]['imp'] += nImp
                dictYtResidualArrangedLog[sRowId]['click'] += nClick
                dictYtResidualArrangedLog[sRowId]['conv_cnt'] += nConvCnt
                dictYtResidualArrangedLog[sRowId]['conv_amnt'] += nConvAmnt
            else:  # if new log requested
                dictYtResidualArrangedLog[sRowId] = {
                    'cost':nCost,'imp':nImp,'click':nClick,'conv_cnt':nConvCnt,'conv_amnt':nConvAmnt
                }
        for sMergedResidualYtLogId, dict_single_arranged_log in dictYtResidualArrangedLog.items():
            aRowId = sMergedResidualYtLogId.split('|@|')
            sCid = aRowId[0]
            sUa = aRowId[1]
            sMedia = aRowId[2]
            sRstType = aRowId[3]
            sBrd = aRowId[4]
            sCamp1st = aRowId[5]
            sCamp2nd = aRowId[6]
            sCamp3rd = aRowId[7]
            sTerm = '(not set)' # assume that term from youtube is always '(not set)'
            fCost = 0.0
            fAgencyFee = 0.0
            fVat = 0.0
            if dict_single_arranged_log['cost'] > 0: # but assume that youtube does not provide free impression
                dictCostRst = self.__redefineCost('adwords', sCid, dict_single_arranged_log['cost'])
                fCost = dictCostRst['cost']
                fAgencyFee = dictCostRst['agency_fee']
                fVat = dictCostRst['vat']
            oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa, sTerm, 'youtube', 
                sRstType, sMedia, sBrd,	sCamp1st, sCamp2nd, sCamp3rd, str(fCost), str(fAgencyFee), str(fVat), 
                dict_single_arranged_log['imp'], dict_single_arranged_log['click'],
                dict_single_arranged_log['conv_cnt'], dict_single_arranged_log['conv_amnt'], 
                0, 0, 0, 0,	0, 0, 0, 0,	sTouchingDate)

    def __cleanupKkoGaRaw(self, lstGaLogSingle):  #, oSvMysql, sTouchingDate):
        # nvad campaign parser should be integrated into svparser class
        sRowId = lstGaLogSingle['ua']+'|@|'+lstGaLogSingle['source']+'|@|'+lstGaLogSingle['rst_type']+'|@|'+lstGaLogSingle['media']+'|@|'+str(lstGaLogSingle['brd'])+'|@|'+ \
            lstGaLogSingle['campaign_1st']+'|@|'+lstGaLogSingle['campaign_2nd']+'|@|'+ lstGaLogSingle['campaign_3rd']+'|@|'+ lstGaLogSingle['term']
        
        nSession = int(lstGaLogSingle['session'])
        fNewPer = float(lstGaLogSingle['new_per'])/100
        fBouncePer = float(lstGaLogSingle['bounce_per'])/100
        fDurSec = float(lstGaLogSingle['duration_sec'])
        fPvs = float(lstGaLogSingle['pvs'])
        nTrs = int(lstGaLogSingle['transactions'])
        nRev = int(lstGaLogSingle['revenue'])
        nReg = int(lstGaLogSingle['registrations'])
        fTotNew = nSession * fNewPer
        fTotBounce = nSession * fBouncePer
        fTotDurSec = nSession * fDurSec
        fTotPvs = nSession * fPvs
        if self.__g_dictKkoMergedDailyLog.get(sRowId, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictKkoMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_registrations'] += nReg
        else:  # if new log requested
            self.__g_dictKkoMergedDailyLog[sRowId] = {
                'session':nSession,'tot_new_session':fTotNew,'tot_bounce':fTotBounce,'tot_duration_sec':fTotDurSec,
                'tot_pvs':fTotPvs,'tot_transactions':nTrs,'tot_revenue':nRev,'tot_registrations':nReg
            }

    def __mergeKkoGaRaw(self, oSvMysql, sTouchingDate):
        dictKkoLogDailyLogSrl = {}
        lstKkoLogDaily = oSvMysql.executeQuery('getKkoLogDaily', sTouchingDate) # kakao itself does not differ by UA
        for dictSingleLog in lstKkoLogDaily:
            if len(dictSingleLog['term']) == 0:
                dictSingleLog['term'] = '(not set)'
            
            sDailyLogId = dictSingleLog['ua']+'|@|'+'kakao'+'|@|'+dictSingleLog['rst_type']+'|@|'+dictSingleLog['media']+'|@|'+str(dictSingleLog['brd'])+'|@|'+\
                dictSingleLog['campaign_1st']+'|@|'+dictSingleLog['campaign_2nd']+'|@|'+dictSingleLog['campaign_3rd']+'|@|'+dictSingleLog['term']
            dictKkoLogDailyLogSrl[sDailyLogId] = {
                'customer_id':dictSingleLog['customer_id'], 'cost_inc_vat':dictSingleLog['cost_inc_vat'],
                'imp':dictSingleLog['imp'], 'click':dictSingleLog['click'], 'conv_cnt_direct':dictSingleLog['conv_cnt_direct'],
                'conv_amnt_direct':dictSingleLog['conv_amnt_direct']
            }
        for sMergedLog, dict_single_row in self.__g_dictKkoMergedDailyLog.items():
            aRowId = sMergedLog.split('|@|')
            sUa = aRowId[0]
            sSource = aRowId[1]
            sRstType = aRowId[2]
            sMedia = aRowId[3]
            sBrd = aRowId[4]
            sCamp1st = aRowId[5]
            sCamp2nd = aRowId[6]
            sCamp3rd = aRowId[7]
            sTerm = aRowId[8]
            nRecCnt = len(lstKkoLogDaily)
            if nRecCnt == 0: # GA log exists without KKO PS data
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa, sTerm, 'kakao', sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                    0,0,0,0,0,0,0,
                    dict_single_row['session'], dict_single_row['tot_new_session'],
                    dict_single_row['tot_bounce'], dict_single_row['tot_duration_sec'],
                    dict_single_row['tot_pvs'], dict_single_row['tot_transactions'],
                    dict_single_row['tot_revenue'], dict_single_row['tot_registrations'], sTouchingDate)
            else:  # GA log exists with KKO PS data
                try:  # if designated log already created
                    dictKkoLogDailyLogSrl[sMergedLog]
                    dictCostRst = self.__redefineCost('kakao', dictKkoLogDailyLogSrl[sMergedLog]['customer_id'],
                        dictKkoLogDailyLogSrl[sMergedLog]['cost_inc_vat'])
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa, sTerm, 'kakao', sRstType, 
                        sMedia, sBrd, sCamp1st, sCamp2nd, sCamp3rd,
                        str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), 
                        dictKkoLogDailyLogSrl[sMergedLog]['imp'], dictKkoLogDailyLogSrl[sMergedLog]['click'],
                        dictKkoLogDailyLogSrl[sMergedLog]['conv_cnt_direct'], dictKkoLogDailyLogSrl[sMergedLog]['conv_amnt_direct'], 
                        dict_single_row['session'], dict_single_row['tot_new_session'],
                        dict_single_row['tot_bounce'], dict_single_row['tot_duration_sec'],
                        dict_single_row['tot_pvs'], dict_single_row['tot_transactions'],
                        dict_single_row['tot_revenue'], dict_single_row['tot_registrations'], sTouchingDate)
                    dictKkoLogDailyLogSrl.pop(sMergedLog) # removed resigtered log
                except KeyError: # if new log requested
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa, sTerm, 'kakao', sRstType, sMedia,
                        sBrd, sCamp1st, sCamp2nd, sCamp3rd, 0,0,0,0,0,0,0,
                        dict_single_row['session'], dict_single_row['tot_new_session'],
                        dict_single_row['tot_bounce'], dict_single_row['tot_duration_sec'],
                        dict_single_row['tot_pvs'], dict_single_row['tot_transactions'],
                        dict_single_row['tot_revenue'], dict_single_row['tot_registrations'], sTouchingDate)
        # proc residual minor kakao moment log
        dictImpression = {'M_1':0, 'M_0':0, 'P_1':0, 'P_0':0} # means mob_brded':0, 'mob_nonbrded':0, 'pc_brded':0, 'pc_nonbrded
        for sDailyLogId in dictKkoLogDailyLogSrl: # regist residual; means KKO MOMENT data without GA log 
            aRowId = sDailyLogId.split('|@|')
            sUa = aRowId[0]
            sSource = aRowId[1]
            sRstType = aRowId[2]
            sMedia = aRowId[3]
            sBrd = aRowId[4]
            sCamp1st = aRowId[5]
            sCamp2nd = aRowId[6]
            sCamp3rd = aRowId[7]
            sTerm = aRowId[8]
            if dictKkoLogDailyLogSrl[sDailyLogId]['cost_inc_vat'] > 0:
                dictCostRst = self.__redefineCost('kakao', dictKkoLogDailyLogSrl[sDailyLogId]['customer_id'], dictKkoLogDailyLogSrl[sDailyLogId]['cost_inc_vat'])	
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog',	sUa, sTerm, sSource, sRstType, sMedia, sBrd,
                    sCamp1st, sCamp2nd, sCamp3rd,
                    str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), dictKkoLogDailyLogSrl[sDailyLogId]['imp'], dictKkoLogDailyLogSrl[sDailyLogId]['click'],
                    dictKkoLogDailyLogSrl[sDailyLogId]['conv_cnt_direct'], dictKkoLogDailyLogSrl[sDailyLogId]['conv_amnt_direct'], 
                    0, 0, 0, 0,	0, 0, 0, 0,	sTouchingDate)
            elif dictKkoLogDailyLogSrl[sDailyLogId]['cost_inc_vat'] == 0:
                sIndication = sUa + '_' + str(sBrd)
                dictImpression[sIndication] = dictImpression[sIndication] + int(dictKkoLogDailyLogSrl[sDailyLogId]['imp'])
        try: 
            for sIdx in dictImpression:
                if dictImpression[sIdx] > 0:
                    aIdx = sIdx.split('_')
                    sUa = aIdx[0]
                    bBrded = aIdx[1]
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog',	sUa, '|@|sv', 
                        'kakao', 'PS','cpc',bBrded, '|@|sv','|@|sv', '|@|sv',0,0, 0, dictImpression[sIdx],
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate)
        except NameError: # if imp only log not exists
            pass

    def __mergeNvAdGaRaw(self, oSvMysql, sTouchingDate):
        dictNvadLogDailyLogSrl = {}
        lstNvadLogDaily = oSvMysql.executeQuery('getNvadLogDaily', sTouchingDate)
        for dictSingleLog in lstNvadLogDaily:
            dictNvadLogDailyLogSrl[dictSingleLog['log_srl']] = {
                'customer_id':dictSingleLog['customer_id'],'ua':dictSingleLog['ua'], 'term':dictSingleLog['term'],
                'rst_type':dictSingleLog['rst_type'], 'media':dictSingleLog['media'], 'brd':dictSingleLog['brd'],
                'campaign_1st':dictSingleLog['campaign_1st'], 'campaign_2nd':dictSingleLog['campaign_2nd'],
                'campaign_3rd':dictSingleLog['campaign_3rd'],
                'cost':dictSingleLog['cost'],'imp':dictSingleLog['imp'],'click':dictSingleLog['click'],
                'conv_cnt':dictSingleLog['conv_cnt'],'conv_amnt':dictSingleLog['conv_amnt']
            }
        for s_unique_tag, dict_row in self.__g_dictNvadMergedDailyLog.items():
            aRowId = s_unique_tag.split('|@|')
            sUa = aRowId[0]
            # sSource = aRowId[1]
            sRstType = aRowId[2]
            sMedia = aRowId[3]
            sBrd = aRowId[4]
            sCamp1st = aRowId[5]
            sCamp2nd = aRowId[6]
            sCamp3rd = aRowId[7]
            sTerm = aRowId[8]
            if sMedia == 'display' and sCamp1st == 'BRS':
                lstNvadLogDaily = oSvMysql.executeQuery('getNvadLogSpecificDisplay', sTouchingDate, sMedia, sTerm, sCamp1st, sCamp2nd, sUa)
            elif sMedia == 'cpc' and sCamp1st == 'NVSHOP' or sCamp1st == 'NVSHOPPING': # merge and create new performance row
                sCamp1st = 'NVSHOP' # unify old and confusing old NVSHOP naming convention
                lstNvadLogDaily = oSvMysql.executeQuery('getNvadLogSpecificNvshop', sTouchingDate, sMedia, sTerm, sCamp1st, sUa)
                if len(lstNvadLogDaily) > 1:
                    pass #self._printDebug('NVAD separates NVSHOPPING item so allocate appropriately')
            else:
                lstNvadLogDaily = oSvMysql.executeQuery('getNvadLogSpecificCpc', sTouchingDate, sMedia, sTerm, 
                    sCamp1st, sCamp2nd, sCamp3rd, sUa)
            
            nRecCnt = len(lstNvadLogDaily)
            if nRecCnt == 0:
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa,sTerm, 'naver', sRstType, sMedia,sBrd,
                    sCamp1st, sCamp2nd, sCamp3rd, 0, 0, 0, 0, 0, 0, 0,
                    dict_row['session'], dict_row['tot_new_session'],
                    dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                    dict_row['tot_pvs'], dict_row['tot_transactions'],
                    dict_row['tot_revenue'], dict_row['tot_registrations'], sTouchingDate)
            elif nRecCnt == 1:
                try:
                    dictNvadLogDailyLogSrl.pop(lstNvadLogDaily[0]['log_srl'])
                except KeyError: 
                    # nvad uppers all alphabet but GA does not, thats why sometime GA log cant match proper NVAD log, 
                    # it this case, bot will lookup into remaining log by term
                    sUpperedOriginalTermFromGA = sTerm.upper()
                    for nLogSrl in dictNvadLogDailyLogSrl:
                        if sUpperedOriginalTermFromGA == dictNvadLogDailyLogSrl[nLogSrl]['term']:
                            dictNvadLogDailyLogSrl.pop(nLogSrl)
                            break
                finally:
                    dictCostRst = self.__redefineCost('naver_ad', lstNvadLogDaily[0]['customer_id'], lstNvadLogDaily[0]['cost'])
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa, sTerm, 'naver', sRstType, sMedia, sBrd,
                        sCamp1st, sCamp2nd, sCamp3rd, str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']),
                        lstNvadLogDaily[0]['imp'], lstNvadLogDaily[0]['click'], lstNvadLogDaily[0]['conv_cnt'],
                        lstNvadLogDaily[0]['conv_amnt'], dict_row['session'], dict_row['tot_new_session'],
                        dict_row['tot_bounce'], dict_row['tot_duration_sec'], dict_row['tot_pvs'], dict_row['tot_transactions'],
                        dict_row['tot_revenue'], dict_row['tot_registrations'], sTouchingDate)
            else:
                # if 3rd campaign code of the NVR BRS ad group name is changed in busy hour, there would be two log with same term and different 3rd campaign code
                if sMedia == 'display' and sCamp1st == 'BRS':
                    dictTempBrsLog = {'cost':0, 'imp':0, 'click':0, 'conv_cnt':0, 'conv_amnt':0}
                    for dictDuplicatedNvrBrsLog in lstNvadLogDaily:
                        dictTempBrsLog['cost'] = dictTempBrsLog['cost'] + dictDuplicatedNvrBrsLog['cost']
                        dictTempBrsLog['imp'] = dictTempBrsLog['imp'] + dictDuplicatedNvrBrsLog['imp']
                        dictTempBrsLog['click'] = dictTempBrsLog['click'] + dictDuplicatedNvrBrsLog['click']
                        dictTempBrsLog['conv_cnt'] = dictTempBrsLog['conv_cnt'] + dictDuplicatedNvrBrsLog['conv_cnt']
                        dictTempBrsLog['conv_amnt'] = dictTempBrsLog['conv_amnt'] + dictDuplicatedNvrBrsLog['conv_amnt']
                    dictCostRst = self.__redefineCost('naver_ad', lstNvadLogDaily[0]['customer_id'], dictTempBrsLog['cost'])
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa, sTerm, 'naver', sRstType, sMedia, sBrd,
                        sCamp1st, sCamp2nd, sCamp3rd, str(dictCostRst['cost']), str(dictCostRst['agency_fee']), 
                        str(dictCostRst['vat']), dictTempBrsLog['imp'], dictTempBrsLog['click'],
                        dictTempBrsLog['conv_cnt'], dictTempBrsLog['conv_amnt'], dict_row['session'], 
                        dict_row['tot_new_session'], dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                        dict_row['tot_pvs'], dict_row['tot_transactions'], dict_row['tot_revenue'], 
                        dict_row['tot_registrations'], sTouchingDate)	
                else:
                    # aggregate each log into a single GA detected NVAD log, if there happens duplicated nvad log by different nvad campaign name; 
                    # GA recognize utm_campaign only, which is same level of NVAD adgrp name
                    self._printDebug('aggregate duplicated nvad log')
                    self._printDebug(sTouchingDate)
                    self._printDebug(s_unique_tag)
                    self._printDebug(lstNvadLogDaily)
                    nCost = 0
                    nImp = 0
                    nClick = 0
                    nConvCnt = 0
                    nConvAmnt = 0
                    for dictSingleNvadLog in lstNvadLogDaily:
                        nCost = nCost + dictSingleNvadLog['cost']
                        nImp = nImp + dictSingleNvadLog['imp']
                        nClick = nClick + dictSingleNvadLog['click']
                        nConvCnt = nConvCnt + dictSingleNvadLog['conv_cnt']
                        nConvAmnt = nConvAmnt + dictSingleNvadLog['conv_amnt']
                        dictNvadLogDailyLogSrl.pop(dictSingleNvadLog['log_srl'])
                    
                    dictCostRst = self.__redefineCost('naver_ad', lstNvadLogDaily[0]['customer_id'], nCost)
                    oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa, sTerm, 'naver', sRstType, sMedia, sBrd,
                        sCamp1st, sCamp2nd, sCamp3rd, str(dictCostRst['cost']), str(dictCostRst['agency_fee']),
                        str(dictCostRst['vat']), nImp, nClick, nConvCnt, nConvAmnt, 
                        dict_row['session'], dict_row['tot_new_session'], dict_row['tot_bounce'], 
                        dict_row['tot_duration_sec'], dict_row['tot_pvs'], dict_row['tot_transactions'],
                        dict_row['tot_revenue'], dict_row['tot_registrations'], sTouchingDate)					

        dictImpression = {'M_1':0, 'M_0':0, 'P_1':0, 'P_0':0} # means mob_brded':0, 'mob_nonbrded':0, 'pc_brded':0, 'pc_nonbrded
        for nLogSrl in dictNvadLogDailyLogSrl: # regist residual; means NVAD data without GA log 
            if dictNvadLogDailyLogSrl[nLogSrl]['cost'] > 0 or dictNvadLogDailyLogSrl[nLogSrl]['campaign_1st'] == 'BRS':
                dictCostRst = self.__redefineCost('naver_ad', dictNvadLogDailyLogSrl[nLogSrl]['customer_id'], 
                    dictNvadLogDailyLogSrl[nLogSrl]['cost'])
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', dictNvadLogDailyLogSrl[nLogSrl]['ua'], 
                    dictNvadLogDailyLogSrl[nLogSrl]['term'], 'naver', dictNvadLogDailyLogSrl[nLogSrl]['rst_type'],
                    dictNvadLogDailyLogSrl[nLogSrl]['media'], dictNvadLogDailyLogSrl[nLogSrl]['brd'],
                    dictNvadLogDailyLogSrl[nLogSrl]['campaign_1st'], dictNvadLogDailyLogSrl[nLogSrl]['campaign_2nd'], 
                    dictNvadLogDailyLogSrl[nLogSrl]['campaign_3rd'], str(dictCostRst['cost']), str(dictCostRst['agency_fee']), 
                    str(dictCostRst['vat']), dictNvadLogDailyLogSrl[nLogSrl]['imp'], dictNvadLogDailyLogSrl[nLogSrl]['click'],
                    dictNvadLogDailyLogSrl[nLogSrl]['conv_cnt'], dictNvadLogDailyLogSrl[nLogSrl]['conv_amnt'], 
                    0, 0, 0, 0,	0, 0, 0, 0,	sTouchingDate)
            elif dictNvadLogDailyLogSrl[nLogSrl]['cost'] == 0:
                sIndication = dictNvadLogDailyLogSrl[nLogSrl]['ua'] + '_' + str(dictNvadLogDailyLogSrl[nLogSrl]['brd'])
                dictImpression[sIndication] = dictImpression[sIndication] + int(dictNvadLogDailyLogSrl[nLogSrl]['imp'])

        for sIdx in dictImpression:
            if dictImpression[sIdx] > 0:
                aIdx = sIdx.split('_')
                sUa = aIdx[0]
                bBrded = aIdx[1]
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa, '|@|sv', 
                    'naver', 'PS','cpc',bBrded, '|@|sv','|@|sv', '|@|sv',
                    0,0, 0, dictImpression[sIdx],0, 0,0, 0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate)

    def __cleanupNvAdGaRaw(self, lstGaLogSingle, oSvMysql, sTouchingDate):
        sTerm = lstGaLogSingle['term'].replace(' ', '')
        if lstGaLogSingle['media'] == 'cpc' and lstGaLogSingle['campaign_1st'].find( 'NVSHOP' ) > -1: # merge and create new performance row
            lstNvadLogDaily = oSvMysql.executeQuery('getNvadLogSpecificNvshop', sTouchingDate, lstGaLogSingle['media'], 
                sTerm, 'NVSHOP', lstGaLogSingle['ua'] )
            if len(lstNvadLogDaily) == 1:	# NVAD does not separate NVSHOPPING item so change term from "nvshop_XXX" to "nvshop"
                # aTerm = sTerm.split('_') # it means "nvshop_XXX"
                sTerm = 'nvshop' #aTerm[0] # it should be 'nvshop' even if a real value is 'nvshopping', 
                lstGaLogSingle['campaign_1st'] = 'NVSHOP'
                
        sRowId = lstGaLogSingle['ua']+'|@|'+lstGaLogSingle['source']+'|@|'+lstGaLogSingle['rst_type']+'|@|'+lstGaLogSingle['media']+'|@|'+str(lstGaLogSingle['brd'])+'|@|'+ \
            lstGaLogSingle['campaign_1st']+'|@|'+lstGaLogSingle['campaign_2nd']+'|@|'+ lstGaLogSingle['campaign_3rd']+'|@|'+sTerm
        
        nSession = int(lstGaLogSingle['session'])
        fNewPer = float(lstGaLogSingle['new_per'])/100
        fBouncePer = float(lstGaLogSingle['bounce_per'])/100
        fDurSec = float(lstGaLogSingle['duration_sec'])
        fPvs = float(lstGaLogSingle['pvs'])
        nTrs = int(lstGaLogSingle['transactions'])
        nRev = int(lstGaLogSingle['revenue'])
        nReg = int(lstGaLogSingle['registrations'])
        fTotNew = nSession * fNewPer
        fTotBounce = nSession * fBouncePer
        fTotDurSec = nSession * fDurSec
        fTotPvs = nSession * fPvs
        if self.__g_dictNvadMergedDailyLog.get(sRowId, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictNvadMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_registrations'] += nReg
        else:  # if new log requested
            self.__g_dictNvadMergedDailyLog[sRowId] = {
                'session':nSession,'tot_new_session':fTotNew,'tot_bounce':fTotBounce,'tot_duration_sec':fTotDurSec,
                'tot_pvs':fTotPvs,'tot_transactions':nTrs,'tot_revenue':nRev,'tot_registrations':nReg
            }

    def __cleanupOtherGaRaw(self, lstGaLogSingle, sTouchingDate):
        sTerm = lstGaLogSingle['term'].replace(' ', '')
        sRowId = lstGaLogSingle['ua']+'|@|'+lstGaLogSingle['source']+'|@|'+lstGaLogSingle['rst_type']+'|@|'+lstGaLogSingle['media']+'|@|'+str(lstGaLogSingle['brd'])+'|@|'+ \
            lstGaLogSingle['campaign_1st']+'|@|'+lstGaLogSingle['campaign_2nd']+'|@|'+ lstGaLogSingle['campaign_3rd']+'|@|'+sTerm
        
        nSession = int(lstGaLogSingle['session'])
        fNewPer = float(lstGaLogSingle['new_per'])/100
        fBouncePer = float(lstGaLogSingle['bounce_per'])/100
        fDurSec = float(lstGaLogSingle['duration_sec'])
        fPvs = float(lstGaLogSingle['pvs'])
        nTrs = int(lstGaLogSingle['transactions'])
        nRev = int(lstGaLogSingle['revenue'])
        nReg = int(lstGaLogSingle['registrations'])
        fTotNew = nSession * fNewPer
        fTotBounce = nSession * fBouncePer
        fTotDurSec = nSession * fDurSec
        fTotPvs = nSession * fPvs
        if self.__g_dictOtherMergedDailyLog.get(sRowId, self.__g_sSvNull) != self.__g_sSvNull:  # if sRowId exists
            self.__g_dictOtherMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_registrations'] += nReg
        else:  # if new log requested
            self.__g_dictOtherMergedDailyLog[sRowId] = {
                'session':nSession,'tot_new_session':fTotNew,'tot_bounce':fTotBounce,'tot_duration_sec':fTotDurSec,
                'tot_pvs':fTotPvs,'tot_transactions':nTrs,'tot_revenue':nRev,'tot_registrations':nReg
            }

    def __mergeOtherGaRaw(self, oSvMysql, sTouchingDate):
        dictNvPnsInfo = self.get_aloocated_pns_cost(oSvMysql, sTouchingDate, 'naver')
        dictFbPnsInfo = self.get_aloocated_pns_cost(oSvMysql, sTouchingDate, 'facebook')
        nTouchingDate = int(sTouchingDate.replace('-', '' ))
        for s_unique_tag, dict_row in self.__g_dictOtherMergedDailyLog.items():
            aRowId = s_unique_tag.split('|@|')
            sUa = aRowId[0]
            sSource = aRowId[1]
            sRstType = aRowId[2]
            sMedia = aRowId[3]
            sBrd = aRowId[4]
            sCamp1st = aRowId[5]
            sCamp2nd = aRowId[6]
            sCamp3rd = aRowId[7]
            sTerm = aRowId[8]
            nMediaRawCost = 0
            nMediaAgencyCost = 0
            nMediaCostVat = 0
            if sSource == 'naver' and sRstType == 'PNS':
                if len(dictNvPnsInfo) > 0:
                    bPnsDetected = False
                    if nTouchingDate <= self.__g_nPnsTouchingDate: # for the old & non-systematic & complicated situation
                        for nIdx in dictNvPnsInfo:
                            nNickIdx = sTerm.find(dictNvPnsInfo[nIdx]['nick'])
                            if nNickIdx > -1 and dictNvPnsInfo[nIdx]['ua'] == sUa:
                                bPnsDetected = True
                                break
                        if bPnsDetected == False:
                            for nIdx in dictNvPnsInfo:
                                nTermIdx = sTerm.find(dictNvPnsInfo[nIdx]['term'])
                                if nTermIdx > -1 and dictNvPnsInfo[nIdx]['ua'] == sUa:
                                    bPnsDetected = True
                                    break
                        if bPnsDetected:
                            nMediaRawCost = dictNvPnsInfo[nIdx]['media_raw_cost']
                            nMediaAgencyCost = dictNvPnsInfo[nIdx]['media_agency_cost']
                            nMediaCostVat = dictNvPnsInfo[nIdx]['vat']
                            dictNvPnsInfo.pop(nIdx)
                    else: # for the latest & systematic situation
                        sTermForPns = sTerm+'_'+sUa
                        try:
                            dictNvPnsInfo[sTermForPns] # check whether the designated term existed in PNS info
                            nMediaRawCost = dictNvPnsInfo[sTermForPns]['media_raw_cost']
                            nMediaAgencyCost = dictNvPnsInfo[sTermForPns]['media_agency_cost']
                            nMediaCostVat = dictNvPnsInfo[sTermForPns]['vat']
                            dictNvPnsInfo.pop(sTermForPns)
                        except KeyError:
                            pass
            if (sSource == 'facebook' and sRstType == 'PNS') or (sSource == 'instagram' and sRstType == 'PNS'):
                if len(dictNvPnsInfo) > 0:
                    # for the latest & systematic situation only
                    sTermForPns = sTerm+'_'+sUa
                    try:
                        dictFbPnsInfo[sTermForPns] # check whether the designated term existed in PNS info
                        nMediaRawCost = dictFbPnsInfo[sTermForPns]['media_raw_cost']
                        nMediaAgencyCost = dictFbPnsInfo[sTermForPns]['media_agency_cost']
                        nMediaCostVat = dictFbPnsInfo[sTermForPns]['vat']
                        dictNvPnsInfo.pop(sTermForPns)
                    except KeyError:
                        pass
            oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                nMediaRawCost,nMediaAgencyCost,nMediaCostVat,0,0,0,0,
                dict_row['session'], dict_row['tot_new_session'],
                dict_row['tot_bounce'], dict_row['tot_duration_sec'],
                dict_row['tot_pvs'], dict_row['tot_transactions'],
                dict_row['tot_revenue'], dict_row['tot_registrations'], sTouchingDate)
        
        # register pns cost info if remaining pns info exists
        if len(dictNvPnsInfo) > 0:
            for sIdx in dictNvPnsInfo:
                if nTouchingDate <= self.__g_nPnsTouchingDate: # for the old & non-systematic & complicated situation
                    sTerm = dictNvPnsInfo[sIdx]['term'] + '_' + dictNvPnsInfo[sIdx]['service_type'] + '_' + dictNvPnsInfo[sIdx]['nick'] + '_' + dictNvPnsInfo[sIdx]['regdate']
                    sCamp1st = self.__g_oSvCampaignParser.getSvPnsServiceTypeTag( dictNvPnsInfo[sIdx]['service_type'] )
                    if sCamp1st == 'RELATED':
                        sTerm = dictNvPnsInfo[sIdx]['term']

                    sCamp2nd = dictNvPnsInfo[sIdx]['regdate']
                    sCamp3rd = '00'
                    sPnsUa = dictNvPnsInfo[sIdx]['ua']
                else: # for the latest & systematic situation
                    sTerm = sIdx.replace('_P', '').replace('_M', '')
                    aIdx = sIdx.split('_')
                    nRegdatePos = len(aIdx) - 2
                    nUaPos = len(aIdx) - 1
                    sCamp1st = self.__g_oSvCampaignParser.getSvPnsServiceTypeTag(aIdx[1])
                    if sCamp1st == 'RELATED':
                        sTerm = aIdx[0]
                    sCamp2nd = aIdx[nRegdatePos]
                    sCamp3rd = '00'
                    sPnsUa = aIdx[nUaPos]

                fMediaRawCost = dictNvPnsInfo[sIdx]['media_raw_cost']
                fMediaAgencyCost = dictNvPnsInfo[sIdx]['media_agency_cost']
                fMediaCostVat = dictNvPnsInfo[sIdx]['vat']
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sPnsUa, sTerm, 'naver', 'PNS', 'organic', 0,
                    sCamp1st, sCamp2nd, sCamp3rd, fMediaRawCost, fMediaAgencyCost, fMediaCostVat,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate)

        if len(dictFbPnsInfo) > 0:
            for sIdx, dict_pns_row in dictFbPnsInfo.items(): # for the latest & systematic situation only
                sTerm = sIdx.replace('_P', '').replace('_M', '')
                aIdx = sIdx.split('_')
                nRegdatePos = len(aIdx) - 2
                nUaPos = len(aIdx) - 1
                sCamp1st = self.__g_oSvCampaignParser.getSvPnsServiceTypeTag(aIdx[1])
                sTerm = aIdx[0]
                sCamp2nd = aIdx[nRegdatePos]
                sCamp3rd = '00'
                sPnsUa = aIdx[nUaPos]
                fMediaRawCost = dict_pns_row['media_raw_cost']
                fMediaAgencyCost = dict_pns_row['media_agency_cost']
                fMediaCostVat = dict_pns_row['vat']
                oSvMysql.executeQuery('insertCompiledGaMediaDailyLog', sPnsUa, sTerm, 'instagram', 'PNS', 'organic', 0,
                    sCamp1st, sCamp2nd, sCamp3rd, fMediaRawCost, fMediaAgencyCost, fMediaCostVat,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate)

    def get_aloocated_pns_cost(self, o_sv_db, s_touching_date, s_source):
        """ get allocated Paid NS cost """
        dict_pns_info = {}
        if s_source not in self.__g_dictPnsSource:
            self._printDebug('invalid pns info request :' + s_source)
            return dict_pns_info
        
        dt_touching_date = datetime.datetime.strptime(s_touching_date, '%Y-%m-%d').date()
        # sql file is in svplugins.integrate_db.queries
        lst_contract_info = o_sv_db.executeQuery('getPnsContract', self.__g_dictPnsSource[s_source],
                                                    dt_touching_date, dt_touching_date)
        if len(lst_contract_info) > 0:
            n_pns_info_idx = 0
            n_touching_date = int(s_touching_date.replace('-', '' ))
            o_reg_ex = re.compile(r"\d+%$") # pattern ex) 2% 23%
            for dict_single_contract in lst_contract_info:
                # define raw cost & agnecy cost -> calculate vat from sum of define raw cost & agnecy cost
                n_period_cost_incl_vat = dict_single_contract['cost_incl_vat']
                n_period_cost_exc_vat = math.ceil(n_period_cost_incl_vat/1.1)
                m = o_reg_ex.search(dict_single_contract['agency_rate_percent']) # match() vs search()
                if m: # if valid percent string
                    f_rate = int(dict_single_contract['agency_rate_percent'].replace('%',''))/100
                else: # if invalid percent string
                    self._printDebug('invalid percent string ' + dict_single_contract['agency_rate_percent'])
                    raise Exception('stop')
                del m

                f_contract_raw_cost = int((1 - f_rate) * n_period_cost_exc_vat)
                f_agency_cost = int(f_rate * n_period_cost_exc_vat)
                dt_delta_days = dict_single_contract['execute_date_end'] - dict_single_contract['execute_date_begin']
                s_contract_type = self.__g_dictPnsContract[dict_single_contract['contract_type']]
                s_term = dict_single_contract['media_term']
                s_nickname = dict_single_contract['contractor_id']
                s_regdate = dict_single_contract['regdate'].strftime('%Y%m%d')
                for s_ua in self.__g_dictNvPnsUaCostPortion:
                    f_portion = self.__g_dictNvPnsUaCostPortion[s_ua]
                    f_daily_media_raw_cost = f_contract_raw_cost / (dt_delta_days.days + 1) * f_portion
                    f_daily_agency_cost = f_agency_cost / (dt_delta_days.days + 1) * f_portion
                    f_vat = (f_daily_media_raw_cost + f_daily_agency_cost) * 0.1
                    if n_touching_date <= self.__g_nPnsTouchingDate: # for the old & non-systematic & complicated situation
                        dict_pns_info[n_pns_info_idx] = {
                            'service_type':s_contract_type, 'term':s_term,'nick':s_nickname,'ua':s_ua, 
                            'media_raw_cost':f_daily_media_raw_cost,'media_agency_cost':f_daily_agency_cost,'vat':f_vat,
                            'regdate':s_regdate
                        }
                        n_pns_info_idx += 1
                    else: # for the latest & systematic situation
                        if s_nickname is '-':
                            s_row_id = s_term+'_'+s_contract_type+'_'+s_regdate+'_'+s_ua
                        else:
                            s_row_id = s_term+'_'+s_contract_type+'_'+s_nickname+'_'+s_regdate+'_'+s_ua
                        dict_pns_info[s_row_id] = {
                            'media_raw_cost':f_daily_media_raw_cost, 'media_agency_cost':f_daily_agency_cost, 'vat':f_vat
                        }
            del o_reg_ex
        return dict_pns_info

    def __redefineCost(self, sMedia, sCustomerId, nCost):
        dictRst = {'cost':0, 'agency_fee':0, 'vat':0}
        if nCost > 0:
            sBeginDate = '20010101' # define default ancient begin date
            sEndDate = datetime.datetime.today().strftime('%Y%m%d')
            fRate = 0.0
            sAgencyInfoFilePath = os.path.join(self.__g_sDataPath, sMedia, str(sCustomerId), 'conf', 'agency_info.tsv')
            try:
                with open(sAgencyInfoFilePath, 'r') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    for row in reader:
                        pass # read last line only
            except FileNotFoundError:
                self._printDebug('failure -> ' + sAgencyInfoFilePath + ' does not exist')
                return dictRst  # raise Exception('stop')
                
            aPeriod = row[0].split('-')
            if len(aPeriod[0]) > 0:
                try: # validate requsted date
                    sBeginDate = datetime.datetime.strptime(aPeriod[0], '%Y%m%d').strftime('%Y%m%d')
                except ValueError:
                    self._printDebug('start date:' + aPeriod[0] + ' is invalid date string')

            if len(aPeriod[1]) > 0:
                try: # validate requsted date
                    sEndDate = datetime.datetime.strptime(aPeriod[1], '%Y%m%d').strftime('%Y%m%d')
                except ValueError:
                    self._printDebug('end date:' + aPeriod[0] + ' is invalid date string')

            dtBegin = datetime.datetime.strptime(sBeginDate, '%Y%m%d').date()
            dtEnd = datetime.datetime.strptime(sEndDate, '%Y%m%d').date()
            dtNow = datetime.date.today()
            if dtNow < dtBegin:
                self._printDebug('invalid ' + sMedia + ' agency begin date')
                raise Exception('stop')
            
            if dtNow > dtEnd:
                self._printDebug('invalid ' + sMedia + ' agency begin date')
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
            if row[3] == 'back':
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
            elif row[3] == 'markup':
                nFinalCost = nCost
                nAgencyCost = fRate * nCost
            elif row[3] == 'direct':
                nFinalCost = nCost
            else:
                self._printDebug('invalid margin type ' + row[3])
                raise Exception('stop')

            if sMedia == 'naver_ad':
                if nCost != nFinalCost + nAgencyCost:
                    self._printDebug(nCost)
                    self._printDebug(nFinalCost)
                    self._printDebug(nAgencyCost)

            # if dictSourceToRetrieve[sMedia] == 'kakao': # csv download based data
            if sMedia == 'kakao': # csv download based data
                nVatFromFinalCost = int(nFinalCost * 0.1)
                nVatFromnAgencyCost = int(nAgencyCost * 0.1)
                dictRst['cost'] = nFinalCost - nVatFromFinalCost
                dictRst['agency_fee'] = nAgencyCost - nVatFromnAgencyCost
                dictRst['vat'] = nVatFromFinalCost + nVatFromnAgencyCost
            else:
                dictRst['cost'] = nFinalCost
                dictRst['agency_fee'] = nAgencyCost
                dictRst['vat'] = (nFinalCost + nAgencyCost ) * 0.1
        return dictRst


if __name__ == '__main__': # for console debugging
    nCliParams = len(sys.argv)
    if nCliParams > 1:
        with svJobPlugin() as oJob: # to enforce to call plugin destructor
            oJob.set_my_name('integrate_db')
            oJob.parse_command(sys.argv)
            oJob.do_task(None)
    else:
        print('warning! [config_loc] params are required for console execution.')
