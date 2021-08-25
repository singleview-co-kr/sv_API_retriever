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

# singleview library
if __name__ == '__main__': # for console debugging
    sys.path.append('../../svcommon')
    import sv_mysql
    import sv_campaign_parser
    import sv_object, sv_api_config_parser, sv_plugin
    sys.path.append('../../conf') # singleview config
    import basic_config
else: # for platform running
    from svcommon import sv_mysql
    from svcommon import sv_campaign_parser
    from svcommon import sv_object, sv_api_config_parser, sv_plugin
    # singleview config
    from conf import basic_config # singleview config


class svJobPlugin(sv_object.ISvObject, sv_plugin.ISvPlugin):
    __g_sMode = None
    __g_bFbProcess = False
    __g_nPnsTouchingDate = 20190126 # to seperate the old & non-systematic & complicated situation for PNS cost process
    __g_sDataPath = None
    __g_sRetrieveMonth = None
    __g_sNvrPnsInfoFilePath = None
    __g_sFbPnsInfoFilePath = None
    __g_dictNvadMergedDailyLog = None
    __g_dictAdwMergedDailyLog = None
    __g_dictKkoMergedDailyLog = None
    __g_dictYtMergedDailyLog = None
    __g_dictFbMergedDailyLog = None
    __g_dictOtherMergedDailyLog = None
    __g_dictNvPnsUaCostPortion = {'M':0.7, 'P':0.3} # sum must be 1
    __g_oSvCampaignParser = None

    def __init__(self):
        """ validate dictParams and allocate params to private global attribute """
        self._g_sVersion = '1.0.15'
        self._g_sLastModifiedDate = '4th, Jul 2021'
        self._g_oLogger = logging.getLogger(__name__ + ' v'+self._g_sVersion)
        self._g_dictParam.update({'yyyymm':None, 'mode':None})

    def do_task(self):
        self.__g_sRetrieveMonth = self._g_dictParam['yyyymm']
        self.__g_sMode = self._g_dictParam['mode']

        oSvApiConfigParser = sv_api_config_parser.SvApiConfigParser(self._g_dictParam['analytical_namespace'], self._g_dictParam['config_loc'])
        oResp = oSvApiConfigParser.getConfig()
        self.__g_oSvCampaignParser = sv_campaign_parser.svCampaignParser()
        dict_acct_info = oResp['variables']['acct_info']
        if dict_acct_info is None:
            self._printDebug('stop -> invalid config_loc')
            #raise Exception('stop')
            return
            
        s_sv_acct_id = list(dict_acct_info.keys())[0]
        s_acct_title = dict_acct_info[s_sv_acct_id]['account_title']
        dict_nvr_ad_acct = dict_acct_info[s_sv_acct_id]['nvr_ad_acct']
        self.__g_sTblPrefix = dict_acct_info[s_sv_acct_id]['tbl_prefix']
        
        del dict_nvr_ad_acct['manager_login_id'], dict_nvr_ad_acct['api_key'], dict_nvr_ad_acct['secret_key']
        s_cid = dict_nvr_ad_acct['customer_id']  # list(dict_nvr_ad_acct.keys())[0]

        self.__g_sDataPath = os.path.join(basic_config.ABSOLUTE_PATH_BOT, 'files', s_sv_acct_id, s_acct_title)
        self.__g_sNvrPnsInfoFilePath = os.path.join(self.__g_sDataPath, 'naver_ad', s_cid, 'conf', 'contract_pns_info.tsv')
        self.__g_sFbPnsInfoFilePath = os.path.join(self.__g_sDataPath, 'fb_biz', dict_acct_info[s_sv_acct_id]['fb_biz_aid'], 'conf', 'contract_pns_info.tsv')

        with sv_mysql.SvMySql('svplugins.integrate_db') as oSvMysql:
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            oSvMysql.initialize()
        
        # aNvrAdAcct = dict_acct_info[s_sv_acct_id]['nvr_ad_acct']
        
        if self.__g_sRetrieveMonth != None:
            dictDateRange = self.__deleteCertainMonth()
        else:
            dictDateRange = self.__getTouchDateRange()
        
        if dictDateRange['start_date'] is None and dictDateRange['end_date'] is None:
            self._printDebug('stop - weird raw data last date')
            return

        start = dictDateRange['start_date']
        end = dictDateRange['end_date']
        if start > end:
            self._printDebug('error - weird raw data last date')
            return

        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]
        nIdx = 0
        nSentinel = len(date_generated)
        for date in date_generated:
            sDate = date.strftime('%Y-%m-%d')
            self.__compileDailyRecord(sDate)
            self._printProgressBar(nIdx + 1, nSentinel, prefix = 'Arrange data:', suffix = 'Complete', length = 50)
            nIdx += 1

    def __deleteCertainMonth(self):
        dictRst = {'start_date': None, 'end_date': None}
        nYr = int(self.__g_sRetrieveMonth[:4])
        nMo = int(self.__g_sRetrieveMonth[4:None])
        try:
            lstMonthRange = calendar.monthrange(nYr, nMo)
        except calendar.IllegalMonthError:
            self._printDebug( 'invalid yyyymm' )
            # raise Exception('remove' )
            return dictRst
        
        try:
            lstDirectory = os.listdir(os.path.join(self.__g_sDataPath, 'fb_biz'))
            # lstFbbizLastDate = []
            for sCid in lstDirectory:
                if sCid == 'alias_info_campaign.tsv':
                    continue
                try:
                    file = open(os.path.join(self.__g_sDataPath, 'fb_biz', sCid, 'conf', 'general.latest', 'r'))
                    self.__g_bFbProcess = True
                except FileNotFoundError:
                    pass
        except FileNotFoundError:
            pass
        
        sStartDateRetrieval = self.__g_sRetrieveMonth[:4] + '-' + self.__g_sRetrieveMonth[4:None] + '-01'
        sEndDateRetrieval = self.__g_sRetrieveMonth[:4] + '-' + self.__g_sRetrieveMonth[4:None] + '-' + str(lstMonthRange[1])
        with sv_mysql.SvMySql('svplugins.integrate_db') as oSvMysql:
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            lstRst = oSvMysql.executeQuery('deleteCompiledLogByPeriod', sStartDateRetrieval, sEndDateRetrieval)

        dictRst['start_date'] = datetime.datetime.strptime(sStartDateRetrieval, '%Y-%m-%d')
        dictRst['end_date'] = datetime.datetime.strptime(sEndDateRetrieval, '%Y-%m-%d')
        return dictRst  # {'start_date': dtStartDateRetrieval, 'end_date':dtEndDateRetrieval}

    def __getTouchDateRange(self):
        """ get latest retrieval info of naver_ad """
        dictRst = {'start_date': None, 'end_date': None}
        lstDirectory = os.listdir(os.path.join(self.__g_sDataPath, 'naver_ad'))
        lstNaveradLastDate = []
        try:
            lstDirectory.remove('alias_info_campaign.tsv')
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
        lstDirectory = os.listdir(os.path.join(self.__g_sDataPath, 'adwords'))
        lstAdwordsLastDate = []
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
        try:
            lstDirectory = os.listdir(os.path.join(self.__g_sDataPath, 'fb_biz'))
            lstFbbizLastDate = []
            for sCid in lstDirectory:
                if sCid == 'alias_info_campaign.tsv':
                    continue
                
                # check adwords agency_info.tsv
                dictCostRst = self.__redefineCost('fb_biz', sCid, 100)  # 100 is dummy to check agency_info.tsv file
                if dictCostRst['cost'] == 0 and dictCostRst['agency_fee'] == 0 and dictCostRst['vat'] == 0:
                    return dictRst
                try:
                    file = open(os.path.join(self.__g_sDataPath, 'fb_biz', sCid, 'general.latest'), 'r')
                    sLatestDate = file.read()
                    dtLatest = datetime.date(int(sLatestDate[:4]), int(sLatestDate[4:6]), int(sLatestDate[6:8]))
                    lstFbbizLastDate.append(dtLatest)
                    self.__g_bFbProcess = True
                except FileNotFoundError:
                    pass
        except FileNotFoundError:
            pass

        if self.__g_sMode == 'ignore_fb':
            self.__g_bFbProcess = False

        with sv_mysql.SvMySql('svplugins.integrate_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            # init rst dictionary
            dictRst = {}
            # define last date of process
            lstLastDate = []

            lstGaLogDateRange = oSvMysql.executeQuery('getGaLogDateMaxMin')
            lstLastDate.append(lstGaLogDateRange[0]['maxdate'])
            
            lstAdwLogDateRange = oSvMysql.executeQuery('getAdwLogDateMin')
            lstNvadLogDateRange = oSvMysql.executeQuery('getNvadLogDateMin')
            if self.__g_bFbProcess:
                lstFbLogDateRange = oSvMysql.executeQuery('getFbIgLogDateMin')
            
            lstLastDate.append(max(lstAdwordsLastDate))
            lstLastDate.append(max(lstNaveradLastDate))
            dictRst['end_date'] = min(lstLastDate)
            
            # define first date of process
            lstFirstDate = []
            lstGrossCompiledLogDateRange = oSvMysql.executeQuery('getGrossCompiledLogDateMaxMin')
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

        # check non integrated date
        self.__g_dictNvadMergedDailyLog = {}
        self.__g_dictAdwMergedDailyLog = {}
        self.__g_dictYtMergedDailyLog = {}
        self.__g_dictKkoMergedDailyLog = {}
        self.__g_dictFbMergedDailyLog = {}
        self.__g_dictOtherMergedDailyLog = {}

        with sv_mysql.SvMySql('svplugins.integrate_db') as oSvMysql: # to enforce follow strict mysql connection mgmt
            oSvMysql.setTablePrefix(self.__g_sTblPrefix)
            lstGaLogDaily = oSvMysql.executeQuery('getGaLogDaily', sTouchingDate)
            
            #if sTouchingDate == '2019-01-27':
            for lstGaLog in lstGaLogDaily:
                # get naver ad media data
                if lstGaLog['source'] == 'naver' and lstGaLog['rst_type'] == 'PS':
                    self.__cleanupNvAdGaRaw(lstGaLog, oSvMysql, sTouchingDate)
                elif lstGaLog['source'] == 'google' and lstGaLog['rst_type'] == 'PS':
                    self.__cleanupAdwGaRaw(lstGaLog, oSvMysql, sTouchingDate)
                elif lstGaLog['source'] == 'youtube' and lstGaLog['rst_type'] == 'PS':
                    self.__cleanupYtGaRaw(lstGaLog, oSvMysql, sTouchingDate)
                elif lstGaLog['source'] == 'kakao' and lstGaLog['rst_type'] == 'PS':
                    self.__cleanupKkoGaRaw(lstGaLog, oSvMysql, sTouchingDate)
                elif self.__g_bFbProcess and lstGaLog['source'] == 'facebook' or self.__g_bFbProcess and lstGaLog['source'] == 'instagram':
                    if lstGaLog['rst_type'] == 'PS':
                        self.__cleanupFbGaRaw(lstGaLog, oSvMysql, sTouchingDate)
                else: # others
                    self.__cleanupOtherGaRaw(lstGaLog, sTouchingDate)
            
            self.__mergeNvAdGaRaw(oSvMysql, sTouchingDate)
            self.__mergeAdwGaRaw(oSvMysql, sTouchingDate)
            self.__mergeYtGaRaw(oSvMysql, sTouchingDate)
            if self.__g_bFbProcess:
                self.__mergeFbGaRaw(oSvMysql, sTouchingDate)
            if len(self.__g_dictKkoMergedDailyLog) > 0: # only if kakao performance log exists
                self.__mergeKkoGaRaw(oSvMysql, sTouchingDate)
            self.__mergeOtherGaRaw(oSvMysql, sTouchingDate)

    def __cleanupFbGaRaw(self, lstGaLogSingle, oSvMysql, sTouchingDate):
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

        try: # if designated log already created
            self.__g_dictFbMergedDailyLog[sRowId]
            self.__g_dictFbMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictFbMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictFbMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictFbMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictFbMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictFbMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictFbMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictFbMergedDailyLog[sRowId]['tot_registrations'] += nReg
        except KeyError: # if new log requested
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
        dictFbMergedDailyLogSortBySession = sorted(self.__g_dictFbMergedDailyLog.items(), key=(lambda x:x[1]['session']), reverse=True)
        for lstMergedLog in self.__g_dictFbMergedDailyLog:
            aRowId = lstMergedLog.split('|@|')
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
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                            str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), lstFbLogDaily[0]['imp'],lstFbLogDaily[0]['click'],
                            lstFbLogDaily[0]['conv_cnt'],lstFbLogDaily[0]['conv_amnt'], 
                            self.__g_dictFbMergedDailyLog[lstMergedLog]['session'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_new_session'],
                            self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                            self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_transactions'],
                            self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_registrations'],
                            sTouchingDate)
                
                if nRecCnt == 0:
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        0,0,0,0,0,0,0,
                        self.__g_dictFbMergedDailyLog[lstMergedLog]['session'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_new_session'],
                        self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                        self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_transactions'],
                        self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_registrations'],
                        sTouchingDate)
            elif nRecCnt == 1:
                try: # if designated log exists
                    dictFbLogDailyLogSrl.pop(lstFbLogDaily[0]['log_srl'])
                    dictCostRst = self.__redefineCost('fb_biz', lstFbLogDaily[0]['biz_id'], lstFbLogDaily[0]['cost'])
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                            str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), lstFbLogDaily[0]['imp'],lstFbLogDaily[0]['click'],
                            lstFbLogDaily[0]['conv_cnt'],lstFbLogDaily[0]['conv_amnt'], 
                            self.__g_dictFbMergedDailyLog[lstMergedLog]['session'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_new_session'],
                            self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                            self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_transactions'],
                            self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_registrations'],
                            sTouchingDate)
                except KeyError: 
                    # if designated log has been already popped; FB sometimes reports utm_term and GA separate FB campaign code by utm_term;
                    # self.__g_dictFbMergedDailyLog already has been sorted by session number; 
                    # as a result specified cost already has been allocated to the most-sessioned utm_term
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        0,0,0,0,0,0,0,
                        self.__g_dictFbMergedDailyLog[lstMergedLog]['session'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_new_session'],
                        self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                        self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_transactions'],
                        self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictFbMergedDailyLog[lstMergedLog]['tot_registrations'],
                        sTouchingDate)
            else:
                self._printDebug('fb record with multiple media data on ' + sTouchingDate)
            
        # proc residual - fb api sends log but GA does not detect
        if len(dictFbLogDailyLogSrl):
            for lstRemainingLog in dictFbLogDailyLogSrl:
                dictCostRst = self.__redefineCost('fb_biz', dictFbLogDailyLogSrl[lstRemainingLog]['biz_id'], dictFbLogDailyLogSrl[lstRemainingLog]['cost'])
                sUa = dictFbLogDailyLogSrl[lstRemainingLog]['ua']
                sSource = dictFbLogDailyLogSrl[lstRemainingLog]['source']
                sRstType = dictFbLogDailyLogSrl[lstRemainingLog]['rst_type']
                sMedia = dictFbLogDailyLogSrl[lstRemainingLog]['media']
                sBrd = dictFbLogDailyLogSrl[lstRemainingLog]['brd']
                sCamp1st = dictFbLogDailyLogSrl[lstRemainingLog]['campaign_1st']
                sCamp2nd = dictFbLogDailyLogSrl[lstRemainingLog]['campaign_2nd']
                sCamp3rd = dictFbLogDailyLogSrl[lstRemainingLog]['campaign_3rd']
                sTerm = ''
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog', sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                    str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), dictFbLogDailyLogSrl[lstRemainingLog]['imp'],
                    dictFbLogDailyLogSrl[lstRemainingLog]['click'], dictFbLogDailyLogSrl[lstRemainingLog]['conv_cnt'], dictFbLogDailyLogSrl[lstRemainingLog]['conv_amnt'], 
                    0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate)

    def __cleanupAdwGaRaw(self, lstGaLogSingle, oSvMysql, sTouchingDate):
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

        try: # if designated log already created
            self.__g_dictAdwMergedDailyLog[sRowId]
            self.__g_dictAdwMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictAdwMergedDailyLog[sRowId]['tot_registrations'] += nReg
        except KeyError: # if new log requested
            self.__g_dictAdwMergedDailyLog[sRowId] = {
                'session':nSession,'tot_new_session':fTotNew,'tot_bounce':fTotBounce,'tot_duration_sec':fTotDurSec,
                'tot_pvs':fTotPvs,'tot_transactions':nTrs,'tot_revenue':nRev,'tot_registrations':nReg
            }

    def __mergeAdwGaRaw(self, oSvMysql, sTouchingDate):
        # assume that adwords log is identified by term and UA basically
        # GDN campaign sometimes is identified by Campaign Code and UA
        # this is very different with YOUTUBE campaign from google ads
        dictAdwLogDailyLogSrl = {}
        lstAdwLogDaily = oSvMysql.executeQuery('getAdwLogDaily', sTouchingDate, 'GG')

        for dictSingleLog in lstAdwLogDaily:
            dictAdwLogDailyLogSrl[dictSingleLog['log_srl']] = {
                'customer_id':dictSingleLog['customer_id'],'ua':dictSingleLog['ua'],'term':dictSingleLog['term'],'rst_type':dictSingleLog['rst_type'],
                'media':dictSingleLog['media'],'brd':dictSingleLog['brd'],'campaign_1st':dictSingleLog['campaign_1st'],
                'campaign_2nd':dictSingleLog['campaign_2nd'],'campaign_3rd':dictSingleLog['campaign_3rd'],
                'cost':dictSingleLog['cost'],'imp':dictSingleLog['imp'],'click':dictSingleLog['click'],
                'conv_cnt':dictSingleLog['conv_cnt'],'conv_amnt':dictSingleLog['conv_amnt']
            }

        for lstMergedLog in self.__g_dictAdwMergedDailyLog:
            aRowId = lstMergedLog.split('|@|')
            sUa = aRowId[0]
            sSource = aRowId[1]
            sRstType = aRowId[2]
            sMedia = aRowId[3]
            sBrd = aRowId[4]
            sCamp1st = aRowId[5]
            sCamp2nd = aRowId[6]
            sCamp3rd = aRowId[7]
            sTerm = aRowId[8]
            
            if sCamp1st.find('GDN') > -1: # if the campaign is related with GDN
                lstAdwLogDailyTemp = oSvMysql.executeQuery('getAdwLogSpecificRmk', sTouchingDate, 'GG', sMedia, sCamp1st, sCamp2nd, sCamp3rd, sUa )
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

                    dictTmp = {'customer_id':lstAdwLogSingle['customer_id'], 'cost': nGrossCost, 'imp': nGrossImp, 'click': nGrossClick, 'conv_cnt': nGrossConvCnt, 'conv_amnt': nGrossConvAmnt}
                    lstAdwLogDaily.append(dictTmp)
            else: # if the campaign is related with normal ADW
                lstAdwLogDaily = oSvMysql.executeQuery('getAdwLogSpecific', sTouchingDate, 'GG', sMedia, sTerm, sCamp1st, sCamp2nd, sCamp3rd, sUa)

            nRecCnt = len(lstAdwLogDaily)
            if nRecCnt == 0: # add new non-media bounded log, if unable to find related ADW raw log
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa,sTerm, 'google', sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        0,0,0,0,0,0,0,
                        self.__g_dictAdwMergedDailyLog[lstMergedLog]['session'], self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_new_session'],
                        self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                        self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_transactions'],
                        self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_registrations'],
                        sTouchingDate)
            elif nRecCnt == 1: # add new media bounded log, if able to find related ADW raw log
                try:
                    dictAdwLogDailyLogSrl.pop(lstAdwLogDaily[0]['log_srl'])
                except KeyError:
                    for lstAdwLogSingle in lstAdwLogDailyTemp:
                        dictAdwLogDailyLogSrl.pop(lstAdwLogSingle['log_srl'])
                finally:
                    dictCostRst = self.__redefineCost('adwords', lstAdwLogDaily[0]['customer_id'], lstAdwLogDaily[0]['cost'])
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa,sTerm, 'google', sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                            str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), lstAdwLogDaily[0]['imp'],lstAdwLogDaily[0]['click'],
                            lstAdwLogDaily[0]['conv_cnt'],lstAdwLogDaily[0]['conv_amnt'], 
                            self.__g_dictAdwMergedDailyLog[lstMergedLog]['session'], self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_new_session'],
                            self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                            self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_transactions'],
                            self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictAdwMergedDailyLog[lstMergedLog]['tot_registrations'],
                            sTouchingDate)
            else:
                self._printDebug('adw record with multiple media data on ' + sTouchingDate)
                self._printDebug(lstMergedLog)
                self._printDebug(self.__g_dictAdwMergedDailyLog[lstMergedLog])
                self._printDebug(lstAdwLogDaily)

        # proc residual
        dictImpression = {'M_1':0, 'M_0':0, 'P_1':0, 'P_0':0} # means mob_brded':0, 'mob_nonbrded':0, 'pc_brded':0, 'pc_nonbrded
        for nLogSrl in dictAdwLogDailyLogSrl:
            if dictAdwLogDailyLogSrl[nLogSrl]['cost'] > 0:
                dictCostRst = self.__redefineCost('adwords', dictAdwLogDailyLogSrl[nLogSrl]['customer_id'], dictAdwLogDailyLogSrl[nLogSrl]['cost'])
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	dictAdwLogDailyLogSrl[nLogSrl]['ua'], dictAdwLogDailyLogSrl[nLogSrl]['term'], 
                    'google', dictAdwLogDailyLogSrl[nLogSrl]['rst_type'],dictAdwLogDailyLogSrl[nLogSrl]['media'],dictAdwLogDailyLogSrl[nLogSrl]['brd'],
                    dictAdwLogDailyLogSrl[nLogSrl]['campaign_1st'], dictAdwLogDailyLogSrl[nLogSrl]['campaign_2nd'], dictAdwLogDailyLogSrl[nLogSrl]['campaign_3rd'],
                    str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), dictAdwLogDailyLogSrl[nLogSrl]['imp'],dictAdwLogDailyLogSrl[nLogSrl]['click'],
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
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, '|@|sv', 
                    'google', 'PS','cpc',bBrded, '|@|sv','|@|sv', '|@|sv',
                    0,0, 0, dictImpression[sIdx],0,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate)

    def __cleanupYtGaRaw(self, lstGaLogSingle, oSvMysql, sTouchingDate):
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

        try: # if designated log already created
            self.__g_dictYtMergedDailyLog[sRowId]
            self.__g_dictYtMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictYtMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictYtMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictYtMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictYtMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictYtMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictYtMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictYtMergedDailyLog[sRowId]['tot_registrations'] += nReg
        except KeyError: # if new log requested
            self.__g_dictYtMergedDailyLog[sRowId] = {
                'session':nSession,'tot_new_session':fTotNew,'tot_bounce':fTotBounce,'tot_duration_sec':fTotDurSec,
                'tot_pvs':fTotPvs,'tot_transactions':nTrs,'tot_revenue':nRev,'tot_registrations':nReg
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
        
        for lstMergedLog in self.__g_dictYtMergedDailyLog:
            aRowId = lstMergedLog.split('|@|')
            sUa = aRowId[0]
            sSource = aRowId[1]
            sRstType = aRowId[2]
            sMedia = aRowId[3]
            sBrd = aRowId[4]
            sCamp1st = aRowId[5]
            sCamp2nd = aRowId[6]
            sCamp3rd = aRowId[7]
            sTerm = '(not set)' # assume that term from youtube is always '(not set)'
            
            lstYtLogDailyTemp = oSvMysql.executeQuery('getAdwLogSpecificRmk', sTouchingDate, 'YT', sMedia, sCamp1st, sCamp2nd, sCamp3rd, sUa)
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

                dictTmp = {'customer_id':lstYtLogSingle['customer_id'], 'cost': nGrossCost, 'imp': nGrossImp, 'click': nGrossClick, 'conv_cnt': nGrossConvCnt, 'conv_amnt': nGrossConvAmnt}
                lstYtLogDaily.append(dictTmp)

            nRecCnt = len(lstYtLogDaily)
            if nRecCnt == 0:
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, sTerm, 'youtube', sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        0,0,0,0,0,0,0,
                        self.__g_dictYtMergedDailyLog[lstMergedLog]['session'], self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_new_session'],
                        self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                        self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_transactions'],
                        self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_registrations'],
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
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, sTerm, 'youtube', sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                            str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), lstYtLogDaily[0]['imp'],lstYtLogDaily[0]['click'],
                            lstYtLogDaily[0]['conv_cnt'],lstYtLogDaily[0]['conv_amnt'], 
                            self.__g_dictYtMergedDailyLog[lstMergedLog]['session'], self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_new_session'],
                            self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                            self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_transactions'],
                            self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictYtMergedDailyLog[lstMergedLog]['tot_registrations'],
                            sTouchingDate)
            else:
                self._printDebug('youtube record with multiple media data on ' + sTouchingDate)
                self._printDebug(lstMergedLog)
                self._printDebug(self.__g_dictYtMergedDailyLog[lstMergedLog])
                self._printDebug(lstYtLogDaily)

        # proc residual
        dictYtResidualArrangedLog = {}
        for nLogSrl in dictYtLogDailyLogSrl:
            sRowId = dictYtLogDailyLogSrl[nLogSrl]['customer_id']+'|@|'+ dictYtLogDailyLogSrl[nLogSrl]['ua']+'|@|'+ \
                dictYtLogDailyLogSrl[nLogSrl]['media']+'|@|'+dictYtLogDailyLogSrl[nLogSrl]['rst_type']+'|@|'+str(dictYtLogDailyLogSrl[nLogSrl]['brd'])+'|@|'+ \
                dictYtLogDailyLogSrl[nLogSrl]['campaign_1st']+'|@|'+dictYtLogDailyLogSrl[nLogSrl]['campaign_2nd']+'|@|'+ \
                dictYtLogDailyLogSrl[nLogSrl]['campaign_3rd']

            nCost = int(dictYtLogDailyLogSrl[nLogSrl]['cost'])
            nImp = int(dictYtLogDailyLogSrl[nLogSrl]['imp'])
            nClick = int(dictYtLogDailyLogSrl[nLogSrl]['click'])
            nConvCnt = int(dictYtLogDailyLogSrl[nLogSrl]['conv_cnt'])
            nConvAmnt = int(dictYtLogDailyLogSrl[nLogSrl]['conv_amnt'])
             
            try: # if designated log already created
                dictYtResidualArrangedLog[sRowId]
                dictYtResidualArrangedLog[sRowId]['cost'] += nCost
                dictYtResidualArrangedLog[sRowId]['imp'] += nImp
                dictYtResidualArrangedLog[sRowId]['click'] += nClick
                dictYtResidualArrangedLog[sRowId]['conv_cnt'] += nConvCnt
                dictYtResidualArrangedLog[sRowId]['conv_amnt'] += nConvAmnt
            except KeyError: # if new log requested
                dictYtResidualArrangedLog[sRowId] = {
                    'cost':nCost,'imp':nImp,'click':nClick,'conv_cnt':nConvCnt,'conv_amnt':nConvAmnt
                }
        
        for sMergedResidualYtLogId in dictYtResidualArrangedLog:
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
            if dictYtResidualArrangedLog[sMergedResidualYtLogId]['cost'] > 0: # but assume that youtube does not provide free impression
                dictCostRst = self.__redefineCost('adwords', sCid, dictYtResidualArrangedLog[sMergedResidualYtLogId]['cost'])
                fCost = dictCostRst['cost']
                fAgencyFee = dictCostRst['agency_fee']
                fVat = dictCostRst['vat']
            
            oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, sTerm, 'youtube', 
                sRstType, sMedia, sBrd,	sCamp1st, sCamp2nd, sCamp3rd, str(fCost), str(fAgencyFee), str(fVat), 
                dictYtResidualArrangedLog[sMergedResidualYtLogId]['imp'],dictYtResidualArrangedLog[sMergedResidualYtLogId]['click'],
                dictYtResidualArrangedLog[sMergedResidualYtLogId]['conv_cnt'],dictYtResidualArrangedLog[sMergedResidualYtLogId]['conv_amnt'], 
                0, 0, 0, 0,	0, 0, 0, 0,	sTouchingDate)

    def __cleanupKkoGaRaw(self, lstGaLogSingle, oSvMysql, sTouchingDate):
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

        try: # if designated log already created
            self.__g_dictKkoMergedDailyLog[sRowId]
            self.__g_dictKkoMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictKkoMergedDailyLog[sRowId]['tot_registrations'] += nReg
        except KeyError: # if new log requested
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
                'customer_id':dictSingleLog['customer_id'],'cost_inc_vat':dictSingleLog['cost_inc_vat'],'imp':dictSingleLog['imp'],'click':dictSingleLog['click'],
                'conv_cnt_direct':dictSingleLog['conv_cnt_direct'],'conv_amnt_direct':dictSingleLog['conv_amnt_direct']
            }

        for sMergedLog in self.__g_dictKkoMergedDailyLog:
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
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, sTerm, 'kakao', sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        0,0,0,0,0,0,0,
                        self.__g_dictKkoMergedDailyLog[sMergedLog]['session'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_new_session'],
                        self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_bounce'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_duration_sec'],
                        self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_pvs'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_transactions'],
                        self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_revenue'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_registrations'],
                        sTouchingDate)
            else: # GA log exists with KKO PS data
                try: # if designated log already created
                    dictKkoLogDailyLogSrl[sMergedLog]
                    dictCostRst = self.__redefineCost('kakao', dictKkoLogDailyLogSrl[sMergedLog]['customer_id'], dictKkoLogDailyLogSrl[sMergedLog]['cost_inc_vat'])
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, sTerm, 'kakao', sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                            str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), dictKkoLogDailyLogSrl[sMergedLog]['imp'],dictKkoLogDailyLogSrl[sMergedLog]['click'],
                            dictKkoLogDailyLogSrl[sMergedLog]['conv_cnt_direct'],dictKkoLogDailyLogSrl[sMergedLog]['conv_amnt_direct'], 
                            self.__g_dictKkoMergedDailyLog[sMergedLog]['session'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_new_session'],
                            self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_bounce'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_duration_sec'],
                            self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_pvs'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_transactions'],
                            self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_revenue'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_registrations'],
                            sTouchingDate)
                    dictKkoLogDailyLogSrl.pop(sMergedLog) # removed resigtered log
                except KeyError: # if new log requested
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, sTerm, 'kakao', sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        0,0,0,0,0,0,0,
                        self.__g_dictKkoMergedDailyLog[sMergedLog]['session'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_new_session'],
                        self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_bounce'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_duration_sec'],
                        self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_pvs'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_transactions'],
                        self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_revenue'], self.__g_dictKkoMergedDailyLog[sMergedLog]['tot_registrations'],
                        sTouchingDate)
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
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, sTerm, sSource, sRstType, sMedia, sBrd,
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
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, '|@|sv', 
                        'kakao', 'PS','cpc',bBrded, '|@|sv','|@|sv', '|@|sv',0,0, 0, dictImpression[sIdx],0, 0,0, 0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate)
        except NameError: # if imp only log not exists
            pass

    def __mergeNvAdGaRaw(self, oSvMysql, sTouchingDate):
        dictNvadLogDailyLogSrl = {}
        lstNvadLogDaily = oSvMysql.executeQuery('getNvadLogDaily', sTouchingDate)
        for dictSingleLog in lstNvadLogDaily:
            dictNvadLogDailyLogSrl[dictSingleLog['log_srl']] = {
                'customer_id':dictSingleLog['customer_id'],'ua':dictSingleLog['ua'],'term':dictSingleLog['term'],'rst_type':dictSingleLog['rst_type'],
                'media':dictSingleLog['media'],'brd':dictSingleLog['brd'],'campaign_1st':dictSingleLog['campaign_1st'],
                'campaign_2nd':dictSingleLog['campaign_2nd'],'campaign_3rd':dictSingleLog['campaign_3rd'],
                'cost':dictSingleLog['cost'],'imp':dictSingleLog['imp'],'click':dictSingleLog['click'],
                'conv_cnt':dictSingleLog['conv_cnt'],'conv_amnt':dictSingleLog['conv_amnt']
            }
        
        for lstMergedLog in self.__g_dictNvadMergedDailyLog:
            aRowId = lstMergedLog.split('|@|')
            sUa = aRowId[0]
            sSource = aRowId[1]
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
                    pass #self._printDebug( 'NVAD separates NVSHOPPING item so allocate appropriately' )
            else:
                lstNvadLogDaily = oSvMysql.executeQuery('getNvadLogSpecificCpc', sTouchingDate, sMedia, sTerm, sCamp1st, sCamp2nd, sCamp3rd, sUa)
            
            nRecCnt = len(lstNvadLogDaily)
            if nRecCnt == 0:
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa,sTerm, 'naver', sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        0,0,0,0,0,0,0,
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['session'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_new_session'],
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_transactions'],
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_registrations'],
                        sTouchingDate)
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
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, sTerm, 'naver', sRstType,sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                            str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), lstNvadLogDaily[0]['imp'],lstNvadLogDaily[0]['click'],
                            lstNvadLogDaily[0]['conv_cnt'],lstNvadLogDaily[0]['conv_amnt'], 
                            self.__g_dictNvadMergedDailyLog[lstMergedLog]['session'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_new_session'],
                            self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                            self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_transactions'],
                            self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_registrations'],
                            sTouchingDate)
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
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, sTerm, 'naver', sRstType,sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), dictTempBrsLog['imp'], dictTempBrsLog['click'],
                        dictTempBrsLog['conv_cnt'], dictTempBrsLog['conv_amnt'], 
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['session'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_new_session'],
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_transactions'],
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_registrations'],
                        sTouchingDate)	
                else:
                    # aggregate each log into a single GA detected NVAD log, if there happens duplicated nvad log by different nvad campaign name; 
                    # GA recognize utm_campaign only, which is same level of NVAD adgrp name
                    self._printDebug('aggregate duplicated nvad log')
                    self._printDebug(sTouchingDate)
                    self._printDebug(lstMergedLog)
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
                    oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, sTerm, 'naver', sRstType,sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                        str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), nImp, nClick, nConvCnt, nConvAmnt, 
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['session'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_new_session'],
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_transactions'],
                        self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictNvadMergedDailyLog[lstMergedLog]['tot_registrations'],
                        sTouchingDate)					

        dictImpression = {'M_1':0, 'M_0':0, 'P_1':0, 'P_0':0} # means mob_brded':0, 'mob_nonbrded':0, 'pc_brded':0, 'pc_nonbrded
        for nLogSrl in dictNvadLogDailyLogSrl: # regist residual; means NVAD data without GA log 
            if dictNvadLogDailyLogSrl[nLogSrl]['cost'] > 0 or dictNvadLogDailyLogSrl[nLogSrl]['campaign_1st'] == 'BRS':
                dictCostRst = self.__redefineCost('naver_ad', dictNvadLogDailyLogSrl[nLogSrl]['customer_id'], dictNvadLogDailyLogSrl[nLogSrl]['cost'])
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	dictNvadLogDailyLogSrl[nLogSrl]['ua'], dictNvadLogDailyLogSrl[nLogSrl]['term'], 
                    'naver', dictNvadLogDailyLogSrl[nLogSrl]['rst_type'],dictNvadLogDailyLogSrl[nLogSrl]['media'],dictNvadLogDailyLogSrl[nLogSrl]['brd'],
                    dictNvadLogDailyLogSrl[nLogSrl]['campaign_1st'], dictNvadLogDailyLogSrl[nLogSrl]['campaign_2nd'], dictNvadLogDailyLogSrl[nLogSrl]['campaign_3rd'],
                    str(dictCostRst['cost']), str(dictCostRst['agency_fee']), str(dictCostRst['vat']), dictNvadLogDailyLogSrl[nLogSrl]['imp'],dictNvadLogDailyLogSrl[nLogSrl]['click'],
                    dictNvadLogDailyLogSrl[nLogSrl]['conv_cnt'],dictNvadLogDailyLogSrl[nLogSrl]['conv_amnt'], 
                    0, 0, 0, 0,	0, 0, 0, 0,	sTouchingDate)
            elif dictNvadLogDailyLogSrl[nLogSrl]['cost'] == 0:
                sIndication = dictNvadLogDailyLogSrl[nLogSrl]['ua'] + '_' + str(dictNvadLogDailyLogSrl[nLogSrl]['brd'])
                dictImpression[sIndication] = dictImpression[sIndication] + int(dictNvadLogDailyLogSrl[nLogSrl]['imp'])

        for sIdx in dictImpression:
            if dictImpression[sIdx] > 0:
                aIdx = sIdx.split('_')
                sUa = aIdx[0]
                bBrded = aIdx[1]
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa, '|@|sv', 
                    'naver', 'PS','cpc',bBrded, '|@|sv','|@|sv', '|@|sv',
                    0,0, 0, dictImpression[sIdx],0, 0,0, 0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate)

    def __cleanupNvAdGaRaw(self, lstGaLogSingle, oSvMysql, sTouchingDate):
        sTerm = lstGaLogSingle['term'].replace(' ', '')
        if lstGaLogSingle['media'] == 'cpc' and lstGaLogSingle['campaign_1st'].find( 'NVSHOP' ) > -1: # merge and create new performance row
            lstNvadLogDaily = oSvMysql.executeQuery('getNvadLogSpecificNvshop', sTouchingDate, lstGaLogSingle['media'], sTerm, 'NVSHOP', lstGaLogSingle['ua'] )
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

        try: # if designated log already created
            self.__g_dictNvadMergedDailyLog[sRowId]
            self.__g_dictNvadMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictNvadMergedDailyLog[sRowId]['tot_registrations'] += nReg
        except KeyError: # if new log requested
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

        try: # if designated log already created
            self.__g_dictOtherMergedDailyLog[sRowId]
            self.__g_dictOtherMergedDailyLog[sRowId]['session'] += nSession
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_new_session'] += fTotNew
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_bounce'] += fTotBounce
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_duration_sec'] += fTotDurSec
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_pvs'] += fTotPvs
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_transactions'] += nTrs
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_revenue'] += nRev
            self.__g_dictOtherMergedDailyLog[sRowId]['tot_registrations'] += nReg
        except KeyError: # if new log requested
            self.__g_dictOtherMergedDailyLog[sRowId] = {
                'session':nSession,'tot_new_session':fTotNew,'tot_bounce':fTotBounce,'tot_duration_sec':fTotDurSec,
                'tot_pvs':fTotPvs,'tot_transactions':nTrs,'tot_revenue':nRev,'tot_registrations':nReg
            }

    def __mergeOtherGaRaw(self, oSvMysql, sTouchingDate):
        dictNvPnsInfo = self.__definePnsCost(sTouchingDate, 'naver')
        dictFbPnsInfo = self.__definePnsCost(sTouchingDate, 'facebook')
        nTouchingDate = int(sTouchingDate.replace('-', '' ))

        for lstMergedLog in self.__g_dictOtherMergedDailyLog:
            aRowId = lstMergedLog.split('|@|')
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
                #self._printDebug( '' )
                #self._printDebug( sTerm + ' on ' + sTouchingDate )
                if len( dictNvPnsInfo ) > 0:
                    bPnsDetected = False
                    if nTouchingDate <= self.__g_nPnsTouchingDate: # for the old & non-systematic & complicated situation
                        for nIdx in dictNvPnsInfo:
                            #self._printDebug( 'try to find nick: ' + dictNvPnsInfo[nIdx]['nick'] )
                            nNickIdx = sTerm.find(dictNvPnsInfo[nIdx]['nick'])
                            if nNickIdx > -1 and dictNvPnsInfo[nIdx]['ua'] == sUa:
                                bPnsDetected = True
                                break
                            #self._printDebug( nIdx )
                        if bPnsDetected == False:
                            for nIdx in dictNvPnsInfo:
                                #self._printDebug( 'try to find term: ' + dictNvPnsInfo[nIdx]['term'] )
                                nTermIdx = sTerm.find(dictNvPnsInfo[nIdx]['term'])
                                if nTermIdx > -1 and dictNvPnsInfo[nIdx]['ua'] == sUa:
                                    bPnsDetected = True
                                    break
                                
                                #self._printDebug( nIdx )
                        if bPnsDetected:
                            nMediaRawCost = dictNvPnsInfo[nIdx]['media_raw_cost']
                            nMediaAgencyCost = dictNvPnsInfo[nIdx]['media_agency_cost']
                            nMediaCostVat = dictNvPnsInfo[nIdx]['vat']
                            dictNvPnsInfo.pop(nIdx)
                            #self._printDebug( '' )
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
                            #self._printDebug( 'no PNS info: ' + sTermForPns )
                            #for sIdx in dictNvPnsInfo:
                            #	self._printDebug( sIdx )
                            #	self._printDebug( dictNvPnsInfo[sIdx] )
                            #self._printDebug( '\n\n' )
            if (sSource == 'facebook' and sRstType == 'PNS') or (sSource == 'instagram' and sRstType == 'PNS'):
                #self._printDebug( '' )
                #self._printDebug( sTerm + ' on ' + sTouchingDate )
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
            oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sUa,sTerm, sSource, sRstType, sMedia,sBrd,sCamp1st,sCamp2nd,sCamp3rd,
                nMediaRawCost,nMediaAgencyCost,nMediaCostVat,0,0,0,0,
                self.__g_dictOtherMergedDailyLog[lstMergedLog]['session'], self.__g_dictOtherMergedDailyLog[lstMergedLog]['tot_new_session'],
                self.__g_dictOtherMergedDailyLog[lstMergedLog]['tot_bounce'], self.__g_dictOtherMergedDailyLog[lstMergedLog]['tot_duration_sec'],
                self.__g_dictOtherMergedDailyLog[lstMergedLog]['tot_pvs'], self.__g_dictOtherMergedDailyLog[lstMergedLog]['tot_transactions'],
                self.__g_dictOtherMergedDailyLog[lstMergedLog]['tot_revenue'], self.__g_dictOtherMergedDailyLog[lstMergedLog]['tot_registrations'],
                sTouchingDate)
        
        # register pns cost info if remaining pns info exists
        if len(dictNvPnsInfo) > 0:
            # self._printDebug( 'proc remaining dictNvPnsInfo on:' + str(nTouchingDate))
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
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sPnsUa, sTerm, 'naver', 'PNS', 'organic',0,sCamp1st,sCamp2nd,sCamp3rd,
                    fMediaRawCost, fMediaAgencyCost, fMediaCostVat,0,0,0,0,#`media_imp`, `media_click`, `media_conv_cnt`, `media_conv_amnt`, 
                    0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate )

        if len(dictFbPnsInfo) > 0:
            #self._printDebug( 'proc remaining dictFbPnsInfo on:' + str(nTouchingDate))
            for sIdx in dictFbPnsInfo: # for the latest & systematic situation only
                sTerm = sIdx.replace('_P', '').replace('_M', '')
                aIdx = sIdx.split('_')
                nRegdatePos = len(aIdx) - 2
                nUaPos = len(aIdx) - 1
                sCamp1st = self.__g_oSvCampaignParser.getSvPnsServiceTypeTag(aIdx[1])
                sTerm = aIdx[0]
                sCamp2nd = aIdx[nRegdatePos]
                sCamp3rd = '00'
                sPnsUa = aIdx[nUaPos]

                fMediaRawCost = dictFbPnsInfo[sIdx]['media_raw_cost']
                fMediaAgencyCost = dictFbPnsInfo[sIdx]['media_agency_cost']
                fMediaCostVat = dictFbPnsInfo[sIdx]['vat']
                oRst = oSvMysql.executeQuery('insertGrossCompiledDailyLog',	sPnsUa, sTerm, 'instagram', 'PNS', 'organic',0,sCamp1st,sCamp2nd,sCamp3rd,
                    fMediaRawCost, fMediaAgencyCost, fMediaCostVat,0,0,0,0,
                    0, 0, 0, 0, 0, 0, 0, 0, sTouchingDate )

    def __definePnsCost(self, sTouchingDate, sSource):
        dictPnsInfo = {}
        dtTouchingDate = datetime.datetime.strptime(sTouchingDate, '%Y-%m-%d').date()
        nTouchingDate = int(sTouchingDate.replace('-', '' ))
        
        if sSource == 'naver':
            sPnsInfoFilePath = self.__g_sNvrPnsInfoFilePath
        elif  sSource == 'facebook':
            sPnsInfoFilePath = self.__g_sFbPnsInfoFilePath
        else:
            self._printDebug('invalid pns info request :' + sSource)
            raise Exception('stop')
        try:
            with codecs.open(sPnsInfoFilePath, 'r',encoding='utf8') as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                nRowCnt = 0
                nPnsInfoIdx = 0
                for row in reader:
                    if nRowCnt > 0:
                        if row[5] != '-':
                            aContractPeriod = row[5].split('~')
                            if len(aContractPeriod[0]) > 0: # contract date - start
                                try: # validate requsted date
                                    dtBeginDate = datetime.datetime.strptime(aContractPeriod[0], '%Y.%m.%d').date()
                                except ValueError:
                                    self._printDebug( 'start date:' + aContractPeriod[0] + ' is invalid date string' )

                            if len(aContractPeriod[1]) > 0: # contract date - end
                                try: # validate requsted date
                                    dtEndDate = datetime.datetime.strptime(aContractPeriod[1], '%Y.%m.%d').date()
                                except ValueError:
                                    self._printDebug( 'end date:' + aContractPeriod[1] + ' is invalid date string' )
                            
                            if dtBeginDate <= dtTouchingDate and dtEndDate >= dtTouchingDate:
                                # define raw cost & agnecy cost -> calculate vat from sum of define raw cost & agnecy cost
                                nPeriodCostInclVat = int(row[3].replace(',', ''))
                                nPeriodCostExcVat = math.ceil(nPeriodCostInclVat/1.1)

                                oRegEx = re.compile(r"\d+%$") # pattern ex) 2% 23%
                                m = oRegEx.search( row[4] ) # match() vs search()
                                if m: # if valid percent string
                                    nPercent = row[4].replace('%','')
                                    fRate = int(nPercent)/100
                                else: # if invalid percent string
                                    self._printDebug('invalid percent string ' + row[4])
                                    raise Exception('stop')

                                fMediaRawCost = int(( 1 - fRate ) * nPeriodCostExcVat)
                                fAgencyCost = int(fRate * nPeriodCostExcVat)
                                dtDeltaDays = dtEndDate - dtBeginDate
                                sServiceType = row[0]
                                sTerm = row[1]
                                sNickName = row[2]
                                sRegdate = row[6].replace('.','')

                                for sUa in self.__g_dictNvPnsUaCostPortion:
                                    fPortion = self.__g_dictNvPnsUaCostPortion[sUa]
                                    fDailyMediaRawCost = fMediaRawCost / (dtDeltaDays.days + 1) * fPortion
                                    fDailyAgencyCost = fAgencyCost / (dtDeltaDays.days + 1) * fPortion
                                    fVat = (fDailyMediaRawCost + fDailyAgencyCost) * 0.1
                                    
                                    if nTouchingDate <= self.__g_nPnsTouchingDate: # for the old & non-systematic & complicated situation
                                        dictPnsInfo[nPnsInfoIdx] = {
                                            'service_type':sServiceType, 'term':sTerm,'nick':sNickName,'ua':sUa, 
                                            'media_raw_cost':fDailyMediaRawCost,'media_agency_cost':fDailyAgencyCost,'vat':fVat,
                                            'regdate':sRegdate
                                        }
                                        nPnsInfoIdx = nPnsInfoIdx + 1
                                    else: # for the latest & systematic situation
                                        if sNickName is '-':
                                            sRowId = sTerm+'_'+sServiceType+'_'+sRegdate+'_'+sUa
                                        else:
                                            sRowId = sTerm+'_'+sServiceType+'_'+sNickName+'_'+sRegdate+'_'+sUa
                                        
                                        dictPnsInfo[sRowId] = {
                                            'media_raw_cost':fDailyMediaRawCost,'media_agency_cost':fDailyAgencyCost,'vat':fVat
                                        }
                    nRowCnt = nRowCnt + 1
        except FileNotFoundError:
            pass
        
        return dictPnsInfo

    def __redefineCost(self, sMedia, sCustomerId, nCost):
        # dictSourceToRetrieve = {'naver_ad':'naver_ad', 'adwords':'adwords', 'fb_biz':'fb_biz', 'kakao':'kakao'}
        dictRst = {'cost':0, 'agency_fee':0, 'vat':0}
        if nCost > 0:
            sBeginDate = '20010101' # define default ancient begin date
            sEndDate = datetime.datetime.today().strftime('%Y%m%d')
            fRate = 0.0
            # try:
            #     sAgencyInfoFilePath = self.__g_sDataPath + '/' + dictSourceToRetrieve[sMedia] +'/' + str( sCustomerId ) + '/agency_info.tsv'
            # except KeyError:
            #     self._printDebug('invalid media classifier' + sMedia)
            #     raise Exception('stop')
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

    # def __printProgressBar(self, iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '='):
    #     """
    #     Print iterations progress
    #     Call in a loop to create terminal progress bar
    #     @params:
    #         iteration   - Required  : current iteration (Int)
    #         total       - Required  : total iterations (Int)
    #         prefix      - Optional  : prefix string (Str)
    #         suffix      - Optional  : suffix string (Str)
    #         decimals    - Optional  : positive number of decimals in percent complete (Int)
    #         length      - Optional  : character length of bar (Int)
    #         fill        - Optional  : bar fill character (Str)
    #     """
    #     if __name__ == '__main__': # for console debugging
    #         percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    #         filledLength = int(length * iteration // total)
    #         bar = fill * filledLength + '-' * (length - filledLength)
    #         print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    #         # Print New Line on Complete
    #         if iteration == total: 
    #             print()

    # def __printDebug( self, sMsg ):
    #     if __name__ == '__main__': # for console debugging
    #         print( sMsg )
    #     else: # for platform running
    #         if( self.__g_oLogger is not None ):
    #             self.__g_oLogger.debug( sMsg )

    # def __getHttpResponse(self, sTargetUrl ):
    #     oSvHttp = sv_http.svHttpCom(sTargetUrl)
    #     oResp = oSvHttp.getUrl()
    #     oSvHttp.close()
    #     if( oResp['error'] == -1 ):
    #         if( oResp['variables'] ): # oResp['variables'] list has items
    #             try:
    #                oResp['variables']['todo']
    #             except KeyError: # if ['variables']['todo'] is not defined
    #                 self.__printDebug( '__checkHttpResp error occured but todo is not defined -> continue')
    #             else: # if ['variables']['todo'] is defined
    #                 sTodo = oResp['variables']['todo']
    #                 if( sTodo == 'stop' ):
    #                     self.__printDebug('HTTP response raised exception!!')
    #                     raise Exception(sTodo)
    #     return oResp


if __name__ == '__main__': # for console debugging
    dictPluginParams = {'config_loc':None, 'yyyymm':None, 'mode':None} # {'config_loc':'1/test_acct', 'yyyymm':'201811'}
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
